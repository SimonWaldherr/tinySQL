// Command ragdemo exercises a complete local RAG pipeline with LM Studio and TinySQL.
// It deliberately prints retrieval evidence before optionally asking an LLM to answer.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	tsql "github.com/SimonWaldherr/tinySQL"
)

type config struct {
	baseURL, embeddingModel, chatModel, docsDir, query string
	chunkSize, overlap, topK, candidateK, batchSize    int
	hybrid, generate, verbose                          bool
}

type chunk struct {
	DocID, Heading, Text string
	Index                int
	Embedding            []float64
}

type hit struct {
	Chunk      chunk
	VectorRank int
	FTSRank    int
	Similarity float64
	Score      float64
}

type evalCase struct {
	Name, Query, DocSuffix, Marker string
}

var evalCases = []evalCase{
	{"neighbor expansion", "How do I retrieve the chunks immediately before and after a vector-search hit?", "rag-guide.md", "RAG_CONTEXT_FROM"},
	{"warm ANN index", "How can I prebuild the HNSW vector index after bulk loading?", "rag-guide.md", "VEC_WARM"},
	{"read-only serving", "How do I reopen a persistent TinySQL snapshot in read-only mode for serving?", "storage-guide.md", "ReadOnly"},
	{"agent schema context", "How can I give an LLM a compact token-budgeted description of the database schema?", "rag-guide.md", "BuildAgentContext"},
	{"request timeout", "Wie setze ich für jede Datenbankabfrage ein Timeout mit Go context?", "developer-integration.md", "context.WithTimeout"},
	{"MCP command", "Which command serves a TinySQL database over MCP for LLM agents?", "cli-guide.md", "cmd/tinysql-mcp-server"},
}

func main() {
	cfg := parseFlags()
	if err := run(context.Background(), cfg); err != nil {
		fmt.Fprintln(os.Stderr, "ragdemo:", err)
		os.Exit(1)
	}
}

func parseFlags() config {
	var c config
	flag.StringVar(&c.baseURL, "base-url", "http://127.0.0.1:1234/v1", "OpenAI-compatible LM Studio API base URL")
	flag.StringVar(&c.embeddingModel, "embedding-model", "text-embedding-granite-embedding-278m-multilingual", "LM Studio embedding model")
	flag.StringVar(&c.chatModel, "chat-model", "google_gemma-3-4b-it-qat", "LM Studio chat model")
	flag.StringVar(&c.docsDir, "docs", "docs", "directory containing Markdown documents")
	flag.StringVar(&c.query, "query", "", "run one query instead of the built-in evaluation suite")
	flag.IntVar(&c.chunkSize, "chunk-size", 900, "target maximum chunk size in Unicode characters")
	flag.IntVar(&c.overlap, "overlap", 250, "overlap between long chunks in Unicode characters")
	flag.IntVar(&c.topK, "top-k", 5, "number of final retrieval hits")
	flag.IntVar(&c.candidateK, "candidate-k", 15, "number of candidates per retriever")
	flag.IntVar(&c.batchSize, "batch-size", 16, "texts per embedding request")
	flag.BoolVar(&c.hybrid, "hybrid", true, "rerank semantic candidates with a conservative BM25 bonus")
	flag.BoolVar(&c.generate, "generate", false, "generate an answer for -query using the chat model")
	flag.BoolVar(&c.verbose, "verbose", false, "print all evaluation hits, not only failures")
	flag.Parse()
	return c
}

func run(ctx context.Context, cfg config) error {
	if cfg.chunkSize < 100 || cfg.overlap < 0 || cfg.overlap >= cfg.chunkSize {
		return errors.New("chunk-size must be >= 100 and overlap must be in [0, chunk-size)")
	}
	if cfg.topK < 1 || cfg.candidateK < cfg.topK || cfg.batchSize < 1 {
		return errors.New("require top-k >= 1, candidate-k >= top-k, and batch-size >= 1")
	}

	chunks, err := loadChunks(cfg.docsDir, cfg.chunkSize, cfg.overlap)
	if err != nil {
		return err
	}
	client := &lmClient{baseURL: strings.TrimRight(cfg.baseURL, "/"), http: &http.Client{Timeout: 3 * time.Minute}}

	started := time.Now()
	for start := 0; start < len(chunks); start += cfg.batchSize {
		end := min(start+cfg.batchSize, len(chunks))
		inputs := make([]string, end-start)
		for i := start; i < end; i++ {
			inputs[i-start] = embeddingText(chunks[i])
		}
		vectors, err := client.embed(ctx, cfg.embeddingModel, inputs)
		if err != nil {
			return fmt.Errorf("embed chunks %d-%d: %w", start+1, end, err)
		}
		for i := range vectors {
			chunks[start+i].Embedding = vectors[i]
		}
		fmt.Fprintf(os.Stderr, "\rembedding chunks: %d/%d", end, len(chunks))
	}
	fmt.Fprintf(os.Stderr, "\rembedding chunks: %d/%d in %s\n", len(chunks), len(chunks), time.Since(started).Round(time.Millisecond))

	db, err := buildDB(chunks)
	if err != nil {
		return err
	}
	fmt.Printf("Corpus: %d chunks from %s | dimensions: %d | chunk=%d overlap=%d\n", len(chunks), cfg.docsDir, len(chunks[0].Embedding), cfg.chunkSize, cfg.overlap)

	if cfg.query != "" {
		hits, err := retrieve(ctx, client, db, chunks, cfg, cfg.query)
		if err != nil {
			return err
		}
		printHits(cfg.query, hits)
		if cfg.generate {
			answer, err := client.answer(ctx, cfg.chatModel, cfg.query, hits)
			if err != nil {
				return err
			}
			fmt.Printf("\nAnswer (%s):\n%s\n", cfg.chatModel, answer)
		}
		return nil
	}
	return evaluate(ctx, client, db, chunks, cfg)
}

func loadChunks(dir string, size, overlap int) ([]chunk, error) {
	paths, err := filepath.Glob(filepath.Join(dir, "*.md"))
	if err != nil {
		return nil, err
	}
	if len(paths) == 0 {
		return nil, fmt.Errorf("no Markdown files found in %q", dir)
	}
	sort.Strings(paths)
	var all []chunk
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		docID := filepath.ToSlash(path)
		parts := chunkMarkdown(string(data), size, overlap)
		for i := range parts {
			parts[i].DocID, parts[i].Index = docID, i
			all = append(all, parts[i])
		}
	}
	if len(all) == 0 {
		return nil, errors.New("documents produced no chunks")
	}
	return all, nil
}

// chunkMarkdown keeps headings attached to their section and only uses overlap
// when a section itself is too long. This avoids blending unrelated sections.
func chunkMarkdown(s string, size, overlap int) []chunk {
	lines := strings.Split(strings.ReplaceAll(s, "\r\n", "\n"), "\n")
	heading := "Document"
	var section []string
	var out []chunk
	flush := func() {
		body := strings.TrimSpace(strings.Join(section, "\n"))
		section = section[:0]
		if body == "" {
			return
		}
		for _, text := range splitRunes(body, size, overlap) {
			out = append(out, chunk{Heading: heading, Text: text})
		}
	}
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "#") && len(strings.TrimLeft(trimmed, "#")) < len(trimmed) {
			flush()
			heading = strings.TrimSpace(strings.TrimLeft(trimmed, "#"))
			continue
		}
		section = append(section, line)
	}
	flush()
	return out
}

func splitRunes(s string, size, overlap int) []string {
	r := []rune(strings.TrimSpace(s))
	if len(r) <= size {
		return []string{string(r)}
	}
	var out []string
	for start := 0; start < len(r); {
		end := min(start+size, len(r))
		if end < len(r) {
			floor := start + size/2
			for i := end; i > floor; i-- {
				if r[i-1] == '\n' || r[i-1] == ' ' {
					end = i
					break
				}
			}
		}
		out = append(out, strings.TrimSpace(string(r[start:end])))
		if end == len(r) {
			break
		}
		start = max(end-overlap, start+1)
	}
	return out
}

func embeddingText(c chunk) string {
	return "Document: " + c.DocID + "\nSection: " + c.Heading + "\n" + c.Text
}

// buildDB creates the corpus table and loads it through ordinary INSERT
// statements — the supported path taught by docs/rag-guide.md. (An earlier
// version of this demo appended directly to table.Rows, which skips
// coerceToVector and the table-version bump vector/FTS caches key on; that
// only happens to work because every row lands before the first query, and
// is not a pattern to copy against a table that queries run against.)
func buildDB(chunks []chunk) (*tsql.DB, error) {
	ctx := context.Background()
	db := tsql.NewDB()
	stmt, err := tsql.ParseSQL(`CREATE TABLE rag_chunks (doc_id TEXT, chunk_index INT, heading TEXT, chunk_text TEXT, embedding VECTOR)`)
	if err != nil {
		return nil, err
	}
	if _, err = tsql.Execute(ctx, db, "default", stmt); err != nil {
		return nil, err
	}
	for _, c := range chunks {
		embJSON, err := json.Marshal(c.Embedding)
		if err != nil {
			return nil, fmt.Errorf("marshal embedding for %s#%d: %w", c.DocID, c.Index, err)
		}
		insertSQL := fmt.Sprintf(
			`INSERT INTO rag_chunks VALUES ('%s', %d, '%s', '%s', VEC_FROM_JSON('%s'))`,
			sqlQuote(c.DocID), c.Index, sqlQuote(c.Heading), sqlQuote(c.Text), embJSON,
		)
		insertStmt, err := tsql.ParseSQL(insertSQL)
		if err != nil {
			return nil, fmt.Errorf("parse insert for %s#%d: %w", c.DocID, c.Index, err)
		}
		if _, err := tsql.Execute(ctx, db, "default", insertStmt); err != nil {
			return nil, fmt.Errorf("insert %s#%d: %w", c.DocID, c.Index, err)
		}
	}
	return db, nil
}

func retrieve(ctx context.Context, client *lmClient, db *tsql.DB, chunks []chunk, cfg config, query string) ([]hit, error) {
	vectors, err := client.embed(ctx, cfg.embeddingModel, []string{query})
	if err != nil {
		return nil, fmt.Errorf("embed query: %w", err)
	}
	qJSON, _ := json.Marshal(vectors[0])
	sql := fmt.Sprintf(`SELECT doc_id, chunk_index, heading, chunk_text, _vec_similarity, _vec_rank FROM VEC_SEARCH('rag_chunks', 'embedding', VEC_FROM_JSON('%s'), %d, 'cosine', 'flat')`, qJSON, cfg.candidateK)
	stmt, err := tsql.ParseSQL(sql)
	if err != nil {
		return nil, err
	}
	rs, err := tsql.Execute(ctx, db, "default", stmt)
	if err != nil {
		return nil, err
	}
	byKey := make(map[string]*hit)
	for _, row := range rs.Rows {
		doc, _ := value[string](row, "doc_id")
		idx, _ := intValue(row, "chunk_index")
		rank, _ := intValue(row, "_vec_rank")
		similarity, _ := value[float64](row, "_vec_similarity")
		c := findChunk(chunks, doc, idx)
		byKey[key(doc, idx)] = &hit{Chunk: c, VectorRank: rank, Similarity: similarity}
	}

	if cfg.hybrid {
		ftsSQL := fmt.Sprintf(`SELECT doc_id, chunk_index, _fts_rank FROM FTS_SEARCH('rag_chunks', '%s', %d, 'doc_id', 'heading', 'chunk_text')`, sqlQuote(ftsQuery(query)), cfg.candidateK)
		ftsStmt, err := tsql.ParseSQL(ftsSQL)
		if err != nil {
			return nil, fmt.Errorf("parse FTS query: %w", err)
		}
		fts, err := tsql.Execute(ctx, db, "default", ftsStmt)
		if err != nil {
			return nil, fmt.Errorf("execute FTS query: %w", err)
		}
		for _, row := range fts.Rows {
			doc, _ := value[string](row, "doc_id")
			idx, _ := intValue(row, "chunk_index")
			rank, _ := intValue(row, "_fts_rank")
			k := key(doc, idx)
			h := byKey[k]
			if h != nil {
				h.FTSRank = rank
			}
		}
	}

	hits := make([]hit, 0, len(byKey))
	for _, h := range byKey {
		if cfg.hybrid {
			// Rerank semantic candidates conservatively: exact terms may move a
			// close candidate up, but cannot replace it with a weak lexical hit.
			h.Score = h.Similarity
			if h.FTSRank > 0 {
				h.Score += .035 * float64(cfg.candidateK-h.FTSRank+1) / float64(cfg.candidateK)
			}
		} else {
			h.Score = h.Similarity
		}
		hits = append(hits, *h)
	}
	sort.Slice(hits, func(i, j int) bool {
		if hits[i].Score == hits[j].Score {
			return hits[i].Similarity > hits[j].Similarity
		}
		return hits[i].Score > hits[j].Score
	})
	if len(hits) > cfg.topK {
		hits = hits[:cfg.topK]
	}
	return hits, nil
}

func evaluate(ctx context.Context, client *lmClient, db *tsql.DB, chunks []chunk, cfg config) error {
	hitsAt1, hitsAtK, reciprocalRank := 0, 0, 0.0
	for _, tc := range evalCases {
		hits, err := retrieve(ctx, client, db, chunks, cfg, tc.Query)
		if err != nil {
			return fmt.Errorf("evaluation %q: %w", tc.Name, err)
		}
		rank := relevantRank(hits, tc)
		if rank == 1 {
			hitsAt1++
		}
		if rank > 0 {
			hitsAtK++
			reciprocalRank += 1 / float64(rank)
		}
		status := "MISS"
		if rank > 0 {
			status = fmt.Sprintf("rank %d", rank)
		}
		fmt.Printf("%-22s %-7s expected=%s / %s\n", tc.Name, status, tc.DocSuffix, tc.Marker)
		if cfg.verbose || rank == 0 {
			printHits(tc.Query, hits)
		}
	}
	n := float64(len(evalCases))
	fmt.Printf("\nRetrieval metrics (%s): Hit@1 %.1f%% | Hit@%d %.1f%% | MRR %.3f\n", modeName(cfg), 100*float64(hitsAt1)/n, cfg.topK, 100*float64(hitsAtK)/n, reciprocalRank/n)
	if hitsAtK != len(evalCases) {
		return fmt.Errorf("retrieval quality gate failed: %d/%d expected chunks found in top %d", hitsAtK, len(evalCases), cfg.topK)
	}
	return nil
}

func relevantRank(hits []hit, tc evalCase) int {
	for i, h := range hits {
		if strings.HasSuffix(h.Chunk.DocID, tc.DocSuffix) && strings.Contains(h.Chunk.Text, tc.Marker) {
			return i + 1
		}
	}
	return 0
}

func printHits(query string, hits []hit) {
	fmt.Printf("\nQuery: %s\n", query)
	for i, h := range hits {
		preview := strings.Join(strings.Fields(h.Chunk.Text), " ")
		if utf8.RuneCountInString(preview) > 190 {
			preview = string([]rune(preview)[:190]) + "…"
		}
		fmt.Printf("  %d. %s#%d [%s] sim=%.4f vec=%s fts=%s score=%.6f\n     %s\n", i+1, h.Chunk.DocID, h.Chunk.Index, h.Chunk.Heading, h.Similarity, rankString(h.VectorRank), rankString(h.FTSRank), h.Score, preview)
	}
}

func modeName(c config) string {
	if c.hybrid {
		return "vector + BM25 rerank"
	}
	return "vector"
}

func rankString(n int) string {
	if n == 0 {
		return "-"
	}
	return fmt.Sprint(n)
}

func key(doc string, idx int) string { return fmt.Sprintf("%s\x00%d", doc, idx) }

func findChunk(chunks []chunk, doc string, idx int) chunk {
	for _, c := range chunks {
		if c.DocID == doc && c.Index == idx {
			return c
		}
	}
	return chunk{DocID: doc, Index: idx}
}

func sqlQuote(s string) string { return strings.ReplaceAll(s, "'", "''") }

// ftsQuery turns a natural-language question into an explicit OR query.
// TinySQL intentionally treats adjacent FTS terms as AND, which is useful for
// search syntax but too strict for verbose user questions.
func ftsQuery(s string) string {
	stop := map[string]bool{
		"a": true, "an": true, "and": true, "are": true, "as": true, "at": true,
		"be": true, "before": true, "can": true, "do": true, "does": true, "for": true,
		"from": true, "how": true, "i": true, "in": true, "is": true, "it": true,
		"of": true, "on": true, "or": true, "the": true, "this": true, "to": true,
		"what": true, "which": true, "with": true,
		"als": true, "auf": true, "das": true, "der": true, "die": true, "ein": true,
		"eine": true, "für": true, "fuer": true, "ich": true, "jede": true, "mit": true,
		"setze": true, "und": true, "wie": true,
	}
	fields := strings.FieldsFunc(strings.ToLower(s), func(r rune) bool {
		return (r < 'a' || r > 'z') && (r < '0' || r > '9') && r != '_' && r != '-'
	})
	seen := make(map[string]bool)
	terms := make([]string, 0, len(fields))
	for _, field := range fields {
		field = strings.Trim(field, "-")
		if len([]rune(field)) < 2 || stop[field] || seen[field] {
			continue
		}
		seen[field] = true
		terms = append(terms, field)
	}
	if len(terms) == 0 {
		return strings.TrimSpace(s)
	}
	return strings.Join(terms, " OR ")
}

func value[T any](row tsql.Row, name string) (T, bool) {
	var zero T
	v, ok := tsql.GetVal(row, name)
	if !ok {
		return zero, false
	}
	t, ok := v.(T)
	return t, ok
}

func intValue(row tsql.Row, name string) (int, bool) {
	v, ok := tsql.GetVal(row, name)
	if !ok {
		return 0, false
	}
	switch n := v.(type) {
	case int:
		return n, true
	case int64:
		return int(n), true
	case float64:
		return int(n), true
	default:
		return 0, false
	}
}

type lmClient struct {
	baseURL string
	http    *http.Client
}

func (c *lmClient) embed(ctx context.Context, model string, input []string) ([][]float64, error) {
	body := struct {
		Model string   `json:"model"`
		Input []string `json:"input"`
	}{model, input}
	var response struct {
		Data []struct {
			Embedding []float64 `json:"embedding"`
			Index     int       `json:"index"`
		} `json:"data"`
	}
	if err := c.post(ctx, "/embeddings", body, &response); err != nil {
		return nil, err
	}
	if len(response.Data) != len(input) {
		return nil, fmt.Errorf("LM Studio returned %d embeddings for %d inputs", len(response.Data), len(input))
	}
	sort.Slice(response.Data, func(i, j int) bool { return response.Data[i].Index < response.Data[j].Index })
	out := make([][]float64, len(response.Data))
	for i := range response.Data {
		out[i] = response.Data[i].Embedding
	}
	return out, nil
}

func (c *lmClient) answer(ctx context.Context, model, query string, hits []hit) (string, error) {
	var contextText strings.Builder
	for i, h := range hits {
		fmt.Fprintf(&contextText, "[Source %d: %s#%d, %s]\n%s\n\n", i+1, h.Chunk.DocID, h.Chunk.Index, h.Chunk.Heading, h.Chunk.Text)
	}
	body := map[string]any{
		"model": model, "temperature": 0.1, "max_tokens": 700,
		"messages": []map[string]string{
			{"role": "system", "content": "Answer only from the supplied sources. If the sources do not contain the answer, say so. Cite claims as [Source N]."},
			{"role": "user", "content": "Question: " + query + "\n\nSources:\n" + contextText.String()},
		},
	}
	var response struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := c.post(ctx, "/chat/completions", body, &response); err != nil {
		return "", err
	}
	if len(response.Choices) == 0 {
		return "", errors.New("LM Studio returned no chat choices")
	}
	return strings.TrimSpace(response.Choices[0].Message.Content), nil
}

func (c *lmClient) post(ctx context.Context, path string, body, out any) error {
	payload, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("LM Studio %s: %s: %s", path, resp.Status, strings.TrimSpace(string(data)))
	}
	return json.NewDecoder(resp.Body).Decode(out)
}
