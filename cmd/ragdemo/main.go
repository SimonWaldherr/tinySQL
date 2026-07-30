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

// hit is one final retrieval result. VectorRank/FTSRank are 0 when that pass
// did not return the chunk at all, which under hybrid fusion is normal: a hit
// may come from either pass alone. Similarity is only meaningful when
// VectorRank > 0. Score is the RRF score under hybrid retrieval and the cosine
// similarity otherwise, so it is comparable within a run but not across modes.
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
	dims := 0
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
			// A short-circuited or partially-loaded embedding model can answer
			// with a different width than it did for the previous batch. Cosine
			// against a mixed-width column is not a weaker ranking, it is a
			// meaningless one, so fail here instead of reporting the metrics of
			// a corpus that cannot be compared against itself.
			if len(vectors[i]) == 0 {
				return fmt.Errorf("embedding model returned an empty vector for chunk %d", start+i+1)
			}
			if dims == 0 {
				dims = len(vectors[i])
			}
			if len(vectors[i]) != dims {
				return fmt.Errorf("embedding model returned %d dimensions for chunk %d, %d for the first chunk", len(vectors[i]), start+i+1, dims)
			}
			chunks[start+i].Embedding = vectors[i]
		}
		fmt.Fprintf(os.Stderr, "\rembedding chunks: %d/%d", end, len(chunks))
	}
	fmt.Fprintf(os.Stderr, "\rembedding chunks: %d/%d in %s\n", len(chunks), len(chunks), time.Since(started).Round(time.Millisecond))

	db, err := buildDB(chunks)
	if err != nil {
		return err
	}
	corpus := make(map[string]chunk, len(chunks))
	for _, c := range chunks {
		corpus[key(c.DocID, c.Index)] = c
	}
	fmt.Printf("Corpus: %d chunks from %s | dimensions: %d | chunk=%d overlap=%d\n", len(chunks), cfg.docsDir, len(chunks[0].Embedding), cfg.chunkSize, cfg.overlap)

	if cfg.query != "" {
		hits, err := retrieve(ctx, client, db, corpus, cfg, cfg.query)
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
	return evaluate(ctx, client, db, corpus, cfg)
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
//
// Two details matter for retrieval quality on documentation corpora. Lines
// inside a fenced code block are never read as headings — technical docs are
// full of shell comments ("# build the index"), and treating those as section
// boundaries shreds the very code examples a question is usually about. And
// each chunk carries the full heading path ("VEC_SEARCH › Options"), not just
// the nearest heading, because deep headings are frequently generic words that
// carry no meaning detached from their parent.
func chunkMarkdown(s string, size, overlap int) []chunk {
	lines := strings.Split(strings.ReplaceAll(s, "\r\n", "\n"), "\n")
	var trail []string
	var section []string
	var out []chunk
	heading := "Document"
	inFence := false
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
		if strings.HasPrefix(trimmed, "```") || strings.HasPrefix(trimmed, "~~~") {
			inFence = !inFence
			section = append(section, line)
			continue
		}
		level := 0
		if !inFence {
			level = len(trimmed) - len(strings.TrimLeft(trimmed, "#"))
		}
		if level > 0 && level < len(trimmed) {
			flush()
			title := strings.TrimSpace(strings.Trim(strings.TrimLeft(trimmed, "#"), "#"))
			if title == "" {
				continue
			}
			trail = append(trail[:min(level-1, len(trail))], title)
			heading = strings.Join(trail, " › ")
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

// retrieveSQL builds the RAG_SEARCH call for one query embedding. Fusion is
// left to the engine deliberately: RAG_SEARCH runs the vector and BM25 passes
// over the same candidate_k window and reciprocal-rank-fuses their union, so a
// chunk found by keyword alone still reaches the final ranking. Reranking in Go
// cannot reproduce that — application code only ever sees the vector candidate
// set, so every lexical-only hit is gone before scoring starts.
func retrieveSQL(cfg config, queryVec []float64, query string) (string, error) {
	opts := map[string]any{
		"metric":      "cosine",
		"index":       "flat",
		"candidate_k": cfg.candidateK,
	}
	cols := "doc_id, chunk_index, _vec_similarity, _vec_rank"
	if cfg.hybrid {
		// heading is searched alongside the body: a section title is short but
		// highly discriminative, and BM25 length normalization rewards that.
		opts["text_columns"] = []string{"heading", "chunk_text"}
		opts["text_query"] = query
		opts["key_columns"] = []string{"doc_id", "chunk_index"}
		cols += ", _fts_rank, _rrf_score"
	}
	optsJSON, err := json.Marshal(opts)
	if err != nil {
		return "", fmt.Errorf("marshal RAG_SEARCH options: %w", err)
	}
	vecJSON, err := json.Marshal(queryVec)
	if err != nil {
		return "", fmt.Errorf("marshal query vector: %w", err)
	}
	return fmt.Sprintf(
		`SELECT %s FROM RAG_SEARCH('rag_chunks', 'embedding', VEC_FROM_JSON('%s'), %d, '%s')`,
		cols, vecJSON, cfg.topK, sqlQuote(string(optsJSON)),
	), nil
}

func retrieve(ctx context.Context, client *lmClient, db *tsql.DB, corpus map[string]chunk, cfg config, query string) ([]hit, error) {
	vectors, err := client.embed(ctx, cfg.embeddingModel, []string{query})
	if err != nil {
		return nil, fmt.Errorf("embed query: %w", err)
	}
	sql, err := retrieveSQL(cfg, vectors[0], query)
	if err != nil {
		return nil, err
	}
	stmt, err := tsql.ParseSQL(sql)
	if err != nil {
		return nil, fmt.Errorf("parse RAG_SEARCH query: %w", err)
	}
	rs, err := tsql.Execute(ctx, db, "default", stmt)
	if err != nil {
		return nil, fmt.Errorf("execute RAG_SEARCH query: %w", err)
	}

	// RAG_SEARCH returns rows best-first, so the result order *is* the ranking.
	hits := make([]hit, 0, len(rs.Rows))
	for _, row := range rs.Rows {
		doc, ok := value[string](row, "doc_id")
		if !ok {
			return nil, fmt.Errorf("RAG_SEARCH row is missing doc_id: %v", row)
		}
		idx, ok := intValue(row, "chunk_index")
		if !ok {
			return nil, fmt.Errorf("RAG_SEARCH row %s is missing chunk_index: %v", doc, row)
		}
		c, ok := corpus[key(doc, idx)]
		if !ok {
			return nil, fmt.Errorf("RAG_SEARCH returned unknown chunk %s#%d", doc, idx)
		}
		h := hit{Chunk: c}
		// A lexical-only hit carries no _vec_* columns and a vector-only hit no
		// _fts_*, so an absent column means "not found by that pass" — hence
		// the rank-0 sentinel rather than an error.
		h.VectorRank, _ = intValue(row, "_vec_rank")
		h.Similarity, _ = floatValue(row, "_vec_similarity")
		h.FTSRank, _ = intValue(row, "_fts_rank")
		if cfg.hybrid {
			h.Score, ok = floatValue(row, "_rrf_score")
			if !ok {
				return nil, fmt.Errorf("RAG_SEARCH hybrid row %s#%d is missing _rrf_score", doc, idx)
			}
		} else {
			h.Score = h.Similarity
		}
		hits = append(hits, h)
	}
	return hits, nil
}

func evaluate(ctx context.Context, client *lmClient, db *tsql.DB, corpus map[string]chunk, cfg config) error {
	hitsAt1, hitsAtK, reciprocalRank := 0, 0, 0.0
	for _, tc := range evalCases {
		hits, err := retrieve(ctx, client, db, corpus, cfg, tc.Query)
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
		sim := "-"
		if h.VectorRank > 0 {
			sim = fmt.Sprintf("%.4f", h.Similarity)
		}
		fmt.Printf("  %d. %s#%d [%s] sim=%s vec=%s fts=%s score=%.6f\n     %s\n", i+1, h.Chunk.DocID, h.Chunk.Index, h.Chunk.Heading, sim, rankString(h.VectorRank), rankString(h.FTSRank), h.Score, preview)
	}
}

func modeName(c config) string {
	if c.hybrid {
		return "vector + BM25, RRF-fused"
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

func sqlQuote(s string) string { return strings.ReplaceAll(s, "'", "''") }

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
	f, ok := floatValue(row, name)
	return int(f), ok
}

// floatValue reads a numeric column without assuming which numeric type the
// engine chose. A plain v.(float64) assertion would yield a silent zero for a
// float32 or integer-typed column, which for a similarity or a rank is
// indistinguishable from a legitimately-computed value — precisely the kind of
// scoring bug that shows up only as slightly-wrong rankings.
func floatValue(row tsql.Row, name string) (float64, bool) {
	v, ok := tsql.GetVal(row, name)
	if !ok {
		return 0, false
	}
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
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
