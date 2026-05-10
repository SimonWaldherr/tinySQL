// Package engine provides a lightweight Full-Text Search (FTS) engine inspired
// by SQLite FTS5.
//
// Design:
//   - CREATE VIRTUAL TABLE t USING fts(col1, col2) creates a regular tinySQL
//     table with FTS indexing enabled.
//   - INSERT/UPDATE/DELETE automatically maintain an in-memory inverted index.
//   - FTS_MATCH(text, query) – boolean match check
//   - FTS_RANK(text, query)  – BM25-like relevance score
//   - FTS_SNIPPET(text, query [, before, after, ellipsis, max_tokens]) – highlighted snippet
//   - BM25(text, query)      – alias for FTS_RANK
package engine

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// ─────────────────────────── FTS Index types ─────────────────────────────────

// ftsPosting holds the per-document occurrence data for a term.
type ftsPosting struct {
	RowID  int
	Freq   int // term frequency in document
	DocLen int // total tokens in document
}

// ftsIndex holds the inverted index and metadata for one virtual FTS table.
type ftsIndex struct {
	Columns   []string                // indexed columns
	InvIndex  map[string][]ftsPosting // term → postings list
	DocLens   map[int]int             // rowID → document length
	AvgDocLen float64
}

// ftsSearchResult holds the result of an FTS search.
type ftsSearchResult struct {
	RowID int
	Score float64
}

// Global FTS registry (tenant/table → index).
var (
	ftsRegistry   = make(map[string]*ftsIndex)
	ftsRegistryMu sync.RWMutex
)

// ─────────────────────────── Stop words ──────────────────────────────────────

var ftsStopWords = map[string]bool{
	"a": true, "an": true, "the": true, "and": true, "or": true,
	"but": true, "in": true, "on": true, "at": true, "to": true,
	"for": true, "of": true, "with": true, "by": true, "is": true,
	"was": true, "are": true, "be": true, "it": true, "as": true,
}

// ─────────────────────────── Tokenizer ───────────────────────────────────────

// ftsTokenize splits text into lowercase tokens, removing stop words.
func ftsTokenize(text string) []string {
	// Replace punctuation with spaces.
	var sb strings.Builder
	for _, r := range text {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
			sb.WriteRune(r)
		} else {
			sb.WriteRune(' ')
		}
	}
	raw := strings.Fields(strings.ToLower(sb.String()))
	out := raw[:0]
	for _, w := range raw {
		if !ftsStopWords[w] && len(w) > 1 {
			out = append(out, ftsStem(w))
		}
	}
	return out
}

// ftsStem applies simple suffix-stripping stemming.
func ftsStem(w string) string {
	for _, suffix := range []string{"ing", "tion", "ed", "ly", "er", "est", "s"} {
		if len(w) > len(suffix)+3 && strings.HasSuffix(w, suffix) {
			return w[:len(w)-len(suffix)]
		}
	}
	return w
}

// ─────────────────────────── Index maintenance ───────────────────────────────

// ftsGetOrCreate returns the ftsIndex for the given key, creating it if needed.
func ftsGetOrCreate(key string, cols []string) *ftsIndex {
	ftsRegistryMu.Lock()
	defer ftsRegistryMu.Unlock()
	idx, ok := ftsRegistry[key]
	if !ok {
		idx = &ftsIndex{
			Columns:  cols,
			InvIndex: make(map[string][]ftsPosting),
			DocLens:  make(map[int]int),
		}
		ftsRegistry[key] = idx
	}
	return idx
}

// ftsIndexRow adds or updates a row's tokens in the inverted index.
// cols is the list of FTS-indexed column names (nil means "use all").
func ftsIndexRow(key, table string, rowID int, cols []string, row []any, colNames []string) {
	ftsRegistryMu.RLock()
	idx, ok := ftsRegistry[key]
	ftsRegistryMu.RUnlock()
	if !ok {
		return // not an FTS table
	}

	// Remove old postings for this rowID.
	ftsRegistryMu.Lock()
	defer ftsRegistryMu.Unlock()

	for term, posts := range idx.InvIndex {
		filtered := posts[:0]
		for _, p := range posts {
			if p.RowID != rowID {
				filtered = append(filtered, p)
			}
		}
		if len(filtered) == 0 {
			delete(idx.InvIndex, term)
		} else {
			idx.InvIndex[term] = filtered
		}
	}
	delete(idx.DocLens, rowID)

	// Collect text from indexed columns.
	var allTokens []string
	activeCols := cols
	if len(activeCols) == 0 {
		activeCols = idx.Columns
	}
	for _, cn := range activeCols {
		for i, name := range colNames {
			if strings.EqualFold(name, cn) && i < len(row) {
				if row[i] != nil {
					allTokens = append(allTokens, ftsTokenize(fmt.Sprintf("%v", row[i]))...)
				}
			}
		}
	}

	if len(allTokens) == 0 {
		return
	}

	// Count term frequencies.
	freq := make(map[string]int)
	for _, t := range allTokens {
		freq[t]++
	}

	docLen := len(allTokens)
	idx.DocLens[rowID] = docLen

	for term, f := range freq {
		idx.InvIndex[term] = append(idx.InvIndex[term], ftsPosting{
			RowID:  rowID,
			Freq:   f,
			DocLen: docLen,
		})
	}

	// Recompute average document length.
	total := 0
	for _, dl := range idx.DocLens {
		total += dl
	}
	if len(idx.DocLens) > 0 {
		idx.AvgDocLen = float64(total) / float64(len(idx.DocLens))
	}
}

// ftsDeleteRow removes all postings for rowID from the index.
func ftsDeleteRow(key string, rowID int) {
	ftsRegistryMu.Lock()
	defer ftsRegistryMu.Unlock()
	idx, ok := ftsRegistry[key]
	if !ok {
		return
	}
	for term, posts := range idx.InvIndex {
		filtered := posts[:0]
		for _, p := range posts {
			if p.RowID != rowID {
				filtered = append(filtered, p)
			}
		}
		if len(filtered) == 0 {
			delete(idx.InvIndex, term)
		} else {
			idx.InvIndex[term] = filtered
		}
	}
	delete(idx.DocLens, rowID)
}

// ─────────────────────────── BM25 Search ─────────────────────────────────────

const (
	bm25K1 = 1.2
	bm25B  = 0.75
)

// ftsSearch performs a BM25-ranked search and returns the top-k results.
// k <= 0 means return all results.
func ftsSearch(key string, query string, k int) []ftsSearchResult {
	ftsRegistryMu.RLock()
	idx, ok := ftsRegistry[key]
	ftsRegistryMu.RUnlock()
	if !ok {
		return nil
	}

	queryTerms := ftsTokenize(query)
	if len(queryTerms) == 0 {
		return nil
	}

	ftsRegistryMu.RLock()
	defer ftsRegistryMu.RUnlock()

	N := float64(len(idx.DocLens))
	if N == 0 {
		return nil
	}

	scores := make(map[int]float64)
	for _, term := range queryTerms {
		posts, ok := idx.InvIndex[term]
		if !ok {
			continue
		}
		df := float64(len(posts))
		idf := math.Log((N-df+0.5)/(df+0.5) + 1)
		for _, p := range posts {
			tf := float64(p.Freq)
			dl := float64(p.DocLen)
			avgdl := idx.AvgDocLen
			if avgdl == 0 {
				avgdl = 1
			}
			tfNorm := (tf * (bm25K1 + 1)) / (tf + bm25K1*(1-bm25B+bm25B*dl/avgdl))
			scores[p.RowID] += idf * tfNorm
		}
	}

	results := make([]ftsSearchResult, 0, len(scores))
	for rowID, score := range scores {
		results = append(results, ftsSearchResult{RowID: rowID, Score: score})
	}
	sort.Slice(results, func(i, j int) bool { return results[i].Score > results[j].Score })

	if k > 0 && len(results) > k {
		results = results[:k]
	}
	return results
}

// ─────────────────────────── FTS virtual table creation ──────────────────────

// executeCreateFTSTable creates the underlying physical table and registers an FTS index.
func executeCreateFTSTable(env ExecEnv, s *CreateTable) (*ResultSet, error) {
	// Build column definitions (all TEXT)
	cols := make([]storage.Column, len(s.FTSColumns))
	for i, cn := range s.FTSColumns {
		cols[i] = storage.Column{Name: cn, Type: storage.TextType}
	}

	t := storage.NewTable(s.Name, cols, false)
	if err := env.db.Put(env.tenant, t); err != nil {
		return nil, err
	}

	// Register the FTS index.
	key := env.tenant + "/" + s.Name
	ftsGetOrCreate(key, s.FTSColumns)

	return nil, nil
}

// ─────────────────────────── Scalar FTS functions ────────────────────────────

// evalFTSMatch returns true if text contains at least one query term.
func evalFTSMatch(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if len(ex.Args) != 2 {
		return nil, fmt.Errorf("FTS_MATCH expects 2 arguments: (text, query)")
	}
	textVal, err := evalExpr(env, ex.Args[0], row)
	if err != nil {
		return nil, err
	}
	queryVal, err := evalExpr(env, ex.Args[1], row)
	if err != nil {
		return nil, err
	}
	if textVal == nil || queryVal == nil {
		return false, nil
	}
	text := fmt.Sprintf("%v", textVal)
	query := fmt.Sprintf("%v", queryVal)
	textTokens := ftsTokenize(text)
	queryTerms := ftsTokenize(query)
	tokenSet := make(map[string]bool, len(textTokens))
	for _, t := range textTokens {
		tokenSet[t] = true
	}
	for _, q := range queryTerms {
		if tokenSet[q] {
			return true, nil
		}
	}
	return false, nil
}

// evalFTSRank computes a simple BM25-like score for text against query.
func evalFTSRank(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if len(ex.Args) < 2 {
		return nil, fmt.Errorf("FTS_RANK expects 2 arguments: (text, query)")
	}
	textVal, err := evalExpr(env, ex.Args[0], row)
	if err != nil {
		return nil, err
	}
	queryVal, err := evalExpr(env, ex.Args[1], row)
	if err != nil {
		return nil, err
	}
	if textVal == nil || queryVal == nil {
		return 0.0, nil
	}
	text := fmt.Sprintf("%v", textVal)
	query := fmt.Sprintf("%v", queryVal)

	textTokens := ftsTokenize(text)
	queryTerms := ftsTokenize(query)
	if len(textTokens) == 0 || len(queryTerms) == 0 {
		return 0.0, nil
	}

	freq := make(map[string]int)
	for _, t := range textTokens {
		freq[t]++
	}

	score := 0.0
	// Standalone BM25 has no corpus avgdl; length normalisation uses 1.0
	// (treating this document as average-length, i.e. b effectively = 0).
	for _, q := range queryTerms {
		tf := float64(freq[q])
		if tf == 0 {
			continue
		}
		tfNorm := (tf * (bm25K1 + 1)) / (tf + bm25K1*(1-bm25B+bm25B*1.0))
		score += tfNorm
	}
	return score, nil
}

// ftsSnippetOpts holds the display options for FTS_SNIPPET.
type ftsSnippetOpts struct {
	before    string
	after     string
	ellipsis  string
	maxTokens int
}

// parseFTSSnippetOpts reads the optional 3rd-6th arguments of FTS_SNIPPET.
func parseFTSSnippetOpts(env ExecEnv, ex *FuncCall, row Row) (ftsSnippetOpts, error) {
	opts := ftsSnippetOpts{before: "<b>", after: "</b>", ellipsis: "...", maxTokens: 20}
	optDefs := []struct {
		idx   int
		apply func(string)
	}{
		{2, func(v string) { opts.before = v }},
		{3, func(v string) { opts.after = v }},
		{4, func(v string) { opts.ellipsis = v }},
	}
	for _, d := range optDefs {
		if len(ex.Args) > d.idx {
			v, err := evalExpr(env, ex.Args[d.idx], row)
			if err == nil && v != nil {
				d.apply(fmt.Sprintf("%v", v))
			}
		}
	}
	if len(ex.Args) >= 6 {
		v, err := evalExpr(env, ex.Args[5], row)
		if err == nil && v != nil {
			if n, _ := toInt(v); n > 0 {
				opts.maxTokens = n
			}
		}
	}
	return opts, nil
}

// buildFTSSnippet builds a highlighted snippet from the given words and query set.
func buildFTSSnippet(words []string, querySet map[string]bool, opts ftsSnippetOpts) string {
	matchIdx := -1
	for i, w := range words {
		if querySet[ftsStem(strings.ToLower(w))] {
			matchIdx = i
			break
		}
	}

	start, end := 0, len(words)
	prefix, suffix := "", ""

	if matchIdx >= 0 {
		start = matchIdx - opts.maxTokens/2
		if start < 0 {
			start = 0
		} else {
			prefix = opts.ellipsis
		}
		end = start + opts.maxTokens
		if end > len(words) {
			end = len(words)
		} else {
			suffix = opts.ellipsis
		}
	} else if end > opts.maxTokens {
		end = opts.maxTokens
		suffix = opts.ellipsis
	}

	var sb strings.Builder
	sb.WriteString(prefix)
	for i, w := range words[start:end] {
		if i > 0 {
			sb.WriteString(" ")
		}
		if querySet[ftsStem(strings.ToLower(w))] {
			sb.WriteString(opts.before)
			sb.WriteString(w)
			sb.WriteString(opts.after)
		} else {
			sb.WriteString(w)
		}
	}
	sb.WriteString(suffix)
	return sb.String()
}

// evalFTSSnippet returns a highlighted snippet of text around matching terms.
func evalFTSSnippet(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if len(ex.Args) < 2 {
		return nil, fmt.Errorf("FTS_SNIPPET expects at least 2 arguments: (text, query[, before, after, ellipsis, max_tokens])")
	}
	textVal, err := evalExpr(env, ex.Args[0], row)
	if err != nil {
		return nil, err
	}
	queryVal, err := evalExpr(env, ex.Args[1], row)
	if err != nil {
		return nil, err
	}
	if textVal == nil || queryVal == nil {
		return nil, nil
	}

	opts, err := parseFTSSnippetOpts(env, ex, row)
	if err != nil {
		return nil, err
	}

	queryTerms := ftsTokenize(fmt.Sprintf("%v", queryVal))
	querySet := make(map[string]bool, len(queryTerms))
	for _, q := range queryTerms {
		querySet[q] = true
	}

	words := strings.Fields(fmt.Sprintf("%v", textVal))
	return buildFTSSnippet(words, querySet, opts), nil
}

// getFTSFunctions returns scalar FTS function handlers.
func getFTSFunctions() map[string]funcHandler {
	return map[string]funcHandler{
		"FTS_MATCH":   evalFTSMatch,
		"FTS_RANK":    evalFTSRank,
		"FTS_SNIPPET": evalFTSSnippet,
		"BM25":        evalFTSRank, // alias
		"MATCH":       evalFTSMatch,
	}
}

// ─────────────────────────── Helper ──────────────────────────────────────────

// colNames extracts column names from a []storage.Column.
func colNames(cols []storage.Column) []string {
	names := make([]string, len(cols))
	for i, c := range cols {
		names[i] = c.Name
	}
	return names
}
