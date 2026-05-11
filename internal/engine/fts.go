// Package engine provides a lightweight Full-Text Search (FTS) engine inspired
// by SQLite FTS5.
//
// Design:
//   - CREATE VIRTUAL TABLE t USING fts(col1, col2) creates a regular tinySQL
//     table with FTS indexing enabled.
//   - INSERT/UPDATE/DELETE automatically maintain an in-memory inverted index.
//   - FTS_MATCH(text, query) – boolean match check with phrase/boolean query support
//   - FTS_RANK(text, query)  – BM25-like relevance score
//   - FTS_SNIPPET(text, query [, before, after, ellipsis, max_tokens]) – highlighted snippet
//   - FTS_HIGHLIGHT(text, query [, before, after]) – full-text highlighting alias
//   - BM25(text, query)      – alias for FTS_RANK
//   - FTS_SEARCH table-valued function for corpus-level k-nearest search
//
// Query syntax supported by FTS_MATCH / FTS_RANK:
//
//	word         – single term
//	"phrase"     – exact phrase (consecutive tokens)
//	word*        – prefix wildcard
//	A AND B      – both terms must match
//	A OR B       – either term must match
//	NOT A        – term must not match
//	A B          – implicit AND (same as A AND B)
package engine

import (
	"context"
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

	// Build highlight set from the parsed boolean query tree.
	node := ftsParseQuery(fmt.Sprintf("%v", queryVal))
	querySet := ftsQueryTerms(node)
	// Also add simple tokenized terms for backward compatibility.
	for _, q := range ftsTokenize(fmt.Sprintf("%v", queryVal)) {
		querySet[q] = true
	}

	// isHighlighted checks if a word matches any query term or prefix.
	isHighlighted := func(w string) bool {
		stemmed := ftsStem(strings.ToLower(w))
		if querySet[stemmed] {
			return true
		}
		// Check prefix wildcards (stored as "prefix*").
		for tok := range querySet {
			if strings.HasSuffix(tok, "*") {
				pfx := strings.TrimSuffix(tok, "*")
				if strings.HasPrefix(stemmed, pfx) {
					return true
				}
			}
		}
		return false
	}

	words := strings.Fields(fmt.Sprintf("%v", textVal))

	// Find first match index.
	matchIdx := -1
	for i, w := range words {
		if isHighlighted(w) {
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
		if isHighlighted(w) {
			sb.WriteString(opts.before)
			sb.WriteString(w)
			sb.WriteString(opts.after)
		} else {
			sb.WriteString(w)
		}
	}
	sb.WriteString(suffix)
	return sb.String(), nil
}

// evalFTSHighlight is an alias for FTS_SNIPPET with a simpler 2-argument API.
// FTS_HIGHLIGHT(text, query [, before, after]) highlights all matching tokens.
func evalFTSHighlight(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalFTSSnippet(env, ex, row)
}

// getFTSFunctions returns scalar FTS function handlers.
func getFTSFunctions() map[string]funcHandler {
	return map[string]funcHandler{
		"FTS_MATCH":      evalFTSMatch,
		"FTS_RANK":       evalFTSRank,
		"FTS_SNIPPET":    evalFTSSnippet,
		"FTS_HIGHLIGHT":  evalFTSHighlight,
		"BM25":           evalFTSRank, // alias
		"MATCH":          evalFTSMatch,
		"FTS_WORD_COUNT": evalFTSWordCount,
	}
}

// ─────────────────────────── FTS boolean query parser ────────────────────────

// ftsQueryNode represents one node in a parsed boolean FTS query tree.
type ftsQueryNode struct {
	op      string   // "AND", "OR", "NOT", "TERM", "PHRASE", "PREFIX"
	term    string   // for TERM / PREFIX
	phrase  []string // for PHRASE (stemmed tokens)
	prefix  string   // for PREFIX (stem prefix without trailing *)
	left    *ftsQueryNode
	right   *ftsQueryNode
	operand *ftsQueryNode // for NOT
}

// ftsParseQuery converts a user query string into a boolean query tree.
// Supported syntax:
//
//	word         single term
//	"phrase"     exact phrase
//	word*        prefix wildcard
//	A AND B      conjunction
//	A OR B       disjunction
//	NOT A        negation
//	A B          implicit AND
func ftsParseQuery(query string) *ftsQueryNode {
	tokens := ftsLexQuery(query)
	if len(tokens) == 0 {
		return nil
	}
	node, _ := ftsParseOr(tokens, 0)
	return node
}

// ftsLexQuery tokenises the query string into atoms, preserving operators and phrases.
func ftsLexQuery(query string) []string {
	var tokens []string
	i := 0
	runes := []rune(query)
	for i < len(runes) {
		// Skip whitespace.
		if runes[i] == ' ' || runes[i] == '\t' {
			i++
			continue
		}
		// Quoted phrase.
		if runes[i] == '"' {
			j := i + 1
			for j < len(runes) && runes[j] != '"' {
				j++
			}
			tokens = append(tokens, string(runes[i:j+1]))
			if j < len(runes) {
				j++
			}
			i = j
			continue
		}
		// Word or operator (possibly ending with *).
		j := i
		for j < len(runes) && runes[j] != ' ' && runes[j] != '\t' && runes[j] != '"' {
			j++
		}
		tok := string(runes[i:j])
		if tok != "" {
			tokens = append(tokens, tok)
		}
		i = j
	}
	return tokens
}

// ftsParseOr parses OR-level expressions.
func ftsParseOr(tokens []string, pos int) (*ftsQueryNode, int) {
	left, pos := ftsParseAnd(tokens, pos)
	for pos < len(tokens) && strings.ToUpper(tokens[pos]) == "OR" {
		pos++
		right, newPos := ftsParseAnd(tokens, pos)
		pos = newPos
		left = &ftsQueryNode{op: "OR", left: left, right: right}
	}
	return left, pos
}

// ftsParseAnd parses AND-level expressions (explicit AND or implicit AND).
func ftsParseAnd(tokens []string, pos int) (*ftsQueryNode, int) {
	left, pos := ftsParseUnary(tokens, pos)
	for pos < len(tokens) {
		tok := tokens[pos]
		upper := strings.ToUpper(tok)
		if upper == "OR" {
			break
		}
		if upper == "AND" {
			pos++
		}
		// Anything else is implicit AND; don't advance pos.
		right, newPos := ftsParseUnary(tokens, pos)
		if newPos == pos {
			break // no progress, avoid infinite loop
		}
		pos = newPos
		left = &ftsQueryNode{op: "AND", left: left, right: right}
	}
	return left, pos
}

// ftsParseUnary parses NOT and atoms.
func ftsParseUnary(tokens []string, pos int) (*ftsQueryNode, int) {
	if pos >= len(tokens) {
		return nil, pos
	}
	if strings.ToUpper(tokens[pos]) == "NOT" {
		pos++
		operand, newPos := ftsParseAtom(tokens, pos)
		return &ftsQueryNode{op: "NOT", operand: operand}, newPos
	}
	return ftsParseAtom(tokens, pos)
}

// ftsParseAtom parses a single atom: TERM, PHRASE, or PREFIX.
func ftsParseAtom(tokens []string, pos int) (*ftsQueryNode, int) {
	if pos >= len(tokens) {
		return nil, pos
	}
	tok := tokens[pos]
	// Skip bare operators that ended up here.
	upper := strings.ToUpper(tok)
	if upper == "AND" || upper == "OR" || upper == "NOT" {
		return nil, pos
	}

	pos++

	// Quoted phrase.
	if len(tok) >= 2 && tok[0] == '"' {
		inner := tok[1:]
		if len(inner) > 0 && inner[len(inner)-1] == '"' {
			inner = inner[:len(inner)-1]
		}
		stemmed := ftsTokenize(inner)
		return &ftsQueryNode{op: "PHRASE", phrase: stemmed}, pos
	}

	// Prefix wildcard.
	if strings.HasSuffix(tok, "*") {
		pfx := strings.ToLower(strings.TrimSuffix(tok, "*"))
		pfx = ftsStem(pfx) // stem the prefix
		return &ftsQueryNode{op: "PREFIX", prefix: pfx}, pos
	}

	// Plain term.
	stemmed := ftsTokenize(tok)
	term := tok
	if len(stemmed) == 1 {
		term = stemmed[0]
	} else if len(stemmed) == 0 {
		// Stop word or empty – still create node but with empty term.
		term = strings.ToLower(tok)
	}
	return &ftsQueryNode{op: "TERM", term: term}, pos
}

// ─────────────────────────── Match / Score using query tree ──────────────────

// ftsQueryTerms collects all positive terms from a query tree for snippet highlighting.
func ftsQueryTerms(node *ftsQueryNode) map[string]bool {
	if node == nil {
		return nil
	}
	out := make(map[string]bool)
	ftsCollectTerms(node, out)
	return out
}

func ftsCollectTerms(node *ftsQueryNode, out map[string]bool) {
	if node == nil {
		return
	}
	switch node.op {
	case "TERM":
		out[node.term] = true
	case "PHRASE":
		for _, t := range node.phrase {
			out[t] = true
		}
	case "PREFIX":
		out[node.prefix+"*"] = true
	case "NOT":
		// Don't highlight negated terms.
	case "AND", "OR":
		ftsCollectTerms(node.left, out)
		ftsCollectTerms(node.right, out)
	}
}

// ftsMatchNode evaluates a query tree node against a token frequency map and token list.
func ftsMatchNode(node *ftsQueryNode, freq map[string]int, tokens []string) bool {
	if node == nil {
		return false
	}
	switch node.op {
	case "TERM":
		return freq[node.term] > 0
	case "PREFIX":
		for tok := range freq {
			if strings.HasPrefix(tok, node.prefix) {
				return true
			}
		}
		return false
	case "PHRASE":
		if len(node.phrase) == 0 {
			return true
		}
		return ftsPhraseMatch(node.phrase, tokens)
	case "AND":
		return ftsMatchNode(node.left, freq, tokens) && ftsMatchNode(node.right, freq, tokens)
	case "OR":
		return ftsMatchNode(node.left, freq, tokens) || ftsMatchNode(node.right, freq, tokens)
	case "NOT":
		return !ftsMatchNode(node.operand, freq, tokens)
	}
	return false
}

// ftsPhraseMatch checks whether tokens contains phrase as a consecutive subsequence.
func ftsPhraseMatch(phrase, tokens []string) bool {
	if len(phrase) > len(tokens) {
		return false
	}
	for i := 0; i <= len(tokens)-len(phrase); i++ {
		match := true
		for j, p := range phrase {
			if tokens[i+j] != p {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}

const phraseMatchBonus = 1.5

func ftsScoreNode(node *ftsQueryNode, freq map[string]int, docLen float64) float64 {
	if node == nil {
		return 0
	}
	switch node.op {
	case "TERM":
		tf := float64(freq[node.term])
		if tf == 0 {
			return 0
		}
		return (tf * (bm25K1 + 1)) / (tf + bm25K1*(1-bm25B+bm25B*docLen))
	case "PREFIX":
		var s float64
		for tok, f := range freq {
			if strings.HasPrefix(tok, node.prefix) {
				tf := float64(f)
				s += (tf * (bm25K1 + 1)) / (tf + bm25K1*(1-bm25B+bm25B*docLen))
			}
		}
		return s
	case "PHRASE":
		if len(node.phrase) == 0 {
			return 0
		}
		// Score as sum of term scores (phrase match bonus)
		var s float64
		for _, t := range node.phrase {
			tf := float64(freq[t])
			if tf > 0 {
				s += (tf * (bm25K1 + 1)) / (tf + bm25K1*(1-bm25B+bm25B*docLen))
			}
		}
		return s * phraseMatchBonus // phrase match bonus
	case "AND":
		return ftsScoreNode(node.left, freq, docLen) + ftsScoreNode(node.right, freq, docLen)
	case "OR":
		l := ftsScoreNode(node.left, freq, docLen)
		r := ftsScoreNode(node.right, freq, docLen)
		if l > r {
			return l
		}
		return r
	case "NOT":
		return 0
	}
	return 0
}

// ─────────────────────────── Upgraded FTS_MATCH / FTS_RANK ───────────────────

// evalFTSMatch returns true if text matches the boolean query.
// Supports: terms, "phrases", prefix*, AND, OR, NOT.
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

	tokens := ftsTokenize(text)
	freq := make(map[string]int, len(tokens))
	for _, t := range tokens {
		freq[t]++
	}

	node := ftsParseQuery(query)
	if node == nil {
		return false, nil
	}
	return ftsMatchNode(node, freq, tokens), nil
}

// evalFTSRank computes a BM25-like score for text against a boolean query.
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

	tokens := ftsTokenize(text)
	if len(tokens) == 0 {
		return 0.0, nil
	}

	freq := make(map[string]int, len(tokens))
	for _, t := range tokens {
		freq[t]++
	}

	node := ftsParseQuery(query)
	if node == nil {
		return 0.0, nil
	}
	// Use doc length of 1.0 (standalone, no corpus avgdl).
	return ftsScoreNode(node, freq, 1.0), nil
}

// evalFTSWordCount returns the number of words in a text.
func evalFTSWordCount(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if len(ex.Args) != 1 {
		return nil, fmt.Errorf("FTS_WORD_COUNT expects 1 argument: (text)")
	}
	v, err := evalExpr(env, ex.Args[0], row)
	if err != nil {
		return nil, err
	}
	if v == nil {
		return 0, nil
	}
	return len(strings.Fields(fmt.Sprintf("%v", v))), nil
}

// ─────────────────────────── FTS_SEARCH table-valued function ─────────────────

// FTSSearchTableFunc implements FTS_SEARCH(table, query, k [, columns...]).
// Usage:
//
//	SELECT * FROM FTS_SEARCH('table_name', 'query text', 10)
//
// Returns all columns from the source table plus:
//
//	_fts_score – BM25 relevance score
//	_fts_rank  – 1-based rank (1 = most relevant)
type FTSSearchTableFunc struct{}

func (f *FTSSearchTableFunc) Name() string { return "FTS_SEARCH" }

func (f *FTSSearchTableFunc) ValidateArgs(args []Expr) error {
	if len(args) < 3 {
		return fmt.Errorf("FTS_SEARCH requires at least 3 arguments: (table, query, k)")
	}
	return nil
}

func (f *FTSSearchTableFunc) Execute(ctx context.Context, args []Expr, env ExecEnv, row Row) (*ResultSet, error) {
	if err := f.ValidateArgs(args); err != nil {
		return nil, err
	}

	tableVal, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, fmt.Errorf("FTS_SEARCH table: %w", err)
	}
	tableName, ok := tableVal.(string)
	if !ok {
		return nil, fmt.Errorf("FTS_SEARCH: table name must be a string")
	}

	queryVal, err := evalExpr(env, args[1], row)
	if err != nil {
		return nil, fmt.Errorf("FTS_SEARCH query: %w", err)
	}
	query, ok := queryVal.(string)
	if !ok {
		return nil, fmt.Errorf("FTS_SEARCH: query must be a string")
	}

	kVal, err := evalExpr(env, args[2], row)
	if err != nil {
		return nil, fmt.Errorf("FTS_SEARCH k: %w", err)
	}
	k, err := toInt(kVal)
	if err != nil {
		return nil, fmt.Errorf("FTS_SEARCH k: %w", err)
	}
	if k <= 0 {
		return nil, fmt.Errorf("FTS_SEARCH: k must be > 0")
	}

	tenant := env.tenant
	if tenant == "" {
		tenant = "default"
	}
	table, err := env.db.Get(tenant, tableName)
	if err != nil {
		return nil, fmt.Errorf("FTS_SEARCH: table %q not found: %w", tableName, err)
	}

	// Determine which columns to search: optional 4th+ args list column names.
	var searchCols []int
	if len(args) > 3 {
		for _, colArg := range args[3:] {
			cv, err := evalExpr(env, colArg, row)
			if err != nil {
				continue
			}
			cn, ok := cv.(string)
			if !ok {
				continue
			}
			idx, err := table.ColIndex(cn)
			if err == nil {
				searchCols = append(searchCols, idx)
			}
		}
	}
	if len(searchCols) == 0 {
		// Default: search all TEXT columns.
		for i, c := range table.Cols {
			if c.Type == storage.TextType || c.Type == storage.StringType {
				searchCols = append(searchCols, i)
			}
		}
		if len(searchCols) == 0 {
			// Fall back to all columns.
			for i := range table.Cols {
				searchCols = append(searchCols, i)
			}
		}
	}

	node := ftsParseQuery(query)

	type scored struct {
		rowIdx int
		score  float64
	}
	var results []scored

	for ri, r := range table.Rows {
		// Aggregate text from searched columns.
		var sb strings.Builder
		for _, ci := range searchCols {
			if ci < len(r) && r[ci] != nil {
				if sb.Len() > 0 {
					sb.WriteByte(' ')
				}
				sb.WriteString(fmt.Sprintf("%v", r[ci]))
			}
		}
		text := sb.String()
		if text == "" {
			continue
		}

		tokens := ftsTokenize(text)
		if len(tokens) == 0 {
			continue
		}
		freq := make(map[string]int, len(tokens))
		for _, t := range tokens {
			freq[t]++
		}

		if node != nil && !ftsMatchNode(node, freq, tokens) {
			continue
		}
		docLen := float64(len(tokens))
		score := ftsScoreNode(node, freq, docLen)
		results = append(results, scored{rowIdx: ri, score: score})
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].score > results[j].score
	})
	if k < len(results) {
		results = results[:k]
	}

	resultCols := make([]string, 0, len(table.Cols)+2)
	for _, c := range table.Cols {
		resultCols = append(resultCols, c.Name)
	}
	resultCols = append(resultCols, "_fts_score", "_fts_rank")

	rows := make([]Row, 0, len(results))
	for rank, sr := range results {
		r := make(Row)
		for ci, c := range table.Cols {
			if ci < len(table.Rows[sr.rowIdx]) {
				r[c.Name] = table.Rows[sr.rowIdx][ci]
			}
		}
		r["_fts_score"] = sr.score
		r["_fts_rank"] = rank + 1
		rows = append(rows, r)
	}

	return &ResultSet{Cols: resultCols, Rows: rows}, nil
}

func init() {
	RegisterTableFunc(&FTSSearchTableFunc{})
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
