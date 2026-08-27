// Package engine provides a lightweight Full-Text Search (FTS) engine inspired
// by SQLite FTS5.
//
// Design:
//   - CREATE VIRTUAL TABLE t USING fts(col1, col2) creates a regular tinySQL
//     table with FTS indexing enabled.
//   - FTS_SEARCH builds a tokenized-document cache and an inverted index (term
//     postings) lazily per searched column set, invalidated by table.Version —
//     so writes need no index maintenance, and the next search rebuilds.
//   - FTS_MATCH(text, query) – boolean match check with phrase/boolean query support
//   - FTS_RANK(text, query)  – BM25-like relevance score
//   - FTS_SNIPPET(text, query [, before, after, ellipsis, max_tokens]) – highlighted snippet
//   - FTS_HIGHLIGHT(text, query [, before, after]) – full-text highlighting alias
//   - BM25(text, query)      – alias for FTS_RANK
//   - FTS_SEARCH table-valued function for corpus-level k-nearest search
//   - CONTAINS_ALL(text, term1, term2, ...)   – true iff every term is a substring of text
//   - CONTAINS_ANY(text, term1, term2, ...)   – true iff any term is a substring of text
//   - CONTAINS_SCORE(text, term1, term2, ...) – count of the given terms found in text
//
// Query syntax supported by FTS_MATCH / FTS_RANK:
//
//	word         – single term
//	"phrase"     – exact phrase (consecutive tokens)
//	word*        – wildcard term (* / % = any sequence, ? / _ = one character)
//	A AND B      – both terms must match
//	A OR B       – either term must match
//	NOT A        – term must not match
//	A B          – implicit AND (same as A AND B)
//
// FTS_MATCH / FTS_RANK vs. CONTAINS_ALL / CONTAINS_ANY / CONTAINS_SCORE:
// FTS_MATCH and FTS_RANK tokenize, stem, and strip stop words from both the
// text and the query, and support a rich boolean query language (phrases,
// prefix wildcards, NOT, nested AND/OR) — they're the right choice for
// natural-language search boxes. CONTAINS_ALL, CONTAINS_ANY, and
// CONTAINS_SCORE instead do a raw case-insensitive substring match against a
// fixed list of literal terms, with no tokenizing, stemming, or query syntax
// — they're the right choice for exact codes/IDs/numbers (which stemming
// would mangle), or for a simple "must contain all/any of these words"
// filter without learning FTS_MATCH's query grammar.
package engine

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
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
	// Replace punctuation with spaces, lowercasing letters in the same pass
	// instead of a second strings.ToLower over the whole result: only
	// a-z/A-Z/0-9 ever survive this loop, so ToLower could only ever affect
	// the A-Z case, which is folded in directly here.
	var sb strings.Builder
	sb.Grow(len(text))
	for _, r := range text {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			sb.WriteRune(r)
		case r >= 'A' && r <= 'Z':
			sb.WriteRune(r + ('a' - 'A'))
		default:
			sb.WriteRune(' ')
		}
	}
	raw := strings.Fields(sb.String())
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

// ─────────────────────────── BM25 Search ─────────────────────────────────────

const (
	bm25K1 = 1.2
	bm25B  = 0.75
)

// ─────────────────────────── FTS virtual table creation ──────────────────────

// executeCreateFTSTable creates the underlying physical table backing a
// CREATE VIRTUAL TABLE ... USING fts(...) declaration. FTS_SEARCH is
// index-free (see the document cache below), so nothing further needs
// registering here.
func executeCreateFTSTable(env ExecEnv, s *CreateTable) (*ResultSet, error) {
	// Build column definitions (all TEXT)
	cols := make([]storage.Column, len(s.FTSColumns))
	for i, cn := range s.FTSColumns {
		cols[i] = storage.Column{Name: cn, Type: storage.TextType}
	}

	t := storage.NewTable(s.Name, cols, false)
	return nil, env.db.Put(env.tenant, t)
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
				d.apply(valueText(v))
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
	queryStr := ftsValueToString(queryVal)
	node := parseCachedFTSQuery(queryStr)
	querySet := ftsQueryTerms(node)
	// Also add simple tokenized terms for backward compatibility.
	for _, q := range ftsTokenize(queryStr) {
		querySet[q] = true
	}

	// Wildcard prefixes ("prefix*" entries in querySet) are precomputed once
	// here rather than re-scanned from isHighlighted on every single word:
	// the query has a handful of terms but potentially many words to check,
	// so this turns an O(words × queryTerms) scan into O(words + queryTerms).
	var wildcardPrefixes []string
	for tok := range querySet {
		if strings.HasSuffix(tok, "*") {
			wildcardPrefixes = append(wildcardPrefixes, strings.TrimSuffix(tok, "*"))
		}
	}

	// isHighlighted checks if a word matches any positive query atom.
	isHighlighted := func(w string) bool {
		stemmed := ftsStem(strings.ToLower(w))
		if ftsHighlightToken(node, stemmed) {
			return true
		}
		if querySet[stemmed] {
			return true
		}
		for _, pfx := range wildcardPrefixes {
			if strings.HasPrefix(stemmed, pfx) {
				return true
			}
		}
		return false
	}

	words := strings.Fields(ftsValueToString(textVal))

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

func ftsHighlightToken(node *ftsQueryNode, token string) bool {
	if node == nil {
		return false
	}
	switch node.op {
	case "TERM":
		return node.term == token
	case "PREFIX":
		return strings.HasPrefix(token, node.prefix)
	case "WILDCARD":
		return ftsWildcardMatch(node.pattern, token)
	case "PHRASE":
		for _, term := range node.phrase {
			if term == token {
				return true
			}
		}
	case "AND", "OR":
		return ftsHighlightToken(node.left, token) || ftsHighlightToken(node.right, token)
	}
	return false
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
		"CONTAINS_ALL":   evalContainsAll,
		"CONTAINS_ANY":   evalContainsAny,
		"CONTAINS_SCORE": evalContainsScore,
	}
}

// ─────────────────────────── FTS boolean query parser ────────────────────────

// ftsQueryNode represents one node in a parsed boolean FTS query tree.
type ftsQueryNode struct {
	op      string   // "AND", "OR", "NOT", "TERM", "PHRASE", "PREFIX", "WILDCARD"
	term    string   // for TERM / PREFIX
	phrase  []string // for PHRASE (stemmed tokens)
	prefix  string   // for PREFIX (stem prefix without trailing *)
	pattern []ftsWildcardAtom
	left    *ftsQueryNode
	right   *ftsQueryNode
	operand *ftsQueryNode // for NOT

	// Precomputed BM25 inverse document frequencies, filled by ftsBindIDF for
	// the corpus-backed search path. IDF depends only on the term and the
	// corpus, so it is constant for a whole query — but it used to be looked up
	// through the ftsIDFLookup closure once per (term, document) pair, costing a
	// postings map lookup and a math.Log per call. On a 20k-document corpus that
	// is tens of thousands of redundant logarithms per query. Binding them once
	// turns the inner loop into a multiply.
	//
	// idfBound distinguishes "bound, and the weight really is 0" (a term absent
	// from the corpus) from "not bound".
	idfBound bool
	termIDF  float64   // TERM
	termIDFs []float64 // PHRASE / EXPANDED, parallel to phrase

	// Corpus term ids resolved alongside the IDF weights, so the per-document
	// hot loop compares int32s instead of hashing and comparing strings. -1
	// means the term is absent from the corpus.
	termID   int32   // TERM
	termIDNs []int32 // PHRASE / EXPANDED, parallel to phrase
}

type ftsWildcardAtom struct {
	rune rune
	kind uint8
}

const (
	ftsWildcardLiteral uint8 = iota
	ftsWildcardOne
	ftsWildcardMany
)

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

// ftsParseAtom parses a single atom: TERM, PHRASE, PREFIX, or WILDCARD.
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

	// Keep the original prefix fast path for the common "word*" spelling.
	// General wildcard terms below add * / % (zero or more characters) and
	// ? / _ (exactly one character), anywhere inside a token.
	if pfxRaw, ok := ftsSimplePrefix(tok); ok {
		pfx := strings.ToLower(pfxRaw)
		pfx = ftsStem(pfx) // stem the prefix
		return &ftsQueryNode{op: "PREFIX", prefix: pfx}, pos
	}
	if pattern, ok := ftsCompileWildcard(tok); ok {
		return &ftsQueryNode{op: "WILDCARD", pattern: pattern}, pos
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

// ftsSimplePrefix recognizes the historically-supported unescaped "word*"
// form. Keeping it separate preserves stemming of the literal prefix while
// more general wildcard patterns operate on the already-normalized index
// tokens.
func ftsSimplePrefix(s string) (string, bool) {
	runes := []rune(s)
	if len(runes) < 2 || (runes[len(runes)-1] != '*' && runes[len(runes)-1] != '%') {
		return "", false
	}
	for i, r := range runes {
		if r == '\\' || ((r == '*' || r == '?' || r == '%' || r == '_') && i != len(runes)-1) {
			return "", false
		}
	}
	return string(runes[:len(runes)-1]), true
}

// ftsCompileWildcard compiles a token wildcard without regular expressions,
// so matching stays allocation-free in the FTS_SEARCH document loop.
// Backslash quotes the next rune. SQL LIKE aliases (% and _) are accepted in
// addition to the search-box spellings (* and ?).
func ftsCompileWildcard(s string) ([]ftsWildcardAtom, bool) {
	var atoms []ftsWildcardAtom
	escaped := false
	hasWildcard := false
	for _, r := range []rune(strings.ToLower(s)) {
		if escaped {
			atoms = append(atoms, ftsWildcardAtom{rune: r, kind: ftsWildcardLiteral})
			escaped = false
			continue
		}
		if r == '\\' {
			escaped = true
			continue
		}
		switch r {
		case '*', '%':
			hasWildcard = true
			if len(atoms) == 0 || atoms[len(atoms)-1].kind != ftsWildcardMany {
				atoms = append(atoms, ftsWildcardAtom{kind: ftsWildcardMany})
			}
		case '?', '_':
			hasWildcard = true
			atoms = append(atoms, ftsWildcardAtom{kind: ftsWildcardOne})
		default:
			atoms = append(atoms, ftsWildcardAtom{rune: r, kind: ftsWildcardLiteral})
		}
	}
	if escaped {
		atoms = append(atoms, ftsWildcardAtom{rune: '\\', kind: ftsWildcardLiteral})
	}
	return atoms, hasWildcard
}

// ftsWildcardMatch matches one normalized token with the usual linear greedy
// glob algorithm. FTS index tokens are ASCII letters/digits by construction,
// so byte indexing is also character indexing and the hot document loop stays
// allocation-free.
func ftsWildcardMatch(pattern []ftsWildcardAtom, token string) bool {
	tokenPos, patternPos := 0, 0
	starPos, starTokenPos := -1, 0
	for tokenPos < len(token) {
		if patternPos < len(pattern) &&
			(pattern[patternPos].kind == ftsWildcardOne ||
				(pattern[patternPos].kind == ftsWildcardLiteral &&
					pattern[patternPos].rune == rune(token[tokenPos]))) {
			tokenPos++
			patternPos++
			continue
		}
		if patternPos < len(pattern) && pattern[patternPos].kind == ftsWildcardMany {
			starPos = patternPos
			patternPos++
			starTokenPos = tokenPos
			continue
		}
		if starPos >= 0 {
			starTokenPos++
			tokenPos = starTokenPos
			patternPos = starPos + 1
			continue
		}
		return false
	}
	for patternPos < len(pattern) && pattern[patternPos].kind == ftsWildcardMany {
		patternPos++
	}
	return patternPos == len(pattern)
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
	case "WILDCARD":
		// Highlighting uses the query tree directly for wildcard terms.
	case ftsExpandedOp:
		// A dictionary-resolved PREFIX/WILDCARD: its terms are literal, so they
		// can be highlighted directly.
		for _, t := range node.phrase {
			out[t] = true
		}
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
	case "WILDCARD":
		for tok := range freq {
			if ftsWildcardMatch(node.pattern, tok) {
				return true
			}
		}
		return false
	case ftsExpandedOp:
		// A PREFIX/WILDCARD already resolved against the corpus dictionary
		// (ftsExpandQuery): matching means the document holds one of those
		// terms. Equivalent to the two cases above — every document token is a
		// dictionary term — but it tests a short term list instead of scanning
		// the document's whole token set.
		for _, term := range node.phrase {
			if freq[term] > 0 {
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

// ftsLiteralORTerms returns the positive leaf terms of a query made solely of
// TERM nodes joined with OR. That narrow form is common in type-ahead search
// and can be evaluated without constructing the token-frequency map required
// by the full FTS query language.
func ftsLiteralORTerms(node *ftsQueryNode) ([]string, bool) {
	if node == nil {
		return nil, false
	}
	switch node.op {
	case "TERM":
		return []string{node.term}, true
	case "OR":
		left, ok := ftsLiteralORTerms(node.left)
		if !ok {
			return nil, false
		}
		right, ok := ftsLiteralORTerms(node.right)
		if !ok {
			return nil, false
		}
		return append(left, right...), true
	default:
		return nil, false
	}
}

// ftsAnyLiteralTermMatch scans the same ASCII-token language as ftsTokenize,
// but never allocates a token slice or frequency map. The scanner is used only
// for literal OR queries, where seeing one matching token decides the result.
// Non-ASCII bytes are delimiters, exactly as ftsTokenize treats non-ASCII
// runes. Uppercase tokens take the small fallback allocation needed to fold
// them; typical indexed prose is already lowercase after normalization.
func ftsAnyLiteralTermMatch(text string, terms []string) bool {
	found := false
	ftsForEachToken(text, func(token string) bool {
		for _, term := range terms {
			if token == term {
				found = true
				return false
			}
		}
		return true
	})
	return found
}

// ftsForEachToken is the allocation-free streaming sibling of ftsTokenize.
// visit returns false to stop scanning early.
func ftsForEachToken(text string, visit func(string) bool) {
	for start := 0; start < len(text); {
		for start < len(text) && !ftsASCIITokenByte(text[start]) {
			start++
		}
		end := start
		hasUpper := false
		for end < len(text) && ftsASCIITokenByte(text[end]) {
			hasUpper = hasUpper || (text[end] >= 'A' && text[end] <= 'Z')
			end++
		}
		if start == end {
			continue
		}
		token := text[start:end]
		if hasUpper {
			buf := make([]byte, len(token))
			for i := range token {
				c := token[i]
				if c >= 'A' && c <= 'Z' {
					c += 'a' - 'A'
				}
				buf[i] = c
			}
			token = string(buf)
		}
		if !ftsStopWords[token] && len(token) > 1 {
			token = ftsStem(token)
			if !visit(token) {
				return
			}
		}
		start = end
	}
}

func ftsASCIITokenByte(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9'
}

// ftsLiteralTermsRank evaluates the TERM/OR subset of FTS_RANK in one text
// pass, without a document-wide token slice or frequency map. counts is
// caller-owned scratch because the compiled ORDER BY scorer is private to one
// execution and can safely reuse it for every row.
func ftsLiteralTermsRank(text string, terms []string, counts []int) float64 {
	clear(counts)
	ftsForEachToken(text, func(token string) bool {
		for i, term := range terms {
			if token == term {
				counts[i]++
			}
		}
		return true
	})
	maxFrequency := 0
	for _, frequency := range counts {
		if frequency > maxFrequency {
			maxFrequency = frequency
		}
	}
	if maxFrequency == 0 {
		return 0
	}
	tf := float64(maxFrequency)
	// ftsScoreNode with normDocLen=1 and no IDF: the length-normalization
	// denominator reduces to tf+k1 because (1-b)+b == 1.
	return (tf * (bm25K1 + 1)) / (tf + bm25K1)
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

// ftsIDFFunc looks up a term's inverse document frequency. nil means "no
// corpus stats available" (e.g. the standalone FTS_RANK/FTS_MATCH path),
// in which case term scores are left unweighted (IDF factor of 1).
type ftsIDFFunc func(term string) float64

// ftsSumMatchingTerms sums termScore over the frequency-map entries satisfying
// matches, in ascending term order.
//
// Sorting is not cosmetic. Summing float64 values while ranging over a Go map
// accumulates them in a randomized order, and float addition is not
// associative, so the same query over the same text used to produce scores that
// differed in their final bits from one call to the next — enough to reorder
// near-tied results and to make any threshold on the score unreliable. Ordering
// the terms makes the result reproducible.
//
// The corpus-backed search path does not reach this function: ftsExpandQuery
// resolves PREFIX/WILDCARD nodes against the dictionary once per query, which
// is both deterministic and far cheaper than a per-document scan. This path
// serves the single-text scalar functions (FTS_RANK/BM25), where there is no
// corpus dictionary to expand against.
func ftsSumMatchingTerms(freq map[string]int, termScore func(string, int) float64, matches func(string) bool) float64 {
	var matched []string
	for tok := range freq {
		if matches(tok) {
			matched = append(matched, tok)
		}
	}
	if len(matched) == 0 {
		return 0
	}
	if len(matched) > 1 {
		sort.Strings(matched)
	}
	var s float64
	for _, tok := range matched {
		s += termScore(tok, freq[tok])
	}
	return s
}

// ftsScoreNode computes a BM25-style score for a parsed query tree.
// normDocLen is the document length already normalized by the corpus
// average (docLen/avgdl); callers with no corpus (evalFTSRank) pass 1.0,
// meaning "assume average length", which reduces to no length penalty at
// all — unchanged from this function's pre-normalization behavior.
func ftsScoreNode(node *ftsQueryNode, freq map[string]int, normDocLen float64, idf ftsIDFFunc) float64 {
	if node == nil {
		return 0
	}
	// lengthNorm is the BM25 denominator's document-length component, identical
	// for every term in this document — hoisted out of termScore so it is
	// computed once per document rather than once per term.
	lengthNorm := bm25K1 * (1 - bm25B + bm25B*normDocLen)
	termScore := func(term string, f int) float64 {
		tf := float64(f)
		if tf == 0 {
			return 0
		}
		s := (tf * (bm25K1 + 1)) / (tf + lengthNorm)
		if idf != nil {
			s *= idf(term)
		}
		return s
	}
	// weightedScore is termScore with the term's IDF already resolved by
	// ftsBindIDF, which is the corpus-backed path's hot loop.
	// The operation order deliberately mirrors termScore's (divide, then scale by
	// IDF) rather than the more natural-looking weight*tf/denominator: float
	// multiplication is not associative, so reordering would shift scores in
	// their final bits and silently change result ordering for near-ties.
	weightedScore := func(weight float64, f int) float64 {
		tf := float64(f)
		if tf == 0 {
			return 0
		}
		s := (tf * (bm25K1 + 1)) / (tf + lengthNorm)
		return s * weight
	}
	switch node.op {
	case "TERM":
		if node.idfBound {
			return weightedScore(node.termIDF, freq[node.term])
		}
		return termScore(node.term, freq[node.term])
	case "PREFIX":
		return ftsSumMatchingTerms(freq, termScore, func(tok string) bool {
			return strings.HasPrefix(tok, node.prefix)
		})
	case "WILDCARD":
		return ftsSumMatchingTerms(freq, termScore, func(tok string) bool {
			return ftsWildcardMatch(node.pattern, tok)
		})
	case ftsExpandedOp:
		// Terms are pre-resolved and sorted, so this sums in a deterministic
		// order over the same (term, frequency) pairs the PREFIX/WILDCARD cases
		// above would visit. Absent terms score 0 and are skipped.
		var s float64
		if node.idfBound {
			for i, term := range node.phrase {
				if f := freq[term]; f > 0 {
					s += weightedScore(node.termIDFs[i], f)
				}
			}
			return s
		}
		for _, term := range node.phrase {
			if f := freq[term]; f > 0 {
				s += termScore(term, f)
			}
		}
		return s
	case "PHRASE":
		if len(node.phrase) == 0 {
			return 0
		}
		// Score as sum of term scores (phrase match bonus)
		var s float64
		if node.idfBound {
			for i, t := range node.phrase {
				s += weightedScore(node.termIDFs[i], freq[t])
			}
		} else {
			for _, t := range node.phrase {
				s += termScore(t, freq[t])
			}
		}
		return s * phraseMatchBonus // phrase match bonus
	case "AND":
		return ftsScoreNode(node.left, freq, normDocLen, idf) + ftsScoreNode(node.right, freq, normDocLen, idf)
	case "OR":
		l := ftsScoreNode(node.left, freq, normDocLen, idf)
		r := ftsScoreNode(node.right, freq, normDocLen, idf)
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
	text := ftsValueToString(textVal)
	query := ftsValueToString(queryVal)

	tokens := ftsTokenize(text)
	freq := make(map[string]int, len(tokens))
	for _, t := range tokens {
		freq[t]++
	}

	node := parseCachedFTSQuery(query)
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
	text := ftsValueToString(textVal)
	query := ftsValueToString(queryVal)

	tokens := ftsTokenize(text)
	if len(tokens) == 0 {
		return 0.0, nil
	}

	freq := make(map[string]int, len(tokens))
	for _, t := range tokens {
		freq[t]++
	}

	node := parseCachedFTSQuery(query)
	if node == nil {
		return 0.0, nil
	}
	// Use normalized doc length of 1.0 (standalone, no corpus avgdl to
	// normalize against) and no IDF weighting (no corpus to compute it from).
	return ftsScoreNode(node, freq, 1.0, nil), nil
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
	return len(strings.Fields(ftsValueToString(v))), nil
}

// ─────────────────────────── CONTAINS_ALL / CONTAINS_ANY / CONTAINS_SCORE ────
//
// These are a deliberately simpler sibling to FTS_MATCH/FTS_RANK: plain
// case-insensitive substring search over N literal terms, with no
// tokenizing, stemming, stop-word removal, or boolean query syntax. Useful
// for users who just want "does this text contain all/any of these words"
// without learning FTS_MATCH's query grammar, and for matching exact
// substrings (IDs, codes, numbers) that stemming would otherwise mangle.

// evalContainsTerms evaluates ex.Args[0] as the text and ex.Args[1:] as the
// literal terms, returning how many terms were found as a case-insensitive
// substring of text. ok is false when there weren't enough arguments to
// evaluate, or when text itself is nil; callers treat !ok as "no text" and
// return false/false/0 without erroring, matching evalFTSMatch's existing
// NULL-input convention.
func evalContainsTerms(env ExecEnv, ex *FuncCall, row Row) (matched int, ok bool, err error) {
	if len(ex.Args) < 2 {
		return 0, false, fmt.Errorf("%s expects at least 2 arguments: (text, term1[, term2, ...])", ex.Name)
	}
	textVal, err := evalExpr(env, ex.Args[0], row)
	if err != nil {
		return 0, false, err
	}
	if textVal == nil {
		return 0, false, nil
	}
	text := strings.ToLower(ftsValueToString(textVal))
	for _, arg := range ex.Args[1:] {
		termVal, err := evalExpr(env, arg, row)
		if err != nil {
			return 0, false, err
		}
		if termVal == nil {
			continue
		}
		term := strings.ToLower(ftsValueToString(termVal))
		if strings.Contains(text, term) {
			matched++
		}
	}
	return matched, true, nil
}

// evalContainsAll returns true iff every term argument is found as a
// case-insensitive substring of text.
func evalContainsAll(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	matched, ok, err := evalContainsTerms(env, ex, row)
	if err != nil {
		return nil, err
	}
	if !ok {
		return false, nil
	}
	return matched == len(ex.Args)-1, nil
}

// evalContainsAny returns true iff at least one term argument is found as a
// case-insensitive substring of text.
func evalContainsAny(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	matched, ok, err := evalContainsTerms(env, ex, row)
	if err != nil {
		return nil, err
	}
	if !ok {
		return false, nil
	}
	return matched > 0, nil
}

// evalContainsScore returns how many of the given term arguments were found
// as a case-insensitive substring of text (0..N, N = number of term args).
// Intended for "ORDER BY CONTAINS_SCORE(...) DESC" ranking by how many
// search words matched.
func evalContainsScore(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	matched, ok, err := evalContainsTerms(env, ex, row)
	if err != nil {
		return nil, err
	}
	if !ok {
		return 0, nil
	}
	return matched, nil
}

// ─────────────────────────── FTS_SEARCH document cache ────────────────────────
//
// Unlike CREATE VIRTUAL TABLE ... USING fts, FTS_SEARCH is index-free: it
// scores directly against whatever table it's pointed at. Its scoring
// (ftsScoreNode) only needs per-document stats (term frequency + doc length),
// never corpus-wide IDF, so per-row tokenization can be cached and reused
// across repeated searches (e.g. a live search box re-querying per keystroke,
// or a dashboard) until the table is mutated. Mirrors the vecSearchColumnCache
// pattern in vector_search.go: keyed by (tenant, table, column set), and
// invalidated by table.Version.
// ftsCachedDoc describes one document as slices into the cache entry's shared
// arenas rather than as its own heap objects.
//
// It used to hold a freq map[string]int plus a tokens []string per document. That
// cost two allocations per document and made scoring the corpus a walk over
// thousands of independently allocated maps: profiling a RAG SELECT put 68% of
// its time in mapaccess1_faststr at roughly 110ns per lookup, which is far above
// the cost of hashing a short string — the lookups were missing cache on every
// probe. String keys also meant hashing and comparing bytes for a value the
// corpus already knows an integer for.
//
// Both are now runs inside per-entry arenas, so scoring successive documents
// walks memory sequentially and compares int32s:
//
//   - termStart/termCount index entry.docTermIDs and entry.docTermCounts, sorted
//     ascending by term ID so a term's frequency is a binary search.
//   - tokenStart/tokenCount index entry.docTokenIDs, in document order, which is
//     what phrase matching needs.
type ftsCachedDoc = storage.FTSDocument

type ftsDocCacheKey struct {
	tenant string
	table  string
	cols   string // sorted, comma-joined column indices
}

type ftsDocCacheEntry struct {
	table   *storage.Table
	version int
	docs    []ftsCachedDoc
	// avgDocLen and postings are corpus-wide BM25 statistics gathered for
	// free while building docs below (one pass, already touching every
	// token). avgDocLen normalizes each document's length so long documents
	// aren't penalized by their absolute token count; postings yields the
	// document frequency per term, which feeds IDF so rare terms outweigh
	// common ones.
	avgDocLen float64
	// postings maps each term to the ascending indices of the documents
	// containing it, and doubles as the corpus term dictionary. It replaces the
	// former docFreq map[string]int: a term's document frequency is exactly
	// len(postings[term]), because the build loop below visits each document
	// once and appends each of its distinct terms once.
	//
	// It serves two purposes beyond IDF. FTS_SEARCH derives a candidate row set
	// from it so documents that cannot match are never scored, and wildcards are
	// resolved against its key set once per query instead of against every token
	// of every document. See fts_index.go.
	postings map[string][]int32
	numDocs  int

	// termIDs assigns each corpus term a dense integer id. Query terms are
	// resolved through it once per query (ftsBindIDF), after which every
	// per-document lookup compares int32s instead of hashing and comparing
	// strings.
	termIDs map[string]int32
	// docTermIDs/docTermCounts hold each document's distinct terms and their
	// frequencies, as one ascending run per document (see ftsCachedDoc).
	docTermIDs    []int32
	docTermCounts []int32
	// docTokenIDs holds each document's tokens in order, for phrase matching.
	docTokenIDs []int32
}

// docFreq returns the number of documents containing term.
func (e ftsDocCacheEntry) docFreq(term string) int { return len(e.postings[term]) }

// termFrequency returns how often the term with id appears in doc, or 0.
//
// The document's terms are a short ascending run, so this is a branchy binary
// search over contiguous int32s — a few cache-resident comparisons, against the
// pointer-chasing map probe it replaces.
func (e ftsDocCacheEntry) termFrequency(doc ftsCachedDoc, id int32) int {
	lo, hi := doc.TermStart, doc.TermStart+doc.TermCount
	for lo < hi {
		mid := (lo + hi) / 2
		switch v := e.docTermIDs[mid]; {
		case v < id:
			lo = mid + 1
		case v > id:
			hi = mid
		default:
			return int(e.docTermCounts[mid])
		}
	}
	return 0
}

// docTokens returns doc's token ids in document order.
func (e ftsDocCacheEntry) docTokens(doc ftsCachedDoc) []int32 {
	return e.docTokenIDs[doc.TokenStart : doc.TokenStart+doc.TokenCount]
}

// ftsDocCacheMaxEntries bounds the tokenized-document cache the same way the
// vector column cache is bounded: each entry pins a *storage.Table, so without
// a cap a long-running process that creates/drops many FTS-backed tables (or
// searches many distinct column sets) leaks one pinned table per orphaned key.
const ftsDocCacheMaxEntries = 256

type ftsDocCacheBuildCall struct{ done chan struct{} }

var (
	ftsDocCacheMu sync.RWMutex
	ftsDocCache   = make(map[ftsDocCacheKey]ftsDocCacheEntry)
	// ftsDocCacheBuilds coalesces concurrent cold reads for the same
	// tokenized document set. Without it, a request burst can make every
	// caller tokenize the whole corpus before one cache entry wins.
	ftsDocCacheBuilds = make(map[ftsDocCacheKey]*ftsDocCacheBuildCall)
	// ftsDocCacheBuildHook lets the focused coalescing test hold a leader
	// build long enough for followers to join. It is always nil in production.
	ftsDocCacheBuildHook func()
	// ftsDocCacheWaitHook lets that test observe followers after they have
	// joined an in-flight build. It is always nil in production.
	ftsDocCacheWaitHook func()
)

// purgeFTSCachesFor drops the tokenized-document cache for one table, called
// from DROP TABLE. Purging is always safe: the cache rebuilds lazily on the
// next FTS_SEARCH.
func purgeFTSCachesFor(tenant, table string) {
	if tenant == "" {
		tenant = "default"
	}
	ftsDocCacheMu.Lock()
	for k := range ftsDocCache {
		if k.tenant == tenant && k.table == table {
			delete(ftsDocCache, k)
		}
	}
	ftsDocCacheMu.Unlock()
	purgeFTSPreparedQueryCachesFor(tenant, table)
}

func ftsColsCacheKey(cols []int) string {
	var b strings.Builder
	for i, c := range cols {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(strconv.Itoa(c))
	}
	return b.String()
}

// ftsArenaEstimateAfter is how many documents to tokenize before extrapolating
// the arena sizes from their average length. Small enough to happen early,
// large enough that one unusually short or long document does not skew it.
const ftsArenaEstimateAfter = 64

// ftsReserve returns s with capacity for at least want elements, preserving its
// contents and length. It is a no-op when the capacity already suffices.
func ftsReserve(s []int32, want int) []int32 {
	if want <= cap(s) {
		return s
	}
	grown := make([]int32, len(s), want)
	copy(grown, s)
	return grown
}

// ftsWriteValue appends v's default formatting to sb.
//
// The generic path is fmt.Fprintf(sb, "%v", v), which reflects on every value.
// Building the cache stringifies every searched column of every row, so for a
// corpus that is one reflective call per cell. These cases cover what storage
// actually holds in a text column and produce byte-identical output to %v; any
// other type still falls through to fmt.
func ftsWriteValue(sb *strings.Builder, v any) {
	switch t := v.(type) {
	case string:
		sb.WriteString(t)
	case int:
		sb.WriteString(strconv.Itoa(t))
	case int64:
		sb.WriteString(strconv.FormatInt(t, 10))
	case bool:
		if t {
			sb.WriteString("true")
		} else {
			sb.WriteString("false")
		}
	default:
		// float64 used to stay on the fmt path here, out of caution that %v's
		// shortest-'g' rules were hard to reproduce with strconv — getting it
		// wrong would silently change which tokens a numeric column
		// contributes. valueText (coerce.go) takes that fast path, and
		// TestValueTextMatchesSprintfFloatSweep pins it against fmt across
		// exponent thresholds, subnormals, non-finite values and ~30k
		// adjacent representable floats, so routing float64 through it is
		// safe. Remaining types still reach fmt inside valueText.
		sb.WriteString(valueText(v))
	}
}

// ftsValueToString formats v exactly as fmt.Sprintf("%v", v) would, for the
// column values passed to FTS_MATCH/FTS_RANK/FTS_SNIPPET/CONTAINS_*, all of
// which run per row. It is valueText — kept as a named alias because callers
// read as FTS code, and TestFTSValueToStringMatchesValueText pins the two
// (and ftsWriteValue) to the same output.
func ftsValueToString(v any) string {
	return valueText(v)
}

const ftsPersistentFormat = 1

func ftsPersistentUsable(index *storage.FTSIndex, table *storage.Table) bool {
	if index == nil || index.Format != ftsPersistentFormat ||
		index.BuiltRows < 0 || index.BuiltRows > len(table.Rows) || len(index.Docs) != index.BuiltRows ||
		len(index.DocTermIDs) != len(index.DocTermCounts) ||
		(index.NumDocs > 0 && (index.Postings == nil || index.TermIDs == nil)) {
		return false
	}
	for _, doc := range index.Docs {
		if doc.TermStart < 0 || doc.TermCount < 0 || int64(doc.TermStart)+int64(doc.TermCount) > int64(len(index.DocTermIDs)) ||
			doc.TokenStart < 0 || doc.TokenCount < 0 || int64(doc.TokenStart)+int64(doc.TokenCount) > int64(len(index.DocTokenIDs)) {
			return false
		}
	}
	for _, id := range index.TermIDs {
		if id < 0 || int(id) >= len(index.TermIDs) {
			return false
		}
	}
	return true
}

func ftsRemovePosting(rows []int32, row int32) []int32 {
	i := sort.Search(len(rows), func(i int) bool { return rows[i] >= row })
	if i >= len(rows) || rows[i] != row {
		return rows
	}
	copy(rows[i:], rows[i+1:])
	return rows[:len(rows)-1]
}

func ftsInsertPosting(rows []int32, row int32) []int32 {
	i := sort.Search(len(rows), func(i int) bool { return rows[i] >= row })
	if i < len(rows) && rows[i] == row {
		return rows
	}
	rows = append(rows, 0)
	copy(rows[i+1:], rows[i:])
	rows[i] = row
	return rows
}

// ftsRefreshUpdatedRows applies UPDATE-only changes directly to a persistent
// index. Document arenas are append-only, but obsolete runs are unlinked from
// postings and counted for later compaction. DELETE and schema changes cannot
// use this path because their physical row positions may have shifted.
func ftsRefreshUpdatedRows(table *storage.Table, cols []int, index *storage.FTSIndex, rows []int) {
	termNames := make([]string, len(index.TermIDs))
	for term, id := range index.TermIDs {
		if id >= 0 && int(id) < len(termNames) {
			termNames[id] = term
		}
	}
	var sb strings.Builder
	for _, ri := range rows {
		if ri < 0 || ri >= len(table.Rows) || ri >= len(index.Docs) {
			continue
		}
		old := index.Docs[ri]
		if old.Valid {
			for _, id := range index.DocTermIDs[old.TermStart : old.TermStart+old.TermCount] {
				if id >= 0 && int(id) < len(termNames) {
					term := termNames[id]
					index.Postings[term] = ftsRemovePosting(index.Postings[term], int32(ri))
				}
			}
			index.TotalDocLen -= old.DocLen
			index.NumDocs--
			index.StaleTerms += int(old.TermCount)
			index.StaleTokens += int(old.TokenCount)
		}
		index.Docs[ri] = storage.FTSDocument{}

		r := table.Rows[ri]
		sb.Reset()
		for _, ci := range cols {
			if ci < len(r) && r[ci] != nil {
				if sb.Len() > 0 {
					sb.WriteByte(' ')
				}
				ftsWriteValue(&sb, r[ci])
			}
		}
		if sb.Len() == 0 {
			continue
		}
		counts := make(map[int32]int32)
		tokenStart := int32(len(index.DocTokenIDs))
		ftsForEachToken(sb.String(), func(term string) bool {
			id, ok := index.TermIDs[term]
			if !ok {
				id = int32(len(index.TermIDs))
				index.TermIDs[term] = id
				termNames = append(termNames, term)
			}
			counts[id]++
			index.DocTokenIDs = append(index.DocTokenIDs, id)
			return true
		})
		if len(counts) == 0 {
			continue
		}
		ids := make([]int32, 0, len(counts))
		for id := range counts {
			ids = append(ids, id)
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		termStart := int32(len(index.DocTermIDs))
		for _, id := range ids {
			index.DocTermIDs = append(index.DocTermIDs, id)
			index.DocTermCounts = append(index.DocTermCounts, counts[id])
			term := termNames[id]
			index.Postings[term] = ftsInsertPosting(index.Postings[term], int32(ri))
		}
		tokenCount := len(index.DocTokenIDs) - int(tokenStart)
		index.Docs[ri] = storage.FTSDocument{TermStart: termStart, TermCount: int32(len(ids)), TokenStart: tokenStart, TokenCount: int32(tokenCount), DocLen: float64(tokenCount), Valid: true}
		index.TotalDocLen += float64(tokenCount)
		index.NumDocs++
	}
	if index.NumDocs > 0 {
		index.AvgDocLen = index.TotalDocLen / float64(index.NumDocs)
	} else {
		index.AvgDocLen = 0
	}
	index.Version = table.Version
	index.StructVersion = table.StructVersion()
}

func ftsPersistentNeedsCompaction(index *storage.FTSIndex) bool {
	// A tiny index is cheaper to keep dense than to rebuild repeatedly during
	// a handful of edits. Once enough obsolete arena data has accumulated, the
	// 25% threshold bounds memory for update-heavy long-lived corpora.
	return (index.StaleTokens >= 1024 && index.StaleTokens*4 > len(index.DocTokenIDs)) ||
		(index.StaleTerms >= 512 && index.StaleTerms*4 > len(index.DocTermIDs))
}

func ftsCacheFromPersistent(table *storage.Table, index *storage.FTSIndex) ftsDocCacheEntry {
	return ftsDocCacheEntry{
		table: table, version: table.Version, docs: index.Docs,
		avgDocLen: index.AvgDocLen, postings: index.Postings, numDocs: index.NumDocs,
		termIDs: index.TermIDs, docTermIDs: index.DocTermIDs,
		docTermCounts: index.DocTermCounts, docTokenIDs: index.DocTokenIDs,
	}
}

// ftsExtendPersistent tokenizes only rows not yet covered by index. Callers
// use a fresh index after structural changes and reuse the existing one after
// append-only INSERTs, turning an O(table) rebuild into O(new rows).
func ftsExtendPersistent(table *storage.Table, cols []int, index *storage.FTSIndex) {
	start := index.BuiltRows
	if index.Postings == nil {
		index.Postings = make(map[string][]int32)
	}
	if index.TermIDs == nil {
		index.TermIDs = make(map[string]int32)
	}
	if len(index.Docs) < len(table.Rows) {
		index.Docs = append(index.Docs, make([]storage.FTSDocument, len(table.Rows)-len(index.Docs))...)
	}
	termNames := make([]string, len(index.TermIDs))
	for term, id := range index.TermIDs {
		termNames[id] = term
	}
	counts := make([]int32, len(index.TermIDs))
	var touched []int32
	var sb strings.Builder
	for ri := start; ri < len(table.Rows); ri++ {
		r := table.Rows[ri]
		sb.Reset()
		for _, ci := range cols {
			if ci < len(r) && r[ci] != nil {
				if sb.Len() > 0 {
					sb.WriteByte(' ')
				}
				ftsWriteValue(&sb, r[ci])
			}
		}
		if sb.Len() == 0 {
			continue
		}
		tokenStart := int32(len(index.DocTokenIDs))
		touched = touched[:0]
		tokenCount := 0
		ftsForEachToken(sb.String(), func(term string) bool {
			id, ok := index.TermIDs[term]
			if !ok {
				id = int32(len(index.TermIDs))
				index.TermIDs[term] = id
				termNames = append(termNames, term)
				counts = append(counts, 0)
			}
			if counts[id] == 0 {
				touched = append(touched, id)
			}
			counts[id]++
			index.DocTokenIDs = append(index.DocTokenIDs, id)
			tokenCount++
			return true
		})
		if tokenCount == 0 {
			continue
		}
		sort.Slice(touched, func(i, j int) bool { return touched[i] < touched[j] })
		termStart := int32(len(index.DocTermIDs))
		for _, id := range touched {
			index.DocTermIDs = append(index.DocTermIDs, id)
			index.DocTermCounts = append(index.DocTermCounts, counts[id])
			counts[id] = 0
		}
		index.Docs[ri] = storage.FTSDocument{
			TermStart: termStart, TermCount: int32(len(touched)),
			TokenStart: tokenStart, TokenCount: int32(tokenCount),
			DocLen: float64(tokenCount), Valid: true,
		}
		index.TotalDocLen += float64(tokenCount)
		index.NumDocs++
		if index.NumDocs == ftsArenaEstimateAfter {
			perDoc := len(index.DocTokenIDs)/index.NumDocs + 1
			rows := len(table.Rows)
			index.DocTokenIDs = ftsReserve(index.DocTokenIDs, perDoc*rows*5/4)
			termsPerDoc := len(index.DocTermIDs)/index.NumDocs + 1
			index.DocTermIDs = ftsReserve(index.DocTermIDs, termsPerDoc*rows*5/4)
			index.DocTermCounts = ftsReserve(index.DocTermCounts, termsPerDoc*rows*5/4)
		}
		for _, id := range touched {
			term := termNames[id]
			index.Postings[term] = append(index.Postings[term], int32(ri))
		}
	}
	if index.NumDocs > 0 {
		index.AvgDocLen = index.TotalDocLen / float64(index.NumDocs)
	}
	index.Format = ftsPersistentFormat
	index.Version = table.Version
	index.StructVersion = table.StructVersion()
	index.BuiltRows = len(table.Rows)
}

// getFTSDocCache returns the tokenized documents (plus corpus-wide BM25
// stats) for the given column set, (re)building them if the table has
// changed since the last call.
func getFTSDocCache(tenant string, table *storage.Table, cols []int) ftsDocCacheEntry {
	key := ftsDocCacheKey{tenant: tenant, table: table.Name, cols: ftsColsCacheKey(cols)}

	var call *ftsDocCacheBuildCall
	for {
		ftsDocCacheMu.RLock()
		if e, ok := ftsDocCache[key]; ok && e.table == table && e.version == table.Version {
			ftsDocCacheMu.RUnlock()
			return e
		}
		ftsDocCacheMu.RUnlock()

		ftsDocCacheMu.Lock()
		if e, ok := ftsDocCache[key]; ok && e.table == table && e.version == table.Version {
			ftsDocCacheMu.Unlock()
			return e
		}
		if call := ftsDocCacheBuilds[key]; call != nil {
			ftsDocCacheMu.Unlock()
			if ftsDocCacheWaitHook != nil {
				ftsDocCacheWaitHook()
			}
			<-call.done
			continue
		}
		call = &ftsDocCacheBuildCall{done: make(chan struct{})}
		ftsDocCacheBuilds[key] = call
		ftsDocCacheMu.Unlock()
		break
	}

	published := false
	defer func() {
		if published {
			return
		}
		ftsDocCacheMu.Lock()
		if ftsDocCacheBuilds[key] == call {
			delete(ftsDocCacheBuilds, key)
			close(call.done)
		}
		ftsDocCacheMu.Unlock()
	}()

	if ftsDocCacheBuildHook != nil {
		ftsDocCacheBuildHook()
	}

	table.DerivedLock()
	index := table.FTSIndexes[key.cols]
	changed := false
	if !ftsPersistentUsable(index, table) {
		index = &storage.FTSIndex{}
		changed = true
	}
	if !changed && index.StructVersion != table.StructVersion() {
		rows, ok := table.UpdatedRowsSince(index.StructVersion)
		if !ok {
			index = &storage.FTSIndex{}
			changed = true
		} else {
			ftsRefreshUpdatedRows(table, cols, index, rows)
			changed = true
			if ftsPersistentNeedsCompaction(index) {
				index = &storage.FTSIndex{}
			}
		}
	}
	if changed || index.Version != table.Version || index.BuiltRows != len(table.Rows) {
		ftsExtendPersistent(table, cols, index)
		changed = true
	}
	if table.FTSIndexes == nil {
		table.FTSIndexes = make(map[string]*storage.FTSIndex)
	}
	table.FTSIndexes[key.cols] = index
	if changed {
		table.MarkFTSIndexesChanged()
	}
	table.DerivedUnlock()
	entry := ftsCacheFromPersistent(table, index)
	ftsDocCacheMu.Lock()
	if _, exists := ftsDocCache[key]; !exists {
		evictOverCap(ftsDocCache, ftsDocCacheMaxEntries)
	}
	ftsDocCache[key] = entry
	delete(ftsDocCacheBuilds, key)
	close(call.done)
	published = true
	ftsDocCacheMu.Unlock()
	return entry
}

// ftsIDFLookup returns a BM25 IDF function (Robertson-Sparck Jones with the
// standard +1 smoothing, always positive) over one doc-cache snapshot's
// document frequencies.
func ftsIDFLookup(entry ftsDocCacheEntry) ftsIDFFunc {
	if entry.numDocs == 0 {
		return nil
	}
	n := float64(entry.numDocs)
	return func(term string) float64 {
		df := entry.docFreq(term)
		if df == 0 {
			return 0
		}
		return math.Log(1 + (n-float64(df)+0.5)/(float64(df)+0.5))
	}
}

// ftsAutoOrExpandStopWords holds the natural-language stopword list used by
// ftsAutoOrExpand. It intentionally duplicates (rather than reuses) the
// smaller ftsStopWords map above: ftsStopWords is tuned for FTS_MATCH/
// FTS_RANK's own tokenizer (which already strips punctuation and applies
// stemming), while this list targets raw natural-language questions/queries
// coming from RAG_SEARCH callers and additionally covers question words
// ("what", "how", ...) and a handful of German stopwords. This is now the only
// copy: cmd/ragdemo used to carry an identical list for its own hand-rolled
// FTS pass and now relies on RAG_SEARCH's auto_or_expand instead.
var ftsAutoOrExpandStopWords = map[string]bool{
	"a": true, "an": true, "and": true, "are": true, "as": true, "at": true,
	"be": true, "before": true, "can": true, "do": true, "does": true, "for": true,
	"from": true, "how": true, "i": true, "in": true, "is": true, "it": true,
	"of": true, "on": true, "or": true, "the": true, "this": true, "to": true,
	"what": true, "which": true, "with": true,
	"als": true, "auf": true, "das": true, "der": true, "die": true, "ein": true,
	"eine": true, "für": true, "fuer": true, "ich": true, "jede": true, "mit": true,
	"setze": true, "und": true, "wie": true,
}

// ftsAutoOrExpand turns a natural-language question into an explicit
// OR-joined FTS_SEARCH query. FTS_SEARCH treats adjacent terms as an implicit
// AND (see ftsParseAnd above), which is useful for search-box syntax but too
// strict for verbose natural-language queries — a single unmatched word
// (often a stopword) would otherwise sink the whole query to zero hits.
// Tokenizing on non-alphanumeric runes, dropping stopwords/duplicates, and
// OR-joining what remains turns "what is the capital of France" into
// "capital OR france", matching if any remaining content word matches.
func ftsAutoOrExpand(query string) string {
	fields := strings.FieldsFunc(strings.ToLower(query), func(r rune) bool {
		return (r < 'a' || r > 'z') && (r < '0' || r > '9') &&
			r != '_' && r != '-' && r != '*' && r != '?' && r != '%' && r != '\\'
	})
	seen := make(map[string]bool)
	terms := make([]string, 0, len(fields))
	for _, field := range fields {
		// '-' and '?' are excluded from the FieldsFunc boundary above so a
		// deliberate mid-word wildcard survives (e.g. "wom?n", "well-known"),
		// but a leading/trailing '?' left over after splitting is virtually
		// always a natural-language question mark, not an intentional
		// single-character wildcard — the last word of "what's on the
		// menu?" would otherwise become "menu?", which ftsCompileWildcard
		// (fts.go) reads as "menu" plus exactly one more required
		// character, matching nothing (the indexed token is plain "menu")
		// and silently zeroing the keyword half of every hybrid score for
		// virtually any real question, since almost all of them end in "?".
		// '*'/'%'/'\\' are deliberately NOT trimmed here: unlike '?', they
		// essentially never occur as incidental natural-language
		// punctuation, so a trailing one is almost always an intentional
		// prefix wildcard (e.g. "program*") that auto-expand should
		// preserve verbatim into the OR-joined query.
		field = strings.Trim(field, "-?")
		if len([]rune(field)) < 2 || ftsAutoOrExpandStopWords[field] || seen[field] {
			continue
		}
		seen[field] = true
		terms = append(terms, field)
	}
	if len(terms) == 0 {
		return strings.TrimSpace(query)
	}
	return strings.Join(terms, " OR ")
}

// ─────────────────────────── FTS_SEARCH table-valued function ─────────────────

// ftsScored pairs a table row index with its computed relevance score.
type ftsScored struct {
	rowIdx int
	score  float64
}

// ftsScoredHeap is a bounded top-k selector mirroring vecScoredHeap in
// vector_search.go: rather than collecting every match and sorting the whole
// set (O(m log m) for m matches, most of which get discarded), it keeps only
// the k best candidates seen so far in a size-k heap, giving O(m log k). The
// heap root is the *worst* of the currently-kept k (lowest score, ties broken
// toward the higher rowIdx), so a new candidate only needs comparing against
// the root to decide whether it displaces the current worst entry.
type ftsScoredHeap []ftsScored

func (h ftsScoredHeap) Len() int { return len(h) }
func (h ftsScoredHeap) Less(i, j int) bool {
	if h[i].score == h[j].score {
		return h[i].rowIdx > h[j].rowIdx
	}
	return h[i].score < h[j].score
}
func (h ftsScoredHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

// ftsScoredHeapPush/Pop/Up/Down replicate container/heap's up/down algorithm
// directly on the concrete ftsScoredHeap type (see vecScoredHeapPush/Pop for
// the rationale: this avoids boxing every candidate through heap.Interface's
// `any` push/pop).
func ftsScoredHeapPush(h *ftsScoredHeap, v ftsScored) {
	*h = append(*h, v)
	ftsScoredHeapUp(*h, len(*h)-1)
}

func ftsScoredHeapPop(h *ftsScoredHeap) ftsScored {
	old := *h
	n := len(old) - 1
	old.Swap(0, n)
	ftsScoredHeapDown(old[:n], 0)
	v := old[n]
	*h = old[:n]
	return v
}

func ftsScoredHeapUp(h ftsScoredHeap, j int) {
	for {
		i := (j - 1) / 2
		if i == j || !h.Less(j, i) {
			break
		}
		h.Swap(i, j)
		j = i
	}
}

func ftsScoredHeapDown(h ftsScoredHeap, i0 int) {
	n := len(h)
	i := i0
	for {
		j1 := 2*i + 1
		if j1 >= n || j1 < 0 {
			break
		}
		j := j1
		if j2 := j1 + 1; j2 < n && h.Less(j2, j1) {
			j = j2
		}
		if !h.Less(j, i) {
			break
		}
		h.Swap(i, j)
		i = j
	}
}

// ftsScoredLess reports whether a outranks b: a strictly higher score wins,
// and on a tie the lower rowIdx wins (i.e. the earlier-seen row is kept),
// matching the order documents are scanned in cache.docs.
func ftsScoredLess(a, b ftsScored) bool {
	if a.score == b.score {
		return a.rowIdx < b.rowIdx
	}
	return a.score > b.score
}

// ftsPushTopK offers rowIdx/score into a size-k top-k heap, mirroring
// pushTopK in vector_search.go.
func ftsPushTopK(h *ftsScoredHeap, rowIdx int, score float64, k int) {
	if k <= 0 {
		return
	}
	if h.Len() < k {
		ftsScoredHeapPush(h, ftsScored{rowIdx: rowIdx, score: score})
		return
	}
	if h.Len() > 0 && ftsScoredLess(ftsScored{rowIdx: rowIdx, score: score}, (*h)[0]) {
		(*h)[0] = ftsScored{rowIdx: rowIdx, score: score}
		ftsScoredHeapDown(*h, 0)
	}
}

// ftsTopKFromHeap drains the heap into a slice ordered best-first (highest
// score first), mirroring topKFromHeap in vector_search.go: each pop removes
// the current worst remaining entry, so filling the output backwards leaves
// the best entry at index 0.
func ftsTopKFromHeap(h *ftsScoredHeap, k int) []ftsScored {
	if k > h.Len() {
		k = h.Len()
	}
	if k <= 0 {
		return nil
	}
	rows := make([]ftsScored, k)
	for i := k - 1; i >= 0; i-- {
		rows[i] = ftsScoredHeapPop(h)
	}
	return rows
}

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
		// Default: search every column — the whole row, not just TEXT/STRING
		// ones. Numeric IDs, dates, booleans, etc. are stringified the same
		// way an explicit column list would be, so e.g. an order number or
		// a status enum is findable without special-casing its type.
		for i := range table.Cols {
			searchCols = append(searchCols, i)
		}
	}
	results, err := ftsSearchCandidates(ctx, tenant, table, query, k, searchCols)
	if err != nil {
		return nil, err
	}
	return materializeFTSCandidates(table, results), nil
}

// ftsSearchCandidates performs lexical matching and BM25 ranking without
// copying source rows. RAG_SEARCH consumes this compact form directly; the
// public FTS_SEARCH table function materializes the same result afterward.
func ftsSearchCandidates(ctx context.Context, tenant string, table *storage.Table, query string, k int, searchCols []int) ([]ftsScored, error) {
	cache := getFTSDocCache(tenant, table, searchCols)
	node, candidates := prepareFTSQuery(tenant, table, searchCols, query, cache)
	if node == nil {
		// Empty or all-stopword query matches nothing. Without this guard every
		// valid document scores 0 (ftsScoreNode(nil,...) == 0) and k arbitrary
		// rows would be injected into the caller's RAG context window.
		return nil, nil
	}

	// Bounded top-k selection (O(m log k) for m candidate docs) instead of
	// collecting every match into a slice and sorting the whole thing
	// (O(m log m)) — the same fix VEC_SEARCH already applies via
	// vecScoredHeap/topKFromHeap in vector_search.go. Only the k best-scoring
	// docs are ever retained. ftsScanTopK additionally splits the scan across
	// workers once there are enough documents to be worth it.
	restricted := !candidates.unrestricted && ftsCandidateScanIsCheaper(candidates.rows, len(cache.docs))
	results, err := ftsScanTopK(ctx, cache, node, nil, candidates.rows, restricted, k)
	if err != nil {
		return nil, fmt.Errorf("FTS_SEARCH: %w", err)
	}
	return results, nil
}

func materializeFTSCandidates(table *storage.Table, results []ftsScored) *ResultSet {
	resultCols := make([]string, 0, len(table.Cols)+2)
	for _, c := range table.Cols {
		resultCols = append(resultCols, c.Name)
	}
	resultCols = append(resultCols, "_fts_score", "_fts_rank")

	rows := make([]Row, 0, len(results))
	for rank, sr := range results {
		// FTS_SEARCH feeds every hybrid RAG query. Pre-sizing the full hit row
		// avoids growing its map repeatedly for wide chunk schemas.
		r := make(Row, len(table.Cols)+2)
		for ci, c := range table.Cols {
			if ci < len(table.Rows[sr.rowIdx]) {
				r[c.Name] = table.Rows[sr.rowIdx][ci]
			}
		}
		r["_fts_score"] = sr.score
		r["_fts_rank"] = rank + 1
		rows = append(rows, r)
	}

	return &ResultSet{Cols: resultCols, Rows: rows}
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
