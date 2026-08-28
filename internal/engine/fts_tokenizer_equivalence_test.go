package engine

import (
	"fmt"
	"math"
	"strings"
	"testing"
)

// The tests here pin three FTS hot-path rewrites against reference
// implementations of the code they replaced. Each rewrite is a pure
// optimization and must not change a single token, stem, verdict, or score:
//
//   - ftsStem switched from a seven-suffix strings.HasSuffix loop to a switch on
//     the word's last byte.
//   - ftsTokenize stopped building a punctuation-stripped copy of the whole text
//     and now delegates its scan to ftsForEachToken.
//   - evalFTSMatch/evalFTSRank now route TERM/OR-only queries through
//     ftsAnyLiteralTermMatch/ftsLiteralTermsRank instead of building a token
//     slice and frequency map per row.
//
// The references below are verbatim copies of the previous implementations, so
// any behavioral drift fails these tests rather than silently changing which
// documents a search returns.

// ftsStemReference is the pre-optimization ftsStem.
func ftsStemReference(w string) string {
	for _, suffix := range []string{"ing", "tion", "ed", "ly", "er", "est", "s"} {
		if len(w) > len(suffix)+3 && strings.HasSuffix(w, suffix) {
			return w[:len(w)-len(suffix)]
		}
	}
	return w
}

// ftsTokenizeReference is the pre-optimization ftsTokenize, using the reference
// stemmer so this test stays independent of TestFTSStemMatchesReference.
func ftsTokenizeReference(text string) []string {
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
			out = append(out, ftsStemReference(w))
		}
	}
	return out
}

// ftsStemCorpus exercises every suffix, every near-miss length boundary, and
// the non-letter/non-ASCII cases the tokenizer can hand the stemmer.
func ftsStemCorpus() []string {
	words := []string{
		// One per recognized suffix, long enough to strip.
		"running", "creation", "computed", "quickly", "faster", "fastest", "words",
		// Exactly at and just under each length boundary (len(w) > len(suffix)+3).
		"s", "es", "aes", "abes", "abcs", "abcds",
		"ed", "aed", "abed", "abced", "abcded",
		"er", "aer", "aber", "abcer", "abcder",
		"ly", "aly", "ably", "abcly", "abcdly",
		"ing", "aing", "abing", "abcing", "abcding",
		"est", "aest", "abest", "abcest", "abcdest",
		"tion", "ation", "bation", "abcation", "abcdation",
		// Words ending in a switch-selected byte that do NOT carry the suffix.
		"bag", "bun", "bad", "bay", "bar", "bat", "bus",
		"catalog", "children", "thousand", "supply", "cellar", "distant", "campus",
		// No recognized final byte at all.
		"alpha", "topic", "vector", "index", "quiz", "hello",
		// Digits and mixed alphanumeric tokens the tokenizer emits.
		"12345", "a1b2c3", "sku9s", "2024ed",
		// Degenerate inputs.
		"", "a", "ab", "abc", "abcd",
	}
	// Also cover every 5-byte word ending in each switch byte, which is where
	// the length guard and the suffix check interact most tightly.
	for _, last := range []string{"g", "n", "d", "y", "r", "t", "s"} {
		for _, stem := range []string{"aaaa", "ainn", "atio", "abcd"} {
			words = append(words, stem+last)
		}
	}
	return words
}

func TestFTSStemMatchesReference(t *testing.T) {
	for _, w := range ftsStemCorpus() {
		if got, want := ftsStem(w), ftsStemReference(w); got != want {
			t.Errorf("ftsStem(%q) = %q, reference = %q", w, got, want)
		}
	}
}

// ftsTokenizeCorpus covers case folding, punctuation, multi-byte UTF-8,
// invalid UTF-8, stop words, and the short-token filter.
func ftsTokenizeCorpus() []string {
	return []string{
		"",
		" ",
		"a",
		"the and of",
		"hello world",
		"Hello World",
		"HELLO WORLD",
		"HeLLo, WoRLD!",
		"The Quick Brown Fox Jumps Over The Lazy Dog",
		"running quickly toward creation",
		"vector search with BM25 ranking",
		"tabs\tand\nnewlines\r\nmixed",
		"punctuation!!!___---***everywhere???",
		"numbers 123 4567 89",
		"mixed42alpha 7up sku-9",
		// Multi-byte UTF-8: every byte is >= 0x80, so a delimiter either way.
		"Grüße aus München",
		"ÄPFEL und Birnen",
		"naïve café résumé",
		"日本語 の テキスト",
		"emoji 🚀 rocket 🎯 target",
		// Invalid UTF-8 bytes interleaved with ASCII.
		"before\xffafter",
		"\x80\x81\x82",
		"bad\xc3sequence",
		// Leading/trailing/internal delimiter runs.
		"   leading spaces",
		"trailing spaces   ",
		"many     internal     spaces",
		"---",
		// Stop words adjacent to content words.
		"the vector and the index",
		"IS IT AS BY",
		// Long-ish document to exercise the slice capacity estimate.
		strings.Repeat("alpha beta gamma delta epsilon ", 40),
		// Short-word text, where the capacity estimate under-reserves.
		strings.Repeat("ab cd ef ", 60),
	}
}

func TestFTSTokenizeMatchesReference(t *testing.T) {
	for _, text := range ftsTokenizeCorpus() {
		got := ftsTokenize(text)
		want := ftsTokenizeReference(text)
		if len(got) != len(want) {
			t.Errorf("ftsTokenize(%q) returned %d tokens %q, reference returned %d tokens %q",
				text, len(got), got, len(want), want)
			continue
		}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("ftsTokenize(%q) token %d = %q, reference = %q (full: %q vs %q)",
					text, i, got[i], want[i], got, want)
				break
			}
		}
	}
}

// TestFTSToASCIILowerOnlyFoldsASCII guards the documented contract that
// ftsToASCIILower must not do a Unicode fold: the tokenizer has always folded
// only 'A'-'Z', and widening that would change which tokens a non-ASCII corpus
// produces.
func TestFTSToASCIILowerOnlyFoldsASCII(t *testing.T) {
	cases := []struct{ in, want string }{
		{"ABC", "abc"},
		{"aBc", "abc"},
		{"abc", "abc"},
		{"", ""},
		{"123!@#", "123!@#"},
		{"ÄÖÜ", "ÄÖÜ"},         // non-ASCII uppercase stays untouched
		{"MÜNCHEN", "mÜnchen"}, // only the ASCII letters fold
		{"\xff\xfe", "\xff\xfe"},
	}
	for _, c := range cases {
		if got := ftsToASCIILower(c.in); got != c.want {
			t.Errorf("ftsToASCIILower(%q) = %q, want %q", c.in, got, c.want)
		}
		if got, want := ftsHasASCIIUpper(c.in), c.in != ftsToASCIILower(c.in); got != want {
			t.Errorf("ftsHasASCIIUpper(%q) = %v, but fold %s the string",
				c.in, got, map[bool]string{true: "changed", false: "did not change"}[want])
		}
	}
}

// ftsLiteralORQueries are queries whose parse tree is TERM/OR-only (and so take
// the fast path) plus queries that must NOT be treated as such.
func ftsLiteralORQueries() []string {
	return []string{
		"alpha",
		"alpha OR beta",
		"alpha OR beta OR gamma",
		"alpha OR beta OR gamma OR delta OR epsilon",
		"running OR creation",
		"Alpha OR BETA",
		"the OR alpha", // stop-word term never matches a token
		"zzznotpresent OR alpha",
		"zzznotpresent",
		// Not literal-OR: these must fall through to the map-based path.
		"alpha beta",
		"alpha AND beta",
		"NOT alpha",
		"alpha OR NOT beta",
		`"alpha beta"`,
		"alpha*",
		"al?ha",
		"alpha OR beta*",
	}
}

// TestFTSLiteralORFastPathMatchesNodeEval pins the FTS_MATCH fast path against
// the map-based ftsMatchNode oracle it bypasses, for every text/query pair.
func TestFTSLiteralORFastPathMatchesNodeEval(t *testing.T) {
	for _, query := range ftsLiteralORQueries() {
		node := parseCachedFTSQuery(query)
		if node == nil {
			continue
		}
		terms, isLiteralOR := ftsRootLiteralORTerms(node)
		for _, text := range ftsTokenizeCorpus() {
			tokens := ftsTokenize(text)
			freq := make(map[string]int, len(tokens))
			for _, tok := range tokens {
				freq[tok]++
			}
			want := ftsMatchNode(node, freq, tokens)
			if !isLiteralOR {
				continue
			}
			if got := ftsAnyLiteralTermMatch(text, terms); got != want {
				t.Errorf("query %q text %q: fast path = %v, ftsMatchNode = %v (terms %q)",
					query, text, got, want, terms)
			}
		}
	}
}

// TestFTSLiteralORRankMatchesScoreNode pins the FTS_RANK fast path against the
// map-based ftsScoreNode oracle it bypasses. Scores must agree bit-for-bit:
// FTS_RANK feeds ORDER BY, so even a last-bit difference could reorder results.
func TestFTSLiteralORRankMatchesScoreNode(t *testing.T) {
	for _, query := range ftsLiteralORQueries() {
		node := parseCachedFTSQuery(query)
		if node == nil {
			continue
		}
		terms, isLiteralOR := ftsRootLiteralORTerms(node)
		if !isLiteralOR {
			continue
		}
		for _, text := range ftsTokenizeCorpus() {
			tokens := ftsTokenize(text)
			freq := make(map[string]int, len(tokens))
			for _, tok := range tokens {
				freq[tok]++
			}
			var want float64
			if len(tokens) > 0 {
				want = ftsScoreNode(node, freq, 1.0, nil)
			}
			got := ftsLiteralTermsRank(text, terms, make([]int, len(terms)))
			if math.Float64bits(got) != math.Float64bits(want) {
				t.Errorf("query %q text %q: fast path score = %v (%#x), ftsScoreNode = %v (%#x)",
					query, text, got, math.Float64bits(got), want, math.Float64bits(want))
			}
		}
	}
}

// TestFTSRootLiteralORTermsOnlyForParsedRoots documents that the precomputed
// decomposition is reported only for trees produced by ftsParseQuery — a node
// synthesized by ftsExpandQuery/ftsBindIDF must report false rather than being
// mistaken for "not an OR tree" in a way that could skip a real check.
func TestFTSRootLiteralORTermsOnlyForParsedRoots(t *testing.T) {
	if _, ok := ftsRootLiteralORTerms(nil); ok {
		t.Error("nil node must not report a decomposition")
	}
	synthesized := &ftsQueryNode{op: "TERM", term: "alpha"}
	if _, ok := ftsRootLiteralORTerms(synthesized); ok {
		t.Error("a node built outside ftsParseQuery must not report a decomposition")
	}
	parsed := parseCachedFTSQuery("alpha OR beta")
	terms, ok := ftsRootLiteralORTerms(parsed)
	if !ok {
		t.Fatal("a parsed TERM/OR root must report its decomposition")
	}
	if fmt.Sprint(terms) != "[alpha beta]" {
		t.Errorf("decomposition = %q, want [alpha beta]", terms)
	}
	// The cached tree is shared; the decomposition must be stable across reads.
	again, _ := ftsRootLiteralORTerms(parseCachedFTSQuery("alpha OR beta"))
	if fmt.Sprint(again) != fmt.Sprint(terms) {
		t.Errorf("decomposition changed between reads: %q then %q", terms, again)
	}
}
