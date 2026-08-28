package engine

import (
	"strings"
	"testing"
)

// These benchmarks A/B each FTS hot-path rewrite against a reference copy of
// the code it replaced, inside a single binary and a single run. Comparing two
// separately-built binaries on this machine is unreliable (wall-clock variance
// between runs is large), but "new" and "reference" here execute back to back
// under identical conditions, and the allocation counters (B/op, allocs/op) are
// deterministic regardless of machine load.

// benchFTSProse is mixed-case English prose with punctuation — the shape of a
// real indexed document, where roughly one word in ten is capitalized.
var benchFTSProse = strings.Repeat(
	"The Quick Brown Fox jumps over the lazy dog, while Alice and Bob are "+
		"running vector searches across the document corpus; ranking results "+
		"by relevance requires computing BM25 scores for every matching term. ",
	12)

// benchFTSLowerProse is the same text already normalized to lower case, the
// common shape for a corpus loaded from a normalizing pipeline.
var benchFTSLowerProse = strings.ToLower(benchFTSProse)

func BenchmarkFTSStem(b *testing.B) {
	words := ftsStemCorpus()
	b.Run("new", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			for _, w := range words {
				sinkString = ftsStem(w)
			}
		}
	})
	b.Run("reference", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			for _, w := range words {
				sinkString = ftsStemReference(w)
			}
		}
	})
}

func BenchmarkFTSTokenizeMixedCase(b *testing.B) {
	b.Run("new", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			sinkTokens = ftsTokenize(benchFTSProse)
		}
	})
	b.Run("reference", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			sinkTokens = ftsTokenizeReference(benchFTSProse)
		}
	})
}

// BenchmarkFTSTokenizeLowerCase is the case the rewrite targets hardest: an
// already-lowercase document needs no case-folding copy at all, so only the
// returned token slice is allocated.
func BenchmarkFTSTokenizeLowerCase(b *testing.B) {
	b.Run("new", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			sinkTokens = ftsTokenize(benchFTSLowerProse)
		}
	})
	b.Run("reference", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			sinkTokens = ftsTokenizeReference(benchFTSLowerProse)
		}
	})
}

// BenchmarkFTSScalarMatchLiteralOR compares the per-row work evalFTSMatch now
// does for a TERM/OR query against the token-slice-plus-frequency-map path it
// used to take unconditionally.
func BenchmarkFTSScalarMatchLiteralOR(b *testing.B) {
	node := parseCachedFTSQuery("vector OR relevance")
	terms, ok := ftsRootLiteralORTerms(node)
	if !ok {
		b.Fatal("expected a literal-OR decomposition for the benchmark query")
	}
	b.Run("fastpath", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			sinkBool = ftsAnyLiteralTermMatch(benchFTSProse, terms)
		}
	})
	b.Run("mappath", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			tokens := ftsTokenize(benchFTSProse)
			freq := make(map[string]int, len(tokens))
			for _, t := range tokens {
				freq[t]++
			}
			sinkBool = ftsMatchNode(node, freq, tokens)
		}
	})
}

// BenchmarkFTSScalarRankLiteralOR is BenchmarkFTSScalarMatchLiteralOR for the
// scoring path evalFTSRank takes.
func BenchmarkFTSScalarRankLiteralOR(b *testing.B) {
	node := parseCachedFTSQuery("vector OR relevance")
	terms, ok := ftsRootLiteralORTerms(node)
	if !ok {
		b.Fatal("expected a literal-OR decomposition for the benchmark query")
	}
	b.Run("fastpath", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			sinkFloat = ftsLiteralTermsRank(benchFTSProse, terms, make([]int, len(terms)))
		}
	})
	b.Run("mappath", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			tokens := ftsTokenize(benchFTSProse)
			freq := make(map[string]int, len(tokens))
			for _, t := range tokens {
				freq[t]++
			}
			sinkFloat = ftsScoreNode(node, freq, 1.0, nil)
		}
	})
}

// Package-level sinks keep the compiler from eliminating the benchmarked work.
var (
	sinkString string
	sinkTokens []string
	sinkBool   bool
	sinkFloat  float64
)
