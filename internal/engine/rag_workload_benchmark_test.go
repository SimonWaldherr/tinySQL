package engine

// RAG-workload benchmarks.
//
// These differ from the pre-existing FTS/vector benchmarks in one way that
// matters: the corpus has a realistic *vocabulary distribution*. setupFTSPerfTable
// gives every document the identical body text, so every document matches every
// query and BM25 selectivity cannot be observed at all. Real RAG corpora are
// Zipf-distributed — a handful of words appear in nearly every chunk, while the
// terms users actually search for (identifiers, error codes, product names)
// appear in a tiny fraction of them.
//
// That distinction is the whole point here: FTS_SEARCH currently scores every
// cached document on every query, so a query for a term present in 3 chunks out
// of 20000 costs the same as a query for a term present in all of them. These
// benchmarks make that cost visible and give the hybrid retrieval path
// (HYBRID_SEARCH, the retriever docs/rag-guide.md recommends by default) an
// end-to-end baseline, which it previously had none of.

import (
	"context"
	"encoding/json"
	"math"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

const (
	ragBenchRows   = 20000 // chunks in the corpus
	ragBenchDims   = 96    // embedding dimensions
	ragBenchVocab  = 4000  // distinct vocabulary terms
	ragBenchTokens = 120   // tokens per chunk
)

// ragLCG is a deterministic linear congruential generator. The corpus must be
// byte-identical across runs and across machines for benchmark numbers to be
// comparable, which rules out math/rand's globally seeded source.
type ragLCG uint64

func (r *ragLCG) next() uint64 {
	*r = ragLCG(uint64(*r)*6364136223846793005 + 1442695040888963407)
	return uint64(*r >> 11)
}

func (r *ragLCG) intn(n int) int { return int(r.next() % uint64(n)) }

// ragVocabulary builds a Zipf-weighted term list. Term i has weight 1/(i+1),
// so term 0 lands in most documents and the tail terms land in very few — the
// distribution that makes BM25 IDF meaningful and that decides how much work a
// selective query *should* be able to skip.
func ragVocabulary() (terms []string, cumulative []float64) {
	terms = make([]string, ragBenchVocab)
	cumulative = make([]float64, ragBenchVocab)
	total := 0.0
	for i := 0; i < ragBenchVocab; i++ {
		terms[i] = "term" + strconv.Itoa(i)
		total += 1.0 / float64(i+1)
		cumulative[i] = total
	}
	for i := range cumulative {
		cumulative[i] /= total
	}
	return terms, cumulative
}

func ragPickTerm(cumulative []float64, u float64) int {
	lo, hi := 0, len(cumulative)-1
	for lo < hi {
		mid := (lo + hi) / 2
		if cumulative[mid] < u {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

// ragBenchCorpus is the shared fixture: a chunk table shaped exactly like the
// schema docs/rag-guide.md recommends, including the chunk_id PRIMARY KEY that
// lets HYBRID_SEARCH detect its fusion identity automatically.
//
// Two term families are injected on top of the Zipf body text:
//
//   - "needle<N>" appears in exactly 3 chunks. This is the exact-identifier
//     query RAG systems rely on the lexical branch for (error codes, symbol
//     names) — the case where scanning the whole corpus is almost entirely
//     wasted work.
//   - the Zipf head terms appear in most chunks, giving the opposite extreme.
func ragBenchCorpus(tb testing.TB) *storage.DB {
	tb.Helper()

	terms, cumulative := ragVocabulary()
	db := storage.NewDB()
	table := storage.NewTable("rag_chunks", []storage.Column{
		{Name: "chunk_id", Type: storage.TextType, Constraint: storage.PrimaryKey},
		{Name: "doc_id", Type: storage.TextType},
		{Name: "chunk_index", Type: storage.IntType},
		{Name: "heading", Type: storage.TextType},
		{Name: "chunk_text", Type: storage.TextType},
		{Name: "search_text", Type: storage.TextType},
		{Name: "document_type", Type: storage.TextType},
		{Name: "quality", Type: storage.FloatType},
		{Name: "embedding", Type: storage.VectorType},
	}, false)

	rng := ragLCG(0x5EED)
	var sb strings.Builder
	docTypes := []string{"guide", "reference", "runbook", "faq"}

	table.Rows = make([][]any, 0, ragBenchRows)
	for i := 0; i < ragBenchRows; i++ {
		sb.Reset()
		for t := 0; t < ragBenchTokens; t++ {
			if t > 0 {
				sb.WriteByte(' ')
			}
			u := float64(rng.next()%1000000) / 1000000.0
			sb.WriteString(terms[ragPickTerm(cumulative, u)])
		}
		// Three chunks per needle, so each needle is present in 0.015% of the
		// corpus: selective enough that a full scan is ~6600x more work than
		// the answer requires.
		if i%3 == 0 && i/3 < 500 {
			sb.WriteString(" needle")
			sb.WriteString(strconv.Itoa(i / 3 % 167))
		}
		body := sb.String()

		vec := make([]float64, ragBenchDims)
		for d := 0; d < ragBenchDims; d++ {
			vec[d] = math.Sin(0.08*float64(i) + 0.17*float64(d))
		}

		table.Rows = append(table.Rows, []any{
			"chunk-" + strconv.Itoa(i),
			"doc-" + strconv.Itoa(i/12),
			i % 12,
			"Section " + strconv.Itoa(i%40),
			body,
			body,
			docTypes[i%len(docTypes)],
			0.5 + 0.5*math.Sin(float64(i)*0.03),
			vec,
		})
	}
	table.Version++
	if err := db.Put("default", table); err != nil {
		tb.Fatal(err)
	}
	return db
}

// ragBenchQueryVector returns a JSON-encoded query vector. VEC_SEARCH's arg
// parser accepts a JSON array string, which keeps these benchmarks free of
// driver-level parameter binding.
func ragBenchQueryVector(tb testing.TB) string {
	tb.Helper()
	q := make([]float64, ragBenchDims)
	for d := range q {
		q[d] = math.Cos(0.08*float64(d) + 0.5)
	}
	raw, err := json.Marshal(q)
	if err != nil {
		tb.Fatal(err)
	}
	return string(raw)
}

// runRAGBench executes sql repeatedly against a warm corpus. The first
// execution outside the timed loop warms the vector column cache and the
// tokenized-document cache, so the measurement reflects steady-state serving
// latency rather than one-off cache construction. Cold-start cost is measured
// separately by BenchmarkRAGFTSColdCache.
func runRAGBench(b *testing.B, db *storage.DB, sql string, wantRows int) {
	b.Helper()
	stmt := mustParse(sql)
	ctx := context.Background()

	rs, err := Execute(ctx, db, "default", stmt)
	if err != nil {
		b.Fatal(err)
	}
	if wantRows >= 0 && len(rs.Rows) != wantRows {
		b.Fatalf("warm-up returned %d rows, want %d", len(rs.Rows), wantRows)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rs, err := Execute(ctx, db, "default", stmt)
		if err != nil {
			b.Fatal(err)
		}
		if rs == nil {
			b.Fatal("nil result set")
		}
	}
}

// ─────────────────────────── end-to-end hybrid retrieval ───────────────────

// BenchmarkRAGHybridSearch measures the retriever docs/rag-guide.md recommends
// as the default: one vector pass plus one BM25 pass over 20k chunks, fused
// with RRF. This is the number a RAG serving deployment actually pays per
// question, and nothing in the repository measured it before.
func BenchmarkRAGHybridSearch(b *testing.B) {
	db := ragBenchCorpus(b)
	qv := ragBenchQueryVector(b)
	runRAGBench(b, db, `
		SELECT chunk_id, doc_id, chunk_text, _rrf_rank
		FROM HYBRID_SEARCH('rag_chunks', 'embedding', 'search_text',
			'term7 term23 term180 needle42', '`+qv+`', 6,
			'{"candidate_k":24,"rrf_k":60,"metric":"cosine","index":"flat"}')
		ORDER BY _rrf_rank`, 6)
}

// BenchmarkRAGHybridSearchWithExpansion adds the neighbor-chunk expansion the
// guide recommends (one chunk before and after each hit), isolating what
// context expansion costs on top of retrieval.
func BenchmarkRAGHybridSearchWithExpansion(b *testing.B) {
	db := ragBenchCorpus(b)
	qv := ragBenchQueryVector(b)
	runRAGBench(b, db, `
		SELECT chunk_id, doc_id, chunk_index, chunk_text, _context_rank
		FROM HYBRID_SEARCH('rag_chunks', 'embedding', 'search_text',
			'term7 term23 term180 needle42', '`+qv+`', 6,
			'{"candidate_k":24,"rrf_k":60,"metric":"cosine","index":"flat",
			  "expand_before":1,"expand_after":1,
			  "doc_id_column":"doc_id","chunk_index_column":"chunk_index"}')
		ORDER BY _context_rank`, -1)
}

// BenchmarkRAGVecSearchBranch and BenchmarkRAGFTSSearchBranch measure the two
// halves of the hybrid query in isolation. Their sum bounds what
// BenchmarkRAGHybridSearch could cost if the two passes ran concurrently
// instead of one after the other, which is how RAG_SEARCH runs them today.
func BenchmarkRAGVecSearchBranch(b *testing.B) {
	db := ragBenchCorpus(b)
	qv := ragBenchQueryVector(b)
	runRAGBench(b, db, `
		SELECT chunk_id, _vec_rank
		FROM VEC_SEARCH('rag_chunks', 'embedding', '`+qv+`', 24, 'cosine', 'flat')`, 24)
}

func BenchmarkRAGFTSSearchBranch(b *testing.B) {
	db := ragBenchCorpus(b)
	runRAGBench(b, db, `
		SELECT chunk_id, _fts_rank
		FROM FTS_SEARCH('rag_chunks',
			'term7 OR term23 OR term180 OR needle42', 24, 'search_text')`, 24)
}

// ─────────────────────────── lexical selectivity ───────────────────────────

// BenchmarkRAGFTSSelectiveTerm queries a term present in exactly 3 of 20000
// chunks — the exact-identifier lookup hybrid retrieval exists to support.
// Every document in the corpus is still tokenized, matched and scored to
// produce those 3 rows.
func BenchmarkRAGFTSSelectiveTerm(b *testing.B) {
	db := ragBenchCorpus(b)
	runRAGBench(b, db, `
		SELECT chunk_id, _fts_score
		FROM FTS_SEARCH('rag_chunks', 'needle42', 10, 'search_text')`, 3)
}

// BenchmarkRAGFTSCommonTerm queries the head of the Zipf distribution, which
// appears in most chunks. This is the honest worst case: the match set really
// is most of the corpus, so no candidate-restriction strategy can avoid the
// work, and it is the control against which a selective-query speedup must be
// judged.
func BenchmarkRAGFTSCommonTerm(b *testing.B) {
	db := ragBenchCorpus(b)
	runRAGBench(b, db, `
		SELECT chunk_id, _fts_score
		FROM FTS_SEARCH('rag_chunks', 'term0', 10, 'search_text')`, 10)
}

// BenchmarkRAGFTSPrefixWildcard measures a prefix wildcard, which must expand
// against the term dictionary rather than a single term.
func BenchmarkRAGFTSPrefixWildcard(b *testing.B) {
	db := ragBenchCorpus(b)
	runRAGBench(b, db, `
		SELECT chunk_id, _fts_score
		FROM FTS_SEARCH('rag_chunks', 'needle4*', 10, 'search_text')`, 10)
}

// BenchmarkRAGFTSPhrase measures a quoted phrase, which needs positional
// verification and not just term frequencies.
func BenchmarkRAGFTSPhrase(b *testing.B) {
	db := ragBenchCorpus(b)
	runRAGBench(b, db, `
		SELECT chunk_id, _fts_score
		FROM FTS_SEARCH('rag_chunks', '"term0 term1"', 10, 'search_text')`, -1)
}

// ─────────────────────────── cold-start cost ───────────────────────────────

// BenchmarkRAGFTSColdCache measures building the tokenized-document cache from
// scratch, by bumping table.Version each iteration to force invalidation. This
// is the latency spike the first query after startup (or after any write) pays,
// and there is no FTS equivalent of VEC_WARM to pay it before admitting traffic.
func BenchmarkRAGFTSColdCache(b *testing.B) {
	db := ragBenchCorpus(b)
	table, err := db.Get("default", "rag_chunks")
	if err != nil {
		b.Fatal(err)
	}
	stmt := mustParse(`
		SELECT chunk_id FROM FTS_SEARCH('rag_chunks', 'needle42', 10, 'search_text')`)
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		table.Version++ // invalidate: next query rebuilds the whole doc cache
		b.StartTimer()
		if _, err := Execute(ctx, db, "default", stmt); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkRAGVecColdCache is the vector-side equivalent, for comparison: how
// much a write costs the vector branch versus the lexical branch.
func BenchmarkRAGVecColdCache(b *testing.B) {
	db := ragBenchCorpus(b)
	table, err := db.Get("default", "rag_chunks")
	if err != nil {
		b.Fatal(err)
	}
	qv := ragBenchQueryVector(b)
	stmt := mustParse(`
		SELECT chunk_id FROM VEC_SEARCH('rag_chunks', 'embedding', '` + qv + `', 24, 'cosine', 'flat')`)
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		table.Version++
		b.StartTimer()
		if _, err := Execute(ctx, db, "default", stmt); err != nil {
			b.Fatal(err)
		}
	}
}

// ─────────────────────────── cache footprint ───────────────────────────────

// TestRAGCacheFootprint is a measurement, not an assertion: it reports how much
// heap the two retrieval caches retain for one 20k-chunk corpus, so the memory
// cost per chunk is a known number rather than an assumption. Run with
// -run TestRAGCacheFootprint -v.
func TestRAGCacheFootprint(t *testing.T) {
	if testing.Short() {
		t.Skip("footprint measurement allocates a full corpus")
	}
	db := ragBenchCorpus(t)
	ctx := context.Background()

	measure := func(label, sql string) {
		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)
		if _, err := Execute(ctx, db, "default", mustParse(sql)); err != nil {
			t.Fatal(err)
		}
		runtime.GC()
		runtime.ReadMemStats(&after)
		delta := int64(after.HeapAlloc) - int64(before.HeapAlloc)
		t.Logf("%s: heap delta %+d bytes (%.1f bytes/chunk over %d chunks)",
			label, delta, float64(delta)/float64(ragBenchRows), ragBenchRows)
	}

	measure("FTS document cache", `
		SELECT chunk_id FROM FTS_SEARCH('rag_chunks', 'needle42', 10, 'search_text')`)
	measure("vector column cache", `
		SELECT chunk_id FROM VEC_SEARCH('rag_chunks', 'embedding', '`+ragBenchQueryVector(t)+`', 10, 'cosine', 'flat')`)
}
