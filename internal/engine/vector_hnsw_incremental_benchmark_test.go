// BenchmarkVecHNSWIncrementalInsert measures the cost of inserting a single
// new row into an already-large, already-warmed HNSW vector index — the
// case getVecHNSWIndex's incremental-extend fast path (vector_index.go)
// exists for. Before that fast path existed, ANY table.Version bump (every
// single INSERT bumps it) invalidated the cached graph outright, so serving
// the next vector query after even one new row meant a full from-scratch
// rebuild over the WHOLE table — O(existing rows), not O(1). This benchmark
// runs at a few different pre-existing table sizes so a comparison against
// a "before" binary (see the stage's instructions: git-stash this package's
// production changes, rebuild, run this same file) shows ns/op and B/op
// scaling with table size on "before" and staying roughly flat on "after".
//
// The per-iteration cache growth is done here by hand (append the new row's
// vector/validity/norm directly onto the existing cache value) rather than
// by calling getVecColumnCache, which invalidates and rebuilds its entire
// O(existing rows) column cache on every single table.Version bump — a
// separate, pre-existing bottleneck in vector_search.go that this stage does
// not touch. Rebuilding it on every one of the b.N iterations below would
// swamp the HNSW-specific cost this benchmark exists to isolate, in both the
// "before" and "after" trees equally, hiding the very difference this
// benchmark is meant to show. getVecHNSWIndex/extendVecHNSWIndex never read
// vecSearchColumnCacheEntry.table/.version themselves (only getVecColumnCache
// does, to decide whether ITS cache is stale), so handing them a by-hand-
// grown cache value is exactly as valid an input as one obtained by calling
// getVecColumnCache after every row.
//
// Otherwise calls getVecHNSWIndex directly instead of going through SQL
// INSERT + VEC_SEARCH: this isolates the index-maintenance cost this stage
// changed from unrelated parsing/planning overhead, and keeps the benchmark
// source compatible with the pre-change tree (it only touches symbols that
// already existed before this stage), which is exactly what makes the
// git-stash A/B comparison possible.
package engine

import (
	"context"
	"math"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func makeVecHNSWGrowthBenchmarkTable(preexisting, dims int) (*storage.DB, *storage.Table) {
	db := storage.NewDB()
	table := storage.NewTable("hnsw_grow", []storage.Column{
		{Name: "id", Type: storage.IntType},
		{Name: "embedding", Type: storage.VectorType},
	}, false)
	table.Rows = make([][]any, 0, preexisting)
	for i := 0; i < preexisting; i++ {
		table.Rows = append(table.Rows, []any{i, vecHNSWGrowthVector(i, dims)})
	}
	if err := db.Put("default", table); err != nil {
		panic(err)
	}
	return db, table
}

// vecHNSWGrowthVector mirrors makeRAGHybridBenchmarkTable's embedding shape
// (vector_search_benchmark_test.go) rather than inventing a new one: a phase
// step per row large enough, combined with a per-dimension phase offset, to
// keep rows well separated in the embedding space instead of nearly
// collinear. Nearly-collinear rows are a worst case for HNSW's greedy graph
// search — its early-stopping condition (searchLayer, vector_index.go) relies
// on being able to tell "no reachable candidate can still improve the
// result", which degrades toward visiting most of the graph when many rows
// are all almost equidistant from the query — and would make this benchmark
// measure that pathology instead of the incremental-extend cost it exists to
// isolate.
func vecHNSWGrowthVector(i, dims int) []float64 {
	vec := make([]float64, dims)
	for d := 0; d < dims; d++ {
		angle := 0.08*float64(i) + 0.17*float64(d)
		vec[d] = math.Sin(angle)
	}
	return vec
}

// benchmarkVecHNSWIncrementalInsert warms an HNSW index over `preexisting`
// rows once, then times b.N single-row insert cycles: append the row to
// t.Rows, bump Version exactly like INSERT does, grow the vector cache by
// exactly one entry (see the file doc comment for why that is done by hand
// here), and re-fetch the HNSW index — the step that actually builds or
// extends the graph, and the one this stage changed.
func benchmarkVecHNSWIncrementalInsert(b *testing.B, preexisting int) {
	const dims = 64
	const metric = "cosine"
	_, table := makeVecHNSWGrowthBenchmarkTable(preexisting, dims)
	colIdx, err := table.ColIndex("embedding")
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()

	cache := getVecColumnCache("default", table, colIdx, metricNeedsNorms(metric))
	if _, err := getVecHNSWIndex(ctx, "default", table, colIdx, metric, dims, cache); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := preexisting + i
		vec := vecHNSWGrowthVector(id, dims)
		table.Rows = append(table.Rows, []any{id, vec})
		table.Version++

		cache.vectors = append(cache.vectors, vec)
		cache.valid = append(cache.valid, true)
		if cache.normsReady {
			cache.norms = append(cache.norms, vectorL2Norm(vec))
		}

		if _, err := getVecHNSWIndex(ctx, "default", table, colIdx, metric, dims, cache); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkVecHNSWIncrementalInsert_1k(b *testing.B) {
	benchmarkVecHNSWIncrementalInsert(b, 1_000)
}

func BenchmarkVecHNSWIncrementalInsert_10k(b *testing.B) {
	benchmarkVecHNSWIncrementalInsert(b, 10_000)
}

func BenchmarkVecHNSWIncrementalInsert_50k(b *testing.B) {
	benchmarkVecHNSWIncrementalInsert(b, 50_000)
}
