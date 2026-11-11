// Measure one-row appends to a warmed HNSW index, including production vector
// cache extension, graph insertion and persisted snapshot publication. Setup
// and the initial graph build are outside the timer. The table grows during
// measurement, so use identical fixed operation counts for before/after runs.
// Enough iterations must cross the publication window to include its cost.
package engine

import (
	"context"
	"math/rand"
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

// vecHNSWGrowthVector generates a deterministic (seeded on i, so calling it
// again for the same i always reproduces the same vector) pseudo-random
// embedding rather than a smooth function of i and d such as
// math.Sin(a*i+b*d): every row from such a function lies on the same
// low-dimensional (effectively one-parameter) curve through the embedding
// space no matter how the phase constants a, b are chosen, which is a worst
// case for HNSW's greedy graph search — its early-stopping condition
// (searchLayer, vector_index.go) relies on being able to tell "no reachable
// candidate can still improve the result", and that degrades toward visiting
// most of the graph when rows are not well separated. A benchmark built on
// such data would measure that pathology instead of the incremental-extend
// cost it exists to isolate, and would scale with table size for reasons
// having nothing to do with what this stage changed.
func vecHNSWGrowthVector(i, dims int) []float64 {
	rng := rand.New(rand.NewSource(int64(i)))
	vec := make([]float64, dims)
	for d := 0; d < dims; d++ {
		vec[d] = rng.NormFloat64()
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
		// Fetch through the production cache path so this benchmark includes
		// the append-only delta extension rather than manually maintaining an
		// obsolete flat cache representation.
		cache = getVecColumnCache("default", table, colIdx, metricNeedsNorms(metric))

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
