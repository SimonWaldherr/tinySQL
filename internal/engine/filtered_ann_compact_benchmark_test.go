package engine

import (
	"fmt"
	"math"
	"runtime"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// Sparse overrides keep the fixture proportional to its selected vectors;
// corpus-sized graph allocations remain part of the measured operation.
func sparseFilteredANNFixture(rows, allowed, dims int) (*storage.Table, vecSearchColumnCacheEntry, *ragRowFilter) {
	table := storage.NewTable("sparse_filtered_ann", []storage.Column{{Name: "embedding", Type: storage.VectorType}}, false)
	cache := vecSearchColumnCacheEntry{table: table, rows: rows, normsReady: true, overrides: make(map[int]vecColumnOverride, allowed)}
	filter := &ragRowFilter{rows: make([]int, allowed)}
	for i := range filter.rows {
		row := i * (rows / allowed)
		filter.rows[i] = row
		vector := make([]float64, dims)
		for d := range vector {
			vector[d] = math.Sin(float64(i/2+1)*0.17 + float64(d)*0.2)
		}
		cache.overrides[row] = vecColumnOverride{vector: vector, norm: vectorL2Norm(vector), valid: true}
	}
	return table, cache, filter
}

func BenchmarkFilteredANNSelectiveBuild(b *testing.B) {
	for _, allowed := range []int{128, 1024, 4096} {
		b.Run(fmt.Sprint(allowed), func(b *testing.B) {
			table, cache, filter := sparseFilteredANNFixture(1000000, allowed, 16)
			b.ReportAllocs()
			for b.Loop() {
				idx, err := buildRAGFilteredANNIndex(b.Context(), table, "cosine", 16, cache, filter)
				if err != nil {
					b.Fatal(err)
				}
				runtime.KeepAlive(idx)
			}
		})
	}
}

func BenchmarkFilteredANNSelectiveSearch(b *testing.B) {
	table, cache, filter := sparseFilteredANNFixture(1000000, 4096, 16)
	// Exercise the usual contiguous-column resolver, not the map-based sparse
	// fixture used to isolate graph-build allocations above. Only selected
	// rows need payloads; corpus-sized headers are setup outside the timer.
	segment := vecColumnSegment{
		vectors: make([][]float64, cache.rowCount()),
		valid:   make([]bool, cache.rowCount()),
		norms:   make([]float64, cache.rowCount()),
	}
	for row, value := range cache.overrides {
		segment.vectors[row], segment.valid[row], segment.norms[row] = value.vector, value.valid, value.norm
	}
	cache.segments = []vecColumnSegment{segment}
	cache.overrides = nil
	idx, err := buildRAGFilteredANNIndex(b.Context(), table, "cosine", 16, cache, filter)
	if err != nil {
		b.Fatal(err)
	}
	query := make([]float64, 16)
	for d := range query {
		query[d] = math.Cos(float64(d) * 0.11)
	}
	norm := vectorL2Norm(query)
	b.ReportAllocs()
	for b.Loop() {
		rows, err := idx.searchFiltered(b.Context(), query, norm, 20, cache, filter)
		if err != nil || len(rows) != 20 {
			b.Fatalf("selection returned %d rows: %v", len(rows), err)
		}
	}
}
