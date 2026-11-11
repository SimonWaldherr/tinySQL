package engine

import (
	"fmt"
	"math"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func vectorValidationFixture(count, parts int, overrides bool) (*storage.Table, *storage.VectorIndex, vecSearchColumnCacheEntry) {
	table := storage.NewTable("vector_validation", []storage.Column{{Name: "embedding", Type: storage.VectorType}}, false)
	table.Rows = make([][]any, count)
	table.Version = 2
	cache := vecSearchColumnCacheEntry{table: table, rows: count}
	vector := []float64{1, 2, 3, 4}
	for part := range parts {
		from, to := part*count/parts, (part+1)*count/parts
		segment := vecColumnSegment{start: from, vectors: make([][]float64, to-from), valid: make([]bool, to-from)}
		for row := range segment.vectors {
			segment.vectors[row], segment.valid[row] = vector, true
		}
		cache.segments = append(cache.segments, segment)
	}
	if overrides {
		cache.overrides = make(map[int]vecColumnOverride)
		for row := 0; row < count; row += 31 {
			cache.overrides[row] = vecColumnOverride{vector: vector, valid: true}
		}
	}
	index := &storage.VectorIndex{
		Format: vecPersistentFormat, Kind: vecIndexHNSW, Column: 0, Metric: "cosine", Dims: 4,
		Version: table.Version, BuiltRows: count, Entry: 0, MaxLevel: 0,
		Levels: make([]int, count), Neighbors: make([][][]int, count),
	}
	layers, links := make([][]int, count), make([]int, count*4)
	for row := range count {
		list := links[row*4 : row*4+4 : row*4+4]
		for offset := range list {
			list[offset] = (row + offset) % count
		}
		layers[row] = list
		index.Neighbors[row] = layers[row : row+1 : row+1]
	}
	if count == 0 {
		index.Entry, index.MaxLevel = -1, -1
	}
	return table, index, cache
}

func TestVectorRowValidityMatchesSeparateLookups(t *testing.T) {
	for _, parts := range []int{1, 8} {
		for _, overrides := range []bool{false, true} {
			t.Run(fmt.Sprintf("segments=%d/overrides=%t", parts, overrides), func(t *testing.T) {
				_, _, cache := vectorValidationFixture(96, parts, overrides)
				for row := 0; row < cache.rows; row++ {
					segment := cache.segmentFor(row)
					local := row - segment.start
					switch row % 7 {
					case 0:
						segment.valid[local] = false
					case 1:
						segment.vectors[local] = nil
					case 2:
						segment.vectors[local] = []float64{1, 2}
					case 3:
						segment.vectors[local] = []float64{0, 0, 0, 0}
					case 4:
						segment.vectors[local] = []float64{math.NaN(), math.Inf(1), 0, 0}
					}
				}
				if overrides {
					cache.overrides[-1] = vecColumnOverride{vector: []float64{1, 2, 3, 4}, valid: true}
					cache.overrides[96] = cache.overrides[-1]
					cache.overrides[31] = vecColumnOverride{vector: []float64{1}, valid: true}
					cache.overrides[62] = vecColumnOverride{valid: false}
				}
				for _, dims := range []int{0, 2, 4, 5} {
					for row := -1; row <= cache.rows; row++ {
						want := row >= 0 && row < cache.rowCount() && cache.validAt(row) && len(cache.vector(row)) == dims
						if got := validCacheRow(cache, row, dims); got != want {
							t.Fatalf("row=%d dims=%d: got %t, want %t", row, dims, got, want)
						}
					}
				}
				if validCacheRow(cache, math.MaxInt, 4) {
					t.Fatal("accepted an out-of-range row")
				}
			})
		}
	}
}

func TestPersistentVectorValidationEligibility(t *testing.T) {
	type mutation func(*storage.VectorIndex, *vecSearchColumnCacheEntry)
	prefix := func(index *storage.VectorIndex, count int) {
		index.BuiltRows = count
		index.Levels, index.Neighbors = index.Levels[:count], index.Neighbors[:count]
		for row := range index.Neighbors {
			index.Neighbors[row] = [][]int{nil}
		}
	}
	empty := func(index *storage.VectorIndex, cache *vecSearchColumnCacheEntry) {
		index.Entry, index.MaxLevel = -1, -1
		for row := range index.Neighbors {
			index.Neighbors[row] = nil
		}
		for part := range cache.segments {
			clear(cache.segments[part].valid)
		}
		cache.overrides = nil
	}
	for _, tc := range []struct {
		name   string
		change mutation
		want   bool
	}{
		{"current", func(*storage.VectorIndex, *vecSearchColumnCacheEntry) {}, true},
		{"format", func(i *storage.VectorIndex, _ *vecSearchColumnCacheEntry) { i.Format-- }, false},
		{"column", func(i *storage.VectorIndex, _ *vecSearchColumnCacheEntry) { i.Column++ }, false},
		{"metric", func(i *storage.VectorIndex, _ *vecSearchColumnCacheEntry) { i.Metric = "dot" }, false},
		{"dims", func(i *storage.VectorIndex, _ *vecSearchColumnCacheEntry) { i.Dims++ }, false},
		{"future-version", func(i *storage.VectorIndex, _ *vecSearchColumnCacheEntry) { i.Version++ }, false},
		{"structure", func(i *storage.VectorIndex, _ *vecSearchColumnCacheEntry) { i.StructVersion++ }, false},
		{"older-prefix", func(i *storage.VectorIndex, _ *vecSearchColumnCacheEntry) { prefix(i, 4); i.Version-- }, true},
		{"current-prefix", func(i *storage.VectorIndex, _ *vecSearchColumnCacheEntry) { prefix(i, 4) }, false},
		{"entry", func(i *storage.VectorIndex, _ *vecSearchColumnCacheEntry) { i.Entry = 8 }, false},
		{"half-empty", func(i *storage.VectorIndex, _ *vecSearchColumnCacheEntry) { i.Entry = -1 }, false},
		{"missing-layer", func(i *storage.VectorIndex, _ *vecSearchColumnCacheEntry) { i.Neighbors[3] = nil }, false},
		{"negative-level", func(i *storage.VectorIndex, _ *vecSearchColumnCacheEntry) { i.Levels[3] = -1 }, false},
		{"extra-layer", func(i *storage.VectorIndex, _ *vecSearchColumnCacheEntry) { i.Neighbors[3] = [][]int{nil, nil} }, false},
		{"degree", func(i *storage.VectorIndex, _ *vecSearchColumnCacheEntry) {
			i.Neighbors[3][0] = make([]int, vecHNSWM0+1)
		}, false},
		{"neighbor", func(i *storage.VectorIndex, _ *vecSearchColumnCacheEntry) { i.Neighbors[3][0][0] = 8 }, false},
		{"negative-neighbor", func(i *storage.VectorIndex, _ *vecSearchColumnCacheEntry) { i.Neighbors[3][0][0] = -1 }, false},
		{"empty", empty, true},
		{"empty-with-vector", func(i *storage.VectorIndex, c *vecSearchColumnCacheEntry) { empty(i, c); c.segments[0].valid[0] = true }, false},
		{"invalid-layer", func(_ *storage.VectorIndex, c *vecSearchColumnCacheEntry) { c.segments[0].valid[0] = false }, false},
		{"invalid-without-layer", func(i *storage.VectorIndex, c *vecSearchColumnCacheEntry) {
			c.segments[0].valid[0] = false
			i.Neighbors[0] = nil
		}, true},
		{"zero-vector", func(_ *storage.VectorIndex, c *vecSearchColumnCacheEntry) {
			c.segments[0].vectors[0] = []float64{0, 0, 0, 0}
		}, true},
		{"nonfinite-flag", func(_ *storage.VectorIndex, c *vecSearchColumnCacheEntry) {
			c.segments[0].vectors[0] = []float64{math.NaN(), 0, 0, 0}
		}, true},
	} {
		for _, parts := range []int{1, 8} {
			t.Run(fmt.Sprintf("%s/segments=%d", tc.name, parts), func(t *testing.T) {
				table, index, cache := vectorValidationFixture(8, parts, false)
				tc.change(index, &cache)
				if got := usablePersistentVecHNSW(index, table, 0, "cosine", 4, cache); got != tc.want {
					t.Fatalf("eligibility = %t, want %t", got, tc.want)
				}
			})
		}
	}
}
