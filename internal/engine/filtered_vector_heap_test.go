package engine

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"runtime"
	"slices"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func filteredVectorHeapFixture(rows int) vecSearchColumnCacheEntry {
	table := storage.NewTable("filtered_heap", []storage.Column{{Name: "embedding", Type: storage.VectorType}}, false)
	for i := range rows {
		var value any = []float64{float64(i % 7), float64(i % 3), 1}
		if i%11 == 0 {
			value = nil
		} else if i%13 == 0 {
			value = []float64{1, 2}
		}
		table.Rows = append(table.Rows, []any{value})
	}
	return buildVecColumnCache(table, 0, true)
}

func referenceFilteredVectorScores(allowed []int, dims, k int, cache vecSearchColumnCacheEntry, distance vecDistanceFunc, needNorm bool) []vecScoredRow {
	if k <= 0 {
		return nil
	}
	var result []vecScoredRow
	for _, row := range allowed {
		vector, norm, valid := cache.resolveRow(row, needNorm)
		if !valid || len(vector) != dims {
			continue
		}
		d, ok := distance(vector, norm)
		if ok && !math.IsNaN(d) {
			result = append(result, vecScoredRow{rowIdx: row, distance: d})
		}
	}
	slices.SortFunc(result, func(a, b vecScoredRow) int {
		if vecScoredRowLess(a, b) {
			return -1
		}
		if vecScoredRowLess(b, a) {
			return 1
		}
		return 0
	})
	return result[:min(len(result), k)]
}

func TestFilteredVectorHeapBoundsMatchScores(t *testing.T) {
	cache := filteredVectorHeapFixture(48)
	segmented := cache
	segmented.segments = append(slices.Clone(cache.segments), vecColumnSegment{start: cache.rows})
	updated := cache
	updated.overrides = map[int]vecColumnOverride{1: {vector: []float64{9, 0, 0}, norm: 9, valid: true}, 2: {valid: false}}
	query := []float64{1, 0, 0}
	for _, candidateCache := range []vecSearchColumnCacheEntry{cache, segmented, updated} {
		for _, metric := range []string{"cosine", "l2", "manhattan", "dot"} {
			distance := buildVecDistanceFunc(metric, query, vectorL2Norm(query))
			for _, allowed := range [][]int{nil, {1}, {-1, 0, 1, 1, 2, 7, 11, 13, 28, 48, 99}} {
				for _, k := range []int{0, 1, 3, 1000, math.MaxInt} {
					got, err := ragVecTopKAllowed(t.Context(), allowed, len(query), k, candidateCache, distance, metric == "cosine")
					want := referenceFilteredVectorScores(allowed, len(query), k, candidateCache, distance, metric == "cosine")
					if err != nil || !reflect.DeepEqual(got, want) {
						t.Fatalf("metric=%s k=%d allowed=%v: got %v / %v, want %v", metric, k, allowed, got, err, want)
					}
				}
			}
		}
	}
	// Even a scorer reporting success must not admit NaN into a result heap.
	nanDistance := func([]float64, float64) (float64, bool) { return math.NaN(), true }
	if got, err := ragVecTopKAllowed(t.Context(), []int{1, 2}, 3, math.MaxInt, cache, nanDistance, false); err != nil || len(got) != 0 {
		t.Fatalf("NaN scores admitted: %v / %v", got, err)
	}
}

func TestFilteredVectorHeapParallelLargeK(t *testing.T) {
	previous := runtime.GOMAXPROCS(2)
	t.Cleanup(func() { runtime.GOMAXPROCS(previous) })
	cache := filteredVectorHeapFixture(8192)
	allowed := make([]int, 4096)
	for i := range allowed {
		allowed[i] = i * 2
	}
	if vecSearchWorkerCount(len(allowed), 3) != 2 {
		t.Fatal("fixture must exercise the parallel merge")
	}
	query := []float64{1, 0, 0}
	distance := buildVecDistanceFunc("l2", query, 0)
	for _, k := range []int{20, 4096, math.MaxInt} {
		got, err := ragVecTopKAllowed(t.Context(), allowed, len(query), k, cache, distance, false)
		want := referenceFilteredVectorScores(allowed, len(query), k, cache, distance, false)
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("k=%d: parallel result differs: got %d / %v, want %d", k, len(got), err, len(want))
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := ragVecTopKAllowed(ctx, allowed, len(query), math.MaxInt, cache, distance, false); !errors.Is(err, context.Canceled) {
		t.Fatal("parallel cancellation", err)
	}
}

func BenchmarkFilteredVectorHeapCapacity(b *testing.B) {
	cache := filteredVectorHeapFixture(8192)
	query := []float64{1, 0, 0}
	distance := buildVecDistanceFunc("l2", query, 0)
	for _, count := range []int{8, 4096} {
		allowed := make([]int, count)
		for i := range allowed {
			allowed[i] = i + 1
		}
		for _, k := range []int{20, 100000} {
			b.Run(fmt.Sprintf("allowed%d_k%d", count, k), func(b *testing.B) {
				want := min(count, k)
				// The fixture deliberately contains missing/malformed vectors.
				if k >= count {
					want = len(referenceFilteredVectorScores(allowed, len(query), k, cache, distance, false))
				}
				b.ReportAllocs()
				for b.Loop() {
					got, err := ragVecTopKAllowed(b.Context(), allowed, len(query), k, cache, distance, false)
					if err != nil || len(got) != want {
						b.Fatalf("got %d / %v, want %d", len(got), err, want)
					}
				}
			})
		}
	}
}
