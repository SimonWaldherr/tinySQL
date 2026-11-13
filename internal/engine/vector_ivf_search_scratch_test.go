package engine

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"slices"
	"sync"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// Preserve the allocating frontier as an independent probe-order and result
// reference, including extending the probe budget when too few rows score.
func legacyIVFSearchScratch(ctx context.Context, idx *vecIVFIndex, query []float64, queryNorm float64, k int, cache vecSearchColumnCacheEntry) ([]vecScoredRow, error) {
	if len(idx.centroids) == 0 {
		return nil, nil
	}
	probes := chooseIVFNProbe(len(idx.centroids), k)
	frontier := make(vecMinScoredHeap, 0, len(idx.centroids))
	kind := vecMetricKind(idx.metric)
	for c, centroid := range idx.centroids {
		distance, ok := vecRankingDistanceKind(kind, centroid, query, idx.centroidNorms[c], queryNorm)
		if ok {
			frontier = append(frontier, vecScoredRow{rowIdx: c, distance: distance})
		}
	}
	for c := len(frontier)/2 - 1; c >= 0; c-- {
		vecMinHeapDown(frontier, c)
	}
	scorer := newVecRowScorer(idx.metric, query, queryNorm, newVecRowResolver(&cache, metricNeedsNorms(idx.metric)))
	results := newScoredHeap(k, cache.rowCount())
	for probed := 0; len(frontier) != 0; probed++ {
		if probed >= probes && results.Len() >= k {
			break
		}
		centroid := vecMinHeapPop(&frontier)
		for position, row := range idx.lists[centroid.rowIdx] {
			if position&1023 == 0 {
				if err := checkCtx(ctx); err != nil {
					return nil, err
				}
			}
			if distance, ok := scorer.distance(row); ok {
				pushTopK(results, row, distance, k)
			}
		}
	}
	return topKFromHeap(results, k), nil
}

func ivfSearchScratchFixture(nlist int, metric string) (*vecIVFIndex, vecSearchColumnCacheEntry) {
	table := storage.NewTable("ivf_search_scratch", []storage.Column{{Name: "v", Type: storage.VectorType}}, false)
	for row := 0; row < 128; row++ {
		vector := []float64{float64(row%7 - 3), float64(row%5 - 2)}
		switch {
		case row%13 == 0:
			vector = nil
		case row%17 == 0:
			vector[0] = math.NaN()
		case row%19 == 0:
			vector = []float64{1}
		case row%23 == 0:
			vector = []float64{0, math.Copysign(0, -1)}
		}
		table.Rows = append(table.Rows, []any{vector})
	}
	cache := buildVecColumnCache(table, 0, true)
	idx := &vecIVFIndex{metric: metric, centroids: make([][]float64, nlist), lists: make([][]int, nlist)}
	for c := range idx.centroids {
		idx.centroids[c] = []float64{float64(c%7 - 3), float64(c%5 - 2)}
		switch {
		case c%17 == 0:
			idx.centroids[c][0] = math.NaN()
		case c%19 == 0:
			idx.centroids[c] = []float64{1}
		case c%23 == 0:
			idx.centroids[c][0] = math.Inf(1)
		}
		if c%29 != 0 {
			for row := c; row < cache.rowCount(); row += nlist {
				idx.lists[c] = append(idx.lists[c], row)
			}
		}
	}
	idx.centroidNorms = centroidNorms(idx.centroids)
	return idx, cache
}

func equalIVFScratchResults(a, b []vecScoredRow) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].rowIdx != b[i].rowIdx || math.Float64bits(a[i].distance) != math.Float64bits(b[i].distance) {
			return false
		}
	}
	return true
}

func TestIVFSearchScratchMatchesAllocatingFrontier(t *testing.T) {
	for _, nlist := range []int{0, 1, 4, 32, 64, 65, 129} {
		for _, metric := range []string{"l2", "cosine", "manhattan", "dot", "invalid"} {
			t.Run(fmt.Sprintf("lists%d/%s", nlist, metric), func(t *testing.T) {
				idx, cache := ivfSearchScratchFixture(nlist, metric)
				for _, query := range [][]float64{{1, 2}, {0, 0}, {-1, 0}, {math.Inf(1), 0}, {math.NaN(), 0}, {1}} {
					for _, k := range []int{1, 10, 128, 100000} {
						for _, canceled := range []bool{false, true} {
							ctx, cancel := context.WithCancel(t.Context())
							if canceled {
								cancel()
							}
							got, err := idx.search(ctx, query, vectorL2Norm(query), k, cache)
							want, refErr := legacyIVFSearchScratch(ctx, idx, query, vectorL2Norm(query), k, cache)
							cancel()
							if fmt.Sprint(err) != fmt.Sprint(refErr) || !equalIVFScratchResults(got, want) {
								t.Fatalf("k%d query%v canceled%t: got %v/%v, want %v/%v", k, query, canceled, got, err, want, refErr)
							}
						}
					}
				}
			})
		}
	}
}

func TestIVFSearchScratchOwnsResultsAndSupportsConcurrentReaders(t *testing.T) {
	idx, cache := ivfSearchScratchFixture(64, "l2")
	query := []float64{1, 2}
	want, err := legacyIVFSearchScratch(t.Context(), idx, query, 0, 10, cache)
	if err != nil {
		t.Fatal(err)
	}
	retained, err := idx.search(t.Context(), query, 0, 10, cache)
	if err != nil {
		t.Fatal(err)
	}
	saved := slices.Clone(retained)
	centroids, lists := make([][]float64, len(idx.centroids)), make([][]int, len(idx.lists))
	for c := range idx.centroids {
		centroids[c], lists[c] = slices.Clone(idx.centroids[c]), slices.Clone(idx.lists[c])
	}
	var workers sync.WaitGroup
	for range 16 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for range 20 {
				got, err := idx.search(t.Context(), query, 0, 10, cache)
				if err != nil || !equalIVFScratchResults(got, want) {
					t.Errorf("concurrent result changed: %v / %v", got, err)
					return
				}
				if len(got) != 0 {
					got[0].rowIdx = -1
				}
			}
		}()
	}
	workers.Wait()
	if !equalIVFScratchResults(retained, saved) || !reflect.DeepEqual(idx.lists, lists) {
		t.Fatal("search changed a retained result or inverted list")
	}
	for c := range centroids {
		for d, value := range centroids[c] {
			if math.Float64bits(value) != math.Float64bits(idx.centroids[c][d]) {
				t.Fatal("search changed a centroid")
			}
		}
	}
}
