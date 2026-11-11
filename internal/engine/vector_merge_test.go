package engine

import (
	"errors"
	"math"
	"reflect"
	"runtime"
	"testing"
)

func TestVectorMergeMatchesIndependentHeap(t *testing.T) {
	input := vectorHeapTestRows(257) // ties, signed zero and infinities
	for _, workers := range []int{1, 2, 7} {
		for _, k := range []int{-1, 0, 1, 20, 128, 1000, math.MaxInt} {
			results := make([]vecSearchWorkerResult, workers)
			wantHeap := newScoredHeap(k, len(input))
			for worker := range workers {
				heap := newScoredHeap(k, (len(input)+workers-1)/workers)
				for i := worker; i < len(input); i += workers {
					pushTopK(heap, input[i].rowIdx, input[i].distance, k)
				}
				results[worker].heapRows = *heap
				for _, row := range *heap {
					pushTopK(wantHeap, row.rowIdx, row.distance, k)
				}
			}
			want := topKFromHeap(wantHeap, k)
			got, err := mergeVecSearchResults(results, k, len(input))
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("workers=%d k=%d: %v / %v, want %v", workers, k, got, err, want)
			}
		}
	}
	first, later := errors.New("first worker"), errors.New("later worker")
	for _, results := range [][]vecSearchWorkerResult{
		{{err: first}, {err: later}},
		{{heapRows: vecScoredHeap{{rowIdx: 1}}}, {err: first}, {err: later}},
	} {
		if rows, err := mergeVecSearchResults(results, 20, 50); rows != nil || !errors.Is(err, first) {
			t.Fatalf("worker error priority: %v / %v", rows, err)
		}
	}
}

func TestVectorParallelMergeMatchesAllScores(t *testing.T) {
	previous := runtime.GOMAXPROCS(2)
	t.Cleanup(func() { runtime.GOMAXPROCS(previous) })
	cache := filteredVectorHeapFixture(8192)
	allowed := make([]int, cache.rows)
	for i := range allowed {
		allowed[i] = i
	}
	query := []float64{1, 0, 0}
	for _, metric := range []string{"cosine", "l2", "manhattan", "dot"} {
		distance := buildVecDistanceFunc(metric, query, vectorL2Norm(query))
		for _, k := range []int{20, 400, 5000, math.MaxInt} {
			got, err := vecSearchTopK(t.Context(), cache.table.Rows, len(query), k, cache, distance, metric == "cosine")
			want := referenceFilteredVectorScores(allowed, len(query), k, cache, distance, metric == "cosine")
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("metric=%s k=%d: flat parallel results differ: %d / %v, want %d", metric, k, len(got), err, len(want))
			}
		}
	}
}

func TestVectorIVFOversizedK(t *testing.T) {
	cache := filteredVectorHeapFixture(48)
	allowed := make([]int, cache.rows)
	for i := range allowed {
		allowed[i] = i
	}
	query := []float64{1, 0, 0}
	for _, metric := range []string{"cosine", "l2", "manhattan", "dot"} {
		idx, err := buildVecIVFIndex(t.Context(), cache.table, metric, len(query), cache)
		if err != nil {
			t.Fatal(err)
		}
		for _, k := range []int{100000, math.MaxInt} {
			got, err := idx.search(t.Context(), query, vectorL2Norm(query), k, cache)
			want := referenceFilteredVectorScores(allowed, len(query), k, cache,
				buildVecDistanceFunc(metric, query, vectorL2Norm(query)), metric == "cosine")
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("metric=%s k=%d: IVF results differ: %v / %v, want %v", metric, k, got, err, want)
			}
		}
	}
}
