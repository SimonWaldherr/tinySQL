package engine

import (
	"errors"
	"fmt"
	"math"
	"math/rand"
	"slices"
	"testing"
)

func legacyVecMerge(results []vecSearchWorkerResult, k, candidateCount int) ([]vecScoredRow, error) {
	var merged vecScoredHeap
	haveHeap := false
	capacity := max(0, min(k, candidateCount))
	for i := range results {
		if results[i].err != nil {
			return nil, results[i].err
		}
		if !haveHeap {
			haveHeap = true
			if cap(results[i].heapRows) >= capacity {
				merged = results[i].heapRows
				results[i].heapRows = nil
				continue
			}
			merged = *newScoredHeap(k, candidateCount)
		}
		for _, row := range results[i].heapRows {
			pushTopK(&merged, row.rowIdx, row.distance, k)
		}
	}
	return legacyVecHeapResults(&merged, k), nil
}

func vectorFullMergeFixture(count, workers int, order string) []vecSearchWorkerResult {
	rng := rand.New(rand.NewSource(1043))
	results := make([]vecSearchWorkerResult, workers)
	for worker := range workers {
		low, high := worker*count/workers, (worker+1)*count/workers
		heap := make(vecScoredHeap, 0, high-low)
		for row := low; row < high; row++ {
			value := rng.Float64()*200 - 100
			switch order {
			case "ascending":
				value = float64(row)
			case "descending":
				value = float64(count - row)
			case "ties":
				value = float64(row % 7)
			case "infinities":
				value = []float64{math.Inf(-1), math.Inf(1), 0}[row%3]
			}
			vecScoredHeapPush(&heap, vecScoredRow{rowIdx: row, distance: value})
		}
		results[worker].heapRows = heap
	}
	return results
}

func cloneVecWorkerResults(input []vecSearchWorkerResult) []vecSearchWorkerResult {
	results := slices.Clone(input)
	for i := range results {
		if input[i].heapRows != nil {
			results[i].heapRows = make(vecScoredHeap, len(input[i].heapRows), cap(input[i].heapRows))
			copy(results[i].heapRows, input[i].heapRows)
		}
	}
	return results
}

func TestVectorFullMergePreservesResults(t *testing.T) {
	for _, count := range []int{1024, 1025} {
		for _, workers := range []int{2, 3, 8, 64} {
			for _, order := range []string{"random", "ascending", "descending", "ties", "infinities"} {
				t.Run(fmt.Sprintf("rows%d/workers%d/%s", count, workers, order), func(t *testing.T) {
					fixture := vectorFullMergeFixture(count, workers, order)
					for _, k := range []int{count, count + 100, math.MaxInt} {
						want, err := legacyVecMerge(cloneVecWorkerResults(fixture), k, count)
						if err != nil {
							t.Fatal(err)
						}
						results := cloneVecWorkerResults(fixture)
						buffers := slices.Clone(results)
						got, ok := mergeFullVecHeaps(results, k, count)
						if !ok {
							t.Fatal("full merge unexpectedly fell back")
						}
						assertVecResultBits(t, got, want)
						for i := range results {
							if results[i].heapRows != nil {
								t.Fatal("consumed worker retained storage")
							}
							for row := range buffers[i].heapRows {
								buffers[i].heapRows[row] = vecScoredRow{rowIdx: row, distance: -999}
							}
						}
						assertVecResultBits(t, got, want)
						got, err = mergeVecSearchResults(cloneVecWorkerResults(fixture), k, count)
						if err != nil {
							t.Fatal(err)
						}
						assertVecResultBits(t, got, want)
					}
				})
			}
		}
	}
}

func TestVectorFullMergeSparseWorkers(t *testing.T) {
	results := vectorFullMergeFixture(2048, 4, "random")
	results[0].heapRows = nil
	results[2].heapRows = nil
	want, err := legacyVecMerge(cloneVecWorkerResults(results), 2048, 2048)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := mergeFullVecHeaps(results, 2048, 2048)
	if !ok || len(got) != 1024 || cap(got) != 1024 {
		t.Fatalf("sparse merge: %d/%d, optimized=%v", len(got), cap(got), ok)
	}
	assertVecResultBits(t, got, want)
}

func TestVectorFullMergeFallbacks(t *testing.T) {
	workerError := errors.New("worker failed")
	for _, scenario := range []string{"small", "selection", "many-workers", "reuse", "signed-zero", "nan", "invalid-heap", "overlap", "alias", "error", "empty", "underestimated", "sparse"} {
		t.Run(scenario, func(t *testing.T) {
			results := vectorFullMergeFixture(1024, 4, "random")
			k, capacity := 1024, 1024
			switch scenario {
			case "small":
				results = vectorFullMergeFixture(1023, 4, "random")
			case "selection":
				k = 1000
			case "many-workers":
				results = vectorFullMergeFixture(1024, 65, "random")
			case "reuse":
				heap := make(vecScoredHeap, len(results[0].heapRows), capacity)
				copy(heap, results[0].heapRows)
				results[0].heapRows = heap
			case "signed-zero":
				results[1].heapRows[10].distance = math.Copysign(0, -1)
			case "nan":
				results[1].heapRows[10].distance = math.NaN()
			case "invalid-heap":
				results[1].heapRows[1].distance = math.Inf(1)
			case "overlap":
				for i := range results[1].heapRows {
					results[1].heapRows[i].rowIdx = results[0].heapRows[i].rowIdx
				}
			case "alias":
				results[1].heapRows = results[0].heapRows
			case "error":
				results[2].err = workerError
			case "empty":
				results = nil
			case "underestimated":
				capacity = -1
			case "sparse":
				results[0].heapRows = nil
				results[1].heapRows = results[1].heapRows[:0]
				results[2].heapRows = nil
				results[3].heapRows = results[3].heapRows[:10]
			}
			before := cloneVecWorkerResults(results)
			if _, ok := mergeFullVecHeaps(results, k, max(0, min(k, capacity))); ok {
				t.Fatal("expected fallback")
			}
			for i := range results {
				assertVecResultBits(t, results[i].heapRows, before[i].heapRows)
			}
			got, err := mergeVecSearchResults(results, k, capacity)
			want, wantErr := legacyVecMerge(before, k, capacity)
			if err != wantErr {
				t.Fatalf("error %v, want %v", err, wantErr)
			}
			assertVecResultBits(t, got, want)
		})
	}
}
