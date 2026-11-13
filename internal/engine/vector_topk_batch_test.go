package engine

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"testing"
)

func vectorTopKBatchFixture(count int, order string) [][]any {
	rng := rand.New(rand.NewSource(61))
	rows := make([][]any, count)
	for row := range rows {
		value := rng.Float64()*float64(count) + 1
		switch order {
		case "ascending":
			value = float64(row + 1)
		case "descending":
			value = float64(count - row)
		case "ties":
			value = float64(row%13 + 1)
		}
		rows[row] = []any{[]float64{value, 1}}
	}
	return rows
}

func assertVectorTopKBatchEqual(t *testing.T, got, want []vecScoredRow) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("length %d, want %d", len(got), len(want))
	}
	for row := range got {
		if got[row].rowIdx != want[row].rowIdx || math.Float64bits(got[row].distance) != math.Float64bits(want[row].distance) {
			t.Fatalf("result %d: %v, want %v", row, got[row], want[row])
		}
	}
}

func assertVectorTopKBatchHeap(t *testing.T, rows vecScoredHeap) {
	t.Helper()
	for child := 1; child < len(rows); child++ {
		if rows.Less(child, (child-1)/2) {
			t.Fatalf("worker returned an invalid heap at row %d", child)
		}
	}
}

func TestVectorTopKBatchMatchesRangeScan(t *testing.T) {
	rows := vectorTopKBatchFixture(1537, "random")
	for row := range rows {
		switch {
		case row%113 == 0:
			rows[row] = nil
		case row%127 == 0:
			rows[row] = []any{[]float64{1}}
		case row%131 == 0:
			rows[row] = []any{[]float64{math.NaN(), 1}}
		case row%139 == 0:
			rows[row] = []any{[]float64{math.Inf(1), 1}}
		case row%149 == 0:
			rows[row] = []any{[]float64{math.Copysign(0, -1), 0}}
		}
	}
	for _, norms := range []bool{false, true} {
		for _, layout := range []string{"contiguous", "segments", "overrides"} {
			boundaries := []int{len(rows)}
			if layout == "segments" {
				boundaries = []int{511, 1024, len(rows)}
			}
			cache := segmentScanCache(rows, boundaries, norms)
			if layout == "overrides" {
				cache.overrides = map[int]vecColumnOverride{
					0: {vector: []float64{2, 1}, norm: math.Sqrt(5), valid: true},
					1: {},
				}
			}
			for _, metric := range []string{"cosine", "l2", "manhattan", "dot"} {
				t.Run(fmt.Sprintf("%s/norms%t/%s", layout, norms, metric), func(t *testing.T) {
					query := []float64{1, 2}
					distance := buildVecDistanceFunc(metric, query, vectorL2Norm(query))
					for _, bounds := range [][2]int{{0, len(rows)}, {7, 1029}, {100, 100}} {
						for _, k := range []int{0, 1, 10, 63, 64, 65, 256, 1024, len(rows), math.MaxInt} {
							got, err := vecSearchTopKRange(t.Context(), rows, bounds[0], bounds[1], 2, k, cache, distance, metric == "cosine")
							want, refErr := referenceVecSegmentScan(t.Context(), rows, bounds[0], bounds[1], 2, k, cache, distance, metric == "cosine")
							if err != nil || refErr != nil {
								t.Fatalf("scan errors: %v, %v", err, refErr)
							}
							assertVectorTopKBatchHeap(t, got)
							assertVectorTopKBatchEqual(t, topKFromHeap(&got, k), topKFromHeap(&want, k))
						}
					}
				})
			}
		}
	}
}

func TestVectorTopKBatchExceptionalScores(t *testing.T) {
	rows := vectorTopKBatchFixture(512, "ascending")
	cache := segmentScanCache(rows, []int{len(rows)}, false)
	distance := func(vector []float64, _ float64) (float64, bool) {
		switch int(vector[0]) % 9 {
		case 0:
			return math.NaN(), true
		case 1:
			return math.Inf(-1), true
		case 2:
			return math.Inf(1), true
		case 3:
			return math.Copysign(0, -1), true
		case 4:
			return 0, true
		default:
			return vector[0], true
		}
	}
	for _, k := range []int{64, 256, len(rows), math.MaxInt} {
		got, err := vecSearchTopKRange(t.Context(), rows, 0, len(rows), 2, k, cache, distance, false)
		want, refErr := referenceVecSegmentScan(t.Context(), rows, 0, len(rows), 2, k, cache, distance, false)
		if err != nil || refErr != nil {
			t.Fatalf("scan errors: %v, %v", err, refErr)
		}
		assertVectorTopKBatchHeap(t, got)
		result := topKFromHeap(&got, k)
		assertVectorTopKBatchEqual(t, result, topKFromHeap(&want, k))
		before := append([]vecScoredRow(nil), result...)
		pushTopK(&got, -1, math.Inf(-1), k)
		assertVectorTopKBatchEqual(t, result, before)
	}
}

func TestVectorTopKBatchCancellation(t *testing.T) {
	rows := vectorTopKBatchFixture(3073, "ascending")
	cache := segmentScanCache(rows, []int{len(rows)}, false)
	for _, k := range []int{64, 256, math.MaxInt} {
		var calls [2]int
		for mode := range calls {
			ctx, cancel := context.WithCancel(t.Context())
			distance := func(vector []float64, _ float64) (float64, bool) {
				calls[mode]++
				if calls[mode] == 70 {
					cancel()
				}
				return vector[0], true
			}
			var err error
			if mode == 0 {
				_, err = vecSearchTopKRange(ctx, rows, 0, len(rows), 2, k, cache, distance, false)
			} else {
				_, err = referenceVecSegmentScan(ctx, rows, 0, len(rows), 2, k, cache, distance, false)
			}
			cancel()
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("k%d: cancellation = %v", k, err)
			}
		}
		if calls[0] != calls[1] {
			t.Fatalf("k%d: scored %d vs %d rows before cancellation", k, calls[0], calls[1])
		}
	}
}

func TestVectorTopKBatchPartialWorkerHeaps(t *testing.T) {
	rows := vectorTopKBatchFixture(8193, "ties")
	for row := range rows {
		if row%31 != 0 {
			rows[row] = nil
		}
	}
	cache := segmentScanCache(rows, []int{len(rows)}, false)
	distance := buildVecDistanceFunc("l2", []float64{1, 2}, 0)
	for _, k := range []int{64, 256, 5000, math.MaxInt} {
		want, err := referenceVecSegmentScan(t.Context(), rows, 0, len(rows), 2, k, cache, distance, false)
		if err != nil {
			t.Fatal(err)
		}
		results := make([]vecSearchWorkerResult, 7)
		for worker := range results {
			start, end := worker*len(rows)/len(results), (worker+1)*len(rows)/len(results)
			results[worker].heapRows, results[worker].err = vecSearchTopKRange(t.Context(), rows, start, end, 2, k, cache, distance, false)
			assertVectorTopKBatchHeap(t, results[worker].heapRows)
		}
		got, err := mergeVecSearchResults(results, k, len(rows))
		if err != nil {
			t.Fatal(err)
		}
		assertVectorTopKBatchEqual(t, got, topKFromHeap(&want, k))
	}
}
