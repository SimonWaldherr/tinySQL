package engine

import (
	"fmt"
	"math"
	"math/rand"
	"slices"
	"testing"
)

// Preserve both full ownership transfer and the unusual partial-drain
// contract: k < len(heap) returns the k worst entries in ascending order.
func legacyVecHeapResults(heapRows *vecScoredHeap, k int) []vecScoredRow {
	if k > heapRows.Len() {
		k = heapRows.Len()
	}
	if k <= 0 {
		return nil
	}
	if k == heapRows.Len() {
		rows := []vecScoredRow(*heapRows)
		for heapRows.Len() > 0 {
			vecScoredHeapPop(heapRows)
		}
		*heapRows = nil
		return rows
	}
	rows := make([]vecScoredRow, k)
	for i := k - 1; i >= 0; i-- {
		rows[i] = vecScoredHeapPop(heapRows)
	}
	return rows
}

func vectorResultSortFixture(count int, order string) vecScoredHeap {
	rng := rand.New(rand.NewSource(1043))
	h := make(vecScoredHeap, 0, count)
	for row := range count {
		value := rng.Float64()*200 - 100
		switch order {
		case "ascending":
			value = float64(row)
		case "descending":
			value = float64(count - row)
		case "ties":
			value = float64(row % 7)
		case "duplicate-ids":
			value = float64(row % 7)
		case "signed-zero":
			value = math.Copysign(0, float64(row%2)-0.5)
		case "nonfinite":
			value = []float64{math.Inf(-1), math.Inf(1), math.NaN(), math.Float64frombits(0x7ff8000000000005)}[row%4]
		}
		id := row
		if order == "duplicate-ids" || order == "signed-zero" || order == "nonfinite" {
			id %= 5
		}
		vecScoredHeapPush(&h, vecScoredRow{rowIdx: id, distance: value})
	}
	return h
}

func assertVecResultBits(t *testing.T, got, want []vecScoredRow) {
	t.Helper()
	if len(got) != len(want) || (got == nil) != (want == nil) {
		t.Fatalf("result lengths/nil differ: %d/%t vs %d/%t", len(got), got == nil, len(want), want == nil)
	}
	for i, row := range got {
		if row.rowIdx != want[i].rowIdx || math.Float64bits(row.distance) != math.Float64bits(want[i].distance) {
			t.Fatalf("result %d: %v bits %x, want %v bits %x", i, row, math.Float64bits(row.distance), want[i], math.Float64bits(want[i].distance))
		}
	}
}

func TestVectorResultSortPreservesHeapResults(t *testing.T) {
	for _, count := range []int{0, 1, 10, 255, 256, 257, 1024} {
		for _, order := range []string{"random", "ascending", "descending", "ties", "duplicate-ids", "signed-zero", "nonfinite"} {
			t.Run(fmt.Sprintf("%s/rows%d", order, count), func(t *testing.T) {
				original := vectorResultSortFixture(count, order)
				for _, k := range []int{-1, 0, 1, count / 2, count, count + 1, math.MaxInt} {
					gotHeap, wantHeap := slices.Clone(original), slices.Clone(original)
					got, want := topKFromHeap(&gotHeap, k), legacyVecHeapResults(&wantHeap, k)
					assertVecResultBits(t, got, want)
					assertVecResultBits(t, gotHeap, wantHeap)
					if k >= count && count > 0 {
						if gotHeap != nil {
							t.Fatal("consumed heap retained storage")
						}
						before := slices.Clone(got)
						for i := range count + 10 {
							vecScoredHeapPush(&gotHeap, vecScoredRow{rowIdx: i, distance: -999})
						}
						assertVecResultBits(t, got, before)
					}
				}
			})
		}
	}
}
