package engine

import (
	"math"
	"testing"
	"unsafe"
)

func TestVecVisitedBitmapMatchesDense(t *testing.T) {
	for _, rows := range []int{1, 63, 64, 65, 257, 10000} {
		dense := &vecVisitedSet{}
		dense.ensure(rows)
		bitmap := &vecVisitedSet{}
		bitmap.ensureBitmap(rows)
		for traversal := range 4 {
			if traversal == 2 {
				dense.epoch = math.MaxUint32
			}
			dense.reset()
			bitmap.reset()
			for i := range rows + 2 {
				row := (i*7919 + traversal*13) % rows
				for range 2 {
					if got, want := bitmap.visit(row), dense.visit(row); got != want {
						t.Fatalf("rows=%d traversal=%d row=%d: bitmap=%t dense=%t", rows, traversal, row, got, want)
					}
				}
			}
			for _, row := range []int{-1, rows, rows + 63, math.MaxInt} {
				if bitmap.visit(row) || dense.visit(row) {
					t.Fatalf("rows=%d accepted out-of-range row %d", rows, row)
				}
			}
			bitmap.reset()
			if len(bitmap.touched) != 0 {
				t.Fatal("reset retained dirty words")
			}
			for _, word := range bitmap.words {
				if word != 0 {
					t.Fatal("reset left visited bits")
				}
			}
		}
	}
}

func TestVecVisitedBitmapGrowthAndModeTransitions(t *testing.T) {
	visited := &vecVisitedSet{}
	visited.ensureBitmap(128)
	visited.reset()
	if !visited.visit(127) || len(visited.touched) != 1 {
		t.Fatal("first word was not recorded")
	}
	visited.ensureBitmap(129)
	if visited.visit(127) || !visited.visit(128) || visited.visit(129) {
		t.Fatal("growth within capacity lost marks or exposed padding bits")
	}
	// A new bitmap allocation starts with no marks, like dense-array growth.
	rows := cap(visited.words)*64 + 1
	visited.ensureBitmap(rows)
	if len(visited.touched) != 0 || !visited.visit(127) || !visited.visit(rows-1) {
		t.Fatal("bitmap reallocation did not start with fresh marks")
	}
	visited.ensure(3)
	visited.reset()
	if visited.words != nil || visited.touched != nil || visited.rows != 0 || !visited.visit(2) || visited.visit(3) {
		t.Fatal("small corpus did not return to bounded dense marks")
	}
	const maxDenseRows = vecVisitedMaxBytes / 4
	visited.ensure(maxDenseRows)
	if cap(visited.marks) != maxDenseRows {
		t.Fatal("dense growth slack exceeds the retention budget")
	}
	visited.resetForPool()
	if visited.marks == nil {
		t.Fatal("dense marks at the budget were discarded")
	}
	visited.ensure(maxDenseRows + 1)
	visited.reset()
	if visited.marks != nil || visited.words == nil || !visited.visit(maxDenseRows) || visited.visit(maxDenseRows+1) {
		t.Fatal("large corpus did not switch to a bounded bitmap")
	}
	visited.resetForPool()
	if visited.words == nil {
		t.Fatal("large corpus bitmap was unnecessarily discarded")
	}
	visited.reset()
	if !visited.visit(maxDenseRows) {
		t.Fatal("pooled bitmap did not reset its marks")
	}
}

func TestVecVisitedBitmapRetentionBudgetIncludesDirtyWords(t *testing.T) {
	const intBytes = int(unsafe.Sizeof(int(0)))
	const words = (vecVisitedMaxBytes - 2*intBytes) / 8
	visited := &vecVisitedSet{
		words:   make([]uint64, 1, words),
		touched: make([]int, 0, 2),
		rows:    64,
	}
	visited.resetForPool()
	if visited.words == nil || visited.touched == nil {
		t.Fatal("bitmap exactly at the combined budget was discarded")
	}
	visited.touched = make([]int, 0, 3)
	visited.resetForPool()
	if visited.words != nil || visited.touched != nil || visited.marks != nil || visited.rows != 0 || visited.epoch != 0 {
		t.Fatal("oversized combined workspace retained its arrays or bounds")
	}
	visited.ensureBitmap(65)
	visited.reset()
	if !visited.visit(64) || visited.visit(65) {
		t.Fatal("discarded bitmap wrapper could not be reused")
	}
}

func TestVecVisitedBitmapGreedyTraversalMatchesDense(t *testing.T) {
	idx, cache := greedyHNSWFixture(48, 8)
	for _, metric := range []string{"cosine", "l2", "manhattan", "dot"} {
		query := []float64{1, 0, 0, 1, 0, 1, 0, 0}
		scorer := newVecRowScorer(metric, query, vectorL2Norm(query), newVecRowResolver(&cache, metricNeedsNorms(metric)))
		dense, bitmap := &vecVisitedSet{}, &vecVisitedSet{}
		dense.ensure(48)
		bitmap.ensureBitmap(48)
		for entry := range 48 {
			distance, ok := scorer.distance(entry)
			if !ok {
				continue
			}
			dense.reset()
			bitmap.reset()
			dense.visit(entry)
			bitmap.visit(entry)
			wantRow, gotRow := entry, entry
			wantDist, gotDist := distance, distance
			for layer := idx.maxLevel; layer >= 0; layer-- {
				wantRow, wantDist = idx.greedyClosest(&scorer, wantRow, wantDist, layer, dense)
				gotRow, gotDist = idx.greedyClosest(&scorer, gotRow, gotDist, layer, bitmap)
				if gotRow != wantRow || math.Float64bits(gotDist) != math.Float64bits(wantDist) {
					t.Fatalf("metric=%s entry=%d layer=%d: bitmap=(%d,%g) dense=(%d,%g)", metric, entry, layer, gotRow, gotDist, wantRow, wantDist)
				}
			}
		}
	}
}
