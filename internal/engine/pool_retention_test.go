package engine

import (
	"errors"
	"fmt"
	"math"
	"testing"
	"unsafe"
)

func TestRawRowPoolRetention(t *testing.T) {
	if resetPooledRow(nil) {
		t.Fatal("nil map should not enter the pool")
	}
	for _, keys := range []int{0, rawRowPoolMaxKeys, rawRowPoolMaxKeys + 1} {
		t.Run(fmt.Sprint(keys), func(t *testing.T) {
			row := make(Row, keys)
			for i := range keys {
				row[fmt.Sprint(i)] = &i
			}
			if retain := resetPooledRow(row); retain != (keys <= rawRowPoolMaxKeys) {
				t.Fatalf("retain=%t for %d keys", retain, keys)
			}
			if len(row) != 0 {
				t.Fatalf("released map retains %d values", len(row))
			}
		})
	}
}

func TestPooledProjectionErrorAfterWideRow(t *testing.T) {
	wantErr := errors.New("projection failed")
	plan := &simpleSelectPlan{}
	for i := range rawRowPoolMaxKeys + 1 {
		plan.projs = append(plan.projs, simpleProjection{key: fmt.Sprint(i), colIdx: 0})
	}
	plan.projs = append(plan.projs, simpleProjection{
		colIdx: -1,
		value:  func([]any) (any, error) { return nil, wantErr },
	})
	if row, err := projectRawRowPooled(plan, []any{"value"}); row != nil || !errors.Is(err, wantErr) {
		t.Fatalf("projection returned row=%v err=%v", row, err)
	}
	// A subsequent projection must be independent of the partial result.
	plan.projs = []simpleProjection{{key: "only", altKey: "t.only", colIdx: 0}}
	row, err := projectRawRowPooled(plan, []any{"fresh"})
	if err != nil || len(row) != 2 || row["only"] != "fresh" || row["t.only"] != "fresh" {
		t.Fatalf("projection after error returned row=%v err=%v", row, err)
	}
	releasePooledRow(row)
}

func TestHNSWScratchRetention(t *testing.T) {
	rowBytes := int(unsafe.Sizeof(vecScoredRow{}))
	maxRows := vecHNSWScratchMaxBytes / rowBytes
	backing := make([]vecScoredRow, maxRows+1)
	// Divide one backing array among every scratch field with explicit
	// capacities, so the limit applies to their sum rather than each slice.
	fixture := func(rows int) *vecHNSWScratch {
		part := rows / 5
		return &vecHNSWScratch{
			candidates:    backing[:part:part],
			results:       backing[part : part*2 : part*2],
			selected:      backing[part*2 : part*3 : part*3],
			pruneScored:   backing[part*3 : part*4 : part*4],
			pruneSelected: backing[part*4 : rows : rows],
		}
	}
	scratch := fixture(maxRows)
	wantCap := []int{cap(scratch.candidates), cap(scratch.results), cap(scratch.selected), cap(scratch.pruneScored), cap(scratch.pruneSelected)}
	scratch.resetForPool()
	slices := [][]vecScoredRow{scratch.candidates, scratch.results, scratch.selected, scratch.pruneScored, scratch.pruneSelected}
	for i, slice := range slices {
		if len(slice) != 0 || cap(slice) != wantCap[i] {
			t.Fatalf("retained slice %d: len=%d cap=%d, want len=0 cap=%d", i, len(slice), cap(slice), wantCap[i])
		}
	}
	scratch = fixture(maxRows + 1)
	scratch.resetForPool()
	if scratch.candidates != nil || scratch.results != nil || scratch.selected != nil || scratch.pruneScored != nil || scratch.pruneSelected != nil {
		t.Fatal("oversized scratch retained backing arrays")
	}
	// The same wrapper can service the next small traversal.
	scratch.candidates = append(scratch.candidates, vecScoredRow{rowIdx: 3})
	scratch.resetForPool()
	if len(scratch.candidates) != 0 || cap(scratch.candidates) == 0 {
		t.Fatal("small scratch was not reusable after dropping large arrays")
	}
}

func TestVisitedScratchRetention(t *testing.T) {
	const maxMarks = vecVisitedMaxBytes / 4
	backing := make([]uint32, maxMarks+1)
	visited := &vecVisitedSet{marks: backing[:1:maxMarks], epoch: 7}
	visited.marks[0] = visited.epoch
	visited.resetForPool()
	if cap(visited.marks) != maxMarks || visited.epoch != 7 || visited.visit(0) {
		t.Fatal("in-budget marks or their epoch were discarded")
	}
	visited.marks = backing[: 1 : maxMarks+1]
	visited.resetForPool()
	if visited.marks != nil || visited.epoch != 0 {
		t.Fatal("oversized visited scratch retained its marks or epoch")
	}
	visited.ensure(3)
	visited.reset()
	if !visited.visit(2) || visited.visit(2) {
		t.Fatal("discarded visited wrapper did not restart with fresh marks")
	}
}

func TestRouteScratchRetention(t *testing.T) {
	scratch := &routeSearchScratch{dist: make([]float64, 1, routeScratchHotMaxBytes/8)}
	if !scratch.resetForPool() || routeSearchScratchBytes(scratch) != routeScratchHotMaxBytes {
		t.Fatal("scratch at the retention limit was discarded")
	}
	// Include every optional buffer to check that dropping an oversized
	// instance releases all of them, regardless of their current lengths.
	scratch.heuristic = make([]float64, 0, 1)
	scratch.prevNode = make([]int, 0, 1)
	scratch.prevEdge = make([]int, 0, 1)
	scratch.generation = make([]uint32, 0, 1)
	scratch.heap = make(routeHeap, 1)
	scratch.mark = 9
	if scratch.resetForPool() || routeSearchScratchBytes(scratch) != 0 || scratch.mark != 0 {
		t.Fatal("oversized route scratch retained arrays or generation")
	}
	scratch.dist = make([]float64, 2)
	scratch.generation = make([]uint32, 2)
	scratch.heap = make(routeHeap, 1, 4)
	scratch.nextGeneration()
	scratch.setDistance(1, 3)
	if !scratch.resetForPool() || cap(scratch.heap) != 4 || len(scratch.heap) != 0 || scratch.distance(1, math.Inf(1)) != 3 {
		t.Fatal("small route scratch was not reusable after dropping large arrays")
	}
}

func TestRouteScratchBytesIncludesAllCapacities(t *testing.T) {
	scratch := &routeSearchScratch{
		dist:       make([]float64, 0, 1),
		heuristic:  make([]float64, 0, 2),
		prevNode:   make([]int, 0, 3),
		prevEdge:   make([]int, 0, 4),
		generation: make([]uint32, 0, 5),
		heap:       make(routeHeap, 0, 6),
	}
	want := uint64(3*8 + 7*unsafe.Sizeof(int(0)) + 5*4 + 6*unsafe.Sizeof(routeHeapEntry{}))
	if got := routeSearchScratchBytes(scratch); got != want {
		t.Fatalf("retained bytes=%d, want %d", got, want)
	}
}

func TestRouteGenerationWrapClearsUnusedCapacity(t *testing.T) {
	scratch := &routeSearchScratch{
		dist:       []float64{0, 0, 123},
		generation: []uint32{1, 1, 2},
		mark:       math.MaxUint32,
	}
	scratch.generation = scratch.generation[:1]
	scratch.nextGeneration()
	if scratch.mark != 1 {
		t.Fatalf("wrapped mark=%d, want 1", scratch.mark)
	}
	for i, mark := range scratch.generation[:cap(scratch.generation)] {
		if mark != 0 {
			t.Fatalf("generation[%d]=%d after wrap", i, mark)
		}
	}
	scratch.generation = scratch.generation[:3]
	scratch.nextGeneration()
	if got := scratch.distance(2, math.Inf(1)); !math.IsInf(got, 1) {
		t.Fatalf("old generation became visible after regrowth: distance=%v", got)
	}
}
