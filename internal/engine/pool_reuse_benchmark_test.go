package engine

import "testing"

// Use the existing acquire/release entry points so these benchmarks can also
// be copied to an earlier checkout when comparing pool policy changes.
func BenchmarkPooledProjectionReuse(b *testing.B) {
	plan := &simpleSelectPlan{projs: []simpleProjection{
		{key: "id", colIdx: 0},
		{key: "name", altKey: "t.name", colIdx: 1},
	}}
	raw := []any{int64(7), "document"}
	row, err := projectRawRowPooled(plan, raw)
	if err != nil {
		b.Fatal(err)
	}
	releasePooledRow(row)
	b.ReportAllocs()
	for b.Loop() {
		row, err := projectRawRowPooled(plan, raw)
		if err != nil {
			b.Fatal(err)
		}
		releasePooledRow(row)
	}
}

func BenchmarkHNSWScratchReuse(b *testing.B) {
	use := func() {
		scratch := acquireHNSWScratch()
		scratch.candidates = append(scratch.candidates[:0], vecScoredRow{rowIdx: 1})
		scratch.results = append(scratch.results[:0], vecScoredRow{rowIdx: 1})
		scratch.selected = append(scratch.selected[:0], vecScoredRow{rowIdx: 1})
		scratch.pruneScored = append(scratch.pruneScored[:0], vecScoredRow{rowIdx: 1})
		scratch.pruneSelected = append(scratch.pruneSelected[:0], vecScoredRow{rowIdx: 1})
		releaseHNSWScratch(scratch)
	}
	use()
	b.ReportAllocs()
	for b.Loop() {
		use()
	}
}

func BenchmarkVisitedScratchReuse(b *testing.B) {
	visited := acquireVisited(10000)
	releaseVisited(visited)
	b.ReportAllocs()
	for b.Loop() {
		visited := acquireVisited(10000)
		visited.reset()
		visited.visit(123)
		releaseVisited(visited)
	}
}

func BenchmarkRouteScratchReuse(b *testing.B) {
	scratch := acquireRouteSearchScratch(1024, true)
	releaseRouteSearchScratch(scratch)
	b.ReportAllocs()
	for b.Loop() {
		scratch := acquireRouteSearchScratch(1024, true)
		scratch.setDistance(123, 42)
		routeHeapPush(&scratch.heap, routeHeapEntry{node: 123, cost: 42})
		releaseRouteSearchScratch(scratch)
	}
}
