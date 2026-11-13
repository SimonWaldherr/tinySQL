package engine

import (
	"fmt"
	"runtime"
	"testing"
)

func BenchmarkVectorSparseMerge(b *testing.B) {
	for _, active := range []int{1, 2} {
		for _, spareCapacity := range []bool{false, true} {
			b.Run(fmt.Sprintf("active%d/spare%t", active, spareCapacity), func(b *testing.B) {
				fixture := vectorFullMergeFixture(4096, 4, "random")
				for worker := range fixture {
					if worker == 0 || worker > active {
						fixture[worker].heapRows = nil
					} else if spareCapacity {
						heap := make(vecScoredHeap, len(fixture[worker].heapRows), 2048)
						copy(heap, fixture[worker].heapRows)
						fixture[worker].heapRows = heap
					}
				}
				results := cloneVecWorkerResults(fixture)
				buffers := cloneVecWorkerResults(fixture)
				b.ReportAllocs()
				for b.Loop() {
					for worker := range results {
						results[worker].heapRows = buffers[worker].heapRows
						copy(results[worker].heapRows, fixture[worker].heapRows)
					}
					rows, err := mergeVecSearchResults(results, 4096, 4096)
					if err != nil || len(rows) != active*1024 {
						b.Fatalf("merge: %d / %v", len(rows), err)
					}
					runtime.KeepAlive(rows)
				}
			})
		}
	}
}
