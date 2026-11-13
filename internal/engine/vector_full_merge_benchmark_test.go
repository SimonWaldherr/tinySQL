package engine

import (
	"fmt"
	"runtime"
	"testing"
)

func BenchmarkVectorFullMerge(b *testing.B) {
	for _, count := range []int{256, 1024, 8192} {
		for _, workers := range []int{2, 8} {
			for _, order := range []string{"random", "descending", "ties"} {
				fixture := vectorFullMergeFixture(count, workers, order)
				for _, legacy := range []bool{true, false} {
					b.Run(fmt.Sprintf("rows%d/workers%d/%s/legacy%t", count, workers, order, legacy), func(b *testing.B) {
						results := cloneVecWorkerResults(fixture)
						buffers := cloneVecWorkerResults(fixture)
						b.ReportAllocs()
						for b.Loop() {
							for worker := range results {
								results[worker].heapRows = buffers[worker].heapRows
								copy(results[worker].heapRows, fixture[worker].heapRows)
							}
							var rows []vecScoredRow
							var err error
							if legacy {
								rows, err = legacyVecMerge(results, count, count)
							} else {
								rows, err = mergeVecSearchResults(results, count, count)
							}
							if err != nil || len(rows) != count {
								b.Fatalf("merge: %d / %v", len(rows), err)
							}
							runtime.KeepAlive(rows)
						}
					})
				}
			}
		}
	}
}
