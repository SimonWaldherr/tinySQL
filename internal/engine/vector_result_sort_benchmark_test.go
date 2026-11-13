package engine

import (
	"fmt"
	"runtime"
	"testing"
)

func BenchmarkVectorResultSort(b *testing.B) {
	for _, count := range []int{10, 64, 256, 1024, 8192} {
		for _, order := range []string{"random", "ascending", "descending", "ties", "signed-zero"} {
			fixture := vectorResultSortFixture(count, order)
			for _, legacy := range []bool{true, false} {
				b.Run(fmt.Sprintf("rows%d/%s/legacy%t", count, order, legacy), func(b *testing.B) {
					heap := make(vecScoredHeap, count)
					b.ReportAllocs()
					for b.Loop() {
						heap = heap[:count]
						copy(heap, fixture)
						var result []vecScoredRow
						if legacy {
							result = legacyVecHeapResults(&heap, count)
						} else {
							result = topKFromHeap(&heap, count)
						}
						runtime.KeepAlive(result)
						heap = result
					}
				})
			}
		}
	}
}
