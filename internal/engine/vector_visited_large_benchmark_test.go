package engine

import (
	"fmt"
	"testing"
)

// Uses the existing visited API so the same workload can be copied to an
// earlier checkout. A traversal touches only a small fraction of the corpus.
func BenchmarkVecVisitedLargeCorpus(b *testing.B) {
	for _, rows := range []int{10000, 3000000, 10000000} {
		b.Run(fmt.Sprint(rows), func(b *testing.B) {
			var nodes [1000]int
			for i := range nodes {
				nodes[i] = ((i + 1) * 7919) % rows
			}
			visit := func() {
				visited := acquireVisited(rows)
				visited.reset()
				for _, row := range nodes {
					if !visited.visit(row) || visited.visit(row) {
						b.Fatal("visited set did not deduplicate a traversal")
					}
				}
				releaseVisited(visited)
			}
			visit()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				visit()
			}
		})
	}
}
