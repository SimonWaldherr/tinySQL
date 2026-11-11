package engine

import (
	"fmt"
	"testing"
)

func BenchmarkVectorParallelHeapMerge(b *testing.B) {
	cache := filteredVectorHeapFixture(8192)
	allowed := make([]int, 4096)
	for i := range allowed {
		allowed[i] = i * 2
	}
	query := []float64{1, 0, 0}
	distance := buildVecDistanceFunc("l2", query, 0)
	for _, filtered := range []bool{false, true} {
		for _, k := range []int{20, 400, 10000} {
			b.Run(fmt.Sprintf("filtered%v_k%d", filtered, k), func(b *testing.B) {
				ctx := b.Context()
				b.ReportAllocs()
				for b.Loop() {
					var got []vecScoredRow
					var err error
					if filtered {
						got, err = ragVecTopKAllowed(ctx, allowed, len(query), k, cache, distance, false)
					} else {
						got, err = vecSearchTopK(ctx, cache.table.Rows, len(query), k, cache, distance, false)
					}
					if err != nil || len(got) == 0 || len(got) > k {
						b.Fatalf("merge: %d / %v", len(got), err)
					}
				}
			})
		}
	}
}

func BenchmarkVectorIVFHeapCapacity(b *testing.B) {
	cache := filteredVectorHeapFixture(48)
	query := []float64{1, 0, 0}
	idx, err := buildVecIVFIndex(b.Context(), cache.table, "l2", len(query), cache)
	if err != nil {
		b.Fatal(err)
	}
	for _, k := range []int{20, 100000} {
		b.Run(fmt.Sprint(k), func(b *testing.B) {
			ctx := b.Context()
			b.ReportAllocs()
			for b.Loop() {
				if rows, err := idx.search(ctx, query, 0, k, cache); err != nil || len(rows) != min(k, 40) {
					b.Fatalf("IVF: %d / %v", len(rows), err)
				}
			}
		})
	}
}
