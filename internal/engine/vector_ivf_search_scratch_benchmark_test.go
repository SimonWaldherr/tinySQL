package engine

import (
	"fmt"
	"runtime"
	"testing"
)

func BenchmarkIVFSearchScratch(b *testing.B) {
	for _, nlist := range []int{4, 32, 64, 65, 129} {
		for _, metric := range []string{"cosine", "l2"} {
			b.Run(fmt.Sprintf("lists%d/%s", nlist, metric), func(b *testing.B) {
				idx, cache := ivfSearchScratchFixture(nlist, metric)
				query := []float64{1, 2}
				queryNorm := vectorL2Norm(query)
				ctx := b.Context()
				b.ReportAllocs()
				for b.Loop() {
					got, err := idx.search(ctx, query, queryNorm, 10, cache)
					if err != nil {
						b.Fatal(err)
					}
					runtime.KeepAlive(got)
				}
			})
		}
	}
}
