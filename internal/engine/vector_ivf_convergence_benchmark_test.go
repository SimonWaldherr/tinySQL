package engine

import (
	"fmt"
	"runtime"
	"testing"
)

func BenchmarkIVFConvergence(b *testing.B) {
	for _, settled := range []bool{true, false} {
		for _, dims := range []int{8, 64, 384} {
			for _, metric := range []string{"cosine", "l2"} {
				b.Run(fmt.Sprintf("settled%t/dims%d/%s", settled, dims, metric), func(b *testing.B) {
					table, cache := ivfConvergenceFixture(4096, dims, settled)
					b.ReportAllocs()
					for b.Loop() {
						idx, err := buildVecIVFIndex(b.Context(), table, metric, dims, cache)
						if err != nil {
							b.Fatal(err)
						}
						runtime.KeepAlive(idx)
					}
				})
			}
		}
	}
}
