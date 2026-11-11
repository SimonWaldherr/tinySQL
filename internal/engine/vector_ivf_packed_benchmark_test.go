package engine

import (
	"fmt"
	"runtime"
	"testing"
)

func BenchmarkIVFPackedBuild(b *testing.B) {
	for _, shape := range [][2]int{{32768, 32768}, {1000000, 1000}, {1000000, 0}} {
		b.Run(fmt.Sprintf("rows%d/valid%d", shape[0], shape[1]), func(b *testing.B) {
			table, cache := ivfPackedFixture(shape[0], shape[1], 8)
			b.ReportAllocs()
			for b.Loop() {
				idx, err := buildVecIVFIndex(b.Context(), table, "cosine", 8, cache)
				if err != nil {
					b.Fatal(err)
				}
				runtime.KeepAlive(idx)
			}
		})
	}
}

func BenchmarkIVFValidRows(b *testing.B) {
	for _, shape := range [][2]int{{1000000, 1000000}, {1000000, 1000}, {1000000, 0}} {
		b.Run(fmt.Sprintf("rows%d/valid%d", shape[0], shape[1]), func(b *testing.B) {
			_, cache := ivfPackedFixture(shape[0], shape[1], 2)
			b.ReportAllocs()
			for b.Loop() {
				rows := validVectorRows(cache, 2)
				runtime.KeepAlive(rows)
			}
		})
	}
}
