package engine

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func BenchmarkIVFPrunedCentroid(b *testing.B) {
	for _, dims := range []int{256, 384, 768, 1536} {
		for _, clustered := range []bool{false, true} {
			query, centroids, hint := ivfPruneFixture(dims, 64, clustered)
			for _, pruned := range []bool{false, true} {
				b.Run(fmt.Sprintf("dims%d/clustered%t/pruned%t", dims, clustered, pruned), func(b *testing.B) {
					b.ReportAllocs()
					for b.Loop() {
						var got int
						if pruned {
							got = nearestCentroidL2Pruned(query, centroids, hint)
						} else {
							got = nearestCentroidL2(query, centroids)
						}
						runtime.KeepAlive(got)
					}
				})
			}
		}
	}
}

func BenchmarkIVFPrunedBuild(b *testing.B) {
	for _, dims := range []int{256, 384, 768, 1536} {
		for _, distribution := range []string{"gaussian", "settled", "periodic", "shuffled"} {
			b.Run(fmt.Sprintf("dims%d/%s", dims, distribution), func(b *testing.B) {
				var table *storage.Table
				var cache vecSearchColumnCacheEntry
				if distribution == "gaussian" || distribution == "settled" {
					table, cache = ivfConvergenceFixture(4096, dims, distribution == "settled")
				} else {
					table, cache = ivfSeedQualityFixture(distribution, 4096, dims)
				}
				ctx := b.Context()
				b.ReportAllocs()
				for b.Loop() {
					idx, err := buildVecIVFIndex(ctx, table, "l2", dims, cache)
					if err != nil {
						b.Fatal(err)
					}
					runtime.KeepAlive(idx)
				}
			})
		}
	}
}
