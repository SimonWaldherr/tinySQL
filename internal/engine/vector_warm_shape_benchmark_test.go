package engine

import (
	"fmt"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// The helper and fixture use the existing warm API for before/after runs.
// Initial vector-cache construction and first statistics scan are excluded.
func BenchmarkVecWarmRepeatedStats(b *testing.B) {
	for _, count := range []int{100000, 1000000} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			table := storage.NewTable("warm_shape_bench", []storage.Column{{Name: "embedding", Type: storage.VectorType}}, false)
			table.Rows = make([][]any, count)
			vector := []float64{1, 2, 3, 4}
			for i := range table.Rows {
				table.Rows[i] = []any{vector}
			}
			b.Cleanup(func() { purgeVectorCachesFor("default", table.Name) })
			if _, _, _, _, _, err := warmVectorStructures(b.Context(), "default", table, 0, "l2", vecIndexFlat); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				rows, vectors, dims, distinct, excluded, err := warmVectorStructures(b.Context(), "default", table, 0, "l2", vecIndexFlat)
				if err != nil || rows != count || vectors != count || dims != 4 || distinct != 1 || excluded != 0 {
					b.Fatal("warm statistics changed", err)
				}
			}
		})
	}
}
