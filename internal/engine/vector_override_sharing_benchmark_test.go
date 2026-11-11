package engine

import (
	"fmt"
	"runtime"
	"testing"
)

// Reuses the existing production entry points so this fixture can also run
// on the previous revision. Each operation creates an independent append view
// of the same immutable base, excluding table growth from the comparison.
func BenchmarkVecColumnAppendWithOverrides(b *testing.B) {
	for _, overrides := range []int{0, 1024} {
		for _, mode := range []string{"no-norms", "cached-norms", "norm-upgrade"} {
			b.Run(fmt.Sprintf("overrides=%d/%s", overrides, mode), func(b *testing.B) {
				norms := mode == "cached-norms"
				includeNorms := mode != "no-norms"
				table, base := overrideSharingFixture(16384, overrides, 64, norms)
				vector := make([]float64, 64)
				for d := range vector {
					vector[d] = float64(d + 1)
				}
				table.Rows = append(table.Rows, []any{vector})
				table.Version++
				b.ReportAllocs()
				for b.Loop() {
					extended := extendVecColumnCache(base, table, 0, includeNorms)
					runtime.KeepAlive(extended)
				}
			})
		}
	}
}
