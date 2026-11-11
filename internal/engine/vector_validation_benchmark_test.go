package engine

import (
	"fmt"
	"testing"
)

// Both workloads use existing APIs so the same fixture can run before/after.
func BenchmarkVectorRowValidity(b *testing.B) {
	for _, parts := range []int{1, 8} {
		for _, overrides := range []bool{false, true} {
			b.Run(fmt.Sprintf("segments=%d/overrides=%t", parts, overrides), func(b *testing.B) {
				_, _, cache := vectorValidationFixture(100000, parts, overrides)
				b.ReportAllocs()
				for b.Loop() {
					count := 0
					for row := 0; row < cache.rowCount(); row++ {
						if validCacheRow(cache, row, 4) {
							count++
						}
					}
					if count != cache.rowCount() {
						b.Fatal("validity scan lost vectors")
					}
				}
			})
		}
	}
}

func BenchmarkPersistentVectorValidation(b *testing.B) {
	for _, parts := range []int{1, 8} {
		for _, overrides := range []bool{false, true} {
			b.Run(fmt.Sprintf("segments=%d/overrides=%t", parts, overrides), func(b *testing.B) {
				table, index, cache := vectorValidationFixture(100000, parts, overrides)
				b.ReportAllocs()
				for b.Loop() {
					if !usablePersistentVecHNSW(index, table, 0, "cosine", 4, cache) {
						b.Fatal("persisted graph lost eligibility")
					}
				}
			})
		}
	}
}
