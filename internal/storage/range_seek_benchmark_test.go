package storage

import (
	"fmt"
	"testing"
)

// Keep index construction outside measurement, for both persisted and mutable
// representations. Each seek returns an owned, table-ordered result.
func BenchmarkRangeSeekScratch(b *testing.B) {
	for _, warm := range []bool{false, true} {
		for _, count := range []int{1, 10, 1000} {
			b.Run(fmt.Sprintf("warm%t/rows%d", warm, count), func(b *testing.B) {
				idx := coldRangeFixture(20000, false)
				if warm {
					idx.hydrate()
				}
				lo := IndexRangeBound{Value: 0, Inclusive: true}
				hi := IndexRangeBound{Value: count - 1, Inclusive: true}
				b.ReportAllocs()
				for b.Loop() {
					rows, err := (&Table{}).LookupSecondaryIndexRange(idx, []any{"a"}, lo, hi)
					if err != nil || len(rows) != count {
						b.Fatal(rows, err)
					}
				}
			})
		}
	}
}
