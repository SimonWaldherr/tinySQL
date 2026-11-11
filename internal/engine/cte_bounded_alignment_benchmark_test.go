package engine

import (
	"fmt"
	"testing"
)

func BenchmarkRecursiveCTEBoundedFanout(b *testing.B) {
	db := setupBoundedRecursiveCTE(b, 20000)
	for _, tc := range []struct{ name, tail string }{
		{"Bounded", "LIMIT 8 OFFSET 2"},
		{"UnboundedControl", ""},
	} {
		b.Run(tc.name, func(b *testing.B) {
			runBench(b, db, fmt.Sprintf(boundedRecursiveCTEQuery, tc.tail))
		})
	}
}
