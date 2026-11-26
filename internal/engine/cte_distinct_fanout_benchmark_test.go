package engine

import (
	"fmt"
	"testing"
)

func BenchmarkRecursiveCTEDistinctFanout(b *testing.B) {
	db := setupBoundedRecursiveCTE(b, 20000)
	for _, tc := range []struct{ name, n, title string }{
		{"duplicates", "f.id % 3 + 1", "'item'"},
		{"unique", "f.id", "f.label"},
	} {
		b.Run(tc.name, func(b *testing.B) {
			runBench(b, db, fmt.Sprintf(recursiveCTEDistinctFanout, tc.n, tc.title, ""))
		})
	}
}
