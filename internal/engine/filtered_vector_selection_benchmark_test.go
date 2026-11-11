package engine

import (
	"fmt"
	"math"
	"testing"
)

func BenchmarkFilteredANNSelection(b *testing.B) {
	idx, cache, allowed := filteredANNSelectionFixture(b, 1280, 64, "cosine")
	query := make([]float64, 64)
	for d := range query {
		query[d] = math.Cos(float64(d) * 0.4)
	}
	queryNorm := vectorL2Norm(query)
	for _, k := range []int{1, 20, 100, 400, 1000} {
		b.Run(fmt.Sprintf("k%d", k), func(b *testing.B) {
			want, err := legacyFilteredANNSelection(b.Context(), idx, query, queryNorm, k, cache, allowed)
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				got, err := idx.searchFiltered(b.Context(), query, queryNorm, k, cache, allowed)
				if err != nil || len(got) != len(want) {
					b.Fatalf("selection count: %d / %v, want %d", len(got), err, len(want))
				}
			}
		})
	}
}
