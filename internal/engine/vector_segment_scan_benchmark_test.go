package engine

import (
	"context"
	"fmt"
	"math"
	"testing"
)

// Uses pre-existing cache/scan entry points so the same fixture also runs on
// the prior revision. Single-segment and override cases are controls.
func BenchmarkVecSegmentedFlatScan(b *testing.B) {
	const count = 65536
	for _, dims := range []int{8, 64} {
		rows := make([][]any, count)
		query := make([]float64, dims)
		for i := range query {
			query[i] = math.Cos(float64(i) * 0.2)
		}
		for i := range rows {
			vector := make([]float64, dims)
			for d := range vector {
				vector[d] = math.Sin(float64(i)*0.3 + float64(d)*0.2)
			}
			rows[i] = []any{vector}
		}
		for _, parts := range []int{1, 8} {
			cache := vecSearchColumnCacheEntry{rows: count, normsReady: true}
			for part := range parts {
				from, to := part*count/parts, (part+1)*count/parts
				cache.segments = append(cache.segments, buildVecColumnSegment(rows[from:to], 0, true, from))
			}
			for _, override := range []bool{false, true} {
				if override && parts == 1 {
					continue
				}
				b.Run(fmt.Sprintf("dims=%d/segments=%d/override=%t", dims, parts, override), func(b *testing.B) {
					view := cache
					if override {
						view.overrides = map[int]vecColumnOverride{count / 2: {}}
					}
					distance := buildVecDistanceFunc("cosine", query, vectorL2Norm(query))
					b.ReportAllocs()
					for b.Loop() {
						heap, err := vecSearchTopKRange(context.Background(), rows, 0, count, dims, 20, view, distance, true)
						if err != nil || len(heap) != 20 {
							b.Fatal("unexpected scan result", err)
						}
					}
				})
			}
		}
	}
}
