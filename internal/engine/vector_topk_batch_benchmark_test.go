package engine

import (
	"fmt"
	"math"
	"runtime"
	"testing"
)

func BenchmarkVectorTopKBatch(b *testing.B) {
	const count = 8192
	for _, order := range []string{"random", "ascending", "descending", "ties"} {
		rows := vectorTopKBatchFixture(count, order)
		cache := segmentScanCache(rows, []int{count}, false)
		distance := buildVecDistanceFunc("l2", []float64{1, 2}, 0)
		for _, k := range []int{1, 10, 64, 256, 1024, count, math.MaxInt} {
			b.Run(fmt.Sprintf("%s/k%d", order, k), func(b *testing.B) {
				ctx := b.Context()
				b.ReportAllocs()
				for b.Loop() {
					heap, err := vecSearchTopKRange(ctx, rows, 0, count, 2, k, cache, distance, false)
					if err != nil {
						b.Fatal(err)
					}
					result := topKFromHeap(&heap, k)
					if len(result) != min(k, count) {
						b.Fatal("missing results")
					}
					runtime.KeepAlive(result)
				}
			})
		}
	}
}

func BenchmarkVectorTopKBatchLayouts(b *testing.B) {
	const count = 8192
	for _, norms := range []bool{false, true} {
		for _, layout := range []string{"contiguous", "segments", "overrides", "mixed-dimensions"} {
			rows := vectorTopKBatchFixture(count, "random")
			if layout == "mixed-dimensions" {
				for row := 0; row < count; row += 7 {
					rows[row] = []any{[]float64{1, 2, 3}}
				}
			}
			boundaries := []int{count}
			if layout == "segments" {
				boundaries = []int{1024, 2048, 4096, count}
			}
			cache := segmentScanCache(rows, boundaries, norms)
			if layout == "overrides" {
				cache.overrides = map[int]vecColumnOverride{0: {vector: []float64{2, 1}, norm: math.Sqrt(5), valid: true}}
			}
			query := []float64{1, 2}
			distance := buildVecDistanceFunc("cosine", query, vectorL2Norm(query))
			for _, k := range []int{10, 64, 1024, math.MaxInt} {
				b.Run(fmt.Sprintf("%s/norms%t/k%d", layout, norms, k), func(b *testing.B) {
					ctx := b.Context()
					b.ReportAllocs()
					for b.Loop() {
						heap, err := vecSearchTopKRange(ctx, rows, 0, count, 2, k, cache, distance, true)
						if err != nil {
							b.Fatal(err)
						}
						runtime.KeepAlive(topKFromHeap(&heap, k))
					}
				})
			}
		}
	}
}
