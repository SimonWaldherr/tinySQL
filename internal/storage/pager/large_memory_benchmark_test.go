package pager

import (
	"fmt"
	"runtime"
	"testing"
)

func BenchmarkPageBufferPoolStartup(b *testing.B) {
	for _, limit := range []int{1024, 131072} {
		b.Run(fmt.Sprintf("pages=%d", limit), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				pool := newPageBufferPool(limit)
				runtime.KeepAlive(pool)
			}
		})
	}
}

func BenchmarkMarshalRowEmbeddingBuffers(b *testing.B) {
	for _, dims := range []int{768, 1536} {
		vector := make([]float64, dims)
		for i := range vector {
			vector[i] = float64(i) * 0.001
		}
		row := []any{float64(1), "doc", vector}
		b.Run(fmt.Sprintf("dims=%d/fresh", dims), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				encoded := MarshalRow(row, nil)
				runtime.KeepAlive(encoded)
			}
		})
		b.Run(fmt.Sprintf("dims=%d/reused", dims), func(b *testing.B) {
			buf := MarshalRow(row, nil)
			b.ReportAllocs()
			for b.Loop() {
				buf = MarshalRow(row, buf)
			}
		})
	}
}
