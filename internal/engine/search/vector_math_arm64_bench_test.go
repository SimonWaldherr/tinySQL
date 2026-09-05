//go:build arm64

package search

import (
	"fmt"
	"math"
	"testing"
)

func BenchmarkVectorDotNEONBySize(b *testing.B) {
	for _, dims := range []int{32, 64, 96, 128} {
		a, vecB := makeVectorMathBenchmarkInputs(dims)
		b.Run(fmt.Sprintf("unrolled/%d", dims), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				vectorMathBenchmarkSink = VectorDotUnrolled(a, vecB)
			}
		})
		b.Run(fmt.Sprintf("neon/%d", dims), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				vectorMathBenchmarkSink = vectorDotNEON(a, vecB)
			}
		})
	}
}

func TestVectorDotNEONMatchesUnrolled(t *testing.T) {
	for _, dims := range []int{0, 1, 7, 8, 31, 32, 64, 96, 127, 128, 768} {
		a, vecB := makeVectorMathBenchmarkInputs(dims)
		got, want := vectorDotNEON(a, vecB), VectorDotUnrolled(a, vecB)
		if math.Abs(got-want) > 1e-12*math.Max(1, math.Abs(want)) {
			t.Fatalf("dims=%d: NEON=%g, unrolled=%g", dims, got, want)
		}
	}
}
