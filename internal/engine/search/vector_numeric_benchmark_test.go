package search

import (
	"fmt"
	"math"
	"testing"
)

// The controls retain the former arithmetic while using the same SIMD
// kernels, so these comparisons isolate the scalar work around each kernel.
func BenchmarkVectorNumeric(b *testing.B) {
	for _, dims := range []int{64, 768} {
		a, v := makeVectorMathBenchmarkInputs(dims)
		out := make([]float64, dims)
		b.Run(fmt.Sprint(dims), func(b *testing.B) {
			b.Run("norm/control", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					vectorMathBenchmarkSink = math.Sqrt(VectorDot(a, a))
				}
			})
			b.Run("norm/stable", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					vectorMathBenchmarkSink = VectorNorm(a)
				}
			})
			b.Run("normalize/control", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					norm := math.Sqrt(VectorDot(a, a))
					for j, x := range a {
						out[j] = x / norm
					}
				}
				vectorMathBenchmarkSink = out[0]
			})
			b.Run("normalize/reciprocal", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					VectorNormalize(out, a)
				}
				vectorMathBenchmarkSink = out[0]
			})
			b.Run("cosine/control", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					dot, an, bn := VectorCosineParts(a, v)
					vectorMathBenchmarkSink = dot / (math.Sqrt(an) * math.Sqrt(bn))
				}
			})
			b.Run("cosine/single_sqrt", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					vectorMathBenchmarkSink, _ = VectorCosine(a, v)
				}
			})
			an, bn := VectorNorm(a), VectorNorm(v)
			b.Run("cached_cosine/control", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					vectorMathBenchmarkSink = VectorDot(a, v) / (an * bn)
				}
			})
			b.Run("cached_cosine/stable", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					vectorMathBenchmarkSink, _ = VectorCosineWithNorms(a, v, an, bn)
				}
			})
		})
	}
}
