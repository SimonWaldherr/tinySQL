package search

import (
	"fmt"
	"math"
	"testing"
)

var vectorMathBenchmarkSink float64
var vectorMathBenchmarkIntSink int

func makeVectorMathBenchmarkInputs(dims int) ([]float64, []float64) {
	a := make([]float64, dims)
	b := make([]float64, dims)
	for i := range a {
		a[i] = math.Sin(float64(i)*0.11) * 0.75
		b[i] = math.Cos(float64(i)*0.07) * 0.5
	}
	return a, b
}

func BenchmarkVectorDot768(b *testing.B) {
	a, vecB := makeVectorMathBenchmarkInputs(768)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		vectorMathBenchmarkSink = VectorDot(a, vecB)
	}
}

func BenchmarkVectorDotUnrolled768(b *testing.B) {
	a, vecB := makeVectorMathBenchmarkInputs(768)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		vectorMathBenchmarkSink = VectorDotUnrolled(a, vecB)
	}
}

func BenchmarkVectorL2Squared768(b *testing.B) {
	a, vecB := makeVectorMathBenchmarkInputs(768)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		vectorMathBenchmarkSink = VectorL2Squared(a, vecB)
	}
}

func BenchmarkVectorL2SquaredUnrolled768(b *testing.B) {
	a, vecB := makeVectorMathBenchmarkInputs(768)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		vectorMathBenchmarkSink = VectorL2SquaredUnrolled(a, vecB)
	}
}

func BenchmarkVectorL1Distance768(b *testing.B) {
	a, vecB := makeVectorMathBenchmarkInputs(768)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		vectorMathBenchmarkSink = VectorL1Distance(a, vecB)
	}
}

func BenchmarkVectorL1DistanceUnrolled768(b *testing.B) {
	a, vecB := makeVectorMathBenchmarkInputs(768)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		vectorMathBenchmarkSink = VectorL1Unrolled(a, vecB)
	}
}

// BenchmarkVectorHammingDistance768 measures VectorHammingDistance (which
// dispatches to the portable 4-way-unrolled VectorHammingUnrolled — see the
// design-rationale comment on VectorHammingDistance in vector_math.go for why
// this metric intentionally has no hand-rolled SIMD kernel) against a plain,
// non-unrolled reference loop (naiveHamming, already defined in
// vector_math_unroll_test.go for correctness cross-checking and reused here).
// Run side by side so any future change to either implementation shows up as
// a relative regression/improvement, not just an absolute number.
func BenchmarkVectorHammingDistance768(b *testing.B) {
	a, vecB := makeVectorMathBenchmarkInputs(768)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		vectorMathBenchmarkIntSink = VectorHammingDistance(a, vecB)
	}
}

func BenchmarkVectorHammingNaive768(b *testing.B) {
	a, vecB := makeVectorMathBenchmarkInputs(768)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		vectorMathBenchmarkIntSink = naiveHamming(a, vecB)
	}
}

// makeCentroidBenchmarkInputs builds `vectors` deterministic 768-ish-dim
// vectors for VEC_CENTROID-style accumulation benchmarks below.
func makeCentroidBenchmarkInputs(vectors, dims int) [][]float64 {
	vecs := make([][]float64, vectors)
	for v := 0; v < vectors; v++ {
		vec := make([]float64, dims)
		for i := range vec {
			vec[i] = math.Sin(float64(i)*0.11+float64(v)) * 0.75
		}
		vecs[v] = vec
	}
	return vecs
}

// BenchmarkVectorCentroidAccumulate768x8 and
// BenchmarkVectorCentroidAccumulateNaive768x8 mirror evalVecCentroid's own
// usage of VectorAccumulateUnrolled: a fresh zeroed accumulator per call,
// with each of 8 768-dim source vectors folded into it in turn (see
// VEC_CENTROID in vector_functions.go). naiveAccumulate is the trivial
// reference loop already defined in vector_math_unroll_test.go for
// correctness cross-checking, reused here for the timing comparison.
func BenchmarkVectorCentroidAccumulate768x8(b *testing.B) {
	vecs := makeCentroidBenchmarkInputs(8, 768)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out := make([]float64, 768)
		for _, v := range vecs {
			VectorAccumulateUnrolled(out, v)
		}
		vectorMathBenchmarkSink = out[0]
	}
}

func BenchmarkVectorCentroidAccumulateNaive768x8(b *testing.B) {
	vecs := makeCentroidBenchmarkInputs(8, 768)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out := make([]float64, 768)
		for _, v := range vecs {
			naiveAccumulate(out, v)
		}
		vectorMathBenchmarkSink = out[0]
	}
}

// BenchmarkVectorDotKernelBySize guards the amd64 kernel dispatch decision in
// vectorDotKernel (vector_math_amd64.go): that function used to fall back to
// the portable VectorDotUnrolled loop for vectors shorter than 128 elements,
// on the assumption that SSE2 setup overhead wasn't worth it below that size.
// Measuring across the dimension sizes real embedding models actually use
// (16 up to 768) showed the SSE2 kernel winning at every single size,
// including 16 — the assembly kernel's own tail loop already handles
// short/odd-length inputs, so there was no setup cost being amortized and
// the threshold only ever left performance on the table. Run both variants
// side by side here so a future change to that threshold (or a new
// portable-loop "improvement") shows up as a regression instead of silently
// reintroducing the bad cutover.
func BenchmarkVectorDotKernelBySize(b *testing.B) {
	for _, dims := range []int{16, 32, 64, 128, 256, 768} {
		a, vecB := makeVectorMathBenchmarkInputs(dims)
		b.Run(fmt.Sprintf("unrolled/%d", dims), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				vectorMathBenchmarkSink = VectorDotUnrolled(a, vecB)
			}
		})
		b.Run(fmt.Sprintf("kernel/%d", dims), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				vectorMathBenchmarkSink = vectorDotKernel(a, vecB)
			}
		})
	}
}
