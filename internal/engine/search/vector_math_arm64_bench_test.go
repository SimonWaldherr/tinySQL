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

func TestARM64FeatureDispatch(t *testing.T) {
	if vectorUseNEON != detectNEON() {
		t.Fatalf("init NEON selection=%t, feature probe=%t", vectorUseNEON, detectNEON())
	}
	if got, want := VectorMathBackend, vectorARM64Backend(vectorUseNEON); got != want {
		t.Fatalf("backend=%q, want %q", got, want)
	}
	for useNEON, want := range map[bool]string{true: "arm64-neon", false: "arm64-portable"} {
		if got := vectorARM64Backend(useNEON); got != want {
			t.Errorf("useNEON=%t: backend=%q, want %q", useNEON, got, want)
		}
	}
}

func BenchmarkVectorCosineNEONBySize(b *testing.B) {
	for _, dims := range []int{32, 64, 96, 128, 768} {
		a, vecB := makeVectorMathBenchmarkInputs(dims)
		b.Run(fmt.Sprintf("unrolled/%d", dims), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				dot, normA2, normB2 := VectorCosineUnrolled(a, vecB)
				vectorMathBenchmarkSink = dot + normA2 + normB2
			}
		})
		b.Run(fmt.Sprintf("neon-fused/%d", dims), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				dot, normA2, normB2 := vectorCosineNEON(a, vecB)
				vectorMathBenchmarkSink = dot + normA2 + normB2
			}
		})
		b.Run(fmt.Sprintf("kernel/%d", dims), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				dot, normA2, normB2 := vectorCosineKernel(a, vecB)
				vectorMathBenchmarkSink = dot + normA2 + normB2
			}
		})
	}
}

func BenchmarkVectorL1NEONBySize(b *testing.B) {
	for _, dims := range []int{32, 64, 96, 128, 768} {
		a, vecB := makeVectorMathBenchmarkInputs(dims)
		b.Run(fmt.Sprintf("unrolled/%d", dims), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				vectorMathBenchmarkSink = VectorL1Unrolled(a, vecB)
			}
		})
		b.Run(fmt.Sprintf("neon/%d", dims), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				vectorMathBenchmarkSink = vectorL1NEON(a, vecB)
			}
		})
		b.Run(fmt.Sprintf("kernel/%d", dims), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				vectorMathBenchmarkSink = vectorL1Kernel(a, vecB)
			}
		})
	}
}

func TestVectorL1NEONMatchesUnrolled(t *testing.T) {
	for _, dims := range []int{0, 1, 7, 8, 31, 32, 63, 64, 96, 127, 128, 768} {
		a, vecB := makeVectorMathBenchmarkInputs(dims)
		got, want := vectorL1NEON(a, vecB), VectorL1Unrolled(a, vecB)
		if math.Abs(got-want) > 1e-12*math.Max(1, math.Abs(want)) {
			t.Fatalf("dims=%d: got %g, want %g", dims, got, want)
		}
	}
}

func BenchmarkVectorAccumulateNEONBySize(b *testing.B) {
	for _, dims := range []int{32, 64, 96, 128, 768} {
		src, seed := makeVectorMathBenchmarkInputs(dims)
		b.Run(fmt.Sprintf("unrolled/%d", dims), func(b *testing.B) {
			dst := append([]float64(nil), seed...)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				VectorAccumulateUnrolled(dst, src)
			}
			vectorMathBenchmarkSink = dst[0]
		})
		b.Run(fmt.Sprintf("neon/%d", dims), func(b *testing.B) {
			dst := append([]float64(nil), seed...)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				vectorAccumulateNEON(dst, src)
			}
			vectorMathBenchmarkSink = dst[0]
		})
		b.Run(fmt.Sprintf("kernel/%d", dims), func(b *testing.B) {
			dst := append([]float64(nil), seed...)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				vectorAccumulateKernel(dst, src)
			}
			vectorMathBenchmarkSink = dst[0]
		})
	}
}

func TestVectorAccumulateNEONMatchesUnrolled(t *testing.T) {
	for _, dims := range []int{0, 1, 7, 8, 31, 32, 63, 64, 96, 127, 128, 768} {
		src, seed := makeVectorMathBenchmarkInputs(dims)
		got := append([]float64(nil), seed...)
		want := append([]float64(nil), seed...)
		vectorAccumulateNEON(got, src)
		VectorAccumulateUnrolled(want, src)
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("dims=%d idx=%d: NEON=%g, unrolled=%g", dims, i, got[i], want[i])
			}
		}
	}
}

func TestVectorCosineNEONMatchesUnrolled(t *testing.T) {
	for _, dims := range []int{0, 1, 7, 8, 31, 32, 63, 64, 96, 127, 128, 768} {
		a, vecB := makeVectorMathBenchmarkInputs(dims)
		gotDot, gotNormA2, gotNormB2 := vectorCosineNEON(a, vecB)
		wantDot, wantNormA2, wantNormB2 := VectorCosineUnrolled(a, vecB)
		for _, check := range []struct {
			name      string
			got, want float64
		}{
			{"dot", gotDot, wantDot},
			{"normA2", gotNormA2, wantNormA2},
			{"normB2", gotNormB2, wantNormB2},
		} {
			if math.Abs(check.got-check.want) > 1e-12*math.Max(1, math.Abs(check.want)) {
				t.Fatalf("dims=%d %s=%g, want %g", dims, check.name, check.got, check.want)
			}
		}
	}
}

func TestVectorL2SquaredNEONMatchesUnrolled(t *testing.T) {
	for _, dims := range []int{0, 1, 7, 8, 31, 32, 63, 64, 96, 127, 128, 129, 768, 769} {
		a, b := makeVectorMathBenchmarkInputs(dims)
		got, want := vectorL2SquaredNEON(a, b), VectorL2SquaredUnrolled(a, b)
		if math.IsNaN(got) || math.Abs(got-want) > 1e-12*math.Max(1, math.Abs(want)) {
			t.Fatalf("dims=%d: NEON=%g, unrolled=%g", dims, got, want)
		}
	}
}
