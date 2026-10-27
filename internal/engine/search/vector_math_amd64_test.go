//go:build amd64

package search

import (
	"fmt"
	"math"
	"testing"
	"unsafe"
)

func TestAMD64FeatureDispatch(t *testing.T) {
	if vectorUseAVX2 != detectAVX2FMA() {
		t.Fatalf("init AVX2/FMA selection=%t, feature probe=%t", vectorUseAVX2, detectAVX2FMA())
	}
	if got, want := VectorMathBackend, vectorAMD64Backend(vectorUseAVX2); got != want {
		t.Fatalf("backend=%q, want %q", got, want)
	}
	for useAVX2, want := range map[bool]string{true: "amd64-avx2-fma", false: "amd64-sse2"} {
		if got := vectorAMD64Backend(useAVX2); got != want {
			t.Errorf("useAVX2=%t: backend=%q, want %q", useAVX2, got, want)
		}
	}
}

func TestAMD64VectorKernelsAlignment(t *testing.T) {
	type vectorKernels struct {
		name   string
		dot    func([]float64, []float64) float64
		l2     func([]float64, []float64) float64
		l1     func([]float64, []float64) float64
		cosine func([]float64, []float64) (float64, float64, float64)
	}
	kernels := []vectorKernels{
		{"SSE2", vectorDotSSE2, vectorL2SquaredSSE2, vectorL1SSE2, vectorCosineSSE2},
		{"dispatch", VectorDot, VectorL2Squared, VectorL1Distance, VectorCosineParts},
	}
	if detectAVX2FMA() {
		kernels = append(kernels, vectorKernels{"AVX2", vectorDotAVX2, vectorL2SquaredAVX2, vectorL1AVX2, vectorCosineAVX2})
	}
	for _, kernel := range kernels {
		t.Run(kernel.name, func(t *testing.T) {
			for _, n := range []int{0, 1, 2, 3, 4, 7, 8, 9, 15, 16, 17, 23, 24, 31, 32, 33, 63, 64, 65, 768} {
				for _, aOffset := range []uintptr{0, 8} {
					for _, bOffset := range []uintptr{0, 8} {
						t.Run(fmt.Sprintf("n=%d/a=%d/b=%d", n, aOffset, bOffset), func(t *testing.T) {
							a := vectorWithAlignment(n, aOffset)
							b := vectorWithAlignment(n, bOffset)
							for i := range a {
								a[i] = float64(i%11-5) * 0.25
								b[i] = float64(i%7-3) * 0.5
							}
							check := func(name string, got, want float64) {
								t.Helper()
								if math.IsNaN(got) || math.Abs(got-want) > 1e-12*math.Max(1, math.Abs(want)) {
									t.Errorf("%s=%v, want %v", name, got, want)
								}
							}
							check("dot", kernel.dot(a, b), VectorDotUnrolled(a, b))
							check("L2Squared", kernel.l2(a, b), VectorL2SquaredUnrolled(a, b))
							check("L1", kernel.l1(a, b), VectorL1Unrolled(a, b))
							dot, normA2, normB2 := kernel.cosine(a, b)
							wantDot, wantA2, wantB2 := VectorCosineUnrolled(a, b)
							check("cosine.dot", dot, wantDot)
							check("cosine.normA2", normA2, wantA2)
							check("cosine.normB2", normB2, wantB2)
						})
					}
				}
			}
		})
	}
}

// Go guarantees only 8-byte alignment for float64 slices. Select both possible
// offsets modulo 16 explicitly so this test also exercises unaligned SIMD loads
// when the allocator happens to return a 16-byte-aligned backing array.
func vectorWithAlignment(n int, offset uintptr) []float64 {
	buf := make([]float64, n+2)
	start := 0
	if uintptr(unsafe.Pointer(&buf[0]))%16 != offset {
		start = 1
	}
	return buf[start : start+n]
}
