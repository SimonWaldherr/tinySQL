//go:build arm64

package engine

const vectorMathBackend = "arm64-neon"

//go:noescape
func vectorDotNEON(a, b []float64) float64

//go:noescape
func vectorL2SquaredNEON(a, b []float64) float64

func vectorDotKernel(a, b []float64) float64 {
	if len(a) < 128 {
		return vectorDotUnrolled(a, b)
	}
	return vectorDotNEON(a, b)
}

func vectorL2SquaredKernel(a, b []float64) float64 {
	if len(a) < 128 {
		return vectorL2SquaredUnrolled(a, b)
	}
	return vectorL2SquaredNEON(a, b)
}

// vectorCosineKernel fuses dot(a,b), dot(a,a), dot(b,b). There is no fused
// NEON kernel (same rationale as vectorL1Kernel below: hand-writing NEON
// without hardware to validate on risks silent corruption), so large inputs
// reuse the proven NEON dot kernel three times — each pass SIMD — and small
// inputs use the portable fused loop.
func vectorCosineKernel(a, b []float64) (dot, normA2, normB2 float64) {
	if len(a) < 128 {
		return vectorCosineUnrolled(a, b)
	}
	return vectorDotNEON(a, b), vectorDotNEON(a, a), vectorDotNEON(b, b)
}

// vectorL1Kernel has no NEON assembly kernel (unlike Dot/L2Squared above):
// writing one would mean hand-deriving a raw FABS.2D vector encoding without
// being able to run it on real ARM64 hardware to confirm correctness, and a
// wrong bit pattern here would silently corrupt distance results rather
// than fail to build. The portable unrolled path is still 4-way unrolled
// (see vectorL1Unrolled) and auto-vectorizes reasonably well under the Go
// compiler; a real NEON kernel is a good follow-up for whoever can validate
// it on actual ARM64 hardware.
func vectorL1Kernel(a, b []float64) float64 {
	return vectorL1Unrolled(a, b)
}
