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
