//go:build arm64

package search

import "golang.org/x/sys/cpu"

// vectorUseNEON is determined once during initialization from the operating
// system's ARM64 feature view. ASIMD/NEON is mandatory for compliant ARMv8
// hosts, but retaining this gate means unusual VMs or future ports fall back
// to the portable loops rather than reaching an unavailable instruction.
var vectorUseNEON bool

// VectorMathBackend names the implementation selected during package init.
var VectorMathBackend string

func init() {
	vectorUseNEON = detectNEON()
	VectorMathBackend = vectorARM64Backend(vectorUseNEON)
}

func detectNEON() bool {
	return cpu.ARM64.HasASIMD
}

func vectorARM64Backend(useNEON bool) string {
	if useNEON {
		return "arm64-neon"
	}
	return "arm64-portable"
}

//go:noescape
func vectorDotNEON(a, b []float64) float64

//go:noescape
func vectorL2SquaredNEON(a, b []float64) float64

//go:noescape
func vectorCosineNEON(a, b []float64) (dot, normA2, normB2 float64)

//go:noescape
func vectorL1NEON(a, b []float64) float64

//go:noescape
func vectorAccumulateNEON(dst, src []float64)

func vectorDotKernel(a, b []float64) float64 {
	// The NEON setup is already amortized for the 64- and 96-dimensional
	// embeddings commonly used by compact RAG models.  Benchmarks on M2 show
	// the assembly kernel winning from 32 elements onward; shorter vectors keep
	// the Go loop, whose call overhead is lower at that size.
	if !vectorUseNEON || len(a) < 32 {
		return VectorDotUnrolled(a, b)
	}
	return vectorDotNEON(a, b)
}

func vectorL2SquaredKernel(a, b []float64) float64 {
	if !vectorUseNEON || len(a) < 128 {
		return VectorL2SquaredUnrolled(a, b)
	}
	return vectorL2SquaredNEON(a, b)
}

// vectorCosineKernel fuses dot(a,b), dot(a,a), dot(b,b) in one NEON pass for
// compact RAG embeddings and larger vectors. This reads each input cache line
// once instead of invoking the dot kernel three times for dot and both norms.
func vectorCosineKernel(a, b []float64) (dot, normA2, normB2 float64) {
	if !vectorUseNEON || len(a) < 32 {
		return VectorCosineUnrolled(a, b)
	}
	return vectorCosineNEON(a, b)
}

// vectorL1Kernel uses NEON's floating-point subtract, absolute-value, and
// add instructions for normal embedding sizes. Small inputs retain the
// inlined Go loop, where an assembly call is not yet amortized.
func vectorL1Kernel(a, b []float64) float64 {
	if vectorUseNEON && len(a) >= 32 {
		return vectorL1NEON(a, b)
	}
	return VectorL1Unrolled(a, b)
}

// vectorAccumulateKernel vectorizes centroid construction and IVF k-means
// training. Short vectors retain the inlined portable loop.
func vectorAccumulateKernel(dst, src []float64) {
	if vectorUseNEON && len(dst) >= 32 {
		vectorAccumulateNEON(dst, src)
		return
	}
	VectorAccumulateUnrolled(dst, src)
}
