//go:build amd64

package engine

// x86cpuid and x86xgetbv are implemented in vector_math_amd64.s. They exist
// so AVX2/FMA detection needs no external dependency (golang.org/x/sys/cpu
// implements the exact same two primitives).
func x86cpuid(eaxArg, ecxArg uint32) (eax, ebx, ecx, edx uint32)

func x86xgetbv() (eax, edx uint32)

// detectAVX2FMA reports whether the CPU and OS together support the
// AVX2+FMA kernels: FMA3 and AVX from CPUID leaf 1, OS-enabled XMM+YMM
// state saving via OSXSAVE/XGETBV (XCR0 bits 1-2), and AVX2 from leaf 7.
// Without the XCR0 check a pre-AVX operating system would fault on the
// first YMM instruction even though the CPU supports it.
func detectAVX2FMA() bool {
	maxID, _, _, _ := x86cpuid(0, 0)
	if maxID < 7 {
		return false
	}
	const (
		cpuid1FMA     = 1 << 12
		cpuid1OSXSAVE = 1 << 27
		cpuid1AVX     = 1 << 28
		want1         = cpuid1FMA | cpuid1OSXSAVE | cpuid1AVX
	)
	_, _, ecx1, _ := x86cpuid(1, 0)
	if ecx1&want1 != want1 {
		return false
	}
	const xcr0XMMYMM = 1<<1 | 1<<2
	xcr0, _ := x86xgetbv()
	if xcr0&xcr0XMMYMM != xcr0XMMYMM {
		return false
	}
	const cpuid7AVX2 = 1 << 5
	_, ebx7, _, _ := x86cpuid(7, 0)
	return ebx7&cpuid7AVX2 != 0
}

// vectorUseAVX2 gates dispatch to the AVX2+FMA kernels. Both features are
// required together: the kernels use VFMADD231PD (FMA3) on YMM registers.
// Detected once at startup via CPUID; the branch in each kernel wrapper is
// perfectly predicted and effectively free.
var vectorUseAVX2 = detectAVX2FMA()

var vectorMathBackend = func() string {
	if vectorUseAVX2 {
		return "amd64-avx2-fma"
	}
	return "amd64-sse2"
}()

//go:noescape
func vectorDotSSE2(a, b []float64) float64

//go:noescape
func vectorL2SquaredSSE2(a, b []float64) float64

//go:noescape
func vectorL1SSE2(a, b []float64) float64

//go:noescape
func vectorCosineSSE2(a, b []float64) (dot, normA2, normB2 float64)

//go:noescape
func vectorDotAVX2(a, b []float64) float64

//go:noescape
func vectorL2SquaredAVX2(a, b []float64) float64

//go:noescape
func vectorL1AVX2(a, b []float64) float64

//go:noescape
func vectorCosineAVX2(a, b []float64) (dot, normA2, normB2 float64)

// The kernels below dispatch AVX2+FMA (4 float64 per instruction, fused
// multiply-add) when the CPU supports it, falling back to baseline SSE2
// (guaranteed on amd64). Short vectors stay on the SSE2 kernels: the AVX2
// main loops consume 8–16 elements per iteration, so below that everything
// would run in their scalar tails anyway, while the SSE2 kernels still
// vectorize from 2 elements up. Benchmarking previously showed SSE2 beating
// the portable unrolled Go loop at every embedding size from 16 through 768,
// so SSE2 remains the floor for all sizes.
func vectorDotKernel(a, b []float64) float64 {
	if vectorUseAVX2 && len(a) >= 16 {
		return vectorDotAVX2(a, b)
	}
	return vectorDotSSE2(a, b)
}

func vectorL2SquaredKernel(a, b []float64) float64 {
	if vectorUseAVX2 && len(a) >= 16 {
		return vectorL2SquaredAVX2(a, b)
	}
	return vectorL2SquaredSSE2(a, b)
}

func vectorL1Kernel(a, b []float64) float64 {
	if vectorUseAVX2 && len(a) >= 16 {
		return vectorL1AVX2(a, b)
	}
	return vectorL1SSE2(a, b)
}

func vectorCosineKernel(a, b []float64) (dot, normA2, normB2 float64) {
	if vectorUseAVX2 && len(a) >= 8 {
		return vectorCosineAVX2(a, b)
	}
	return vectorCosineSSE2(a, b)
}
