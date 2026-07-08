//go:build amd64

package engine

const vectorMathBackend = "amd64-sse2"

//go:noescape
func vectorDotSSE2(a, b []float64) float64

//go:noescape
func vectorL2SquaredSSE2(a, b []float64) float64

//go:noescape
func vectorL1SSE2(a, b []float64) float64

// The kernels below used to fall back to the portable unrolled Go loop for
// vectors shorter than 128 elements, on the assumption that SIMD setup
// overhead wasn't worth it for small inputs. Benchmarking across the
// dimension sizes real embedding models actually use (16-768) showed SSE2
// winning at every size, including 16 — the tail loop in the assembly
// kernel already handles short/odd-length inputs directly, so there is no
// setup cost being amortized and the threshold was only ever leaving
// performance on the table. amd64 guarantees SSE2, so this path is always
// safe.
func vectorDotKernel(a, b []float64) float64 {
	return vectorDotSSE2(a, b)
}

func vectorL2SquaredKernel(a, b []float64) float64 {
	return vectorL2SquaredSSE2(a, b)
}

func vectorL1Kernel(a, b []float64) float64 {
	return vectorL1SSE2(a, b)
}
