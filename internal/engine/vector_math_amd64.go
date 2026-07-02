//go:build amd64

package engine

const vectorMathBackend = "amd64-sse2"

//go:noescape
func vectorDotSSE2(a, b []float64) float64

//go:noescape
func vectorL2SquaredSSE2(a, b []float64) float64

func vectorDotKernel(a, b []float64) float64 {
	if len(a) < 128 {
		return vectorDotUnrolled(a, b)
	}
	return vectorDotSSE2(a, b)
}

func vectorL2SquaredKernel(a, b []float64) float64 {
	if len(a) < 128 {
		return vectorL2SquaredUnrolled(a, b)
	}
	return vectorL2SquaredSSE2(a, b)
}
