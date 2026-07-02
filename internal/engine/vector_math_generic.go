//go:build !arm64 && !amd64

package engine

const vectorMathBackend = "portable-unrolled"

func vectorDotKernel(a, b []float64) float64 {
	return vectorDotUnrolled(a, b)
}

func vectorL2SquaredKernel(a, b []float64) float64 {
	return vectorL2SquaredUnrolled(a, b)
}
