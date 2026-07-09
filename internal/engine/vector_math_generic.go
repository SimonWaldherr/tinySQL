//go:build !arm64 && !amd64

package engine

const vectorMathBackend = "portable-unrolled"

func vectorDotKernel(a, b []float64) float64 {
	return vectorDotUnrolled(a, b)
}

func vectorL2SquaredKernel(a, b []float64) float64 {
	return vectorL2SquaredUnrolled(a, b)
}

func vectorL1Kernel(a, b []float64) float64 {
	return vectorL1Unrolled(a, b)
}

func vectorCosineKernel(a, b []float64) (dot, normA2, normB2 float64) {
	return vectorCosineUnrolled(a, b)
}
