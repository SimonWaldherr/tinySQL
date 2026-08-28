//go:build !arm64 && !amd64

package search

const VectorMathBackend = "portable-unrolled"

func vectorDotKernel(a, b []float64) float64 {
	return VectorDotUnrolled(a, b)
}

func vectorL2SquaredKernel(a, b []float64) float64 {
	return VectorL2SquaredUnrolled(a, b)
}

func vectorL1Kernel(a, b []float64) float64 {
	return VectorL1Unrolled(a, b)
}

func vectorCosineKernel(a, b []float64) (dot, normA2, normB2 float64) {
	return VectorCosineUnrolled(a, b)
}
