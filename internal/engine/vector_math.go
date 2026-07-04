package engine

import "math"

func vectorDot(a, b []float64) float64 {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	return vectorDotKernel(a[:n], b[:n])
}

func vectorDotUnrolled(a, b []float64) float64 {
	var s0, s1, s2, s3 float64
	i := 0
	for ; i+3 < len(a); i += 4 {
		s0 += a[i] * b[i]
		s1 += a[i+1] * b[i+1]
		s2 += a[i+2] * b[i+2]
		s3 += a[i+3] * b[i+3]
	}
	sum := (s0 + s1) + (s2 + s3)
	for ; i < len(a); i++ {
		sum += a[i] * b[i]
	}
	return sum
}

func vectorL2Squared(a, b []float64) float64 {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	return vectorL2SquaredKernel(a[:n], b[:n])
}

func vectorL2SquaredUnrolled(a, b []float64) float64 {
	var s0, s1, s2, s3 float64
	i := 0
	for ; i+3 < len(a); i += 4 {
		d0 := a[i] - b[i]
		d1 := a[i+1] - b[i+1]
		d2 := a[i+2] - b[i+2]
		d3 := a[i+3] - b[i+3]
		s0 += d0 * d0
		s1 += d1 * d1
		s2 += d2 * d2
		s3 += d3 * d3
	}
	sum := (s0 + s1) + (s2 + s3)
	for ; i < len(a); i++ {
		d := a[i] - b[i]
		sum += d * d
	}
	return sum
}

// vectorL1Distance computes the Manhattan (L1) distance, dispatching to a
// SIMD kernel where one exists (see vectorL1Kernel in the per-arch files) —
// mirroring how vectorDot/vectorL2Squared dispatch. Previously this metric
// had no SIMD path at all, unlike Dot and L2Squared.
func vectorL1Distance(a, b []float64) float64 {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	return vectorL1Kernel(a[:n], b[:n])
}

func vectorL1Unrolled(a, b []float64) float64 {
	var s0, s1, s2, s3 float64
	i := 0
	for ; i+3 < len(a); i += 4 {
		s0 += math.Abs(a[i] - b[i])
		s1 += math.Abs(a[i+1] - b[i+1])
		s2 += math.Abs(a[i+2] - b[i+2])
		s3 += math.Abs(a[i+3] - b[i+3])
	}
	sum := (s0 + s1) + (s2 + s3)
	for ; i < len(a); i++ {
		sum += math.Abs(a[i] - b[i])
	}
	return sum
}

func vectorDistance(metric string, a, b []float64, normA, normB float64) (float64, bool) {
	if len(a) != len(b) {
		return 0, false
	}
	switch metric {
	case "cosine":
		if normA == 0 || normB == 0 {
			return 0, false
		}
		return 1.0 - vectorDot(a, b)/(normA*normB), true
	case "l2":
		return math.Sqrt(vectorL2Squared(a, b)), true
	case "manhattan":
		return vectorL1Distance(a, b), true
	case "dot":
		return -vectorDot(a, b), true
	default:
		return 0, false
	}
}
