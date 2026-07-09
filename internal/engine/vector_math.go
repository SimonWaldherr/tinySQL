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

// vectorCosineParts computes dot(a,b), dot(a,a) and dot(b,b) in a single
// pass over both vectors, dispatching to a fused SIMD kernel where one
// exists. Cosine similarity needs all three quantities; when no cached norm
// is available (the scalar VEC_COSINE_SIMILARITY path), one fused pass costs
// the same memory traffic as a plain dot product instead of three separate
// loops.
func vectorCosineParts(a, b []float64) (dot, normA2, normB2 float64) {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	return vectorCosineKernel(a[:n], b[:n])
}

func vectorCosineUnrolled(a, b []float64) (dot, normA2, normB2 float64) {
	var d0, d1, n0, n1, m0, m1 float64
	i := 0
	for ; i+1 < len(a); i += 2 {
		a0, a1 := a[i], a[i+1]
		b0, b1 := b[i], b[i+1]
		d0 += a0 * b0
		d1 += a1 * b1
		n0 += a0 * a0
		n1 += a1 * a1
		m0 += b0 * b0
		m1 += b1 * b1
	}
	if i < len(a) {
		a0, b0 := a[i], b[i]
		d0 += a0 * b0
		n0 += a0 * a0
		m0 += b0 * b0
	}
	return d0 + d1, n0 + n1, m0 + m1
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
