package engine

import "math"

func vectorDot(a, b []float64) float64 {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	return vectorDotUnrolled(a[:n], b[:n])
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
	var s0, s1, s2, s3 float64
	i := 0
	for ; i+3 < n; i += 4 {
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
	for ; i < n; i++ {
		d := a[i] - b[i]
		sum += d * d
	}
	return sum
}

func vectorL1Distance(a, b []float64) float64 {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	var sum float64
	for i := 0; i < n; i++ {
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
