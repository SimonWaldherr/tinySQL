package search

import "math"

const smallestNormalFloat64 = 0x1p-1022

// VectorNorm uses the dot kernel for ordinary vectors, and scales components
// when squaring would overflow or lose precision in the subnormal range.
func VectorNorm(v []float64) float64 {
	sum := VectorDot(v, v)
	if sum >= smallestNormalFloat64 && sum <= math.MaxFloat64 {
		return math.Sqrt(sum)
	}
	scale, norm := vectorScaledNorm(v)
	return scale * norm
}

func vectorScaledNorm(v []float64) (scale, norm float64) {
	for _, x := range v {
		x = math.Abs(x)
		if math.IsNaN(x) {
			return x, 1
		}
		if x > scale {
			scale = x
		}
	}
	if scale == 0 || math.IsInf(scale, 1) {
		return scale, 1
	}
	var sum float64
	for _, x := range v {
		y := x / scale
		sum += y * y
	}
	return scale, math.Sqrt(sum)
}

// VectorNormalize writes a unit-length copy to dst. It returns false for a
// zero vector. dst must have the same length as src and may be src itself.
func VectorNormalize(dst, src []float64) bool {
	dst = dst[:len(src)]
	sum := VectorDot(src, src)
	if sum >= smallestNormalFloat64 && sum <= math.MaxFloat64 {
		inv := 1 / math.Sqrt(sum)
		for i, x := range src {
			dst[i] = x * inv
		}
		return true
	}
	scale, norm := vectorScaledNorm(src)
	if scale == 0 {
		return false
	}
	for i, x := range src {
		dst[i] = (x / scale) / norm
	}
	return true
}

// VectorCosine computes cosine similarity with one fused pass in the common
// case. Extreme finite inputs use scaled components instead of squared norms.
func VectorCosine(a, b []float64) (float64, bool) {
	if len(a) != len(b) {
		return 0, false
	}
	dot, normA2, normB2 := VectorCosineParts(a, b)
	product := normA2 * normB2
	if normA2 >= smallestNormalFloat64 && normB2 >= smallestNormalFloat64 && product >= smallestNormalFloat64 && product <= math.MaxFloat64 {
		return dot / math.Sqrt(product), true
	}
	return vectorScaledCosine(a, b)
}

// VectorCosineWithNorms reuses cached norms without scanning ordinary vectors
// again. Scaling is reserved for products that overflow or become subnormal.
func VectorCosineWithNorms(a, b []float64, normA, normB float64) (float64, bool) {
	if len(a) != len(b) || normA == 0 || normB == 0 {
		return 0, false
	}
	denom := normA * normB
	if normA >= smallestNormalFloat64 && normB >= smallestNormalFloat64 && denom >= smallestNormalFloat64 && denom <= math.MaxFloat64 {
		dot := VectorDot(a, b)
		if !math.IsInf(dot, 0) && !math.IsNaN(dot) {
			return dot / denom, true
		}
	}
	return vectorScaledCosine(a, b)
}

func vectorScaledCosine(a, b []float64) (float64, bool) {
	var scaleA, scaleB float64
	for i, x := range a {
		ax, bx := math.Abs(x), math.Abs(b[i])
		if math.IsNaN(ax) || math.IsNaN(bx) {
			return math.NaN(), true
		}
		if ax > scaleA {
			scaleA = ax
		}
		if bx > scaleB {
			scaleB = bx
		}
	}
	if math.IsInf(scaleA, 1) || math.IsInf(scaleB, 1) {
		return math.NaN(), true
	}
	if scaleA == 0 || scaleB == 0 {
		return 0, false
	}
	var dot, normA2, normB2 float64
	for i, x := range a {
		x, y := x/scaleA, b[i]/scaleB
		dot += x * y
		normA2 += x * x
		normB2 += y * y
	}
	return dot / math.Sqrt(normA2*normB2), true
}
