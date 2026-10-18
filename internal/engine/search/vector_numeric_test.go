package search

import (
	"math"
	"testing"
)

func TestVectorNormExtremeMagnitudes(t *testing.T) {
	for _, scale := range []float64{1, 1e200, 1e-200, math.SmallestNonzeroFloat64} {
		v := []float64{3 * scale, 4 * scale}
		want := 5 * scale
		got := VectorNorm(v)
		if math.IsInf(got, 0) || math.IsNaN(got) || math.Abs(got/want-1) > 2e-15 {
			t.Errorf("norm(%v) = %v, want %v", v, got, want)
		}
	}
	for _, tc := range []struct {
		name string
		v    []float64
		want float64
	}{
		{"empty", nil, 0},
		{"signed zero", []float64{math.Copysign(0, -1)}, 0},
		{"infinity", []float64{math.Inf(1), 2}, math.Inf(1)},
		{"overflowing result", []float64{math.MaxFloat64, math.MaxFloat64}, math.Inf(1)},
		{"nan", []float64{math.NaN(), 2}, math.NaN()},
		{"nan and infinity", []float64{math.Inf(1), math.NaN()}, math.NaN()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := VectorNorm(tc.v)
			if math.IsNaN(tc.want) {
				if !math.IsNaN(got) {
					t.Fatalf("got %v, want NaN", got)
				}
			} else if got != tc.want || math.Signbit(got) {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestVectorNormalizeExtremeMagnitudes(t *testing.T) {
	for _, scale := range []float64{1, 1e200, 1e-200, math.SmallestNonzeroFloat64} {
		src := []float64{3 * scale, -4 * scale, math.Copysign(0, -1)}
		dst := make([]float64, len(src))
		if !VectorNormalize(dst, src) {
			t.Fatalf("normalize(%v) rejected", src)
		}
		if math.Abs(dst[0]-0.6) > 2e-15 || math.Abs(dst[1]+0.8) > 2e-15 || dst[2] != 0 || !math.Signbit(dst[2]) {
			t.Errorf("normalize(%v) = %v", src, dst)
		}
		if src[0] != 3*scale || src[1] != -4*scale || !math.Signbit(src[2]) {
			t.Fatalf("source mutated: %v", src)
		}
		copy(dst, src)
		if !VectorNormalize(dst, dst) || math.Abs(VectorNorm(dst)-1) > 2e-15 {
			t.Errorf("in-place normalize failed: %v", dst)
		}
	}
	large := []float64{math.MaxFloat64, math.MaxFloat64}
	if !VectorNormalize(large, large) || math.Abs(large[0]-math.Sqrt(0.5)) > 2e-15 {
		t.Fatalf("overflowing norm should still normalize: %v", large)
	}
	for _, zero := range [][]float64{nil, {0, math.Copysign(0, -1)}} {
		if VectorNormalize(make([]float64, len(zero)), zero) {
			t.Fatalf("zero vector accepted: %v", zero)
		}
	}
	out := make([]float64, 2)
	VectorNormalize(out, []float64{math.Inf(1), -2})
	if !math.IsNaN(out[0]) || out[1] != 0 || !math.Signbit(out[1]) {
		t.Fatalf("nonfinite normalization changed: %v", out)
	}
	VectorNormalize(out, []float64{math.NaN(), 2})
	if !math.IsNaN(out[0]) || !math.IsNaN(out[1]) {
		t.Fatalf("NaN normalization changed: %v", out)
	}
}

func TestVectorCosineExtremeMagnitudes(t *testing.T) {
	for _, scaleA := range []float64{1, 1e200, 1e-200, math.SmallestNonzeroFloat64, math.MaxFloat64 / 4} {
		for _, scaleB := range []float64{1, 1e200, 1e-200, math.SmallestNonzeroFloat64} {
			a, b := []float64{3 * scaleA, 4 * scaleA}, []float64{4 * scaleB, 3 * scaleB}
			for _, cached := range []bool{false, true} {
				var got float64
				var ok bool
				if cached {
					got, ok = VectorCosineWithNorms(a, b, VectorNorm(a), VectorNorm(b))
				} else {
					got, ok = VectorCosine(a, b)
				}
				if !ok || math.IsNaN(got) || math.Abs(got-0.96) > 2e-15 {
					t.Errorf("cosine(%v,%v) cached=%v: %v, %v", a, b, cached, got, ok)
				}
			}
		}
	}
	// Rounded subnormal cached norms must not turn parallel vectors into a
	// similarity above one when the other vector has an ordinary magnitude.
	a, b := []float64{math.SmallestNonzeroFloat64, math.SmallestNonzeroFloat64}, []float64{1e200, 1e200}
	if got, ok := VectorCosineWithNorms(a, b, VectorNorm(a), VectorNorm(b)); !ok || math.Abs(got-1) > 2e-15 {
		t.Fatalf("subnormal cached norm: %v, %v", got, ok)
	}
	for _, pair := range []struct{ a, b []float64 }{{nil, nil}, {[]float64{0}, []float64{1}}, {[]float64{1}, nil}} {
		if _, ok := VectorCosine(pair.a, pair.b); ok {
			t.Errorf("invalid pair accepted: %v %v", pair.a, pair.b)
		}
	}
	for _, v := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		a, b := []float64{v, 2}, []float64{1, 2}
		if got, ok := VectorCosine(a, b); !ok || !math.IsNaN(got) {
			t.Errorf("nonfinite cosine = %v, %v", got, ok)
		}
	}
}
