package engine

import (
	"math"
	"strings"
	"testing"
)

func TestVectorNumericFunctionsExtremeMagnitudes(t *testing.T) {
	for _, scale := range []float64{1e200, 1e-200, math.SmallestNonzeroFloat64} {
		vec := []float64{3 * scale, 4 * scale}
		call := &FuncCall{Name: "VEC_NORM", Args: []Expr{&Literal{Val: vec}}}
		got, err := evalVecNorm(ExecEnv{}, call, nil)
		if err != nil || math.IsInf(got.(float64), 0) || math.Abs(got.(float64)/(5*scale)-1) > 2e-15 {
			t.Fatalf("VEC_NORM(%v) = %v, %v", vec, got, err)
		}
		call.Name = "VEC_NORMALIZE"
		got, err = evalVecNormalize(ExecEnv{}, call, nil)
		if err != nil {
			t.Fatal(err)
		}
		out := got.([]float64)
		if math.Abs(out[0]-0.6) > 2e-15 || math.Abs(out[1]-0.8) > 2e-15 {
			t.Fatalf("VEC_NORMALIZE(%v) = %v", vec, out)
		}
		out[0] = 42
		if vec[0] != 3*scale {
			t.Fatal("normalization result aliases its argument")
		}
		if got, err := cosineSimilarity(vec, []float64{4, 3}); err != nil || math.IsNaN(got) || math.Abs(got-0.96) > 2e-15 {
			t.Fatalf("cosine(%v) = %v, %v", vec, got, err)
		}
	}
	for _, vec := range [][]float64{nil, {0, math.Copysign(0, -1)}} {
		_, err := evalVecNormalize(ExecEnv{}, &FuncCall{Name: "VEC_NORMALIZE", Args: []Expr{&Literal{Val: vec}}}, nil)
		if err == nil || !strings.Contains(err.Error(), "zero-length") {
			t.Fatalf("VEC_NORMALIZE(%v): %v", vec, err)
		}
	}
	if _, err := cosineSimilarity([]float64{1}, []float64{1, 2}); err == nil || !strings.Contains(err.Error(), "dimension mismatch") {
		t.Fatalf("cosine dimension error: %v", err)
	}
}

func TestVectorQuantizeExtremeRangeAndOwnership(t *testing.T) {
	for _, bits := range []int{8, 16} {
		vec := []float64{-math.MaxFloat64, -1e308, 0, 1e308, math.MaxFloat64}
		call := &FuncCall{Name: "VEC_QUANTIZE", Args: []Expr{&Literal{Val: vec}, &Literal{Val: bits}}}
		got, err := evalVecQuantize(ExecEnv{}, call, nil)
		if err != nil {
			t.Fatal(err)
		}
		out := got.([]float64)
		maxError := math.MaxFloat64 / float64((uint32(1)<<uint(bits))-1)
		for i, v := range out {
			if math.IsNaN(v) || math.IsInf(v, 0) || math.Abs(v-vec[i]) > maxError*1.001 {
				t.Errorf("VEC_QUANTIZE(%d bits)[%d] = %v, want near %v", bits, i, v, vec[i])
			}
		}
		if out[0] != vec[0] || out[len(out)-1] != vec[len(vec)-1] {
			t.Fatalf("quantization lost endpoints: %v", out)
		}
		out[0] = 42
		if vec[0] != -math.MaxFloat64 {
			t.Fatal("quantized result aliases its argument")
		}
	}
	negativeZero := math.Copysign(0, -1)
	for _, vec := range [][]float64{{negativeZero, negativeZero}, {math.NaN(), 1}, {math.Inf(1), 1}} {
		got, err := evalVecQuantize(ExecEnv{}, &FuncCall{Name: "VEC_QUANTIZE", Args: []Expr{&Literal{Val: vec}, &Literal{Val: 8}}}, nil)
		if err != nil {
			t.Fatal(err)
		}
		out := got.([]float64)
		if vec[0] == 0 {
			if !math.Signbit(out[0]) || !math.Signbit(out[1]) {
				t.Fatalf("constant signed zero changed: %v", out)
			}
		} else if !math.IsNaN(out[0]) || !math.IsNaN(out[1]) {
			t.Fatalf("nonfinite quantization changed: %v", out)
		}
	}
}

func TestVectorCentroidOwnershipAndShape(t *testing.T) {
	a, b := []float64{1, 2, 3}, []float64{3, 4, 5}
	call := &FuncCall{Name: "VEC_CENTROID", Args: []Expr{&Literal{Val: a}, &Literal{Val: b}}}
	got, err := evalVecCentroid(ExecEnv{}, call, nil)
	if err != nil {
		t.Fatal(err)
	}
	out := got.([]float64)
	for i, want := range []float64{2, 3, 4} {
		if out[i] != want {
			t.Fatalf("centroid = %v", out)
		}
	}
	out[0] = 42
	if a[0] != 1 || b[0] != 3 {
		t.Fatal("centroid mutated its input")
	}
	call.Args[1] = &Literal{Val: []float64{1}}
	if _, err := evalVecCentroid(ExecEnv{}, call, nil); err == nil || !strings.Contains(err.Error(), "arg2: 1 vs 3") {
		t.Fatalf("dimension error: %v", err)
	}
	call.Args[0], call.Args[1] = &Literal{Val: []float64{}}, &Literal{Val: []float64{}}
	if got, err := evalVecCentroid(ExecEnv{}, call, nil); err != nil || len(got.([]float64)) != 0 {
		t.Fatalf("empty centroid: %v, %v", got, err)
	}
}
