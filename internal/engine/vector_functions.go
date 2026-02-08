// Package engine provides vector/embedding functions for tinySQL, enabling
// RAG (Retrieval-Augmented Generation) workloads and vector similarity search.
//
// Supported functions:
//
//	VEC_FROM_JSON(json_string)           – parse "[1.0, 2.0, 3.0]" → []float64
//	VEC_TO_JSON(vector)                  – serialize vector → JSON string
//	VEC_DIM(vector)                      – number of dimensions
//	VEC_NORM(vector)                     – L2 (Euclidean) norm
//	VEC_NORMALIZE(vector)                – unit-length normalised copy
//	VEC_ADD(v1, v2)                      – element-wise addition
//	VEC_SUB(v1, v2)                      – element-wise subtraction
//	VEC_MUL(v1, v2)                      – element-wise multiplication (Hadamard)
//	VEC_SCALE(vector, scalar)            – scalar multiplication
//	VEC_DOT(v1, v2)                      – dot / inner product
//	VEC_COSINE_SIMILARITY(v1, v2)        – cosine similarity  ∈ [-1, 1]
//	VEC_COSINE_DISTANCE(v1, v2)          – 1 - cosine similarity ∈ [0, 2]
//	VEC_L2_DISTANCE(v1, v2)              – Euclidean distance
//	VEC_MANHATTAN_DISTANCE(v1, v2)       – L1 / Manhattan distance
//	VEC_DISTANCE(v1, v2 [, metric])      – generic distance, metric = 'cosine' | 'l2' | 'manhattan' | 'dot'
//	VEC_SLICE(vector, start, length)     – sub-vector extraction
//	VEC_CONCAT(v1, v2)                   – vector concatenation
//	VEC_QUANTIZE(vector, bits)           – quantize to reduced precision (8/16 bit simulation)
//	VEC_RANDOM(dimensions [, seed])      – random unit vector of given dimension
package engine

import (
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"strings"
)

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// toVec extracts a []float64 from an expression argument, supporting
// direct []float64 values, JSON strings, and []any slices.
func toVec(env ExecEnv, expr Expr, row Row) ([]float64, error) {
	v, err := evalExpr(env, expr, row)
	if err != nil {
		return nil, err
	}
	switch x := v.(type) {
	case []float64:
		return x, nil
	case string:
		var arr []float64
		if err := json.Unmarshal([]byte(x), &arr); err == nil {
			return arr, nil
		}
		var anyArr []any
		if err := json.Unmarshal([]byte(x), &anyArr); err == nil {
			f, err := anySliceToFloat64(anyArr)
			if err != nil {
				return nil, err
			}
			return f, nil
		}
		return nil, fmt.Errorf("cannot interpret string as vector: %q", x)
	case []any:
		return anySliceToFloat64(x)
	case nil:
		return nil, fmt.Errorf("NULL is not a valid vector")
	default:
		return nil, fmt.Errorf("cannot convert %T to vector", v)
	}
}

func requireArgs(name string, ex *FuncCall, min, max int) error {
	n := len(ex.Args)
	if n < min || n > max {
		if min == max {
			return fmt.Errorf("%s requires exactly %d argument(s), got %d", name, min, n)
		}
		return fmt.Errorf("%s requires %d–%d arguments, got %d", name, min, max, n)
	}
	return nil
}

// ---------------------------------------------------------------------------
// VEC_FROM_JSON – parse JSON array → vector
// ---------------------------------------------------------------------------

func evalVecFromJSON(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if err := requireArgs("VEC_FROM_JSON", ex, 1, 1); err != nil {
		return nil, err
	}
	val, err := evalExpr(env, ex.Args[0], row)
	if err != nil {
		return nil, err
	}
	s, ok := val.(string)
	if !ok {
		return nil, fmt.Errorf("VEC_FROM_JSON requires a string argument, got %T", val)
	}
	var arr []float64
	if err := json.Unmarshal([]byte(s), &arr); err != nil {
		// Try via []any
		var anyArr []any
		if err2 := json.Unmarshal([]byte(s), &anyArr); err2 != nil {
			return nil, fmt.Errorf("VEC_FROM_JSON: invalid JSON array: %v", err)
		}
		arr2, err2 := anySliceToFloat64(anyArr)
		if err2 != nil {
			return nil, fmt.Errorf("VEC_FROM_JSON: %v", err2)
		}
		return arr2, nil
	}
	return arr, nil
}

// ---------------------------------------------------------------------------
// VEC_TO_JSON – serialize vector → JSON string
// ---------------------------------------------------------------------------

func evalVecToJSON(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if err := requireArgs("VEC_TO_JSON", ex, 1, 1); err != nil {
		return nil, err
	}
	vec, err := toVec(env, ex.Args[0], row)
	if err != nil {
		return nil, fmt.Errorf("VEC_TO_JSON: %w", err)
	}
	b, err := json.Marshal(vec)
	if err != nil {
		return nil, fmt.Errorf("VEC_TO_JSON: %w", err)
	}
	return string(b), nil
}

// ---------------------------------------------------------------------------
// VEC_DIM – number of dimensions
// ---------------------------------------------------------------------------

func evalVecDim(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if err := requireArgs("VEC_DIM", ex, 1, 1); err != nil {
		return nil, err
	}
	vec, err := toVec(env, ex.Args[0], row)
	if err != nil {
		return nil, fmt.Errorf("VEC_DIM: %w", err)
	}
	return len(vec), nil
}

// ---------------------------------------------------------------------------
// VEC_NORM – L2 norm
// ---------------------------------------------------------------------------

func evalVecNorm(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if err := requireArgs("VEC_NORM", ex, 1, 1); err != nil {
		return nil, err
	}
	vec, err := toVec(env, ex.Args[0], row)
	if err != nil {
		return nil, fmt.Errorf("VEC_NORM: %w", err)
	}
	var sum float64
	for _, v := range vec {
		sum += v * v
	}
	return math.Sqrt(sum), nil
}

// ---------------------------------------------------------------------------
// VEC_NORMALIZE – return unit vector
// ---------------------------------------------------------------------------

func evalVecNormalize(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if err := requireArgs("VEC_NORMALIZE", ex, 1, 1); err != nil {
		return nil, err
	}
	vec, err := toVec(env, ex.Args[0], row)
	if err != nil {
		return nil, fmt.Errorf("VEC_NORMALIZE: %w", err)
	}
	var sum float64
	for _, v := range vec {
		sum += v * v
	}
	norm := math.Sqrt(sum)
	if norm == 0 {
		return nil, fmt.Errorf("VEC_NORMALIZE: zero-length vector cannot be normalized")
	}
	out := make([]float64, len(vec))
	for i, v := range vec {
		out[i] = v / norm
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// VEC_ADD – element-wise addition
// ---------------------------------------------------------------------------

func evalVecAdd(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if err := requireArgs("VEC_ADD", ex, 2, 2); err != nil {
		return nil, err
	}
	a, err := toVec(env, ex.Args[0], row)
	if err != nil {
		return nil, fmt.Errorf("VEC_ADD arg1: %w", err)
	}
	b, err := toVec(env, ex.Args[1], row)
	if err != nil {
		return nil, fmt.Errorf("VEC_ADD arg2: %w", err)
	}
	if len(a) != len(b) {
		return nil, fmt.Errorf("VEC_ADD: dimension mismatch %d vs %d", len(a), len(b))
	}
	out := make([]float64, len(a))
	for i := range a {
		out[i] = a[i] + b[i]
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// VEC_SUB – element-wise subtraction
// ---------------------------------------------------------------------------

func evalVecSub(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if err := requireArgs("VEC_SUB", ex, 2, 2); err != nil {
		return nil, err
	}
	a, err := toVec(env, ex.Args[0], row)
	if err != nil {
		return nil, fmt.Errorf("VEC_SUB arg1: %w", err)
	}
	b, err := toVec(env, ex.Args[1], row)
	if err != nil {
		return nil, fmt.Errorf("VEC_SUB arg2: %w", err)
	}
	if len(a) != len(b) {
		return nil, fmt.Errorf("VEC_SUB: dimension mismatch %d vs %d", len(a), len(b))
	}
	out := make([]float64, len(a))
	for i := range a {
		out[i] = a[i] - b[i]
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// VEC_MUL – element-wise (Hadamard) product
// ---------------------------------------------------------------------------

func evalVecMul(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if err := requireArgs("VEC_MUL", ex, 2, 2); err != nil {
		return nil, err
	}
	a, err := toVec(env, ex.Args[0], row)
	if err != nil {
		return nil, fmt.Errorf("VEC_MUL arg1: %w", err)
	}
	b, err := toVec(env, ex.Args[1], row)
	if err != nil {
		return nil, fmt.Errorf("VEC_MUL arg2: %w", err)
	}
	if len(a) != len(b) {
		return nil, fmt.Errorf("VEC_MUL: dimension mismatch %d vs %d", len(a), len(b))
	}
	out := make([]float64, len(a))
	for i := range a {
		out[i] = a[i] * b[i]
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// VEC_SCALE – scalar multiplication
// ---------------------------------------------------------------------------

func evalVecScale(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if err := requireArgs("VEC_SCALE", ex, 2, 2); err != nil {
		return nil, err
	}
	vec, err := toVec(env, ex.Args[0], row)
	if err != nil {
		return nil, fmt.Errorf("VEC_SCALE arg1: %w", err)
	}
	sVal, err := evalExpr(env, ex.Args[1], row)
	if err != nil {
		return nil, fmt.Errorf("VEC_SCALE arg2: %w", err)
	}
	scalar, err := toFloat64(sVal)
	if err != nil {
		return nil, fmt.Errorf("VEC_SCALE scalar: %w", err)
	}
	out := make([]float64, len(vec))
	for i, v := range vec {
		out[i] = v * scalar
	}
	return out, nil
}

// toFloat64 converts a numeric value to float64.
func toFloat64(v any) (float64, error) {
	switch x := v.(type) {
	case float64:
		return x, nil
	case int:
		return float64(x), nil
	case int64:
		return float64(x), nil
	default:
		return 0, fmt.Errorf("expected numeric, got %T", v)
	}
}

// ---------------------------------------------------------------------------
// VEC_DOT – dot product / inner product
// ---------------------------------------------------------------------------

func evalVecDot(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if err := requireArgs("VEC_DOT", ex, 2, 2); err != nil {
		return nil, err
	}
	a, err := toVec(env, ex.Args[0], row)
	if err != nil {
		return nil, fmt.Errorf("VEC_DOT arg1: %w", err)
	}
	b, err := toVec(env, ex.Args[1], row)
	if err != nil {
		return nil, fmt.Errorf("VEC_DOT arg2: %w", err)
	}
	if len(a) != len(b) {
		return nil, fmt.Errorf("VEC_DOT: dimension mismatch %d vs %d", len(a), len(b))
	}
	var dot float64
	for i := range a {
		dot += a[i] * b[i]
	}
	return dot, nil
}

// ---------------------------------------------------------------------------
// VEC_COSINE_SIMILARITY – cosine similarity ∈ [-1, 1]
// ---------------------------------------------------------------------------

func evalVecCosineSimilarity(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if err := requireArgs("VEC_COSINE_SIMILARITY", ex, 2, 2); err != nil {
		return nil, err
	}
	a, err := toVec(env, ex.Args[0], row)
	if err != nil {
		return nil, fmt.Errorf("VEC_COSINE_SIMILARITY arg1: %w", err)
	}
	b, err := toVec(env, ex.Args[1], row)
	if err != nil {
		return nil, fmt.Errorf("VEC_COSINE_SIMILARITY arg2: %w", err)
	}
	sim, err := cosineSimilarity(a, b)
	if err != nil {
		return nil, fmt.Errorf("VEC_COSINE_SIMILARITY: %w", err)
	}
	return sim, nil
}

func cosineSimilarity(a, b []float64) (float64, error) {
	if len(a) != len(b) {
		return 0, fmt.Errorf("dimension mismatch %d vs %d", len(a), len(b))
	}
	var dot, normA, normB float64
	for i := range a {
		dot += a[i] * b[i]
		normA += a[i] * a[i]
		normB += b[i] * b[i]
	}
	denom := math.Sqrt(normA) * math.Sqrt(normB)
	if denom == 0 {
		return 0, fmt.Errorf("zero-length vector")
	}
	return dot / denom, nil
}

// ---------------------------------------------------------------------------
// VEC_COSINE_DISTANCE – 1 - cosine similarity ∈ [0, 2]
// ---------------------------------------------------------------------------

func evalVecCosineDistance(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if err := requireArgs("VEC_COSINE_DISTANCE", ex, 2, 2); err != nil {
		return nil, err
	}
	a, err := toVec(env, ex.Args[0], row)
	if err != nil {
		return nil, fmt.Errorf("VEC_COSINE_DISTANCE arg1: %w", err)
	}
	b, err := toVec(env, ex.Args[1], row)
	if err != nil {
		return nil, fmt.Errorf("VEC_COSINE_DISTANCE arg2: %w", err)
	}
	sim, err := cosineSimilarity(a, b)
	if err != nil {
		return nil, fmt.Errorf("VEC_COSINE_DISTANCE: %w", err)
	}
	return 1.0 - sim, nil
}

// ---------------------------------------------------------------------------
// VEC_L2_DISTANCE – Euclidean distance
// ---------------------------------------------------------------------------

func evalVecL2Distance(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if err := requireArgs("VEC_L2_DISTANCE", ex, 2, 2); err != nil {
		return nil, err
	}
	a, err := toVec(env, ex.Args[0], row)
	if err != nil {
		return nil, fmt.Errorf("VEC_L2_DISTANCE arg1: %w", err)
	}
	b, err := toVec(env, ex.Args[1], row)
	if err != nil {
		return nil, fmt.Errorf("VEC_L2_DISTANCE arg2: %w", err)
	}
	if len(a) != len(b) {
		return nil, fmt.Errorf("VEC_L2_DISTANCE: dimension mismatch %d vs %d", len(a), len(b))
	}
	var sum float64
	for i := range a {
		d := a[i] - b[i]
		sum += d * d
	}
	return math.Sqrt(sum), nil
}

// ---------------------------------------------------------------------------
// VEC_MANHATTAN_DISTANCE – L1 / city-block distance
// ---------------------------------------------------------------------------

func evalVecManhattanDistance(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if err := requireArgs("VEC_MANHATTAN_DISTANCE", ex, 2, 2); err != nil {
		return nil, err
	}
	a, err := toVec(env, ex.Args[0], row)
	if err != nil {
		return nil, fmt.Errorf("VEC_MANHATTAN_DISTANCE arg1: %w", err)
	}
	b, err := toVec(env, ex.Args[1], row)
	if err != nil {
		return nil, fmt.Errorf("VEC_MANHATTAN_DISTANCE arg2: %w", err)
	}
	if len(a) != len(b) {
		return nil, fmt.Errorf("VEC_MANHATTAN_DISTANCE: dimension mismatch %d vs %d", len(a), len(b))
	}
	var sum float64
	for i := range a {
		sum += math.Abs(a[i] - b[i])
	}
	return sum, nil
}

// ---------------------------------------------------------------------------
// VEC_DISTANCE – generic distance function with metric selection
// VEC_DISTANCE(v1, v2)                → defaults to 'cosine'
// VEC_DISTANCE(v1, v2, 'cosine')
// VEC_DISTANCE(v1, v2, 'l2')
// VEC_DISTANCE(v1, v2, 'manhattan')
// VEC_DISTANCE(v1, v2, 'dot')
// ---------------------------------------------------------------------------

func evalVecDistance(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if err := requireArgs("VEC_DISTANCE", ex, 2, 3); err != nil {
		return nil, err
	}
	a, err := toVec(env, ex.Args[0], row)
	if err != nil {
		return nil, fmt.Errorf("VEC_DISTANCE arg1: %w", err)
	}
	b, err := toVec(env, ex.Args[1], row)
	if err != nil {
		return nil, fmt.Errorf("VEC_DISTANCE arg2: %w", err)
	}

	metric := "cosine"
	if len(ex.Args) == 3 {
		mv, err := evalExpr(env, ex.Args[2], row)
		if err != nil {
			return nil, fmt.Errorf("VEC_DISTANCE metric: %w", err)
		}
		ms, ok := mv.(string)
		if !ok {
			return nil, fmt.Errorf("VEC_DISTANCE: metric must be a string, got %T", mv)
		}
		metric = strings.ToLower(strings.TrimSpace(ms))
	}

	if len(a) != len(b) {
		return nil, fmt.Errorf("VEC_DISTANCE: dimension mismatch %d vs %d", len(a), len(b))
	}

	switch metric {
	case "cosine":
		sim, err := cosineSimilarity(a, b)
		if err != nil {
			return nil, err
		}
		return 1.0 - sim, nil
	case "l2", "euclidean":
		var sum float64
		for i := range a {
			d := a[i] - b[i]
			sum += d * d
		}
		return math.Sqrt(sum), nil
	case "manhattan", "l1":
		var sum float64
		for i := range a {
			sum += math.Abs(a[i] - b[i])
		}
		return sum, nil
	case "dot", "inner_product":
		var dot float64
		for i := range a {
			dot += a[i] * b[i]
		}
		// For distance: lower = more similar, so negate dot product
		return -dot, nil
	default:
		return nil, fmt.Errorf("VEC_DISTANCE: unknown metric %q (supported: cosine, l2, manhattan, dot)", metric)
	}
}

// ---------------------------------------------------------------------------
// VEC_SLICE – extract sub-vector
// VEC_SLICE(vector, start, length)
// ---------------------------------------------------------------------------

func evalVecSlice(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if err := requireArgs("VEC_SLICE", ex, 3, 3); err != nil {
		return nil, err
	}
	vec, err := toVec(env, ex.Args[0], row)
	if err != nil {
		return nil, fmt.Errorf("VEC_SLICE arg1: %w", err)
	}
	startVal, err := evalExpr(env, ex.Args[1], row)
	if err != nil {
		return nil, fmt.Errorf("VEC_SLICE start: %w", err)
	}
	lenVal, err := evalExpr(env, ex.Args[2], row)
	if err != nil {
		return nil, fmt.Errorf("VEC_SLICE length: %w", err)
	}

	start, err := toInt(startVal)
	if err != nil {
		return nil, fmt.Errorf("VEC_SLICE start: %w", err)
	}
	length, err := toInt(lenVal)
	if err != nil {
		return nil, fmt.Errorf("VEC_SLICE length: %w", err)
	}

	if start < 0 || start >= len(vec) {
		return nil, fmt.Errorf("VEC_SLICE: start %d out of bounds (dim=%d)", start, len(vec))
	}
	end := start + length
	if end > len(vec) {
		end = len(vec)
	}
	out := make([]float64, end-start)
	copy(out, vec[start:end])
	return out, nil
}

func toInt(v any) (int, error) {
	switch x := v.(type) {
	case int:
		return x, nil
	case int64:
		return int(x), nil
	case float64:
		return int(x), nil
	default:
		return 0, fmt.Errorf("expected integer, got %T", v)
	}
}

// ---------------------------------------------------------------------------
// VEC_CONCAT – concatenate two vectors
// ---------------------------------------------------------------------------

func evalVecConcat(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if err := requireArgs("VEC_CONCAT", ex, 2, 2); err != nil {
		return nil, err
	}
	a, err := toVec(env, ex.Args[0], row)
	if err != nil {
		return nil, fmt.Errorf("VEC_CONCAT arg1: %w", err)
	}
	b, err := toVec(env, ex.Args[1], row)
	if err != nil {
		return nil, fmt.Errorf("VEC_CONCAT arg2: %w", err)
	}
	out := make([]float64, len(a)+len(b))
	copy(out, a)
	copy(out[len(a):], b)
	return out, nil
}

// ---------------------------------------------------------------------------
// VEC_QUANTIZE – reduce precision (simulate int8/int16 quantization)
// VEC_QUANTIZE(vector, bits)   bits = 8 or 16
// ---------------------------------------------------------------------------

func evalVecQuantize(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if err := requireArgs("VEC_QUANTIZE", ex, 2, 2); err != nil {
		return nil, err
	}
	vec, err := toVec(env, ex.Args[0], row)
	if err != nil {
		return nil, fmt.Errorf("VEC_QUANTIZE arg1: %w", err)
	}
	bitsVal, err := evalExpr(env, ex.Args[1], row)
	if err != nil {
		return nil, fmt.Errorf("VEC_QUANTIZE bits: %w", err)
	}
	bits, err := toInt(bitsVal)
	if err != nil {
		return nil, fmt.Errorf("VEC_QUANTIZE bits: %w", err)
	}

	if bits != 8 && bits != 16 {
		return nil, fmt.Errorf("VEC_QUANTIZE: bits must be 8 or 16, got %d", bits)
	}

	// Find min/max for normalization
	if len(vec) == 0 {
		return vec, nil
	}
	minV, maxV := vec[0], vec[0]
	for _, v := range vec[1:] {
		if v < minV {
			minV = v
		}
		if v > maxV {
			maxV = v
		}
	}

	rangeV := maxV - minV
	if rangeV == 0 {
		// All values are the same
		out := make([]float64, len(vec))
		for i := range out {
			out[i] = vec[0]
		}
		return out, nil
	}

	levels := math.Pow(2, float64(bits)) - 1
	out := make([]float64, len(vec))
	for i, v := range vec {
		// Quantize: normalize to [0,1], scale to levels, round, scale back
		normalized := (v - minV) / rangeV
		quantized := math.Round(normalized * levels)
		out[i] = quantized/levels*rangeV + minV
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// VEC_RANDOM – random unit vector of given dimension
// VEC_RANDOM(dimensions)
// VEC_RANDOM(dimensions, seed)
// ---------------------------------------------------------------------------

func evalVecRandom(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if err := requireArgs("VEC_RANDOM", ex, 1, 2); err != nil {
		return nil, err
	}
	dimVal, err := evalExpr(env, ex.Args[0], row)
	if err != nil {
		return nil, fmt.Errorf("VEC_RANDOM dim: %w", err)
	}
	dim, err := toInt(dimVal)
	if err != nil {
		return nil, fmt.Errorf("VEC_RANDOM dim: %w", err)
	}
	if dim <= 0 || dim > 65536 {
		return nil, fmt.Errorf("VEC_RANDOM: dimensions must be 1–65536, got %d", dim)
	}

	var rng *rand.Rand
	if len(ex.Args) == 2 {
		seedVal, err := evalExpr(env, ex.Args[1], row)
		if err != nil {
			return nil, fmt.Errorf("VEC_RANDOM seed: %w", err)
		}
		seed, err := toInt(seedVal)
		if err != nil {
			return nil, fmt.Errorf("VEC_RANDOM seed: %w", err)
		}
		rng = rand.New(rand.NewSource(int64(seed)))
	} else {
		rng = rand.New(rand.NewSource(rand.Int63()))
	}

	vec := make([]float64, dim)
	var norm float64
	for i := range vec {
		vec[i] = rng.NormFloat64()
		norm += vec[i] * vec[i]
	}
	// Normalize to unit length
	norm = math.Sqrt(norm)
	if norm > 0 {
		for i := range vec {
			vec[i] /= norm
		}
	}
	return vec, nil
}

// ---------------------------------------------------------------------------
// VEC_AVG – compute the element-wise average of vectors (aggregate helper)
// VEC_AVG(v1, v2) for scalar use; also used as aggregate
// ---------------------------------------------------------------------------

func evalVecAvg(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if err := requireArgs("VEC_AVG", ex, 2, 2); err != nil {
		return nil, err
	}
	a, err := toVec(env, ex.Args[0], row)
	if err != nil {
		return nil, fmt.Errorf("VEC_AVG arg1: %w", err)
	}
	b, err := toVec(env, ex.Args[1], row)
	if err != nil {
		return nil, fmt.Errorf("VEC_AVG arg2: %w", err)
	}
	if len(a) != len(b) {
		return nil, fmt.Errorf("VEC_AVG: dimension mismatch %d vs %d", len(a), len(b))
	}
	out := make([]float64, len(a))
	for i := range a {
		out[i] = (a[i] + b[i]) / 2.0
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// getVectorFunctions returns all vector function handlers
// ---------------------------------------------------------------------------

func getVectorFunctions() map[string]funcHandler {
	return map[string]funcHandler{
		// Parsing / serialization
		"VEC_FROM_JSON": evalVecFromJSON,
		"VEC_TO_JSON":   evalVecToJSON,

		// Introspection
		"VEC_DIM":  evalVecDim,
		"VEC_NORM": evalVecNorm,

		// Normalization
		"VEC_NORMALIZE": evalVecNormalize,

		// Arithmetic
		"VEC_ADD":   evalVecAdd,
		"VEC_SUB":   evalVecSub,
		"VEC_MUL":   evalVecMul,
		"VEC_SCALE": evalVecScale,

		// Similarity & distance
		"VEC_DOT":                evalVecDot,
		"VEC_DOT_PRODUCT":        evalVecDot,
		"VEC_INNER_PRODUCT":      evalVecDot,
		"VEC_COSINE_SIMILARITY":  evalVecCosineSimilarity,
		"VEC_COSINE_DISTANCE":    evalVecCosineDistance,
		"VEC_L2_DISTANCE":        evalVecL2Distance,
		"VEC_EUCLIDEAN_DISTANCE": evalVecL2Distance,
		"VEC_MANHATTAN_DISTANCE": evalVecManhattanDistance,
		"VEC_L1_DISTANCE":        evalVecManhattanDistance,
		"VEC_DISTANCE":           evalVecDistance,

		// Manipulation
		"VEC_SLICE":  evalVecSlice,
		"VEC_CONCAT": evalVecConcat,

		// Quantization
		"VEC_QUANTIZE": evalVecQuantize,

		// Generation
		"VEC_RANDOM": evalVecRandom,

		// Aggregation helper
		"VEC_AVG": evalVecAvg,
	}
}
