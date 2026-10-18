package engine

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
)

// vecFromValue is toVec after expression evaluation. Native vectors are
// borrowed; conversions allocate their own result.
func vecFromValue(v any) ([]float64, error) {
	switch x := v.(type) {
	case []float64:
		return x, nil
	case string:
		arr, err := parseVectorJSON(x)
		if err != nil {
			// Keep element-specific diagnostics for valid nonnumeric arrays.
			var anyArr []any
			if json.Unmarshal([]byte(x), &anyArr) == nil {
				return anySliceToFloat64(anyArr)
			}
			return nil, fmt.Errorf("cannot interpret string as vector: %q", x)
		}
		return arr, nil
	case []any:
		return anySliceToFloat64(x)
	case nil:
		return nil, fmt.Errorf("NULL is not a valid vector")
	default:
		return nil, fmt.Errorf("cannot convert %T to vector", v)
	}
}

// parseVectorJSON parses flat numeric arrays without reflection or repeated
// output growth. Other JSON shapes and invalid input go through encoding/json
// for the usual errors. Like unmarshalling into []float64, JSON null is a nil
// vector and null elements become zero.
func parseVectorJSON(s string) ([]float64, error) {
	text := strings.Trim(s, " \t\r\n")
	if text == "null" {
		return nil, nil
	}
	if len(text) >= 2 && text[0] == '[' && text[len(text)-1] == ']' {
		body := strings.Trim(text[1:len(text)-1], " \t\r\n")
		if len(body) == 0 {
			return []float64{}, nil
		}
		// Commas inside a string or nested structure are not dimensions.
		// Decline these shapes before sizing a numeric output buffer.
		if strings.IndexByte(body, '"') >= 0 || strings.IndexByte(body, '[') >= 0 || strings.IndexByte(body, '{') >= 0 {
			goto fallback
		}
		dimensions := strings.Count(body, ",") + 1
		if dimensions > int(^uint(0)>>1)/8 {
			return nil, fmt.Errorf("vector too large")
		}
		out := make([]float64, dimensions)
		for i := range out {
			var part string
			part, body, _ = strings.Cut(body, ",")
			part = strings.Trim(part, " \t\r\n")
			if part == "null" {
				continue
			}
			if !isVectorJSONNumber(part) {
				goto fallback
			}
			f, err := strconv.ParseFloat(part, 64)
			if err != nil {
				goto fallback
			}
			out[i] = f
		}
		return out, nil
	}

fallback:
	var out []float64
	err := json.Unmarshal([]byte(s), &out)
	return out, err
}

// ParseFloat accepts Go forms (leading +, hexadecimal, NaN, underscores) that
// JSON does not. Check JSON's number grammar before using it on a token.
func isVectorJSONNumber(s string) bool {
	i := 0
	if i < len(s) && s[i] == '-' {
		i++
	}
	if i >= len(s) {
		return false
	}
	if s[i] == '0' {
		i++
	} else {
		if s[i] < '1' || s[i] > '9' {
			return false
		}
		for i < len(s) && s[i] >= '0' && s[i] <= '9' {
			i++
		}
	}
	if i < len(s) && s[i] == '.' {
		i++
		start := i
		for i < len(s) && s[i] >= '0' && s[i] <= '9' {
			i++
		}
		if i == start {
			return false
		}
	}
	if i < len(s) && (s[i] == 'e' || s[i] == 'E') {
		i++
		if i < len(s) && (s[i] == '+' || s[i] == '-') {
			i++
		}
		start := i
		for i < len(s) && s[i] >= '0' && s[i] <= '9' {
			i++
		}
		if i == start {
			return false
		}
	}
	return i == len(s)
}

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
	arr, err := parseVectorJSON(s)
	if err != nil {
		var anyArr []any
		if json.Unmarshal([]byte(s), &anyArr) != nil {
			return nil, fmt.Errorf("VEC_FROM_JSON: invalid JSON array: %v", err)
		}
		arr, err = anySliceToFloat64(anyArr)
		if err != nil {
			return nil, fmt.Errorf("VEC_FROM_JSON: %v", err)
		}
	}
	return arr, nil
}

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

// VEC_TO_BYTES returns lowercase hexadecimal text containing little-endian
// float32 values. Build the text directly instead of retaining a binary copy.
func evalVecToBytes(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if err := requireArgs("VEC_TO_BYTES", ex, 1, 1); err != nil {
		return nil, err
	}
	vec, err := toVec(env, ex.Args[0], row)
	if err != nil {
		return nil, fmt.Errorf("VEC_TO_BYTES: %w", err)
	}
	return vectorToHex(vec)
}

func vectorToHex(vec []float64) (string, error) {
	if len(vec) > int(^uint(0)>>1)/8 {
		return "", fmt.Errorf("VEC_TO_BYTES: vector too large")
	}
	const digits = "0123456789abcdef"
	var out strings.Builder
	out.Grow(len(vec) * 8)
	var encoded [8]byte
	for _, v := range vec {
		bits := math.Float32bits(float32(v))
		for j := 0; j < 4; j++ {
			b := byte(bits >> (j * 8))
			encoded[j*2], encoded[j*2+1] = digits[b>>4], digits[b&15]
		}
		out.Write(encoded[:])
	}
	return out.String(), nil
}

func evalVecFromBytes(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if err := requireArgs("VEC_FROM_BYTES", ex, 1, 1); err != nil {
		return nil, err
	}
	val, err := evalExpr(env, ex.Args[0], row)
	if err != nil {
		return nil, err
	}
	switch x := val.(type) {
	case string:
		return vectorFromHex(strings.TrimSpace(x))
	case []byte:
		if len(x)%4 != 0 {
			return nil, fmt.Errorf("VEC_FROM_BYTES: byte length %d is not a multiple of 4", len(x))
		}
		if len(x)/4 > int(^uint(0)>>1)/8 {
			return nil, fmt.Errorf("VEC_FROM_BYTES: vector too large")
		}
		vec := make([]float64, len(x)/4)
		for i := range vec {
			vec[i] = float64(math.Float32frombits(binary.LittleEndian.Uint32(x[i*4:])))
		}
		return vec, nil
	case nil:
		return nil, fmt.Errorf("VEC_FROM_BYTES: NULL input")
	default:
		return nil, fmt.Errorf("VEC_FROM_BYTES: expected string or bytes, got %T", val)
	}
}

func vectorFromHex(s string) ([]float64, error) {
	if len(s)%8 != 0 {
		// Preserve invalid-hex errors before byte-length errors for malformed
		// inputs, without an intermediate binary buffer on the normal path.
		if _, err := hexDecodeString(s); err != nil {
			return nil, fmt.Errorf("VEC_FROM_BYTES: invalid hex: %w", err)
		}
		return nil, fmt.Errorf("VEC_FROM_BYTES: byte length %d is not a multiple of 4", len(s)/2)
	}
	vec := make([]float64, len(s)/8)
	for i := range vec {
		var bits uint32
		for j := 0; j < 8; j += 2 {
			pos := i*8 + j
			hi, ok1 := hexNibble(s[pos])
			lo, ok2 := hexNibble(s[pos+1])
			if !ok1 || !ok2 {
				return nil, fmt.Errorf("VEC_FROM_BYTES: invalid hex: invalid hex character at position %d", pos)
			}
			bits |= uint32(hi<<4|lo) << (j * 4)
		}
		vec[i] = float64(math.Float32frombits(bits))
	}
	return vec, nil
}

func hexDecodeString(s string) ([]byte, error) {
	if len(s)%2 != 0 {
		return nil, fmt.Errorf("odd hex length")
	}
	out := make([]byte, len(s)/2)
	for i := 0; i < len(s); i += 2 {
		hi, ok1 := hexNibble(s[i])
		lo, ok2 := hexNibble(s[i+1])
		if !ok1 || !ok2 {
			return nil, fmt.Errorf("invalid hex character at position %d", i)
		}
		out[i/2] = hi<<4 | lo
	}
	return out, nil
}

func hexNibble(c byte) (byte, bool) {
	n := vectorHexNibbles[c]
	return n, n != 0xff
}

// A constant lookup avoids repeated numeric/lowercase/uppercase branches for
// every nibble. Invalid characters, including non-ASCII bytes, map to 0xff.
const vectorHexNibbles = "" +
	"\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff" +
	"\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff" +
	"\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff" +
	"\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\xff\xff\xff\xff\xff\xff" +
	"\xff\x0a\x0b\x0c\x0d\x0e\x0f\xff\xff\xff\xff\xff\xff\xff\xff\xff" +
	"\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff" +
	"\xff\x0a\x0b\x0c\x0d\x0e\x0f\xff\xff\xff\xff\xff\xff\xff\xff\xff" +
	"\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff" +
	"\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff" +
	"\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff" +
	"\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff" +
	"\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff" +
	"\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff" +
	"\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff" +
	"\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff" +
	"\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff"
