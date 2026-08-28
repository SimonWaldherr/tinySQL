// Type inference and coercion. Every value entering a column passes through
// here, which is where SQLite's type affinities are applied.
package engine

import (
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/engine/sqlval"
	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// valueText forwards to sqlval.ValueText. The implementation lives in
// internal/engine/sqlval because internal/engine/search needs it too and
// cannot import engine (see that package's doc comment); this forwarder keeps
// engine's existing call sites unchanged.
func valueText(v any) string { return sqlval.ValueText(v) }

func inferType(v any) storage.ColType {
	switch v.(type) {
	case int, int64:
		return storage.IntType
	case float64:
		return storage.FloatType
	case bool:
		return storage.BoolType
	case string:
		return storage.TextType
	case []float64:
		return storage.VectorType
	case []byte:
		return storage.BlobType
	case *big.Rat, big.Rat:
		return storage.DecimalType
	case time.Time:
		return storage.DateTimeType
	default:
		return storage.JsonType
	}
}

func coerceToTypeAllowNull(v any, t storage.ColType) (any, error) {
	if v == nil {
		return nil, nil
	}
	switch t {
	case storage.IntType:
		return coerceToInt(v)
	case storage.FloatType:
		return coerceToFloat(v)
	case storage.TextType:
		if s, ok := v.(string); ok {
			return s, nil
		}
		return valueText(v), nil
	case storage.BoolType:
		return coerceToBool(v)
	case storage.JsonType:
		return coerceToJson(v)
	case storage.VectorType:
		return coerceToVector(v)
	case storage.GeometryType:
		return coerceToGeometry(v)
	case storage.BlobType:
		return coerceToBlob(v)
	default:
		return v, nil
	}
}

// coerceColumnValue applies SQLite's documented affinity rules only to
// SQLite-style declarations. Native tinySQL columns retain their existing
// strict conversion behaviour. SQLite affinity conversion is deliberately
// lossless: a value which cannot be represented without changing meaning is
// retained with its original storage class rather than rejected or truncated.
func coerceColumnValue(v any, col storage.Column) (any, error) {
	if v == nil {
		return nil, nil
	}
	switch col.Affinity {
	case storage.AffinityInteger:
		return coerceSQLiteInteger(v)
	case storage.AffinityReal:
		return coerceSQLiteReal(v)
	case storage.AffinityText:
		// SQLite does not coerce BLOB values when applying TEXT affinity.
		if _, ok := v.([]byte); ok {
			return v, nil
		}
		if s, ok := v.(string); ok {
			return s, nil
		}
		return valueText(v), nil
	case storage.AffinityNumeric:
		return coerceSQLiteNumeric(v)
	case storage.AffinityBlob:
		return v, nil
	default:
		return coerceToTypeAllowNull(v, col.Type)
	}
}

func coerceSQLiteInteger(v any) (any, error) {
	switch x := v.(type) {
	case string:
		s := strings.TrimSpace(x)
		i, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			f, floatErr := strconv.ParseFloat(s, 64)
			if floatErr != nil || math.Trunc(f) != f || f < math.MinInt64 || f > math.MaxInt64 {
				return v, nil
			}
			return int(f), nil
		}
		return int(i), nil
	case float64:
		if math.Trunc(x) == x && x >= math.MinInt64 && x <= math.MaxInt64 {
			return int(x), nil
		}
	}
	return v, nil
}

func coerceSQLiteReal(v any) (any, error) {
	switch x := v.(type) {
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(x), 64)
		if err != nil {
			return v, nil
		}
		return f, nil
	case int:
		return float64(x), nil
	case int64:
		return float64(x), nil
	}
	return v, nil
}

func coerceSQLiteNumeric(v any) (any, error) {
	switch x := v.(type) {
	case string:
		s := strings.TrimSpace(x)
		if i, err := strconv.ParseInt(s, 10, 64); err == nil {
			return int(i), nil
		}
		if f, err := strconv.ParseFloat(s, 64); err == nil {
			if math.Trunc(f) == f && f >= math.MinInt64 && f <= math.MaxInt64 {
				return int(f), nil
			}
			return f, nil
		}
	}
	return v, nil
}

// MaxBlobBytes bounds a single BLOB accepted by the SQL executor. It avoids
// integer/codec overflows and accidental unbounded allocations while staying
// comfortably above normal compressed MVT payloads.
const MaxBlobBytes = 64 << 20

func coerceToBlob(v any) (any, error) {
	b, ok := v.([]byte)
	if !ok {
		return nil, fmt.Errorf("cannot convert %T to BLOB", v)
	}
	if len(b) > MaxBlobBytes {
		return nil, fmt.Errorf("BLOB is %d bytes; maximum is %d", len(b), MaxBlobBytes)
	}
	// Driver callers commonly reuse their parameter buffer. The database owns
	// its row bytes, so retain neither the input nor a caller-visible alias.
	return append([]byte(nil), b...), nil
}

func coerceToInt(v any) (any, error) {
	switch x := v.(type) {
	case int:
		return x, nil
	case int64:
		return int(x), nil
	case float64:
		return int(x), nil
	case *big.Rat:
		f, _ := x.Float64()
		return int(f), nil
	case big.Rat:
		f, _ := x.Float64()
		return int(f), nil
	case string:
		n, err := strconv.Atoi(strings.TrimSpace(x))
		if err != nil {
			return nil, fmt.Errorf("cannot convert %q to INT", x)
		}
		return n, nil
	case bool:
		if x {
			return 1, nil
		}
		return 0, nil
	default:
		return nil, fmt.Errorf("cannot convert %T to INT", v)
	}
}

func coerceToBool(v any) (any, error) {
	switch x := v.(type) {
	case bool:
		return x, nil
	case int:
		return x != 0, nil
	case int64:
		// Deliberately its own case, not merged with `int` into `case int,
		// int64:`. In a multi-type case, x keeps the switch's static type
		// (any), so `x != 0` became an interface comparison between
		// any(int64 value) and the untyped constant 0 (defaulting to int)
		// -- different dynamic types are never interface-equal regardless
		// of numeric value, so int64(0) != 0 evaluated to true and
		// coerceToBool(int64(0)) incorrectly returned true instead of
		// false. Splitting the case gives x the concrete type int64, so
		// `x != 0` is a normal numeric comparison again.
		return x != 0, nil
	case float64:
		return x != 0, nil
	case *big.Rat:
		return x.Sign() != 0, nil
	case big.Rat:
		return x.Sign() != 0, nil
	case string:
		s := strings.ToLower(strings.TrimSpace(x))
		return s == "true" || s == "1" || s == "t" || s == "yes", nil
	default:
		return nil, fmt.Errorf("cannot convert %T to BOOL", v)
	}
}

func coerceToJson(v any) (any, error) {
	switch x := v.(type) {
	case string:
		var anyv any
		if json.Unmarshal([]byte(x), &anyv) == nil {
			return anyv, nil
		}
		return x, nil
	default:
		return x, nil
	}
}

// coerceToVector converts a value to []float64 for VECTOR columns.
// Accepts: []float64 (passthrough), JSON string "[1.0, 2.0, 3.0]",
// []any (from JSON parse), or []int.
func coerceToVector(v any) (any, error) {
	switch x := v.(type) {
	case []float64:
		return x, nil
	case string:
		s := strings.TrimSpace(x)
		if s == "" {
			return nil, fmt.Errorf("cannot convert empty string to VECTOR")
		}
		var arr []float64
		if err := json.Unmarshal([]byte(s), &arr); err == nil {
			return arr, nil
		}
		// Try parsing as []any from JSON
		var anyArr []any
		if err := json.Unmarshal([]byte(s), &anyArr); err == nil {
			return anySliceToFloat64(anyArr)
		}
		return nil, fmt.Errorf("cannot convert %q to VECTOR", s)
	case []any:
		return anySliceToFloat64(x)
	case []int:
		out := make([]float64, len(x))
		for i, v := range x {
			out[i] = float64(v)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("cannot convert %T to VECTOR", v)
	}
}

// coerceToGeometry converts a value to canonical GeoJSON text for GEOMETRY
// columns. Accepts a GeoJSON string, []byte, json.RawMessage, or an
// already-decoded map[string]any; validates that its "type" is one of the
// GeoJSON Geometry types (not Feature/FeatureCollection); and returns the
// re-marshaled, canonically-ordered text. See canonicalGeoJSON/
// validateGeometryShape in geo_functions.go, which do the actual decoding
// and validation this just gates by input shape.
func coerceToGeometry(v any) (any, error) {
	switch v.(type) {
	case string, []byte, json.RawMessage, map[string]any:
		return canonicalGeoJSON(v)
	default:
		return nil, fmt.Errorf("cannot convert %T to GEOMETRY", v)
	}
}

// anySliceToFloat64 converts a []any of numeric values to []float64.
func anySliceToFloat64(arr []any) ([]float64, error) {
	out := make([]float64, len(arr))
	for i, v := range arr {
		switch n := v.(type) {
		case float64:
			out[i] = n
		case int:
			out[i] = float64(n)
		case int64:
			out[i] = float64(n)
		case json.Number:
			f, err := n.Float64()
			if err != nil {
				return nil, fmt.Errorf("vector element %d: %w", i, err)
			}
			out[i] = f
		default:
			return nil, fmt.Errorf("vector element %d: cannot convert %T to float64", i, v)
		}
	}
	return out, nil
}
