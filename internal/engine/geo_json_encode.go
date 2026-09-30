// A direct encoder for the geometry objects GIS functions return.
//
// json.Marshal of a map[string]any holding nested []any and boxed float64
// values cost as much as the geometry work that produced it. marshalGeoJSON
// writes the same bytes without reflection for the small set of value types
// geometry results are built from, and hands anything else -- an unexpected
// type, a NaN or infinity, a string that would need escaping -- to json.Marshal
// unchanged, so output and errors are identical either way.
// geo_json_encode_test.go compares the two encoders byte for byte.
package engine

import (
	"encoding/json"
	"math"
	"sort"
	"strconv"
)

// marshalGeoJSON is json.Marshal for geometry result values.
func marshalGeoJSON(v any) ([]byte, error) {
	if out, ok := appendGeoJSON(make([]byte, 0, 256), v, 0); ok {
		return out, nil
	}
	return json.Marshal(v)
}

const geoJSONMaxDepth = 32

// appendGeoJSON appends v to dst, or reports ok=false if it met anything it
// cannot encode exactly as encoding/json would.
func appendGeoJSON(dst []byte, v any, depth int) ([]byte, bool) {
	if depth > geoJSONMaxDepth {
		return dst, false
	}
	switch x := v.(type) {
	case nil:
		return append(dst, "null"...), true
	case bool:
		if x {
			return append(dst, "true"...), true
		}
		return append(dst, "false"...), true
	case string:
		return appendGeoJSONString(dst, x)
	case float64:
		return appendGeoJSONFloat(dst, x)
	case int:
		return strconv.AppendInt(dst, int64(x), 10), true
	case int64:
		return strconv.AppendInt(dst, x, 10), true
	case int32:
		return strconv.AppendInt(dst, int64(x), 10), true
	case uint8:
		return strconv.AppendUint(dst, uint64(x), 10), true
	case []float64:
		if x == nil {
			return append(dst, "null"...), true
		}
		dst = append(dst, '[')
		for i, f := range x {
			if i > 0 {
				dst = append(dst, ',')
			}
			var ok bool
			if dst, ok = appendGeoJSONFloat(dst, f); !ok {
				return dst, false
			}
		}
		return append(dst, ']'), true
	case []string:
		if x == nil {
			return append(dst, "null"...), true
		}
		dst = append(dst, '[')
		for i, value := range x {
			if i > 0 {
				dst = append(dst, ',')
			}
			var ok bool
			if dst, ok = appendGeoJSONString(dst, value); !ok {
				return dst, false
			}
		}
		return append(dst, ']'), true
	case []any:
		if x == nil {
			return append(dst, "null"...), true
		}
		dst = append(dst, '[')
		for i, e := range x {
			if i > 0 {
				dst = append(dst, ',')
			}
			var ok bool
			if dst, ok = appendGeoJSON(dst, e, depth+1); !ok {
				return dst, false
			}
		}
		return append(dst, ']'), true
	case map[string]any:
		if x == nil {
			return append(dst, "null"...), true
		}
		// encoding/json writes object members in sorted key order.
		var stack [8]string
		keys := stack[:0]
		for k := range x {
			keys = append(keys, k)
		}
		if len(keys) > 1 {
			sort.Strings(keys)
		}
		dst = append(dst, '{')
		for i, k := range keys {
			if i > 0 {
				dst = append(dst, ',')
			}
			var ok bool
			if dst, ok = appendGeoJSONString(dst, k); !ok {
				return dst, false
			}
			dst = append(dst, ':')
			if dst, ok = appendGeoJSON(dst, x[k], depth+1); !ok {
				return dst, false
			}
		}
		return append(dst, '}'), true
	}
	return dst, false
}

// appendGeoJSONString writes s only when it is plain printable ASCII that
// encoding/json would copy through verbatim: no quote, backslash, control
// character, or the HTML-sensitive <, > and & it escapes by default.
func appendGeoJSONString(dst []byte, s string) ([]byte, bool) {
	for i := 0; i < len(s); i++ {
		switch c := s[i]; {
		case c < 0x20 || c >= 0x7f || c == '"' || c == '\\' || c == '<' || c == '>' || c == '&':
			return dst, false
		}
	}
	dst = append(dst, '"')
	dst = append(dst, s...)
	return append(dst, '"'), true
}

// appendGeoJSONFloat formats f the way encoding/json does: the shortest
// round-tripping digits, in plain notation except below 1e-6 and from 1e21,
// where it switches to exponent form with the exponent's leading zero removed.
func appendGeoJSONFloat(dst []byte, f float64) ([]byte, bool) {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return dst, false // json.Marshal reports the error
	}
	format := byte('f')
	if abs := math.Abs(f); abs != 0 && (abs < 1e-6 || abs >= 1e21) {
		format = 'e'
	}
	dst = strconv.AppendFloat(dst, f, format, -1, 64)
	if format == 'e' {
		// clean up e-09 to e-9
		if n := len(dst); n >= 4 && dst[n-4] == 'e' && dst[n-3] == '-' && dst[n-2] == '0' {
			dst[n-2] = dst[n-1]
			dst = dst[:n-1]
		}
	}
	return dst, true
}
