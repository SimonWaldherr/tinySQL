package storage

import (
	"encoding/json"
	"math/big"

	"github.com/google/uuid"
)

// normalizeForJSON converts known internal types to JSON-friendly values.
// It intentionally keeps common types as-is and converts *big.Rat and uuid.UUID
// to string representations so json.Marshal yields readable output.
func normalizeForJSON(v any) any {
	out, _, safe := normalizeJSONValue(v)
	if !safe {
		// A custom marshaler can mutate another container while encoding. The
		// eager path retains the old isolation for values we cannot classify.
		return normalizeJSONEager(v)
	}
	return out
}

// normalizeJSONRows classifies the entire row set before encoding. A custom
// marshaler in one row can mutate a container in another, so eager isolation
// must cover all rows whenever any value has unknown marshaling behavior.
func normalizeJSONRows(rows [][]any) [][]any {
	if rows == nil {
		return make([][]any, 0)
	}
	var copied [][]any
	for i, row := range rows {
		value, changed, safe := normalizeJSONSlice(row)
		if !safe {
			return normalizeJSONRowsEager(rows)
		}
		if changed {
			if copied == nil {
				copied = append([][]any(nil), rows...)
			}
			copied[i] = value
		}
	}
	if copied != nil {
		return copied
	}
	return rows
}

func normalizeJSONRowsEager(rows [][]any) [][]any {
	normalized := make([][]any, len(rows))
	for i, row := range rows {
		normalized[i] = normalizeJSONEager(row).([]any)
	}
	return normalized
}

func normalizeJSONTable(dt diskTable) diskTable {
	// Defaults are encoded before Rows and can also contain user-defined
	// marshalers. Keep their old representation while isolating row containers.
	for _, column := range dt.Cols {
		switch column.DefaultValue.(type) {
		case nil, bool, string, int, int8, int16, int32, int64,
			uint, uint8, uint16, uint32, uint64, uintptr, float32, float64,
			json.Number, []byte, *big.Rat, big.Rat, uuid.UUID:
		default:
			// Do not traverse structured defaults: they may contain cycles,
			// which the JSON encoder must report instead of this normalizer.
			dt.Rows = normalizeJSONRowsEager(dt.Rows)
			return dt
		}
	}
	dt.Rows = normalizeJSONRows(dt.Rows)
	return dt
}

// normalizeJSONValue copies containers only when a descendant changes. A
// typed nil map or slice still becomes an empty container, as before. Unknown
// types require the eager path because they may contain custom marshalers.
func normalizeJSONValue(v any) (value any, changed, safe bool) {
	switch x := v.(type) {
	case *big.Rat:
		return x.String(), true, true
	case big.Rat:
		return x.String(), true, true
	case uuid.UUID:
		return x.String(), true, true
	case nil, bool, string, int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64, uintptr, float32, float64, json.Number, []byte:
		return v, false, true
	case map[string]any:
		if x == nil {
			return make(map[string]any), true, true
		}
		var copied map[string]any
		for k, old := range x {
			value, changed, safe := normalizeJSONValue(old)
			if !safe {
				return nil, false, false
			}
			if changed && copied == nil {
				copied = make(map[string]any, len(x))
				for key, original := range x {
					copied[key] = original
				}
			}
			if changed {
				copied[k] = value
			}
		}
		if copied != nil {
			return copied, true, true
		}
		return x, false, true
	case []any:
		out, changed, safe := normalizeJSONSlice(x)
		if !safe {
			return nil, false, false
		}
		if changed {
			return out, true, true
		}
		return v, false, true
	default:
		return nil, false, false
	}
}

// A typed slice result avoids boxing each unchanged disk row into interfaces.
func normalizeJSONSlice(values []any) (out []any, changed, safe bool) {
	if values == nil {
		return make([]any, 0), true, true
	}
	var copied []any
	for i, old := range values {
		value, changed, safe := normalizeJSONValue(old)
		if !safe {
			return nil, false, false
		}
		if changed && copied == nil {
			copied = append([]any(nil), values...)
		}
		if changed {
			copied[i] = value
		}
	}
	if copied != nil {
		return copied, true, true
	}
	return values, false, true
}

func normalizeJSONEager(v any) any {
	switch x := v.(type) {
	case *big.Rat:
		return x.String()
	case big.Rat:
		return x.String()
	case uuid.UUID:
		return x.String()
	case map[string]any:
		m := make(map[string]any, len(x))
		for k, value := range x {
			m[k] = normalizeJSONEager(value)
		}
		return m
	case []any:
		out := make([]any, len(x))
		for i, value := range x {
			out[i] = normalizeJSONEager(value)
		}
		return out
	default:
		return v
	}
}

// JSONMarshal marshals v after converting internal types to JSON-friendly
// representations (e.g. big.Rat -> string).
func JSONMarshal(v any) ([]byte, error) {
	return json.Marshal(normalizeForJSON(v))
}

// normalizeJSONNumbers preserves the usual float64 representation outside
// schema-directed integer cells when a table decoder uses UseNumber.
func normalizeJSONNumbers(v any) (any, error) {
	switch x := v.(type) {
	case json.Number:
		return x.Float64()
	case []any:
		for i := range x {
			value, err := normalizeJSONNumbers(x[i])
			if err != nil {
				return nil, err
			}
			x[i] = value
		}
	case map[string]any:
		for k, old := range x {
			value, err := normalizeJSONNumbers(old)
			if err != nil {
				return nil, err
			}
			x[k] = value
		}
	}
	return v, nil
}
