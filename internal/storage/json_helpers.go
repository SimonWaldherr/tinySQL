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
	switch x := v.(type) {
	case nil:
		return nil
	case *big.Rat:
		return x.String()
	case big.Rat:
		return x.String()
	case uuid.UUID:
		return x.String()
	case map[string]any:
		m := make(map[string]any, len(x))
		for k, vv := range x {
			m[k] = normalizeForJSON(vv)
		}
		return m
	case []any:
		out := make([]any, len(x))
		for i, vv := range x {
			out[i] = normalizeForJSON(vv)
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
