//go:build js && wasm

// Package wasmbridge transfers query results from Go to JavaScript.
package wasmbridge

import (
	"encoding/json"
	"math"
	"syscall/js"
	"unicode/utf8"
)

// QueryResult preserves the native-object query response. For larger primitive
// result sets, one JSON.parse replaces per-cell syscall/js boundary crossings.
// Small responses and values whose JSON semantics differ retain ValueOf.
func QueryResult(columns []string, rows [][]any, errMsg string, count int, elapsedNs int64) any {
	if len(rows) >= 32 && utf8.ValidString(errMsg) && jsonSafeRows(columns, rows) {
		payload := struct {
			Columns []string `json:"columns"`
			Rows    [][]any  `json:"rows"`
			Count   int      `json:"count"`
			Elapsed int64    `json:"elapsed_ms"`
			Error   string   `json:"error,omitempty"`
		}{columns, rows, count, elapsedNs, errMsg}
		if data, err := json.Marshal(payload); err == nil {
			return js.Global().Get("JSON").Call("parse", string(data))
		}
	}
	var cols, out any
	if columns != nil {
		values := make([]any, len(columns))
		for i, column := range columns {
			values[i] = column
		}
		cols = values
	}
	if rows != nil {
		values := make([]any, len(rows))
		for i, row := range rows {
			values[i] = row
		}
		out = values
	}
	result := map[string]any{"columns": cols, "rows": out, "count": count, "elapsed_ms": elapsedNs}
	if errMsg != "" {
		result["error"] = errMsg
	}
	return result
}

func jsonSafeRows(columns []string, rows [][]any) bool {
	for _, column := range columns {
		if !utf8.ValidString(column) {
			return false
		}
	}
	for _, row := range rows {
		if row == nil {
			return false
		}
		for _, value := range row {
			switch v := value.(type) {
			case nil, bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
			case string:
				if !utf8.ValidString(v) {
					return false
				}
			case float64:
				if math.IsNaN(v) || math.IsInf(v, 0) || (v == 0 && math.Signbit(v)) {
					return false
				}
			default:
				// In particular float32 JSON formatting can round differently from
				// ValueOf's float64 conversion, so it must keep the direct path.
				return false
			}
		}
	}
	return true
}
