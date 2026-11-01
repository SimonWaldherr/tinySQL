//go:build js && wasm

// Package wasmbridge transfers query results from Go to JavaScript.
package wasmbridge

import (
	"encoding/json"
	"math"
	"syscall/js"
	"unicode/utf8"
)

// Keep temporary JSON buffers bounded independently of the total result size.
// A row that exceeds the byte budget uses ValueOf instead of a JSON copy.
const (
	resultBatchRows  = 1024
	resultBatchBytes = 256 * 1024
)

// QueryResult preserves the native-object query response. Larger primitive
// results cross the boundary in bounded JSON batches. Values whose JSON
// semantics differ use ValueOf without disabling batching for adjacent rows.
func QueryResult(columns []string, rows [][]any, errMsg string, count int, elapsedNs int64) any {
	end := 0
	if len(rows) >= 32 && utf8.ValidString(errMsg) && validColumns(columns) {
		end = jsonBatchEnd(rows, 0)
	}
	if end == len(rows) && end != 0 {
		payload := struct {
			Columns []string `json:"columns"`
			Rows    any      `json:"rows"`
			Count   int      `json:"count"`
			Elapsed int64    `json:"elapsed_ms"`
			Error   string   `json:"error,omitempty"`
		}{columns, jsonRows(rows), count, elapsedNs, errMsg}
		if data, err := json.Marshal(payload); err == nil {
			return js.Global().Get("JSON").Call("parse", string(data))
		}
	}
	var cols any
	if columns != nil {
		values := make([]any, len(columns))
		for i, column := range columns {
			values[i] = column
		}
		cols = values
	}
	result := map[string]any{"columns": cols, "rows": nil, "count": count, "elapsed_ms": elapsedNs}
	if errMsg != "" {
		result["error"] = errMsg
	}
	if len(rows) > resultBatchRows || end > 0 {
		// push.apply appends at most resultBatchRows arguments per call. concat
		// would repeatedly copy the entire prefix as the result grows.
		output := js.Global().Get("Array").New()
		push := output.Get("push")
		for start := 0; start < len(rows); {
			end = jsonBatchEnd(rows, start)
			if end > start {
				values := jsonRows(rows[start:end])
				// Retain the outer interface so the common-value encoder also
				// handles the array itself, not just its elements.
				if data, err := json.Marshal(&values); err == nil {
					batch := js.Global().Get("JSON").Call("parse", string(data))
					push.Call("apply", output, batch)
					start = end
					continue
				}
			}
			output.SetIndex(start, js.ValueOf(rows[start]))
			start++
		}
		result["rows"] = output
	} else if rows != nil {
		values := make([]any, len(rows))
		for i, row := range rows {
			values[i] = row
		}
		result["rows"] = values
	}
	return result
}

// Use generic JSON arrays so the encoder's common-value path handles cells
// without constructing an addressable reflection value for each one.
func jsonRows(rows [][]any) any {
	values := make([]any, len(rows))
	for i, row := range rows {
		values[i] = row
	}
	return values
}

func validColumns(columns []string) bool {
	for _, column := range columns {
		if !utf8.ValidString(column) {
			return false
		}
	}
	return true
}

// jsonBatchEnd bounds the worst-case encoded size, including JSON escaping.
// It returns start if the first row needs the direct conversion path.
func jsonBatchEnd(rows [][]any, start int) int {
	bytes := 2 // outer brackets
	end := start
	for ; end < len(rows) && end-start < resultBatchRows; end++ {
		row := rows[end]
		if row == nil {
			break
		}
		size := 3 // row brackets and separator
		for _, value := range row {
			cellBytes := 32 // numeric scalars, booleans, null and separator
			switch v := value.(type) {
			case nil, bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
			case string:
				if !utf8.ValidString(v) {
					return end
				}
				// Check before multiplying: a very large string cannot overflow
				// the estimate on wasm's 32-bit int.
				if len(v) > resultBatchBytes/6 {
					return end
				}
				cellBytes = 6*len(v) + 3
			case float64:
				if math.IsNaN(v) || math.IsInf(v, 0) || (v == 0 && math.Signbit(v)) {
					return end
				}
			default:
				// In particular float32 JSON formatting can round differently from
				// ValueOf's float64 conversion, so it must keep the direct path.
				return end
			}
			size += cellBytes
			if size > resultBatchBytes-bytes {
				return end
			}
		}
		bytes += size
	}
	return end
}

// MakeRows allocates one cell backing store rather than one per row. Capping
// each row's capacity keeps append from overwriting its neighbor.
func MakeRows(rowCount, columnCount int) [][]any {
	rows := make([][]any, rowCount)
	cells := make([]any, rowCount*columnCount)
	for i := range rows {
		start, end := i*columnCount, (i+1)*columnCount
		rows[i] = cells[start:end:end]
	}
	return rows
}
