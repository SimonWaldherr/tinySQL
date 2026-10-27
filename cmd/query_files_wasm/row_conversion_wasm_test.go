//go:build js && wasm

package main

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"

	tinysql "github.com/SimonWaldherr/tinySQL"
)

func TestResultRowsToJSNormalization(t *testing.T) {
	columns := []string{"ID", "Name", "Missing", "ID", "Bytes", "Null", "Ünicode"}
	rows := []tinysql.Row{{"id": int64(42), "name": "Ada", "bytes": []byte{1, 2}, "null": nil, "ünicode": "<&"}}
	want := map[string]interface{}{"ID": int64(42), "Name": "Ada", "Missing": nil, "Bytes": "[1 2]", "Null": nil, "Ünicode": "<&"}
	got := resultRowsToJS(columns, rows)
	if len(got) != 1 || !reflect.DeepEqual(got[0], want) {
		t.Fatalf("rows = %#v, want %#v", got, want)
	}
	encoded, size, err := streamJSRow(columns, resultColumnKeys(columns), rows[0])
	expected, marshalErr := json.Marshal(want)
	if err != nil || marshalErr != nil || string(encoded) != string(expected) || size != int64(len(expected)+1) {
		t.Fatalf("stream row = %s (%d), error = %v; want %s", encoded, size, err, expected)
	}
	if _, _, err := streamJSRow([]string{"VALUE"}, []string{"value"}, tinysql.Row{"value": math.NaN()}); err == nil {
		t.Fatal("expected NaN encoding error")
	}
	if got := resultRowsToJS(columns, nil); len(got) != 0 || got == nil {
		t.Fatalf("empty result rows = %#v", got)
	}
}

func TestResultLimitsUseNormalizedColumns(t *testing.T) {
	result := &tinysql.ResultSet{
		Cols: []string{"Name"},
		Rows: []tinysql.Row{{"name": "A & B"}, {"name": "C"}},
	}
	limited, info, err := limitResultForBrowser(result)
	if err != nil || limited != result || info.Truncated || info.RowsRetained != 2 {
		t.Fatalf("result = %#v, info = %#v, error = %v", limited, info, err)
	}
	first, _ := json.Marshal(map[string]interface{}{"Name": "A & B"})
	second, _ := json.Marshal(map[string]interface{}{"Name": "C"})
	if expected := int64(len(first) + len(second) + 2); info.ResultBytes != expected {
		t.Fatalf("result bytes = %d, want %d", info.ResultBytes, expected)
	}

	result.Rows = make([]tinysql.Row, streamPreviewMaxRows+1)
	limited, info, err = limitResultForBrowser(result)
	if err != nil || !info.Truncated || info.Reason != "row" || len(limited.Rows) != streamPreviewMaxRows || info.RowsProduced != streamPreviewMaxRows+1 {
		t.Fatalf("row cap: info = %#v, error = %v", info, err)
	}
	result.Rows = []tinysql.Row{{"name": strings.Repeat("a", streamPreviewMaxBytes)}}
	limited, info, err = limitResultForBrowser(result)
	if err != nil || !info.Truncated || info.Reason != "size" || len(limited.Rows) != 0 || info.ResultBytes != 0 {
		t.Fatalf("byte cap: info = %#v, error = %v", info, err)
	}
}

var benchmarkConvertedRows []interface{}
var benchmarkStreamBytes int64

func benchmarkRows() ([]string, []tinysql.Row) {
	columns := make([]string, 20)
	rows := make([]tinysql.Row, 128)
	for i := range columns {
		columns[i] = fmt.Sprintf("COLUMN_%02d", i)
	}
	for i := range rows {
		row := make(tinysql.Row, len(columns))
		for j := range columns {
			row[fmt.Sprintf("column_%02d", j)] = i + j
		}
		rows[i] = row
	}
	return columns, rows
}

// legacyResultRow reproduces the per-cell lookup used before precomputing
// lowercase keys, so the benchmark also measures lowercase column names.
func legacyResultRow(columns []string, row tinysql.Row) map[string]interface{} {
	out := make(map[string]interface{}, len(columns))
	for _, col := range columns {
		val, ok := tinysql.GetVal(row, col)
		if !ok || val == nil {
			out[col] = nil
			continue
		}
		switch v := val.(type) {
		case string, int, int32, int64, float32, float64, bool:
			out[col] = v
		default:
			out[col] = fmt.Sprintf("%v", v)
		}
	}
	return out
}

func BenchmarkResultConversion(b *testing.B) {
	columns, rows := benchmarkRows()
	for _, casing := range []string{"upper", "lower"} {
		cols := append([]string(nil), columns...)
		if casing == "lower" {
			for i := range cols {
				cols[i] = strings.ToLower(cols[i])
			}
		}
		b.Run(casing, func(b *testing.B) {
			for _, mode := range []string{"legacy_page", "page", "legacy_stream", "stream"} {
				b.Run(mode, func(b *testing.B) {
					keys := resultColumnKeys(cols)
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						switch mode {
						case "legacy_page":
							converted := make([]interface{}, 0, len(rows))
							for _, row := range rows {
								converted = append(converted, legacyResultRow(cols, row))
							}
							benchmarkConvertedRows = converted
						case "page":
							benchmarkConvertedRows = resultRowsToJS(cols, rows)
						case "legacy_stream":
							var size int64
							for _, row := range rows {
								encoded, err := json.Marshal(legacyResultRow(cols, row))
								if err != nil {
									b.Fatal(err)
								}
								size += int64(len(encoded) + 1)
							}
							benchmarkStreamBytes = size
						case "stream":
							var size int64
							for _, row := range rows {
								_, n, err := streamJSRow(cols, keys, row)
								if err != nil {
									b.Fatal(err)
								}
								size += n
							}
							benchmarkStreamBytes = size
						}
					}
				})
			}
		})
	}
}
