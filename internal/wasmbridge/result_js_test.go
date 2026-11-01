//go:build js && wasm

package wasmbridge

import (
	"fmt"
	"math"
	"strings"
	"syscall/js"
	"testing"
)

func TestQueryResultMixedBatches(t *testing.T) {
	rows := MakeRows(4*resultBatchRows+3, 2)
	for i := range rows {
		rows[i][0], rows[i][1] = i, "é漢🙂\x00<&\n\"\\"
	}
	// Exercise fallback before, at and after batch boundaries, including the
	// legacy empty-array conversion of a nil inner slice.
	for i, value := range []any{math.NaN(), math.Inf(1), math.Inf(-1), math.Copysign(0, -1), float32(1.2), "\xff", js.ValueOf("native")} {
		rows[resultBatchRows-1+i][1] = value
	}
	rows[2*resultBatchRows] = nil
	// A wide row must not force a large temporary JSON buffer.
	rows[3*resultBatchRows][1] = strings.Repeat("<&", resultBatchBytes)
	columns := []string{"id", "text"}
	result := js.ValueOf(QueryResult(columns, rows, "failure", len(rows), 987))
	if result.Get("rows").Length() != len(rows) || result.Get("error").String() != "failure" || result.Get("elapsed_ms").Int() != 987 {
		t.Fatal("batched response metadata changed")
	}
	for i, row := range rows {
		got := result.Get("rows").Index(i)
		if got.Length() != len(row) {
			t.Fatalf("row %d length changed", i)
		}
		for j, value := range row {
			if !js.Global().Get("Object").Call("is", got.Index(j), js.ValueOf(value)).Bool() {
				t.Fatalf("row %d cell %d changed", i, j)
			}
		}
	}
}

func TestQueryResultJSONBudget(t *testing.T) {
	for _, width := range []int{0, 1000, resultBatchBytes} {
		t.Run(fmt.Sprint(width), func(t *testing.T) {
			rows := MakeRows(2*resultBatchRows+3, 1)
			text := strings.Repeat("\x00", width)
			for i := range rows {
				rows[i][0] = text
				if width == resultBatchBytes && i != resultBatchRows {
					rows[i][0] = ""
				}
			}
			parser := js.Global().Get("JSON").Get("parse")
			calls := 0
			spy := js.FuncOf(func(_ js.Value, args []js.Value) any {
				calls++
				encoded := args[0].String()
				if len(encoded) > resultBatchBytes {
					t.Errorf("JSON transfer exceeded budget: %d bytes", len(encoded))
				}
				return parser.Invoke(args[0])
			})
			js.Global().Get("JSON").Set("parse", spy)
			defer func() {
				js.Global().Get("JSON").Set("parse", parser)
				spy.Release()
			}()
			result := js.ValueOf(QueryResult([]string{"text"}, rows, "", len(rows), 0))
			if result.Get("rows").Length() != len(rows) {
				t.Fatal("rows were lost")
			}
			if calls < 3 {
				t.Fatal("large result was not split into batches")
			}
			if result.Get("rows").Index(resultBatchRows).Index(0).String() != text {
				t.Fatal("wide cell changed")
			}
		})
	}
}

func TestMakeRowsIsolation(t *testing.T) {
	rows := MakeRows(2, 1)
	rows[0][0], rows[1][0] = "first", "second"
	rows[0] = append(rows[0], "appended")
	if rows[1][0] != "second" {
		t.Fatal("appending a row overwrote its neighbor")
	}
	for _, shape := range [][2]int{{0, 0}, {0, 2}, {2, 0}} {
		result := js.ValueOf(QueryResult(nil, MakeRows(shape[0], shape[1]), "", shape[0], 0))
		if result.Get("rows").IsNull() || result.Get("rows").Length() != shape[0] {
			t.Fatal("empty result rows must stay arrays")
		}
		for i := 0; i < shape[0]; i++ {
			if result.Get("rows").Index(i).IsNull() || result.Get("rows").Index(i).Length() != shape[1] {
				t.Fatal("zero-column row must stay an empty array")
			}
		}
	}
}

var benchmarkRows [][]any

func BenchmarkMakeRows(b *testing.B) {
	b.Run("per-row", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			rows := make([][]any, 10000)
			for j := range rows {
				rows[j] = make([]any, 4)
			}
			benchmarkRows = rows
		}
	})
	b.Run("contiguous", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			benchmarkRows = MakeRows(10000, 4)
		}
	})
}

func BenchmarkQueryResult(b *testing.B) {
	for _, n := range []int{1, 200, 10000} {
		for _, special := range []bool{false, true} {
			rows := make([][]any, n)
			for i := range rows {
				rows[i] = []any{i, "é漢🙂 <text> & \"quote\"", true, nil}
			}
			if special {
				rows[n/2][0] = math.Copysign(0, -1)
			}
			b.Run(fmt.Sprintf("rows=%d/special=%t", n, special), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					js.ValueOf(QueryResult([]string{"id", "text", "flag", "null"}, rows, "", n, 123))
				}
			})
		}
	}
}

func TestQueryResultTransfer(t *testing.T) {
	for _, n := range []int{0, 1, 31, 32, 200} {
		rows := make([][]any, n)
		for i := range rows {
			rows[i] = []any{i, "é漢🙂\n\"\\", true, nil, float64(1.25), int64(9007199254740993), uint64(^uint64(0))}
		}
		response := QueryResult([]string{"id", "text", "flag", "null", "float", "large", "uint"}, rows, "", n, 123)
		_, batched := response.(js.Value)
		if batched != (n >= 32) {
			t.Fatalf("rows=%d: unexpected transfer path", n)
		}
		result := js.ValueOf(response)
		if result.Get("rows").Length() != n || result.Get("count").Int() != n || result.Get("elapsed_ms").Int() != 123 {
			t.Fatal("response shape changed")
		}
		if !result.Get("error").IsUndefined() {
			t.Fatal("empty error should be omitted")
		}
		for i, row := range rows {
			actual := result.Get("rows").Index(i)
			for j, value := range row {
				if !js.Global().Get("Object").Call("is", actual.Index(j), js.ValueOf(value)).Bool() {
					t.Fatalf("row %d cell %d changed", i, j)
				}
			}
		}
	}
}

func TestQueryResultFallback(t *testing.T) {
	for _, value := range []any{math.NaN(), math.Inf(1), math.Inf(-1), math.Copysign(0, -1), float32(1.2), "\xff\xc0\xaf", js.ValueOf("native")} {
		rows := make([][]any, 32)
		for i := range rows {
			rows[i] = []any{value}
		}
		response := QueryResult([]string{"v"}, rows, "", 32, 0)
		if _, batched := response.(js.Value); batched {
			t.Fatalf("special value %v took JSON path", value)
		}
		got := js.ValueOf(response).Get("rows").Index(0).Index(0)
		if !js.Global().Get("Object").Call("is", got, js.ValueOf(value)).Bool() {
			t.Fatalf("special value %v changed", value)
		}
	}
	rows := make([][]any, 32)
	result := js.ValueOf(QueryResult(nil, rows, "failure", 32, 0))
	if !result.Get("columns").IsNull() || result.Get("rows").Index(0).Length() != 0 || result.Get("error").String() != "failure" {
		t.Fatal("nil inner rows changed")
	}
	result = js.ValueOf(QueryResult(nil, nil, "failure", 0, 0))
	if !result.Get("columns").IsNull() || !result.Get("rows").IsNull() {
		t.Fatal("nil outer slices changed")
	}
}
