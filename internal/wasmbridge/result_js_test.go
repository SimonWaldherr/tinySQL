//go:build js && wasm

package wasmbridge

import (
	"math"
	"syscall/js"
	"testing"
)

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
