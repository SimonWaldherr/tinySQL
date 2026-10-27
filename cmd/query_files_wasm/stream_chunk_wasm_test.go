//go:build js && wasm

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"syscall/js"
	"testing"
	"time"

	tinysql "github.com/SimonWaldherr/tinySQL"
)

func benchmarkRawRows() []json.RawMessage {
	rows := make([]json.RawMessage, 128)
	for i := range rows {
		data, _ := json.Marshal(map[string]interface{}{
			"id": i, "name": fmt.Sprintf("Item %d", i), "value": i * 3,
			"description": "Local query row with <HTML> & other text",
		})
		rows[i] = data
	}
	return rows
}

func TestStreamChunkPayloadPreservesValues(t *testing.T) {
	columns := []string{"id", "text", "value"}
	inputs := []tinysql.Row{
		{"id": int64(42), "text": "<p>😊 & \"quote\"\n", "value": nil},
		{"id": int64(43), "text": "東京", "value": true},
	}
	rows := make([]json.RawMessage, 0, len(inputs))
	keys := resultColumnKeys(columns)
	for _, row := range inputs {
		encoded, size, err := streamJSRow(columns, keys, row)
		if err != nil || size != int64(len(encoded)+1) {
			t.Fatalf("encoded row = %s, size = %d, err = %v", encoded, size, err)
		}
		rows = append(rows, encoded)
	}
	var got map[string]interface{}
	var gotID int
	reporter := js.FuncOf(func(_ js.Value, args []js.Value) interface{} {
		gotID = args[0].Int()
		encoded := js.Global().Get("JSON").Call("stringify", args[1]).String()
		if err := json.Unmarshal([]byte(encoded), &got); err != nil {
			t.Error(err)
		}
		return nil
	})
	previous := js.Global().Get("__tinySQLReportQueryStream")
	js.Global().Set("__tinySQLReportQueryStream", reporter)
	t.Cleanup(func() {
		js.Global().Set("__tinySQLReportQueryStream", previous)
		reporter.Release()
	})
	op := &streamQueryOperation{requestID: 12, columns: columns, rows: inputs, startedAt: time.Now(), resultB: 123}
	op.emitChunk(rows)
	if gotID != 12 || got["phase"] != "chunk" || got["rowsRetained"] != float64(2) || got["resultBytes"] != float64(123) {
		t.Fatalf("request = %d payload = %#v", gotID, got)
	}
	var wantRows interface{}
	encoded, _ := json.Marshal(rows)
	if err := json.Unmarshal(encoded, &wantRows); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got["rows"], wantRows) || !reflect.DeepEqual(got["columns"], []interface{}{"id", "text", "value"}) {
		t.Fatalf("payload rows/columns = %#v / %#v; want %#v / %#v", got["rows"], got["columns"], wantRows, columns)
	}
	got = nil
	op.emitChunk(nil)
	if got != nil {
		t.Fatal("empty batch should not be emitted")
	}
}

func TestExecuteQueryStreamEmitsChunks(t *testing.T) {
	previousDB, previousCache, previousResult := db, queryCache, lastResult
	db = tinysql.NewDB()
	queryCache = tinysql.NewQueryCache(queryCacheSize)
	lastResult = nil
	t.Cleanup(func() {
		db, queryCache, lastResult = previousDB, previousCache, previousResult
		lastResultPager.reset()
	})
	for _, query := range []string{
		`CREATE TABLE chunk_test (id INT, label TEXT)`,
		func() string {
			values := make([]string, 130)
			for i := range values {
				values[i] = fmt.Sprintf("(%d, 'row<&-%d')", i, i)
			}
			return `INSERT INTO chunk_test VALUES ` + strings.Join(values, ",")
		}(),
	} {
		stmt, err := tinysql.ParseSQL(query)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tinysql.Execute(context.Background(), db, defaultTenant, stmt); err != nil {
			t.Fatal(err)
		}
	}
	var streamed []struct {
		ID    int    `json:"id"`
		Label string `json:"label"`
	}
	chunks := 0
	reporter := js.FuncOf(func(_ js.Value, args []js.Value) interface{} {
		event := args[1]
		if event.Get("phase").String() == "chunk" {
			chunks++
			encoded := js.Global().Get("JSON").Call("stringify", event.Get("rows")).String()
			var part []struct {
				ID    int    `json:"id"`
				Label string `json:"label"`
			}
			if err := json.Unmarshal([]byte(encoded), &part); err != nil {
				t.Error(err)
			}
			streamed = append(streamed, part...)
		}
		return nil
	})
	previousReporter := js.Global().Get("__tinySQLReportQueryStream")
	js.Global().Set("__tinySQLReportQueryStream", reporter)
	t.Cleanup(func() {
		js.Global().Set("__tinySQLReportQueryStream", previousReporter)
		reporter.Release()
	})
	finished := make(chan js.Value, 1)
	callback := js.FuncOf(func(_ js.Value, args []js.Value) interface{} {
		finished <- args[0]
		return nil
	})
	defer callback.Release()
	promise := executeQueryStream(js.Undefined(), []js.Value{js.ValueOf(1973), js.ValueOf(`SELECT id, label FROM chunk_test`)}).(js.Value)
	promise.Call("then", callback)
	select {
	case result := <-finished:
		if !result.Get("success").Bool() || result.Get("totalRows").Int() != 130 || result.Get("rows").Length() != 100 {
			t.Fatalf("stream result: %s", js.Global().Get("JSON").Call("stringify", result).String())
		}
	case <-time.After(10 * time.Second):
		t.Fatal("stream timed out")
	}
	if chunks < 2 || len(streamed) != 130 {
		t.Fatalf("received %d rows in %d chunks", len(streamed), chunks)
	}
	for i, row := range streamed {
		if row.ID != i || row.Label != fmt.Sprintf("row<&-%d", i) {
			t.Fatalf("row %d = %#v", i, row)
		}
	}
}

func BenchmarkStreamChunkEmission(b *testing.B) {
	rows := benchmarkRawRows()
	legacyRows := make([]interface{}, len(rows))
	for i, row := range rows {
		legacyRows[i] = row
	}
	op := &streamQueryOperation{requestID: 1, columns: []string{"id", "name", "value", "description"}, startedAt: time.Now()}
	reporter := js.FuncOf(func(_ js.Value, _ []js.Value) interface{} { return nil })
	previous := js.Global().Get("__tinySQLReportQueryStream")
	js.Global().Set("__tinySQLReportQueryStream", reporter)
	b.Cleanup(func() {
		js.Global().Set("__tinySQLReportQueryStream", previous)
		reporter.Release()
	})
	b.Run("legacy", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			op.emit("chunk", map[string]interface{}{"columns": stringsToInterfaces(op.columns), "rows": legacyRows})
		}
	})
	b.Run("direct", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			op.emitChunk(rows)
		}
	})
}
