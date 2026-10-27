//go:build js && wasm

package main

import (
	"strings"
	"syscall/js"
	"testing"

	tinysql "github.com/SimonWaldherr/tinySQL"
)

func TestWASMImportJSONLPreservesValuesAndLongRecords(t *testing.T) {
	previousDB, previousCache, previousResult := db, queryCache, lastResult
	db = tinysql.NewDB()
	queryCache = tinysql.NewQueryCache(queryCacheSize)
	t.Cleanup(func() {
		db, queryCache, lastResult = previousDB, previousCache, previousResult
		lastResultPager.reset()
	})
	longValue := strings.Repeat("x", 100*1024)
	input := `{"name":"O'Reilly","text":"` + longValue + `"}` + "\n" + `{"name":"Ada","text":"second"}`
	response := importFileRequest(js.Undefined(), []js.Value{js.ValueOf("records.jsonl"), js.ValueOf(input), js.ValueOf("records")}).(map[string]interface{})
	if response["success"] != true || response["rowsImported"] != 2 {
		t.Fatalf("import response = %#v", response)
	}
	result, err := executeSQLText("SELECT name, text FROM records")
	if err != nil || len(result.Rows) != 2 {
		t.Fatalf("query result = %#v, error %v", result, err)
	}
	name, _ := tinysql.GetVal(result.Rows[0], "name")
	text, _ := tinysql.GetVal(result.Rows[0], "text")
	if name != "O'Reilly" || text != longValue {
		actualText, _ := text.(string)
		t.Fatalf("first row name = %#v, text length = %d", name, len(actualText))
	}
}
