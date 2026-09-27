//go:build cgo

package main

/*
#include <stdlib.h>
*/
import "C"

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"unsafe"

	tsql "github.com/SimonWaldherr/tinySQL"
)

// The functions below keep the original single-database libtinysql ABI
// (formerly built from bindings/python) available for existing callers.
// New code should use the handle-based TinySQLDatabase* functions.

var legacy = struct {
	sync.Mutex
	db *tsql.DB
}{db: tsql.NewDB()}

//export TinySQLVersion
func TinySQLVersion() *C.char { return C.CString(tsql.Version()) }

//export TinySQLSave
func TinySQLSave(path *C.char) *C.char {
	legacy.Lock()
	defer legacy.Unlock()
	if err := tsql.SaveToFile(legacy.db, C.GoString(path)); err != nil {
		return legacyError(err)
	}
	return legacyJSON(map[string]any{"status": "ok"})
}

//export TinySQLLoad
func TinySQLLoad(path *C.char) *C.char {
	legacy.Lock()
	defer legacy.Unlock()
	db, err := tsql.LoadFromFile(C.GoString(path))
	if err != nil {
		return legacyError(err)
	}
	legacy.db = db
	return legacyJSON(map[string]any{"status": "ok"})
}

//export TinySQLExec
func TinySQLExec(query *C.char) *C.char {
	legacy.Lock()
	defer legacy.Unlock()
	rs, err := tsql.ExecSQL(context.Background(), legacy.db, "default", C.GoString(query))
	if err != nil {
		return legacyError(err)
	}
	if rs == nil {
		return legacyJSON(map[string]any{"status": "ok", "rows": 0})
	}
	rows := make([]map[string]any, len(rs.Rows))
	for i, row := range rs.Rows {
		object := make(map[string]any, len(rs.Cols))
		for _, column := range rs.Cols {
			object[column] = row[strings.ToLower(column)]
		}
		rows[i] = object
	}
	return legacyJSON(map[string]any{"status": "ok", "columns": rs.Cols, "rows": rows})
}

//export TinySQLReset
func TinySQLReset() {
	legacy.Lock()
	defer legacy.Unlock()
	legacy.db = tsql.NewDB()
}

//export TinySQLFree
func TinySQLFree(ptr *C.char) {
	if ptr != nil {
		C.free(unsafe.Pointer(ptr))
	}
}

func legacyError(err error) *C.char {
	return legacyJSON(map[string]any{"status": "error", "error": err.Error()})
}

func legacyJSON(v any) *C.char {
	encoded, err := json.Marshal(v)
	if err != nil {
		encoded, _ = json.Marshal(map[string]any{"status": "error", "error": err.Error()})
	}
	return C.CString(string(encoded))
}
