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

var (
	pyDB   = tsql.NewDB()
	pyLock sync.Mutex
)

//export TinySQLExec
func TinySQLExec(sql *C.char) *C.char {
	pyLock.Lock()
	defer pyLock.Unlock()

	query := C.GoString(sql)
	stmt, err := tsql.ParseSQL(query)
	if err != nil {
		return cStringError(err)
	}

	rs, err := tsql.Execute(context.Background(), pyDB, "default", stmt)
	if err != nil {
		return cStringError(err)
	}

	if rs == nil {
		return cStringJSON(map[string]any{
			"status": "ok",
			"rows":   0,
		})
	}

	rows := make([]map[string]any, len(rs.Rows))
	for i, row := range rs.Rows {
		obj := make(map[string]any)
		for _, col := range rs.Cols {
			obj[col] = row[strings.ToLower(col)]
		}
		rows[i] = obj
	}

	payload := map[string]any{
		"status":  "ok",
		"columns": rs.Cols,
		"rows":    rows,
	}

	return cStringJSON(payload)
}

//export TinySQLReset
func TinySQLReset() {
	pyLock.Lock()
	defer pyLock.Unlock()
	pyDB = tsql.NewDB()
}

//export TinySQLFree
func TinySQLFree(ptr *C.char) {
	if ptr != nil {
		C.free(unsafe.Pointer(ptr))
	}
}

func cStringError(err error) *C.char {
	return cStringJSON(map[string]any{
		"status": "error",
		"error":  err.Error(),
	})
}

func cStringJSON(v any) *C.char {
	buf, _ := json.Marshal(v)
	return C.CString(string(buf))
}

func main() {}
