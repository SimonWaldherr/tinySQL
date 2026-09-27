//go:build cgo

package main

/*
#include <stdint.h>
#include <stdlib.h>
*/
import "C"

import "unsafe"

// cResponse copies an encoded response into a NUL-terminated C buffer owned
// by the caller. Encoded JSON never contains a NUL byte.
func cResponse(fn func() (response, error)) *C.char {
	return cBuffer(encodeResponse(fn))
}

func cBuffer(encoded []byte) *C.char {
	buffer := C.malloc(C.size_t(len(encoded) + 1))
	out := unsafe.Slice((*byte)(buffer), len(encoded)+1)
	copy(out, encoded)
	out[len(encoded)] = 0
	return (*C.char)(buffer)
}

//export TinySQLABIVersion
func TinySQLABIVersion() C.int32_t { return C.int32_t(abiVersion) }

//export TinySQLInfo
func TinySQLInfo() *C.char { return cResponse(info) }

//export TinySQLDatabaseOpen
func TinySQLDatabaseOpen(path *C.char) *C.char {
	return cResponse(func() (response, error) { return openDatabase(C.GoString(path)) })
}

//export TinySQLDatabaseOpenWithOptions
func TinySQLDatabaseOpenWithOptions(options *C.char) *C.char {
	return cResponse(func() (response, error) { return openWithOptions(C.GoString(options)) })
}

//export TinySQLDatabaseExecute
func TinySQLDatabaseExecute(handle C.uint64_t, query, parameters *C.char) *C.char {
	return cResponse(func() (response, error) {
		return runSQL(uint64(handle), C.GoString(query), C.GoString(parameters), modeExec)
	})
}

//export TinySQLDatabaseQuery
func TinySQLDatabaseQuery(handle C.uint64_t, query, parameters *C.char) *C.char {
	return cResponse(func() (response, error) {
		return runSQL(uint64(handle), C.GoString(query), C.GoString(parameters), modeQuery)
	})
}

//export TinySQLDatabaseRun
func TinySQLDatabaseRun(handle C.uint64_t, query, parameters *C.char) *C.char {
	return cResponse(func() (response, error) {
		return runSQL(uint64(handle), C.GoString(query), C.GoString(parameters), modeAuto)
	})
}

//export TinySQLDatabaseExecuteBatch
func TinySQLDatabaseExecuteBatch(handle C.uint64_t, query, parameterSets *C.char) *C.char {
	return cResponse(func() (response, error) {
		return executeBatch(uint64(handle), C.GoString(query), C.GoString(parameterSets))
	})
}

//export TinySQLDatabaseExecuteScript
func TinySQLDatabaseExecuteScript(handle C.uint64_t, script *C.char) *C.char {
	return cResponse(func() (response, error) { return executeScript(uint64(handle), C.GoString(script)) })
}

//export TinySQLDatabaseSave
func TinySQLDatabaseSave(handle C.uint64_t, path *C.char) *C.char {
	return cResponse(func() (response, error) { return saveDatabase(uint64(handle), C.GoString(path)) })
}

//export TinySQLDatabaseSync
func TinySQLDatabaseSync(handle C.uint64_t) *C.char {
	return cResponse(func() (response, error) { return syncDatabase(uint64(handle)) })
}

//export TinySQLDatabaseClose
func TinySQLDatabaseClose(handle C.uint64_t) *C.char {
	return cResponse(func() (response, error) { return closeDatabase(uint64(handle)) })
}

//export TinySQLDatabaseFree
func TinySQLDatabaseFree(buffer *C.char) { C.free(unsafe.Pointer(buffer)) }

func main() {}
