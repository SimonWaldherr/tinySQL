package main

/*
#include <stdint.h>
#include <stdlib.h>
*/
import "C"

import "unsafe"

//export TinySQLDatabaseOpen
func TinySQLDatabaseOpen(path *C.char) *C.char {
	return C.CString(string(encodeResponse(func() (response, error) { return openDatabase(C.GoString(path)) })))
}

//export TinySQLDatabaseExecute
func TinySQLDatabaseExecute(handle C.uint64_t, query, parameters *C.char) *C.char {
	return C.CString(string(encodeResponse(func() (response, error) {
		return runSQL(uint64(handle), C.GoString(query), C.GoString(parameters), false)
	})))
}

//export TinySQLDatabaseQuery
func TinySQLDatabaseQuery(handle C.uint64_t, query, parameters *C.char) *C.char {
	return C.CString(string(encodeResponse(func() (response, error) {
		return runSQL(uint64(handle), C.GoString(query), C.GoString(parameters), true)
	})))
}

//export TinySQLDatabaseSave
func TinySQLDatabaseSave(handle C.uint64_t, path *C.char) *C.char {
	return C.CString(string(encodeResponse(func() (response, error) { return saveDatabase(uint64(handle), C.GoString(path)) })))
}

//export TinySQLDatabaseClose
func TinySQLDatabaseClose(handle C.uint64_t) *C.char {
	return C.CString(string(encodeResponse(func() (response, error) { return closeDatabase(uint64(handle)) })))
}

//export TinySQLDatabaseFree
func TinySQLDatabaseFree(buffer *C.char) { C.free(unsafe.Pointer(buffer)) }

func main() {}
