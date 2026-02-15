// Package main implements an ODBC (Open Database Connectivity) driver for tinySQL.
//
// This package exposes tinySQL through C-compatible ODBC API functions, allowing
// standard ODBC clients (Excel, Python pyodbc, unixODBC tools, etc.) to connect
// and query the database.
//
// Build as a shared library:
//
//	go build -buildmode=c-shared -o libtinysqlodbc.so .
//
// Register the driver with your ODBC manager (unixODBC example):
//
//	[tinySQL]
//	Description = TinySQL ODBC Driver
//	Driver = /path/to/libtinysqlodbc.so
//	Setup = /path/to/libtinysqlodbc.so
package main

/*
#include <stdlib.h>
#include <string.h>

// ODBC types and constants
typedef void* SQLHENV;
typedef void* SQLHDBC;
typedef void* SQLHSTMT;
typedef short SQLSMALLINT;
typedef unsigned short SQLUSMALLINT;
typedef int SQLINTEGER;
typedef unsigned char SQLUCHAR;
typedef long SQLLEN;
typedef unsigned long SQLULEN;
typedef void* SQLPOINTER;
typedef SQLSMALLINT SQLRETURN;

#define SQL_SUCCESS 0
#define SQL_SUCCESS_WITH_INFO 1
#define SQL_ERROR -1
#define SQL_INVALID_HANDLE -2
#define SQL_NO_DATA 100

#define SQL_HANDLE_ENV 1
#define SQL_HANDLE_DBC 2
#define SQL_HANDLE_STMT 3

#define SQL_ATTR_ODBC_VERSION 200
#define SQL_OV_ODBC3 3

#define SQL_NULL_DATA -1
#define SQL_NTS -3

#define SQL_COMMIT 0
#define SQL_ROLLBACK 1

#define SQL_DRIVER_NOPROMPT 0
#define SQL_DRIVER_COMPLETE 1
#define SQL_DRIVER_PROMPT 2
#define SQL_DRIVER_COMPLETE_REQUIRED 3

// SQL data types
#define SQL_CHAR 1
#define SQL_VARCHAR 12
#define SQL_LONGVARCHAR -1
#define SQL_WCHAR -8
#define SQL_WVARCHAR -9
#define SQL_WLONGVARCHAR -10
#define SQL_C_CHAR 1
#define SQL_C_WCHAR -8

// Statement attributes
#define SQL_ATTR_ROW_ARRAY_SIZE 27
#define SQL_ATTR_ROWS_FETCHED_PTR 26

// Connection attributes
#define SQL_ATTR_AUTOCOMMIT 102
#define SQL_AUTOCOMMIT_ON 1
#define SQL_AUTOCOMMIT_OFF 0

// Info types for SQLGetInfo
#define SQL_DRIVER_NAME 6
#define SQL_DRIVER_VER 7
#define SQL_DBMS_NAME 17
#define SQL_DBMS_VER 18
#define SQL_MAX_COLUMN_NAME_LEN 30
#define SQL_MAX_TABLE_NAME_LEN 35
#define SQL_MAX_COLUMNS_IN_SELECT 100
#define SQL_MAX_IDENTIFIER_LEN 10014
#define SQL_DATABASE_NAME 16
#define SQL_DATA_SOURCE_NAME 2
#define SQL_CATALOG_NAME_SEPARATOR 41
#define SQL_CATALOG_TERM 42
#define SQL_SCHEMA_TERM 39
#define SQL_TABLE_TERM 45
#define SQL_ACCESSIBLE_TABLES 19
#define SQL_GETDATA_EXTENSIONS 81

// Nullable info
#define SQL_NO_NULLS 0
#define SQL_NULLABLE 1
#define SQL_NULLABLE_UNKNOWN 2
*/
import "C"

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"unicode/utf8"
	"unsafe"

	tinysql "github.com/SimonWaldherr/tinySQL"
)

// valueToString converts common tinySQL cell values to string without
// always using fmt.Sprint to reduce allocations on hot paths.
func valueToString(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case []byte:
		return string(t)
	case int:
		return strconv.FormatInt(int64(t), 10)
	case int8:
		return strconv.FormatInt(int64(t), 10)
	case int16:
		return strconv.FormatInt(int64(t), 10)
	case int32:
		return strconv.FormatInt(int64(t), 10)
	case int64:
		return strconv.FormatInt(t, 10)
	case uint, uint8, uint16, uint32, uint64:
		return fmt.Sprint(t)
	case float32:
		return strconv.FormatFloat(float64(t), 'f', -1, 32)
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64)
	case bool:
		if t {
			return "true"
		}
		return "false"
	default:
		return fmt.Sprint(t)
	}
}

// Global handle registry
var (
	envMu    sync.RWMutex
	envMap           = make(map[uintptr]*environment)
	envNext  uintptr = 1
	connMap          = make(map[uintptr]*connection)
	connNext uintptr = 1
	stmtMap          = make(map[uintptr]*statement)
	stmtNext uintptr = 1
)

// environment represents an ODBC environment (HENV)
type environment struct {
	id      uintptr
	version int
}

// connection represents an ODBC connection (HDBC)
type connection struct {
	id     uintptr
	envID  uintptr
	db     *tinysql.DB
	tenant string
	inTx   bool
}

// statement represents an ODBC statement (HSTMT)
type statement struct {
	id       uintptr
	connID   uintptr
	sql      string
	rs       *tinysql.ResultSet
	rowIndex int
	// Reusable buffers to avoid per-row allocations in hot paths.
	utf16Buf []uint16
	// charBuf could be added if we needed to reuse byte buffers for ASCII copies.
}

// ============================================================================
// ODBC API Functions - exported to C
// ============================================================================

// SQLAllocHandle allocates an ODBC handle of the requested type and returns
// SQL_SUCCESS on success. This matches the ODBC API's SQLAllocHandle.
func SQLAllocHandle(handleType C.SQLSMALLINT, inputHandle C.SQLPOINTER, outputHandlePtr *C.SQLPOINTER) C.SQLRETURN {
	envMu.Lock()
	defer envMu.Unlock()

	switch handleType {
	case C.SQL_HANDLE_ENV:
		env := &environment{id: envNext}
		envNext++
		envMap[env.id] = env
		*outputHandlePtr = C.SQLPOINTER(unsafe.Pointer(uintptr(env.id)))
		return C.SQL_SUCCESS

	case C.SQL_HANDLE_DBC:
		envID := uintptr(inputHandle)
		if _, ok := envMap[envID]; !ok {
			return C.SQL_INVALID_HANDLE
		}
		conn := &connection{
			id:     connNext,
			envID:  envID,
			tenant: "default",
		}
		connNext++
		connMap[conn.id] = conn
		*outputHandlePtr = C.SQLPOINTER(unsafe.Pointer(uintptr(conn.id)))
		return C.SQL_SUCCESS

	case C.SQL_HANDLE_STMT:
		connID := uintptr(inputHandle)
		if _, ok := connMap[connID]; !ok {
			return C.SQL_INVALID_HANDLE
		}
		stmt := &statement{
			id:     stmtNext,
			connID: connID,
		}
		stmtNext++
		stmtMap[stmt.id] = stmt
		*outputHandlePtr = C.SQLPOINTER(unsafe.Pointer(uintptr(stmt.id)))
		return C.SQL_SUCCESS

	default:
		return C.SQL_ERROR
	}
}

// SQLFreeHandle releases an ODBC handle previously allocated with
// SQLAllocHandle. It returns SQL_SUCCESS on success.
func SQLFreeHandle(handleType C.SQLSMALLINT, handle C.SQLPOINTER) C.SQLRETURN {
	envMu.Lock()
	defer envMu.Unlock()

	id := uintptr(handle)
	switch handleType {
	case C.SQL_HANDLE_ENV:
		delete(envMap, id)
		return C.SQL_SUCCESS
	case C.SQL_HANDLE_DBC:
		delete(connMap, id)
		return C.SQL_SUCCESS
	case C.SQL_HANDLE_STMT:
		delete(stmtMap, id)
		return C.SQL_SUCCESS
	default:
		return C.SQL_ERROR
	}
}

// SQLSetEnvAttr sets attributes on an ODBC environment handle. Only
// SQL_ATTR_ODBC_VERSION is supported by this shim.
func SQLSetEnvAttr(environmentHandle C.SQLHENV, attribute C.SQLINTEGER, valuePtr C.SQLPOINTER, stringLength C.SQLINTEGER) C.SQLRETURN {
	envMu.Lock()
	defer envMu.Unlock()

	envID := uintptr(environmentHandle)
	env, ok := envMap[envID]
	if !ok {
		return C.SQL_INVALID_HANDLE
	}

	if attribute == C.SQL_ATTR_ODBC_VERSION {
		version := int(uintptr(valuePtr))
		env.version = version
		return C.SQL_SUCCESS
	}

	return C.SQL_SUCCESS
}

// SQLConnect establishes a connection to a tinySQL database. The
// serverName parameter may be a DSN like "mem://" or "file:/path".
func SQLConnect(connectionHandle C.SQLHDBC, serverName *C.SQLUCHAR, nameLength1 C.SQLSMALLINT,
	userName *C.SQLUCHAR, nameLength2 C.SQLSMALLINT, authentication *C.SQLUCHAR, nameLength3 C.SQLSMALLINT) C.SQLRETURN {

	envMu.Lock()
	defer envMu.Unlock()

	connID := uintptr(connectionHandle)
	conn, ok := connMap[connID]
	if !ok {
		return C.SQL_INVALID_HANDLE
	}

	dsn := ""
	if serverName != nil {
		dsn = C.GoString((*C.char)(unsafe.Pointer(serverName)))
	}

	var db *tinysql.DB
	var err error
	if dsn == "" || dsn == "mem://" {
		db = tinysql.NewDB()
	} else if len(dsn) > 5 && dsn[:5] == "file:" {
		path := dsn[5:]
		db, err = tinysql.LoadFromFile(path)
		if err != nil {
			db = tinysql.NewDB()
		}
	} else {
		db = tinysql.NewDB()
	}

	conn.db = db
	return C.SQL_SUCCESS
}

// SQLDriverConnect connects using a full connection string. The function
// parses common keys (DSN, Database) and opens the corresponding DB.
func SQLDriverConnect(connectionHandle C.SQLHDBC, windowHandle C.SQLPOINTER, inConnectionString *C.SQLUCHAR, stringLength1 C.SQLSMALLINT,
	outConnectionString *C.SQLUCHAR, bufferLength C.SQLSMALLINT, stringLength2Ptr *C.SQLSMALLINT, driverCompletion C.SQLUSMALLINT) C.SQLRETURN {

	envMu.Lock()
	defer envMu.Unlock()

	connID := uintptr(connectionHandle)
	conn, ok := connMap[connID]
	if !ok {
		return C.SQL_INVALID_HANDLE
	}

	connStr := ""
	if inConnectionString != nil {
		connStr = C.GoString((*C.char)(unsafe.Pointer(inConnectionString)))
	}

	// Parse connection string to extract DSN or Database parameter
	dsn := ""
	// Simple parsing for DSN=xxx or Database=xxx
	for _, part := range []string{"DSN=", "Database=", "DATABASE=", "Server=", "SERVER="} {
		if len(connStr) > len(part) {
			idx := 0
			for i := 0; i < len(connStr)-len(part)+1; i++ {
				if connStr[i:i+len(part)] == part {
					idx = i + len(part)
					end := idx
					for end < len(connStr) && connStr[end] != ';' {
						end++
					}
					dsn = connStr[idx:end]
					break
				}
			}
			if dsn != "" {
				break
			}
		}
	}

	var db *tinysql.DB
	var err error
	if dsn == "" || dsn == "mem://" || dsn == "tinysql_memory" {
		db = tinysql.NewDB()
	} else if len(dsn) > 5 && dsn[:5] == "file:" {
		path := dsn[5:]
		db, err = tinysql.LoadFromFile(path)
		if err != nil {
			db = tinysql.NewDB()
		}
	} else {
		// Try as file path
		db, err = tinysql.LoadFromFile(dsn)
		if err != nil {
			db = tinysql.NewDB()
		}
	}

	conn.db = db

	// Copy connection string back if requested
	if outConnectionString != nil && bufferLength > 0 {
		cStr := C.CString(connStr)
		defer C.free(unsafe.Pointer(cStr))
		C.strncpy((*C.char)(unsafe.Pointer(outConnectionString)), cStr, C.size_t(bufferLength))
	}
	if stringLength2Ptr != nil {
		*stringLength2Ptr = C.SQLSMALLINT(len(connStr))
	}

	return C.SQL_SUCCESS
}

// SQLDisconnect closes the ODBC connection handle (logical disconnect).
// It does not persist or destroy the underlying tinySQL DB instance.
func SQLDisconnect(connectionHandle C.SQLHDBC) C.SQLRETURN {
	envMu.Lock()
	defer envMu.Unlock()

	connID := uintptr(connectionHandle)
	conn, ok := connMap[connID]
	if !ok {
		return C.SQL_INVALID_HANDLE
	}

	conn.db = nil
	return C.SQL_SUCCESS
}

// SQLExecDirect executes the provided SQL statement directly and stores
// the resulting ResultSet on the statement handle for subsequent fetches.
func SQLExecDirect(statementHandle C.SQLHSTMT, statementText *C.SQLUCHAR, textLength C.SQLINTEGER) C.SQLRETURN {
	envMu.RLock()
	stmtID := uintptr(statementHandle)
	stmt, ok := stmtMap[stmtID]
	if !ok {
		envMu.RUnlock()
		return C.SQL_INVALID_HANDLE
	}
	conn, ok := connMap[stmt.connID]
	if !ok {
		envMu.RUnlock()
		return C.SQL_INVALID_HANDLE
	}
	envMu.RUnlock()

	if conn.db == nil {
		return C.SQL_ERROR
	}

	sql := C.GoString((*C.char)(unsafe.Pointer(statementText)))
	stmt.sql = sql

	parser := tinysql.NewParser(sql)
	st, err := parser.ParseStatement()
	if err != nil {
		return C.SQL_ERROR
	}

	ctx := context.Background()
	rs, err := tinysql.Execute(ctx, conn.db, conn.tenant, st)
	if err != nil {
		return C.SQL_ERROR
	}

	stmt.rs = rs
	stmt.rowIndex = 0

	return C.SQL_SUCCESS
}

// SQLFetch advances the result cursor to the next row. Returns SQL_NO_DATA
// when no more rows are available.
func SQLFetch(statementHandle C.SQLHSTMT) C.SQLRETURN {
	envMu.RLock()
	defer envMu.RUnlock()

	stmtID := uintptr(statementHandle)
	stmt, ok := stmtMap[stmtID]
	if !ok {
		return C.SQL_INVALID_HANDLE
	}

	if stmt.rs == nil || stmt.rowIndex >= len(stmt.rs.Rows) {
		return C.SQL_NO_DATA
	}

	stmt.rowIndex++
	return C.SQL_SUCCESS
}

// SQLGetData retrieves column data for the current row into the caller
// provided buffer. Supports `SQL_C_CHAR` and `SQL_C_WCHAR` target types.
func SQLGetData(statementHandle C.SQLHSTMT, columnNumber C.SQLUSMALLINT, targetType C.SQLSMALLINT,
	targetValuePtr C.SQLPOINTER, bufferLength C.SQLLEN, strLenOrIndPtr *C.SQLLEN) C.SQLRETURN {

	envMu.RLock()
	defer envMu.RUnlock()

	stmtID := uintptr(statementHandle)
	stmt, ok := stmtMap[stmtID]
	if !ok {
		return C.SQL_INVALID_HANDLE
	}

	if stmt.rs == nil || stmt.rowIndex <= 0 || stmt.rowIndex > len(stmt.rs.Rows) {
		return C.SQL_ERROR
	}

	row := stmt.rs.Rows[stmt.rowIndex-1]
	colIdx := int(columnNumber) - 1
	if colIdx < 0 || colIdx >= len(stmt.rs.Cols) {
		return C.SQL_ERROR
	}

	colName := stmt.rs.Cols[colIdx]
	val, ok := tinysql.GetVal(row, colName)
	if !ok {
		if strLenOrIndPtr != nil {
			*strLenOrIndPtr = C.SQL_NULL_DATA
		}
		return C.SQL_SUCCESS
	}

	strVal := fmt.Sprintf("%v", val)

	// Reuse statement buffer to avoid per-call allocations for UTF-16 conversion.
	// Obtain the statement pointer (we already have stmt from map above).
	s := stmt

	// Handle wide char (UTF-16) encoding for SQL_C_WCHAR
	if targetType == C.SQL_C_WCHAR {
		// Count runes (needed for resulting UTF-16 length)
		runeCount := utf8.RuneCountInString(strVal)
		needed := runeCount + 1 // null terminator
		if cap(s.utf16Buf) < needed {
			s.utf16Buf = make([]uint16, 0, needed)
		}
		s.utf16Buf = s.utf16Buf[:0]

		for _, r := range strVal {
			if r < 0x10000 {
				s.utf16Buf = append(s.utf16Buf, uint16(r))
			} else {
				r -= 0x10000
				s.utf16Buf = append(s.utf16Buf, uint16(0xD800+(r>>10)))
				s.utf16Buf = append(s.utf16Buf, uint16(0xDC00+(r&0x3FF)))
			}
		}
		s.utf16Buf = append(s.utf16Buf, 0)

		if targetValuePtr != nil && bufferLength > 0 && len(s.utf16Buf) > 0 {
			bytesToCopy := len(s.utf16Buf) * 2
			if bytesToCopy > int(bufferLength) {
				bytesToCopy = int(bufferLength)
			}
			C.memcpy(unsafe.Pointer(targetValuePtr), unsafe.Pointer(&s.utf16Buf[0]), C.size_t(bytesToCopy))
		}

		if strLenOrIndPtr != nil {
			*strLenOrIndPtr = C.SQLLEN((len(s.utf16Buf) - 1) * 2)
		}
	} else {
		// Handle regular C string (SQL_C_CHAR) without allocating C string.
		if targetValuePtr != nil && bufferLength > 0 {
			// Copy up to bufferLength-1 bytes and null-terminate.
			lenToCopy := len(strVal)
			if lenToCopy >= int(bufferLength) {
				lenToCopy = int(bufferLength) - 1
			}
			if lenToCopy > 0 {
				srcPtr := unsafe.Pointer(unsafe.StringData(strVal))
				C.memcpy(unsafe.Pointer(targetValuePtr), srcPtr, C.size_t(lenToCopy))
			}
			// Null terminate
			targetPtr := unsafe.Pointer(uintptr(unsafe.Pointer(targetValuePtr)) + uintptr(lenToCopy))
			*(*C.char)(targetPtr) = 0
		}

		if strLenOrIndPtr != nil {
			*strLenOrIndPtr = C.SQLLEN(len(strVal))
		}
	}

	return C.SQL_SUCCESS
}

// SQLNumResultCols returns the number of columns in the current result set.
func SQLNumResultCols(statementHandle C.SQLHSTMT, columnCountPtr *C.SQLSMALLINT) C.SQLRETURN {
	envMu.RLock()
	defer envMu.RUnlock()

	stmtID := uintptr(statementHandle)
	stmt, ok := stmtMap[stmtID]
	if !ok {
		return C.SQL_INVALID_HANDLE
	}

	if stmt.rs == nil {
		*columnCountPtr = 0
	} else {
		*columnCountPtr = C.SQLSMALLINT(len(stmt.rs.Cols))
	}

	return C.SQL_SUCCESS
}

// SQLRowCount returns the number of rows in the current result set (if known).
func SQLRowCount(statementHandle C.SQLHSTMT, rowCountPtr *C.SQLLEN) C.SQLRETURN {
	envMu.RLock()
	defer envMu.RUnlock()

	stmtID := uintptr(statementHandle)
	stmt, ok := stmtMap[stmtID]
	if !ok {
		return C.SQL_INVALID_HANDLE
	}

	if stmt.rs == nil {
		*rowCountPtr = 0
	} else {
		*rowCountPtr = C.SQLLEN(len(stmt.rs.Rows))
	}

	return C.SQL_SUCCESS
}

// SQLDescribeCol provides basic metadata about a result column (name, type,
// size, nullable). It fills the supplied output buffers.
func SQLDescribeCol(statementHandle C.SQLHSTMT, columnNumber C.SQLUSMALLINT,
	columnName *C.SQLUCHAR, bufferLength C.SQLSMALLINT, nameLengthPtr *C.SQLSMALLINT,
	dataTypePtr *C.SQLSMALLINT, columnSizePtr *C.SQLULEN, decimalDigitsPtr *C.SQLSMALLINT, nullablePtr *C.SQLSMALLINT) C.SQLRETURN {

	envMu.RLock()
	defer envMu.RUnlock()

	stmtID := uintptr(statementHandle)
	stmt, ok := stmtMap[stmtID]
	if !ok {
		return C.SQL_INVALID_HANDLE
	}

	if stmt.rs == nil {
		return C.SQL_ERROR
	}

	colIdx := int(columnNumber) - 1
	if colIdx < 0 || colIdx >= len(stmt.rs.Cols) {
		return C.SQL_ERROR
	}

	colNameStr := stmt.rs.Cols[colIdx]
	if columnName != nil && bufferLength > 0 {
		lenToCopy := len(colNameStr)
		if lenToCopy >= int(bufferLength) {
			lenToCopy = int(bufferLength) - 1
		}
		if lenToCopy > 0 {
			srcPtr := unsafe.Pointer(unsafe.StringData(colNameStr))
			C.memcpy(unsafe.Pointer(columnName), srcPtr, C.size_t(lenToCopy))
		}
		// Null terminate
		termPtr := unsafe.Pointer(uintptr(unsafe.Pointer(columnName)) + uintptr(lenToCopy))
		*(*C.char)(termPtr) = 0
	}
	if nameLengthPtr != nil {
		*nameLengthPtr = C.SQLSMALLINT(len(colNameStr))
	}

	if dataTypePtr != nil {
		*dataTypePtr = 12 // SQL_VARCHAR
	}
	if columnSizePtr != nil {
		*columnSizePtr = 255
	}
	if decimalDigitsPtr != nil {
		*decimalDigitsPtr = 0
	}
	if nullablePtr != nil {
		*nullablePtr = 1 // SQL_NULLABLE
	}

	return C.SQL_SUCCESS
}

// SQLEndTran commits or rolls back a transaction associated with the handle.
// This implementation tracks an `inTx` flag on the connection object.
func SQLEndTran(handleType C.SQLSMALLINT, handle C.SQLPOINTER, completionType C.SQLSMALLINT) C.SQLRETURN {
	envMu.Lock()
	defer envMu.Unlock()

	if handleType == C.SQL_HANDLE_DBC {
		connID := uintptr(handle)
		conn, ok := connMap[connID]
		if !ok {
			return C.SQL_INVALID_HANDLE
		}

		if completionType == C.SQL_COMMIT {
			conn.inTx = false
			return C.SQL_SUCCESS
		} else if completionType == C.SQL_ROLLBACK {
			conn.inTx = false
			return C.SQL_SUCCESS
		}
	}

	return C.SQL_ERROR
}

// SQLPrepare stores the provided SQL text on the statement handle for later
// execution via SQLExecute. It does not perform parsing until execute time.
func SQLPrepare(statementHandle C.SQLHSTMT, statementText *C.SQLUCHAR, textLength C.SQLINTEGER) C.SQLRETURN {
	envMu.Lock()
	defer envMu.Unlock()

	stmtID := uintptr(statementHandle)
	stmt, ok := stmtMap[stmtID]
	if !ok {
		return C.SQL_INVALID_HANDLE
	}

	sql := C.GoString((*C.char)(unsafe.Pointer(statementText)))
	stmt.sql = sql

	return C.SQL_SUCCESS
}

// SQLExecute runs a previously prepared statement (SQLPrepare) and delegates
// to SQLExecDirect for execution.
func SQLExecute(statementHandle C.SQLHSTMT) C.SQLRETURN {
	envMu.RLock()
	stmtID := uintptr(statementHandle)
	stmt, ok := stmtMap[stmtID]
	if !ok {
		envMu.RUnlock()
		return C.SQL_INVALID_HANDLE
	}
	sql := stmt.sql
	envMu.RUnlock()

	cSQL := C.CString(sql)
	defer C.free(unsafe.Pointer(cSQL))

	return SQLExecDirect(statementHandle, (*C.SQLUCHAR)(unsafe.Pointer(cSQL)), C.SQL_NTS)
}

// SQLGetInfo returns driver and data source information strings/numbers as
// defined by the ODBC specification (driver name, version, limits, etc.).
func SQLGetInfo(connectionHandle C.SQLHDBC, infoType C.SQLUSMALLINT, infoValuePtr C.SQLPOINTER, bufferLength C.SQLSMALLINT, stringLengthPtr *C.SQLSMALLINT) C.SQLRETURN {
	envMu.RLock()
	defer envMu.RUnlock()

	connID := uintptr(connectionHandle)
	_, ok := connMap[connID]
	if !ok {
		return C.SQL_INVALID_HANDLE
	}

	var infoStr string
	var infoInt int

	switch infoType {
	case C.SQL_DRIVER_NAME:
		infoStr = "libtinysqlodbc.dylib"
	case C.SQL_DRIVER_VER:
		infoStr = "01.00.0000"
	case C.SQL_DBMS_NAME:
		infoStr = "tinySQL"
	case C.SQL_DBMS_VER:
		infoStr = "01.00.0000"
	case C.SQL_DATABASE_NAME:
		infoStr = "tinySQL"
	case C.SQL_DATA_SOURCE_NAME:
		infoStr = "tinySQL"
	case C.SQL_CATALOG_NAME_SEPARATOR:
		infoStr = "."
	case C.SQL_CATALOG_TERM:
		infoStr = "database"
	case C.SQL_SCHEMA_TERM:
		infoStr = "schema"
	case C.SQL_TABLE_TERM:
		infoStr = "table"
	case C.SQL_ACCESSIBLE_TABLES:
		infoStr = "Y"
	case C.SQL_MAX_COLUMN_NAME_LEN:
		infoInt = 128
	case C.SQL_MAX_TABLE_NAME_LEN:
		infoInt = 128
	case C.SQL_MAX_COLUMNS_IN_SELECT:
		infoInt = 1000
	case C.SQL_MAX_IDENTIFIER_LEN:
		infoInt = 128
	case C.SQL_GETDATA_EXTENSIONS:
		infoInt = 0
	default:
		return C.SQL_ERROR
	}

	if infoStr != "" {
		if infoValuePtr != nil && bufferLength > 0 {
			lenToCopy := len(infoStr)
			if lenToCopy >= int(bufferLength) {
				lenToCopy = int(bufferLength) - 1
			}
			if lenToCopy > 0 {
				srcPtr := unsafe.Pointer(unsafe.StringData(infoStr))
				C.memcpy(unsafe.Pointer(infoValuePtr), srcPtr, C.size_t(lenToCopy))
			}
			// Null terminate
			termPtr := unsafe.Pointer(uintptr(unsafe.Pointer(infoValuePtr)) + uintptr(lenToCopy))
			*(*C.char)(termPtr) = 0
		}
		if stringLengthPtr != nil {
			*stringLengthPtr = C.SQLSMALLINT(len(infoStr))
		}
	} else if infoValuePtr != nil {
		*(*C.SQLUSMALLINT)(infoValuePtr) = C.SQLUSMALLINT(infoInt)
	}

	return C.SQL_SUCCESS
}

// SQLTables returns a result set describing tables accessible in the current
// connection. It maps tinySQL's SHOW TABLES output to the ODBC standard
// result layout.
func SQLTables(statementHandle C.SQLHSTMT, catalogName *C.SQLUCHAR, nameLength1 C.SQLSMALLINT,
	schemaName *C.SQLUCHAR, nameLength2 C.SQLSMALLINT, tableName *C.SQLUCHAR, nameLength3 C.SQLSMALLINT,
	tableType *C.SQLUCHAR, nameLength4 C.SQLSMALLINT) C.SQLRETURN {

	envMu.RLock()
	stmtID := uintptr(statementHandle)
	stmt, ok := stmtMap[stmtID]
	if !ok {
		envMu.RUnlock()
		return C.SQL_INVALID_HANDLE
	}
	conn, ok := connMap[stmt.connID]
	if !ok {
		envMu.RUnlock()
		return C.SQL_INVALID_HANDLE
	}
	envMu.RUnlock()

	if conn.db == nil {
		return C.SQL_ERROR
	}

	// Get list of tables from tinySQL
	ctx := context.Background()
	parser := tinysql.NewParser("SHOW TABLES")
	st, err := parser.ParseStatement()
	if err != nil {
		// If SHOW TABLES not supported, return empty result set
		stmt.rs = &tinysql.ResultSet{
			Cols: []string{"table_cat", "table_schem", "table_name", "table_type", "remarks"},
			Rows: []tinysql.Row{},
		}
		stmt.rowIndex = 0
		return C.SQL_SUCCESS
	}

	rs, err := tinysql.Execute(ctx, conn.db, conn.tenant, st)
	if err != nil {
		stmt.rs = &tinysql.ResultSet{
			Cols: []string{"table_cat", "table_schem", "table_name", "table_type", "remarks"},
			Rows: []tinysql.Row{},
		}
		stmt.rowIndex = 0
		return C.SQL_SUCCESS
	}

	// Convert to ODBC standard table format (avoid fmt for common types)
	odbcRows := make([]tinysql.Row, 0)
	for _, row := range rs.Rows {
		for _, col := range rs.Cols {
			if val, ok := tinysql.GetVal(row, col); ok {
				tableName := valueToString(val)
				odbcRows = append(odbcRows, tinysql.Row{
					"table_cat":   "",
					"table_schem": "",
					"table_name":  tableName,
					"table_type":  "TABLE",
					"remarks":     "",
				})
			}
		}
	}

	stmt.rs = &tinysql.ResultSet{
		Cols: []string{"table_cat", "table_schem", "table_name", "table_type", "remarks"},
		Rows: odbcRows,
	}
	stmt.rowIndex = 0

	return C.SQL_SUCCESS
}

// SQLColumns returns column metadata for the named table. It produces the
// standard ODBC columns: table_name, column_name, data_type, etc.
func SQLColumns(statementHandle C.SQLHSTMT, catalogName *C.SQLUCHAR, nameLength1 C.SQLSMALLINT,
	schemaName *C.SQLUCHAR, nameLength2 C.SQLSMALLINT, tableName *C.SQLUCHAR, nameLength3 C.SQLSMALLINT,
	columnName *C.SQLUCHAR, nameLength4 C.SQLSMALLINT) C.SQLRETURN {

	envMu.RLock()
	stmtID := uintptr(statementHandle)
	stmt, ok := stmtMap[stmtID]
	if !ok {
		envMu.RUnlock()
		return C.SQL_INVALID_HANDLE
	}
	conn, ok := connMap[stmt.connID]
	if !ok {
		envMu.RUnlock()
		return C.SQL_INVALID_HANDLE
	}
	envMu.RUnlock()

	if conn.db == nil {
		return C.SQL_ERROR
	}

	table := ""
	if tableName != nil {
		// Avoid C string allocation; copy Go string from C pointer
		table = C.GoString((*C.char)(unsafe.Pointer(tableName)))
	}

	// Query the table to get column information
	ctx := context.Background()
	query := fmt.Sprintf("SELECT * FROM %s LIMIT 0", table)
	parser := tinysql.NewParser(query)
	st, err := parser.ParseStatement()
	if err != nil {
		stmt.rs = &tinysql.ResultSet{
			Cols: []string{"table_cat", "table_schem", "table_name", "column_name", "data_type", "type_name", "column_size", "buffer_length", "decimal_digits", "num_prec_radix", "nullable", "remarks"},
			Rows: []tinysql.Row{},
		}
		stmt.rowIndex = 0
		return C.SQL_SUCCESS
	}

	rs, err := tinysql.Execute(ctx, conn.db, conn.tenant, st)
	if err != nil {
		stmt.rs = &tinysql.ResultSet{
			Cols: []string{"table_cat", "table_schem", "table_name", "column_name", "data_type", "type_name", "column_size", "buffer_length", "decimal_digits", "num_prec_radix", "nullable", "remarks"},
			Rows: []tinysql.Row{},
		}
		stmt.rowIndex = 0
		return C.SQL_SUCCESS
	}

	// Convert columns to ODBC standard format
	odbcRows := make([]tinysql.Row, 0)
	for _, colName := range rs.Cols {
		odbcRows = append(odbcRows, tinysql.Row{
			"table_cat":      "",
			"table_schem":    "",
			"table_name":     table,
			"column_name":    colName,
			"data_type":      "12", // SQL_VARCHAR
			"type_name":      "VARCHAR",
			"column_size":    "255",
			"buffer_length":  "255",
			"decimal_digits": "",
			"num_prec_radix": "10",
			"nullable":       "1", // SQL_NULLABLE
			"remarks":        "",
		})
	}

	stmt.rs = &tinysql.ResultSet{
		Cols: []string{"table_cat", "table_schem", "table_name", "column_name", "data_type", "type_name", "column_size", "buffer_length", "decimal_digits", "num_prec_radix", "nullable", "remarks"},
		Rows: odbcRows,
	}
	stmt.rowIndex = 0

	return C.SQL_SUCCESS
}

// SQLSetConnectAttr sets connection-level attributes such as autocommit.
// This shim accepts the attribute and returns success without extensive
// side effects.
func SQLSetConnectAttr(connectionHandle C.SQLHDBC, attribute C.SQLINTEGER, valuePtr C.SQLPOINTER, stringLength C.SQLINTEGER) C.SQLRETURN {
	envMu.Lock()
	defer envMu.Unlock()

	connID := uintptr(connectionHandle)
	conn, ok := connMap[connID]
	if !ok {
		return C.SQL_INVALID_HANDLE
	}

	if attribute == C.SQL_ATTR_AUTOCOMMIT {
		// Just accept autocommit setting
		_ = conn
		return C.SQL_SUCCESS
	}

	return C.SQL_SUCCESS
}

// SQLSetStmtAttr sets statement-level attributes. The implementation
// currently accepts attributes but does not change behavior.
func SQLSetStmtAttr(statementHandle C.SQLHSTMT, attribute C.SQLINTEGER, valuePtr C.SQLPOINTER, stringLength C.SQLINTEGER) C.SQLRETURN {
	envMu.Lock()
	defer envMu.Unlock()

	stmtID := uintptr(statementHandle)
	_, ok := stmtMap[stmtID]
	if !ok {
		return C.SQL_INVALID_HANDLE
	}

	// Accept statement attributes but don't need to act on them
	return C.SQL_SUCCESS
}

// SQLMoreResults reports whether additional result sets are available for a
// statement. tinySQL does not support multiple result sets, so this returns
// SQL_NO_DATA.
func SQLMoreResults(statementHandle C.SQLHSTMT) C.SQLRETURN {
	return C.SQL_NO_DATA
}

// SQLGetDiagRec returns diagnostic information for the last error.
// This simplified implementation returns a generic error message when
// requested; it can be extended to provide richer diagnostics.
func SQLGetDiagRec(handleType C.SQLSMALLINT, handle C.SQLPOINTER, recNumber C.SQLSMALLINT,
	sqlState *C.SQLUCHAR, nativeErrorPtr *C.SQLINTEGER, messageText *C.SQLUCHAR,
	bufferLength C.SQLSMALLINT, textLengthPtr *C.SQLSMALLINT) C.SQLRETURN {

	// Simple implementation - return generic error message
	if recNumber > 1 {
		return C.SQL_NO_DATA
	}

	state := "HY000"
	message := "General error"

	if sqlState != nil {
		cStr := C.CString(state)
		defer C.free(unsafe.Pointer(cStr))
		C.strncpy((*C.char)(unsafe.Pointer(sqlState)), cStr, 6)
	}

	if nativeErrorPtr != nil {
		*nativeErrorPtr = 1
	}

	if messageText != nil && bufferLength > 0 {
		cStr := C.CString(message)
		defer C.free(unsafe.Pointer(cStr))
		C.strncpy((*C.char)(unsafe.Pointer(messageText)), cStr, C.size_t(bufferLength))
	}

	if textLengthPtr != nil {
		*textLengthPtr = C.SQLSMALLINT(len(message))
	}

	return C.SQL_SUCCESS
}

func main() {}
