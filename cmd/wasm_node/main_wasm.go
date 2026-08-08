//go:build js && wasm

package main

import (
	"context"
	"encoding"
	"fmt"
	"strings"
	"syscall/js"
	"time"

	tsql "github.com/SimonWaldherr/tinySQL"
	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

const wasmQueryCacheSize = 256

// Global state
var (
	ctx = context.Background()
	// wasmStorageDB is the committed Node-local database. transactionDB is a
	// full snapshot copy used only while a JS transaction is active, avoiding
	// database/sql and the driver/connection-pool stack in the Node bundle:
	// a WASM instance only ever has one connection and one goroutine, so
	// there is nothing to pool.
	wasmStorageDB *storage.DB
	transactionDB *storage.DB
	wasmTenant    = "default"
	wasmConnected bool
	queryCache    = tsql.NewQueryCache(wasmQueryCacheSize)
)

// apiResult builds the standardized API response shape as a plain
// map[string]any so it can be handed directly to js.FuncOf's automatic
// js.ValueOf conversion, instead of being JSON-marshaled and re-parsed on the
// JS side. Field names and the omitempty-style omission of empty
// error/message match the previous APIResponse struct's `json:"..."` tags
// exactly.
func apiResult(success bool, errMsg, message string) map[string]any {
	m := map[string]any{"success": success}
	if errMsg != "" {
		m["error"] = errMsg
	}
	if message != "" {
		m["message"] = message
	}
	return m
}

// queryResultMap builds the QueryResult response shape as a plain
// map[string]any. columns/rows are passed through stringsToAny/rowsToAny so a
// nil slice still round-trips as JS null (matching what json.Marshal produced
// for a nil []string/[][]any with no `omitempty`), while a non-nil-but-empty
// slice round-trips as an empty JS array. elapsedNs mirrors the previous
// `Elapsed time.Duration` field: time.Duration has no MarshalJSON/MarshalText,
// so it always serialized as a plain nanosecond count under the (mislabeled)
// "elapsed_ms" key -- preserved here bit-for-bit rather than "fixed".
func queryResultMap(columns []string, rows [][]any, errMsg string, count int, elapsedNs int64) map[string]any {
	m := map[string]any{
		"columns":    stringsToAny(columns),
		"rows":       rowsToAny(rows),
		"count":      count,
		"elapsed_ms": elapsedNs,
	}
	if errMsg != "" {
		m["error"] = errMsg
	}
	return m
}

// stringsToAny converts a []string to []any for js.ValueOf, preserving nil.
// The return type is `any`, not `[]any`: a nil []string boxed into a []any
// return value would still carry the concrete type []any inside the
// interface, so js.ValueOf's `case []any:` would match it and produce an
// empty JS array instead of `case nil:` producing JS null. Returning bare
// `nil` from an `any`-returning function keeps it a true nil interface,
// matching what json.Marshal produced for a nil slice (JSON null).
func stringsToAny(ss []string) any {
	if ss == nil {
		return nil
	}
	out := make([]any, len(ss))
	for i, s := range ss {
		out[i] = s
	}
	return out
}

// rowsToAny converts a [][]any to []any for js.ValueOf, preserving nil (see
// stringsToAny for why the return type must be `any` rather than `[]any`).
// Each inner []any is already js.ValueOf-safe (see convertValue) and needs
// no further conversion -- only the outer container's type needs to become
// the literal []any that js.ValueOf recognizes.
func rowsToAny(rows [][]any) any {
	if rows == nil {
		return nil
	}
	out := make([]any, len(rows))
	for i, r := range rows {
		out[i] = r
	}
	return out
}

// Logger for WASM environment
func logInfo(msg string) {
	if !js.Global().Get("tinySQLWasmDebug").Truthy() {
		return
	}
	if console := js.Global().Get("console"); console.Truthy() {
		console.Call("log", fmt.Sprintf("[tinySQL-WASM] %s", msg))
	}
}

func logError(msg string, err error) {
	errMsg := fmt.Sprintf("[tinySQL-WASM] ERROR: %s", msg)
	if err != nil {
		errMsg += fmt.Sprintf(" - %v", err)
	}
	if console := js.Global().Get("console"); console.Truthy() {
		console.Call("error", errMsg)
	}
}

// validateArgs checks if the required arguments are provided
func validateArgs(args []js.Value, minCount int, expectedType js.Type) error {
	if len(args) < minCount {
		return fmt.Errorf("expected at least %d arguments, got %d", minCount, len(args))
	}
	if minCount > 0 && args[0].Type() != expectedType {
		return fmt.Errorf("expected argument type %v, got %v", expectedType, args[0].Type())
	}
	return nil
}

func currentStorageDB() *storage.DB {
	if !wasmConnected {
		return nil
	}
	if transactionDB != nil {
		return transactionDB
	}
	return wasmStorageDB
}

func bindStorageDB(next *storage.DB, dsn string) error {
	if next == nil {
		next = storage.NewDB()
	}
	tenant, err := wasmTenantFromDSN(dsn)
	if err != nil {
		return err
	}

	wasmStorageDB = next
	transactionDB = nil
	wasmTenant = tenant
	wasmConnected = true
	queryCache.Clear()
	return nil
}

// wasmTenantFromDSN preserves the small Node API's mem:// syntax without
// linking the database/sql driver. Only in-memory DSNs are supported: unlike
// the browser bundle, this Node embedding has no snapshot export/import
// helpers, so there is no equivalent persistence path to fall back to.
func wasmTenantFromDSN(dsn string) (string, error) {
	dsn = strings.TrimSpace(dsn)
	if dsn == "" {
		return "default", nil
	}
	if !strings.HasPrefix(strings.ToLower(dsn), "mem://") {
		return "", fmt.Errorf("WASM node module supports only mem:// DSNs")
	}
	tenant := "default"
	if queryAt := strings.IndexByte(dsn, '?'); queryAt >= 0 {
		for _, field := range strings.Split(dsn[queryAt+1:], "&") {
			key, value, ok := strings.Cut(field, "=")
			if ok && strings.EqualFold(strings.TrimSpace(key), "tenant") {
				tenant = strings.TrimSpace(value)
			}
		}
	}
	if tenant == "" {
		return "", fmt.Errorf("tenant must not be empty")
	}
	return tenant, nil
}

func executeWASMStatement(sqlText string) (*tsql.ResultSet, error) {
	source := currentStorageDB()
	if source == nil {
		return nil, fmt.Errorf("database not opened")
	}
	compiled, err := queryCache.Compile(sqlText)
	if err != nil {
		return nil, err
	}
	return tsql.ExecuteCompiled(ctx, source, wasmTenant, compiled)
}

// jsOpen opens a database connection
func jsOpen(this js.Value, args []js.Value) any {
	logInfo("Opening database connection...")

	// Default DSN
	dsn := "mem://?tenant=default"

	// Override with provided DSN if available
	if len(args) > 0 && args[0].Type() == js.TypeString {
		dsn = args[0].String()
		logInfo(fmt.Sprintf("Using provided DSN: %s", dsn))
	} else {
		logInfo(fmt.Sprintf("Using default DSN: %s", dsn))
	}

	if err := bindStorageDB(storage.NewDB(), dsn); err != nil {
		logError("Failed to open database", err)
		return apiResult(false, err.Error(), "")
	}

	logInfo("Database connection established successfully")
	return apiResult(true, "", "Database opened successfully")
}

// jsBegin starts a new transaction
func jsBegin(this js.Value, args []js.Value) any {
	logInfo("Starting transaction...")

	if currentStorageDB() == nil {
		return apiResult(false, "database not opened", "")
	}

	if transactionDB != nil {
		return apiResult(false, "transaction already active", "")
	}

	// A snapshot copy keeps the Node API transactional without linking the
	// database/sql driver. Save/Load preserves rows, indexes and catalog state.
	snapshot, err := storage.SaveToBytes(wasmStorageDB)
	if err != nil {
		logError("Failed to snapshot transaction", err)
		return apiResult(false, err.Error(), "")
	}
	transactionDB, err = storage.LoadFromBytes(snapshot)
	if err != nil {
		logError("Failed to open transaction snapshot", err)
		return apiResult(false, err.Error(), "")
	}

	logInfo("Transaction started successfully")
	return apiResult(true, "", "Transaction started")
}

// jsCommit commits the current transaction
func jsCommit(this js.Value, args []js.Value) any {
	logInfo("Committing transaction...")

	if transactionDB == nil {
		return apiResult(false, "no active transaction", "")
	}

	wasmStorageDB = transactionDB
	transactionDB = nil
	logInfo("Transaction committed successfully")
	return apiResult(true, "", "Transaction committed")
}

// jsRollback rolls back the current transaction
func jsRollback(this js.Value, args []js.Value) any {
	logInfo("Rolling back transaction...")

	if transactionDB == nil {
		return apiResult(false, "no active transaction", "")
	}

	transactionDB = nil
	logInfo("Transaction rolled back successfully")
	return apiResult(true, "", "Transaction rolled back")
}

// jsExec executes a SQL statement
func jsExec(this js.Value, args []js.Value) any {
	if err := validateArgs(args, 1, js.TypeString); err != nil {
		return apiResult(false, err.Error(), "")
	}

	if currentStorageDB() == nil {
		return apiResult(false, "database not opened", "")
	}

	sqlStr := args[0].String()
	logInfo(fmt.Sprintf("Executing SQL: %s", sqlStr))

	start := time.Now()
	result, err := executeWASMStatement(sqlStr)
	elapsed := time.Since(start)

	if err != nil {
		logError("SQL execution failed", err)
		return apiResult(false, err.Error(), "")
	}

	rowsAffected := resultRowsAffected(result)
	// tinySQL has no auto-increment identity concept, and the database/sql
	// driver this replaced never implemented it either (its Result type
	// always reports LastInsertId()==0), so the field is kept at 0 here for
	// message-shape compatibility with callers that parse this string.
	const lastInsertId = 0

	logInfo(fmt.Sprintf("SQL executed successfully in %v, rows affected: %d", elapsed, rowsAffected))

	return apiResult(true, "", fmt.Sprintf("Executed successfully. Rows affected: %d, Last insert ID: %d, Elapsed: %v",
		rowsAffected, lastInsertId, elapsed))
}

// jsQuery executes a SQL query and returns results
func jsQuery(this js.Value, args []js.Value) any {
	if err := validateArgs(args, 1, js.TypeString); err != nil {
		return queryResultMap(nil, nil, err.Error(), 0, 0)
	}

	if currentStorageDB() == nil {
		return queryResultMap(nil, nil, "database not opened", 0, 0)
	}

	sqlStr := args[0].String()
	logInfo(fmt.Sprintf("Executing query: %s", sqlStr))

	start := time.Now()
	resultSet, err := executeWASMStatement(sqlStr)
	if err != nil {
		logError("Query execution failed", err)
		return queryResultMap(nil, nil, err.Error(), 0, 0)
	}
	if resultSet == nil {
		resultSet = &tsql.ResultSet{}
	}

	// Prepare result rows
	rows := make([][]any, 0, len(resultSet.Rows))

	lowerColumns := make([]string, len(resultSet.Cols))
	for i, column := range resultSet.Cols {
		lowerColumns[i] = strings.ToLower(column)
	}
	for _, sourceRow := range resultSet.Rows {
		row := make([]any, len(resultSet.Cols))
		for i, column := range lowerColumns {
			row[i] = convertValue(sourceRow[column])
		}
		rows = append(rows, row)
	}

	count := len(rows)
	elapsed := time.Since(start)

	logInfo(fmt.Sprintf("Query executed successfully in %v, returned %d rows", elapsed, count))

	return queryResultMap(resultSet.Cols, rows, "", count, int64(elapsed))
}

// jsClose closes the database connection
func jsClose(this js.Value, args []js.Value) any {
	logInfo("Closing database connection...")

	if transactionDB != nil {
		logInfo("Rolling back active transaction...")
		transactionDB = nil
	}

	wasmStorageDB = nil
	wasmConnected = false
	queryCache.Clear()

	logInfo("Database connection closed successfully")
	return apiResult(true, "", "Database closed")
}

// jsStatus returns the current status of the database
func jsStatus(this js.Value, args []js.Value) any {
	status := map[string]any{
		"connected":          currentStorageDB() != nil,
		"transaction_active": transactionDB != nil,
		"driver":             "tinysql-wasm-direct",
		"version":            "1.0.0",
		"build_time":         time.Now().Format(time.RFC3339),
	}

	if source := currentStorageDB(); source != nil {
		status["connection_stats"] = map[string]any{
			"open_connections": 1,
			"tables":           len(source.ListTables(wasmTenant)),
		}
	}

	return status
}

// Helper functions

// convertValue converts database row values directly into types accepted by
// syscall/js.ValueOf, reproducing the same display semantics the previous
// json.Marshal-based path produced for each concrete type: []byte and
// time.Time keep their bespoke formatting (RFC3339Nano matches the format
// the database/sql driver this replaced used to produce, preserving
// sub-second precision), and any other value that isn't already one of
// ValueOf's native scalar kinds gets the same text json.Marshal would have
// produced via encoding.TextMarshaler (e.g. *big.Rat for DECIMAL/MONEY,
// uuid.UUID) or, failing that, a fmt.Sprintf fallback.
func convertValue(val any) any {
	if val == nil {
		return nil
	}

	switch v := val.(type) {
	case []byte:
		return string(v)
	case *any:
		return convertValue(*v)
	case time.Time:
		return v.Format(time.RFC3339Nano)
	case string, bool,
		int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64:
		return v
	case encoding.TextMarshaler:
		if text, err := v.MarshalText(); err == nil {
			return string(text)
		}
		return fmt.Sprintf("%v", val)
	default:
		return fmt.Sprintf("%v", val)
	}
}

func resultRowsAffected(result *tsql.ResultSet) int {
	if result == nil || len(result.Rows) == 0 {
		return 0
	}
	for _, key := range []string{"updated", "deleted"} {
		if value, ok := tsql.GetVal(result.Rows[0], key); ok {
			switch n := value.(type) {
			case int:
				return n
			case int64:
				return int(n)
			}
		}
	}
	return len(result.Rows)
}

// registerAPI registers all API functions with JavaScript
func registerAPI() {
	logInfo("Registering tinySQL API...")

	api := js.Global().Get("Object").New()

	// Core database operations
	api.Set("open", js.FuncOf(jsOpen))
	api.Set("close", js.FuncOf(jsClose))
	api.Set("status", js.FuncOf(jsStatus))

	// Transaction operations
	api.Set("begin", js.FuncOf(jsBegin))
	api.Set("commit", js.FuncOf(jsCommit))
	api.Set("rollback", js.FuncOf(jsRollback))

	// SQL operations
	api.Set("exec", js.FuncOf(jsExec))
	api.Set("query", js.FuncOf(jsQuery))

	// Register the API globally
	js.Global().Set("tinySQL", api)

	logInfo("tinySQL API registered successfully")

	// Emit a ready event (only in browser environment)
	if js.Global().Get("document").Truthy() {
		js.Global().Call("dispatchEvent", js.Global().Get("CustomEvent").New("tinySQLReady", map[string]any{
			"detail": map[string]any{
				"version": "1.0.0",
				"api":     []string{"open", "close", "status", "begin", "commit", "rollback", "exec", "query"},
			},
		}))
	}
}

func main() {
	logInfo("tinySQL WASM module starting...")

	// Register the API
	registerAPI()

	logInfo("tinySQL WASM module ready")

	// Keep the program alive
	select {}
}
