//go:build js && wasm

package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"syscall/js"
	"time"

	tsql "github.com/SimonWaldherr/tinySQL"
	"github.com/SimonWaldherr/tinySQL/internal/engine"
	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// Global state
var (
	ctx = context.Background()
	// keep JS function references alive to avoid GC and subsequent panics
	retainedFuncs []js.Func
	// wasmStorageDB is the committed browser-local database. transactionDB is a
	// full snapshot copy used only while a JS transaction is active, avoiding
	// database/sql and the driver/server stack in the browser bundle.
	wasmStorageDB *storage.DB
	transactionDB *storage.DB
	wasmTenant    = "default"
	wasmConnected bool
)

// QueryResult represents the result of a SQL query
type QueryResult struct {
	Columns []string      `json:"columns"`
	Rows    [][]any       `json:"rows"`
	Error   string        `json:"error,omitempty"`
	Count   int           `json:"count"`
	Elapsed time.Duration `json:"elapsed_ms"`
}

// APIResponse represents a standardized API response
type APIResponse struct {
	Success bool   `json:"success"`
	Error   string `json:"error,omitempty"`
	Message string `json:"message,omitempty"`
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
	return nil
}

// wasmTenantFromDSN preserves the small browser API's mem:// syntax without
// linking the database/sql driver. Persistent DSNs are intentionally rejected:
// browser persistence is handled by exportDB/importDB and local storage.
func wasmTenantFromDSN(dsn string) (string, error) {
	dsn = strings.TrimSpace(dsn)
	if dsn == "" {
		return "default", nil
	}
	if !strings.HasPrefix(strings.ToLower(dsn), "mem://") {
		return "", fmt.Errorf("WASM supports only mem:// DSNs; use exportDB/importDB for persistence")
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
	stmt, err := tsql.ParseSQL(sqlText)
	if err != nil {
		return nil, err
	}
	return tsql.Execute(ctx, source, wasmTenant, stmt)
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
		return jsonResponse(APIResponse{Success: false, Error: err.Error()})
	}

	logInfo("Database connection established successfully")
	return jsonResponse(APIResponse{Success: true, Message: "Database opened successfully"})
}

// jsExportDB serializes the current in-memory database as a base64 GOB snapshot.
func jsExportDB(this js.Value, args []js.Value) any {
	source := currentStorageDB()
	if source == nil {
		return jsonResponse(APIResponse{Success: false, Error: "database not opened"})
	}
	data, err := storage.SaveToBytes(source)
	if err != nil {
		logError("Failed to export database", err)
		return jsonResponse(APIResponse{Success: false, Error: err.Error()})
	}
	return jsonResponse(map[string]any{
		"success":    true,
		"message":    "Database exported successfully",
		"data":       base64.StdEncoding.EncodeToString(data),
		"size_bytes": len(data),
	})
}

// jsImportDB replaces the current in-memory database with a base64 GOB snapshot.
func jsImportDB(this js.Value, args []js.Value) any {
	if err := validateArgs(args, 1, js.TypeString); err != nil {
		return jsonResponse(APIResponse{Success: false, Error: err.Error()})
	}

	encoded := strings.TrimSpace(args[0].String())
	if encoded == "" {
		return jsonResponse(APIResponse{Success: false, Error: "snapshot must not be empty"})
	}

	data, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return jsonResponse(APIResponse{Success: false, Error: fmt.Sprintf("invalid base64 snapshot: %v", err)})
	}
	loaded, err := storage.LoadFromBytes(data)
	if err != nil {
		logError("Failed to import database", err)
		return jsonResponse(APIResponse{Success: false, Error: err.Error()})
	}
	if err = bindStorageDB(loaded, "mem://?tenant=default"); err != nil {
		logError("Failed to bind imported database", err)
		return jsonResponse(APIResponse{Success: false, Error: err.Error()})
	}

	logInfo("Database snapshot imported successfully")
	return jsonResponse(map[string]any{
		"success":    true,
		"message":    "Database imported successfully",
		"size_bytes": len(data),
	})
}

// jsBegin starts a new transaction
func jsBegin(this js.Value, args []js.Value) any {
	logInfo("Starting transaction...")

	if currentStorageDB() == nil {
		return jsonResponse(APIResponse{Success: false, Error: "database not opened"})
	}

	if transactionDB != nil {
		return jsonResponse(APIResponse{Success: false, Error: "transaction already active"})
	}

	// A snapshot copy keeps the browser API transactional without linking the
	// database/sql driver. Save/Load preserves rows, indexes and catalog state.
	snapshot, err := storage.SaveToBytes(wasmStorageDB)
	if err != nil {
		logError("Failed to snapshot transaction", err)
		return jsonResponse(APIResponse{Success: false, Error: err.Error()})
	}
	transactionDB, err = storage.LoadFromBytes(snapshot)
	if err != nil {
		logError("Failed to open transaction snapshot", err)
		return jsonResponse(APIResponse{Success: false, Error: err.Error()})
	}

	logInfo("Transaction started successfully")
	return jsonResponse(APIResponse{Success: true, Message: "Transaction started"})
}

// jsExplain returns a simple query plan for a given SQL string.
func jsExplain(this js.Value, args []js.Value) any {
	if len(args) < 1 || args[0].Type() != js.TypeString {
		return jsonResponse(APIResponse{Success: false, Error: "sql string required"})
	}
	sqlStr := args[0].String()

	stmt, err := tsql.ParseSQL(sqlStr)
	if err != nil {
		return jsonResponse(map[string]any{"error": err.Error()})
	}

	// Build a simple plan representation
	type PlanStep struct {
		Operation string `json:"operation"`
		Object    string `json:"object"`
		Cost      string `json:"cost"`
		Details   string `json:"details"`
	}
	plan := make([]PlanStep, 0)

	switch s := stmt.(type) {
	case *engine.Select:
		if s.From.Table != "" {
			plan = append(plan, PlanStep{Operation: "TABLE SCAN", Object: s.From.Table, Cost: "low", Details: "Sequential scan of table"})
		}
		for _, join := range s.Joins {
			joinTypeStr := "INNER"
			switch join.Type {
			case engine.JoinLeft:
				joinTypeStr = "LEFT"
			case engine.JoinRight:
				joinTypeStr = "RIGHT"
			}
			plan = append(plan, PlanStep{Operation: "NESTED LOOP JOIN", Object: join.Right.Table, Cost: "medium", Details: fmt.Sprintf("%s join", joinTypeStr)})
		}
		if s.Where != nil {
			plan = append(plan, PlanStep{Operation: "FILTER", Object: "-", Cost: "low", Details: "Apply WHERE conditions"})
		}
		if len(s.GroupBy) > 0 {
			plan = append(plan, PlanStep{Operation: "AGGREGATE", Object: "-", Cost: "medium", Details: "Group and aggregate"})
		}
		if len(s.OrderBy) > 0 {
			plan = append(plan, PlanStep{Operation: "SORT", Object: "-", Cost: "medium-high", Details: "Sort results"})
		}
		if s.Limit != nil || s.Offset != nil {
			plan = append(plan, PlanStep{Operation: "LIMIT/OFFSET", Object: "-", Cost: "low", Details: "Apply row limits"})
		}
		plan = append(plan, PlanStep{Operation: "PROJECT", Object: "-", Cost: "low", Details: fmt.Sprintf("Return %d columns", len(s.Projs))})
	case *engine.Insert:
		plan = append(plan, PlanStep{Operation: "INSERT", Object: s.Table, Cost: "low", Details: fmt.Sprintf("Insert %d row(s)", len(s.Rows))})
	case *engine.Update:
		plan = append(plan, PlanStep{Operation: "TABLE SCAN", Object: s.Table, Cost: "low"})
		plan = append(plan, PlanStep{Operation: "UPDATE", Object: s.Table, Cost: "low", Details: fmt.Sprintf("Update %d columns", len(s.Sets))})
	case *engine.Delete:
		plan = append(plan, PlanStep{Operation: "TABLE SCAN", Object: s.Table, Cost: "low"})
		plan = append(plan, PlanStep{Operation: "DELETE", Object: s.Table, Cost: "low"})
	default:
		plan = append(plan, PlanStep{Operation: "UNKNOWN", Object: "-", Cost: "-", Details: "Cannot build plan for this statement type"})
	}

	return jsonResponse(map[string]any{"plan": plan})
}

// jsListTables returns all table names in the current storage DB tenant.
func jsListTables(this js.Value, args []js.Value) any {
	source := currentStorageDB()
	if source == nil {
		return jsonResponse(map[string]any{"error": "database not initialized"})
	}
	tenant := wasmTenant
	if len(args) > 0 && args[0].Type() == js.TypeString {
		tenant = args[0].String()
	}
	tables := source.ListTables(tenant)
	names := make([]string, 0, len(tables))
	for _, t := range tables {
		names = append(names, t.Name)
	}
	return jsonResponse(map[string]any{"tables": names})
}

// jsDescribeTable returns column information for a given table.
func jsDescribeTable(this js.Value, args []js.Value) any {
	source := currentStorageDB()
	if source == nil {
		return jsonResponse(map[string]any{"error": "database not initialized"})
	}
	if len(args) < 1 || args[0].Type() != js.TypeString {
		return jsonResponse(map[string]any{"error": "table name required"})
	}
	tenant := wasmTenant
	tableName := args[0].String()
	if len(args) > 1 && args[1].Type() == js.TypeString {
		tenant = args[1].String()
	}
	t, err := source.Get(tenant, tableName)
	if err != nil || t == nil {
		return jsonResponse(map[string]any{"error": fmt.Sprintf("table %s not found", tableName)})
	}
	cols := make([]map[string]any, 0, len(t.Cols))
	for _, c := range t.Cols {
		cols = append(cols, map[string]any{"name": c.Name, "type": c.Type.String(), "primary": c.Constraint == storage.PrimaryKey})
	}
	return jsonResponse(map[string]any{"table": tableName, "columns": cols, "rows": len(t.Rows)})
}

// jsCommit commits the current transaction
func jsCommit(this js.Value, args []js.Value) any {
	logInfo("Committing transaction...")

	if transactionDB == nil {
		return jsonResponse(APIResponse{Success: false, Error: "no active transaction"})
	}

	wasmStorageDB = transactionDB
	transactionDB = nil
	logInfo("Transaction committed successfully")
	return jsonResponse(APIResponse{Success: true, Message: "Transaction committed"})
}

// jsRollback rolls back the current transaction
func jsRollback(this js.Value, args []js.Value) any {
	logInfo("Rolling back transaction...")

	if transactionDB == nil {
		return jsonResponse(APIResponse{Success: false, Error: "no active transaction"})
	}

	transactionDB = nil
	logInfo("Transaction rolled back successfully")
	return jsonResponse(APIResponse{Success: true, Message: "Transaction rolled back"})
}

// jsExec executes a SQL statement
func jsExec(this js.Value, args []js.Value) any {
	if err := validateArgs(args, 1, js.TypeString); err != nil {
		return jsonResponse(APIResponse{Success: false, Error: err.Error()})
	}

	if currentStorageDB() == nil {
		return jsonResponse(APIResponse{Success: false, Error: "database not opened"})
	}

	sqlStr := args[0].String()
	// Preserve database/sql-style transaction commands used by the reference
	// UI while the browser bundle executes all regular SQL directly.
	switch strings.ToUpper(strings.TrimSpace(strings.TrimSuffix(sqlStr, ";"))) {
	case "BEGIN", "BEGIN TRANSACTION":
		return jsBegin(this, nil)
	case "COMMIT", "END":
		return jsCommit(this, nil)
	case "ROLLBACK":
		return jsRollback(this, nil)
	}
	logInfo(fmt.Sprintf("Executing SQL: %s", sqlStr))

	start := time.Now()
	result, err := executeWASMStatement(sqlStr)

	elapsed := time.Since(start)

	if err != nil {
		logError("SQL execution failed", err)
		return jsonResponse(APIResponse{Success: false, Error: err.Error()})
	}

	rowsAffected := resultRowsAffected(result)

	logInfo(fmt.Sprintf("SQL executed successfully in %v, rows affected: %d", elapsed, rowsAffected))

	return jsonResponse(APIResponse{
		Success: true,
		Message: fmt.Sprintf("Executed successfully. Rows affected: %d, Elapsed: %v", rowsAffected, elapsed),
	})
}

// jsQuery executes a SQL query and returns results
func jsQuery(this js.Value, args []js.Value) any {
	if err := validateArgs(args, 1, js.TypeString); err != nil {
		return jsonResponse(QueryResult{Error: err.Error()})
	}

	if currentStorageDB() == nil {
		return jsonResponse(QueryResult{Error: "database not opened"})
	}

	sqlStr := args[0].String()
	logInfo(fmt.Sprintf("Executing query: %s", sqlStr))

	start := time.Now()
	resultSet, err := executeWASMStatement(sqlStr)
	if err != nil {
		logError("Query execution failed", err)
		return jsonResponse(QueryResult{Error: err.Error()})
	}
	if resultSet == nil {
		resultSet = &tsql.ResultSet{}
	}

	// Prepare result structure
	result := QueryResult{
		Columns: resultSet.Cols,
		Rows:    make([][]any, 0, len(resultSet.Rows)),
	}

	for _, sourceRow := range resultSet.Rows {
		row := make([]any, len(resultSet.Cols))
		for i, column := range resultSet.Cols {
			value, _ := tsql.GetVal(sourceRow, column)
			row[i] = convertValue(value)
		}
		result.Rows = append(result.Rows, row)
	}

	result.Count = len(result.Rows)
	result.Elapsed = time.Since(start)

	logInfo(fmt.Sprintf("Query executed successfully in %v, returned %d rows", result.Elapsed, result.Count))

	return jsonResponse(result)
}

// jsClose closes the database connection
func jsClose(this js.Value, args []js.Value) any {
	logInfo("Closing database connection...")

	transactionDB = nil
	wasmStorageDB = nil
	wasmConnected = false

	logInfo("Database connection closed successfully")
	return jsonResponse(APIResponse{Success: true, Message: "Database closed"})
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

	return jsonResponse(status)
}

// Helper functions

// convertValue converts database values to JSON-compatible types
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
		return v.Format(time.RFC3339)
	default:
		return v
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

// jsonResponse marshals any value to JSON string
func jsonResponse(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		logError("Failed to marshal JSON response", err)
		return `{"error":"internal JSON marshaling error"}`
	}
	return string(b)
}

// registerAPI registers all API functions with JavaScript
func registerAPI() {
	logInfo("Registering tinySQL API...")

	api := js.Global().Get("Object").New()

	// helper to create and retain a js.Func
	retain := func(fn func(this js.Value, args []js.Value) any) js.Func {
		f := js.FuncOf(fn)
		retainedFuncs = append(retainedFuncs, f)
		return f
	}

	// Core database operations
	api.Set("open", retain(jsOpen))
	api.Set("close", retain(jsClose))
	api.Set("status", retain(jsStatus))

	// Transaction operations
	api.Set("begin", retain(jsBegin))
	api.Set("commit", retain(jsCommit))
	api.Set("rollback", retain(jsRollback))

	// SQL operations
	api.Set("exec", retain(jsExec))
	api.Set("query", retain(jsQuery))
	api.Set("exportDB", retain(jsExportDB))
	api.Set("importDB", retain(jsImportDB))

	// Explain / schema helpers
	api.Set("explain", retain(jsExplain))
	api.Set("listTables", retain(jsListTables))
	api.Set("describeTable", retain(jsDescribeTable))

	// Register the API globally
	js.Global().Set("tinySQL", api)

	logInfo("tinySQL API registered successfully")

	// Emit a ready event (only in browser environment)
	if js.Global().Get("document").Truthy() {
		// Build detail object in JS to avoid ValueOf panics on Go maps/slices
		detail := js.Global().Get("Object").New()
		detail.Set("version", "1.0.0")
		apiArr := js.Global().Get("Array").New()
		for _, m := range []string{"open", "close", "status", "begin", "commit", "rollback", "exec", "query", "exportDB", "importDB", "explain", "listTables", "describeTable"} {
			apiArr.Call("push", m)
		}
		detail.Set("api", apiArr)

		ce := js.Global().Get("CustomEvent")
		if ce.Truthy() {
			// Modern browsers expect an options object with a 'detail' property
			opts := js.Global().Get("Object").New()
			opts.Set("detail", detail)
			evt := ce.New("tinySQLReady", opts)
			js.Global().Get("document").Call("dispatchEvent", evt)
		} else {
			// Fallback for older browsers
			evt := js.Global().Get("document").Call("createEvent", "CustomEvent")
			evt.Call("initCustomEvent", "tinySQLReady", false, false, detail)
			js.Global().Get("document").Call("dispatchEvent", evt)
		}
	}
}

func main() {
	defer func() {
		if r := recover(); r != nil {
			logError("panic in WASM main", fmt.Errorf("%v", r))
		}
	}()

	logInfo("tinySQL WASM module starting...")

	// Register the API
	registerAPI()

	logInfo("tinySQL WASM module ready")

	// Keep the program alive
	select {}
}
