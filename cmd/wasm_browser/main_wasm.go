//go:build js && wasm

package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"syscall/js"
	"time"

	drv "github.com/SimonWaldherr/tinySQL/internal/driver"
	"github.com/SimonWaldherr/tinySQL/internal/storage"
	"github.com/SimonWaldherr/tinySQL/internal/engine"
	tsql "github.com/SimonWaldherr/tinySQL"
)

// Global state
var (
	db  *sql.DB
	tx  *sql.Tx
	ctx = context.Background()
	// keep JS function references alive to avoid GC and subsequent panics
	retainedFuncs []js.Func
	// Keep a reference to the underlying storage DB when running in WASM
	wasmStorageDB *storage.DB
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
	log.Printf("[tinySQL-WASM] %s", msg)
	js.Global().Get("console").Call("log", fmt.Sprintf("[tinySQL-WASM] %s", msg))
}

func logError(msg string, err error) {
	errMsg := fmt.Sprintf("[tinySQL-WASM] ERROR: %s", msg)
	if err != nil {
		errMsg += fmt.Sprintf(" - %v", err)
	}
	log.Print(errMsg)
	js.Global().Get("console").Call("error", errMsg)
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

	// Close existing connection if any
	if db != nil {
		logInfo("Closing existing database connection...")
		if err := db.Close(); err != nil {
			logError("Failed to close existing connection", err)
		}
		db = nil
		tx = nil
	}

	// Create an underlying storage DB and set it as default for the driver
	wasmStorageDB = storage.NewDB()
	drv.SetDefaultDB(wasmStorageDB)

	// Open new connection
	var err error
	db, err = sql.Open("tinysql", dsn)
	if err != nil {
		logError("Failed to open database", err)
		return jsonResponse(APIResponse{Success: false, Error: err.Error()})
	}

	// Test the connection
	if err = db.PingContext(ctx); err != nil {
		logError("Database ping failed", err)
		return jsonResponse(APIResponse{Success: false, Error: fmt.Sprintf("connection test failed: %v", err)})
	}

	logInfo("Database connection established successfully")
	return jsonResponse(APIResponse{Success: true, Message: "Database opened successfully"})
}

// jsBegin starts a new transaction
func jsBegin(this js.Value, args []js.Value) any {
	logInfo("Starting transaction...")

	if db == nil {
		return jsonResponse(APIResponse{Success: false, Error: "database not opened"})
	}

	if tx != nil {
		return jsonResponse(APIResponse{Success: false, Error: "transaction already active"})
	}

	var err error
	tx, err = db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelDefault,
		ReadOnly:  false,
	})
	if err != nil {
		logError("Failed to begin transaction", err)
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
	if wasmStorageDB == nil {
		return jsonResponse(map[string]any{"error": "database not initialized"})
	}
	tenant := "default"
	if len(args) > 0 && args[0].Type() == js.TypeString {
		tenant = args[0].String()
	}
	tables := wasmStorageDB.ListTables(tenant)
	names := make([]string, 0, len(tables))
	for _, t := range tables {
		names = append(names, t.Name)
	}
	return jsonResponse(map[string]any{"tables": names})
}

// jsDescribeTable returns column information for a given table.
func jsDescribeTable(this js.Value, args []js.Value) any {
	if wasmStorageDB == nil {
		return jsonResponse(map[string]any{"error": "database not initialized"})
	}
	if len(args) < 1 || args[0].Type() != js.TypeString {
		return jsonResponse(map[string]any{"error": "table name required"})
	}
	tenant := "default"
	tableName := args[0].String()
	if len(args) > 1 && args[1].Type() == js.TypeString {
		tenant = args[1].String()
	}
	t, err := wasmStorageDB.Get(tenant, tableName)
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

	if tx == nil {
		return jsonResponse(APIResponse{Success: false, Error: "no active transaction"})
	}

	if err := tx.Commit(); err != nil {
		logError("Failed to commit transaction", err)
		return jsonResponse(APIResponse{Success: false, Error: err.Error()})
	}

	tx = nil
	logInfo("Transaction committed successfully")
	return jsonResponse(APIResponse{Success: true, Message: "Transaction committed"})
}

// jsRollback rolls back the current transaction
func jsRollback(this js.Value, args []js.Value) any {
	logInfo("Rolling back transaction...")

	if tx == nil {
		return jsonResponse(APIResponse{Success: false, Error: "no active transaction"})
	}

	if err := tx.Rollback(); err != nil {
		logError("Failed to rollback transaction", err)
		return jsonResponse(APIResponse{Success: false, Error: err.Error()})
	}

	tx = nil
	logInfo("Transaction rolled back successfully")
	return jsonResponse(APIResponse{Success: true, Message: "Transaction rolled back"})
}

// jsExec executes a SQL statement
func jsExec(this js.Value, args []js.Value) any {
	if err := validateArgs(args, 1, js.TypeString); err != nil {
		return jsonResponse(APIResponse{Success: false, Error: err.Error()})
	}

	if db == nil {
		return jsonResponse(APIResponse{Success: false, Error: "database not opened"})
	}

	sqlStr := args[0].String()
	logInfo(fmt.Sprintf("Executing SQL: %s", sqlStr))

	start := time.Now()
	var result sql.Result
	var err error

	if tx != nil {
		result, err = tx.ExecContext(ctx, sqlStr)
	} else {
		result, err = db.ExecContext(ctx, sqlStr)
	}

	elapsed := time.Since(start)

	if err != nil {
		logError("SQL execution failed", err)
		return jsonResponse(APIResponse{Success: false, Error: err.Error()})
	}

	rowsAffected, _ := result.RowsAffected()
	lastInsertId, _ := result.LastInsertId()

	logInfo(fmt.Sprintf("SQL executed successfully in %v, rows affected: %d", elapsed, rowsAffected))

	return jsonResponse(APIResponse{
		Success: true,
		Message: fmt.Sprintf("Executed successfully. Rows affected: %d, Last insert ID: %d, Elapsed: %v",
			rowsAffected, lastInsertId, elapsed),
	})
}

// jsQuery executes a SQL query and returns results
func jsQuery(this js.Value, args []js.Value) any {
	if err := validateArgs(args, 1, js.TypeString); err != nil {
		return jsonResponse(QueryResult{Error: err.Error()})
	}

	if db == nil {
		return jsonResponse(QueryResult{Error: "database not opened"})
	}

	sqlStr := args[0].String()
	logInfo(fmt.Sprintf("Executing query: %s", sqlStr))

	start := time.Now()
	var rows *sql.Rows
	var err error

	if tx != nil {
		rows, err = tx.QueryContext(ctx, sqlStr)
	} else {
		rows, err = db.QueryContext(ctx, sqlStr)
	}

	if err != nil {
		logError("Query execution failed", err)
		return jsonResponse(QueryResult{Error: err.Error()})
	}
	defer rows.Close()

	// Get column information
	columns, err := rows.Columns()
	if err != nil {
		logError("Failed to get columns", err)
		return jsonResponse(QueryResult{Error: err.Error()})
	}

	// Prepare result structure
	result := QueryResult{
		Columns: columns,
		Rows:    make([][]any, 0),
	}

	// Scan all rows
	for rows.Next() {
		values := make([]any, len(columns))
		valuePtrs := make([]any, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			logError("Failed to scan row", err)
			return jsonResponse(QueryResult{Error: err.Error()})
		}

		// Convert values to proper types
		row := make([]any, len(columns))
		for i, val := range values {
			row[i] = convertValue(val)
		}

		result.Rows = append(result.Rows, row)
	}

	if err = rows.Err(); err != nil {
		logError("Row iteration error", err)
		return jsonResponse(QueryResult{Error: err.Error()})
	}

	result.Count = len(result.Rows)
	result.Elapsed = time.Since(start)

	logInfo(fmt.Sprintf("Query executed successfully in %v, returned %d rows", result.Elapsed, result.Count))

	return jsonResponse(result)
}

// jsClose closes the database connection
func jsClose(this js.Value, args []js.Value) any {
	logInfo("Closing database connection...")

	if tx != nil {
		logInfo("Rolling back active transaction...")
		if err := tx.Rollback(); err != nil {
			logError("Failed to rollback transaction during close", err)
		}
		tx = nil
	}

	if db != nil {
		if err := db.Close(); err != nil {
			logError("Failed to close database", err)
			return jsonResponse(APIResponse{Success: false, Error: err.Error()})
		}
		db = nil
	}

	logInfo("Database connection closed successfully")
	return jsonResponse(APIResponse{Success: true, Message: "Database closed"})
}

// jsStatus returns the current status of the database
func jsStatus(this js.Value, args []js.Value) any {
	status := map[string]any{
		"connected":          db != nil,
		"transaction_active": tx != nil,
		"driver":             "tinysql",
		"version":            "1.0.0",
		"build_time":         time.Now().Format(time.RFC3339),
	}

	if db != nil {
		stats := db.Stats()
		status["connection_stats"] = map[string]any{
			"open_connections":    stats.OpenConnections,
			"in_use":              stats.InUse,
			"idle":                stats.Idle,
			"wait_count":          stats.WaitCount,
			"wait_duration":       stats.WaitDuration.String(),
			"max_idle_closed":     stats.MaxIdleClosed,
			"max_lifetime_closed": stats.MaxLifetimeClosed,
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
		for _, m := range []string{"open", "close", "status", "begin", "commit", "rollback", "exec", "query", "explain", "listTables", "describeTable"} {
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
