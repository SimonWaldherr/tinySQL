//go:build js && wasm

package main

import (
	"context"
	"encoding"
	"encoding/base64"
	"fmt"
	"strings"
	"syscall/js"
	"time"

	tsql "github.com/SimonWaldherr/tinySQL"
	"github.com/SimonWaldherr/tinySQL/internal/engine"
	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

const wasmQueryCacheSize = 256

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

// planStepsToJS converts a []PlanStep to []any of map[string]any for js.ValueOf.
func planStepsToJS(steps []PlanStep) []any {
	out := make([]any, len(steps))
	for i, s := range steps {
		out[i] = map[string]any{
			"operation": s.Operation,
			"object":    s.Object,
			"cost":      s.Cost,
			"details":   s.Details,
		}
	}
	return out
}

// Logger for WASM environment. The debug-flag check runs before format is
// ever evaluated, so a disabled logger (the default) costs one JS boundary
// crossing instead of also paying for fmt's reflection-driven formatting on
// every jsExec/jsQuery call -- the hot path for a SQL-in-the-browser API.
func logInfo(format string, args ...any) {
	if !js.Global().Get("tinySQLWasmDebug").Truthy() {
		return
	}
	msg := format
	if len(args) > 0 {
		msg = fmt.Sprintf(format, args...)
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
		logInfo("Using provided DSN: %s", dsn)
	} else {
		logInfo("Using default DSN: %s", dsn)
	}

	if err := bindStorageDB(storage.NewDB(), dsn); err != nil {
		logError("Failed to open database", err)
		return apiResult(false, err.Error(), "")
	}

	logInfo("Database connection established successfully")
	return apiResult(true, "", "Database opened successfully")
}

// jsExportDB serializes the current in-memory database as a base64 GOB snapshot.
func jsExportDB(this js.Value, args []js.Value) any {
	source := currentStorageDB()
	if source == nil {
		return apiResult(false, "database not opened", "")
	}
	data, err := storage.SaveToBytes(source)
	if err != nil {
		logError("Failed to export database", err)
		return apiResult(false, err.Error(), "")
	}
	return map[string]any{
		"success":    true,
		"message":    "Database exported successfully",
		"data":       base64.StdEncoding.EncodeToString(data),
		"size_bytes": len(data),
	}
}

// jsImportDB replaces the current in-memory database with a base64 GOB snapshot.
func jsImportDB(this js.Value, args []js.Value) any {
	if err := validateArgs(args, 1, js.TypeString); err != nil {
		return apiResult(false, err.Error(), "")
	}

	encoded := strings.TrimSpace(args[0].String())
	if encoded == "" {
		return apiResult(false, "snapshot must not be empty", "")
	}

	data, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return apiResult(false, fmt.Sprintf("invalid base64 snapshot: %v", err), "")
	}
	loaded, err := storage.LoadFromBytes(data)
	if err != nil {
		logError("Failed to import database", err)
		return apiResult(false, err.Error(), "")
	}
	if err = bindStorageDB(loaded, "mem://?tenant=default"); err != nil {
		logError("Failed to bind imported database", err)
		return apiResult(false, err.Error(), "")
	}

	logInfo("Database snapshot imported successfully")
	return map[string]any{
		"success":    true,
		"message":    "Database imported successfully",
		"size_bytes": len(data),
	}
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

	// DeepClone keeps the browser API transactional without linking the
	// database/sql driver -- the same in-memory MVCC-light clone the SQL
	// driver's own BeginTx uses (internal/driver/conn.go), instead of the
	// previous round trip through storage.SaveToBytes/LoadFromBytes. That
	// GOB round trip paid for reflection-based serialization twice per BEGIN
	// and, worse, silently dropped runtime state DeepClone preserves (WAL,
	// audit log, MVCC coordinator, scheduler, storage backend, config,
	// extensions): LoadFromBytes only ever reconstructs tables and the
	// catalog. LockContentForRead/UnlockContentForRead mirrors BeginTx's own
	// guard against a concurrent mutation of the live rows DeepClone reads.
	wasmStorageDB.LockContentForRead()
	transactionDB = wasmStorageDB.DeepClone()
	wasmStorageDB.UnlockContentForRead()

	logInfo("Transaction started successfully")
	return apiResult(true, "", "Transaction started")
}

// PlanStep is one step of the simplified EXPLAIN plan built by jsExplain.
type PlanStep struct {
	Operation string `json:"operation"`
	Object    string `json:"object"`
	Cost      string `json:"cost"`
	Details   string `json:"details"`
}

// jsExplain returns a simple query plan for a given SQL string.
func jsExplain(this js.Value, args []js.Value) any {
	if len(args) < 1 || args[0].Type() != js.TypeString {
		return apiResult(false, "sql string required", "")
	}
	sqlStr := args[0].String()

	stmt, err := tsql.ParseSQL(sqlStr)
	if err != nil {
		return map[string]any{"error": err.Error()}
	}

	// Build a simple plan representation
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

	return map[string]any{"plan": planStepsToJS(plan)}
}

// jsListTables returns all table names in the current storage DB tenant.
func jsListTables(this js.Value, args []js.Value) any {
	source := currentStorageDB()
	if source == nil {
		return map[string]any{"error": "database not initialized"}
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
	return map[string]any{"tables": stringsToAny(names)}
}

// jsDescribeTable returns column information for a given table.
func jsDescribeTable(this js.Value, args []js.Value) any {
	source := currentStorageDB()
	if source == nil {
		return map[string]any{"error": "database not initialized"}
	}
	if len(args) < 1 || args[0].Type() != js.TypeString {
		return map[string]any{"error": "table name required"}
	}
	tenant := wasmTenant
	tableName := args[0].String()
	if len(args) > 1 && args[1].Type() == js.TypeString {
		tenant = args[1].String()
	}
	t, err := source.Get(tenant, tableName)
	if err != nil || t == nil {
		return map[string]any{"error": fmt.Sprintf("table %s not found", tableName)}
	}
	cols := make([]any, 0, len(t.Cols))
	for _, c := range t.Cols {
		cols = append(cols, map[string]any{"name": c.Name, "type": c.Type.String(), "primary": c.Constraint == storage.PrimaryKey})
	}
	return map[string]any{"table": tableName, "columns": cols, "rows": len(t.Rows)}
}

// jsCommit commits the current transaction
func jsCommit(this js.Value, args []js.Value) any {
	logInfo("Committing transaction...")

	if transactionDB == nil {
		return apiResult(false, "no active transaction", "")
	}

	transactionDB.PromoteShadow()
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
	logInfo("Executing SQL: %s", sqlStr)

	start := time.Now()
	result, err := executeWASMStatement(sqlStr)

	elapsed := time.Since(start)

	if err != nil {
		logError("SQL execution failed", err)
		return apiResult(false, err.Error(), "")
	}

	rowsAffected := resultRowsAffected(result)

	logInfo("SQL executed successfully in %v, rows affected: %d", elapsed, rowsAffected)

	return apiResult(true, "", fmt.Sprintf("Executed successfully. Rows affected: %d, Elapsed: %v", rowsAffected, elapsed))
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
	logInfo("Executing query: %s", sqlStr)

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

	logInfo("Query executed successfully in %v, returned %d rows", elapsed, count)

	return queryResultMap(resultSet.Cols, rows, "", count, int64(elapsed))
}

// jsClose closes the database connection
func jsClose(this js.Value, args []js.Value) any {
	logInfo("Closing database connection...")

	transactionDB = nil
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
// time.Time keep their bespoke formatting, and any other value that isn't
// already one of ValueOf's native scalar kinds gets the same text
// json.Marshal would have produced via encoding.TextMarshaler (e.g. *big.Rat
// for DECIMAL/MONEY, uuid.UUID) or, failing that, a fmt.Sprintf fallback.
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
