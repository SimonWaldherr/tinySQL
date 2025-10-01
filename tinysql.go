// Package tinysql provides a lightweight, embeddable SQL database for Go applications.
//
// TinySQL is an educational SQL database that demonstrates core database concepts including:
//   - SQL parsing and execution (DDL, DML, SELECT with joins, aggregates, CTEs)
//   - Multi-Version Concurrency Control (MVCC) with snapshot isolation
//   - Write-Ahead Logging (WAL) for durability and crash recovery
//   - Multi-tenancy support for isolated data namespaces
//   - In-memory and persistent storage with GOB serialization
//
// # Basic Usage
//
// Create a database, execute SQL, and query results:
//
//	db := tinysql.NewDB()
//	ctx := context.Background()
//
//	// Parse and execute DDL
//	stmt, _ := tinysql.ParseSQL("CREATE TABLE users (id INT, name TEXT)")
//	tinysql.Execute(ctx, db, "default", stmt)
//
//	// Insert data
//	stmt, _ = tinysql.ParseSQL("INSERT INTO users VALUES (1, 'Alice')")
//	tinysql.Execute(ctx, db, "default", stmt)
//
//	// Query data
//	stmt, _ = tinysql.ParseSQL("SELECT * FROM users WHERE id = 1")
//	rs, _ := tinysql.Execute(ctx, db, "default", stmt)
//	for _, row := range rs.Rows {
//	    fmt.Println(row)
//	}
//
// # Persistence
//
// Save and load database snapshots:
//
//	// Save to file
//	tinysql.SaveToFile(db, "mydb.gob")
//
//	// Load from file
//	db, err := tinysql.LoadFromFile("mydb.gob")
//
// # Advanced Features
//
// Enable MVCC for concurrent transactions:
//
//	mvcc := db.MVCC()
//	tx, _ := mvcc.BeginTx(tinysql.SnapshotIsolation)
//	// ... perform transaction operations ...
//	mvcc.CommitTx(tx.ID)
//
// Enable WAL for durability:
//
//	wal, _ := tinysql.NewAdvancedWAL("data/wal.log")
//	db.AttachAdvancedWAL(wal)
//
// # Query Compilation
//
// Pre-compile queries for better performance:
//
//	cache := tinysql.NewQueryCache(100)
//	query, _ := cache.Compile("SELECT * FROM users WHERE id = ?")
//	rs, _ := query.Execute(ctx, db, "default")
//
// For more examples, see the example_test.go file in the repository.
package tinysql

import (
	"context"
	"strings"

	"github.com/SimonWaldherr/tinySQL/internal/engine"
	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// ============================================================================
// Core Types - Re-exported from internal packages for public API
// ============================================================================

// DB represents a multi-tenant database instance with support for MVCC and WAL.
// Use NewDB to create a new instance.
type DB = storage.DB

// Table represents a database table with columns and rows.
// Tables are created via CREATE TABLE statements and accessed through the DB.
type Table = storage.Table

// Column represents a table column with a name and type.
type Column = storage.Column

// ColType enumerates supported column data types (INT, TEXT, BOOL, JSON, etc.).
type ColType = storage.ColType

// Row represents a single result row mapped by column name (case-insensitive).
// Keys include both qualified (table.column) and unqualified (column) names.
type Row = engine.Row

// ResultSet holds query results with column names and data rows.
// Returned by SELECT queries and available for inspection.
type ResultSet = engine.ResultSet

// Statement is the base interface for all parsed SQL statements.
// Use Parser.ParseStatement() to obtain a Statement from SQL text.
type Statement = engine.Statement

// Parser parses SQL text into executable Statement objects.
// Create with NewParser and call ParseStatement() to parse.
type Parser = engine.Parser

// QueryCache stores compiled queries for reuse, similar to prepared statements.
// Create with NewQueryCache and use Compile() to cache queries.
type QueryCache = engine.QueryCache

// CompiledQuery represents a pre-parsed SQL statement that can be executed
// multiple times efficiently.
type CompiledQuery = engine.CompiledQuery

// ============================================================================
// MVCC Types - Transaction management and isolation
// ============================================================================

// MVCCManager coordinates multi-version concurrency control with snapshot isolation.
// Provides transaction begin/commit/abort operations and visibility checking.
type MVCCManager = storage.MVCCManager

// TxID represents a unique transaction identifier.
type TxID = storage.TxID

// TxContext holds the state of an active transaction including read/write sets
// and isolation level.
type TxContext = storage.TxContext

// TxStatus represents the current state of a transaction.
type TxStatus = storage.TxStatus

// Timestamp represents a logical timestamp for MVCC visibility checks.
type Timestamp = storage.Timestamp

// IsolationLevel defines transaction isolation semantics.
type IsolationLevel = storage.IsolationLevel

// Transaction status constants
const (
	TxStatusInProgress = storage.TxStatusInProgress // Transaction is active
	TxStatusCommitted  = storage.TxStatusCommitted  // Transaction committed successfully
	TxStatusAborted    = storage.TxStatusAborted    // Transaction was aborted
)

// Isolation level constants
const (
	ReadCommitted     IsolationLevel = storage.ReadCommitted     // Read only committed data
	RepeatableRead    IsolationLevel = storage.RepeatableRead    // Repeatable reads within transaction
	SnapshotIsolation IsolationLevel = storage.SnapshotIsolation // Full snapshot isolation
	Serializable      IsolationLevel = storage.Serializable      // Serializable transactions with conflict detection
)

// ============================================================================
// WAL Types - Write-Ahead Logging for durability
// ============================================================================

// AdvancedWAL manages row-level write-ahead logging with REDO/UNDO support.
// Provides durability, crash recovery, and point-in-time recovery.
type AdvancedWAL = storage.AdvancedWAL

// AdvancedWALConfig configures the advanced WAL behavior.
type AdvancedWALConfig = storage.AdvancedWALConfig

// LSN (Log Sequence Number) provides total ordering of log records.
type LSN = storage.LSN

// WALRecord represents a single log entry with before/after images.
type WALRecord = storage.WALRecord

// WALOperationType defines the type of WAL operation.
type WALOperationType = storage.WALOperationType

// WAL operation type constants
const (
	WALOpBegin      = storage.WALOpBegin      // Transaction begin
	WALOpInsert     = storage.WALOpInsert     // Row insert
	WALOpUpdate     = storage.WALOpUpdate     // Row update
	WALOpDelete     = storage.WALOpDelete     // Row delete
	WALOpCommit     = storage.WALOpCommit     // Transaction commit
	WALOpAbort      = storage.WALOpAbort      // Transaction abort
	WALOpCheckpoint = storage.WALOpCheckpoint // Checkpoint operation
)

// ============================================================================
// Column Type Constants - Supported data types
// ============================================================================

const (
	// Integer types
	IntType   ColType = storage.IntType
	Int8Type  ColType = storage.Int8Type
	Int16Type ColType = storage.Int16Type
	Int32Type ColType = storage.Int32Type
	Int64Type ColType = storage.Int64Type
	UintType  ColType = storage.UintType
	Uint8Type ColType = storage.Uint8Type

	// Floating point types
	Float32Type ColType = storage.Float32Type
	Float64Type ColType = storage.Float64Type
	FloatType   ColType = storage.FloatType

	// String types
	StringType ColType = storage.StringType
	TextType   ColType = storage.TextType

	// Boolean type
	BoolType ColType = storage.BoolType

	// Time types
	TimeType      ColType = storage.TimeType
	DateType      ColType = storage.DateType
	DateTimeType  ColType = storage.DateTimeType
	TimestampType ColType = storage.TimestampType

	// Complex types
	JsonType  ColType = storage.JsonType
	JsonbType ColType = storage.JsonbType
)

// ============================================================================
// Database Creation and Management
// ============================================================================

// NewDB creates a new in-memory multi-tenant database instance with MVCC support.
//
// The database starts empty with no tables. Use SQL DDL statements to create tables,
// or load from a file with LoadFromFile.
//
// Example:
//
//	db := tinysql.NewDB()
//	defer db.Close() // Optional cleanup
//
// The returned DB is safe for concurrent use and includes an integrated MVCC manager.
func NewDB() *DB {
	return storage.NewDB()
}

// ============================================================================
// SQL Parsing
// ============================================================================

// NewParser creates a new SQL parser for the provided input string.
//
// The parser supports a practical subset of SQL including:
//   - DDL: CREATE TABLE, DROP TABLE
//   - DML: INSERT, UPDATE, DELETE
//   - Queries: SELECT with WHERE, JOIN, GROUP BY, HAVING, ORDER BY, LIMIT, OFFSET
//   - Set operations: UNION, INTERSECT, EXCEPT
//   - CTEs: WITH clauses
//   - Expressions: arithmetic, comparisons, functions, aggregates
//
// Example:
//
//	parser := tinysql.NewParser("SELECT * FROM users WHERE active = true")
//	stmt, err := parser.ParseStatement()
//	if err != nil {
//	    log.Fatal(err)
//	}
//
// For better performance with repeated queries, consider using QueryCache.
func NewParser(sql string) *Parser {
	return engine.NewParser(sql)
}

// ParseSQL is a convenience function that creates a parser and parses a SQL statement
// in one call. Equivalent to NewParser(sql).ParseStatement().
//
// Example:
//
//	stmt, err := tinysql.ParseSQL("SELECT id, name FROM users")
//	if err != nil {
//	    log.Fatal(err)
//	}
//
// Returns the parsed Statement or an error if parsing fails.
func ParseSQL(sql string) (Statement, error) {
	return NewParser(sql).ParseStatement()
}

// MustParseSQL is like ParseSQL but panics if parsing fails.
// Useful for static SQL in tests or initialization code.
//
// Example:
//
//	stmt := tinysql.MustParseSQL("CREATE TABLE users (id INT, name TEXT)")
func MustParseSQL(sql string) Statement {
	stmt, err := ParseSQL(sql)
	if err != nil {
		panic(err)
	}
	return stmt
}

// ============================================================================
// Query Compilation and Caching
// ============================================================================

// NewQueryCache creates a new query cache for compiling and reusing parsed queries.
//
// Query compilation parses SQL once and reuses the AST for multiple executions,
// similar to prepared statements. This improves performance for repeated queries.
//
// The maxSize parameter limits the number of cached queries (LRU eviction).
// Use 0 for unlimited cache size (not recommended for production).
//
// Example:
//
//	cache := tinysql.NewQueryCache(100)
//	query, _ := cache.Compile("SELECT * FROM users WHERE id = ?")
//
//	// Execute multiple times
//	for _, id := range userIDs {
//	    rs, _ := query.Execute(ctx, db, "default")
//	    // process results...
//	}
func NewQueryCache(maxSize int) *QueryCache {
	return engine.NewQueryCache(maxSize)
}

// Compile parses and caches a SQL query for reuse, similar to regexp.Compile.
//
// The compiled query can be executed multiple times without re-parsing.
// This is useful for queries executed repeatedly with different parameters.
//
// Example:
//
//	cache := tinysql.NewQueryCache(100)
//	query, err := tinysql.Compile(cache, "SELECT * FROM users WHERE active = true")
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	// Execute the compiled query
//	rs, _ := query.Execute(ctx, db, "default")
//
// Returns a CompiledQuery that can be executed via ExecuteCompiled or query.Execute.
func Compile(cache *QueryCache, sql string) (*CompiledQuery, error) {
	return cache.Compile(sql)
}

// MustCompile is like Compile but panics on error, similar to regexp.MustCompile.
//
// Useful for static queries in tests or initialization where errors are unexpected.
//
// Example:
//
//	cache := tinysql.NewQueryCache(100)
//	query := tinysql.MustCompile(cache, "SELECT * FROM users")
func MustCompile(cache *QueryCache, sql string) *CompiledQuery {
	return cache.MustCompile(sql)
}

// ============================================================================
// SQL Execution
// ============================================================================

// Execute executes a parsed SQL statement against the database.
//
// The context allows for cancellation and timeout control. The tenant parameter
// provides data isolation - each tenant has its own namespace of tables.
//
// For DDL statements (CREATE TABLE, DROP TABLE), returns nil ResultSet.
// For DML statements (INSERT, UPDATE, DELETE), returns nil ResultSet.
// For SELECT queries, returns ResultSet with columns and rows.
//
// Example:
//
//	ctx := context.Background()
//	stmt, _ := tinysql.ParseSQL("SELECT * FROM users WHERE age > 18")
//	rs, err := tinysql.Execute(ctx, db, "default", stmt)
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	// Process results
//	for _, row := range rs.Rows {
//	    name, _ := tinysql.GetVal(row, "name")
//	    fmt.Println(name)
//	}
//
// The tenant parameter is required. Use "default" for single-tenant applications.
func Execute(ctx context.Context, db *DB, tenant string, stmt Statement) (*ResultSet, error) {
	return engine.Execute(ctx, db, tenant, stmt)
}

// ExecuteCompiled executes a pre-compiled query against the database.
//
// This is more efficient than Execute for queries executed repeatedly,
// as parsing is done only once during compilation.
//
// Example:
//
//	cache := tinysql.NewQueryCache(100)
//	query, _ := cache.Compile("SELECT * FROM users")
//	rs, err := tinysql.ExecuteCompiled(ctx, db, "default", query)
//
// Returns ResultSet for SELECT queries, nil for DDL/DML statements.
func ExecuteCompiled(ctx context.Context, db *DB, tenant string, compiled *CompiledQuery) (*ResultSet, error) {
	return compiled.Execute(ctx, db, tenant)
}

// ============================================================================
// Result Access Helpers
// ============================================================================

// GetVal retrieves a value from a result row by column name (case-insensitive).
//
// Returns the value and true if the column exists, or nil and false otherwise.
// This is the recommended way to access row data as it handles case-insensitivity.
//
// Example:
//
//	for _, row := range rs.Rows {
//	    id, ok := tinysql.GetVal(row, "id")
//	    if ok {
//	        fmt.Printf("ID: %v\n", id)
//	    }
//
//	    name, _ := tinysql.GetVal(row, "Name") // Case-insensitive
//	    fmt.Printf("Name: %v\n", name)
//	}
//
// For type-safe access, use type assertion after retrieving the value.
func GetVal(row Row, name string) (any, bool) {
	v, ok := row[strings.ToLower(name)]
	return v, ok
}

// ============================================================================
// Persistence - GOB Serialization
// ============================================================================

// SaveToFile serializes the entire database to a GOB file for persistence.
//
// This creates a snapshot of all tables, rows, and metadata for all tenants.
// The file can be loaded later with LoadFromFile to restore the database state.
//
// Example:
//
//	err := tinysql.SaveToFile(db, "mydb.gob")
//	if err != nil {
//	    log.Fatal(err)
//	}
//
// Note: This saves the current state only. For durability during crashes,
// use AttachAdvancedWAL to enable write-ahead logging.
func SaveToFile(db *DB, filename string) error {
	return storage.SaveToFile(db, filename)
}

// LoadFromFile deserializes a database from a GOB file created by SaveToFile.
//
// This restores all tables, rows, and metadata from the file. The returned
// database instance is ready for use immediately.
//
// Example:
//
//	db, err := tinysql.LoadFromFile("mydb.gob")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer db.Close()
//
// Returns a new DB instance or an error if the file cannot be read.
func LoadFromFile(filename string) (*DB, error) {
	return storage.LoadFromFile(filename)
}

// ============================================================================
// Advanced WAL - Write-Ahead Logging
// ============================================================================

// NewAdvancedWAL creates a new write-ahead log manager with default configuration.
//
// The WAL logs all database modifications (INSERT, UPDATE, DELETE) to disk before
// applying them. This enables:
//   - Crash recovery: replay committed transactions after restart
//   - Point-in-time recovery: restore to any previous state
//   - Durability: changes survive system crashes
//
// Example:
//
//	wal, err := tinysql.NewAdvancedWAL("data/wal.log")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer wal.Close()
//
//	db.AttachAdvancedWAL(wal)
//
//	// Now all database modifications are logged
//	stmt, _ := tinysql.ParseSQL("INSERT INTO users VALUES (1, 'Alice')")
//	tinysql.Execute(ctx, db, "default", stmt)
//
// The path parameter specifies the WAL file location.
func NewAdvancedWAL(path string) (*AdvancedWAL, error) {
	return storage.OpenAdvancedWAL(storage.AdvancedWALConfig{
		Path:               path,
		CheckpointPath:     path + ".checkpoint",
		CheckpointEvery:    1000,
		CheckpointInterval: 300000000000, // 5 minutes in nanoseconds
		Compress:           false,
		BufferSize:         65536, // 64KB
	})
}

// OpenAdvancedWAL creates or opens a WAL with custom configuration.
//
// This provides full control over WAL behavior including checkpoint intervals,
// compression, and buffer sizes.
//
// Example:
//
//	wal, err := tinysql.OpenAdvancedWAL(tinysql.AdvancedWALConfig{
//	    Path:               "data/wal.log",
//	    CheckpointPath:     "data/checkpoint",
//	    CheckpointEvery:    5000,
//	    CheckpointInterval: 10 * time.Minute,
//	    Compress:           true,
//	    BufferSize:         128 * 1024,
//	})
//
// Returns the WAL instance or an error if initialization fails.
func OpenAdvancedWAL(config AdvancedWALConfig) (*AdvancedWAL, error) {
	return storage.OpenAdvancedWAL(config)
}

// ============================================================================
// Table Operations - Direct table access
// ============================================================================

// NewTable creates a new table with the specified columns.
//
// This is a low-level API. Normally tables are created via CREATE TABLE statements.
// Use this when programmatically building table structures.
//
// Example:
//
//	cols := []tinysql.Column{
//	    {Name: "id", Type: tinysql.IntType},
//	    {Name: "name", Type: tinysql.TextType},
//	}
//	table := tinysql.NewTable("users", cols, false)
//	db.Put("default", table)
//
// The isTemp parameter indicates if this is a temporary table (not persisted).
func NewTable(name string, cols []Column, isTemp bool) *Table {
	return storage.NewTable(name, cols, isTemp)
}
