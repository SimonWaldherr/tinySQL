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
	"io"
	"strings"

	"github.com/SimonWaldherr/tinySQL/internal/engine"
	"github.com/SimonWaldherr/tinySQL/internal/importer"
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
// Storage Modes - Pluggable persistence strategies
// ============================================================================

// StorageMode defines how the database manages data between memory and disk.
// Different modes trade off speed, memory usage, and durability.
type StorageMode = storage.StorageMode

// StorageConfig configures database storage behaviour. Pass to OpenDB to
// create a database with the desired persistence strategy.
type StorageConfig = storage.StorageConfig

// StorageBackend is the interface implemented by all storage backends.
type StorageBackend = storage.StorageBackend

// BackendStats provides observability into storage backend behaviour.
type BackendStats = storage.BackendStats

// Storage mode constants.
const (
	// ModeMemory keeps all data in RAM. Persistence only occurs via explicit
	// SaveToFile calls or when DB.Close is invoked. Fastest mode.
	ModeMemory StorageMode = storage.ModeMemory

	// ModeWAL keeps all data in RAM and writes a Write-Ahead Log for crash
	// recovery. Periodic checkpoints create full GOB snapshots.
	ModeWAL StorageMode = storage.ModeWAL

	// ModeDisk stores each table as a separate GOB file on disk. Tables are
	// loaded into memory on demand and flushed back on Sync/Close.
	ModeDisk StorageMode = storage.ModeDisk

	// ModeIndex keeps table schemas in RAM while row data resides on disk.
	// Rows are loaded on demand with aggressive eviction. Memory usage is
	// proportional to schema size, not data size.
	ModeIndex StorageMode = storage.ModeIndex

	// ModeHybrid uses an LRU buffer pool with a configurable memory limit.
	// Hot tables stay in RAM; cold tables spill to disk.
	ModeHybrid StorageMode = storage.ModeHybrid
)

// ============================================================================
// Column Type Constants - Supported data types
// ============================================================================

const (
	// IntType represents a generic integer column type.
	IntType ColType = storage.IntType
	// Int8Type represents an 8-bit signed integer column type.
	Int8Type ColType = storage.Int8Type
	// Int16Type represents a 16-bit signed integer column type.
	Int16Type ColType = storage.Int16Type
	// Int32Type represents a 32-bit signed integer column type.
	Int32Type ColType = storage.Int32Type
	// Int64Type represents a 64-bit signed integer column type.
	Int64Type ColType = storage.Int64Type
	// UintType represents an unsigned integer column type.
	UintType ColType = storage.UintType
	// Uint8Type represents an 8-bit unsigned integer column type.
	Uint8Type ColType = storage.Uint8Type

	// Float32Type represents a 32-bit floating point column type.
	Float32Type ColType = storage.Float32Type
	// Float64Type represents a 64-bit floating point column type.
	Float64Type ColType = storage.Float64Type
	// FloatType is an alias for Float64Type.
	FloatType ColType = storage.FloatType

	// StringType represents a short string/text column.
	StringType ColType = storage.StringType
	// TextType represents a longer textual column.
	TextType ColType = storage.TextType

	// BoolType represents a boolean column (true/false).
	BoolType ColType = storage.BoolType

	// TimeType represents a time-only column type.
	TimeType ColType = storage.TimeType
	// DateType represents a date-only column type.
	DateType ColType = storage.DateType
	// DateTimeType represents a combined date and time column.
	DateTimeType ColType = storage.DateTimeType
	// TimestampType represents a timestamp column type.
	TimestampType ColType = storage.TimestampType

	// JsonType represents a JSON textual column.
	JsonType ColType = storage.JsonType
	// JsonbType represents a binary JSON column (if supported by backend).
	JsonbType ColType = storage.JsonbType
	// GeometryType represents spatial geometry values (GeoJSON/WKB).
	GeometryType ColType = storage.GeometryType
	// Decimal and money types
	DecimalType  ColType = storage.DecimalType
	MoneyType    ColType = storage.MoneyType
	UUIDType     ColType = storage.UUIDType
	BlobType     ColType = storage.BlobType
	XMLType      ColType = storage.XMLType
	IntervalType ColType = storage.IntervalType
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

// OpenDB creates or opens a database with the specified storage mode and
// configuration. This is the primary entry point for creating databases that
// persist data to disk using different strategies.
//
// Storage Mode Overview:
//
//	ModeMemory  – All data in RAM. Fast. Save on Close if Path is set.
//	ModeWAL     – All data in RAM + Write-Ahead Log for crash recovery.
//	ModeDisk    – Per-table files on disk, lazy-loaded into RAM.
//	ModeIndex   – Schemas in RAM, rows on disk with small LRU cache.
//	ModeHybrid  – Disk-backed with configurable LRU memory limit.
//
// Examples:
//
//	// In-memory with save-on-close
//	db, _ := tinysql.OpenDB(tinysql.StorageConfig{
//	    Mode: tinysql.ModeMemory,
//	    Path: "mydb.gob",
//	})
//	defer db.Close()
//
//	// Disk mode for large databases
//	db, _ := tinysql.OpenDB(tinysql.StorageConfig{
//	    Mode: tinysql.ModeDisk,
//	    Path: "/data/mydb",
//	})
//	defer db.Close()
//
//	// Hybrid mode with 512 MB cache
//	db, _ := tinysql.OpenDB(tinysql.StorageConfig{
//	    Mode:           tinysql.ModeHybrid,
//	    Path:           "/data/mydb",
//	    MaxMemoryBytes: 512 * 1024 * 1024,
//	})
//	defer db.Close()
//
// Always call db.Close() to ensure data is flushed to disk.
func OpenDB(cfg StorageConfig) (*DB, error) {
	return storage.OpenDB(cfg)
}

// ParseStorageMode converts a string like "memory", "wal", "disk", "index",
// or "hybrid" into a StorageMode constant (case-insensitive).
func ParseStorageMode(s string) (StorageMode, error) {
	return storage.ParseStorageMode(s)
}

// DefaultStorageConfig returns a StorageConfig with sensible defaults for
// the given mode. The caller should set Path before passing to OpenDB.
func DefaultStorageConfig(mode StorageMode) StorageConfig {
	return storage.DefaultStorageConfig(mode)
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

// ============================================================================
// File Import - Auto-import structured data files
// ============================================================================

// ImportOptions re-exports importer.ImportOptions for convenience.
// Configure import behavior including type inference, batching, and null handling.
type ImportOptions = importer.ImportOptions

// ImportResult re-exports importer.ImportResult for convenience.
// Contains metadata about the import operation.
type ImportResult = importer.ImportResult

// ImportFile imports a structured data file (CSV, TSV, JSON, XML) into a table.
// The format is auto-detected from the file extension or content.
//
// Supported formats:
//   - CSV (.csv) - Comma-separated values with auto-detected delimiters
//   - TSV (.tsv, .tab) - Tab-separated values
//   - JSON (.json) - Array of objects format: [{"id": 1, "name": "Alice"}, ...]
//   - XML (.xml) - Simple row-based XML (limited support)
//   - Compressed (.gz) - Transparent gzip decompression
//
// Example:
//
//	db := tinysql.NewDB()
//	result, err := tinysql.ImportFile(ctx, db, "default", "users", "data.csv", &tinysql.ImportOptions{
//	    CreateTable: true,
//	    TypeInference: true,
//	    BatchSize: 1000,
//	})
//	fmt.Printf("Imported %d rows\n", result.RowsInserted)
//
// Parameters:
//   - ctx: Context for cancellation
//   - db: Target database instance
//   - tenant: Tenant/schema name (use "default" for single-tenant mode)
//   - tableName: Target table name (if empty, derived from filename)
//   - filePath: Path to the file to import
//   - opts: Optional configuration (nil uses sensible defaults)
//
// Returns ImportResult with metadata and any error encountered.
func ImportFile(ctx context.Context, db *DB, tenant, tableName, filePath string, opts *ImportOptions) (*ImportResult, error) {
	return importer.ImportFile(ctx, db, tenant, tableName, filePath, opts)
}

// ImportCSV imports CSV/TSV data from a reader into a table.
// Use this for streaming imports or when you already have an io.Reader.
//
// Example:
//
//	f, _ := os.Open("data.csv")
//	defer f.Close()
//	result, err := tinysql.ImportCSV(ctx, db, "default", "users", f, &tinysql.ImportOptions{
//	    HeaderMode: "auto",
//	    DelimiterCandidates: []rune{',', ';', '\t'},
//	})
//
// Parameters:
//   - ctx: Context for cancellation
//   - db: Target database instance
//   - tenant: Tenant/schema name
//   - tableName: Target table name
//   - src: Input reader (file, network stream, stdin, etc.)
//   - opts: Optional configuration (nil uses defaults)
//
// Returns ImportResult with metadata and any error encountered.
func ImportCSV(ctx context.Context, db *DB, tenant, tableName string, src io.Reader, opts *ImportOptions) (*ImportResult, error) {
	return importer.ImportCSV(ctx, db, tenant, tableName, src, opts)
}

// ImportJSON imports JSON data from a reader into a table.
// Supports array of objects format: [{"id": 1, "name": "Alice"}, ...]
//
// Example:
//
//	jsonData := `[{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]`
//	result, err := tinysql.ImportJSON(ctx, db, "default", "users",
//	    strings.NewReader(jsonData), nil)
//
// Parameters:
//   - ctx: Context for cancellation
//   - db: Target database instance
//   - tenant: Tenant/schema name
//   - tableName: Target table name
//   - src: Input reader
//   - opts: Optional configuration
//
// Returns ImportResult with metadata and any error encountered.
func ImportJSON(ctx context.Context, db *DB, tenant, tableName string, src io.Reader, opts *ImportOptions) (*ImportResult, error) {
	return importer.ImportJSON(ctx, db, tenant, tableName, src, opts)
}

// OpenFile opens a data file and returns a DB with the data loaded.
// This is a convenience function for quick data exploration.
//
// Example:
//
//	db, tableName, _ := tinysql.OpenFile(context.Background(), "data.csv", nil)
//	stmt, _ := tinysql.ParseSQL(fmt.Sprintf("SELECT * FROM %s LIMIT 10", tableName))
//	rs, _ := tinysql.Execute(ctx, db, "default", stmt)
//	for _, row := range rs.Rows {
//	    fmt.Println(row)
//	}
//
// Parameters:
//   - ctx: Context for cancellation
//   - filePath: Path to the data file
//   - opts: Optional import configuration
//
// Returns:
//   - db: New database instance with imported data
//   - tableName: The table name where data was loaded
//   - error: Any error encountered during import
func OpenFile(ctx context.Context, filePath string, opts *ImportOptions) (*DB, string, error) {
	return importer.OpenFile(ctx, filePath, opts)
}

// ============================================================================
// Fuzzy Import - Tolerant parsing for malformed data
// ============================================================================

// FuzzyImportOptions extends ImportOptions with fuzzy parsing capabilities.
// Use this for importing data that may have formatting issues, inconsistent
// delimiters, malformed quotes, or other common data quality problems.
type FuzzyImportOptions = importer.FuzzyImportOptions

// FuzzyImportCSV is a more forgiving version of ImportCSV that handles malformed data.
// It attempts to automatically fix common issues like:
//   - Inconsistent column counts (pads/truncates rows)
//   - Unmatched quotes in CSV fields
//   - Mixed delimiters within the same file
//   - Invalid UTF-8 characters
//   - Numbers with thousand separators
//   - Mixed data types in columns
//
// Example:
//
//	opts := &tinysql.FuzzyImportOptions{
//	    ImportOptions: &tinysql.ImportOptions{
//	        CreateTable: true,
//	        TypeInference: true,
//	    },
//	    SkipInvalidRows: true,
//	    FixQuotes: true,
//	    CoerceTypes: true,
//	}
//	result, err := tinysql.FuzzyImportCSV(ctx, db, "default", "messy_data", file, opts)
//
// Parameters are the same as ImportCSV.
// Returns ImportResult which includes errors encountered (non-fatal in fuzzy mode).
func FuzzyImportCSV(ctx context.Context, db *DB, tenant, tableName string, src io.Reader, opts *FuzzyImportOptions) (*ImportResult, error) {
	return importer.FuzzyImportCSV(ctx, db, tenant, tableName, src, opts)
}

// FuzzyImportJSON attempts to parse malformed JSON data.
// It handles common JSON issues like:
//   - Single quotes instead of double quotes
//   - Unquoted object keys
//   - Line-delimited JSON (NDJSON format)
//   - Trailing commas
//
// Example:
//
//	// Works even with malformed JSON like {'name': 'Alice', 'age': 30}
//	result, err := tinysql.FuzzyImportJSON(ctx, db, "default", "users", file, nil)
//
// Parameters are the same as ImportJSON.
func FuzzyImportJSON(ctx context.Context, db *DB, tenant, tableName string, src io.Reader, opts *FuzzyImportOptions) (*ImportResult, error) {
	return importer.FuzzyImportJSON(ctx, db, tenant, tableName, src, opts)
}
