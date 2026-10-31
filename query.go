package tinysql

import (
	"context"
	"fmt"
	"strings"

	"github.com/SimonWaldherr/tinySQL/internal/engine"
	"github.com/SimonWaldherr/tinySQL/internal/sqlbind"
	"github.com/SimonWaldherr/tinySQL/standards"
)

// IndexAdvisor observes explicitly routed queries and optionally creates indexes.
type IndexAdvisor = engine.IndexAdvisor

type IndexAdvisorOptions = engine.IndexAdvisorOptions

type IndexRecommendation = engine.IndexRecommendation

func NewIndexAdvisor(db *DB, options IndexAdvisorOptions) (*IndexAdvisor, error) {
	return engine.NewIndexAdvisor(db, options)
}

// Row represents a single result row mapped by column name (case-insensitive).
// Keys include both qualified (table.column) and unqualified (column) names.
type Row = engine.Row

// ResultSet holds query results with column names and data rows.
// Returned by SELECT queries and available for inspection.
type ResultSet = engine.ResultSet

// ColumnarResultSet holds positional result columns without a map per row.
type ColumnarResultSet = engine.ColumnarResultSet

// ResultStream exposes query rows incrementally. Call Next and Row in a loop,
// check Err afterward, and call Close when abandoning the stream early.
type ResultStream = engine.ResultStream

// StreamOptions controls ResultStream buffering and backpressure. It is
// intentionally additive: ExecuteStream keeps its existing default buffer;
// callers that need strict or larger buffering can use the *WithOptions
// variants.
type StreamOptions = engine.StreamOptions

// StreamStats is a point-in-time snapshot of a running or completed stream.
type StreamStats = engine.StreamStats

// DefaultStreamBuffer is the producer buffer used by ExecuteStream and its
// convenience wrappers. Use StreamOptions with a *WithOptions function to
// select a different capacity.
const DefaultStreamBuffer = engine.DefaultResultStreamBuffer

// SQLStateError attaches an ISO/IEC 9075 SQLSTATE code to an error.
type SQLStateError = standards.SQLStateError

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

const (
	SQLStateSuccessfulCompletion = standards.SQLStateSuccessfulCompletion
	SQLStateWarning              = standards.SQLStateWarning
	SQLStateNoData               = standards.SQLStateNoData
	SQLStateDataException        = standards.SQLStateDataException
	SQLStateInvalidDatetime      = standards.SQLStateInvalidDatetime
	SQLStateInvalidParameter     = standards.SQLStateInvalidParameter
	SQLStateSyntaxError          = standards.SQLStateSyntaxError
	SQLStateUndefinedTable       = standards.SQLStateUndefinedTable
	SQLStateIntegrityViolation   = standards.SQLStateIntegrityViolation
	SQLStateInvalidAuthorization = standards.SQLStateInvalidAuthorization
	SQLStateTransactionRollback  = standards.SQLStateTransactionRollback
	SQLStateInternalError        = standards.SQLStateInternalError
)

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

// WithSQLState wraps err with a SQLSTATE code. It is useful for integrations
// that expose tinySQL errors over SQL drivers, HTTP APIs, or observability
// pipelines where stable machine-readable error classes matter.
func WithSQLState(state string, err error) error {
	return standards.WithSQLState(state, err)
}

// SQLState returns the SQLSTATE attached to err, if present.
func SQLState(err error) string {
	return standards.SQLState(err)
}

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

// ExecuteColumnar executes a SELECT into Values[column][row]. Simple scans
// avoid Row maps; complex SELECTs transpose the ordinary executor's result.
func ExecuteColumnar(ctx context.Context, db *DB, tenant string, stmt Statement) (*ColumnarResultSet, error) {
	return engine.ExecuteColumnar(ctx, db, tenant, stmt)
}

// ExecSQLColumnar parses and executes one SELECT with compact column results.
func ExecSQLColumnar(ctx context.Context, db *DB, tenant, sql string) (*ColumnarResultSet, error) {
	stmt, err := ParseSQL(sql)
	if err != nil {
		return nil, err
	}
	return ExecuteColumnar(ctx, db, tenant, stmt)
}

// ExecuteStream starts a parsed statement and returns as soon as its result
// columns are known. Simple scans yield rows while execution continues;
// blocking operations such as ORDER BY, GROUP BY and DISTINCT preserve their
// semantics and begin yielding after materialization.
func ExecuteStream(ctx context.Context, db *DB, tenant string, stmt Statement) (*ResultStream, error) {
	return engine.ExecuteStream(ctx, db, tenant, stmt)
}

// ExecuteStreamWithOptions starts a parsed statement with explicit stream
// backpressure settings. A zero Buffer is valid and provides strict
// producer/consumer handoff.
func ExecuteStreamWithOptions(ctx context.Context, db *DB, tenant string, stmt Statement, opts StreamOptions) (*ResultStream, error) {
	return engine.ExecuteStreamWithOptions(ctx, db, tenant, stmt, opts)
}

// ExecSQL parses and executes exactly one SQL statement. It is the concise
// public entry point for dynamic SQL, scripts with one statement per call, and
// small embedded applications. For repeated SQL, prefer Compile or database/sql
// prepared statements so parsing and planning can be reused.
//
// The tenant is required; use "default" for single-tenant applications.
func ExecSQL(ctx context.Context, db *DB, tenant, sql string) (*ResultSet, error) {
	stmt, err := ParseSQL(sql)
	if err != nil {
		return nil, err
	}
	return Execute(ctx, db, tenant, stmt)
}

// ExecSQLArgs binds args to the ?, $n or :n placeholders of one SQL
// statement, then parses and executes it. Use it for application or user
// values instead of formatting them into SQL text.
//
// Values become SQL literals with the database/sql driver's rules: nil is
// NULL, strings are single-quoted with doubled quotes, []byte is a BLOB
// (X'..'), time.Time is an RFC 3339 UTC string, and maps, slices and structs
// are JSON text. A '?' consumes the next argument; $n and :n reference the
// n-th argument and may repeat. Every argument must be used. Placeholder
// characters inside string literals, quoted identifiers and comments are SQL
// text, not parameters.
func ExecSQLArgs(ctx context.Context, db *DB, tenant, sql string, args ...any) (*ResultSet, error) {
	bound, err := sqlbind.BindValues(sql, args...)
	if err != nil {
		return nil, err
	}
	return ExecSQL(ctx, db, tenant, bound)
}

// ExecScript executes a multi-statement SQL script in order and returns the
// result of its last statement. Statements are split with SplitStatements
// semantics (see package sqlutil): semicolons in literals, quoted identifiers,
// comments and CREATE TRIGGER bodies do not split.
//
// Execution stops at the first failing statement; the error names its
// 1-based position and wraps the engine error. Statements that already ran
// stay applied. Transaction control (BEGIN/COMMIT/ROLLBACK) is provided by the
// database/sql driver, not by this direct API.
func ExecScript(ctx context.Context, db *DB, tenant, script string) (*ResultSet, error) {
	var last *ResultSet
	for i, statement := range engine.SplitStatements(script) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		rs, err := ExecSQL(ctx, db, tenant, statement)
		if err != nil {
			return nil, fmt.Errorf("statement %d: %w", i+1, err)
		}
		last = rs
	}
	return last, nil
}

// ExecSQLStream parses one SQL statement and exposes its result rows
// incrementally. Call Close if iteration stops before Next returns false.
func ExecSQLStream(ctx context.Context, db *DB, tenant, sql string) (*ResultStream, error) {
	stmt, err := ParseSQL(sql)
	if err != nil {
		return nil, err
	}
	return ExecuteStream(ctx, db, tenant, stmt)
}

// ExecSQLStreamWithOptions parses one SQL statement and starts it with
// explicit stream backpressure settings.
func ExecSQLStreamWithOptions(ctx context.Context, db *DB, tenant, sql string, opts StreamOptions) (*ResultStream, error) {
	stmt, err := ParseSQL(sql)
	if err != nil {
		return nil, err
	}
	return ExecuteStreamWithOptions(ctx, db, tenant, stmt, opts)
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
	if compiled == nil {
		return nil, fmt.Errorf("cannot execute a nil compiled query")
	}
	return compiled.Execute(ctx, db, tenant)
}

// ExecuteCompiledStream executes a pre-compiled query and exposes rows
// incrementally where the query shape permits it.
func ExecuteCompiledStream(ctx context.Context, db *DB, tenant string, compiled *CompiledQuery) (*ResultStream, error) {
	if compiled == nil {
		return nil, fmt.Errorf("cannot stream a nil compiled query")
	}
	return compiled.Stream(ctx, db, tenant)
}

// ExecuteCompiledStreamWithOptions executes a pre-compiled query with
// explicit stream backpressure settings.
func ExecuteCompiledStreamWithOptions(ctx context.Context, db *DB, tenant string, compiled *CompiledQuery, opts StreamOptions) (*ResultStream, error) {
	if compiled == nil {
		return nil, fmt.Errorf("cannot stream a nil compiled query")
	}
	return compiled.StreamWithOptions(ctx, db, tenant, opts)
}

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
