package tinysql

import (
	"context"
	"strings"

	"tinysql/internal/engine"
	"tinysql/internal/storage"
)

// NewDB creates a new database instance
func NewDB() *storage.DB {
	return storage.NewDB()
}

// NewParser creates a new SQL parser
func NewParser(sql string) *engine.Parser {
	return engine.NewParser(sql)
}

// NewQueryCache creates a new query cache for compiling and reusing queries
func NewQueryCache(maxSize int) *engine.QueryCache {
	return engine.NewQueryCache(maxSize)
}

// Compile parses and caches a SQL query for reuse (like regexp.Compile)
func Compile(cache *engine.QueryCache, sql string) (*engine.CompiledQuery, error) {
	return cache.Compile(sql)
}

// MustCompile is like Compile but panics on error (like regexp.MustCompile)
func MustCompile(cache *engine.QueryCache, sql string) *engine.CompiledQuery {
	return cache.MustCompile(sql)
}

// Execute executes a SQL statement
func Execute(ctx context.Context, db *storage.DB, tenant string, stmt engine.Statement) (*engine.ResultSet, error) {
	return engine.Execute(ctx, db, tenant, stmt)
}

// ExecuteCompiled executes a compiled query
func ExecuteCompiled(ctx context.Context, db *storage.DB, tenant string, compiled *engine.CompiledQuery) (*engine.ResultSet, error) {
	return compiled.Execute(ctx, db, tenant)
}

// GetVal retrieves a value from a row by column name
func GetVal(row engine.Row, name string) (any, bool) {
	v, ok := row[strings.ToLower(name)]
	return v, ok
}
