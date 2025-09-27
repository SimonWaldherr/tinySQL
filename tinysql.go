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

// Execute executes a SQL statement
func Execute(ctx context.Context, db *storage.DB, tenant string, stmt engine.Statement) (*engine.ResultSet, error) {
	return engine.Execute(ctx, db, tenant, stmt)
}

// GetVal retrieves a value from a row by column name
func GetVal(row engine.Row, name string) (any, bool) {
	v, ok := row[strings.ToLower(name)]
	return v, ok
}
