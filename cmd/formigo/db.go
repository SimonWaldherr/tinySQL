package main

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"strings"
	"time"

	_ "github.com/SimonWaldherr/tinySQL/driver"
	_ "github.com/microsoft/go-mssqldb"
)

//go:embed schema_tinysql.sql schema_sqlserver.sql
var schemaFS embed.FS

// dialect contains DB-specific SQL differences used by the store.
type dialect struct {
	name string
}

// detectDialect returns the database dialect and database/sql driver name for a DSN.
func detectDialect(dsn string) (dialect, string) {
	lower := strings.ToLower(dsn)
	if strings.HasPrefix(lower, "sqlserver://") || strings.Contains(lower, "server=") {
		return dialect{name: "sqlserver"}, "sqlserver"
	}
	return dialect{name: "tinysql"}, "tinysql"
}

// placeholder returns a dialect-specific positional placeholder.
func (d dialect) placeholder(n int) string {
	if d.name == "sqlserver" {
		return fmt.Sprintf("@p%d", n)
	}
	return "?"
}

// placeholders returns n placeholders for use in INSERT statements.
func (d dialect) placeholders(n int) string {
	parts := make([]string, n)
	for i := range parts {
		parts[i] = d.placeholder(i + 1)
	}
	return strings.Join(parts, ", ")
}

// insertReturningID inserts a row and returns its generated primary key.
func (d dialect) insertReturningID(ctx context.Context, tx *sql.Tx, table string, columns []string, args ...any) (int64, error) {
	if d.name == "sqlserver" {
		colList := strings.Join(columns, ", ")
		query := fmt.Sprintf("INSERT INTO %s (%s) OUTPUT INSERTED.id VALUES (%s)", table, colList, d.placeholders(len(columns)))
		var id int64
		if err := tx.QueryRowContext(ctx, query, args...).Scan(&id); err != nil {
			return 0, err
		}
		return id, nil
	}

	// tinySQL: determine next id via MAX(id) + 1 on the tx snapshot.
	var maxID sql.NullInt64
	if err := tx.QueryRowContext(ctx, "SELECT MAX(id) FROM "+table).Scan(&maxID); err != nil {
		// Ignore – table may be empty (NULL scan falls back to NullInt64{Valid:false}).
		maxID = sql.NullInt64{}
	}
	newID := maxID.Int64 + 1

	allCols := make([]string, 0, len(columns)+1)
	allCols = append(allCols, "id")
	allCols = append(allCols, columns...)

	allArgs := make([]any, 0, len(args)+1)
	allArgs = append(allArgs, newID)
	allArgs = append(allArgs, args...)

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", table, strings.Join(allCols, ", "), d.placeholders(len(allCols)))
	if _, err := tx.ExecContext(ctx, query, allArgs...); err != nil {
		return 0, err
	}
	return newID, nil
}

// openDB opens the configured database and applies connection settings.
func openDB(ctx context.Context, dsn string) (*sql.DB, dialect, error) {
	d, driverName := detectDialect(dsn)

	db, err := sql.Open(driverName, dsn)
	if err != nil {
		return nil, d, err
	}

	if d.name == "sqlserver" {
		db.SetMaxOpenConns(20)
		db.SetMaxIdleConns(5)
		db.SetConnMaxLifetime(30 * time.Minute)
	} else {
		// tinySQL: single writer, allow a small reader pool.
		db.SetMaxOpenConns(8)
		db.SetMaxIdleConns(4)
	}

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, d, err
	}
	return db, d, nil
}

// migrate applies the embedded schema for the active database dialect.
func migrate(ctx context.Context, db *sql.DB, d dialect) error {
	name := "schema_tinysql.sql"
	if d.name == "sqlserver" {
		name = "schema_sqlserver.sql"
	}
	b, err := schemaFS.ReadFile(name)
	if err != nil {
		return err
	}
	for _, stmt := range splitMigrationStatements(string(b)) {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			// ALTER TABLE may fail when the column already exists (idempotent re-migration).
			if strings.Contains(strings.ToUpper(stmt), "ALTER TABLE") {
				continue
			}
			return fmt.Errorf("migration statement failed: %w\n%s", err, stmt)
		}
	}
	return nil
}

// splitMigrationStatements splits schema files on marker lines.
func splitMigrationStatements(schema string) []string {
	chunks := strings.Split(schema, "-- statement")
	out := make([]string, 0, len(chunks))
	for _, c := range chunks {
		c = strings.TrimSpace(c)
		if c != "" {
			out = append(out, c)
		}
	}
	return out
}

// withTx executes fn inside a transaction and commits only if fn succeeds.
func withTx(ctx context.Context, db *sql.DB, fn func(*sql.Tx) error) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}
