package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strconv"
	"testing"

	tinysql "github.com/SimonWaldherr/tinySQL"
)

type failingMigrationDriver struct{}
type failingMigrationConn struct{}
type failingMigrationRows struct {
	next   int
	buffer []byte
}

var errMigrationSource = errors.New("source interrupted")

func (failingMigrationDriver) Open(string) (driver.Conn, error) { return failingMigrationConn{}, nil }
func (failingMigrationConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("not supported")
}
func (failingMigrationConn) Begin() (driver.Tx, error) { return nil, errors.New("not supported") }
func (failingMigrationConn) Close() error              { return nil }
func (failingMigrationConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	return &failingMigrationRows{}, nil
}
func (*failingMigrationRows) Columns() []string { return []string{"id", "label"} }
func (*failingMigrationRows) ColumnTypeDatabaseTypeName(i int) string {
	if i == 0 {
		return "INTEGER"
	}
	return "TEXT"
}
func (*failingMigrationRows) Close() error { return nil }
func (r *failingMigrationRows) Next(dest []driver.Value) error {
	if r.next == 300 {
		return errMigrationSource
	}
	r.next++
	r.buffer = strconv.AppendInt(r.buffer[:0], int64(r.next), 10)
	dest[0], dest[1] = int64(r.next), r.buffer
	return nil
}
func init() { sql.Register("tinysql_migration_failing_test", failingMigrationDriver{}) }

func TestExternalImportSourceFailureRetainsCompletedRows(t *testing.T) {
	src, err := sql.Open("tinysql_migration_failing_test", "")
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	db := tinysql.NewDB()
	defer db.Close()
	stats, err := importFromExternal(db, t.Context(), "default", src, "source", "target")
	if !errors.Is(err, errMigrationSource) || stats.Imported != 300 || stats.Skipped != 0 {
		t.Fatalf("%+v %v", stats, err)
	}
	table, err := db.Get("default", "target")
	if err != nil {
		t.Fatal(err)
	}
	if len(table.Rows) != 300 {
		t.Fatal(len(table.Rows))
	}
	for i, row := range table.Rows {
		if row[1] != strconv.Itoa(i+1) {
			t.Fatalf("driver buffer reused: row %d = %v", i, row)
		}
	}
}
