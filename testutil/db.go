// Package testutil provides isolated tinySQL databases for Go tests and benchmarks.
package testutil

import (
	"database/sql"
	"testing"

	tinysql "github.com/SimonWaldherr/tinySQL"
	"github.com/SimonWaldherr/tinySQL/driver"
)

// Open creates a private in-memory database, executes fixtures in order, and
// registers cleanup of both the SQL pool and its native database with t.
// Each fixture is one SQL statement. A setup or cleanup error fails the test.
// Each call is independent, including calls from parallel tests with identical
// names and schemas. Close query rows and transactions before test cleanup.
func Open(t testing.TB, fixtures ...string) *sql.DB {
	t.Helper()
	native := tinysql.NewDB()
	t.Cleanup(func() {
		if err := native.Close(); err != nil {
			t.Errorf("tinySQL test database close: %v", err)
		}
	})
	db, err := driver.OpenWithDB(native)
	if err != nil {
		t.Fatalf("tinySQL test database open: %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("tinySQL test SQL pool close: %v", err)
		}
	})
	for i, query := range fixtures {
		if _, err := db.ExecContext(t.Context(), query); err != nil {
			t.Fatalf("tinySQL fixture %d: %v", i+1, err)
		}
	}
	return db
}
