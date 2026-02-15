package driver

import (
	"database/sql"

	id "github.com/SimonWaldherr/tinySQL/internal/driver"
	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// DriverName is the registered database/sql driver name for tinySQL.
const DriverName = "tinysql"

// Open is a convenience wrapper around `sql.Open(DriverName, dsn)`.
func Open(dsn string) (*sql.DB, error) { return sql.Open(DriverName, dsn) }

// OpenFile is a convenience wrapper that opens a file-backed tinySQL database
// by constructing a `file:` DSN for `sql.Open`.
func OpenFile(path string) (*sql.DB, error) { return Open("file:" + path) }

// OpenWithDB registers the provided storage.DB as the driver's default DB and
// returns a *sql.DB connected to it. This is useful for embedding or tests.
func OpenWithDB(db *storage.DB) (*sql.DB, error) {
	// Register provided DB instance for subsequent Open("") calls.
	SetDefaultDB(db)
	return Open("")
}

// Re-export selected symbols from the internal driver package so external
// consumers can use a stable public API while the implementation remains
// hidden under `internal/driver`.
var (
	OpenInMemory = id.OpenInMemory
	SetDefaultDB = id.SetDefaultDB
)
