package driver

import (
	"database/sql"

	tinysql "github.com/SimonWaldherr/tinySQL"
	id "github.com/SimonWaldherr/tinySQL/internal/driver"
)

// DriverName is the registered database/sql driver name for tinySQL.
const DriverName = "tinysql"

// ErrTransactionConflict indicates a retryable optimistic-concurrency
// conflict while committing a transaction. Use errors.Is to check it.
var ErrTransactionConflict = id.ErrTransactionConflict

// Open is a convenience wrapper around `sql.Open(DriverName, dsn)`.
func Open(dsn string) (*sql.DB, error) { return sql.Open(DriverName, dsn) }

// OpenFile is a convenience wrapper that opens a file-backed tinySQL database
// by constructing a `file:` DSN for `sql.Open`.
func OpenFile(path string) (*sql.DB, error) { return Open("file:" + path) }

// OpenWithDB returns a SQL pool bound to the provided public tinySQL database.
// It does not change the global default database. All pooled connections use
// db, including connections opened later. The caller retains ownership of db:
// close the SQL pool before closing db. A nil or closed db is rejected.
func OpenWithDB(db *tinysql.DB) (*sql.DB, error) {
	return id.OpenWithDB(db)
}

// OpenInMemory returns a database/sql handle backed by an in-memory tinySQL
// server. If tenant is empty, the default tenant is used.
func OpenInMemory(tenant string) (*sql.DB, error) { return id.OpenInMemory(tenant) }

// SetDefaultDB selects the public tinySQL database used by subsequently opened
// default driver connections. It is primarily intended for embedding and
// tests; ordinary applications should prefer OpenWithConfig.
func SetDefaultDB(db *tinysql.DB) { id.SetDefaultDB(db) }

// CurrentDefaultDB returns the public tinySQL database currently backing the
// driver's default server, if one exists.
func CurrentDefaultDB() *tinysql.DB { return id.CurrentDefaultDB() }
