// Package driver provides a lightweight database/sql driver for tinySQL.
//
// The driver exposes tinySQL through the standard `database/sql` API and
// supports both in-memory and file-backed databases. Key features:
//
//   - DSN formats: `mem://` and `file:/path/to/db.gob?options` (see `parseDSN`).
//   - Optional Write-Ahead Log (WAL) and autosave for durability.
//   - `mode=` selects any storage.StorageMode (disk, json, hybrid, wal, ...)
//     via storage.OpenDB instead of the default GOB-snapshot behavior.
//   - Reader/writer pools and simple MVCC-style snapshots for transactions.
//   - Simple, safe placeholder binding: sequential `?` and numbered `$1`/`:1`.
//
// Use `sql.Open("tinysql", dsn)` to create a connection. Each sql.Open call
// creates one Connector which owns one lazily opened server/storage.DB. The
// physical connections subsequently created by database/sql share that server,
// while transactions and prepared statements remain connection-local. Separate
// sql.Open calls never share a DSN-backed database instance implicitly.
//
// See applyDSNOption and applyQueryOptions for available DSN options and
// defaults.
package driver

import (
	"database/sql"
	"encoding/gob"
	"errors"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// init registers the "tinysql" driver and pre-registers common GOB types.
// This enables database/sql.Open("tinysql", dsn) to work out of the box.
// Supported DSNs:
//   - mem://?tenant=default&pool_readers=4&busy_timeout=250ms
//   - file:/path/to/db.gob?tenant=default&autosave=1
//   - file:/path/to/dbdir?tenant=default&mode=json (or disk, hybrid, wal, ...)
//
// See parseDSN for all available options.
// defaultDrv is the package-global driver instance registered with database/sql.
var defaultDrv = &drv{}

// ErrTransactionConflict reports that a transaction attempted to commit a
// table that was changed after its snapshot was taken. Callers can use
// errors.Is to distinguish a retryable write conflict from other failures.
var ErrTransactionConflict = errors.New("tinysql: transaction conflict")

func init() {
	sql.Register("tinysql", defaultDrv)
	gob.Register(map[string]any{})
	gob.Register([]any{})
}

// SetDefaultDB allows external code to provide a storage.DB instance that will
// be used by the driver when opening connections. This is useful for embedding
// environments (WASM) that want to keep a reference to the underlying DB.
func SetDefaultDB(db *storage.DB) {
	if db == nil {
		return
	}
	// Create a default cfg with sane defaults
	c := cfg{
		tenant:      "default",
		maxReaders:  4,
		maxWriters:  1,
		busyTimeout: 250 * time.Millisecond,
	}
	// Pre-create server using provided DB so subsequent Open() calls reuse it.
	// Note: This allows embedding consumers to control the underlying DB
	// instance (for example tests or WASM hosts) while still using the
	// database/sql API.
	defaultDrv.mu.Lock()
	defaultDrv.srv = newServer(db, c)
	defaultDrv.mu.Unlock()
}

// CurrentDefaultDB returns the storage database currently backing the default
// driver server, if one exists.
func CurrentDefaultDB() *storage.DB {
	defaultDrv.mu.RLock()
	srv := defaultDrv.srv
	defaultDrv.mu.RUnlock()
	if srv == nil {
		return nil
	}
	srv.mu.RLock()
	defer srv.mu.RUnlock()
	return srv.db
}

// OpenInMemory returns a *sql.DB backed by an in-memory tinySQL server.
// If tenant is empty the default tenant is used.
func OpenInMemory(tenant string) (*sql.DB, error) {
	dsn := "mem://"
	if tenant != "" {
		dsn += "?tenant=" + tenant
	}
	return sql.Open("tinysql", dsn)
}
