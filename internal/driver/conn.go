// The connection and its lifecycle in the pool.
//
// database/sql only knows about transactions started through BeginTx. A
// transaction started by Exec("BEGIN") is invisible to the pool, so ResetSession
// and Close roll back an abandoned one: otherwise the connection stays inside it
// and silently discards every later write.
package driver

import (
	"context"
	"database/sql/driver"
	"fmt"
	"log"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

type conn struct {
	srv    *server
	tenant string

	inTx       bool
	txBase     *storage.DB // Snapshot base used for conflict detection
	shadow     *storage.DB // Snapshot copy (MVCC-light)
	txReadOnly bool        // Active tx requested as read-only
	txDirty    bool        // A successful write ran against shadow.
	// txCatalogRev is the shadow catalog's revision when the transaction began,
	// and txCatalog a copy of its contents. commitTx uses the revision as a
	// cheap gate — unchanged means nothing touched the catalog at all — and the
	// contents to tell an actual change (CREATE VIEW/TRIGGER/JOB, GRANT,
	// materialized-view staleness) from incidental bookkeeping that ordinary
	// DML performs. Only an actual change is installed on the live database,
	// and only if no concurrent change would be discarded by doing so.
	txCatalogRev uint64
	txCatalog    storage.CatalogSnapshot

	// wrote is set the first time this connection executes a statement that
	// is not a pure read (see execStatement's isWrite switch — the same
	// decision point that gates the per-statement/per-commit persist() call).
	// Close() uses it to skip persistBestEffort() for connections that never
	// wrote, so read-only pool churn (idle reaping, ConnMaxLifetime) does not
	// pay for a durable-storage sync that could not possibly have anything to
	// flush. A connection that wrote even once still persists on Close(),
	// unchanged from before — this only removes work that was pure overhead.
	wrote bool
}

func (c *conn) Prepare(query string) (driver.Stmt, error) {
	// database/sql may call Prepare for arbitrary SQL. Failing to build the
	// optional prepared-AST fast path must not change its historical behavior:
	// QueryContext will use the text-binding fallback below.
	prepared, _ := buildPreparedQuery(query)
	return &stmt{c: c, sql: query, prepared: prepared}, nil
}

func (c *conn) Close() error {
	// A connection can be closed with a SQL-level BEGIN still open, because
	// database/sql does not know about a transaction started by Exec("BEGIN")
	// rather than by BeginTx. Discard it here so the shadow is not leaked and no
	// ModeAdvancedWAL transaction is left without an abort record.
	c.discardOpenTx()
	if c.wrote {
		c.srv.persistBestEffort()
	}
	return nil
}

func (c *conn) Begin() (driver.Tx, error) { return c.BeginTx(context.Background(), driver.TxOptions{}) }

// ResetSession implements driver.SessionResetter. database/sql calls it before
// handing a pooled connection to the next user.
//
// Without it, a connection returned to the pool with an open transaction — the
// shape an application produces by running Exec("BEGIN") and then forgetting to
// commit, or by having the statement after BEGIN fail — stayed in that
// transaction forever. Every later write on that connection went to the
// abandoned shadow and was silently discarded, and no other connection could
// see any of it. Rolling back here makes the connection clean for its next user
// and matches what every other database/sql driver does.
func (c *conn) ResetSession(ctx context.Context) error {
	if c.srv == nil || c.srv.db == nil {
		return driver.ErrBadConn
	}
	c.discardOpenTx()
	return nil
}

// IsValid implements driver.Validator so the pool discards a connection whose
// database has been closed underneath it instead of returning errors from it.
func (c *conn) IsValid() bool {
	return c.srv != nil && c.srv.db != nil && !c.srv.db.IsClosed()
}

// discardOpenTx rolls back a transaction still open on this connection. It is
// deliberately quiet: the caller is a lifecycle hook, and there is nobody left
// to report a rollback failure to.
func (c *conn) discardOpenTx() {
	if !c.inTx {
		return
	}
	if err := c.rollbackTx(); err != nil {
		log.Printf("tinysql: discarding abandoned transaction: %v", err)
	}
}

func (c *conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if c.inTx {
		return nil, fmt.Errorf("tinysql: transaction already active")
	}
	// Only the default isolation level is supported; other levels are rejected.
	switch opts.Isolation {
	case driver.IsolationLevel(0): // Default
		// Allow default isolation
	default:
		return nil, fmt.Errorf("unsupported isolation level: %v", opts.Isolation)
	}
	// An immutable/read-only database never has a writer to conflict with.
	// Avoid DeepClone here: cloning a disk-backed ModeIndex catalog would both
	// defeat its memory bound and lose its backend reference. The shared,
	// immutable DB itself is the transaction snapshot.
	if c.srv.db.IsReadOnly() {
		c.inTx = true
		c.txBase = nil
		c.shadow = nil
		c.txReadOnly = true
		c.txDirty = false
		c.txCatalogRev = 0
		return &tx{c: c}, nil
	}

	// Create snapshot copy under read lock; writer blocks commit briefly.
	if err := c.srv.acquireReader(ctx); err != nil {
		return nil, err
	}
	defer c.srv.releaseReader()
	c.srv.mu.RLock()
	// DeepClone/SnapshotForTx read every table's live Rows/Cols/Version --
	// exactly what storage.DB.contentMu exists to guard against a concurrent
	// mutation of the same fields (see its doc comment in
	// internal/storage/db.go). server.mu.RLock() above only coordinates with
	// this *driver*'s own writers; it says nothing about a caller that
	// mutates the same *storage.DB directly through engine.Execute/
	// tinysql.Execute -- the job scheduler (internal/storage/scheduler.go),
	// reachable when an app shares one DB between a driver server and
	// StartJobScheduler via SetDefaultDB. Taking contentMu for read here
	// closes that gap: LockContentForRead/LockContentForWrite is the same
	// choke point executeStatement already uses for every ordinary
	// statement, driver-mediated or not.
	c.srv.db.LockContentForRead()
	var base, shadow *storage.DB
	if opts.ReadOnly {
		shadow = c.srv.db.DeepClone()
	} else {
		// A read-only transaction produces no changes to merge, so it needs
		// only a single stable read snapshot and no conflict-detection base.
		// A read-write transaction needs a mutable shadow plus a
		// lightweight version-only base (SnapshotForTx copies rows once,
		// not twice).
		base, shadow = c.srv.db.SnapshotForTx()
	}
	c.srv.db.UnlockContentForRead()
	c.srv.mu.RUnlock()

	c.inTx = true
	c.txBase = base
	c.shadow = shadow
	c.txReadOnly = opts.ReadOnly
	c.txDirty = false
	// The shadow's catalog is a private deep copy of the live one, so its
	// contents here are also the live contents at BEGIN.
	c.txCatalogRev = shadow.Catalog().Revision()
	c.txCatalog = shadow.SnapshotCatalog()
	return &tx{c: c}, nil
}

// Ping implements driver.Pinger so database/sql can health-check the connection.
func (c *conn) Ping(ctx context.Context) error {
	if c.srv == nil {
		return fmt.Errorf("tinysql: no server")
	}
	if err := c.srv.acquireReader(ctx); err != nil {
		return err
	}
	c.srv.releaseReader()
	return nil
}

type tx struct{ c *conn }

func (t *tx) Commit() error {
	return t.c.commitTx()
}

func (t *tx) Rollback() error {
	return t.c.rollbackTx()
}
