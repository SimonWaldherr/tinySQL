// Regression tests for two related ModeWAL-only bugs found by reading
// storage.DB.ShallowCloneForTable (internal/storage/db.go) and this
// package's execStatement/BeginTx/commitTx (driver.go), and already
// characterized (but deliberately not fixed) by the Stage 0 WAL harness —
// see internal/driver/wal_fixture_gen_test.go and
// internal/storage/wal_fixture_test.go's package doc comments for the
// original writeup this test confirms empirically.
//
// Both tests below drive the real database/sql driver against a mode=wal
// DSN, mirroring TestDriverModeJSONPersistsAndReopens and
// TestGenerateWALManagerFixture: no WALChange/walRecord value is ever
// hand-constructed, and "restart" is simulated the same way the Stage 0
// fixture-replay tests do — by closing the *sql.DB and reopening the same
// DSN/path from scratch, which forces the ModeWAL recovery path
// (storage.OpenDB -> OpenWAL -> replayWAL) to reconstruct the table from
// whatever was actually durably logged.
package driver

import (
	"database/sql"
	"path/filepath"
	"testing"
)

// TestModeWALAutocommitFirstWriteDetachesWALManager reproduces Bug A: the
// very first autocommit write against a brand-new, empty ModeWAL database
// goes through conn.execStatement's non-transaction path, which clones the
// live DB via storage.DB.ShallowCloneForTable. That function's
// `len(db.tenants) == 0` special case returns a bare storage.NewDB() that
// does not copy the .wal field, and conn.execStatement then makes that bare
// clone the new live database (`c.srv.db = shadow`). Every later autocommit
// statement clones from that already wal-less live database, so once
// detached the WAL never comes back for the rest of the process's life:
// no subsequent write is ever logged, silently, though it remains fully
// visible in memory for the current session.
func TestModeWALAutocommitFirstWriteDetachesWALManager(t *testing.T) {
	dir := t.TempDir()
	dsn := "file:" + filepath.Join(dir, "walbugA") + "?tenant=default&mode=wal"

	db, err := sql.Open("tinysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	// First write: CREATE TABLE. Per the bug, this is the statement whose
	// ShallowCloneForTable call hits the empty-tenants special case and
	// detaches .wal from the live DB going forward.
	if _, err := db.Exec(`CREATE TABLE t (id INT, val TEXT)`); err != nil {
		t.Fatalf("create table (first write): %v", err)
	}
	// Second write: a plain INSERT. If the WAL was silently detached by the
	// first write, this insert is applied in memory but never logged.
	if _, err := db.Exec(`INSERT INTO t VALUES (1, 'a')`); err != nil {
		t.Fatalf("insert (second write): %v", err)
	}

	// Sanity: the insert is visible within the same live session regardless
	// of whether it was durably logged.
	var liveCount int
	if err := db.QueryRow(`SELECT COUNT(*) AS c FROM t`).Scan(&liveCount); err != nil {
		t.Fatalf("count in live session: %v", err)
	}
	if liveCount != 1 {
		t.Fatalf("expected 1 row in the live session, got %d", liveCount)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Simulate a restart: reopen the same WAL-backed database from disk and
	// replay whatever was actually durably logged (storage.OpenDB -> OpenWAL
	// -> replayWAL, exercised here through database/sql).
	reopened, err := sql.Open("tinysql", dsn)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()

	var recoveredCount int
	if err := reopened.QueryRow(`SELECT COUNT(*) AS c FROM t`).Scan(&recoveredCount); err != nil {
		t.Fatalf("count after reopen: %v", err)
	}
	if recoveredCount != 1 {
		t.Fatalf("BUG A: insert lost across restart (WAL silently detached from the live DB after the first autocommit write): got %d rows after reopen, want 1", recoveredCount)
	}
}

// TestModeWALExplicitTransactionRollbackLeavesWALEntry reproduces Bug B for
// the explicit-transaction path: conn.BeginTx builds its shadow via
// storage.DB.SnapshotForTx, which (unlike ShallowCloneForTable) copies .wal
// unconditionally, so the shadow used to run each statement inside a
// BEGIN...COMMIT/ROLLBACK block carries a live WAL reference throughout the
// transaction. internal/engine.executeStatement unconditionally calls
// maybeLogToWALManager once per atomic-DML statement (INSERT/UPDATE/DELETE)
// against whatever *storage.DB it executes on — so each statement inside an
// explicit transaction is logged to the real, shared WAL file immediately,
// as its own committed WALManager transaction, the instant it runs and
// long before the SQL-level transaction actually commits. If that
// transaction is later rolled back instead, the in-memory shadow is simply
// discarded (correct), but the already-flushed per-statement WAL record is
// not retracted: it stays on disk looking exactly like a committed
// transaction and gets replayed on the next recovery, resurrecting data
// that should have been rolled back.
func TestModeWALExplicitTransactionRollbackLeavesWALEntry(t *testing.T) {
	dir := t.TempDir()
	dsn := "file:" + filepath.Join(dir, "walbugB") + "?tenant=default&mode=wal"

	db, err := sql.Open("tinysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	// Create the table inside its own explicit transaction so this test
	// exercises SnapshotForTx (BeginTx's path) from the very first write,
	// rather than ShallowCloneForTable's empty-tenants special case (Bug A,
	// covered separately above) — SnapshotForTx copies .wal unconditionally,
	// so it is not affected by Bug A regardless of tenant emptiness.
	setupTx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin (setup): %v", err)
	}
	if _, err := setupTx.Exec(`CREATE TABLE t (id INT, val TEXT)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if err := setupTx.Commit(); err != nil {
		t.Fatalf("commit (setup): %v", err)
	}

	// A transaction whose single INSERT is rolled back, not committed.
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := tx.Exec(`INSERT INTO t VALUES (99, 'should-not-survive')`); err != nil {
		t.Fatalf("insert in tx: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback: %v", err)
	}

	// Sanity: the live in-memory database never saw the rolled-back row —
	// conn.commitTx is the only path that ever applies a shadow's changes
	// onto the live DB, and it never ran here.
	var liveCount int
	if err := db.QueryRow(`SELECT COUNT(*) AS c FROM t WHERE id = 99`).Scan(&liveCount); err != nil {
		t.Fatalf("count in live session: %v", err)
	}
	if liveCount != 0 {
		t.Fatalf("rolled-back row visible in the live in-memory session (got %d), want 0", liveCount)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Simulate a restart: reopen and let WAL recovery replay whatever was
	// actually durably logged.
	reopened, err := sql.Open("tinysql", dsn)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()

	var recoveredCount int
	if err := reopened.QueryRow(`SELECT COUNT(*) AS c FROM t WHERE id = 99`).Scan(&recoveredCount); err != nil {
		t.Fatalf("count after reopen: %v", err)
	}
	if recoveredCount != 0 {
		t.Fatalf("BUG B: rolled-back row resurrected by WAL replay after restart (got %d matching rows, want 0): each statement inside the transaction was logged to the WAL immediately as it ran, before the transaction's rollback", recoveredCount)
	}
}

// TestModeWALAutocommitWriteDropsCatalogViaShallowClone is a supporting
// probe (not one of the two bugs assigned) checking whether
// ShallowCloneForTable's *general* (non-empty-tenants) path — the one Bug A
// falls through to once at least one table exists — carries forward
// catalog-backed state (views, RBAC, scheduled jobs) once its output
// becomes the new live DB. It builds `out := NewDB()` and copies only
// .wal, never .catalog, so any catalog lazily created on the shadow after
// the swap starts empty. This documents whether that gap is real; it is
// not required to pass for Bug A/B to be considered fixed.
func TestModeWALAutocommitWriteDropsCatalogViaShallowClone(t *testing.T) {
	dir := t.TempDir()
	dsn := "file:" + filepath.Join(dir, "walbugC") + "?tenant=default&mode=wal"

	db, err := sql.Open("tinysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(`CREATE TABLE t (id INT)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO t VALUES (1)`); err != nil {
		t.Fatalf("insert 1: %v", err)
	}
	if _, err := db.Exec(`CREATE VIEW v AS SELECT id FROM t`); err != nil {
		t.Fatalf("create view: %v", err)
	}
	// One more autocommit write after the view exists, going through
	// ShallowCloneForTable's general (non-empty-tenants) path.
	if _, err := db.Exec(`INSERT INTO t VALUES (2)`); err != nil {
		t.Fatalf("insert 2: %v", err)
	}

	var cnt int
	if err := db.QueryRow(`SELECT COUNT(*) AS c FROM v`).Scan(&cnt); err != nil {
		t.Fatalf("query view after subsequent write: %v", err)
	}
	if cnt != 2 {
		t.Fatalf("view lost rows after a later autocommit write dropped the catalog: got %d, want 2", cnt)
	}
}
