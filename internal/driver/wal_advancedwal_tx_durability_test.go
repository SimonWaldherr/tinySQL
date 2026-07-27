// Regression tests for Bug B's ambient-WAL-transaction mechanism
// (storage.DB.BeginAmbientWALTx/CommitAmbientWALTx/AbortAmbientWALTx, wired
// up in conn.execStatement/commitTx/rollbackTx) specifically under
// mode=advancedwal.
//
// The existing wal_shallowclone_bug_test.go and wal_update_delta_test.go
// files drive every explicit-transaction scenario through mode=wal only.
// ModeAdvancedWAL logs row operations as they happen rather than at commit,
// so it needs its own ambient transaction grouping (see DB.BeginAmbientWALTx)
// to get the same all-or-nothing guarantee across a restart, and that path
// was otherwise untested end-to-end through database/sql.
package driver

import (
	"database/sql"
	"path/filepath"
	"testing"
)

func advancedWALDSN(t *testing.T, name string) string {
	t.Helper()
	return "file:" + filepath.Join(t.TempDir(), name) + "?tenant=default&mode=advancedwal"
}

// TestAdvancedWALExplicitTransactionRollbackLeavesNoTrace mirrors
// TestModeWALExplicitTransactionRollbackLeavesWALEntry but for
// mode=advancedwal: a multi-statement BEGIN...ROLLBACK block must leave no
// trace in the AdvancedWAL after a simulated restart. Without ambient
// transaction grouping, each statement inside the block is logged and
// immediately committed as its own AdvancedWAL transaction the instant it
// runs, so recovery would replay it regardless of the later ROLLBACK.
func TestAdvancedWALExplicitTransactionRollbackLeavesNoTrace(t *testing.T) {
	dsn := advancedWALDSN(t, "advwal_rollback")

	db, err := sql.Open("tinysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	setupTx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin (setup): %v", err)
	}
	if _, err := setupTx.Exec(`CREATE TABLE t (id INT, val TEXT)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := setupTx.Exec(`INSERT INTO t VALUES (1, 'keep')`); err != nil {
		t.Fatalf("seed insert: %v", err)
	}
	if err := setupTx.Commit(); err != nil {
		t.Fatalf("commit (setup): %v", err)
	}

	// A multi-statement transaction that is rolled back, not committed.
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := tx.Exec(`INSERT INTO t VALUES (99, 'should-not-survive')`); err != nil {
		t.Fatalf("insert in tx: %v", err)
	}
	if _, err := tx.Exec(`UPDATE t SET val = 'mutated' WHERE id = 1`); err != nil {
		t.Fatalf("update in tx: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened, err := sql.Open("tinysql", dsn)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()

	var count99 int
	if err := reopened.QueryRow(`SELECT COUNT(*) AS c FROM t WHERE id = 99`).Scan(&count99); err != nil {
		t.Fatalf("count after reopen: %v", err)
	}
	if count99 != 0 {
		t.Fatalf("rolled-back INSERT resurrected by AdvancedWAL replay after restart (got %d matching rows, want 0)", count99)
	}

	var keptVal string
	if err := reopened.QueryRow(`SELECT val FROM t WHERE id = 1`).Scan(&keptVal); err != nil {
		t.Fatalf("query surviving row after reopen: %v", err)
	}
	if keptVal != "keep" {
		t.Fatalf("rolled-back UPDATE resurrected by AdvancedWAL replay after restart: got val=%q, want %q", keptVal, "keep")
	}
}

// TestAdvancedWALExplicitTransactionCommitPersistsAllStatements is the
// commit-side counterpart: it guards against a fix for the rollback bug that
// over-corrects by suppressing ambient-transaction logging altogether, which
// would silently drop every statement of a committed multi-statement
// transaction instead of just the rolled-back ones.
func TestAdvancedWALExplicitTransactionCommitPersistsAllStatements(t *testing.T) {
	dsn := advancedWALDSN(t, "advwal_commit")

	db, err := sql.Open("tinysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := tx.Exec(`CREATE TABLE t (id INT, val TEXT)`); err != nil {
		t.Fatalf("create table in tx: %v", err)
	}
	if _, err := tx.Exec(`INSERT INTO t VALUES (1, 'a')`); err != nil {
		t.Fatalf("insert 1 in tx: %v", err)
	}
	if _, err := tx.Exec(`INSERT INTO t VALUES (2, 'b')`); err != nil {
		t.Fatalf("insert 2 in tx: %v", err)
	}
	if _, err := tx.Exec(`UPDATE t SET val = 'a2' WHERE id = 1`); err != nil {
		t.Fatalf("update in tx: %v", err)
	}
	if _, err := tx.Exec(`DELETE FROM t WHERE id = 2`); err != nil {
		t.Fatalf("delete in tx: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened, err := sql.Open("tinysql", dsn)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()

	var count int
	if err := reopened.QueryRow(`SELECT COUNT(*) AS c FROM t`).Scan(&count); err != nil {
		t.Fatalf("count after reopen: %v", err)
	}
	if count != 1 {
		t.Fatalf("committed transaction did not survive restart intact: got %d rows, want 1 (id=2 deleted, id=1 kept)", count)
	}

	var val string
	if err := reopened.QueryRow(`SELECT val FROM t WHERE id = 1`).Scan(&val); err != nil {
		t.Fatalf("query row 1 after reopen: %v", err)
	}
	if val != "a2" {
		t.Fatalf("committed UPDATE lost across restart: got val=%q, want %q", val, "a2")
	}
}
