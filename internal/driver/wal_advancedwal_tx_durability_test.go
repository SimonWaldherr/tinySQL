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
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func advancedWALDSN(t *testing.T, name string) string {
	t.Helper()
	return "file:" + filepath.Join(t.TempDir(), name) + "?tenant=default&mode=advancedwal"
}

func TestAdvancedWALRollbackOnlyCommit(t *testing.T) {
	for _, api := range []string{"sql.Tx", "SQL COMMIT"} {
		t.Run(api, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			const suffix = "/db?mode=advanced_wal"
			db, err := sql.Open("tinysql", "file:"+dir+suffix)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			conn, err := db.Conn(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			if _, err := conn.ExecContext(ctx, "CREATE TABLE t (id INT PRIMARY KEY, val TEXT)"); err != nil {
				t.Fatal(err)
			}

			exec := conn.ExecContext
			commit := func() error {
				_, err := exec(ctx, "COMMIT")
				return err
			}
			if api == "sql.Tx" {
				tx, err := conn.BeginTx(ctx, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Rollback()
				exec, commit = tx.ExecContext, tx.Commit
			} else if _, err := exec(ctx, "BEGIN"); err != nil {
				t.Fatal(err)
			}
			for _, stmt := range []string{"INSERT INTO t VALUES (1, 'pending')", "SAVEPOINT s"} {
				if _, err := exec(ctx, stmt); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := exec(ctx, "INSERT INTO t VALUES (2, 'first'), (2, 'duplicate')"); !errors.Is(err, storage.ErrAmbientWALRollbackOnly) {
				t.Fatalf("partial statement error = %v", err)
			}
			for _, stmt := range []string{"ROLLBACK TO s", "INSERT INTO t VALUES (3, 'later')"} {
				if _, err := exec(ctx, stmt); !errors.Is(err, storage.ErrAmbientWALRollbackOnly) {
					t.Fatalf("%s after failed write: %v", stmt, err)
				}
			}
			if err := commit(); !errors.Is(err, storage.ErrAmbientWALRollbackOnly) {
				t.Fatalf("commit error = %v", err)
			}

			// Exercise the same connection and flush the aborted group's WAL
			// records before reopening a copy of the durable files.
			if _, err := conn.ExecContext(ctx, "INSERT INTO t VALUES (4, 'committed')"); err != nil {
				t.Fatal(err)
			}
			want := []string{"4:committed"}
			if got := savepointRows(t, db); !slices.Equal(got, want) {
				t.Fatalf("live rows = %v, want %v", got, want)
			}
			crashDir := t.TempDir()
			copyDBFilesForCrash(t, dir, crashDir)
			reopened, err := sql.Open("tinysql", "file:"+crashDir+suffix)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			if got := savepointRows(t, reopened); !slices.Equal(got, want) {
				t.Fatalf("recovered rows = %v, want %v", got, want)
			}
		})
	}
}

func TestAdvancedWALStatementFailureBeforeLoggingAllowsCommit(t *testing.T) {
	runSavepointDurabilityScenario(t, "advancedwal",
		[]string{"CREATE TABLE t (id INT, val TEXT)"},
		[]savepointStep{
			{sql: "INSERT INTO t VALUES (1, 'first')"},
			{sql: "INSERT INTO missing_table VALUES (2)", wantErr: "missing_table"},
			{sql: "INSERT INTO t VALUES (3, 'last')"},
		},
		[]string{"1:first", "3:last"})
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

// TestAdvancedWALExplicitTransactionSchemaOnlyCommitCheckpointsLiveDB pins the
// shadow-transaction case for the DDL durability barrier. A CREATE TABLE has
// no row-level AdvancedWAL record, so it cannot wait for the normal row-count
// checkpoint trigger; COMMIT must publish a checkpoint of the live DB before
// it returns, not merely enqueue one for a later scheduler turn.
func TestAdvancedWALExplicitTransactionSchemaOnlyCommitCheckpointsLiveDB(t *testing.T) {
	path := filepath.Join(t.TempDir(), "advwal_schema_only")
	dsn := "file:" + path + "?tenant=default&mode=advancedwal"
	db, err := sql.Open("tinysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := tx.Exec(`CREATE TABLE schema_only (id INT, label TEXT)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if info, err := os.Stat(path + ".checkpoint"); err != nil || info.Size() == 0 {
		t.Fatalf("schema-only COMMIT returned before a live checkpoint existed: info=%v err=%v", info, err)
	}
}

// TestAdvancedWALRolledBackSchemaDoesNotCheckpointLiveDB makes the other half
// of the shadow-DDL durability contract explicit. A shadow has to remember
// that committed DDL needs a live checkpoint, but publishing that fact to the
// shared WAL while the transaction is still reversible would force replicas
// to re-bootstrap after an otherwise harmless ROLLBACK.
func TestAdvancedWALRolledBackSchemaDoesNotCheckpointLiveDB(t *testing.T) {
	path := filepath.Join(t.TempDir(), "advwal_schema_rollback")
	dsn := "file:" + path + "?tenant=default&mode=advancedwal"
	db, err := sql.Open("tinysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if _, err := os.Stat(path + ".checkpoint"); !os.IsNotExist(err) {
		t.Fatalf("fresh advanced-WAL database unexpectedly has checkpoint: %v", err)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := tx.Exec(`CREATE TABLE rolled_back_schema (id INT)`); err != nil {
		t.Fatalf("create table in transaction: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback: %v", err)
	}

	// Give the scheduler more than one prompt wake-up opportunity. With the
	// old shared RequestCheckpoint call, the rollback's abort wake-up raced the
	// scheduler into writing a snapshot; the shadow-local flag leaves no work
	// that reaches ShouldCheckpoint at all.
	deadline := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline) {
		if info, err := os.Stat(path + ".checkpoint"); err == nil {
			t.Fatalf("rolled-back schema change triggered a live checkpoint: size=%d", info.Size())
		} else if !os.IsNotExist(err) {
			t.Fatalf("stat checkpoint after rollback: %v", err)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
