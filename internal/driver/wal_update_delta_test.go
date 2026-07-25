// Durability tests for the WAL's in-place UPDATE delta record.
//
// An UPDATE used to force a whole-table WAL record: changing one row of a
// 10,000-row table serialized and fsynced all 10,000. WALManager now logs only
// the rows storage.Table.MarkRowUpdated reported, positioned by index, and
// falls back to the whole table whenever the shape of the change cannot be
// described that way. These tests pin down that the delta survives a restart in
// each of the shapes that matter, since a delta that replays incorrectly is
// silent corruption rather than a visible failure.
package driver

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
)

// reopenWAL closes db and reopens the same DSN, forcing recovery to rebuild the
// database from the checkpoint plus whatever the WAL durably holds.
func reopenWAL(t *testing.T, db *sql.DB, dsn string) *sql.DB {
	t.Helper()
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	reopened, err := sql.Open("tinysql", dsn)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if err := reopened.Ping(); err != nil {
		t.Fatalf("ping after reopen: %v", err)
	}
	return reopened
}

func walDSN(t *testing.T, name string) string {
	t.Helper()
	return "file:" + filepath.Join(t.TempDir(), name) + "?tenant=default&mode=wal"
}

func seedWALTable(t *testing.T, db *sql.DB, rows int) {
	t.Helper()
	if _, err := db.Exec(`CREATE TABLE t (id INT, val TEXT, n INT)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin seed: %v", err)
	}
	for i := 0; i < rows; i++ {
		if _, err := tx.Exec(`INSERT INTO t VALUES (?, ?, ?)`, i, fmt.Sprintf("v%d", i), i); err != nil {
			_ = tx.Rollback()
			t.Fatalf("seed %d: %v", i, err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit seed: %v", err)
	}
}

func rowVal(t *testing.T, db *sql.DB, id int) string {
	t.Helper()
	var val string
	if err := db.QueryRow(`SELECT val FROM t WHERE id = ?`, id).Scan(&val); err != nil {
		t.Fatalf("select id=%d: %v", id, err)
	}
	return val
}

// TestWALUpdateDeltaSurvivesRestart is the base case: a single-row UPDATE in a
// table large enough that a delta and a whole-table record differ, then a
// restart.
func TestWALUpdateDeltaSurvivesRestart(t *testing.T) {
	dsn := walDSN(t, "updelta")
	db, err := sql.Open("tinysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	seedWALTable(t, db, 200)

	if _, err := db.Exec(`UPDATE t SET val = 'updated' WHERE id = ?`, 137); err != nil {
		t.Fatalf("update: %v", err)
	}

	reopened := reopenWAL(t, db, dsn)
	defer reopened.Close()

	if got := rowVal(t, reopened, 137); got != "updated" {
		t.Errorf("row 137 val = %q after restart, want %q", got, "updated")
	}
	// Every other row must be intact: a delta that replaced the table with only
	// its updated rows would lose them.
	var count int
	if err := reopened.QueryRow(`SELECT COUNT(*) AS c FROM t`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 200 {
		t.Errorf("row count = %d after restart, want 200", count)
	}
	if got := rowVal(t, reopened, 0); got != "v0" {
		t.Errorf("row 0 val = %q after restart, want %q", got, "v0")
	}
	if got := rowVal(t, reopened, 199); got != "v199" {
		t.Errorf("row 199 val = %q after restart, want %q", got, "v199")
	}
}

// TestWALMultiRowUpdateDeltaSurvivesRestart covers an UPDATE matching many rows
// in one statement, so the record carries several positions.
func TestWALMultiRowUpdateDeltaSurvivesRestart(t *testing.T) {
	dsn := walDSN(t, "multidelta")
	db, err := sql.Open("tinysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	seedWALTable(t, db, 100)

	if _, err := db.Exec(`UPDATE t SET val = 'even' WHERE n < 50`); err != nil {
		t.Fatalf("update: %v", err)
	}

	reopened := reopenWAL(t, db, dsn)
	defer reopened.Close()

	var updated int
	if err := reopened.QueryRow(`SELECT COUNT(*) AS c FROM t WHERE val = 'even'`).Scan(&updated); err != nil {
		t.Fatal(err)
	}
	if updated != 50 {
		t.Errorf("updated rows after restart = %d, want 50", updated)
	}
	var total int
	if err := reopened.QueryRow(`SELECT COUNT(*) AS c FROM t`).Scan(&total); err != nil {
		t.Fatal(err)
	}
	if total != 100 {
		t.Errorf("row count = %d after restart, want 100", total)
	}
	if got := rowVal(t, reopened, 99); got != "v99" {
		t.Errorf("row 99 val = %q, want untouched %q", got, "v99")
	}
}

// TestWALUpdateThenInsertSurvivesRestart mixes shapes: an INSERT after an
// UPDATE invalidates the row list, so the fallback whole-table record must
// carry both changes.
func TestWALUpdateThenInsertSurvivesRestart(t *testing.T) {
	dsn := walDSN(t, "upthenins")
	db, err := sql.Open("tinysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	seedWALTable(t, db, 50)

	if _, err := db.Exec(`UPDATE t SET val = 'changed' WHERE id = 10`); err != nil {
		t.Fatalf("update: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO t VALUES (999, 'appended', 999)`); err != nil {
		t.Fatalf("insert: %v", err)
	}

	reopened := reopenWAL(t, db, dsn)
	defer reopened.Close()

	if got := rowVal(t, reopened, 10); got != "changed" {
		t.Errorf("row 10 val = %q after restart, want %q", got, "changed")
	}
	if got := rowVal(t, reopened, 999); got != "appended" {
		t.Errorf("row 999 val = %q after restart, want %q", got, "appended")
	}
	var total int
	if err := reopened.QueryRow(`SELECT COUNT(*) AS c FROM t`).Scan(&total); err != nil {
		t.Fatal(err)
	}
	if total != 51 {
		t.Errorf("row count = %d after restart, want 51", total)
	}
}

// TestWALDeleteThenUpdateSurvivesRestart is the dangerous shape: DELETE moves
// row positions, so a positional delta written afterwards would land on the
// wrong rows. The delete must force the whole-table fallback.
func TestWALDeleteThenUpdateSurvivesRestart(t *testing.T) {
	dsn := walDSN(t, "delthenup")
	db, err := sql.Open("tinysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	seedWALTable(t, db, 20)

	if _, err := db.Exec(`DELETE FROM t WHERE id < 5`); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := db.Exec(`UPDATE t SET val = 'after-delete' WHERE id = 19`); err != nil {
		t.Fatalf("update: %v", err)
	}

	reopened := reopenWAL(t, db, dsn)
	defer reopened.Close()

	var total int
	if err := reopened.QueryRow(`SELECT COUNT(*) AS c FROM t`).Scan(&total); err != nil {
		t.Fatal(err)
	}
	if total != 15 {
		t.Errorf("row count = %d after restart, want 15", total)
	}
	if got := rowVal(t, reopened, 19); got != "after-delete" {
		t.Errorf("row 19 val = %q after restart, want %q", got, "after-delete")
	}
	if got := rowVal(t, reopened, 5); got != "v5" {
		t.Errorf("row 5 val = %q after restart, want untouched %q", got, "v5")
	}
	var deleted int
	if err := reopened.QueryRow(`SELECT COUNT(*) AS c FROM t WHERE id < 5`).Scan(&deleted); err != nil {
		t.Fatal(err)
	}
	if deleted != 0 {
		t.Errorf("%d rows with id < 5 survived the restart, want 0", deleted)
	}
}

// TestWALUpdateInTransactionSurvivesRestart checks the transaction path, where
// the change is logged once at COMMIT from the live database rather than
// per statement.
func TestWALUpdateInTransactionSurvivesRestart(t *testing.T) {
	dsn := walDSN(t, "txupdate")
	db, err := sql.Open("tinysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	seedWALTable(t, db, 80)

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`UPDATE t SET val = 'tx' WHERE id = 42`); err != nil {
		_ = tx.Rollback()
		t.Fatalf("update in tx: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	reopened := reopenWAL(t, db, dsn)
	defer reopened.Close()

	if got := rowVal(t, reopened, 42); got != "tx" {
		t.Errorf("row 42 val = %q after restart, want %q", got, "tx")
	}
	var total int
	if err := reopened.QueryRow(`SELECT COUNT(*) AS c FROM t`).Scan(&total); err != nil {
		t.Fatal(err)
	}
	if total != 80 {
		t.Errorf("row count = %d after restart, want 80", total)
	}
}

// TestWALRepeatedUpdatesSurviveRestart applies many separate UPDATE statements,
// each its own logged transaction, and checks that replaying them in order
// reproduces the final state.
func TestWALRepeatedUpdatesSurviveRestart(t *testing.T) {
	dsn := walDSN(t, "repeated")
	db, err := sql.Open("tinysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	seedWALTable(t, db, 30)

	for i := 0; i < 30; i += 3 {
		if _, err := db.Exec(`UPDATE t SET val = ? WHERE id = ?`, fmt.Sprintf("u%d", i), i); err != nil {
			t.Fatalf("update %d: %v", i, err)
		}
	}
	// Overwrite one of them again, so replay order matters.
	if _, err := db.Exec(`UPDATE t SET val = 'final' WHERE id = 0`); err != nil {
		t.Fatalf("final update: %v", err)
	}

	reopened := reopenWAL(t, db, dsn)
	defer reopened.Close()

	if got := rowVal(t, reopened, 0); got != "final" {
		t.Errorf("row 0 val = %q after restart, want %q", got, "final")
	}
	for i := 3; i < 30; i += 3 {
		want := fmt.Sprintf("u%d", i)
		if got := rowVal(t, reopened, i); got != want {
			t.Errorf("row %d val = %q after restart, want %q", i, got, want)
		}
	}
	for i := 1; i < 30; i += 3 {
		want := fmt.Sprintf("v%d", i)
		if got := rowVal(t, reopened, i); got != want {
			t.Errorf("row %d val = %q after restart, want untouched %q", i, got, want)
		}
	}
}
