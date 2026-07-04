// Integration test for automatic AdvancedWAL logging (wal_logging.go).
// Before this, nothing in the INSERT/UPDATE/DELETE execution path ever
// called AdvancedWAL's LogInsert/LogUpdate/LogDelete — a caller using
// ModeAdvancedWAL and writing through the ordinary Execute API got no
// durability logging at all. This test proves the fix by simulating an
// actual crash: writes go through Execute() only (no manual Log* calls),
// the WAL is closed without any checkpoint, and a *separate* fresh DB is
// recovered purely from the WAL file — the same shape as TestAdvancedWALRecovery
// in internal/storage, but exercised through the real engine entry point.
package engine

import (
	"path/filepath"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestAdvancedWALAutoLoggingSurvivesSimulatedCrash(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "auto.wal")

	// --- "Live" session: write through Execute only, then crash (no checkpoint). ---
	func() {
		wal, err := storage.OpenAdvancedWAL(storage.AdvancedWALConfig{Path: walPath})
		if err != nil {
			t.Fatalf("open WAL: %v", err)
		}
		defer wal.Close()

		db := storage.NewDB()
		db.AttachAdvancedWAL(wal)

		execSQL(t, db, `CREATE TABLE users (id INT, name TEXT, score FLOAT64)`)
		execSQL(t, db, `INSERT INTO users VALUES (1, 'Alice', 10.0)`)
		execSQL(t, db, `INSERT INTO users VALUES (2, 'Bob', 20.0)`)
		execSQL(t, db, `INSERT INTO users VALUES (3, 'Carol', 30.0)`)
		execSQL(t, db, `UPDATE users SET score = 99.0 WHERE id = 2`)
		execSQL(t, db, `DELETE FROM users WHERE id = 3`)
		// No checkpoint, no explicit Sync/Close of db — simulates a crash
		// right here, with only the WAL file surviving.
	}()

	// --- "Restart": recover a brand-new DB purely from the WAL file. ---
	recoveredDB := storage.NewDB()
	wal2, err := storage.OpenAdvancedWAL(storage.AdvancedWALConfig{Path: walPath})
	if err != nil {
		t.Fatalf("reopen WAL: %v", err)
	}
	defer wal2.Close()

	if _, err := wal2.Recover(recoveredDB); err != nil {
		t.Fatalf("recover: %v", err)
	}

	rs := execSQL(t, recoveredDB, `SELECT id, name, score FROM users ORDER BY id`)
	if len(rs.Rows) != 2 {
		t.Fatalf("expected 2 surviving rows (Alice, Bob; Carol deleted), got %d: %+v", len(rs.Rows), rs.Rows)
	}
	if got := expectAsInt(t, rs.Rows[0]["id"]); got != 1 {
		t.Errorf("row 0: expected id=1, got %v", got)
	}
	if rs.Rows[0]["name"] != "Alice" {
		t.Errorf("row 0: expected name=Alice, got %v", rs.Rows[0]["name"])
	}
	expectFloat(t, rs.Rows[0]["score"], 10.0, 1e-9, "row 0 score")

	if got := expectAsInt(t, rs.Rows[1]["id"]); got != 2 {
		t.Errorf("row 1: expected id=2, got %v", got)
	}
	if rs.Rows[1]["name"] != "Bob" {
		t.Errorf("row 1: expected name=Bob, got %v", rs.Rows[1]["name"])
	}
	expectFloat(t, rs.Rows[1]["score"], 99.0, 1e-9, "row 1 score (post-UPDATE)")
}

// TestAdvancedWALAutoLoggingNoOpWithoutWAL confirms Execute behaves exactly
// as before when no AdvancedWAL is attached — the overwhelmingly common
// case (ModeMemory/ModeDisk/ModeJSON/ModeHybrid/ModeWAL) must see zero
// behavior change.
func TestAdvancedWALAutoLoggingNoOpWithoutWAL(t *testing.T) {
	db := storage.NewDB() // no AttachAdvancedWAL call
	execSQL(t, db, `CREATE TABLE t (id INT)`)
	execSQL(t, db, `INSERT INTO t VALUES (1)`)
	execSQL(t, db, `UPDATE t SET id = 2 WHERE id = 1`)
	execSQL(t, db, `DELETE FROM t WHERE id = 2`)

	rs := execSQL(t, db, `SELECT COUNT(*) AS cnt FROM t`)
	if got := expectAsInt(t, rs.Rows[0]["cnt"]); got != 0 {
		t.Errorf("expected 0 rows, got %d", got)
	}
}
