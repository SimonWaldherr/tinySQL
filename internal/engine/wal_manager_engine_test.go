// Regression tests for engine.Execute driving the basic WALManager
// (ModeWAL). Before this fix, internal/engine never touched db.WAL() at
// all — only internal/driver's connection wrapper did — so a caller using
// ModeWAL through the primary documented API (tinysql.Execute, and
// tinySQL's own reference cmd/server) got a WAL file and a healthy-looking
// DB.HealthCheck(), but zero durability: every INSERT/UPDATE/DELETE was
// lost on the next open. These tests exercise engine.Execute directly
// (never internal/driver) against a ModeWAL-backed DB and confirm data
// survives a Close + reopen cycle.
package engine

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestModeWALSurvivesReopenViaEngineExecute(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "waldb.gob")
	ctx := context.Background()

	db, err := storage.OpenDB(storage.StorageConfig{Mode: storage.ModeWAL, Path: path})
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	if db.WAL() == nil {
		t.Fatal("WAL should be attached")
	}

	// Only engine.Execute is used here — never internal/driver — to match
	// how tinysql.Execute/ExecSQL and cmd/server reach the engine directly.
	if _, err := Execute(ctx, db, "default", mustParse(`CREATE TABLE t (id INT, name TEXT)`)); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := Execute(ctx, db, "default", mustParse(`INSERT INTO t VALUES (1, 'Alice')`)); err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	if _, err := Execute(ctx, db, "default", mustParse(`INSERT INTO t VALUES (2, 'Bob')`)); err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	if _, err := Execute(ctx, db, "default", mustParse(`UPDATE t SET name = 'Robert' WHERE id = 2`)); err != nil {
		t.Fatalf("UPDATE: %v", err)
	}

	// Sync() is a no-op for ModeWAL (no backend is attached — durability
	// comes entirely from the WAL), and Close() only flushes/closes the WAL
	// file, so this exercises exactly the crash-recovery path: whatever
	// wasn't logged to the WAL is lost.
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := storage.OpenDB(storage.StorageConfig{Mode: storage.ModeWAL, Path: path})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()

	rs, err := Execute(ctx, reopened, "default", mustParse(`SELECT id, name FROM t ORDER BY id`))
	if err != nil {
		t.Fatalf("SELECT after reopen: %v", err)
	}
	if len(rs.Rows) != 2 {
		t.Fatalf("expected 2 rows to survive reopen, got %d: %+v", len(rs.Rows), rs.Rows)
	}
	if rs.Rows[0]["name"] != "Alice" {
		t.Errorf("row0.name = %v, want Alice", rs.Rows[0]["name"])
	}
	if rs.Rows[1]["name"] != "Robert" {
		t.Errorf("row1.name = %v, want Robert (the UPDATE must also survive)", rs.Rows[1]["name"])
	}
}

func TestModeWALDeleteSurvivesReopenViaEngineExecute(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "waldb.gob")
	ctx := context.Background()

	db, err := storage.OpenDB(storage.StorageConfig{Mode: storage.ModeWAL, Path: path})
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}

	Execute(ctx, db, "default", mustParse(`CREATE TABLE t (id INT)`))
	Execute(ctx, db, "default", mustParse(`INSERT INTO t VALUES (1)`))
	Execute(ctx, db, "default", mustParse(`INSERT INTO t VALUES (2)`))
	if _, err := Execute(ctx, db, "default", mustParse(`DELETE FROM t WHERE id = 1`)); err != nil {
		t.Fatalf("DELETE: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := storage.OpenDB(storage.StorageConfig{Mode: storage.ModeWAL, Path: path})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()

	rs, err := Execute(ctx, reopened, "default", mustParse(`SELECT id FROM t`))
	if err != nil {
		t.Fatalf("SELECT after reopen: %v", err)
	}
	if len(rs.Rows) != 1 {
		t.Fatalf("expected 1 surviving row after DELETE+reopen, got %d: %+v", len(rs.Rows), rs.Rows)
	}
	expectInt(t, rs.Rows[0]["id"], 2, "surviving row id")
}
