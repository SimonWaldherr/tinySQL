// Durability tests for DDL on the direct engine.Execute path in ModeWAL.
//
// WAL logging used to be reachable only from a statement's atomic-DML rollback
// snapshot, which exists for INSERT/UPDATE/DELETE and nothing else. Every DDL
// statement therefore produced no WAL record at all for callers using the
// public tinysql.Execute API rather than database/sql: a committed DROP TABLE
// was undone on the next open, because the log still held the records that
// built the table, and a CREATE TABLE with no rows yet left the log empty.
//
// The engine now diffs a metadata pre-image (storage.DB.MetaSnapshot) instead,
// which sees created and dropped tables as well as changed ones.
package engine

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func walDDLDB(t *testing.T, path string) *storage.DB {
	t.Helper()
	cfg := storage.DefaultStorageConfig(storage.ModeWAL)
	cfg.Path = path
	db, err := storage.OpenDB(cfg)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	return db
}

func execWAL(t *testing.T, db *storage.DB, tenant, sql string) {
	t.Helper()
	stmt, err := NewParser(sql).ParseStatement()
	if err != nil {
		t.Fatalf("parse %q: %v", sql, err)
	}
	if _, err := Execute(context.Background(), db, tenant, stmt); err != nil {
		t.Fatalf("exec %q: %v", sql, err)
	}
}

func countWAL(t *testing.T, db *storage.DB, tenant, sql string) int {
	t.Helper()
	stmt, err := NewParser(sql).ParseStatement()
	if err != nil {
		t.Fatalf("parse %q: %v", sql, err)
	}
	rs, err := Execute(context.Background(), db, tenant, stmt)
	if err != nil {
		t.Fatalf("exec %q: %v", sql, err)
	}
	return len(rs.Rows)
}

// TestWALDropTableIsDurableViaEngineExecute pins the worst shape: a DROP that
// the log never learned about, so recovery rebuilt the table from the records
// that had created and filled it.
func TestWALDropTableIsDurableViaEngineExecute(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ddldrop")

	db := walDDLDB(t, path)
	execWAL(t, db, "public", `CREATE TABLE secrets (id INT)`)
	execWAL(t, db, "public", `INSERT INTO secrets VALUES (42)`)
	execWAL(t, db, "public", `DROP TABLE secrets`)
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened := walDDLDB(t, path)
	defer reopened.Close()
	if _, err := reopened.Get("public", "secrets"); err == nil {
		rows := countWAL(t, reopened, "public", `SELECT id FROM secrets`)
		t.Fatalf("dropped table resurrected after restart with %d row(s)", rows)
	}
}

// TestWALCreateTableIsDurableViaEngineExecute covers a schema-only change: a
// table created but never written to still has to survive a restart.
func TestWALCreateTableIsDurableViaEngineExecute(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ddlcreate")

	db := walDDLDB(t, path)
	execWAL(t, db, "public", `CREATE TABLE only_schema (id INT, label TEXT)`)
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened := walDDLDB(t, path)
	defer reopened.Close()
	table, err := reopened.Get("public", "only_schema")
	if err != nil {
		t.Fatalf("CREATE TABLE not durable, table missing after restart: %v", err)
	}
	if len(table.Cols) != 2 {
		t.Errorf("recovered table has %d columns, want 2", len(table.Cols))
	}
}

// TestWALCreateThenInsertThenDropOtherTable checks that a DDL record does not
// disturb an unrelated table's rows.
func TestWALCreateThenInsertThenDropOtherTable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ddlmixed")

	db := walDDLDB(t, path)
	execWAL(t, db, "public", `CREATE TABLE keep (id INT)`)
	execWAL(t, db, "public", `CREATE TABLE gone (id INT)`)
	execWAL(t, db, "public", `INSERT INTO keep VALUES (1)`)
	execWAL(t, db, "public", `INSERT INTO gone VALUES (2)`)
	execWAL(t, db, "public", `INSERT INTO keep VALUES (3)`)
	execWAL(t, db, "public", `DROP TABLE gone`)
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened := walDDLDB(t, path)
	defer reopened.Close()
	if _, err := reopened.Get("public", "gone"); err == nil {
		t.Error("dropped table resurrected after restart")
	}
	if got := countWAL(t, reopened, "public", `SELECT id FROM keep`); got != 2 {
		t.Errorf("keep has %d rows after restart, want 2", got)
	}
}
