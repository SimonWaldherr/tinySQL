package tinysql_test

import (
	"context"
	"path/filepath"
	"testing"

	tsql "github.com/SimonWaldherr/tinySQL"
)

// TestReadOnlyReadmeExample exercises the exact load -> close -> reopen
// read-only -> warm cycle documented in the README's "Read-only mode for
// bulk-load / serve-only workloads" section, so the example stays accurate
// as the public API evolves.
func TestReadOnlyReadmeExample(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "db.gob")

	// Load phase: in-memory, with a path so Close() writes a full snapshot.
	// (ModeWAL/ModeAdvancedWAL do not currently log tsql.Execute mutations
	// automatically — see the README caveat under "Read-only mode".)
	db, err := tsql.OpenDB(tsql.StorageConfig{Mode: tsql.ModeMemory, Path: dbPath})
	if err != nil {
		t.Fatalf("open read-write: %v", err)
	}
	ctx := context.Background()
	for _, sql := range []string{
		`CREATE TABLE docs (id INT, body TEXT, embedding VECTOR)`,
		`INSERT INTO docs VALUES (1, 'hello world', '[0.1, 0.2, 0.3]')`,
		`INSERT INTO docs VALUES (2, 'goodbye world', '[0.4, 0.5, 0.6]')`,
	} {
		stmt, err := tsql.ParseSQL(sql)
		if err != nil {
			t.Fatalf("parse %q: %v", sql, err)
		}
		if _, err := tsql.Execute(ctx, db, "default", stmt); err != nil {
			t.Fatalf("execute %q: %v", sql, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close read-write db: %v", err)
	}

	// Serve phase: read-only, loaded from the same snapshot.
	serveDB, err := tsql.OpenDB(tsql.StorageConfig{Mode: tsql.ModeMemory, Path: dbPath, ReadOnly: true})
	if err != nil {
		t.Fatalf("open read-only: %v", err)
	}
	defer serveDB.Close()
	if !serveDB.IsReadOnly() {
		t.Fatal("expected IsReadOnly() = true")
	}

	warmStmt, err := tsql.ParseSQL(`SELECT * FROM VEC_WARM('docs', 'embedding', 'cosine', 'hnsw')`)
	if err != nil {
		t.Fatalf("parse VEC_WARM: %v", err)
	}
	rs, err := tsql.Execute(ctx, serveDB, "default", warmStmt)
	if err != nil {
		t.Fatalf("execute VEC_WARM: %v", err)
	}
	if len(rs.Rows) != 1 {
		t.Fatalf("expected 1 warm-up stats row, got %d", len(rs.Rows))
	}

	// A write must be rejected in read-only mode.
	insertStmt, _ := tsql.ParseSQL(`INSERT INTO docs VALUES (3, 'nope', '[0,0,0]')`)
	if _, err := tsql.Execute(ctx, serveDB, "default", insertStmt); err == nil {
		t.Fatal("expected write to be rejected in read-only mode")
	}

	// Reads (including the now-warmed vector search) still work.
	searchStmt, _ := tsql.ParseSQL(`SELECT id FROM VEC_SEARCH('docs', 'embedding', '[0.1, 0.2, 0.3]', 1, 'cosine', 'hnsw')`)
	rs, err = tsql.Execute(ctx, serveDB, "default", searchStmt)
	if err != nil {
		t.Fatalf("execute VEC_SEARCH: %v", err)
	}
	if len(rs.Rows) != 1 {
		t.Fatalf("expected 1 search result, got %d", len(rs.Rows))
	}
}
