package storage

import (
	"os"
	"path/filepath"
	"testing"
)

// These tests guard a confirmed data-loss bug: for the evictable storage
// modes (ModeHybrid, ModeIndex, ModePagedIndex) DB.Get returns a table that
// is loaded lazily from the backend without being registered in db.tenants
// (see DB.backendTablesEvictable). A mutation applied in place on that
// returned *Table (exactly what the SQL engine's INSERT/UPDATE/DELETE do)
// was invisible to DB.Sync/DB.Close, which only walked db.tenants — so
// Sync()/Close() returned nil while silently never writing the mutated rows
// to disk. A process restart then lost every write made since the table was
// last resident in db.tenants.

func tablePathFor(dir, tenant, name string) string {
	return filepath.Join(dir, tenant, name+".tbl")
}

// TestHybridSync_FlushesPoolResidentMutations reproduces the bug for
// ModeHybrid: a table loaded lazily in a fresh session (not present in
// db.tenants) is mutated in place, then Sync() must actually flush it.
func TestHybridSync_FlushesPoolResidentMutations(t *testing.T) {
	dir := t.TempDir()

	// Session 1: create the table so it exists on disk, then close. This
	// empties db.tenants for the next session.
	db1, err := OpenDB(StorageConfig{Mode: ModeHybrid, Path: dir, MaxMemoryBytes: 8 * 1024 * 1024})
	if err != nil {
		t.Fatal(err)
	}
	tbl := NewTable("widgets", []Column{{Name: "id", Type: IntType}}, false)
	if err := db1.Put("default", tbl); err != nil {
		t.Fatal(err)
	}
	if err := db1.Close(); err != nil {
		t.Fatal(err)
	}

	path := tablePathFor(dir, "default", "widgets")
	before, err := os.Stat(path)
	if err != nil {
		t.Fatalf("table file missing after session 1: %v", err)
	}

	// Session 2: db.tenants starts empty, so Get() must lazily load the
	// table through the backend's own evictable pool (not db.tenants).
	db2, err := OpenDB(StorageConfig{Mode: ModeHybrid, Path: dir, MaxMemoryBytes: 8 * 1024 * 1024})
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := db2.Get("default", "widgets")
	if err != nil {
		t.Fatal(err)
	}
	loaded.Rows = append(loaded.Rows, []any{1})
	loaded.Version++

	if err := db2.Sync(); err != nil {
		t.Fatalf("Sync reported an error: %v", err)
	}

	after, err := os.Stat(path)
	if err != nil {
		t.Fatalf("table file missing after Sync: %v", err)
	}
	if after.Size() == before.Size() && after.ModTime().Equal(before.ModTime()) {
		t.Fatalf("table file on disk was not touched by Sync(): the INSERT was never flushed despite Sync() returning nil")
	}

	if err := db2.Close(); err != nil {
		t.Fatal(err)
	}

	// Session 3: reopen and verify the inserted row actually survived.
	db3, err := OpenDB(StorageConfig{Mode: ModeHybrid, Path: dir, MaxMemoryBytes: 8 * 1024 * 1024})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db3.Close() }()
	reloaded, err := db3.Get("default", "widgets")
	if err != nil {
		t.Fatal(err)
	}
	if len(reloaded.Rows) != 1 {
		t.Fatalf("data loss: expected 1 row after reopen, got %d", len(reloaded.Rows))
	}
}

// TestIndexSync_FlushesPoolResidentMutations is the ModeIndex analogue of
// TestHybridSync_FlushesPoolResidentMutations.
func TestIndexSync_FlushesPoolResidentMutations(t *testing.T) {
	dir := t.TempDir()

	db1, err := OpenDB(StorageConfig{Mode: ModeIndex, Path: dir, MaxMemoryBytes: 8 * 1024 * 1024})
	if err != nil {
		t.Fatal(err)
	}
	tbl := NewTable("widgets", []Column{{Name: "id", Type: IntType}}, false)
	if err := db1.Put("default", tbl); err != nil {
		t.Fatal(err)
	}
	if err := db1.Close(); err != nil {
		t.Fatal(err)
	}

	path := tablePathFor(dir, "default", "widgets")
	before, err := os.Stat(path)
	if err != nil {
		t.Fatalf("table file missing after session 1: %v", err)
	}

	db2, err := OpenDB(StorageConfig{Mode: ModeIndex, Path: dir, MaxMemoryBytes: 8 * 1024 * 1024})
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := db2.Get("default", "widgets")
	if err != nil {
		t.Fatal(err)
	}
	loaded.Rows = append(loaded.Rows, []any{1})
	loaded.Version++

	if err := db2.Sync(); err != nil {
		t.Fatalf("Sync reported an error: %v", err)
	}

	after, err := os.Stat(path)
	if err != nil {
		t.Fatalf("table file missing after Sync: %v", err)
	}
	if after.Size() == before.Size() && after.ModTime().Equal(before.ModTime()) {
		t.Fatalf("table file on disk was not touched by Sync(): the INSERT was never flushed despite Sync() returning nil")
	}

	if err := db2.Close(); err != nil {
		t.Fatal(err)
	}

	db3, err := OpenDB(StorageConfig{Mode: ModeIndex, Path: dir, MaxMemoryBytes: 8 * 1024 * 1024})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db3.Close() }()
	reloaded, err := db3.Get("default", "widgets")
	if err != nil {
		t.Fatal(err)
	}
	if len(reloaded.Rows) != 1 {
		t.Fatalf("data loss: expected 1 row after reopen, got %d", len(reloaded.Rows))
	}
}
