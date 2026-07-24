package storage

import (
	"os"
	"path/filepath"
	"strings"
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

// TestPagedIndexSync_FlushesPoolResidentMutations is the ModePagedIndex
// analogue of TestHybridSync_FlushesPoolResidentMutations. PagedIndexBackend
// stores every table in one shared paged file (no per-table file to stat),
// so this only checks the reopen roundtrip, not file mtime/size.
func TestPagedIndexSync_FlushesPoolResidentMutations(t *testing.T) {
	dir := t.TempDir()

	db1, err := OpenDB(StorageConfig{Mode: ModePagedIndex, Path: dir, MaxMemoryBytes: 8 * 1024 * 1024})
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

	// Session 2: db.tenants starts empty, so Get() must lazily load the
	// table through PagedIndexBackend's own table pool (not db.tenants).
	db2, err := OpenDB(StorageConfig{Mode: ModePagedIndex, Path: dir, MaxMemoryBytes: 8 * 1024 * 1024})
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
	if err := db2.Close(); err != nil {
		t.Fatal(err)
	}

	db3, err := OpenDB(StorageConfig{Mode: ModePagedIndex, Path: dir, MaxMemoryBytes: 8 * 1024 * 1024})
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

// TestPagedIndexBackend_LoadTableReusesPooledMutation guards a related, even
// more immediate variant of the same defect: before PagedIndexBackend had a
// table-object pool, LoadTable always decoded a brand-new *Table from the
// pager, so a mutation applied to a previously returned lease was lost on
// the very next LoadTable call in the same process — no restart needed.
func TestPagedIndexBackend_LoadTableReusesPooledMutation(t *testing.T) {
	dir := t.TempDir()
	b, err := NewPagedIndexBackend(dir, 8*1024*1024, false)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = b.Close() }()
	tbl := NewTable("widgets", []Column{{Name: "id", Type: IntType}}, false)
	if err := b.SaveTable("default", tbl); err != nil {
		t.Fatal(err)
	}

	loaded, err := b.LoadTable("default", "widgets")
	if err != nil {
		t.Fatal(err)
	}
	loaded.Rows = append(loaded.Rows, []any{1})
	loaded.Version++

	again, err := b.LoadTable("default", "widgets")
	if err != nil {
		t.Fatal(err)
	}
	if len(again.Rows) != 1 {
		t.Fatalf("mutation on a loaded table lease was lost on the very next LoadTable in the same process: got %d rows, want 1", len(again.Rows))
	}
}

// TestHybridBackend_EvictionFlushesDirtyTableBeforeDropping forces the
// buffer pool to evict a table under memory pressure while it is still
// dirty (mutated but never explicitly saved) and proves the eviction itself
// flushes it to disk first — the second, related risk called out alongside
// the Sync gap: BufferPool.evictLRU must never drop the only surviving
// reference to an unflushed mutation.
func TestHybridBackend_EvictionFlushesDirtyTableBeforeDropping(t *testing.T) {
	dir := t.TempDir()
	const budget = 20 * 1024

	db1, err := OpenDB(StorageConfig{Mode: ModeHybrid, Path: dir, MaxMemoryBytes: budget})
	if err != nil {
		t.Fatal(err)
	}
	alpha := makeTestTable("alpha", 400)
	bravo := makeTestTable("bravo", 400)
	if err := db1.Put("default", alpha); err != nil {
		t.Fatal(err)
	}
	if err := db1.Put("default", bravo); err != nil {
		t.Fatal(err)
	}
	if err := db1.Close(); err != nil {
		t.Fatal(err)
	}

	// Session 2: the memory budget fits only one of the two tables' row
	// data at a time, so loading bravo right after mutating alpha must
	// evict alpha from the pool while it is still dirty.
	db2, err := OpenDB(StorageConfig{Mode: ModeHybrid, Path: dir, MaxMemoryBytes: budget})
	if err != nil {
		t.Fatal(err)
	}
	loadedAlpha, err := db2.Get("default", "alpha")
	if err != nil {
		t.Fatal(err)
	}
	loadedAlpha.Rows = append(loadedAlpha.Rows, []any{9999, "evicted-but-durable", 1.5})
	loadedAlpha.Version++

	if _, err := db2.Get("default", "bravo"); err != nil {
		t.Fatal(err)
	}

	hb, ok := db2.backend.(*HybridBackend)
	if !ok {
		t.Fatalf("expected *HybridBackend, got %T", db2.backend)
	}
	for _, ref := range hb.PooledTables() {
		if strings.EqualFold(ref.Table.Name, "alpha") {
			t.Fatal("test setup didn't trigger eviction of alpha; adjust table size or MaxMemoryBytes")
		}
	}

	if err := db2.Close(); err != nil {
		t.Fatal(err)
	}

	db3, err := OpenDB(StorageConfig{Mode: ModeHybrid, Path: dir, MaxMemoryBytes: budget})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db3.Close() }()
	reloaded, err := db3.Get("default", "alpha")
	if err != nil {
		t.Fatal(err)
	}
	if len(reloaded.Rows) != 401 {
		t.Fatalf("eviction dropped an unflushed mutation: expected 401 rows, got %d", len(reloaded.Rows))
	}
}
