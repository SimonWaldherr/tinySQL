package storage

import (
	"os"
	"path/filepath"
	"sort"
	"testing"
)

// ───────────────────────────────────────────────────────────────────────────
// Helpers
// ───────────────────────────────────────────────────────────────────────────

func makeTestTable(name string, nrows int) *Table {
	cols := []Column{
		{Name: "id", Type: IntType},
		{Name: "name", Type: StringType},
		{Name: "score", Type: Float64Type},
	}
	t := NewTable(name, cols, false)
	for i := 0; i < nrows; i++ {
		t.Rows = append(t.Rows, []any{i, "row_" + name, float64(i) * 1.1})
	}
	t.Version = nrows
	return t
}

func assertTableEqual(t *testing.T, got, want *Table) {
	t.Helper()
	if got.Name != want.Name {
		t.Errorf("table name: got %q, want %q", got.Name, want.Name)
	}
	if len(got.Cols) != len(want.Cols) {
		t.Fatalf("col count: got %d, want %d", len(got.Cols), len(want.Cols))
	}
	if len(got.Rows) != len(want.Rows) {
		t.Fatalf("row count: got %d, want %d", len(got.Rows), len(want.Rows))
	}
	for i := range got.Rows {
		for j := range got.Rows[i] {
			if got.Rows[i][j] != want.Rows[i][j] {
				t.Errorf("row[%d][%d]: got %v, want %v", i, j, got.Rows[i][j], want.Rows[i][j])
			}
		}
	}
}

// ───────────────────────────────────────────────────────────────────────────
// ParseStorageMode
// ───────────────────────────────────────────────────────────────────────────

func TestParseStorageMode(t *testing.T) {
	tests := []struct {
		input string
		want  StorageMode
		err   bool
	}{
		{"memory", ModeMemory, false},
		{"mem", ModeMemory, false},
		{"ram", ModeMemory, false},
		{"", ModeMemory, false},
		{"wal", ModeWAL, false},
		{"disk", ModeDisk, false},
		{"index", ModeIndex, false},
		{"hybrid", ModeHybrid, false},
		{"unknown", ModeMemory, true},
	}
	for _, tc := range tests {
		got, err := ParseStorageMode(tc.input)
		if (err != nil) != tc.err {
			t.Errorf("ParseStorageMode(%q) error = %v, wantErr %v", tc.input, err, tc.err)
			continue
		}
		if !tc.err && got != tc.want {
			t.Errorf("ParseStorageMode(%q) = %v, want %v", tc.input, got, tc.want)
		}
	}
}

func TestStorageModeString(t *testing.T) {
	modes := []struct {
		m    StorageMode
		want string
	}{
		{ModeMemory, "memory"},
		{ModeWAL, "wal"},
		{ModeDisk, "disk"},
		{ModeIndex, "index"},
		{ModeHybrid, "hybrid"},
	}
	for _, tc := range modes {
		if got := tc.m.String(); got != tc.want {
			t.Errorf("StorageMode(%d).String() = %q, want %q", tc.m, got, tc.want)
		}
	}
}

// ───────────────────────────────────────────────────────────────────────────
// MemoryBackend
// ───────────────────────────────────────────────────────────────────────────

func TestMemoryBackend(t *testing.T) {
	mb := NewMemoryBackend("")
	if mb.Mode() != ModeMemory {
		t.Fatalf("mode: got %v, want %v", mb.Mode(), ModeMemory)
	}

	// LoadTable always returns nil (nothing on disk)
	tbl, err := mb.LoadTable("default", "users")
	if err != nil || tbl != nil {
		t.Fatalf("LoadTable: expected nil, nil; got %v, %v", tbl, err)
	}
	if mb.TableExists("default", "users") {
		t.Fatal("TableExists should be false")
	}
	names, _ := mb.ListTableNames("default")
	if len(names) != 0 {
		t.Fatalf("ListTableNames: expected empty, got %v", names)
	}

	// Sync and Close are no-ops
	if err := mb.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := mb.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestMemoryBackend_SaveOnClose(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.gob")

	// Create a DB with memory backend that saves on close.
	db := NewDB()
	mb := NewMemoryBackend(path)
	mb.setDB(db)
	db.backend = mb

	// Add a table
	tbl := makeTestTable("users", 10)
	if err := db.Put("default", tbl); err != nil {
		t.Fatal(err)
	}

	// Close should save
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Verify file was created
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("file not created: %v", err)
	}

	// Load and verify
	db2, err := LoadFromFile(path)
	if err != nil {
		t.Fatal(err)
	}
	got, err := db2.Get("default", "users")
	if err != nil {
		t.Fatal(err)
	}
	assertTableEqual(t, got, tbl)
}

// ───────────────────────────────────────────────────────────────────────────
// DiskBackend
// ───────────────────────────────────────────────────────────────────────────

func TestDiskBackend_BasicCRUD(t *testing.T) {
	dir := t.TempDir()
	b, err := NewDiskBackend(dir, false)
	if err != nil {
		t.Fatal(err)
	}
	if b.Mode() != ModeDisk {
		t.Fatalf("mode: got %v, want %v", b.Mode(), ModeDisk)
	}

	// Save a table
	tbl := makeTestTable("products", 50)
	if err := b.SaveTable("default", tbl); err != nil {
		t.Fatal(err)
	}

	// File should exist
	fp := filepath.Join(dir, "default", "products.tbl")
	if _, err := os.Stat(fp); err != nil {
		t.Fatalf("table file not created: %v", err)
	}

	// Manifest should exist
	if _, err := os.Stat(filepath.Join(dir, "manifest.json")); err != nil {
		t.Fatal("manifest not created")
	}

	// TableExists
	if !b.TableExists("default", "products") {
		t.Fatal("TableExists should be true")
	}
	if b.TableExists("default", "nonexistent") {
		t.Fatal("TableExists should be false for nonexistent")
	}

	// ListTableNames
	names, err := b.ListTableNames("default")
	if err != nil {
		t.Fatal(err)
	}
	if len(names) != 1 || names[0] != "products" {
		t.Fatalf("ListTableNames: got %v, want [products]", names)
	}

	// LoadTable
	loaded, err := b.LoadTable("default", "products")
	if err != nil {
		t.Fatal(err)
	}
	assertTableEqual(t, loaded, tbl)

	// LoadTable for nonexistent returns nil
	missing, err := b.LoadTable("default", "nonexistent")
	if err != nil || missing != nil {
		t.Fatalf("LoadTable nonexistent: got %v, %v", missing, err)
	}

	// DeleteTable
	if err := b.DeleteTable("default", "products"); err != nil {
		t.Fatal(err)
	}
	if b.TableExists("default", "products") {
		t.Fatal("TableExists should be false after delete")
	}
	if _, err := os.Stat(fp); !os.IsNotExist(err) {
		t.Fatal("file should be deleted")
	}

	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestDiskBackend_Compressed(t *testing.T) {
	dir := t.TempDir()
	b, err := NewDiskBackend(dir, true)
	if err != nil {
		t.Fatal(err)
	}

	tbl := makeTestTable("logs", 100)
	if err := b.SaveTable("default", tbl); err != nil {
		t.Fatal(err)
	}

	// File should be .tbl.gz
	fp := filepath.Join(dir, "default", "logs.tbl.gz")
	if _, err := os.Stat(fp); err != nil {
		t.Fatalf("compressed file not created: %v", err)
	}

	loaded, err := b.LoadTable("default", "logs")
	if err != nil {
		t.Fatal(err)
	}
	assertTableEqual(t, loaded, tbl)
}

func TestDiskBackend_MultipleTenants(t *testing.T) {
	dir := t.TempDir()
	b, err := NewDiskBackend(dir, false)
	if err != nil {
		t.Fatal(err)
	}

	t1 := makeTestTable("users", 5)
	t2 := makeTestTable("orders", 10)
	if err := b.SaveTable("tenant_a", t1); err != nil {
		t.Fatal(err)
	}
	if err := b.SaveTable("tenant_b", t2); err != nil {
		t.Fatal(err)
	}

	namesA, _ := b.ListTableNames("tenant_a")
	namesB, _ := b.ListTableNames("tenant_b")
	if len(namesA) != 1 || namesA[0] != "users" {
		t.Fatalf("tenant_a tables: %v", namesA)
	}
	if len(namesB) != 1 || namesB[0] != "orders" {
		t.Fatalf("tenant_b tables: %v", namesB)
	}
}

func TestDiskBackend_Persistence(t *testing.T) {
	dir := t.TempDir()

	// Create and save
	b1, err := NewDiskBackend(dir, false)
	if err != nil {
		t.Fatal(err)
	}
	tbl := makeTestTable("data", 25)
	if err := b1.SaveTable("default", tbl); err != nil {
		t.Fatal(err)
	}
	if err := b1.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen from manifest
	b2, err := NewDiskBackend(dir, false)
	if err != nil {
		t.Fatal(err)
	}
	if !b2.TableExists("default", "data") {
		t.Fatal("table should exist after reopen")
	}
	loaded, err := b2.LoadTable("default", "data")
	if err != nil {
		t.Fatal(err)
	}
	assertTableEqual(t, loaded, tbl)
}

func TestDiskBackend_IsDirty(t *testing.T) {
	dir := t.TempDir()
	b, err := NewDiskBackend(dir, false)
	if err != nil {
		t.Fatal(err)
	}

	tbl := makeTestTable("items", 3)
	if err := b.SaveTable("default", tbl); err != nil {
		t.Fatal(err)
	}

	// Same version should not be dirty
	if b.IsDirty("default", "items", tbl.Version) {
		t.Fatal("should not be dirty with same version")
	}
	// Different version should be dirty
	if !b.IsDirty("default", "items", tbl.Version+1) {
		t.Fatal("should be dirty with different version")
	}
	// Unknown table should be dirty
	if !b.IsDirty("default", "unknown", 0) {
		t.Fatal("unknown table should be dirty")
	}
}

func TestDiskBackend_Stats(t *testing.T) {
	dir := t.TempDir()
	b, err := NewDiskBackend(dir, false)
	if err != nil {
		t.Fatal(err)
	}
	tbl := makeTestTable("metrics", 10)
	_ = b.SaveTable("default", tbl)
	_, _ = b.LoadTable("default", "metrics")

	stats := b.Stats()
	if stats.Mode != ModeDisk {
		t.Errorf("mode: got %v, want %v", stats.Mode, ModeDisk)
	}
	if stats.TablesOnDisk != 1 {
		t.Errorf("tables on disk: got %d, want 1", stats.TablesOnDisk)
	}
	if stats.DiskUsedBytes <= 0 {
		t.Error("disk used should be > 0")
	}
	if stats.LoadCount != 1 {
		t.Errorf("load count: got %d, want 1", stats.LoadCount)
	}
}

// ───────────────────────────────────────────────────────────────────────────
// HybridBackend
// ───────────────────────────────────────────────────────────────────────────

func TestHybridBackend_BasicCRUD(t *testing.T) {
	dir := t.TempDir()
	b, err := NewHybridBackend(dir, 10*1024*1024, false, ModeHybrid)
	if err != nil {
		t.Fatal(err)
	}
	if b.Mode() != ModeHybrid {
		t.Fatalf("mode: got %v, want %v", b.Mode(), ModeHybrid)
	}

	tbl := makeTestTable("cache_test", 20)
	if err := b.SaveTable("default", tbl); err != nil {
		t.Fatal(err)
	}

	// Load should hit cache (no disk I/O needed)
	loaded, err := b.LoadTable("default", "cache_test")
	if err != nil {
		t.Fatal(err)
	}
	assertTableEqual(t, loaded, tbl)

	// Delete
	if err := b.DeleteTable("default", "cache_test"); err != nil {
		t.Fatal(err)
	}
	if b.TableExists("default", "cache_test") {
		t.Fatal("should not exist after delete")
	}

	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestHybridBackend_DirtyTracking(t *testing.T) {
	dir := t.TempDir()
	b, err := NewHybridBackend(dir, 10*1024*1024, false, ModeHybrid)
	if err != nil {
		t.Fatal(err)
	}

	tbl := makeTestTable("tracked", 5)
	_ = b.SaveTable("default", tbl)

	// Mark dirty
	b.MarkDirty("default", "tracked")

	// Modify the cached table
	tbl.Rows = append(tbl.Rows, []any{99, "new_row", 99.9})
	tbl.Version++
	_ = b.pool.Put("default", "tracked", tbl)

	// Sync should flush
	if err := b.Sync(); err != nil {
		t.Fatal(err)
	}

	// Verify by loading from a fresh disk backend
	b2, _ := NewDiskBackend(dir, false)
	loaded, err := b2.LoadTable("default", "tracked")
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded.Rows) != 6 {
		t.Fatalf("expected 6 rows after sync, got %d", len(loaded.Rows))
	}
}

func TestHybridBackend_IndexMode(t *testing.T) {
	dir := t.TempDir()
	b, err := NewHybridBackend(dir, 1*1024*1024, false, ModeIndex)
	if err != nil {
		t.Fatal(err)
	}
	if b.Mode() != ModeIndex {
		t.Fatalf("mode: got %v, want %v", b.Mode(), ModeIndex)
	}

	tbl := makeTestTable("indexed", 10)
	if err := b.SaveTable("default", tbl); err != nil {
		t.Fatal(err)
	}

	loaded, err := b.LoadTable("default", "indexed")
	if err != nil {
		t.Fatal(err)
	}
	assertTableEqual(t, loaded, tbl)
}

// ───────────────────────────────────────────────────────────────────────────
// OpenDB integration tests
// ───────────────────────────────────────────────────────────────────────────

func TestOpenDB_Memory(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "mem.gob")

	db, err := OpenDB(StorageConfig{Mode: ModeMemory, Path: path})
	if err != nil {
		t.Fatal(err)
	}
	if db.StorageMode() != ModeMemory {
		t.Fatalf("mode: got %v, want %v", db.StorageMode(), ModeMemory)
	}

	tbl := makeTestTable("t1", 5)
	if err := db.Put("default", tbl); err != nil {
		t.Fatal(err)
	}

	// Close should save
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen should load
	db2, err := OpenDB(StorageConfig{Mode: ModeMemory, Path: path})
	if err != nil {
		t.Fatal(err)
	}
	got, err := db2.Get("default", "t1")
	if err != nil {
		t.Fatal(err)
	}
	assertTableEqual(t, got, tbl)
	_ = db2.Close()
}

func TestOpenDB_Disk(t *testing.T) {
	dir := t.TempDir()
	dbDir := filepath.Join(dir, "diskdb")

	db, err := OpenDB(StorageConfig{Mode: ModeDisk, Path: dbDir})
	if err != nil {
		t.Fatal(err)
	}
	if db.StorageMode() != ModeDisk {
		t.Fatalf("mode: got %v, want %v", db.StorageMode(), ModeDisk)
	}

	// Create table
	tbl := makeTestTable("products", 30)
	if err := db.Put("default", tbl); err != nil {
		t.Fatal(err)
	}

	// Verify file exists on disk
	fp := filepath.Join(dbDir, "default", "products.tbl")
	if _, err := os.Stat(fp); err != nil {
		t.Fatalf("table file not on disk: %v", err)
	}

	// Evict from memory
	if err := db.Evict("default", "products"); err != nil {
		t.Fatal(err)
	}

	// Get should lazy-load from disk
	got, err := db.Get("default", "products")
	if err != nil {
		t.Fatal(err)
	}
	assertTableEqual(t, got, tbl)

	// ListTables should include the table even if evicted
	if err := db.Evict("default", "products"); err != nil {
		t.Fatal(err)
	}
	tables := db.ListTables("default")
	if len(tables) != 1 {
		t.Fatalf("ListTables: got %d, want 1", len(tables))
	}

	// Drop should delete from disk too
	if err := db.Drop("default", "products"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(fp); !os.IsNotExist(err) {
		t.Fatal("file should be deleted after Drop")
	}

	_ = db.Close()
}

func TestOpenDB_Disk_Reopen(t *testing.T) {
	dir := t.TempDir()
	dbDir := filepath.Join(dir, "diskdb2")

	// Create and populate
	db, err := OpenDB(StorageConfig{Mode: ModeDisk, Path: dbDir})
	if err != nil {
		t.Fatal(err)
	}
	t1 := makeTestTable("alpha", 10)
	t2 := makeTestTable("beta", 20)
	_ = db.Put("default", t1)
	_ = db.Put("default", t2)
	_ = db.Close()

	// Reopen
	db2, err := OpenDB(StorageConfig{Mode: ModeDisk, Path: dbDir})
	if err != nil {
		t.Fatal(err)
	}

	// Tables should be loadable
	got1, err := db2.Get("default", "alpha")
	if err != nil {
		t.Fatal(err)
	}
	assertTableEqual(t, got1, t1)

	got2, err := db2.Get("default", "beta")
	if err != nil {
		t.Fatal(err)
	}
	assertTableEqual(t, got2, t2)

	// TableExists
	if !db2.TableExists("default", "alpha") {
		t.Fatal("alpha should exist")
	}
	if db2.TableExists("default", "gamma") {
		t.Fatal("gamma should not exist")
	}

	_ = db2.Close()
}

func TestOpenDB_Hybrid(t *testing.T) {
	dir := t.TempDir()
	dbDir := filepath.Join(dir, "hybriddb")

	db, err := OpenDB(StorageConfig{
		Mode:           ModeHybrid,
		Path:           dbDir,
		MaxMemoryBytes: 10 * 1024 * 1024,
	})
	if err != nil {
		t.Fatal(err)
	}
	if db.StorageMode() != ModeHybrid {
		t.Fatalf("mode: got %v, want %v", db.StorageMode(), ModeHybrid)
	}

	tbl := makeTestTable("hybrid_test", 15)
	if err := db.Put("default", tbl); err != nil {
		t.Fatal(err)
	}

	got, err := db.Get("default", "hybrid_test")
	if err != nil {
		t.Fatal(err)
	}
	assertTableEqual(t, got, tbl)

	stats := db.BackendStats()
	if stats.Mode != ModeHybrid {
		t.Errorf("stats mode: got %v, want %v", stats.Mode, ModeHybrid)
	}

	_ = db.Close()
}

func TestOpenDB_Index(t *testing.T) {
	dir := t.TempDir()
	dbDir := filepath.Join(dir, "indexdb")

	db, err := OpenDB(StorageConfig{
		Mode: ModeIndex,
		Path: dbDir,
	})
	if err != nil {
		t.Fatal(err)
	}
	if db.StorageMode() != ModeIndex {
		t.Fatalf("mode: got %v, want %v", db.StorageMode(), ModeIndex)
	}

	tbl := makeTestTable("indexed_data", 100)
	if err := db.Put("default", tbl); err != nil {
		t.Fatal(err)
	}

	got, err := db.Get("default", "indexed_data")
	if err != nil {
		t.Fatal(err)
	}
	assertTableEqual(t, got, tbl)

	_ = db.Close()
}

func TestOpenDB_WAL(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "waldb.gob")

	db, err := OpenDB(StorageConfig{Mode: ModeWAL, Path: path})
	if err != nil {
		t.Fatal(err)
	}
	if db.StorageMode() != ModeWAL {
		t.Fatalf("mode: got %v, want %v", db.StorageMode(), ModeWAL)
	}
	if db.WAL() == nil {
		t.Fatal("WAL should be attached")
	}

	tbl := makeTestTable("wal_data", 10)
	if err := db.Put("default", tbl); err != nil {
		t.Fatal(err)
	}

	_ = db.Close()
}

func TestOpenDB_ErrorCases(t *testing.T) {
	// Missing path for disk modes
	for _, mode := range []StorageMode{ModeWAL, ModeDisk, ModeIndex, ModeHybrid} {
		_, err := OpenDB(StorageConfig{Mode: mode})
		if err == nil {
			t.Errorf("OpenDB with %v and no path should fail", mode)
		}
	}
}

// ───────────────────────────────────────────────────────────────────────────
// DB.Sync and DB.Evict
// ───────────────────────────────────────────────────────────────────────────

func TestDB_Sync_Disk(t *testing.T) {
	dir := t.TempDir()
	dbDir := filepath.Join(dir, "syncdb")

	db, err := OpenDB(StorageConfig{Mode: ModeDisk, Path: dbDir})
	if err != nil {
		t.Fatal(err)
	}

	tbl := makeTestTable("synced", 10)
	_ = db.Put("default", tbl)

	// Mutate in memory
	tbl.Rows = append(tbl.Rows, []any{999, "mutated", 999.0})
	tbl.Version++

	// Sync should detect dirty and flush
	if err := db.Sync(); err != nil {
		t.Fatal(err)
	}

	// Verify from fresh backend
	b, _ := NewDiskBackend(dbDir, false)
	loaded, err := b.LoadTable("default", "synced")
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded.Rows) != 11 {
		t.Fatalf("expected 11 rows, got %d", len(loaded.Rows))
	}

	_ = db.Close()
}

func TestDB_Evict(t *testing.T) {
	dir := t.TempDir()
	dbDir := filepath.Join(dir, "evictdb")

	db, err := OpenDB(StorageConfig{Mode: ModeDisk, Path: dbDir})
	if err != nil {
		t.Fatal(err)
	}

	tbl := makeTestTable("evict_me", 5)
	_ = db.Put("default", tbl)

	// Verify it's in memory
	db.mu.RLock()
	td := db.getTenantRO("default")
	_, inMem := td.tables["evict_me"]
	db.mu.RUnlock()
	if !inMem {
		t.Fatal("should be in memory before evict")
	}

	// Evict
	if err := db.Evict("default", "evict_me"); err != nil {
		t.Fatal(err)
	}

	// Verify it's not in memory
	db.mu.RLock()
	td = db.getTenantRO("default")
	_, inMem = td.tables["evict_me"]
	db.mu.RUnlock()
	if inMem {
		t.Fatal("should not be in memory after evict")
	}

	// Should still be accessible (lazy load)
	got, err := db.Get("default", "evict_me")
	if err != nil {
		t.Fatal(err)
	}
	assertTableEqual(t, got, tbl)

	// Evict on memory-only DB should fail
	memDB := NewDB()
	if err := memDB.Evict("default", "anything"); err == nil {
		t.Fatal("evict on memory DB should fail")
	}

	_ = db.Close()
}

// ───────────────────────────────────────────────────────────────────────────
// MigrateToBackend
// ───────────────────────────────────────────────────────────────────────────

func TestDB_MigrateToBackend(t *testing.T) {
	// Start with in-memory DB
	db := NewDB()
	t1 := makeTestTable("alpha", 5)
	t2 := makeTestTable("beta", 10)
	_ = db.Put("default", t1)
	_ = db.Put("other", t2)

	// Migrate to disk
	dir := t.TempDir()
	backend, err := NewDiskBackend(dir, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.MigrateToBackend(backend); err != nil {
		t.Fatal(err)
	}

	if db.StorageMode() != ModeDisk {
		t.Fatalf("mode: got %v, want %v", db.StorageMode(), ModeDisk)
	}

	// Tables should be on disk
	names, _ := backend.ListTableNames("default")
	sort.Strings(names)
	if len(names) != 1 || names[0] != "alpha" {
		t.Fatalf("default tables: %v", names)
	}
	names, _ = backend.ListTableNames("other")
	if len(names) != 1 || names[0] != "beta" {
		t.Fatalf("other tables: %v", names)
	}

	_ = db.Close()
}

// ───────────────────────────────────────────────────────────────────────────
// DiskBackend ImportFromDB
// ───────────────────────────────────────────────────────────────────────────

func TestDiskBackend_ImportFromDB(t *testing.T) {
	// Create an in-memory DB with several tables
	db := NewDB()
	_ = db.Put("default", makeTestTable("t1", 5))
	_ = db.Put("default", makeTestTable("t2", 10))
	_ = db.Put("org1", makeTestTable("t3", 3))

	dir := t.TempDir()
	backend, err := NewDiskBackend(dir, false)
	if err != nil {
		t.Fatal(err)
	}

	if err := backend.ImportFromDB(db); err != nil {
		t.Fatal(err)
	}

	// All tables should be on disk
	names, _ := backend.ListTableNames("default")
	sort.Strings(names)
	if len(names) != 2 {
		t.Fatalf("expected 2 tables in default, got %v", names)
	}
	names, _ = backend.ListTableNames("org1")
	if len(names) != 1 {
		t.Fatalf("expected 1 table in org1, got %v", names)
	}

	// Verify data integrity
	loaded, _ := backend.LoadTable("default", "t2")
	if len(loaded.Rows) != 10 {
		t.Fatalf("t2 rows: got %d, want 10", len(loaded.Rows))
	}
}

// ───────────────────────────────────────────────────────────────────────────
// BackendStats
// ───────────────────────────────────────────────────────────────────────────

func TestDB_BackendStats(t *testing.T) {
	// Memory DB (no backend)
	db := NewDB()
	stats := db.BackendStats()
	if stats.Mode != ModeMemory {
		t.Errorf("memory stats mode: got %v, want %v", stats.Mode, ModeMemory)
	}

	// Disk DB
	dir := t.TempDir()
	db2, _ := OpenDB(StorageConfig{Mode: ModeDisk, Path: filepath.Join(dir, "statsdb")})
	_ = db2.Put("default", makeTestTable("s", 10))
	stats2 := db2.BackendStats()
	if stats2.Mode != ModeDisk {
		t.Errorf("disk stats mode: got %v, want %v", stats2.Mode, ModeDisk)
	}
	if stats2.TablesOnDisk != 1 {
		t.Errorf("tables on disk: got %d, want 1", stats2.TablesOnDisk)
	}
	_ = db2.Close()
}
