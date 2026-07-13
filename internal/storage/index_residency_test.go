package storage

import (
	"bytes"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

// TestModeIndexBackendLoadsDoNotEscapeTheBoundedPool is the catalog-regression
// test for read-mostly artifacts. Historically DB.Get put a second permanent
// reference to every backend-loaded table in DB.tenants, so BufferPool eviction
// could never release it. A table larger than the cache budget is deliberately
// returned as a transient lease but not admitted to the pool or catalog.
func TestModeIndexBackendLoadsDoNotEscapeTheBoundedPool(t *testing.T) {
	dir := t.TempDir()
	writer, err := OpenDB(StorageConfig{Mode: ModeIndex, Path: dir, MaxMemoryBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	table := NewTable("images", []Column{{Name: "tile_id", Type: TextType}, {Name: "tile_data", Type: BlobType}}, false)
	for i := 0; i < 64; i++ {
		table.Rows = append(table.Rows, []any{"tile", bytes.Repeat([]byte{byte(i)}, 2048)})
	}
	if err := writer.Put("default", table); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	const limit = int64(1024)
	reader, err := OpenDB(StorageConfig{Mode: ModeIndex, Path: dir, MaxMemoryBytes: limit, ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	loaded, err := reader.Get("default", "images")
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded.Rows) != 64 {
		t.Fatalf("loaded %d rows, want 64", len(loaded.Rows))
	}
	loaded = nil
	runtime.GC()

	reader.mu.RLock()
	td := reader.getTenantRO("default")
	retained := td != nil && td.tables["images"] != nil
	reader.mu.RUnlock()
	if retained {
		t.Fatal("ModeIndex DB catalog retained a backend-loaded table")
	}
	stats := reader.BackendStats()
	if stats.MemoryUsedBytes > limit {
		t.Fatalf("buffer pool retained %d bytes with a %d-byte limit", stats.MemoryUsedBytes, limit)
	}

	// A second lookup must remain bounded too; it may reload because the table
	// is larger than the cache budget, but it cannot become permanently
	// reachable through the tenant catalog.
	if _, err := reader.Get("default", "images"); err != nil {
		t.Fatal(err)
	}
	reader.mu.RLock()
	td = reader.getTenantRO("default")
	retained = td != nil && td.tables["images"] != nil
	reader.mu.RUnlock()
	if retained {
		t.Fatal("second ModeIndex lookup retained a backend-loaded table")
	}
}

func TestBufferPoolRejectsOversizedAdmission(t *testing.T) {
	pool := NewBufferPool(&MemoryPolicy{
		MaxMemoryBytes:      512,
		EvictionThreshold:   1,
		EnableEviction:      true,
		EvictionBatchSize:   1,
		TrackAccessPatterns: true,
	})
	table := NewTable("oversized", []Column{{Name: "data", Type: BlobType}}, false)
	table.Rows = append(table.Rows, []any{bytes.Repeat([]byte{1}, 4096)})
	if err := pool.Put("default", "oversized", table); err == nil {
		t.Fatal("oversized table was admitted despite a strict memory limit")
	}
	if got := pool.GetMemoryUsage(); got != 0 {
		t.Fatalf("oversized admission left %d bytes resident", got)
	}
}

func TestReadOnlyWALIsRejectedBeforeItCreatesASidecar(t *testing.T) {
	path := filepath.Join(t.TempDir(), "artifact.gob")
	if _, err := OpenDB(StorageConfig{Mode: ModeWAL, Path: path, ReadOnly: true}); err == nil {
		t.Fatal("read-only WAL open unexpectedly succeeded")
	}
	if _, err := os.Stat(path + ".wal"); !os.IsNotExist(err) {
		t.Fatalf("read-only WAL open created sidecar: %v", err)
	}
}
