package storage

import (
	"os"
	"path/filepath"
	"testing"
)

// TestModeAdvancedWALCheckpointCompression verifies that
// StorageConfig.CompressFiles, for ModeAdvancedWAL, gzip-compresses the
// periodic checkpoint snapshot (named "<path>.checkpoint.gz" instead of
// "<path>.checkpoint") while leaving the live WAL log file itself
// untouched — and that a database opened this way still round-trips data
// correctly across an explicit checkpoint and a close/reopen cycle.
func TestModeAdvancedWALCheckpointCompression(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	db, err := OpenDB(StorageConfig{Mode: ModeAdvancedWAL, Path: path, CompressFiles: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	tbl := makeTestTable("events", 20)
	if err := db.Put("default", tbl); err != nil {
		t.Fatalf("put: %v", err)
	}

	wal := db.AdvancedWAL()
	if wal == nil {
		t.Fatal("expected AdvancedWAL to be attached")
	}
	if err := wal.Checkpoint(db); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	compressedPath := path + ".checkpoint.gz"
	data, err := os.ReadFile(compressedPath)
	if err != nil {
		t.Fatalf("expected compressed checkpoint file %q: %v", compressedPath, err)
	}
	if len(data) < 2 || data[0] != 0x1f || data[1] != 0x8b {
		t.Fatalf("checkpoint file does not start with the gzip magic bytes: %x", data[:min(2, len(data))])
	}
	if _, err := os.Stat(path + ".checkpoint"); !os.IsNotExist(err) {
		t.Fatalf("expected no uncompressed checkpoint file to exist alongside the compressed one")
	}

	// The live WAL log itself must remain plain gob, not gzip: compression
	// only ever applies to the checkpoint snapshot (see OpenDB's
	// ModeAdvancedWAL case and AdvancedWALConfig's doc comment).
	logData, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read wal log: %v", err)
	}
	if len(logData) >= 2 && logData[0] == 0x1f && logData[1] == 0x8b {
		t.Fatalf("WAL log file unexpectedly starts with gzip magic bytes — the live log must never be compressed")
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	db2, err := OpenDB(StorageConfig{Mode: ModeAdvancedWAL, Path: path, CompressFiles: true})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = db2.Close() }()
	got, err := db2.Get("default", "events")
	if err != nil {
		t.Fatalf("get after reopen: %v", err)
	}
	assertTableEqual(t, got, tbl)
}
