//go:build sqliteimport && !js && !wasm && !baremetal

package storage

import (
	"path/filepath"
	"testing"
)

func TestSQLiteCloseReportsCheckpointFailure(t *testing.T) {
	backend, err := NewSQLiteBackend(filepath.Join(t.TempDir(), "db.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	// A closed underlying pool forces the final checkpoint to fail.
	if err := backend.db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := backend.Close(); err == nil {
		t.Fatal("close discarded checkpoint failure")
	}
	if err := backend.Close(); err != nil {
		t.Fatalf("repeated close: %v", err)
	}
}
