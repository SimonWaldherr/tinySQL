// mode= means two different things in SQLite and in tinySQL: an access mode
// there, a storage backend here. These tests pin the targeted diagnostic for
// the colliding spellings, and pin that the one overlapping value ("memory")
// still resolves instead of erroring.
package storage

import (
	"strings"
	"testing"
)

// TestParseStorageModeRejectsSQLiteAccessModes checks the diagnostic, not just
// the error. A migrant who wrote mode=ro used to be told only "unknown storage
// mode", which is true and useless; the message must name the collision and
// point at read_only=1 plus a persistent backend.
func TestParseStorageModeRejectsSQLiteAccessModes(t *testing.T) {
	for _, in := range []string{"ro", "rw", "rwc", "RO", " rwc ", "RWC"} {
		mode, err := ParseStorageMode(in)
		if err == nil {
			t.Errorf("ParseStorageMode(%q) = %v, want an error: it is a SQLite access mode, not a backend", in, mode)
			continue
		}
		if mode != ModeMemory {
			t.Errorf("ParseStorageMode(%q) mode = %v, want ModeMemory alongside the error (same as the generic unknown-value branch)", in, mode)
		}
		msg := err.Error()
		for _, want := range []string{"access mode", "storage backend", "read_only=1"} {
			if !strings.Contains(msg, want) {
				t.Errorf("ParseStorageMode(%q) error %q does not mention %q", in, msg, want)
			}
		}
		// It must NOT read as a translation offer: nothing here silently maps
		// mode=rwc onto a backend.
		if strings.Contains(msg, "unknown storage mode") {
			t.Errorf("ParseStorageMode(%q) fell through to the generic message: %q", in, msg)
		}
	}
}

// TestParseStorageModeMemoryStillResolves guards the one value that is BOTH a
// tinySQL storage mode and a SQLite access mode. It must never join the
// access-mode error branch: mode=memory already does what a SQLite user
// writing it wants.
func TestParseStorageModeMemoryStillResolves(t *testing.T) {
	for _, in := range []string{"memory", "MEMORY", " Memory ", "mem", "ram", ""} {
		got, err := ParseStorageMode(in)
		if err != nil {
			t.Errorf("ParseStorageMode(%q) returned error: %v", in, err)
			continue
		}
		if got != ModeMemory {
			t.Errorf("ParseStorageMode(%q) = %v, want ModeMemory", in, got)
		}
	}
}

// TestParseStorageModeGenericErrorPreserved ensures the new targeted branch did
// not swallow genuinely unknown values into the SQLite-specific message.
func TestParseStorageModeGenericErrorPreserved(t *testing.T) {
	for _, in := range []string{"unknown", "readonly", "rwx", "r", "sqlite3"} {
		_, err := ParseStorageMode(in)
		if err == nil {
			t.Errorf("ParseStorageMode(%q) succeeded, want an error", in)
			continue
		}
		if !strings.Contains(err.Error(), "unknown storage mode") {
			t.Errorf("ParseStorageMode(%q) error = %q, want the generic unknown-storage-mode message", in, err)
		}
	}
}
