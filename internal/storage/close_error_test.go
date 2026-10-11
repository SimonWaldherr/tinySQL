package storage

import (
	"bufio"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

type failingCloseWriter struct{ err error }

func (w failingCloseWriter) Write([]byte) (int, error) { return 0, w.err }

func TestWALCloseReleasesFileAfterFlushError(t *testing.T) {
	flushErr := errors.New("injected flush failure")
	for _, advanced := range []bool{false, true} {
		t.Run(map[bool]string{false: "basic", true: "advanced"}[advanced], func(t *testing.T) {
			var file *os.File
			var closeWAL func() error
			writer := bufio.NewWriter(failingCloseWriter{flushErr})
			if err := writer.WriteByte('x'); err != nil {
				t.Fatal(err)
			}
			if advanced {
				wal, err := OpenAdvancedWAL(AdvancedWALConfig{Path: filepath.Join(t.TempDir(), "log.wal")})
				if err != nil {
					t.Fatal(err)
				}
				file, closeWAL = wal.file, wal.Close
				wal.writer = writer
			} else {
				wal, err := OpenWAL(NewDB(), WALConfig{Path: filepath.Join(t.TempDir(), "db.gob")})
				if err != nil {
					t.Fatal(err)
				}
				file, closeWAL = wal.file, wal.Close
				wal.writer = writer
			}
			t.Cleanup(func() { file.Close() })
			if err := closeWAL(); !errors.Is(err, flushErr) {
				t.Fatalf("lost flush error: %v", err)
			}
			if _, err := file.Stat(); !errors.Is(err, os.ErrClosed) {
				t.Fatalf("WAL file remained open after failed flush: %v", err)
			}
			if err := closeWAL(); err != nil {
				t.Fatalf("repeated close: %v", err)
			}
		})
	}
}

func TestAuditCloseReleasesFileAfterFlushError(t *testing.T) {
	audit, err := OpenAuditLog(filepath.Join(t.TempDir(), "audit.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	file := audit.file
	t.Cleanup(func() { file.Close() })
	flushErr := errors.New("injected audit flush failure")
	audit.writer = bufio.NewWriter(failingCloseWriter{flushErr})
	if err := audit.writer.WriteByte('x'); err != nil {
		t.Fatal(err)
	}
	if err := audit.Close(); !errors.Is(err, flushErr) {
		t.Fatalf("lost flush error: %v", err)
	}
	if _, err := file.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("audit file remained open after failed flush: %v", err)
	}
	if err := audit.Close(); err != nil {
		t.Fatalf("repeated close: %v", err)
	}
}

type closeErrorBackend struct {
	StorageBackend
	syncErr  error
	closeErr error
}

func (b closeErrorBackend) Sync() error  { return b.syncErr }
func (b closeErrorBackend) Close() error { return b.closeErr }

func TestDBClosePreservesSyncAndCloseErrors(t *testing.T) {
	syncErr := errors.New("sync failed")
	closeErr := errors.New("close failed")
	db := NewDB()
	db.backend = closeErrorBackend{syncErr: syncErr, closeErr: closeErr}
	err := db.Close()
	if !errors.Is(err, syncErr) || !errors.Is(err, closeErr) {
		t.Fatalf("expected both shutdown failures, got %v", err)
	}
}
