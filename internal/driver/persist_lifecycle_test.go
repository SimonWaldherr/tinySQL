package driver

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestFlushPersistWaitsForRunningTimer(t *testing.T) {
	s := newServer(storage.NewDB(), cfg{persistDebounce: time.Millisecond})
	started, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(func() {
		unblock()
		s.flushPersist()
		s.db.Close()
	})
	s.persistDelayForTest = func() { close(started); <-release }
	if err := s.persist(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("debounce callback did not start")
	}
	flushed := make(chan error, 1)
	go func() { flushed <- s.flushPersist() }()
	select {
	case err := <-flushed:
		t.Fatalf("flush returned while the timer was still syncing: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	unblock()
	if err := <-flushed; err != nil {
		t.Fatal(err)
	}
}

func TestFlushPersistInvalidatesStaleTimer(t *testing.T) {
	s := newServer(storage.NewDB(), cfg{persistDebounce: time.Hour})
	t.Cleanup(func() { s.flushPersist(); s.db.Close() })
	s.schedulePersist()
	oldGeneration := s.persistGeneration
	if err := s.flushPersist(); err != nil {
		t.Fatal(err)
	}
	s.schedulePersist()
	newTimer := s.persistTimer
	// Simulate a callback that already fired but had to wait for a flush.
	s.runDebouncedPersist(oldGeneration)
	if s.persistTimer != newTimer || !s.persistDirty || s.persistSyncCount != 1 {
		t.Fatal("stale callback consumed a newer timer's pending work")
	}
	if err := s.flushPersist(); err != nil || s.persistSyncCount != 2 {
		t.Fatalf("flush of newer work: count=%d, err=%v", s.persistSyncCount, err)
	}
}

func TestFailedDebouncedPersistRetainsFlushObligation(t *testing.T) {
	path := t.TempDir() // renaming a snapshot onto a directory must fail
	s := newServer(storage.NewDB(), cfg{filePath: path, autosave: true, persistDebounce: time.Hour})
	t.Cleanup(func() { s.db.Close() })
	s.schedulePersist()
	s.persistTimer.Stop()
	s.runDebouncedPersist(s.persistGeneration)
	if !s.persistDirty {
		t.Fatal("failed timer sync discarded pending work")
	}
	s.filePath = filepath.Join(path, "recovered.gob")
	if err := s.flushPersist(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(s.filePath); err != nil {
		t.Fatalf("final flush did not retry the failed sync: %v", err)
	}
	if s.persistDirty || s.persistSyncCount != 2 {
		t.Fatalf("retry state: dirty=%v, count=%d", s.persistDirty, s.persistSyncCount)
	}
}
