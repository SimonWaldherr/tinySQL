// The server: the one *storage.DB behind a sql.Open, the reader/writer gating
// that serialises access to it, and persistence.
//
// persist reports failure and its callers propagate it. Acknowledging a write
// whose durable store rejected it loses data with no way for the caller to find
// out.
package driver

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// server coordinates access to the shared storage.DB and manages
// concurrency primitives plus optional persistence hooks.
type server struct {
	mu          sync.RWMutex
	db          *storage.DB
	filePath    string
	autosave    bool
	readerPool  chan struct{}
	writerPool  chan struct{}
	busyTimeout time.Duration
	// usesStorageBackend is true when db was opened via storage.OpenDB with
	// an explicit mode= DSN option, rather than the driver's original
	// LoadFromFile/NewDB + SaveToFile-on-close scheme. Such backends persist
	// via DB.Sync() (which flushes dirty tables to whatever backend is
	// attached — GOB, JSON, ...), not via a whole-database GOB snapshot.
	usesStorageBackend bool

	// persistDebounce is zero by default, meaning persist() below always
	// takes the immediate synchronous path (unchanged behavior). When
	// positive (persist_debounce_ms DSN option), persist() defers the actual
	// sync to at most one per debounce window; see persist's doc comment.
	persistDebounce time.Duration

	persistMu    sync.Mutex
	persistTimer *time.Timer
	// persistDirty is true when a debounced persist() call has been
	// requested since the last actual sync completed. It lets
	// runDebouncedPersist and flushPersist tell "nothing to do" (e.g.
	// debounce enabled but this server never actually wrote anything) apart
	// from "a sync is owed", so neither one performs surprise I/O that the
	// immediate/default path would never have done.
	persistDirty bool
	// persistSyncCount counts every actual underlying sync performed by
	// persistNow, whether reached directly (debounce off) or via the
	// debounce timer/flush (debounce on). It is cheap bookkeeping kept for
	// tests and diagnostics; it has no effect on behavior.
	persistSyncCount uint64
}

func newServer(db *storage.DB, c cfg) *server {
	s := &server{
		db:                 db,
		filePath:           c.filePath,
		autosave:           c.autosave,
		busyTimeout:        c.busyTimeout,
		usesStorageBackend: c.modeSet && c.mode != storage.ModeMemory,
		persistDebounce:    c.persistDebounce,
	}
	if c.maxReaders > 0 {
		s.readerPool = make(chan struct{}, c.maxReaders)
	}
	if c.maxWriters > 0 {
		s.writerPool = make(chan struct{}, c.maxWriters)
	}
	return s
}

func (s *server) acquireReader(ctx context.Context) error {
	return s.acquire(ctx, s.readerPool)
}

func (s *server) releaseReader() {
	s.release(s.readerPool)
}

func (s *server) acquireWriter(ctx context.Context) error {
	return s.acquire(ctx, s.writerPool)
}

func (s *server) releaseWriter() {
	s.release(s.writerPool)
}

//nolint:gocyclo // Connection throttling must cover timeout, context, and immediate acquisition paths.
func (s *server) acquire(ctx context.Context, pool chan struct{}) error {
	if pool == nil {
		if ctx != nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
		}
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if s.busyTimeout <= 0 {
		select {
		case pool <- struct{}{}:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	timeout := s.busyTimeout
	if deadline, ok := ctx.Deadline(); ok {
		remain := time.Until(deadline)
		if remain <= 0 {
			return ctx.Err()
		}
		if remain < timeout {
			timeout = remain
		}
	}
	select {
	case pool <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	timer := time.NewTimer(timeout)
	defer func() {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	}()
	select {
	case pool <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return fmt.Errorf("tinysql: busy timeout after %s", timeout)
	}
}

func (s *server) release(pool chan struct{}) {
	// Non-blocking release: if the pool is empty or nil, simply return.
	if pool == nil {
		return
	}
	select {
	case <-pool:
	default:
	}
}

// persist flushes the database to its durable storage and reports whether that
// succeeded. Callers that are about to acknowledge a write to the application
// must propagate the error: reporting success for a statement whose durable
// write failed loses data silently, and the in-memory state then disagrees with
// what a restart will find.
//
// By default (persistDebounce == 0, the only behavior before that option
// existed) every call takes the immediate path below and performs its sync
// synchronously before returning, exactly as always.
//
// When persistDebounce is positive (opt in via the persist_debounce_ms DSN
// option), persist() instead marks the database as needing a sync and
// returns immediately without error; the actual sync runs at most once per
// debounce window, on a timer, so a burst of N rapid statements against the
// same connection pool collapses into far fewer than N actual
// backend.SaveTable/Sync calls. This changes durability timing: a crash
// within the debounce window can lose the most recent statement(s) that
// would otherwise have been immediately durable. The obligation to
// eventually sync is never dropped — a timer is always pending while there
// is unsynced work, and flushPersist (called from the connector's Close, the
// sql.DB.Close() path) forces one final synchronous sync so a clean process
// exit never leaves a debounced write unflushed.
func (s *server) persist() error {
	// A read-only open must be observational: physical connection closes and
	// database/sql pool churn must never create manifests, checkpoints, WAL
	// files, or rewritten snapshots.
	if s.db == nil || s.db.IsReadOnly() {
		return nil
	}
	if s.persistDebounce <= 0 {
		return s.persistNow()
	}
	s.schedulePersist()
	return nil
}

// persistNow performs the actual, unconditional, synchronous durable sync.
// It is the entire body of persist() before debouncing existed, extracted
// so both the immediate path and the deferred (timer/flush) paths share one
// implementation.
func (s *server) persistNow() error {
	if s.db == nil || s.db.IsReadOnly() {
		return nil
	}
	s.persistMu.Lock()
	s.persistSyncCount++
	s.persistMu.Unlock()
	if s.usesStorageBackend {
		// Disk-backed modes (ModeDisk, ModeJSON, ModeHybrid, ModeIndex)
		// persist via their attached backend's Sync, not a whole-database
		// GOB snapshot; ModeWAL/ModeAdvancedWAL rely on their own
		// checkpoint machinery and treat Sync as a no-op (see DB.Sync's
		// doc comment). Always sync here regardless of the autosave flag —
		// choosing a durable mode is itself the opt-in.
		if err := s.db.Sync(); err != nil {
			return fmt.Errorf("tinysql: sync to durable storage failed: %w", err)
		}
		return nil
	}
	if s.autosave && s.filePath != "" {
		if err := storage.SaveToFile(s.db, s.filePath); err != nil {
			return fmt.Errorf("tinysql: autosave to %s failed: %w", s.filePath, err)
		}
	}
	return nil
}

// schedulePersist records that a sync is owed and, if none is already
// pending, starts a timer that performs it after persistDebounce elapses.
// Calls that arrive while a timer is already pending just ride along with
// it — that coalescing is the entire point of the option: N calls inside one
// window produce one persistNow, not N.
func (s *server) schedulePersist() {
	s.persistMu.Lock()
	defer s.persistMu.Unlock()
	s.persistDirty = true
	if s.persistTimer != nil {
		return
	}
	s.persistTimer = time.AfterFunc(s.persistDebounce, s.runDebouncedPersist)
}

// runDebouncedPersist is the timer callback: it performs the sync owed by
// whichever persist() calls scheduled or rode along with this window, then
// clears the pending state so the next persist() call starts a fresh timer.
// There is no caller left to report a failure to here, so it is logged —
// the same best-effort posture persistBestEffort already uses for cleanup
// paths, and DB.Sync additionally records its own failures for HealthCheck.
func (s *server) runDebouncedPersist() {
	s.persistMu.Lock()
	s.persistTimer = nil
	dirty := s.persistDirty
	s.persistDirty = false
	s.persistMu.Unlock()
	if !dirty {
		return
	}
	if err := s.persistNow(); err != nil {
		log.Printf("tinysql: debounced persist failed: %v", err)
	}
}

// flushPersist cancels any pending debounce timer and, if a sync is owed,
// performs it synchronously now. It is a no-op — zero extra work, zero extra
// I/O — whenever there is nothing owed, which is always true when debouncing
// is off, so calling it unconditionally from a shutdown path never changes
// the default (debounce disabled) behavior. Shutdown paths call it so a
// pending debounced sync is never silently lost when the process exits
// cleanly.
func (s *server) flushPersist() error {
	s.persistMu.Lock()
	if s.persistTimer != nil {
		s.persistTimer.Stop()
		s.persistTimer = nil
	}
	dirty := s.persistDirty
	s.persistDirty = false
	s.persistMu.Unlock()
	if !dirty {
		return nil
	}
	return s.persistNow()
}

// persistBestEffort is persist for cleanup paths that have no caller left to
// report to, such as a physical connection being closed by the pool. The error
// is logged and also recorded on the database, where HealthCheck surfaces it.
func (s *server) persistBestEffort() {
	if err := s.persist(); err != nil {
		log.Printf("tinysql: %v", err)
	}
}
