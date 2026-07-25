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
}

func newServer(db *storage.DB, c cfg) *server {
	s := &server{
		db:                 db,
		filePath:           c.filePath,
		autosave:           c.autosave,
		busyTimeout:        c.busyTimeout,
		usesStorageBackend: c.modeSet && c.mode != storage.ModeMemory,
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
func (s *server) persist() error {
	// A read-only open must be observational: physical connection closes and
	// database/sql pool churn must never create manifests, checkpoints, WAL
	// files, or rewritten snapshots.
	if s.db == nil || s.db.IsReadOnly() {
		return nil
	}
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

// persistBestEffort is persist for cleanup paths that have no caller left to
// report to, such as a physical connection being closed by the pool. The error
// is logged and also recorded on the database, where HealthCheck surfaces it.
func (s *server) persistBestEffort() {
	if err := s.persist(); err != nil {
		log.Printf("tinysql: %v", err)
	}
}
