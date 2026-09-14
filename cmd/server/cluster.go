package main

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// replicaHealth measures feed freshness, not a promise of synchronous reads.
// A successful empty response is a heartbeat; an error withdraws readiness
// immediately. A silent/black-holed connection expires without a timer goroutine.
type replicaHealth struct {
	mu        sync.Mutex
	primary   string
	transport string
	timeout   time.Duration
	lastOK    time.Time
	connected bool
	resumeLSN uint64
	epoch     uint64
	lastError string
}

func (h *replicaHealth) observe(lsn, epoch uint64, err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.connected = err == nil
	if err != nil {
		h.lastError = err.Error()
		return
	}
	h.lastOK = time.Now()
	h.resumeLSN, h.epoch = lsn, epoch
	h.lastError = ""
}

func (h *replicaHealth) available() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.connected && !h.lastOK.IsZero() && time.Since(h.lastOK) < h.timeout
}

func (h *replicaHealth) snapshot() map[string]any {
	h.mu.Lock()
	defer h.mu.Unlock()
	result := map[string]any{
		"role": "replica", "primary": h.primary, "transport": h.transport,
		"connected": h.connected, "epoch": h.epoch, "resume_lsn": h.resumeLSN,
		"ready_timeout": h.timeout.String(), "last_error": h.lastError,
	}
	if !h.lastOK.IsZero() {
		result["last_success"] = h.lastOK.UTC().Format(time.RFC3339Nano)
		result["response_age_ms"] = time.Since(h.lastOK).Milliseconds()
	}
	return result
}

func (s *server) isReady() bool {
	return s.ready.Load() && (s.replica == nil || s.replica.available())
}

var errServerNotReady = status.Error(codes.Unavailable, "server not ready")

// acquireDatabase pins the current snapshot. Check both sides of RLock so new
// requests fail promptly while a writer waits for old streams to finish.
func (s *server) acquireDatabase() (func(), error) {
	if !s.isReady() {
		return nil, errServerNotReady
	}
	s.dbMu.RLock()
	if !s.isReady() {
		s.dbMu.RUnlock()
		return nil, errServerNotReady
	}
	return s.dbMu.RUnlock, nil
}

func (s *server) replaceReplicaDatabase(db *storage.DB) {
	db.SetReadOnly(true)
	s.dbMu.Lock()
	old := s.db
	s.db = db
	// Parsed statements can retain execution hints from the previous snapshot.
	s.cache.Clear()
	s.dbMu.Unlock()
	_ = old.Close()
}

// Bootstrap retries also cover cold starts while the primary is unavailable.
// Re-bootstrap first withdraws readiness and only publishes a complete snapshot.
// The first applied feed response then makes that snapshot eligible for reads.
func (s *server) serveReplicaLoop(ctx context.Context, primary string, opts replicaOptions, changesLoop replicaChangesLoopFunc) {
	opts.Observe = s.replica.observe
	backoff := replicaMinPollBackoff
	for ctx.Err() == nil {
		s.replica.observe(0, 0, errors.New("replica is bootstrapping"))
		db, watermark, epoch, err := runReplicaBootstrap(ctx, primary, s.defaultT, opts)
		if err != nil {
			s.replica.observe(0, 0, err)
			if !replicaSleep(ctx, backoff) {
				return
			}
			backoff = replicaNextBackoff(backoff)
			continue
		}
		if ctx.Err() != nil {
			_ = db.Close()
			return
		}
		s.replaceReplicaDatabase(db)
		backoff = replicaMinPollBackoff
		log.Printf("serving replica from %s at lsn=%d epoch=%d", primary, watermark, epoch)
		err = changesLoop(ctx, db, primary, s.defaultT, watermark, epoch, opts)
		if err == nil {
			err = errors.New("replication feed stopped")
		}
		s.replica.observe(0, 0, err)
		if ctx.Err() != nil {
			return
		}
		if !errors.Is(err, errReplicaNeedsRebootstrap) && !replicaSleep(ctx, backoff) {
			return
		}
	}
}
