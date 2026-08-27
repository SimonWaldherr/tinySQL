package storage

import (
	"errors"
	"fmt"
	"time"
)

// startAdvancedWALCheckpointScheduler starts the small storage-owned worker
// that turns AdvancedWAL's configured record, byte, and time thresholds into
// real checkpoints. AdvancedWAL historically exposed ShouldCheckpoint but no
// code called Checkpoint automatically, so a long-running server could grow a
// log forever despite an apparently configured limit.
//
// The scheduler is deliberately independent from Scheduler (the SQL job
// runner): it has no user SQL to execute and needs no JobExecutor. Its only
// shared lock order is DB content lock -> AdvancedWAL lock, the same order as
// a SQL mutation and SnapshotWithWatermark, so it cannot form a lock cycle
// with a statement that is committing a row change.
func (db *DB) startAdvancedWALCheckpointScheduler(wal *AdvancedWAL) {
	if db == nil || wal == nil || wal.checkpointPath == "" {
		return
	}

	db.advancedCheckpointMu.Lock()
	if db.advancedCheckpointStop != nil {
		db.advancedCheckpointMu.Unlock()
		return
	}
	stop := make(chan struct{})
	done := make(chan struct{})
	db.advancedCheckpointStop = stop
	db.advancedCheckpointDone = done
	db.advancedCheckpointMu.Unlock()

	interval := wal.checkpointInterval
	if interval <= 0 {
		// OpenAdvancedWAL normalizes this already; retain a defensive fallback
		// for an internally-constructed WAL used by a package test.
		interval = 5 * time.Minute
	}

	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		// A recovered WAL can already exceed a record/size threshold before
		// any fresh commit arrives. Check once on startup, then rely on both
		// the periodic deadline and coalesced commit notifications.
		db.maybeCheckpointAdvancedWAL(wal)
		for {
			select {
			case <-stop:
				return
			case <-wal.checkpointNotify:
			case <-ticker.C:
			}
			db.maybeCheckpointAdvancedWAL(wal)
		}
	}()
}

// maybeCheckpointAdvancedWAL is intentionally best-effort: a normal SQL
// commit remains durable in the WAL even if a later snapshot checkpoint has a
// transient I/O failure. The failure is recorded in DB health and the next
// wake-up retries it; callers never receive a false successful checkpoint.
func (db *DB) maybeCheckpointAdvancedWAL(wal *AdvancedWAL) {
	if db == nil || wal == nil || !wal.checkpointWorkPending() || !wal.ShouldCheckpoint() {
		return
	}
	if err := wal.Checkpoint(db); err != nil {
		// An explicit/ambient transaction may span several statements. It is
		// unsafe to checkpoint its mutable in-memory state before its terminal
		// commit/abort; that terminal event wakes this worker again.
		if errors.Is(err, ErrAdvancedWALCheckpointActiveTransactions) {
			return
		}
		db.markError(fmt.Errorf("advanced WAL automatic checkpoint: %w", err))
		return
	}
	db.markSynced()
}

// stopAdvancedWALCheckpointScheduler waits for an in-progress checkpoint to
// complete before returning. DB.Close invokes it before closing the WAL file,
// preventing a background goroutine from writing through a descriptor Close
// has just released.
func (db *DB) stopAdvancedWALCheckpointScheduler() {
	if db == nil {
		return
	}
	db.advancedCheckpointMu.Lock()
	stop := db.advancedCheckpointStop
	done := db.advancedCheckpointDone
	db.advancedCheckpointStop = nil
	db.advancedCheckpointDone = nil
	db.advancedCheckpointMu.Unlock()
	if stop == nil {
		return
	}
	close(stop)
	<-done
}
