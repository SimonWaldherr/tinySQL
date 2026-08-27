// Primitives for a one-way replication feed built on top of AdvancedWAL:
// reading committed-only records since some LSN watermark, applying a
// received record on the subscriber side, and taking a snapshot tagged with
// the watermark a feed should resume from.
//
// This file deliberately does not touch Recover (wal_advanced.go): its
// BEGIN/COMMIT/ABORT bookkeeping is duplicated here for ReadCommittedSince
// rather than factored out, so existing crash-recovery behavior and its
// tests are zero-risk to this addition.
package storage

import (
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
)

// ErrReplicaTooFarBehind is returned by ReadCommittedSince when sinceLSN is
// below the highest LSN a checkpoint has actually captured (see
// checkpointDataWatermark's doc comment in wal_advanced.go for exactly what
// that is and why it is not simply wal.checkpointWatermark): the caller is
// asking for a range that was already checkpointed and truncated out of the
// live WAL file, so it can no longer be served incrementally without
// silently skipping every record between sinceLSN and that boundary. That
// silent gap is exactly what the checkpointWatermark skip inside
// ReadCommittedSince's decode loop would otherwise produce for any request
// in this state -- returning this error up front instead makes the
// condition observable, so a caller (see cmd/server's GetChangesSince
// handler, which maps this to a distinguishable gRPC status) can react by
// re-bootstrapping the replica from a fresh snapshot instead of quietly
// missing data.
var ErrReplicaTooFarBehind = errors.New("replica requested WAL records already checkpointed away; a full re-bootstrap is required")

// ErrReplicaEpochMismatch rejects applying a change batch from a different
// primary WAL incarnation than the snapshot the receiver bootstrapped from.
// The server normally catches this before it reaches storage, but enforcing it
// here as well makes the package API safe for callers that consume a feed
// directly rather than through cmd/server.
var ErrReplicaEpochMismatch = errors.New("replica WAL epoch does not match its bootstrap snapshot")

// ErrReplicaWALOrder rejects a malformed or out-of-order committed-record
// batch. A feed may legitimately omit BEGIN/COMMIT marker LSNs, so this is a
// strict ordering check rather than a false requirement for contiguous LSNs.
var ErrReplicaWALOrder = errors.New("replica WAL records are not strictly ordered")

// ReadCommittedSince returns every WALRecord belonging to a COMMITTED
// transaction in wal's on-disk log whose LSN is greater than sinceLSN, in
// LSN order, plus the resume LSN a caller should pass as sinceLSN on its
// next call (the highest LSN returned, or sinceLSN unchanged if nothing new
// was found).
//
// Records belonging to a transaction that aborts, or that never reaches a
// terminal BEGIN/COMMIT/ABORT state (e.g. a crash mid-transaction, or a
// transaction still open at the moment this is called), are never
// returned — operations are buffered per transaction as they're read and
// only released into the result once that transaction's COMMIT record is
// seen, mirroring exactly how Recover distinguishes committed from
// pending/aborted transactions before replaying anything. A replication
// feed must never apply an uncommitted operation, so this filtering is not
// optional.
//
// Like Recover, a record at or below wal's checkpoint watermark is skipped:
// it is already reflected in the checkpoint snapshot loaded when the WAL was
// opened, and would only still be on disk because of a crash between that
// snapshot's save and the WAL truncation that normally removes it.
//
// ReadCommittedSince holds wal's lock for the duration of the read, the same
// as Recover, so it does not observe a WAL file mid-write.
func ReadCommittedSince(wal *AdvancedWAL, sinceLSN uint64) ([]WALRecord, uint64, error) {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	// Compares against checkpointDataWatermark, not checkpointWatermark: for
	// ordinary row work it is the final real operation LSN the snapshot
	// contains, while the latter is the later checkpoint marker. A feed resume
	// token is likewise an operation LSN, so comparing against the marker would
	// reject a replica that already has all row state. Metadata-only checkpoints
	// deliberately store their marker in checkpointDataWatermark because no
	// incremental record can reconstruct their schema/catalog mutation.
	if sinceLSN < uint64(wal.checkpointDataWatermark) {
		return nil, sinceLSN, ErrReplicaTooFarBehind
	}

	file, err := os.Open(wal.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, sinceLSN, nil
		}
		return nil, sinceLSN, err
	}
	defer func() { _ = file.Close() }()

	dec := gob.NewDecoder(file)

	// Same bookkeeping as Recover: operations are buffered per transaction
	// in pending and only moved into results when that transaction's COMMIT
	// record is seen. committed/aborted exist only so the WALOpCheckpoint
	// case below can tell "already resolved, already removed from pending"
	// apart from "still genuinely in flight" — exactly as in Recover.
	pending := make(map[TxID][]*WALRecord)
	committed := make(map[TxID]bool)
	aborted := make(map[TxID]bool)

	var results []WALRecord

	for {
		var record WALRecord
		if decErr := dec.Decode(&record); decErr != nil {
			if errors.Is(decErr, io.EOF) {
				break
			}
			// Corruption: stop reading here, the same tolerant behavior
			// Recover uses rather than surfacing a hard error.
			break
		}

		if record.Checksum != wal.calculateChecksum(&record) && record.Checksum != wal.legacyChecksum(&record) {
			break
		}

		if record.LSN <= wal.checkpointWatermark {
			continue
		}

		switch record.OpType {
		case WALOpBegin:
			pending[record.TxID] = make([]*WALRecord, 0)

		case WALOpInsert, WALOpUpdate, WALOpDelete:
			if _, exists := pending[record.TxID]; exists {
				rec := record
				pending[record.TxID] = append(pending[record.TxID], &rec)
			}

		case WALOpCommit:
			committed[record.TxID] = true
			if ops, exists := pending[record.TxID]; exists {
				for _, op := range ops {
					if uint64(op.LSN) > sinceLSN {
						results = append(results, *op)
					}
				}
				delete(pending, record.TxID)
			}

		case WALOpAbort:
			aborted[record.TxID] = true
			delete(pending, record.TxID)

		case WALOpCheckpoint:
			for txID := range pending {
				if !committed[txID] && !aborted[txID] {
					delete(pending, txID)
				}
			}
		}
	}

	sort.Slice(results, func(i, j int) bool { return results[i].LSN < results[j].LSN })

	resume := sinceLSN
	if len(results) > 0 {
		resume = uint64(results[len(results)-1].LSN)
	}

	return results, resume, nil
}

// ApplyWALRecord applies one already-committed row operation under DB's
// content write lock. It remains as the compatibility API for embedders; feed
// consumers should prefer ApplyReplicaWALRecords so one response becomes one
// reader-visible atomic batch and duplicate delivery is harmless.
func ApplyWALRecord(db *DB, record *WALRecord) (*Table, error) {
	if db == nil || record == nil {
		return nil, errors.New("nil database or WAL record")
	}
	db.LockContentForWrite()
	defer db.UnlockContentForWrite()
	// Direct streams retain their source *Table after releasing contentMu.
	// Keep that source immutable even when an embedder applies one record via
	// this compatibility API rather than through engine.Execute.
	db.DetachPinnedTableForWrite(record.Tenant, record.Table)
	table, err := applyOperation(db, record)
	if err != nil {
		return nil, err
	}
	if table != nil {
		if err := finalizeReplicaTables(map[*Table]struct{}{table: {}}); err != nil {
			return nil, err
		}
	}
	return table, nil
}

// InitializeReplicaState records the epoch and LSN watermark that accompanied
// a Bootstrap snapshot. It must run immediately after LoadFromBytes and before
// applying changes; together with ApplyReplicaWALRecords it makes a retry of a
// successfully received gRPC response idempotent instead of duplicating its
// INSERT operations.
func InitializeReplicaState(db *DB, epoch, watermark uint64) error {
	if db == nil {
		return errors.New("nil database")
	}
	db.LockContentForWrite()
	defer db.UnlockContentForWrite()
	if db.replicaEpoch != 0 && epoch != db.replicaEpoch {
		return ErrReplicaEpochMismatch
	}
	if watermark < db.replicaAppliedLSN {
		return fmt.Errorf("replica bootstrap watermark %d is behind applied LSN %d", watermark, db.replicaAppliedLSN)
	}
	db.replicaEpoch = epoch
	db.replicaAppliedLSN = watermark
	return nil
}

// ReplicaApplyWatermark returns the in-memory source epoch and highest applied
// operation/Bootstrap LSN. It is primarily useful to package users exposing
// replication health; v1 replicas intentionally bootstrap again on process
// restart, so this state is not persisted separately from their snapshot.
func ReplicaApplyWatermark(db *DB) (epoch, appliedLSN uint64) {
	if db == nil {
		return 0, 0
	}
	db.LockContentForRead()
	defer db.UnlockContentForRead()
	return db.replicaEpoch, db.replicaAppliedLSN
}

// ApplyReplicaWALRecords applies one committed feed batch atomically with
// respect to readers and tracks its source LSN for idempotent retries. The
// input is expected to contain only INSERT/UPDATE/DELETE records from
// ReadCommittedSince; marker records are rejected rather than silently
// treating an unexpected transport payload as a no-op.
//
// The full statement snapshot is deliberately used only for a non-empty batch
// of new records. A replication response may touch several tables, and a
// later bad record or secondary-index rebuild failure must not leave the
// replica half advanced while its resume LSN stays unchanged. Normal responses
// are small and the safety property matters more than optimizing that rare
// failure path.
func ApplyReplicaWALRecords(db *DB, epoch uint64, records []WALRecord) (int, error) {
	if db == nil {
		return 0, errors.New("nil database")
	}
	db.LockContentForWrite()
	defer db.UnlockContentForWrite()

	if db.replicaEpoch != epoch {
		return 0, ErrReplicaEpochMismatch
	}

	newRecords := make([]WALRecord, 0, len(records))
	var previous LSN
	for i := range records {
		record := records[i]
		if record.OpType != WALOpInsert && record.OpType != WALOpUpdate && record.OpType != WALOpDelete {
			return 0, fmt.Errorf("replica record %d has unsupported operation %s", record.LSN, record.OpType)
		}
		if record.LSN == 0 {
			return 0, fmt.Errorf("replica record has zero LSN")
		}
		if previous != 0 && record.LSN <= previous {
			return 0, ErrReplicaWALOrder
		}
		previous = record.LSN
		if record.LSN > LSN(db.replicaAppliedLSN) {
			newRecords = append(newRecords, record)
		}
	}
	if len(newRecords) == 0 {
		return 0, nil
	}

	// Detach every potentially affected streamed table before the rollback
	// snapshot captures table pointers. applyOperation looks the mapping up
	// after this point, so it mutates the clone while a direct ResultStream
	// continues to read its original immutable source. The first detach for a
	// table swaps the mapping; repeated records then see the unpinned clone and
	// do no additional copying.
	for i := range newRecords {
		db.DetachPinnedTableForWrite(newRecords[i].Tenant, newRecords[i].Table)
	}
	snapshot := db.SnapshotForStatement()
	defer db.ReleaseStatementSnapshot(snapshot)
	touched := make(map[*Table]struct{})
	rollback := func(err error) (int, error) {
		db.RestoreStatementSnapshot(snapshot)
		return 0, err
	}

	for i := range newRecords {
		table, err := applyOperation(db, &newRecords[i])
		if err != nil {
			return rollback(fmt.Errorf("apply replica WAL record lsn=%d: %w", newRecords[i].LSN, err))
		}
		if table != nil {
			touched[table] = struct{}{}
		}
	}
	if err := finalizeReplicaTables(touched); err != nil {
		return rollback(fmt.Errorf("finalize replica WAL batch: %w", err))
	}

	db.replicaAppliedLSN = uint64(newRecords[len(newRecords)-1].LSN)
	return len(newRecords), nil
}

// finalizeReplicaTables rebuilds all derived state exactly once per affected
// table. applyOperation intentionally leaves that work to its caller so crash
// recovery and replication can batch thousands of row operations without an
// O(rows log rows) rebuild after each individual change.
func finalizeReplicaTables(touched map[*Table]struct{}) error {
	for table := range touched {
		if err := table.RebuildSecondaryIndexes(); err != nil {
			return err
		}
		table.InvalidateStats()
		table.MarkDirtyFrom(-1)
	}
	return nil
}

// SnapshotWithWatermark takes a consistent snapshot of db together with the
// WAL LSN watermark up to which that snapshot is known to be complete and
// wal's current epoch identifier, for a replication feed to use as its
// starting point (hand the bytes to LoadFromBytes, then call
// ReadCommittedSince(wal, watermark) for everything after). A caller should
// remember epoch alongside watermark and compare it against whatever epoch
// every later GetChangesSince response carries (see cmd/server's Bootstrap/
// GetChangesSince handlers) -- a mismatch means the primary's WAL/checkpoint
// files were wiped or restored from backup, and calling this again for a
// fresh snapshot is the only safe response (see AdvancedWAL.epoch's doc
// comment).
//
// It holds DB's content read lock first and then wal's lock for the whole
// encode. SQL mutations hold the same content lock in write mode across their
// in-memory change and WAL append/commit, so no snapshot can now land between
// those two steps. The lock order matches Checkpoint exactly and is important:
// acquiring wal first and then waiting for DB content would deadlock against a
// writer that already holds content and is trying to append its WAL record.
// The watermark and epoch are read directly from wal's fields because the
// exported accessors take the same non-reentrant mutex.
func SnapshotWithWatermark(db *DB, wal *AdvancedWAL) (data []byte, watermark uint64, epoch uint64, err error) {
	if db == nil || wal == nil {
		return nil, 0, 0, errors.New("nil database or advanced WAL")
	}
	db.LockContentForRead()
	defer db.UnlockContentForRead()
	wal.mu.Lock()
	defer wal.mu.Unlock()

	if wal.closed {
		return nil, 0, 0, errors.New("advanced WAL is closed")
	}

	// A normal snapshot advances through the most recent COMMIT marker. A
	// metadata-only checkpoint may instead require its own later marker as the
	// feed boundary because DDL/catalog work has no row record to ship; return
	// the higher value so a freshly bootstrapped replica is never rejected as
	// "behind" the very snapshot it just received.
	watermark = uint64(wal.committedLSN)
	if boundary := uint64(wal.checkpointDataWatermark); boundary > watermark {
		watermark = boundary
	}
	epoch = wal.epoch

	data, err = SaveToBytes(db, watermark)
	if err != nil {
		return nil, 0, 0, err
	}

	return data, watermark, epoch, nil
}
