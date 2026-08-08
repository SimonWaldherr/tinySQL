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

	// Compares against checkpointDataWatermark, not checkpointWatermark: the
	// latter is the checkpoint marker's own LSN, which sits at least one
	// past the last real commit a checkpoint's snapshot actually captured.
	// sinceLSN is always a real operation's LSN (from a previous Bootstrap
	// or ReadCommittedSince call), so comparing against the marker's LSN
	// would reject a replica that already has everything real -- see
	// checkpointDataWatermark's doc comment (wal_advanced.go) for why that
	// is not just an occasional false positive but a standing risk of an
	// unbounded re-bootstrap loop against an idle primary.
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

// ApplyWALRecord applies a single WAL record to db, as a subscriber on the
// receiving end of a replication feed would for each record returned by
// ReadCommittedSince. It is a plain function wrapping the existing
// unexported applyOperation (used internally by Recover) so both paths
// share the exact same row-location semantics — value-equality against
// BeforeImage — instead of a second, potentially-diverging implementation.
func ApplyWALRecord(db *DB, record *WALRecord) (*Table, error) {
	return applyOperation(db, record)
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
// It mirrors Checkpoint's locking exactly: wal's lock is held for the whole
// duration, including the snapshot encode, so no LogInsert/LogUpdate/
// LogDelete/LogCommit/LogAbort call (all of which also take wal's lock) can
// interleave between reading the committed LSN and encoding the snapshot.
// The watermark and epoch are read directly from wal's committedLSN/epoch
// fields rather than through the exported GetCommittedLSN/Epoch methods,
// because those methods take the same lock themselves and sync.Mutex is not
// reentrant — calling either while already holding the lock here would
// deadlock.
//
// Known limitation inherited from Checkpoint, not fixed here: this closes
// one race (concurrent WAL-log calls) but not another, pre-existing one.
// The engine's DML dispatch mutates a table's rows and appends the
// corresponding WAL record as two separate steps under two different locks
// (db.mu for the row mutation, wal.mu for the WAL append), not atomically
// together. Holding wal.mu here blocks the second step but cannot see or
// block the first, so a snapshot taken here can race with an in-flight
// mutation whose row change has landed in memory but whose WAL record
// hasn't been appended yet (or vice versa) — the same snapshot/WAL-append
// race Checkpoint has always had.
func SnapshotWithWatermark(db *DB, wal *AdvancedWAL) (data []byte, watermark uint64, epoch uint64, err error) {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	if wal.closed {
		return nil, 0, 0, errors.New("advanced WAL is closed")
	}

	watermark = uint64(wal.committedLSN)
	epoch = wal.epoch

	data, err = SaveToBytes(db, watermark)
	if err != nil {
		return nil, 0, 0, err
	}

	return data, watermark, epoch, nil
}
