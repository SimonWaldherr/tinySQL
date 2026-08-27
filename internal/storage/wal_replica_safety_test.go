package storage

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSnapshotWithWatermarkWaitsForMutationAndCommit(t *testing.T) {
	tmp := t.TempDir()
	wal, err := OpenAdvancedWAL(AdvancedWALConfig{Path: filepath.Join(tmp, "primary.wal")})
	if err != nil {
		t.Fatalf("OpenAdvancedWAL: %v", err)
	}
	defer func() { _ = wal.Close() }()

	db := NewDB()
	cols := []Column{{Name: "id", Type: IntType}}
	table := NewTable("events", cols, false)
	if err := db.Put("default", table); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Keep the same content write lock an engine statement owns across its
	// in-memory mutation and WAL commit. A Bootstrap snapshot must not slip
	// into that interval and return the row with the old LSN watermark.
	db.LockContentForWrite()
	type snapshotResult struct {
		data      []byte
		watermark uint64
		err       error
	}
	done := make(chan snapshotResult, 1)
	go func() {
		data, watermark, _, snapErr := SnapshotWithWatermark(db, wal)
		done <- snapshotResult{data: data, watermark: watermark, err: snapErr}
	}()
	select {
	case result := <-done:
		t.Fatalf("snapshot completed while content write lock was held: %+v", result)
	case <-time.After(40 * time.Millisecond):
	}

	table.Rows = append(table.Rows, []any{1})
	table.Version++
	if _, err := wal.LogBegin(1); err != nil {
		t.Fatalf("LogBegin: %v", err)
	}
	if _, err := wal.LogInsert(1, "default", "events", 0, []any{1}, cols); err != nil {
		t.Fatalf("LogInsert: %v", err)
	}
	commitLSN, err := wal.LogCommit(1)
	if err != nil {
		t.Fatalf("LogCommit: %v", err)
	}
	db.UnlockContentForWrite()

	var result snapshotResult
	select {
	case result = <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("snapshot did not finish after content write lock was released")
	}
	if result.err != nil {
		t.Fatalf("SnapshotWithWatermark: %v", result.err)
	}
	if result.watermark != uint64(commitLSN) {
		t.Fatalf("snapshot watermark = %d, want committed LSN %d", result.watermark, commitLSN)
	}
	loaded, err := LoadFromBytes(result.data)
	if err != nil {
		t.Fatalf("LoadFromBytes: %v", err)
	}
	loadedTable, err := loaded.Get("default", "events")
	if err != nil {
		t.Fatalf("loaded table: %v", err)
	}
	if got := len(loadedTable.Rows); got != 1 {
		t.Fatalf("snapshot rows = %d, want 1", got)
	}
}

func TestAdvancedWALCheckpointPersistsBootstrapWatermarkAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "primary.wal")
	cfg := StorageConfig{
		Mode:               ModeAdvancedWAL,
		Path:               path,
		CheckpointEvery:    1 << 30,
		CheckpointInterval: time.Hour,
	}
	db, err := OpenDB(cfg)
	if err != nil {
		t.Fatalf("OpenDB first: %v", err)
	}

	cols := []Column{{Name: "id", Type: IntType}}
	table := NewTable("events", cols, false)
	if err := db.Put("default", table); err != nil {
		t.Fatalf("Put: %v", err)
	}
	wal := db.AdvancedWAL()
	if wal == nil {
		t.Fatal("missing advanced WAL")
	}
	db.LockContentForWrite()
	table.Rows = append(table.Rows, []any{1})
	table.Version++
	if _, err := wal.LogBegin(1); err != nil {
		t.Fatalf("LogBegin: %v", err)
	}
	insertLSN, err := wal.LogInsert(1, "default", "events", 0, []any{1}, cols)
	if err != nil {
		t.Fatalf("LogInsert: %v", err)
	}
	commitLSN, err := wal.LogCommit(1)
	db.UnlockContentForWrite()
	if err != nil {
		t.Fatalf("LogCommit: %v", err)
	}
	if err := wal.Checkpoint(db); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if got, err := ReadCheckpointDataWatermark(path + ".checkpoint"); err != nil || got != uint64(insertLSN) {
		t.Fatalf("checkpoint data watermark = %d, %v; want operation LSN %d, nil", got, err, insertLSN)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close first: %v", err)
	}

	reopened, err := OpenDB(cfg)
	if err != nil {
		t.Fatalf("OpenDB reopen: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	reopenedWAL := reopened.AdvancedWAL()
	if reopenedWAL == nil {
		t.Fatal("missing reopened advanced WAL")
	}
	_, watermark, _, err := SnapshotWithWatermark(reopened, reopenedWAL)
	if err != nil {
		t.Fatalf("SnapshotWithWatermark after reopen: %v", err)
	}
	if watermark != uint64(commitLSN) {
		t.Fatalf("post-reopen bootstrap watermark = %d, want %d", watermark, commitLSN)
	}
	if _, _, err := ReadCommittedSince(reopenedWAL, watermark); err != nil {
		t.Fatalf("fresh bootstrap watermark was rejected after reopen: %v", err)
	}
}

func TestModeAdvancedWALAutomaticallyCheckpointsCommittedWork(t *testing.T) {
	path := filepath.Join(t.TempDir(), "automatic.wal")
	db, err := OpenDB(StorageConfig{
		Mode:               ModeAdvancedWAL,
		Path:               path,
		CheckpointEvery:    1,
		CheckpointInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	defer func() { _ = db.Close() }()

	cols := []Column{{Name: "id", Type: IntType}}
	table := NewTable("events", cols, false)
	if err := db.Put("default", table); err != nil {
		t.Fatalf("Put: %v", err)
	}
	wal := db.AdvancedWAL()
	if wal == nil {
		t.Fatal("missing advanced WAL")
	}
	db.LockContentForWrite()
	table.Rows = append(table.Rows, []any{1})
	table.Version++
	if _, err := wal.LogBegin(1); err != nil {
		t.Fatalf("LogBegin: %v", err)
	}
	insertLSN, err := wal.LogInsert(1, "default", "events", 0, []any{1}, cols)
	if err != nil {
		t.Fatalf("LogInsert: %v", err)
	}
	_, err = wal.LogCommit(1)
	db.UnlockContentForWrite()
	if err != nil {
		t.Fatalf("LogCommit: %v", err)
	}

	checkpointPath := path + ".checkpoint"
	deadline := time.Now().Add(2 * time.Second)
	for {
		watermark, readErr := ReadCheckpointDataWatermark(checkpointPath)
		if readErr == nil && watermark == uint64(insertLSN) {
			break
		}
		if time.Now().After(deadline) {
			info, statErr := os.Stat(checkpointPath)
			t.Fatalf("automatic checkpoint did not persist operation LSN %d (watermark=%d err=%v stat=%v info=%v)", insertLSN, watermark, readErr, statErr, info)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if wal.ShouldCheckpoint() {
		t.Fatal("automatic checkpoint left WAL above its configured threshold")
	}
}

func TestModeAdvancedWALAutomaticallyCheckpointsOnInterval(t *testing.T) {
	path := filepath.Join(t.TempDir(), "automatic-interval.wal")
	db, err := OpenDB(StorageConfig{
		Mode:               ModeAdvancedWAL,
		Path:               path,
		CheckpointEvery:    1 << 30,
		CheckpointInterval: 15 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	defer func() { _ = db.Close() }()

	cols := []Column{{Name: "id", Type: IntType}}
	table := NewTable("events", cols, false)
	if err := db.Put("default", table); err != nil {
		t.Fatalf("Put: %v", err)
	}
	wal := db.AdvancedWAL()
	if wal == nil {
		t.Fatal("missing advanced WAL")
	}
	db.LockContentForWrite()
	table.Rows = append(table.Rows, []any{1})
	table.Version++
	if _, err := wal.LogBegin(1); err != nil {
		db.UnlockContentForWrite()
		t.Fatalf("LogBegin: %v", err)
	}
	insertLSN, err := wal.LogInsert(1, "default", "events", 0, []any{1}, cols)
	if err != nil {
		db.UnlockContentForWrite()
		t.Fatalf("LogInsert: %v", err)
	}
	_, err = wal.LogCommit(1)
	db.UnlockContentForWrite()
	if err != nil {
		t.Fatalf("LogCommit: %v", err)
	}

	checkpointPath := path + ".checkpoint"
	deadline := time.Now().Add(2 * time.Second)
	for {
		watermark, readErr := ReadCheckpointDataWatermark(checkpointPath)
		if readErr == nil && watermark == uint64(insertLSN) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("interval checkpoint did not persist operation LSN %d (watermark=%d err=%v)", insertLSN, watermark, readErr)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestMetadataCheckpointForcesOlderReplicaToBootstrap(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metadata.wal")
	checkpointPath := path + ".checkpoint"
	wal, err := OpenAdvancedWAL(AdvancedWALConfig{Path: path, CheckpointPath: checkpointPath})
	if err != nil {
		t.Fatalf("OpenAdvancedWAL: %v", err)
	}
	defer func() { _ = wal.Close() }()
	db := NewDB()
	if err := db.Put("default", NewTable("schema_only", []Column{{Name: "id", Type: IntType}}, false)); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// No row operation exists for this schema change. Marking it before the
	// checkpoint makes the marker an invalidation boundary for older replicas.
	wal.RequestCheckpoint()
	if err := wal.Checkpoint(db); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	boundary, err := ReadCheckpointDataWatermark(checkpointPath)
	if err != nil || boundary == 0 {
		t.Fatalf("metadata checkpoint boundary = %d, %v; want non-zero marker", boundary, err)
	}
	if _, _, err := ReadCommittedSince(wal, 0); !errors.Is(err, ErrReplicaTooFarBehind) {
		t.Fatalf("older replica did not require bootstrap: %v", err)
	}
	// A later abort-only checkpoint must retain that boundary. Otherwise it
	// would lower the feed floor back to the prior row LSN and allow a replica
	// that missed the DDL to continue with a stale schema.
	if _, err := wal.LogBegin(99); err != nil {
		t.Fatalf("LogBegin abort-only follow-up: %v", err)
	}
	if _, err := wal.LogAbort(99); err != nil {
		t.Fatalf("LogAbort abort-only follow-up: %v", err)
	}
	if err := wal.Checkpoint(db); err != nil {
		t.Fatalf("Checkpoint after abort-only work: %v", err)
	}
	if afterBoundary, err := ReadCheckpointDataWatermark(checkpointPath); err != nil || afterBoundary < boundary {
		t.Fatalf("metadata checkpoint boundary regressed from %d to %d (%v)", boundary, afterBoundary, err)
	}
	if _, _, err := ReadCommittedSince(wal, 0); !errors.Is(err, ErrReplicaTooFarBehind) {
		t.Fatalf("abort-only checkpoint reopened stale replica feed: %v", err)
	}

	// The bootstrap snapshot itself advertises the marker boundary, so the
	// client that receives its schema stays eligible for future incremental
	// row changes instead of being rejected on its very next poll.
	_, watermark, _, err := SnapshotWithWatermark(db, wal)
	if err != nil {
		t.Fatalf("SnapshotWithWatermark: %v", err)
	}
	if watermark == 0 {
		t.Fatal("metadata bootstrap watermark is zero")
	}
	if _, _, err := ReadCommittedSince(wal, watermark); err != nil {
		t.Fatalf("fresh metadata bootstrap watermark was rejected: %v", err)
	}
}

func TestAdvancedWALCheckpointRejectsOpenTransaction(t *testing.T) {
	tmp := t.TempDir()
	checkpointPath := filepath.Join(tmp, "active.checkpoint")
	wal, err := OpenAdvancedWAL(AdvancedWALConfig{
		Path:           filepath.Join(tmp, "active.wal"),
		CheckpointPath: checkpointPath,
	})
	if err != nil {
		t.Fatalf("OpenAdvancedWAL: %v", err)
	}
	defer func() { _ = wal.Close() }()
	if _, err := wal.LogBegin(1); err != nil {
		t.Fatalf("LogBegin: %v", err)
	}
	if err := wal.Checkpoint(NewDB()); !errors.Is(err, ErrAdvancedWALCheckpointActiveTransactions) {
		t.Fatalf("Checkpoint with active transaction = %v, want ErrAdvancedWALCheckpointActiveTransactions", err)
	}
	if _, err := os.Stat(checkpointPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("checkpoint file exists after rejected checkpoint: %v", err)
	}
	if _, err := wal.LogAbort(1); err != nil {
		t.Fatalf("LogAbort: %v", err)
	}
	if err := wal.Checkpoint(NewDB()); err != nil {
		t.Fatalf("Checkpoint after abort: %v", err)
	}
}

func TestApplyReplicaWALRecordsIsLockedAndIdempotent(t *testing.T) {
	db := NewDB()
	const epoch = uint64(77)
	if err := InitializeReplicaState(db, epoch, 10); err != nil {
		t.Fatalf("InitializeReplicaState: %v", err)
	}
	cols := []Column{{Name: "id", Type: IntType}}
	records := []WALRecord{
		{LSN: 11, OpType: WALOpInsert, Tenant: "default", Table: "events", RowID: 0, AfterImage: []any{1}, Columns: cols},
		{LSN: 12, OpType: WALOpInsert, Tenant: "default", Table: "events", RowID: 1, AfterImage: []any{2}, Columns: cols},
	}

	// A reader pin blocks the whole incoming batch rather than allowing a
	// served replica to expose the first row of a committed two-row response.
	db.LockContentForRead()
	done := make(chan struct{})
	var applied int
	var applyErr error
	go func() {
		applied, applyErr = ApplyReplicaWALRecords(db, epoch, records)
		close(done)
	}()
	select {
	case <-done:
		t.Fatal("replica batch applied while a content reader was active")
	case <-time.After(40 * time.Millisecond):
	}
	db.UnlockContentForRead()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("replica batch did not finish after reader released")
	}
	if applyErr != nil {
		t.Fatalf("ApplyReplicaWALRecords: %v", applyErr)
	}
	if applied != len(records) {
		t.Fatalf("applied = %d, want %d", applied, len(records))
	}
	if epochGot, lsnGot := ReplicaApplyWatermark(db); epochGot != epoch || lsnGot != 12 {
		t.Fatalf("replica watermark = (%d, %d), want (%d, 12)", epochGot, lsnGot, epoch)
	}

	// A completed gRPC response can be retried after a transport failure. The
	// receiver-side watermark must make that harmless instead of duplicating
	// rows, which is especially important for INSERT-only RAG ingestion.
	applied, err := ApplyReplicaWALRecords(db, epoch, records)
	if err != nil {
		t.Fatalf("idempotent retry: %v", err)
	}
	if applied != 0 {
		t.Fatalf("idempotent retry applied %d records, want 0", applied)
	}
	table, err := db.Get("default", "events")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got := len(table.Rows); got != 2 {
		t.Fatalf("rows after retry = %d, want 2", got)
	}
	if _, err := ApplyReplicaWALRecords(db, epoch+1, nil); !errors.Is(err, ErrReplicaEpochMismatch) {
		t.Fatalf("epoch mismatch = %v, want ErrReplicaEpochMismatch", err)
	}
}

func TestReplicaApplyDetachesPinnedStreamTable(t *testing.T) {
	db := NewDB()
	cols := []Column{{Name: "id", Type: IntType}}
	table := NewTable("events", cols, false)
	table.Rows = append(table.Rows, []any{1})
	if err := db.Put("default", table); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// The compatibility single-record path must not mutate a direct stream's
	// source table in place.
	legacySource, releaseLegacy := pinReplicaTestTable(t, db, "default", "events")
	defer releaseLegacy()
	if _, err := ApplyWALRecord(db, &WALRecord{
		LSN: 1, OpType: WALOpInsert, Tenant: "default", Table: "events",
		RowID: 1, AfterImage: []any{2}, Columns: cols,
	}); err != nil {
		t.Fatalf("ApplyWALRecord: %v", err)
	}
	liveAfterSingle, err := db.Get("default", "events")
	if err != nil {
		t.Fatalf("Get after single apply: %v", err)
	}
	if liveAfterSingle == legacySource {
		t.Fatal("single-record replica apply mutated the table pinned by a stream")
	}
	if got := len(legacySource.Rows); got != 1 {
		t.Fatalf("single-record pinned stream source has %d rows, want 1", got)
	}

	// Apply the primary batch path against a newly pinned current table. Its
	// snapshot must be captured after the detach so a later rollback can never
	// restore row data into the stream's immutable source.
	batchSource, releaseBatch := pinReplicaTestTable(t, db, "default", "events")
	defer releaseBatch()
	const epoch = uint64(21)
	if err := InitializeReplicaState(db, epoch, 1); err != nil {
		t.Fatalf("InitializeReplicaState: %v", err)
	}
	applied, err := ApplyReplicaWALRecords(db, epoch, []WALRecord{{
		LSN: 2, OpType: WALOpInsert, Tenant: "default", Table: "events",
		RowID: 2, AfterImage: []any{3}, Columns: cols,
	}})
	if err != nil {
		t.Fatalf("ApplyReplicaWALRecords: %v", err)
	}
	if applied != 1 {
		t.Fatalf("applied = %d, want 1", applied)
	}
	liveAfterBatch, err := db.Get("default", "events")
	if err != nil {
		t.Fatalf("Get after batch apply: %v", err)
	}
	if liveAfterBatch == batchSource {
		t.Fatal("batch replica apply mutated the table pinned by a stream")
	}
	if got := len(batchSource.Rows); got != 2 {
		t.Fatalf("batch pinned stream source has %d rows, want 2", got)
	}
	if got := len(liveAfterBatch.Rows); got != 3 {
		t.Fatalf("live table has %d rows after applies, want 3", got)
	}
}

func pinReplicaTestTable(t *testing.T, db *DB, tenant, name string) (*Table, func()) {
	t.Helper()
	db.LockContentForRead()
	defer db.UnlockContentForRead()
	table, err := db.Get(tenant, name)
	if err != nil {
		t.Fatalf("Get table to pin: %v", err)
	}
	release, ok := db.PinTableForStream(table)
	if !ok {
		t.Fatal("PinTableForStream returned false")
	}
	return table, release
}
