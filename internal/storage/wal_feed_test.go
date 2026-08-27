package storage

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// TestReadCommittedSinceExcludesAbortedAndFiltersBySinceLSN builds a small,
// hand-driven WAL (via AdvancedWAL's real Log* API, the same way
// wal_advanced_test.go does) containing a committed transaction, an aborted
// transaction, and a second committed transaction, then checks
// ReadCommittedSince only ever returns records from committed transactions
// and correctly filters by sinceLSN.
func TestReadCommittedSinceExcludesAbortedAndFiltersBySinceLSN(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "feed.wal")

	wal, err := OpenAdvancedWAL(AdvancedWALConfig{Path: walPath})
	if err != nil {
		t.Fatalf("OpenAdvancedWAL: %v", err)
	}
	defer func() { _ = wal.Close() }()

	cols := []Column{{Name: "id", Type: IntType}}

	// Tx 1: committed insert.
	if _, err := wal.LogBegin(1); err != nil {
		t.Fatalf("LogBegin(1): %v", err)
	}
	insLSN1, err := wal.LogInsert(1, "default", "t", 1, []any{1}, cols)
	if err != nil {
		t.Fatalf("LogInsert(1): %v", err)
	}
	if _, err := wal.LogCommit(1); err != nil {
		t.Fatalf("LogCommit(1): %v", err)
	}

	// Tx 2: aborted insert -- must never be returned by ReadCommittedSince.
	if _, err := wal.LogBegin(2); err != nil {
		t.Fatalf("LogBegin(2): %v", err)
	}
	if _, err := wal.LogInsert(2, "default", "t", 2, []any{2}, cols); err != nil {
		t.Fatalf("LogInsert(2): %v", err)
	}
	if _, err := wal.LogAbort(2); err != nil {
		t.Fatalf("LogAbort(2): %v", err)
	}
	// LogAbort (unlike LogCommit) does not flush the buffered writer. Force
	// the abort's insert record onto disk here so the assertions below
	// genuinely exercise ReadCommittedSince's abort filtering, rather than
	// passing merely because the record never reached the file.
	if err := wal.flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// Tx 3: committed insert, after the aborted transaction.
	if _, err := wal.LogBegin(3); err != nil {
		t.Fatalf("LogBegin(3): %v", err)
	}
	insLSN3, err := wal.LogInsert(3, "default", "t", 3, []any{3}, cols)
	if err != nil {
		t.Fatalf("LogInsert(3): %v", err)
	}
	if _, err := wal.LogCommit(3); err != nil {
		t.Fatalf("LogCommit(3): %v", err)
	}

	// From the very start: both committed inserts, none of the aborted one.
	records, resume, err := ReadCommittedSince(wal, 0)
	if err != nil {
		t.Fatalf("ReadCommittedSince: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("got %d records, want 2: %+v", len(records), records)
	}
	if records[0].LSN != insLSN1 || records[1].LSN != insLSN3 {
		t.Fatalf("record LSNs = %d, %d; want %d, %d", records[0].LSN, records[1].LSN, insLSN1, insLSN3)
	}
	for _, r := range records {
		if r.TxID == 2 {
			t.Fatalf("aborted transaction's record leaked into result: %+v", r)
		}
	}
	if resume != uint64(insLSN3) {
		t.Fatalf("resume = %d, want %d", resume, insLSN3)
	}

	// From just after tx1's insert: only tx3's insert should come back.
	records2, resume2, err := ReadCommittedSince(wal, uint64(insLSN1))
	if err != nil {
		t.Fatalf("ReadCommittedSince (since insLSN1): %v", err)
	}
	if len(records2) != 1 || records2[0].LSN != insLSN3 {
		t.Fatalf("got %+v, want exactly tx3's insert at LSN %d", records2, insLSN3)
	}
	if resume2 != uint64(insLSN3) {
		t.Fatalf("resume2 = %d, want %d", resume2, insLSN3)
	}

	// From the last returned LSN onward: nothing new, resume stays put.
	records3, resume3, err := ReadCommittedSince(wal, uint64(insLSN3))
	if err != nil {
		t.Fatalf("ReadCommittedSince (since insLSN3): %v", err)
	}
	if len(records3) != 0 {
		t.Fatalf("got %d records, want 0: %+v", len(records3), records3)
	}
	if resume3 != uint64(insLSN3) {
		t.Fatalf("resume3 = %d, want unchanged %d", resume3, insLSN3)
	}
}

// TestReadCommittedSinceReturnsErrReplicaTooFarBehindAfterCheckpoint checks
// the other half of the checkpoint-outran-replica case (see cmd/server's
// replica_test.go for the end-to-end poll-loop behavior this enables): once
// a checkpoint has truncated the live WAL, asking ReadCommittedSince for
// anything at or before the new checkpoint watermark must fail loudly with
// ErrReplicaTooFarBehind instead of silently returning an empty (and wrong)
// result for a range that was actually never scanned.
func TestReadCommittedSinceReturnsErrReplicaTooFarBehindAfterCheckpoint(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "outrun.wal")
	checkpointPath := filepath.Join(tmpDir, "outrun.chk")

	wal, err := OpenAdvancedWAL(AdvancedWALConfig{Path: walPath, CheckpointPath: checkpointPath})
	if err != nil {
		t.Fatalf("OpenAdvancedWAL: %v", err)
	}
	defer func() { _ = wal.Close() }()

	db := NewDB()
	cols := []Column{{Name: "id", Type: IntType}}

	if _, err := wal.LogBegin(1); err != nil {
		t.Fatalf("LogBegin: %v", err)
	}
	insLSN, err := wal.LogInsert(1, "default", "t", 1, []any{1}, cols)
	if err != nil {
		t.Fatalf("LogInsert: %v", err)
	}
	if _, err := wal.LogCommit(1); err != nil {
		t.Fatalf("LogCommit: %v", err)
	}

	// A read for exactly what was just committed still works: nothing has
	// been checkpointed away yet.
	if _, _, err := ReadCommittedSince(wal, 0); err != nil {
		t.Fatalf("ReadCommittedSince before checkpoint: %v", err)
	}

	if err := wal.Checkpoint(db); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}

	// sinceLSN below insLSN now falls behind the checkpoint's data watermark
	// the Checkpoint call above just advanced past it -- the record is only
	// still reachable via the checkpoint snapshot, not the (now-truncated)
	// live WAL, so this must be reported, not silently answered with zero
	// records.
	if _, _, err := ReadCommittedSince(wal, uint64(insLSN)-1); !errors.Is(err, ErrReplicaTooFarBehind) {
		t.Fatalf("ReadCommittedSince(sinceLSN=%d) after checkpoint = %v, want ErrReplicaTooFarBehind", uint64(insLSN)-1, err)
	}

	// ReadCommittedSince returns the final row-operation LSN as its resume
	// token, not the invisible COMMIT marker. A replica that successfully
	// applied that response is caught up and must remain eligible after the
	// primary checkpoints; using the later marker as checkpointDataWatermark
	// here would turn this into a false ErrReplicaTooFarBehind.
	if _, _, err := ReadCommittedSince(wal, uint64(insLSN)); err != nil {
		t.Fatalf("ReadCommittedSince(sinceLSN=last operation) after checkpoint: %v", err)
	}

	// A Bootstrap returns the COMMIT marker as a high-but-safe resume value in
	// the normal row-change case, which must remain accepted as well.
	committedBeforeCheckpoint := wal.GetCommittedLSN()
	if committedBeforeCheckpoint != insLSN+1 {
		t.Fatalf("wal.GetCommittedLSN() = %d, want %d (the commit's own LSN)", committedBeforeCheckpoint, insLSN+1)
	}
	if _, _, err := ReadCommittedSince(wal, uint64(committedBeforeCheckpoint)); err != nil {
		t.Fatalf("ReadCommittedSince(sinceLSN=committedLSN) after checkpoint: %v", err)
	}
}

// TestApplyWALRecordInsertUpdateDelete builds WALRecords by hand (no WAL
// file involved) and checks ApplyWALRecord inserts, updates, and deletes
// rows the same way applyOperation / Recover would.
func TestApplyWALRecordInsertUpdateDelete(t *testing.T) {
	db := NewDB()
	cols := []Column{
		{Name: "id", Type: IntType},
		{Name: "name", Type: StringType},
	}

	insert := &WALRecord{
		OpType:     WALOpInsert,
		Tenant:     "default",
		Table:      "people",
		AfterImage: []any{1, "Alice"},
		Columns:    cols,
	}
	if _, err := ApplyWALRecord(db, insert); err != nil {
		t.Fatalf("ApplyWALRecord(insert): %v", err)
	}
	tbl, err := db.Get("default", "people")
	if err != nil {
		t.Fatalf("Get after insert: %v", err)
	}
	if len(tbl.Rows) != 1 || tbl.Rows[0][1] != "Alice" {
		t.Fatalf("after insert, rows = %+v", tbl.Rows)
	}

	update := &WALRecord{
		OpType:      WALOpUpdate,
		Tenant:      "default",
		Table:       "people",
		BeforeImage: []any{1, "Alice"},
		AfterImage:  []any{1, "Alicia"},
		Columns:     cols,
	}
	if _, err := ApplyWALRecord(db, update); err != nil {
		t.Fatalf("ApplyWALRecord(update): %v", err)
	}
	tbl, err = db.Get("default", "people")
	if err != nil {
		t.Fatalf("Get after update: %v", err)
	}
	if len(tbl.Rows) != 1 || tbl.Rows[0][1] != "Alicia" {
		t.Fatalf("after update, rows = %+v", tbl.Rows)
	}

	del := &WALRecord{
		OpType:      WALOpDelete,
		Tenant:      "default",
		Table:       "people",
		BeforeImage: []any{1, "Alicia"},
		Columns:     cols,
	}
	if _, err := ApplyWALRecord(db, del); err != nil {
		t.Fatalf("ApplyWALRecord(delete): %v", err)
	}
	tbl, err = db.Get("default", "people")
	if err != nil {
		t.Fatalf("Get after delete: %v", err)
	}
	if len(tbl.Rows) != 0 {
		t.Fatalf("after delete, rows = %+v, want empty", tbl.Rows)
	}
}

// TestSnapshotWithWatermarkRoundTrips checks that the bytes SnapshotWithWatermark
// returns round-trip through LoadFromBytes (data plus the embedded watermark),
// and that the reported watermark matches wal.GetCommittedLSN() at the time
// of the call.
func TestSnapshotWithWatermarkRoundTrips(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "snap.wal")

	wal, err := OpenAdvancedWAL(AdvancedWALConfig{Path: walPath})
	if err != nil {
		t.Fatalf("OpenAdvancedWAL: %v", err)
	}
	defer func() { _ = wal.Close() }()

	db := NewDB()
	cols := []Column{{Name: "id", Type: IntType}}
	tbl := NewTable("t", cols, false)
	tbl.Rows = append(tbl.Rows, []any{1})
	if err := db.Put("default", tbl); err != nil {
		t.Fatalf("Put: %v", err)
	}

	if _, err := wal.LogBegin(1); err != nil {
		t.Fatalf("LogBegin: %v", err)
	}
	if _, err := wal.LogInsert(1, "default", "t", 1, []any{1}, cols); err != nil {
		t.Fatalf("LogInsert: %v", err)
	}
	commitLSN, err := wal.LogCommit(1)
	if err != nil {
		t.Fatalf("LogCommit: %v", err)
	}

	data, watermark, epoch, err := SnapshotWithWatermark(db, wal)
	if err != nil {
		t.Fatalf("SnapshotWithWatermark: %v", err)
	}
	if watermark != uint64(commitLSN) {
		t.Fatalf("watermark = %d, want %d", watermark, commitLSN)
	}
	if got := wal.GetCommittedLSN(); watermark != uint64(got) {
		t.Fatalf("watermark = %d, GetCommittedLSN() = %d", watermark, got)
	}
	if epoch == 0 {
		t.Fatal("expected a nonzero epoch from a freshly created AdvancedWAL")
	}
	if got := wal.Epoch(); epoch != got {
		t.Fatalf("epoch = %d, wal.Epoch() = %d", epoch, got)
	}

	var gotWatermark uint64
	loaded, err := LoadFromBytes(data, &gotWatermark)
	if err != nil {
		t.Fatalf("LoadFromBytes: %v", err)
	}
	if gotWatermark != watermark {
		t.Fatalf("decoded watermark = %d, want %d", gotWatermark, watermark)
	}
	loadedTbl, err := loaded.Get("default", "t")
	if err != nil {
		t.Fatalf("Get after LoadFromBytes: %v", err)
	}
	if len(loadedTbl.Rows) != 1 {
		t.Fatalf("loaded rows = %+v, want 1 row", loadedTbl.Rows)
	}
}

// TestAdvancedWALEpochPersistsAcrossReopenButChangesAfterWipe checks both
// halves of the epoch safety net: reopening the exact same WAL/checkpoint
// pair (an ordinary restart) must see the same epoch every time -- it must
// never churn just because the process restarted -- while reopening at the
// same path after both files have been deleted (simulating a wipe or a
// restore from backup) must mint a different one, exactly the signal a
// replica uses to detect it can no longer trust its remembered LSN (see
// cmd/server/replica_test.go for the end-to-end replica-side behavior this
// enables).
func TestAdvancedWALEpochPersistsAcrossReopenButChangesAfterWipe(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "epoch.wal")
	checkpointPath := filepath.Join(tmpDir, "epoch.chk")
	cfg := AdvancedWALConfig{Path: walPath, CheckpointPath: checkpointPath}

	wal1, err := OpenAdvancedWAL(cfg)
	if err != nil {
		t.Fatalf("OpenAdvancedWAL (first): %v", err)
	}
	epoch1 := wal1.Epoch()
	if epoch1 == 0 {
		t.Fatal("expected a nonzero epoch from a freshly created AdvancedWAL")
	}

	// Persist it via a checkpoint (the only mechanism epoch has, mirroring
	// checkpointWatermark -- see AdvancedWAL.epoch's doc comment), then
	// close cleanly, simulating an ordinary restart.
	db := NewDB()
	if err := wal1.Checkpoint(db); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := wal1.Close(); err != nil {
		t.Fatalf("Close (first): %v", err)
	}

	wal2, err := OpenAdvancedWAL(cfg)
	if err != nil {
		t.Fatalf("OpenAdvancedWAL (reopen): %v", err)
	}
	if got := wal2.Epoch(); got != epoch1 {
		t.Fatalf("epoch after a plain reopen = %d, want unchanged %d", got, epoch1)
	}
	if err := wal2.Close(); err != nil {
		t.Fatalf("Close (reopen): %v", err)
	}

	// Simulate a wipe / restore-from-backup: both files gone, so the next
	// open sees no pre-existing WAL and must mint a fresh epoch.
	if err := os.Remove(walPath); err != nil {
		t.Fatalf("remove WAL: %v", err)
	}
	if err := os.Remove(checkpointPath); err != nil {
		t.Fatalf("remove checkpoint: %v", err)
	}

	wal3, err := OpenAdvancedWAL(cfg)
	if err != nil {
		t.Fatalf("OpenAdvancedWAL (after wipe): %v", err)
	}
	defer func() { _ = wal3.Close() }()
	if got := wal3.Epoch(); got == epoch1 {
		t.Fatalf("epoch after a wipe = %d, want different from the pre-wipe epoch %d", got, epoch1)
	}
	if wal3.Epoch() == 0 {
		t.Fatal("expected a nonzero epoch after a wipe")
	}
}
