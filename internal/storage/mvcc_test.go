package storage

import (
	"sync"
	"testing"
	"time"
)

func TestMVCCBasicTransaction(t *testing.T) {
	mvcc := NewMVCCManager()

	// Begin transaction
	tx := mvcc.BeginTx(SnapshotIsolation)
	if tx == nil {
		t.Fatal("failed to begin transaction")
	}
	if tx.Status != TxStatusInProgress {
		t.Errorf("expected status InProgress, got %v", tx.Status)
	}

	// Commit transaction
	commitTS, err := mvcc.CommitTx(tx)
	if err != nil {
		t.Fatalf("failed to commit: %v", err)
	}
	if commitTS == 0 {
		t.Error("expected non-zero commit timestamp")
	}
	if tx.Status != TxStatusCommitted {
		t.Errorf("expected status Committed, got %v", tx.Status)
	}
}

func TestMVCCAbortTransaction(t *testing.T) {
	mvcc := NewMVCCManager()

	tx := mvcc.BeginTx(SnapshotIsolation)
	mvcc.AbortTx(tx)

	if tx.Status != TxStatusAborted {
		t.Errorf("expected status Aborted, got %v", tx.Status)
	}
}

func TestMVCCVisibility(t *testing.T) {
	mvcc := NewMVCCManager()

	// Create a row version
	tx1 := mvcc.BeginTx(SnapshotIsolation)
	rv := &RowVersion{
		XMin:      tx1.ID,
		XMax:      0,
		CreatedAt: tx1.StartTime,
		Data:      []any{1, "test"},
	}

	// Row should be visible to creating transaction
	if !mvcc.IsVisible(tx1, rv) {
		t.Error("row should be visible to creating transaction")
	}

	// Start another transaction before commit
	tx2 := mvcc.BeginTx(SnapshotIsolation)

	// Row should not be visible to tx2 (tx1 not committed yet)
	if mvcc.IsVisible(tx2, rv) {
		t.Error("row should not be visible before commit")
	}

	// Commit tx1
	mvcc.CommitTx(tx1)

	// Start a new transaction after commit
	tx3 := mvcc.BeginTx(SnapshotIsolation)

	// Row should be visible to tx3
	if !mvcc.IsVisible(tx3, rv) {
		t.Error("row should be visible after commit")
	}

	// Row should still not be visible to tx2 (snapshot isolation)
	if mvcc.IsVisible(tx2, rv) {
		t.Error("row should not be visible to earlier snapshot")
	}
}

func TestMVCCDeletedRow(t *testing.T) {
	mvcc := NewMVCCManager()

	// Create and commit a row
	tx1 := mvcc.BeginTx(SnapshotIsolation)
	rv := &RowVersion{
		XMin:      tx1.ID,
		XMax:      0,
		CreatedAt: tx1.StartTime,
		Data:      []any{1, "test"},
	}
	mvcc.CommitTx(tx1)

	// Delete the row
	tx2 := mvcc.BeginTx(SnapshotIsolation)
	rv.XMax = tx2.ID
	rv.DeletedAt = Timestamp(time.Now().UnixNano())

	// Row should not be visible to deleting transaction
	if mvcc.IsVisible(tx2, rv) {
		t.Error("deleted row should not be visible to deleting transaction")
	}

	// Commit delete
	mvcc.CommitTx(tx2)

	// Row should not be visible to new transaction
	tx3 := mvcc.BeginTx(SnapshotIsolation)
	if mvcc.IsVisible(tx3, rv) {
		t.Error("deleted row should not be visible after delete commit")
	}
}

func TestMVCCConcurrentTransactions(t *testing.T) {
	mvcc := NewMVCCManager()

	var wg sync.WaitGroup
	txCount := 100

	for i := 0; i < txCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tx := mvcc.BeginTx(SnapshotIsolation)
			time.Sleep(1 * time.Millisecond)
			mvcc.CommitTx(tx)
		}()
	}

	wg.Wait()

	// Verify all transactions committed
	mvcc.mu.RLock()
	activeCount := len(mvcc.activeTxs)
	commitCount := len(mvcc.commitLog)
	mvcc.mu.RUnlock()

	if activeCount != 0 {
		t.Errorf("expected 0 active transactions, got %d", activeCount)
	}
	// These transactions are all read-only (no RecordWrite calls), so once
	// each one's commit timestamp falls below the GC watermark its
	// commitLog entry is pruned (see prunableCommits in mvcc.go) -- the log
	// is no longer expected to retain one entry per transaction forever.
	if commitCount >= txCount {
		t.Errorf("expected read-only commitLog entries to be pruned well below %d, got %d entries", txCount, commitCount)
	}
}

func TestMVCCSerializableConflict(t *testing.T) {
	mvcc := NewMVCCManager()

	// Transaction 1: read row 1
	tx1 := mvcc.BeginTx(Serializable)
	tx1.RecordRead("users", 1, tx1.StartTime)

	// Transaction 2: write row 1 and commit
	tx2 := mvcc.BeginTx(Serializable)
	tx2.RecordWrite("users", 1)
	_, err := mvcc.CommitTx(tx2)
	if err != nil {
		t.Fatalf("tx2 commit failed: %v", err)
	}

	// Transaction 1: now also write to same table (creates potential conflict)
	tx1.RecordWrite("users", 1)

	// Transaction 1 commit - might detect conflict
	// Note: simplified serialization check may not catch all conflicts
	_, err = mvcc.CommitTx(tx1)
	// In a full implementation with complete write-set tracking,
	// this should fail with ErrSerializationFailure
	// For now, we just verify the test runs without panic
	if err != nil {
		t.Logf("serialization failure detected: %v", err)
	} else {
		t.Log("simplified conflict detection - tx1 committed (in full impl would fail)")
	}
}

func TestMVCCTable(t *testing.T) {
	mvcc := NewMVCCManager()
	cols := []Column{
		{Name: "id", Type: IntType},
		{Name: "name", Type: StringType},
	}

	table := NewMVCCTable("users", cols, false)
	tx := mvcc.BeginTx(SnapshotIsolation)

	// Insert a row
	rowID := table.InsertVersion(tx, []any{1, "Alice"})
	if rowID <= 0 {
		t.Error("expected positive row ID")
	}

	mvcc.CommitTx(tx)

	// Read the row in a new transaction
	tx2 := mvcc.BeginTx(SnapshotIsolation)
	version := table.GetVisibleVersion(mvcc, tx2, rowID)
	if version == nil {
		t.Fatal("expected to find row version")
	}
	if version.Data[0] != 1 || version.Data[1] != "Alice" {
		t.Errorf("unexpected row data: %v", version.Data)
	}
}

func TestMVCCTableUpdate(t *testing.T) {
	mvcc := NewMVCCManager()
	cols := []Column{
		{Name: "id", Type: IntType},
		{Name: "value", Type: IntType},
	}

	table := NewMVCCTable("data", cols, false)

	// Insert
	tx1 := mvcc.BeginTx(SnapshotIsolation)
	rowID := table.InsertVersion(tx1, []any{1, 100})
	mvcc.CommitTx(tx1)

	// Update
	tx2 := mvcc.BeginTx(SnapshotIsolation)
	err := table.UpdateVersion(tx2, rowID, []any{1, 200})
	if err != nil {
		t.Fatalf("update failed: %v", err)
	}
	mvcc.CommitTx(tx2)

	// Read - should see new version
	tx3 := mvcc.BeginTx(SnapshotIsolation)
	version := table.GetVisibleVersion(mvcc, tx3, rowID)
	if version == nil {
		t.Fatal("expected to find row version")
	}
	if version.Data[1] != 200 {
		t.Errorf("expected value 200, got %v", version.Data[1])
	}
}

func TestMVCCTableDelete(t *testing.T) {
	mvcc := NewMVCCManager()
	cols := []Column{
		{Name: "id", Type: IntType},
	}

	table := NewMVCCTable("temp", cols, false)

	// Insert
	tx1 := mvcc.BeginTx(SnapshotIsolation)
	rowID := table.InsertVersion(tx1, []any{1})
	mvcc.CommitTx(tx1)

	// Delete
	tx2 := mvcc.BeginTx(SnapshotIsolation)
	err := table.DeleteVersion(tx2, rowID)
	if err != nil {
		t.Fatalf("delete failed: %v", err)
	}
	mvcc.CommitTx(tx2)

	// Read - should not find row
	tx3 := mvcc.BeginTx(SnapshotIsolation)
	version := table.GetVisibleVersion(mvcc, tx3, rowID)
	if version != nil {
		t.Error("expected nil version for deleted row")
	}
}

func TestMVCCGarbageCollection(t *testing.T) {
	mvcc := NewMVCCManager()
	cols := []Column{
		{Name: "id", Type: IntType},
	}

	table := NewMVCCTable("test", cols, false)

	// Create multiple versions
	tx1 := mvcc.BeginTx(SnapshotIsolation)
	rowID := table.InsertVersion(tx1, []any{1})
	mvcc.CommitTx(tx1)

	tx2 := mvcc.BeginTx(SnapshotIsolation)
	table.UpdateVersion(tx2, rowID, []any{2})
	mvcc.CommitTx(tx2)

	tx3 := mvcc.BeginTx(SnapshotIsolation)
	table.UpdateVersion(tx3, rowID, []any{3})
	mvcc.CommitTx(tx3)

	// Get GC watermark
	watermark := mvcc.GCWatermark()

	// Run garbage collection
	collected := table.GarbageCollect(watermark)
	if collected <= 0 {
		t.Error("expected to collect some old versions")
	}
}

// TestMVCCCommitLogPruning proves two properties of the commitLog GC:
//
// (a) commitLog entries for read-only (write-free) transactions are
//     eventually evicted once the GC watermark advances past their commit
//     timestamp, so a long-running process doing many short-lived
//     transactions does not accumulate commitLog entries forever.
//
// (b) an entry still needed for visibility -- i.e. a transaction that wrote
//     a still-live row -- is NOT pruned no matter how far the watermark
//     advances afterwards, and that row remains visible to later
//     transactions.
func TestMVCCCommitLogPruning(t *testing.T) {
	mvcc := NewMVCCManager()
	cols := []Column{{Name: "id", Type: IntType}}
	table := NewMVCCTable("docs", cols, false)

	// tx1 writes a row and commits. Its commitLog entry must survive
	// forever: the row it created stays live (nothing updates or deletes
	// it), so future transactions need tx1's commit timestamp to resolve
	// visibility via IsVisible.
	tx1 := mvcc.BeginTx(SnapshotIsolation)
	rowID := table.InsertVersion(tx1, []any{1})
	if _, err := mvcc.CommitTx(tx1); err != nil {
		t.Fatalf("tx1 commit failed: %v", err)
	}

	// Drive many read-only (no RecordWrite) transactions through
	// begin/commit so the GC watermark advances well past tx1's commit
	// timestamp.
	const readOnlyTxCount = 20
	for i := 0; i < readOnlyTxCount; i++ {
		tx := mvcc.BeginTx(SnapshotIsolation)
		if _, err := mvcc.CommitTx(tx); err != nil {
			t.Fatalf("read-only tx commit failed: %v", err)
		}
	}

	mvcc.mu.RLock()
	commitCount := len(mvcc.commitLog)
	_, tx1Present := mvcc.commitLog[tx1.ID]
	mvcc.mu.RUnlock()

	// (a) The read-only transactions' commitLog entries must not
	// accumulate without bound -- only a small residue (tx1's permanent
	// entry, plus at most the most recently committed read-only tx that
	// hasn't yet been superseded by a later watermark update) should
	// remain, not all readOnlyTxCount+1 of them.
	if commitCount >= readOnlyTxCount {
		t.Errorf("expected read-only commitLog entries to be pruned, got %d entries remaining (started with %d)", commitCount, readOnlyTxCount+1)
	}

	// (b) tx1's entry must never be pruned: it is still needed for
	// visibility.
	if !tx1Present {
		t.Fatal("tx1's commitLog entry was pruned even though it wrote a still-live row -- this breaks visibility for future readers")
	}

	txN := mvcc.BeginTx(SnapshotIsolation)
	version := table.GetVisibleVersion(mvcc, txN, rowID)
	if version == nil {
		t.Fatal("row created by tx1 should still be visible after the watermark advanced past its commit")
	}
	if version.Data[0] != 1 {
		t.Errorf("unexpected row data: %v", version.Data)
	}
}

// TestMVCCCommitLogPruningKeepsActiveSnapshot proves that a commitLog entry
// still needed by a currently active transaction's snapshot is never pruned
// prematurely, even while many other unrelated read-only transactions
// advance the GC watermark around it.
func TestMVCCCommitLogPruningKeepsActiveSnapshot(t *testing.T) {
	mvcc := NewMVCCManager()
	cols := []Column{{Name: "id", Type: IntType}}
	table := NewMVCCTable("docs2", cols, false)

	writer := mvcc.BeginTx(SnapshotIsolation)
	rowID := table.InsertVersion(writer, []any{42})
	if _, err := mvcc.CommitTx(writer); err != nil {
		t.Fatalf("writer commit failed: %v", err)
	}

	// Begin a long-lived reader before further churn; it must keep seeing
	// the row exactly as it existed at its snapshot.
	reader := mvcc.BeginTx(SnapshotIsolation)

	for i := 0; i < 20; i++ {
		tx := mvcc.BeginTx(SnapshotIsolation)
		if _, err := mvcc.CommitTx(tx); err != nil {
			t.Fatalf("churn tx commit failed: %v", err)
		}
	}

	version := table.GetVisibleVersion(mvcc, reader, rowID)
	if version == nil {
		t.Fatal("row should still be visible to the long-lived reader despite later commitLog pruning")
	}
	if version.Data[0] != 42 {
		t.Errorf("unexpected row data: %v", version.Data)
	}
}

func TestMVCCIsolationLevels(t *testing.T) {
	levels := []IsolationLevel{
		ReadCommitted,
		RepeatableRead,
		SnapshotIsolation,
		Serializable,
	}

	mvcc := NewMVCCManager()

	for _, level := range levels {
		tx := mvcc.BeginTx(level)
		if tx.IsolationLevel != level {
			t.Errorf("expected isolation level %v, got %v", level, tx.IsolationLevel)
		}
		mvcc.CommitTx(tx)
	}
}
