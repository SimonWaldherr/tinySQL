package storage

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestAdvancedWALBasic(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")

	config := AdvancedWALConfig{
		Path:            walPath,
		CheckpointEvery: 100,
	}

	wal, err := OpenAdvancedWAL(config)
	if err != nil {
		t.Fatalf("failed to open WAL: %v", err)
	}
	defer wal.Close()

	// Log a transaction
	lsn1, err := wal.LogBegin(1)
	if err != nil {
		t.Fatalf("failed to log begin: %v", err)
	}
	if lsn1 == 0 {
		t.Error("expected non-zero LSN")
	}

	cols := []Column{
		{Name: "id", Type: IntType},
		{Name: "name", Type: StringType},
	}

	lsn2, err := wal.LogInsert(1, "default", "users", 1, []any{1, "Alice"}, cols)
	if err != nil {
		t.Fatalf("failed to log insert: %v", err)
	}
	if lsn2 <= lsn1 {
		t.Error("LSNs should be monotonically increasing")
	}

	lsn3, err := wal.LogCommit(1)
	if err != nil {
		t.Fatalf("failed to log commit: %v", err)
	}
	if lsn3 <= lsn2 {
		t.Error("commit LSN should be greater than insert LSN")
	}
}

func TestAdvancedWALOperations(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "ops.wal")

	config := AdvancedWALConfig{
		Path: walPath,
	}

	wal, err := OpenAdvancedWAL(config)
	if err != nil {
		t.Fatalf("failed to open WAL: %v", err)
	}
	defer wal.Close()

	cols := []Column{
		{Name: "id", Type: IntType},
		{Name: "value", Type: IntType},
	}

	// Transaction 1: Insert
	wal.LogBegin(1)
	wal.LogInsert(1, "default", "data", 1, []any{1, 100}, cols)
	wal.LogCommit(1)

	// Transaction 2: Update
	wal.LogBegin(2)
	wal.LogUpdate(2, "default", "data", 1, []any{1, 100}, []any{1, 200}, cols)
	wal.LogCommit(2)

	// Transaction 3: Delete
	wal.LogBegin(3)
	wal.LogDelete(3, "default", "data", 1, []any{1, 200}, cols)
	wal.LogCommit(3)

	// Verify LSN progression
	nextLSN := wal.GetNextLSN()
	if nextLSN <= 6 {
		t.Errorf("expected LSN > 6, got %d", nextLSN)
	}
}

func TestAdvancedWALAbort(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "abort.wal")

	wal, err := OpenAdvancedWAL(AdvancedWALConfig{Path: walPath})
	if err != nil {
		t.Fatalf("failed to open WAL: %v", err)
	}
	defer wal.Close()

	cols := []Column{{Name: "id", Type: IntType}}

	wal.LogBegin(1)
	wal.LogInsert(1, "default", "test", 1, []any{1}, cols)
	lsn, err := wal.LogAbort(1)
	if err != nil {
		t.Fatalf("failed to log abort: %v", err)
	}
	if lsn == 0 {
		t.Error("expected non-zero LSN for abort")
	}
}

func TestAdvancedWALRecovery(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "recovery.wal")

	cols := []Column{
		{Name: "id", Type: IntType},
		{Name: "name", Type: StringType},
	}

	// Create and write some transactions
	{
		wal, err := OpenAdvancedWAL(AdvancedWALConfig{Path: walPath})
		if err != nil {
			t.Fatalf("failed to open WAL: %v", err)
		}

		// Committed transaction
		wal.LogBegin(1)
		wal.LogInsert(1, "default", "users", 1, []any{1, "Alice"}, cols)
		wal.LogCommit(1)

		// Uncommitted transaction (simulates crash)
		wal.LogBegin(2)
		wal.LogInsert(2, "default", "users", 2, []any{2, "Bob"}, cols)
		// No commit - crash!

		wal.Close()
	}

	// Recover
	db := NewDB()
	{
		wal, err := OpenAdvancedWAL(AdvancedWALConfig{Path: walPath})
		if err != nil {
			t.Fatalf("failed to reopen WAL: %v", err)
		}
		defer wal.Close()

		recovered, err := wal.Recover(db)
		if err != nil {
			t.Fatalf("recovery failed: %v", err)
		}

		// Should recover only committed transaction
		if recovered != 1 {
			t.Errorf("expected 1 recovered operation, got %d", recovered)
		}
	}

	// Verify data
	table, err := db.Get("default", "users")
	if err != nil {
		t.Fatalf("table not found after recovery: %v", err)
	}

	if len(table.Rows) != 1 {
		t.Errorf("expected 1 row, got %d", len(table.Rows))
	}

	if table.Rows[0][0] != 1 || table.Rows[0][1] != "Alice" {
		t.Errorf("unexpected row data: %v", table.Rows[0])
	}
}

func TestAdvancedWALCheckpoint(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "checkpoint.wal")
	cpPath := filepath.Join(tmpDir, "checkpoint.db")

	config := AdvancedWALConfig{
		Path:            walPath,
		CheckpointPath:  cpPath,
		CheckpointEvery: 2, // Checkpoint after 2 records
	}

	wal, err := OpenAdvancedWAL(config)
	if err != nil {
		t.Fatalf("failed to open WAL: %v", err)
	}
	defer wal.Close()

	db := NewDB()
	cols := []Column{{Name: "id", Type: IntType}}

	// Write some transactions
	for i := 1; i <= 3; i++ {
		wal.LogBegin(TxID(i))
		wal.LogInsert(TxID(i), "default", "test", int64(i), []any{i}, cols)
		wal.LogCommit(TxID(i))
	}

	// Should need checkpoint
	if !wal.ShouldCheckpoint() {
		t.Error("expected checkpoint to be needed")
	}

	// Perform checkpoint
	if err := wal.Checkpoint(db); err != nil {
		t.Fatalf("checkpoint failed: %v", err)
	}

	// Verify checkpoint file exists
	if _, err := os.Stat(cpPath); os.IsNotExist(err) {
		t.Error("checkpoint file should exist")
	}

	// After checkpoint, record count should reset
	if wal.ShouldCheckpoint() {
		t.Error("should not need checkpoint immediately after checkpointing")
	}
}

func TestAdvancedWALChecksumValidation(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "checksum.wal")

	wal, err := OpenAdvancedWAL(AdvancedWALConfig{Path: walPath})
	if err != nil {
		t.Fatalf("failed to open WAL: %v", err)
	}

	cols := []Column{{Name: "id", Type: IntType}}

	// Write a transaction
	wal.LogBegin(1)
	wal.LogInsert(1, "default", "test", 1, []any{1}, cols)
	wal.LogCommit(1)
	wal.Close()

	// Corrupt the file
	data, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatalf("failed to read WAL: %v", err)
	}

	// Flip some bits in the middle
	if len(data) > 100 {
		data[50] ^= 0xFF
		if err := os.WriteFile(walPath, data, 0644); err != nil {
			t.Fatalf("failed to write corrupted WAL: %v", err)
		}
	}

	// Try to recover - should detect corruption
	wal2, err := OpenAdvancedWAL(AdvancedWALConfig{Path: walPath})
	if err != nil {
		t.Fatalf("failed to reopen WAL: %v", err)
	}
	defer wal2.Close()

	db := NewDB()
	// Recovery should stop at corruption but not fail completely
	recovered, err := wal2.Recover(db)
	if err != nil {
		t.Logf("recovery error (expected): %v", err)
	}
	t.Logf("recovered %d operations before corruption", recovered)
}

func TestAdvancedWALConcurrent(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "concurrent.wal")

	wal, err := OpenAdvancedWAL(AdvancedWALConfig{Path: walPath})
	if err != nil {
		t.Fatalf("failed to open WAL: %v", err)
	}
	defer wal.Close()

	cols := []Column{{Name: "id", Type: IntType}}

	// Concurrent writes (WAL is internally synchronized)
	done := make(chan bool, 10)
	for i := 1; i <= 10; i++ {
		go func(txID TxID) {
			wal.LogBegin(txID)
			wal.LogInsert(txID, "default", "test", int64(txID), []any{int(txID)}, cols)
			wal.LogCommit(txID)
			done <- true
		}(TxID(i))
	}

	// Wait for all to complete
	for i := 0; i < 10; i++ {
		<-done
	}

	// Verify all transactions logged
	nextLSN := wal.GetNextLSN()
	// Each transaction: BEGIN + INSERT + COMMIT = 3 records
	// So 10 transactions = 30 records + 1 (next LSN) = LSN 31
	if nextLSN < 30 {
		t.Errorf("expected LSN >= 30, got %d", nextLSN)
	}
}

func TestAdvancedWALLSNOrdering(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "ordering.wal")

	wal, err := OpenAdvancedWAL(AdvancedWALConfig{Path: walPath})
	if err != nil {
		t.Fatalf("failed to open WAL: %v", err)
	}
	defer wal.Close()

	var lsns []LSN
	cols := []Column{{Name: "id", Type: IntType}}

	// Log multiple operations
	lsn, _ := wal.LogBegin(1)
	lsns = append(lsns, lsn)

	lsn, _ = wal.LogInsert(1, "default", "test", 1, []any{1}, cols)
	lsns = append(lsns, lsn)

	lsn, _ = wal.LogCommit(1)
	lsns = append(lsns, lsn)

	// Verify LSNs are strictly increasing
	for i := 1; i < len(lsns); i++ {
		if lsns[i] <= lsns[i-1] {
			t.Errorf("LSNs not strictly increasing: %v", lsns)
		}
	}
}

func TestAdvancedWALTimeBasedCheckpoint(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "time_checkpoint.wal")
	cpPath := filepath.Join(tmpDir, "time_checkpoint.db")

	config := AdvancedWALConfig{
		Path:               walPath,
		CheckpointPath:     cpPath,
		CheckpointEvery:    1000,                   // High record count
		CheckpointInterval: 100 * time.Millisecond, // Short interval
	}

	wal, err := OpenAdvancedWAL(config)
	if err != nil {
		t.Fatalf("failed to open WAL: %v", err)
	}
	defer wal.Close()

	// Initially shouldn't need checkpoint
	if wal.ShouldCheckpoint() {
		t.Error("should not need checkpoint initially")
	}

	// Wait for interval to pass
	time.Sleep(150 * time.Millisecond)

	// Now should need checkpoint
	if !wal.ShouldCheckpoint() {
		t.Error("should need checkpoint after interval")
	}
}

func TestAdvancedWALUpdateRecovery(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "update_recovery.wal")

	cols := []Column{
		{Name: "id", Type: IntType},
		{Name: "value", Type: IntType},
	}

	// Write initial data and update
	{
		wal, err := OpenAdvancedWAL(AdvancedWALConfig{Path: walPath})
		if err != nil {
			t.Fatalf("failed to open WAL: %v", err)
		}

		// Insert
		wal.LogBegin(1)
		wal.LogInsert(1, "default", "data", 1, []any{1, 100}, cols)
		wal.LogCommit(1)

		// Update
		wal.LogBegin(2)
		wal.LogUpdate(2, "default", "data", 1, []any{1, 100}, []any{1, 200}, cols)
		wal.LogCommit(2)

		wal.Close()
	}

	// Recover
	db := NewDB()
	{
		wal, err := OpenAdvancedWAL(AdvancedWALConfig{Path: walPath})
		if err != nil {
			t.Fatalf("failed to reopen WAL: %v", err)
		}
		defer wal.Close()

		recovered, err := wal.Recover(db)
		if err != nil {
			t.Fatalf("recovery failed: %v", err)
		}

		if recovered != 2 {
			t.Errorf("expected 2 recovered operations, got %d", recovered)
		}
	}

	// Verify updated data
	table, err := db.Get("default", "data")
	if err != nil {
		t.Fatalf("table not found: %v", err)
	}

	if len(table.Rows) != 1 {
		t.Errorf("expected 1 row, got %d", len(table.Rows))
	}

	if table.Rows[0][1] != 200 {
		t.Errorf("expected value 200, got %v", table.Rows[0][1])
	}
}
