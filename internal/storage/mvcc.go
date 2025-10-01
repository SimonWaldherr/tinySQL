// Package storage - MVCC (Multi-Version Concurrency Control) implementation
//
// What: Full MVCC with row-level versioning, snapshot isolation, and visibility checks.
// How: Each row carries version metadata (xmin, xmax, timestamps). Transactions get
//      a unique TxID and snapshot timestamp. Visibility rules determine which row
//      versions are visible to each transaction.
// Why: Enables true concurrent reads and writes without blocking, implements
//      standard ACID snapshot isolation semantics.

package storage

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// TxID represents a unique transaction identifier.
type TxID uint64

// Timestamp represents a logical timestamp for MVCC visibility.
type Timestamp uint64

// TxStatus represents the current state of a transaction.
type TxStatus uint8

const (
	TxStatusInProgress TxStatus = iota
	TxStatusCommitted
	TxStatusAborted
)

// MVCCManager coordinates transaction IDs, timestamps, and visibility.
type MVCCManager struct {
	mu sync.RWMutex
	
	// Monotonically increasing transaction ID
	nextTxID atomic.Uint64
	
	// Monotonically increasing timestamp
	nextTimestamp atomic.Uint64
	
	// Active transactions
	activeTxs map[TxID]*TxContext
	
	// Transaction commit timestamps
	commitLog map[TxID]Timestamp
	
	// Oldest active transaction (for GC)
	oldestActive TxID
	
	// GC watermark - versions older than this can be cleaned
	gcWatermark Timestamp
}

// TxContext holds the state of an active transaction.
type TxContext struct {
	ID            TxID
	StartTime     Timestamp
	Status        TxStatus
	ReadSnapshot  Timestamp // Snapshot timestamp for reads
	WriteSet      map[string]map[int64]bool // table -> row IDs modified
	ReadSet       map[string]map[int64]Timestamp // table -> row IDs read with version
	IsolationLevel IsolationLevel
	mu            sync.RWMutex
}

// IsolationLevel defines transaction isolation semantics.
type IsolationLevel uint8

const (
	ReadCommitted IsolationLevel = iota
	RepeatableRead
	SnapshotIsolation
	Serializable
)

// RowVersion contains MVCC metadata for a single row version.
type RowVersion struct {
	// Transaction that created this version
	XMin TxID
	
	// Transaction that deleted/updated this version (0 if still valid)
	XMax TxID
	
	// Creation timestamp
	CreatedAt Timestamp
	
	// Deletion/update timestamp (0 if still valid)
	DeletedAt Timestamp
	
	// Actual row data
	Data []any
	
	// Pointer to next version (for version chain)
	NextVersion *RowVersion
}

// MVCCTable extends Table with version chains.
type MVCCTable struct {
	*Table
	
	// Version chains: row ID -> latest version
	versions map[int64]*RowVersion
	
	// Next row ID
	nextRowID atomic.Int64
	
	mu sync.RWMutex
}

// NewMVCCManager creates a new MVCC coordinator.
func NewMVCCManager() *MVCCManager {
	m := &MVCCManager{
		activeTxs: make(map[TxID]*TxContext),
		commitLog: make(map[TxID]Timestamp),
	}
	m.nextTxID.Store(1)
	m.nextTimestamp.Store(1)
	return m
}

// BeginTx starts a new transaction and returns its context.
func (m *MVCCManager) BeginTx(level IsolationLevel) *TxContext {
	txID := TxID(m.nextTxID.Add(1))
	now := Timestamp(m.nextTimestamp.Add(1))
	
	tx := &TxContext{
		ID:             txID,
		StartTime:      now,
		Status:         TxStatusInProgress,
		ReadSnapshot:   now,
		WriteSet:       make(map[string]map[int64]bool),
		ReadSet:        make(map[string]map[int64]Timestamp),
		IsolationLevel: level,
	}
	
	m.mu.Lock()
	m.activeTxs[txID] = tx
	m.updateOldestActive()
	m.mu.Unlock()
	
	return tx
}

// CommitTx marks a transaction as committed and records its commit timestamp.
func (m *MVCCManager) CommitTx(tx *TxContext) (Timestamp, error) {
	if tx.Status != TxStatusInProgress {
		return 0, ErrTxNotActive
	}
	
	// Serializable isolation: check for conflicts
	if tx.IsolationLevel == Serializable {
		if err := m.checkSerializableConflicts(tx); err != nil {
			return 0, err
		}
	}
	
	commitTS := Timestamp(m.nextTimestamp.Add(1))
	
	tx.mu.Lock()
	tx.Status = TxStatusCommitted
	tx.mu.Unlock()
	
	m.mu.Lock()
	m.commitLog[tx.ID] = commitTS
	delete(m.activeTxs, tx.ID)
	m.updateOldestActive()
	m.mu.Unlock()
	
	return commitTS, nil
}

// AbortTx marks a transaction as aborted.
func (m *MVCCManager) AbortTx(tx *TxContext) {
	if tx.Status != TxStatusInProgress {
		return
	}
	
	tx.mu.Lock()
	tx.Status = TxStatusAborted
	tx.mu.Unlock()
	
	m.mu.Lock()
	delete(m.activeTxs, tx.ID)
	m.updateOldestActive()
	m.mu.Unlock()
}

// IsVisible determines if a row version is visible to a transaction.
func (m *MVCCManager) IsVisible(tx *TxContext, rv *RowVersion) bool {
	// Row was created by this transaction
	if rv.XMin == tx.ID {
		// Not deleted by this transaction
		return rv.XMax == 0 || rv.XMax != tx.ID
	}
	
	// Check if creator transaction was committed before our snapshot
	m.mu.RLock()
	creatorCommitTS, creatorCommitted := m.commitLog[rv.XMin]
	m.mu.RUnlock()
	
	// Creator not committed or committed after our snapshot
	if !creatorCommitted || creatorCommitTS > tx.ReadSnapshot {
		return false
	}
	
	// Row not deleted
	if rv.XMax == 0 {
		return true
	}
	
	// Row deleted by this transaction
	if rv.XMax == tx.ID {
		return false
	}
	
	// Check if deleter transaction was committed before our snapshot
	m.mu.RLock()
	deleterCommitTS, deleterCommitted := m.commitLog[rv.XMax]
	m.mu.RUnlock()
	
	// Deleter not committed or committed after our snapshot - row still visible
	if !deleterCommitted || deleterCommitTS > tx.ReadSnapshot {
		return true
	}
	
	// Row was deleted before our snapshot
	return false
}

// RecordRead tracks a read operation for conflict detection.
func (tx *TxContext) RecordRead(table string, rowID int64, version Timestamp) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	
	if tx.ReadSet[table] == nil {
		tx.ReadSet[table] = make(map[int64]Timestamp)
	}
	tx.ReadSet[table][rowID] = version
}

// RecordWrite tracks a write operation.
func (tx *TxContext) RecordWrite(table string, rowID int64) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	
	if tx.WriteSet[table] == nil {
		tx.WriteSet[table] = make(map[int64]bool)
	}
	tx.WriteSet[table][rowID] = true
}

// checkSerializableConflicts detects read-write conflicts for serializable isolation.
func (m *MVCCManager) checkSerializableConflicts(tx *TxContext) error {
	tx.mu.RLock()
	defer tx.mu.RUnlock()
	
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	// Check if any concurrent transaction wrote to rows we read
	// Look at commit log for recently committed transactions
	for otherTxID, commitTS := range m.commitLog {
		if otherTxID == tx.ID {
			continue
		}
		
		// Only check transactions that committed after we started
		if commitTS <= tx.StartTime {
			continue
		}
		
		// This is a simplified check - in a real system we'd need
		// to track write sets of committed transactions
		// For now, check if there are any overlapping table accesses
		for table := range tx.ReadSet {
			// If we read from a table and another transaction
			// committed writes after our start, flag conflict
			if len(tx.WriteSet[table]) > 0 {
				return ErrSerializationFailure
			}
		}
	}
	
	return nil
}

// updateOldestActive updates the watermark for the oldest active transaction.
func (m *MVCCManager) updateOldestActive() {
	var oldest TxID = 0
	var oldestTS Timestamp = Timestamp(m.nextTimestamp.Load())
	
	for txID, tx := range m.activeTxs {
		if oldest == 0 || txID < oldest {
			oldest = txID
			oldestTS = tx.StartTime
		}
	}
	
	m.oldestActive = oldest
	if oldest == 0 {
		// No active transactions - can GC up to latest commit
		m.gcWatermark = Timestamp(m.nextTimestamp.Load())
	} else {
		m.gcWatermark = oldestTS
	}
}

// GCWatermark returns the timestamp before which row versions can be garbage collected.
func (m *MVCCManager) GCWatermark() Timestamp {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.gcWatermark
}

// NewMVCCTable creates a table with MVCC support.
func NewMVCCTable(name string, cols []Column, isTemp bool) *MVCCTable {
	return &MVCCTable{
		Table:    NewTable(name, cols, isTemp),
		versions: make(map[int64]*RowVersion),
	}
}

// InsertVersion adds a new row version.
func (t *MVCCTable) InsertVersion(tx *TxContext, data []any) int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	
	rowID := t.nextRowID.Add(1)
	
	rv := &RowVersion{
		XMin:      tx.ID,
		XMax:      0,
		CreatedAt: tx.StartTime,
		DeletedAt: 0,
		Data:      data,
	}
	
	t.versions[rowID] = rv
	tx.RecordWrite(t.Name, rowID)
	
	return rowID
}

// UpdateVersion creates a new version for an update.
func (t *MVCCTable) UpdateVersion(tx *TxContext, rowID int64, newData []any) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	
	oldVersion := t.versions[rowID]
	if oldVersion == nil {
		return ErrRowNotFound
	}
	
	// Mark old version as deleted by this transaction
	oldVersion.XMax = tx.ID
	oldVersion.DeletedAt = Timestamp(time.Now().UnixNano())
	
	// Create new version
	newVersion := &RowVersion{
		XMin:        tx.ID,
		XMax:        0,
		CreatedAt:   Timestamp(time.Now().UnixNano()),
		DeletedAt:   0,
		Data:        newData,
		NextVersion: oldVersion,
	}
	
	t.versions[rowID] = newVersion
	tx.RecordWrite(t.Name, rowID)
	
	return nil
}

// DeleteVersion marks a row version as deleted.
func (t *MVCCTable) DeleteVersion(tx *TxContext, rowID int64) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	
	version := t.versions[rowID]
	if version == nil {
		return ErrRowNotFound
	}
	
	version.XMax = tx.ID
	version.DeletedAt = Timestamp(time.Now().UnixNano())
	tx.RecordWrite(t.Name, rowID)
	
	return nil
}

// GetVisibleVersion returns the visible version of a row for the given transaction.
func (t *MVCCTable) GetVisibleVersion(mvcc *MVCCManager, tx *TxContext, rowID int64) *RowVersion {
	t.mu.RLock()
	defer t.mu.RUnlock()
	
	version := t.versions[rowID]
	
	// Walk the version chain to find a visible version
	for version != nil {
		if mvcc.IsVisible(tx, version) {
			tx.RecordRead(t.Name, rowID, version.CreatedAt)
			return version
		}
		version = version.NextVersion
	}
	
	return nil
}

// GarbageCollect removes old row versions that are no longer visible.
func (t *MVCCTable) GarbageCollect(watermark Timestamp) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	
	collected := 0
	toDelete := make([]int64, 0)
	
	for rowID, version := range t.versions {
		// Check if latest version is deleted and old enough
		if version.DeletedAt > 0 && version.DeletedAt < watermark {
			// Can delete entire chain
			toDelete = append(toDelete, rowID)
			
			// Count versions in chain
			curr := version
			for curr != nil {
				collected++
				curr = curr.NextVersion
			}
		} else {
			// Keep the latest version, but clean up old versions in the chain
			prev := version
			curr := version.NextVersion
			
			for curr != nil {
				if curr.DeletedAt > 0 && curr.DeletedAt < watermark {
					// Remove this version from chain
					prev.NextVersion = curr.NextVersion
					collected++
					curr = prev.NextVersion
				} else if curr.CreatedAt < watermark && curr.DeletedAt > 0 {
					// Old deleted version
					prev.NextVersion = curr.NextVersion
					collected++
					curr = prev.NextVersion
				} else {
					prev = curr
					curr = curr.NextVersion
				}
			}
		}
	}
	
	// Delete entire chains that are obsolete
	for _, rowID := range toDelete {
		delete(t.versions, rowID)
	}
	
	return collected
}

// Errors
var (
	ErrTxNotActive           = fmt.Errorf("transaction is not active")
	ErrSerializationFailure  = fmt.Errorf("could not serialize access due to concurrent update")
	ErrRowNotFound           = fmt.Errorf("row not found")
)
