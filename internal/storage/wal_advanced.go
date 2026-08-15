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
	"crypto/rand"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
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

	// autoTxID hands out transaction IDs for the engine's implicit
	// single-statement transactions (see NewAutoTxID) — separate from any
	// explicit transaction machinery (internal/driver's BeginTx uses the
	// unrelated basic WALManager, not this one), so there's no ID space to
	// collide with.
	autoTxID atomic.Uint64

	// WAL file path
	path string

	// Checkpoint path
	checkpointPath string

	// File handle
	file *os.File

	// Byte counter between file and writer (bounds WAL growth)
	bytes *countingWriter

	// Buffered writer
	writer *bufio.Writer

	// GOB encoder
	encoder *gob.Encoder

	// Next LSN to assign
	nextLSN LSN

	// Checkpoint configuration
	checkpointEvery    uint64
	checkpointInterval time.Duration
	checkpointMaxBytes int64
	lastCheckpoint     time.Time
	recordsSinceCP     uint64

	// Active transactions (for recovery)
	activeTxs map[TxID]*WALTxState

	// Committed LSN (for durability guarantees)
	committedLSN LSN

	// Flushed LSN (written to disk)
	flushedLSN LSN

	// LSN up to which the last checkpoint's saved snapshot already reflects
	// every operation (loaded from the checkpoint file at open time, and
	// updated after each successful Checkpoint). Recover uses this to skip
	// re-applying records at or below it: without this, a crash between
	// Checkpoint's snapshot save and its WAL truncation leaves an intact
	// WAL whose already-checkpointed operations would otherwise be replayed
	// a second time on top of a snapshot that already contains them,
	// silently duplicating every row written since the previous checkpoint.
	checkpointWatermark LSN

	// checkpointDataWatermark is the highest LSN of any real, committed
	// operation that a checkpoint has captured -- committedLSN at the exact
	// moment Checkpoint ran, captured before that call's checkpoint-marker
	// record consumes its own (necessarily later) LSN. It exists
	// specifically for ReadCommittedSince's ErrReplicaTooFarBehind check
	// (wal_feed.go), which must not reuse checkpointWatermark directly: the
	// marker record's LSN is always at least one past the last real commit
	// (checkpointWatermark == committedLSN+1 in the common case, more if
	// other non-data LSNs were consumed around the same moment), so a
	// replica's sinceLSN -- always a real operation's LSN, from a previous
	// Bootstrap or ReadCommittedSince call, never a marker's -- can
	// legitimately equal committedLSN while still being numerically less
	// than checkpointWatermark. Comparing against checkpointWatermark there
	// would reject a replica that is not actually missing anything, and
	// would do so on every poll after every checkpoint that isn't
	// immediately followed by a new write -- not a rare boundary case but a
	// standing risk of spinning in an unbounded re-bootstrap loop against an
	// otherwise idle primary.
	//
	// Initialized from checkpointWatermark itself at open time (the only
	// information available before this process has run a checkpoint of
	// its own) and refined to the precise value on every subsequent
	// Checkpoint call. It is deliberately never persisted: a fresh process
	// falling back to the coarser checkpointWatermark right after it
	// restarts, before its own first checkpoint, keeps that narrow window
	// exactly as conservative as this field not existing at all --
	// unlike letting it reset to zero across a restart would, which would
	// silently defeat the whole check for any replica whose sinceLSN
	// predates a checkpoint from a previous run.
	checkpointDataWatermark LSN

	// loggedTables tracks, for this AdvancedWAL instance's lifetime, every
	// (tenant, table) pair whose first Insert/Update record has already been
	// logged with WALRecord.Columns populated -- see loggedColumnsFor's doc
	// comment for why only that first record needs it. Lazily initialized by
	// loggedColumnsFor; a nil map (the zero value, e.g. a bare &AdvancedWAL{}
	// in a test that never logs anything) is never dereferenced.
	loggedTables map[tableKey]struct{}

	// epoch identifies which "incarnation" of this WAL/checkpoint pair a
	// replica is talking to: a random, non-zero value minted once, the
	// first time OpenAdvancedWAL creates wal.path fresh (see
	// OpenAdvancedWAL), and carried forward across every later open of the
	// same WAL by being persisted in the checkpoint file alongside
	// checkpointWatermark (see Checkpoint and ReadCheckpointEpoch). 0 is
	// reserved as the "no epoch recorded" sentinel for a pre-existing WAL/
	// checkpoint pair that predates this field (upgrading a running
	// deployment must not manufacture spurious epoch churn on an ordinary
	// restart), so OpenAdvancedWAL only ever mints a fresh non-zero epoch
	// when wal.path itself did not already exist -- never merely because a
	// checkpoint carrying one is missing.
	//
	// A replication feed (see SnapshotWithWatermark, and cmd/server's
	// Bootstrap/GetChangesSince handlers, which surface this in every
	// response) uses a change in epoch to detect that the primary's WAL/
	// checkpoint files were wiped or restored from backup out from under an
	// already-syncing replica: LSNs alone cannot catch this reliably, since
	// a fresh WAL restarts LSN numbering from 1 and could easily assign
	// small LSNs that still look "not yet requested" to a replica's stale,
	// numerically-larger sinceLSN from the previous incarnation. A replica
	// that sees its remembered epoch stop matching must never keep applying
	// records incrementally against the new incarnation's unrelated
	// history -- it must re-bootstrap from scratch instead.
	epoch uint64

	closed bool
}

// newWALEpoch returns a fresh, non-zero random identifier for
// OpenAdvancedWAL to tag a newly created WAL file with (see
// AdvancedWAL.epoch's doc comment for what it is for and why 0 is reserved).
func newWALEpoch() uint64 {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand.Read failing is effectively unheard of on any real
		// platform. Fall back to a coarse but still-useful discriminator
		// rather than silently leaving the epoch at the reserved zero
		// sentinel, which would make a genuinely fresh WAL indistinguishable
		// from "no epoch recorded" and defeat the safety net entirely.
		return uint64(time.Now().UnixNano()) | 1
	}
	e := binary.BigEndian.Uint64(b[:])
	if e == 0 {
		e = 1
	}
	return e
}

// WALTxState tracks the state of a transaction in the WAL.
type WALTxState struct {
	TxID       TxID
	StartLSN   LSN
	Operations []LSN
	Status     TxStatus
}

// AdvancedWALConfig configures the advanced WAL.
//
// There is deliberately no Compress option here: the live WAL log is a
// continuously-appended, crash-recoverable stream, and compressing it
// safely would need a materially more complex design (resumable framing,
// truncation handling that doesn't regress the per-record checksum's
// corruption detection). Compression for ModeAdvancedWAL instead applies to
// the periodic checkpoint snapshot only, via CheckpointPath's ".gz" suffix
// (see OpenDB's ModeAdvancedWAL case) — a checkpoint is a whole snapshot
// written once and read back whole, so it can reuse SaveToFile/
// loadGOBInto's existing, already-tested gzip support with no new risk.
type AdvancedWALConfig struct {
	Path               string
	CheckpointPath     string
	CheckpointEvery    uint64        // Checkpoint after N records
	CheckpointInterval time.Duration // Checkpoint after duration
	CheckpointMaxBytes int64         // Checkpoint once WAL exceeds this size (0 = 64 MB default, <0 disables)
	BufferSize         int           // Buffer size for writing
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

	// Recorded before O_CREATE below can bring the file into existence, so
	// it reflects whether this call is creating wal.path fresh -- the only
	// condition under which a new epoch (see AdvancedWAL.epoch's doc
	// comment) is minted.
	walFileExisted := false
	if _, statErr := os.Stat(config.Path); statErr == nil {
		walFileExisted = true
	}

	// Open or create WAL file
	file, err := os.OpenFile(config.Path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open WAL file: %w", err)
	}

	var walSize int64
	if fi, statErr := file.Stat(); statErr == nil {
		walSize = fi.Size()
	}
	cw := &countingWriter{w: file, n: walSize}
	writer := bufio.NewWriterSize(cw, config.BufferSize)

	var checkpointWatermark LSN
	var epoch uint64
	if config.CheckpointPath != "" {
		w, err := ReadCheckpointWatermark(config.CheckpointPath)
		if err != nil {
			_ = file.Close()
			return nil, fmt.Errorf("read checkpoint watermark: %w", err)
		}
		checkpointWatermark = LSN(w)

		e, err := ReadCheckpointEpoch(config.CheckpointPath)
		if err != nil {
			_ = file.Close()
			return nil, fmt.Errorf("read checkpoint epoch: %w", err)
		}
		epoch = e
	}
	if !walFileExisted {
		// A genuinely fresh WAL: mint a new epoch now, regardless of
		// whatever a stale/mismatched checkpoint file happened to carry.
		// Persisted lazily -- the same as checkpointWatermark itself -- the
		// next time Checkpoint runs.
		epoch = newWALEpoch()
	}

	wal := &AdvancedWAL{
		path:                    config.Path,
		checkpointPath:          config.CheckpointPath,
		file:                    file,
		bytes:                   cw,
		writer:                  writer,
		checkpointEvery:         config.CheckpointEvery,
		checkpointInterval:      config.CheckpointInterval,
		checkpointMaxBytes:      normalizeCheckpointMaxBytes(config.CheckpointMaxBytes),
		lastCheckpoint:          time.Now(),
		activeTxs:               make(map[TxID]*WALTxState),
		nextLSN:                 checkpointWatermark + 1,
		checkpointWatermark:     checkpointWatermark,
		checkpointDataWatermark: checkpointWatermark,
		epoch:                   epoch,
	}

	wal.encoder = gob.NewEncoder(writer)

	return wal, nil
}

// NewAutoTxID returns a fresh transaction ID for the engine to use as an
// implicit single-statement transaction (one INSERT/UPDATE/DELETE statement
// = one WAL transaction, autocommitted). Safe for concurrent use.
func (w *AdvancedWAL) NewAutoTxID() TxID {
	return TxID(w.autoTxID.Add(1))
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

// tableKey identifies a table within a tenant, for AdvancedWAL.loggedTables
// (see loggedColumnsFor).
type tableKey struct {
	tenant string
	table  string
}

// loggedColumnsFor reports whether an Insert/Update record about to be
// logged for (tenant, table) must carry its full column schema in
// WALRecord.Columns, returning cols verbatim if so and nil otherwise. Must
// be called with w.mu held (LogInsert/LogUpdate already hold it).
//
// applyOperation (below) reads record.Columns in exactly one place: the
// "table doesn't exist yet on the applying side" bootstrap branch, entered
// only for WALOpInsert/WALOpUpdate (a delete can never bootstrap-create a
// table -- see LogDelete, which never populates Columns at all). Once that
// branch has fired once for a table -- whether during this process's own
// crash recovery (Recover) or on a replication feed's receiving end
// (ApplyWALRecord, wal_feed.go) -- every later checkpoint snapshot (a full
// DB snapshot, never a WAL replay) is guaranteed to already contain that
// table, so no later record for it can ever hit the bootstrap branch again.
// loggedTables tracks, for this AdvancedWAL instance's lifetime, which
// (tenant, table) pairs have already had their first Insert/Update logged
// with Columns populated, so every following record for that table can
// safely omit it -- avoiding a gob encode of the full column descriptor
// (names, types, constraints, foreign key pointers) on every single
// row-level record, the overwhelming common case.
//
// This is deliberately per-process, not persisted or derived from
// checkpoint state: a fresh AdvancedWAL instance (e.g. after a restart)
// starts with an empty map and will redundantly re-populate Columns on the
// very next Insert/Update per table even when that table already existed
// before the restart -- one wasted encode per table per process lifetime at
// worst, never a correctness problem, and far cheaper than the per-row cost
// this replaces.
func (w *AdvancedWAL) loggedColumnsFor(tenant, table string, cols []Column) []Column {
	if w.loggedTables == nil {
		w.loggedTables = make(map[tableKey]struct{})
	}
	key := tableKey{tenant, table}
	if _, seen := w.loggedTables[key]; seen {
		return nil
	}
	w.loggedTables[key] = struct{}{}
	return cols
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
		Columns:    w.loggedColumnsFor(tenant, table, cols),
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
		Columns:     w.loggedColumnsFor(tenant, table, cols),
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
//
// cols is accepted for API symmetry with LogInsert/LogUpdate (existing
// callers pass it) but deliberately never stored on the record: applyOperation
// (below) only ever reads record.Columns in its bootstrap branch for
// WALOpInsert/WALOpUpdate -- a delete falls through to "ignore delete/update
// for non-existent table" and never creates anything, so no delete record,
// first-for-its-table or not, ever needs its schema.
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
	if w.closed {
		return fmt.Errorf("advanced WAL is closed")
	}

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

	// Save database snapshot together with the LSN watermark up to which it
	// already reflects every operation (this checkpoint marker's own LSN —
	// everything before it was already applied to db directly by the live
	// engine, not via replay, since AdvancedWAL only logs alongside that
	// mutation). If a crash lands between this save and the WAL truncation
	// below, Recover uses the watermark to skip re-applying records this
	// snapshot already contains, instead of silently duplicating them.
	//
	// w.epoch rides along as a second extra value (see ReadCheckpointEpoch,
	// which decodes it back out in that same order) so a replication feed
	// reading this checkpoint later can tell this WAL's identity apart from
	// any other incarnation that might come to occupy the same path (see
	// AdvancedWAL.epoch's doc comment).
	if err := SaveToFile(db, w.checkpointPath, uint64(lsn), w.epoch); err != nil {
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
	w.bytes = &countingWriter{w: file}
	w.writer = bufio.NewWriter(w.bytes)
	w.encoder = gob.NewEncoder(w.writer)
	w.recordsSinceCP = 0
	w.lastCheckpoint = time.Now()
	w.checkpointWatermark = lsn
	// Refines checkpointDataWatermark to the precise value for this
	// process's lifetime -- committedLSN is unchanged by everything above
	// (only nextLSN/checkpointWatermark move), so this is exactly "the
	// highest real, committed LSN this checkpoint's snapshot captured," not
	// the checkpoint marker's own (necessarily later) LSN. See
	// checkpointDataWatermark's doc comment for why ReadCommittedSince
	// (wal_feed.go) needs this distinction to avoid rejecting a replica
	// that is not actually missing anything.
	w.checkpointDataWatermark = w.committedLSN
	// nextLSN is deliberately NOT reset here: LSN is documented as globally
	// unique and monotonically increasing for the database's lifetime (see
	// the LSN doc comment and GetNextLSN/GetCommittedLSN/GetFlushedLSN,
	// which external callers — e.g. a backup/replication feed — may rely
	// on). Continuing the sequence also keeps it consistent with
	// checkpointWatermark across repeated checkpoint cycles: if LSNs reset
	// to 1 here, a later crash-recovery pass could not tell a fresh
	// post-checkpoint LSN 1 apart from the LSN 1 of checkpoints ago, and
	// the watermark check above would wrongly skip real new records.

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

	if w.checkpointMaxBytes > 0 && w.bytes != nil && w.bytes.n >= w.checkpointMaxBytes {
		return true
	}

	return false
}

// Recover replays the WAL to restore database state after a crash.
//
//nolint:gocyclo // Recovery must cover diverse WAL scenarios including corruption handling.
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
	defer func() { _ = file.Close() }()

	dec := gob.NewDecoder(file)

	// Track pending transactions
	pending := make(map[TxID][]*WALRecord)
	committed := make(map[TxID]bool)
	aborted := make(map[TxID]bool)

	recovered := 0
	// Tables touched by at least one replayed operation. Rebuilding secondary
	// indexes and invalidating stats is deferred until after the whole WAL
	// has been scanned (see the loop below) and done once per table here,
	// instead of after every single replayed row: replaying M operations
	// against a table already carrying secondary-index metadata previously
	// cost O(M) full index rebuilds (each itself O(rows log rows)) instead
	// of one.
	touchedTables := make(map[*Table]struct{})
	// Seed from the checkpoint's own watermark (loaded in OpenAdvancedWAL),
	// not zero, so nextLSN below continues monotonically even when the WAL
	// file has nothing left to scan (e.g. a cleanly truncated WAL after a
	// prior successful checkpoint) — otherwise the next checkpoint's LSN
	// numbering would restart from a value already used before, and this
	// same watermark check would wrongly treat genuinely new records as
	// already-checkpointed.
	maxLSN := w.checkpointWatermark

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

		// Verify checksum (CRC32C; fall back to the legacy additive checksum
		// so WAL files written by older versions still recover).
		if record.Checksum != w.calculateChecksum(&record) && record.Checksum != w.legacyChecksum(&record) {
			fmt.Printf("WAL checksum mismatch at LSN %d, stopping recovery\n", record.LSN)
			break
		}

		if record.LSN > maxLSN {
			maxLSN = record.LSN
		}

		if record.LSN <= w.checkpointWatermark {
			// Already reflected in the checkpoint snapshot loaded before
			// Recover ran (see OpenDB's ModeAdvancedWAL case) — this record
			// only still exists because a crash landed between the
			// snapshot save and the WAL truncation that would normally
			// have removed it. Re-applying it would duplicate a row this
			// state already contains.
			continue
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
					table, err := applyOperation(db, op)
					if err != nil {
						return recovered, fmt.Errorf("apply operation at LSN %d: %w", op.LSN, err)
					}
					if table != nil {
						touchedTables[table] = struct{}{}
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

	// Now that every record has been applied, bring each touched table's
	// derived state (secondary indexes, cached stats, dirty tracking) up to
	// date exactly once, regardless of how many operations it replayed.
	for table := range touchedTables {
		if err := table.RebuildSecondaryIndexes(); err != nil {
			return recovered, err
		}
		table.InvalidateStats()
		table.MarkDirtyFrom(-1)
	}

	// Update next LSN
	w.nextLSN = maxLSN + 1

	return recovered, nil
}

// applyOperation applies a single WAL operation to the database, returning
// the table it touched (or nil for a no-op delete/update against a table
// that no longer exists) so the caller can defer index/stats maintenance
// until the whole WAL has been replayed instead of redoing it per op.
//
// This is a plain function, not a method: it needs no AdvancedWAL state
// (nor does rowsEqual, below) — only db and the record being applied — so it
// can be reused as-is by ApplyWALRecord (wal_feed.go) for a replication
// feed's apply path without needing a *AdvancedWAL to call it on.
func applyOperation(db *DB, record *WALRecord) (*Table, error) {
	table, err := db.Get(record.Tenant, record.Table)
	if err != nil {
		// Table doesn't exist - create it
		if record.OpType == WALOpInsert || record.OpType == WALOpUpdate {
			table = NewTable(record.Table, record.Columns, false)
			if err := db.Put(record.Tenant, table); err != nil {
				return nil, err
			}
		} else {
			return nil, nil // Ignore delete/update for non-existent table
		}
	}

	switch record.OpType {
	case WALOpInsert:
		table.Rows = append(table.Rows, record.AfterImage)
		table.Version++

	case WALOpUpdate:
		// Find and update the row
		found := false
		if i, ok := locateLoggedRow(table, record); ok {
			table.Rows[i] = record.AfterImage
			found = true
		}
		if !found {
			// Row not found - treat as insert
			table.Rows = append(table.Rows, record.AfterImage)
		} else {
			// An in-place replacement, unlike the insert fallback above, so
			// anything keyed on "only appends happened since I last looked"
			// (see Table.noteStructuralChange) must not treat this table as
			// append-only anymore, and executor state derived from the old
			// row contents has to go.
			table.noteStructuralChange()
			table.dropDerived()
		}
		table.Version++

	case WALOpDelete:
		// Find and remove the row
		if i, ok := locateLoggedRow(table, record); ok {
			table.Rows = append(table.Rows[:i], table.Rows[i+1:]...)
			table.noteStructuralChange()
			// Every later row shifts down one position, which executor
			// state built from row positions cannot reconcile.
			table.dropDerived()
		}
		table.Version++
	}
	// A recovered table can already own materialized secondary-index metadata
	// from its last checkpoint, and replay changes rows directly. The caller
	// (Recover) rebuilds indexes/stats for this table once after the whole
	// WAL has been replayed, rather than after every individual op here.
	return table, nil
}

// locateLoggedRow finds the row a WAL update/delete record refers to.
//
// Every record carries the row position it was written at (see LogUpdate and
// LogDelete), so replay checks that position first and only falls back to
// scanning when it does not hold the expected before-image — which it will not
// when an earlier delete in the same log shifted rows down, or when the log is
// replayed onto a base that does not match the one it was written against.
// Before the hint was consulted, every record cost a scan of the whole table,
// making replay of a heavily-updated table quadratic in its row count.
func locateLoggedRow(table *Table, record *WALRecord) (int, bool) {
	if hint := int(record.RowID); hint >= 0 && hint < len(table.Rows) {
		if rowsEqual(table.Rows[hint], record.BeforeImage) {
			return hint, true
		}
	}
	for i, row := range table.Rows {
		if rowsEqual(row, record.BeforeImage) {
			return i, true
		}
	}
	return 0, false
}

// rowsEqual compares two rows for equality.
//
// It compares cell by cell through CanonicalIndexValueEqual rather than with
// Go's `!=`. Comparing two `any` values directly panics at runtime ("comparing
// uncomparable type") as soon as either side holds a slice or map — which is
// every BLOB, VECTOR and JSON column this package supports — so recovering a
// WAL that touched such a table used to crash the process instead of replaying.
func rowsEqual(a, b []any) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !CanonicalIndexValueEqual(a[i], b[i]) {
			return false
		}
	}
	return true
}

// writeRecord writes a single WAL record.
func (w *AdvancedWAL) writeRecord(record *WALRecord) error {
	if w.closed || w.encoder == nil {
		return fmt.Errorf("advanced WAL is closed")
	}
	return w.encoder.Encode(record)
}

// flush flushes the write buffer and syncs to disk.
func (w *AdvancedWAL) flush() error {
	if w.closed {
		return nil
	}
	if w.writer != nil {
		if err := w.writer.Flush(); err != nil {
			return err
		}
	}
	if w.file == nil {
		return nil
	}
	if err := w.file.Sync(); err != nil {
		return err
	}
	return nil
}

// walCRCTable is the CRC32-Castagnoli table used for WAL record checksums.
// Castagnoli has hardware support (SSE4.2 / ARMv8 CRC) and far better error
// detection than the legacy additive checksum.
var walCRCTable = crc32.MakeTable(crc32.Castagnoli)

// Package-level constants for hashWALImage's framing bytes: sharing one
// preallocated slice per marker across every checksum computation, instead
// of converting a string literal to []byte at every call, means the h.Write
// escape cost is paid once for the package's lifetime rather than per call.
var (
	walOpenBracket  = []byte("[")
	walCloseBracket = []byte("]")
	walTildeMarker  = []byte("~")
)

// calculateChecksum computes a CRC32-Castagnoli checksum over every record
// field — including the before/after row images, which the legacy checksum
// did not cover, so image corruption previously went undetected.
func (w *AdvancedWAL) calculateChecksum(record *WALRecord) uint32 {
	h := crc32.New(walCRCTable)
	var b [8]byte
	writeU64 := func(v uint64) {
		binary.LittleEndian.PutUint64(b[:], v)
		_, _ = h.Write(b[:])
	}
	writeU64(uint64(record.LSN))
	writeU64(uint64(record.TxID))
	_, _ = h.Write([]byte{byte(record.OpType)})
	_, _ = io.WriteString(h, record.Tenant)
	_, _ = h.Write([]byte{0})
	_, _ = io.WriteString(h, record.Table)
	_, _ = h.Write([]byte{0})
	writeU64(uint64(record.RowID))
	writeU64(uint64(record.Timestamp.UnixNano()))
	// scratch is reused across every value/column below instead of each
	// hashWALValue call declaring its own local buffer: a buffer whose slice
	// is handed to h.Write (an interface method) escapes to the heap, so one
	// shared, heap-allocated-once buffer beats one fresh escape per value.
	var scratch [40]byte
	hashWALImage(h, record.BeforeImage, &scratch)
	hashWALImage(h, record.AfterImage, &scratch)
	// One buffer + one Write per column instead of five separate calls
	// (three of them single-byte literals, one a dynamic-string conversion):
	// each separate call to h.Write on the interface is a distinct potential
	// heap escape, whereas building "c<name>;<type>;" once in scratch and
	// writing it in a single call amortizes to the one shared allocation.
	for _, c := range record.Columns {
		b := append(scratch[:0], 'c')
		b = append(b, c.Name...)
		b = append(b, ';')
		b = strconv.AppendInt(b, int64(c.Type), 10)
		b = append(b, ';')
		_, _ = h.Write(b)
	}
	return h.Sum32()
}

// hashWALImage writes a canonical byte representation of a row image.
// The encoding must be identical before writing and after a gob round-trip:
// time.Time loses its monotonic clock reading in gob, so it is hashed via
// UnixNano, and maps are hashed in sorted key order.
func hashWALImage(h io.Writer, image []any, scratch *[40]byte) {
	if image == nil {
		_, _ = h.Write(walTildeMarker)
		return
	}
	_, _ = h.Write(walOpenBracket)
	for _, v := range image {
		hashWALValue(h, v, scratch)
	}
	_, _ = h.Write(walCloseBracket)
}

func hashWALValue(h io.Writer, v any, scratch *[40]byte) {
	switch t := v.(type) {
	case nil:
		_, _ = io.WriteString(h, "n;")
	case time.Time:
		_, _ = fmt.Fprintf(h, "t%d;", t.UnixNano())
	// Fast paths for the scalar kinds that make up the overwhelming majority
	// of logged column values, avoiding fmt.Fprintf's reflection overhead on
	// every value of every row image of every WAL record. These must produce
	// bytes identical to what the "%T%v;" fallback below would have written
	// for the same value — testdata/wal_fixtures/advancedwal_legacy.wal has
	// checksums baked in against that exact format, so this is a speed-only
	// change, not a format change (see TestAdvancedWALFixtureReplaysCorrectly).
	case int64:
		b := append(scratch[:0], "int64"...)
		b = strconv.AppendInt(b, t, 10)
		b = append(b, ';')
		_, _ = h.Write(b)
	case int:
		b := append(scratch[:0], "int"...)
		b = strconv.AppendInt(b, int64(t), 10)
		b = append(b, ';')
		_, _ = h.Write(b)
	case float64:
		b := append(scratch[:0], "float64"...)
		b = strconv.AppendFloat(b, t, 'g', -1, 64)
		b = append(b, ';')
		_, _ = h.Write(b)
	case string:
		// append(dst, t...) copies t's bytes directly (a compiler-recognized
		// idiom), unlike io.WriteString(h, t) which — since crc32's digest
		// doesn't implement io.StringWriter — falls back to a fresh []byte(t)
		// allocation the compiler cannot elide through the h.Write interface
		// call.
		b := append(scratch[:0], "string"...)
		b = append(b, t...)
		b = append(b, ';')
		_, _ = h.Write(b)
	case bool:
		if t {
			_, _ = io.WriteString(h, "booltrue;")
		} else {
			_, _ = io.WriteString(h, "boolfalse;")
		}
	case []float64:
		_, _ = io.WriteString(h, "V")
		var b [8]byte
		for _, f := range t {
			binary.LittleEndian.PutUint64(b[:], math.Float64bits(f))
			_, _ = h.Write(b[:])
		}
	case []any:
		_, _ = io.WriteString(h, "[")
		for _, e := range t {
			hashWALValue(h, e, scratch)
		}
		_, _ = io.WriteString(h, "]")
	case map[string]any:
		keys := make([]string, 0, len(t))
		for k := range t {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		_, _ = io.WriteString(h, "{")
		for _, k := range keys {
			_, _ = fmt.Fprintf(h, "%q:", k)
			hashWALValue(h, t[k], scratch)
		}
		_, _ = io.WriteString(h, "}")
	default:
		_, _ = fmt.Fprintf(h, "%T%v;", v, v)
	}
}

// legacyChecksum is the pre-CRC additive checksum. It is kept only so WAL
// files written by older versions still pass verification during recovery.
func (w *AdvancedWAL) legacyChecksum(record *WALRecord) uint32 {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)

	// Encode everything except the checksum field
	_ = enc.Encode(record.LSN)
	_ = enc.Encode(record.TxID)
	_ = enc.Encode(record.OpType)
	_ = enc.Encode(record.Tenant)
	_ = enc.Encode(record.Table)
	_ = enc.Encode(record.RowID)

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
	if w.closed {
		return nil
	}

	if err := w.flush(); err != nil {
		return err
	}

	if w.file != nil {
		if err := w.file.Close(); err != nil {
			return err
		}
	}
	w.closed = true
	w.file = nil
	w.writer = nil
	w.encoder = nil
	return nil
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

// Epoch returns this WAL's current epoch identifier -- see AdvancedWAL.epoch's
// doc comment for what it identifies and why a replication feed compares it
// across calls instead of relying on LSNs alone.
func (w *AdvancedWAL) Epoch() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.epoch
}

// GetFlushedLSN returns the LSN of the last flushed record.
func (w *AdvancedWAL) GetFlushedLSN() LSN {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.flushedLSN
}
