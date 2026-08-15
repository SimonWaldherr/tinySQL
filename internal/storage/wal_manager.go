package storage

import (
	"bufio"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// WALConfig configures WAL and checkpoint behavior.
type WALConfig struct {
	Path               string
	CheckpointEvery    uint64
	CheckpointInterval time.Duration
	// CheckpointMaxBytes forces a checkpoint once the WAL file exceeds this
	// size, bounding WAL growth independently of transaction count and time.
	// Zero means default (64 MB); negative disables the size trigger.
	CheckpointMaxBytes int64
}

// defaultCheckpointMaxBytes bounds WAL growth when no explicit limit is set.
const defaultCheckpointMaxBytes = 64 << 20 // 64 MB

// normalizeCheckpointMaxBytes maps the config convention (0 = default,
// negative = disabled) onto the internal one (0 = disabled).
func normalizeCheckpointMaxBytes(v int64) int64 {
	if v == 0 {
		return defaultCheckpointMaxBytes
	}
	if v < 0 {
		return 0
	}
	return v
}

// WALManager encapsulates WAL append, recovery, and checkpoints.
type WALManager struct {
	mu                 sync.Mutex
	path               string
	checkpointPath     string
	checkpointEvery    uint64
	checkpointInterval time.Duration
	checkpointMaxBytes int64
	file               *os.File
	bytes              *countingWriter
	writer             *bufio.Writer
	encoder            *gob.Encoder
	nextSeq            uint64
	nextTxID           uint64
	txSinceCheckpoint  uint64
	lastCheckpoint     time.Time
	closed             bool
	recovery           RecoveryStatus
	// checkpointWatermark is the highest Seq the checkpoint file already
	// reflects. Checkpointing writes the snapshot first and only then truncates
	// the log, so a crash in between leaves a log whose records are all already
	// in the snapshot; replaying them would apply each append-rows delta a
	// second time and duplicate committed rows. Recovery skips every record at
	// or below this mark.
	//
	// Seq therefore has to keep increasing across checkpoints. Resetting it to 1
	// after a truncation, as this used to, would make the next records compare
	// below a watermark from before the truncation and be skipped.
	checkpointWatermark uint64
}

// ready-to-use manager. It attaches no WAL when Path is empty.
func OpenWAL(db *DB, cfg WALConfig) (*WALManager, error) {
	if cfg.Path == "" {
		return nil, nil
	}
	if cfg.CheckpointEvery == 0 {
		cfg.CheckpointEvery = 32
	}
	if cfg.CheckpointInterval <= 0 {
		cfg.CheckpointInterval = 30 * time.Second
	}
	basePath := cfg.Path
	if strings.HasSuffix(strings.ToLower(basePath), ".gz") {
		basePath = strings.TrimSuffix(basePath, ".gz")
	}
	walPath := basePath + ".wal"
	// The checkpoint that OpenDB has already loaded into db records how far it
	// reflects the log. Records at or below that mark are already in db; applying
	// them again would duplicate rows appended by an append-rows delta.
	watermark, err := ReadCheckpointWatermark(cfg.Path)
	if err != nil {
		return nil, err
	}
	nextSeq, nextTxID, committed, truncated, err := replayWAL(db, walPath, watermark)
	if err != nil {
		return nil, err
	}
	if nextSeq <= watermark {
		nextSeq = watermark + 1
	}
	if err := os.MkdirAll(filepath.Dir(cfg.Path), 0o755); err != nil && !errors.Is(err, os.ErrExist) {
		return nil, err
	}
	f, err := os.OpenFile(walPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	size, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	cw := &countingWriter{w: f, n: size}
	writer := bufio.NewWriter(cw)
	wm := &WALManager{
		path:                walPath,
		checkpointWatermark: watermark,
		checkpointPath:      cfg.Path,
		checkpointEvery:     cfg.CheckpointEvery,
		checkpointInterval:  cfg.CheckpointInterval,
		checkpointMaxBytes:  normalizeCheckpointMaxBytes(cfg.CheckpointMaxBytes),
		file:                f,
		bytes:               cw,
		writer:              writer,
		nextSeq:             nextSeq,
		nextTxID:            nextTxID,
		txSinceCheckpoint:   committed,
		lastCheckpoint:      time.Now(),
		recovery: RecoveryStatus{
			Mode:                  ModeWAL,
			Path:                  walPath,
			RecoveredTransactions: committed,
			Truncated:             truncated,
			RecoveredAt:           time.Now(),
		},
	}
	wm.encoder = gob.NewEncoder(writer)
	return wm, nil
}

// LogTransaction appends all changes atomically to the WAL.
// It returns true when a checkpoint is recommended.
func (w *WALManager) LogTransaction(changes []WALChange) (bool, error) {
	if w == nil || len(changes) == 0 {
		return false, nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return false, fmt.Errorf("wal is closed")
	}
	txID := w.nextTxID
	w.nextTxID++
	if err := w.writeRecord(&walRecord{Seq: w.nextSeq, TxID: txID, Type: walRecordBegin, WrittenAt: time.Now().UnixNano()}); err != nil {
		return false, err
	}
	w.nextSeq++
	for _, ch := range changes {
		rec := &walRecord{
			Seq:       w.nextSeq,
			TxID:      txID,
			Tenant:    ch.Tenant,
			TableName: ch.Name,
			WrittenAt: time.Now().UnixNano(),
		}
		if ch.Drop {
			rec.Type = walRecordDropTable
		} else if ch.Table != nil {
			dirty := ch.Table.DirtyFrom()
			updated, exact := ch.Table.DirtyRows()
			switch {
			case dirty >= 0 && dirty < len(ch.Table.Rows):
				// Append-only change: write only the new rows.
				dt := tableToDiskRange(ch.Tenant, ch.Table, dirty, len(ch.Table.Rows))
				rec.Type = walRecordAppendRows
				rec.Table = &dt
			case exact && len(updated) < len(ch.Table.Rows):
				// In-place UPDATE: write only the rows that changed. Without
				// this an UPDATE of one row in a large table serialized and
				// fsynced every row in it.
				dt, idx := tableToDiskRows(ch.Tenant, ch.Table, updated)
				rec.Type = walRecordUpdateRows
				rec.Table = &dt
				rec.RowIndexes = idx
			default:
				// Full table change (DELETE, CREATE, or unknown shape).
				dt := tableToDisk(ch.Tenant, ch.Table)
				rec.Type = walRecordApplyTable
				rec.Table = &dt
			}
			ch.Table.ResetDirty()
		} else {
			continue
		}
		if err := w.writeRecord(rec); err != nil {
			return false, err
		}
		w.nextSeq++
	}
	if err := w.writeRecord(&walRecord{Seq: w.nextSeq, TxID: txID, Type: walRecordCommit, WrittenAt: time.Now().UnixNano()}); err != nil {
		return false, err
	}
	w.nextSeq++
	if err := w.flushSync(); err != nil {
		return false, err
	}
	w.txSinceCheckpoint++
	need := w.txSinceCheckpoint >= w.checkpointEvery
	if !need && w.checkpointInterval > 0 && time.Since(w.lastCheckpoint) >= w.checkpointInterval {
		need = true
	}
	if !need && w.checkpointMaxBytes > 0 && w.bytes.n >= w.checkpointMaxBytes {
		need = true
	}
	return need, nil
}

// Checkpoint writes a DB snapshot and resets the WAL file.
func (w *WALManager) Checkpoint(db *DB) error {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return fmt.Errorf("wal is closed")
	}
	if w.checkpointPath == "" {
		return nil
	}
	// Everything written so far is in db and therefore in the snapshot about to
	// be saved. Record that as the watermark inside the snapshot itself, so a
	// crash before the truncation below does not replay records the snapshot
	// already contains.
	watermark := w.nextSeq - 1
	if err := SaveToFile(db, w.checkpointPath, watermark); err != nil {
		return err
	}
	if err := w.flushSync(); err != nil {
		return err
	}
	if err := w.file.Close(); err != nil {
		return err
	}
	if err := os.Truncate(w.path, 0); err != nil {
		return err
	}
	f, err := os.OpenFile(w.path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		_ = f.Close()
		return err
	}
	w.file = f
	w.bytes = &countingWriter{w: f}
	w.writer = bufio.NewWriter(w.bytes)
	w.encoder = gob.NewEncoder(w.writer)
	// Seq deliberately keeps counting from where it was: see checkpointWatermark.
	w.checkpointWatermark = watermark
	w.txSinceCheckpoint = 0
	w.lastCheckpoint = time.Now()
	return nil
}

// Close flushes, syncs, and closes the WAL resources.
func (w *WALManager) Close() error {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	if w.writer != nil {
		if err := w.writer.Flush(); err != nil {
			return err
		}
	}
	if w.file != nil {
		if err := w.file.Sync(); err != nil {
			return err
		}
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

func (w *WALManager) writeRecord(rec *walRecord) error {
	if w.closed || w.encoder == nil {
		return fmt.Errorf("wal is closed")
	}
	return w.encoder.Encode(rec)
}

func (w *WALManager) flushSync() error {
	if w.writer != nil {
		if err := w.writer.Flush(); err != nil {
			return err
		}
	}
	if w.file != nil {
		if err := w.file.Sync(); err != nil {
			return err
		}
	}
	return nil
}

// replayWAL applies the log to db. Records at or below watermark are skipped:
// the checkpoint db was loaded from already reflects them.
func replayWAL(db *DB, walPath string, watermark uint64) (nextSeq, nextTxID, committed uint64, truncated bool, err error) {
	f, err := os.Open(walPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 1, 1, 0, false, nil
		}
		return 0, 0, 0, false, err
	}
	defer func() { _ = f.Close() }()
	cr := newCountingReader(f)
	dec := gob.NewDecoder(cr)
	pending := make(map[uint64][]walOperation)
	var lastSeq uint64
	var lastTx uint64
	var lastGood int64
	lastSeq = watermark
	for {
		var rec walRecord
		if err := dec.Decode(&rec); err != nil {
			// A clean end of file: everything in the log decoded and was applied.
			//
			// EOF can also mean "gob stopped early" rather than "no more data": a
			// run of zero bytes decodes as a zero-length message, which gob
			// reports as EOF. Leaving such a remainder in place would make every
			// record appended after it invisible to the next recovery — silent
			// loss of committed data — so an unconsumed remainder counts as a
			// damaged tail, like any other decode error.
			if errors.Is(err, io.EOF) {
				if size, statErr := fileSize(f); statErr != nil || lastGood >= size {
					return lastSeq + 1, lastTx + 1, committed, false, nil
				}
			}
			// The tail is damaged: a write interrupted by a crash, a partially
			// flushed buffer, a corrupted or zero-filled byte range. Everything up
			// to lastGood decoded cleanly and has been applied, so cut the log
			// there and report the truncation rather than refusing to open the
			// database. Treating a damaged tail as fatal left the database
			// permanently unopenable, with no way to reach the data that had in
			// fact recovered fine.
			_ = f.Close()
			if truncErr := os.Truncate(walPath, lastGood); truncErr != nil {
				return 0, 0, 0, false, fmt.Errorf("truncate torn wal at %d: %w", lastGood, truncErr)
			}
			return lastSeq + 1, lastTx + 1, committed, true, nil
		}
		lastGood = cr.n
		if rec.Seq > lastSeq {
			lastSeq = rec.Seq
		}
		if rec.TxID > lastTx {
			lastTx = rec.TxID
		}
		if rec.Seq != 0 && rec.Seq <= watermark {
			// Already reflected in the checkpoint this database was loaded from.
			// Applying it again would duplicate rows for an append-rows delta.
			continue
		}
		handleWalRecord(db, rec, pending, &committed)
	}
}

// handleWalRecord processes a single WAL record and updates pending map and committed count.
func handleWalRecord(db *DB, rec walRecord, pending map[uint64][]walOperation, committed *uint64) {
	switch rec.Type {
	case walRecordBegin:
		pending[rec.TxID] = nil
	case walRecordApplyTable:
		if rec.Table == nil {
			return
		}
		dt := *rec.Table
		pending[rec.TxID] = append(pending[rec.TxID], walOperation{tenant: rec.Tenant, name: dt.Name, table: &dt})
	case walRecordAppendRows:
		if rec.Table == nil {
			return
		}
		dt := *rec.Table
		pending[rec.TxID] = append(pending[rec.TxID], walOperation{tenant: rec.Tenant, name: dt.Name, table: &dt, appendOnly: true})
	case walRecordUpdateRows:
		if rec.Table == nil || len(rec.RowIndexes) != len(rec.Table.Rows) {
			// A record whose positions do not line up with its rows cannot be
			// applied safely. Dropping the operation and keeping the rest of the
			// transaction would resurrect a partial state, so drop the whole
			// transaction: without a matching commit its other operations are
			// discarded too.
			delete(pending, rec.TxID)
			return
		}
		dt := *rec.Table
		pending[rec.TxID] = append(pending[rec.TxID], walOperation{
			tenant:     rec.Tenant,
			name:       dt.Name,
			table:      &dt,
			rowIndexes: append([]int(nil), rec.RowIndexes...),
		})
	case walRecordDropTable:
		pending[rec.TxID] = append(pending[rec.TxID], walOperation{tenant: rec.Tenant, name: rec.TableName, drop: true})
	case walRecordCommit:
		ops := pending[rec.TxID]
		for _, op := range ops {
			if op.drop {
				_ = db.Drop(op.tenant, op.name)
				continue
			}
			if op.appendOnly {
				// Delta replay: append rows to existing table.
				existing, _ := db.Get(op.tenant, op.name)
				if existing != nil {
					delta := diskToTable(*op.table)
					existing.Rows = append(existing.Rows, delta.Rows...)
					existing.Version = delta.Version
					// WAL deltas carry rows, while the existing table owns the
					// durable index definitions. Rebuild so recovered index row IDs
					// and table rows are atomically consistent.
					_ = existing.RebuildSecondaryIndexes()
					continue
				}
				// Fallback: table not found, apply as full table.
			}
			if op.rowIndexes != nil {
				// Delta replay: put each logged row back at its own position.
				existing, _ := db.Get(op.tenant, op.name)
				if existing != nil && rowIndexesFit(op.rowIndexes, len(existing.Rows)) {
					delta := diskToTable(*op.table)
					// Replaces rows without changing the row count, so
					// executor state derived from them cannot detect the
					// change by itself and has to be dropped here.
					existing.dropDerived()
					for i, idx := range op.rowIndexes {
						existing.Rows[idx] = delta.Rows[i]
					}
					existing.Version = delta.Version
					// Replaces existing rows in place, so anything keyed on
					// "only appends happened since I last looked" (see
					// Table.noteStructuralChange) must not treat this table
					// as append-only anymore.
					existing.noteStructuralChange()
					_ = existing.RebuildSecondaryIndexes()
					continue
				}
				// The table is gone, or shorter than the record expects: this
				// delta describes rows that no longer exist, so there is nothing
				// meaningful to replay. Skip it rather than writing a table that
				// holds only the updated rows.
				continue
			}
			db.upsertTable(op.tenant, diskToTable(*op.table))
		}
		delete(pending, rec.TxID)
		*committed++
	}
}

// rowIndexesFit reports whether every position in an update-rows delta still
// addresses a row of the table being recovered.
func rowIndexesFit(indexes []int, rows int) bool {
	for _, idx := range indexes {
		if idx < 0 || idx >= rows {
			return false
		}
	}
	return true
}

// fileSize reports the size of an open file.
func fileSize(f *os.File) (int64, error) {
	info, err := f.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

// countingReader tracks how many bytes have been consumed, so a torn WAL can be
// truncated at the end of its last complete record.
//
// It implements io.ByteReader on purpose. gob.NewDecoder wraps a reader that
// does not in a bufio.Reader of its own, which reads ahead: the count then
// reflects how much the decoder pulled rather than how much it used, and
// truncating at that offset either cuts into a good record or leaves part of a
// torn one behind — the latter making the database unopenable on the next start.
// Implementing ReadByte makes gob read straight through, so the count is exact.
type countingReader struct {
	r  io.Reader
	br *bufio.Reader
	n  int64
}

func newCountingReader(r io.Reader) *countingReader {
	return &countingReader{r: r, br: bufio.NewReader(r)}
}

func (c *countingReader) Read(p []byte) (int, error) {
	if c.br == nil {
		n, err := c.r.Read(p)
		c.n += int64(n)
		return n, err
	}
	n, err := c.br.Read(p)
	c.n += int64(n)
	return n, err
}

func (c *countingReader) ReadByte() (byte, error) {
	if c.br == nil {
		var buf [1]byte
		n, err := c.r.Read(buf[:])
		c.n += int64(n)
		if n == 1 {
			return buf[0], nil
		}
		if err == nil {
			err = io.ErrNoProgress
		}
		return 0, err
	}
	b, err := c.br.ReadByte()
	if err == nil {
		c.n++
	}
	return b, err
}

// countingWriter tracks the number of bytes written through it. Used to
// bound WAL file growth without stat() calls on the hot path.
type countingWriter struct {
	w io.Writer
	n int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}
