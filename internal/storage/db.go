package storage

import (
	"bufio"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

type ColType int

const (
	IntType ColType = iota
	FloatType
	TextType
	BoolType
	JsonType
	DateType
	DateTimeType
	DurationType
	ComplexType
	PointerType
)

func (t ColType) String() string {
	switch t {
	case IntType:
		return "INT"
	case FloatType:
		return "FLOAT"
	case TextType:
		return "TEXT"
	case BoolType:
		return "BOOL"
	case JsonType:
		return "JSON"
	case DateType:
		return "DATE"
	case DateTimeType:
		return "DATETIME"
	case DurationType:
		return "DURATION"
	case ComplexType:
		return "COMPLEX"
	case PointerType:
		return "POINTER"
	default:
		return "UNKNOWN"
	}
}

type ConstraintType int

const (
	NoConstraint ConstraintType = iota
	PrimaryKey
	ForeignKey
	Unique
)

func (c ConstraintType) String() string {
	switch c {
	case PrimaryKey:
		return "PRIMARY KEY"
	case ForeignKey:
		return "FOREIGN KEY"
	case Unique:
		return "UNIQUE"
	default:
		return ""
	}
}

type ForeignKeyRef struct {
	Table  string
	Column string
}

type Column struct {
	Name         string
	Type         ColType
	Constraint   ConstraintType
	ForeignKey   *ForeignKeyRef // Only used if Constraint == ForeignKey
	PointerTable string         // Target table for POINTER type
}

type Table struct {
	Name    string
	Cols    []Column
	Rows    [][]any
	IsTemp  bool
	colPos  map[string]int
	Version int
}

func NewTable(name string, cols []Column, isTemp bool) *Table {
	pos := make(map[string]int)
	for i, c := range cols {
		pos[strings.ToLower(c.Name)] = i
	}
	return &Table{Name: name, Cols: cols, colPos: pos, IsTemp: isTemp}
}
func (t *Table) ColIndex(name string) (int, error) {
	i, ok := t.colPos[strings.ToLower(name)]
	if !ok {
		return -1, fmt.Errorf("unknown column %q on table %q", name, t.Name)
	}
	return i, nil
}

type tenantDB struct {
	tables map[string]*Table
}

type DB struct {
	mu      sync.RWMutex
	tenants map[string]*tenantDB
	wal     *WALManager
}

func NewDB() *DB { return &DB{tenants: map[string]*tenantDB{}} }

func (db *DB) getTenant(tn string) *tenantDB {
	tn = strings.ToLower(tn)
	td := db.tenants[tn]
	if td == nil {
		td = &tenantDB{tables: map[string]*Table{}}
		db.tenants[tn] = td
	}
	return td
}

func (db *DB) Get(tn, name string) (*Table, error) {
	td := db.getTenant(tn)
	t, ok := td.tables[strings.ToLower(name)]
	if !ok {
		return nil, fmt.Errorf("no such table %q (tenant %q)", name, tn)
	}
	return t, nil
}
func (db *DB) Put(tn string, t *Table) error {
	td := db.getTenant(tn)
	lc := strings.ToLower(t.Name)
	if _, exists := td.tables[lc]; exists {
		return fmt.Errorf("table %q already exists (tenant %q)", t.Name, tn)
	}
	td.tables[lc] = t
	return nil
}
func (db *DB) Drop(tn, name string) error {
	td := db.getTenant(tn)
	lc := strings.ToLower(name)
	if _, ok := td.tables[lc]; !ok {
		return fmt.Errorf("no such table %q (tenant %q)", name, tn)
	}
	delete(td.tables, lc)
	return nil
}

func (db *DB) ListTables(tn string) []*Table {
	td := db.getTenant(tn)
	names := make([]string, 0, len(td.tables))
	for k := range td.tables {
		names = append(names, k)
	}
	sort.Strings(names)
	out := make([]*Table, 0, len(names))
	for _, n := range names {
		out = append(out, td.tables[n])
	}
	return out
}

// DeepClone: defensive snapshot für "MVCC-light" Transaktionen.
// Achtung: kein Copy-on-Write, volle Kopie (einfach, aber O(n)).
func (db *DB) DeepClone() *DB {
	out := NewDB()
	out.wal = db.wal
	for tn, tdb := range db.tenants {
		for _, t := range tdb.tables {
			cols := make([]Column, len(t.Cols))
			copy(cols, t.Cols)
			nt := NewTable(t.Name, cols, t.IsTemp)
			nt.Version = t.Version
			nt.Rows = make([][]any, len(t.Rows))
			for i := range t.Rows {
				row := make([]any, len(t.Rows[i]))
				copy(row, t.Rows[i])
				nt.Rows[i] = row
			}
			out.Put(tn, nt)
		}
	}
	return out
}

// ------------------------ GOB Checkpoint (Load/Save) ------------------------

type diskColumn struct {
	Name         string
	Type         ColType
	Constraint   ConstraintType
	ForeignKey   *ForeignKeyRef
	PointerTable string
}
type diskTable struct {
	Tenant  string
	Name    string
	Cols    []diskColumn
	Rows    [][]any // JSON-Spalten als string gespeichert
	IsTemp  bool
	Version int
}

func tableToDisk(tn string, t *Table) diskTable {
	dt := diskTable{
		Tenant:  tn,
		Name:    t.Name,
		IsTemp:  t.IsTemp,
		Version: t.Version,
		Cols:    make([]diskColumn, len(t.Cols)),
		Rows:    make([][]any, len(t.Rows)),
	}
	for i, c := range t.Cols {
		dt.Cols[i] = diskColumn{
			Name:         c.Name,
			Type:         c.Type,
			Constraint:   c.Constraint,
			ForeignKey:   c.ForeignKey,
			PointerTable: c.PointerTable,
		}
	}
	for i, r := range t.Rows {
		row := make([]any, len(r))
		for j, v := range r {
			if v == nil {
				row[j] = nil
				continue
			}
			if t.Cols[j].Type == JsonType {
				b, _ := json.Marshal(v)
				row[j] = string(b)
			} else {
				row[j] = v
			}
		}
		dt.Rows[i] = row
	}
	return dt
}

func diskToTable(dt diskTable) *Table {
	cols := make([]Column, len(dt.Cols))
	for i, c := range dt.Cols {
		cols[i] = Column{
			Name:         c.Name,
			Type:         c.Type,
			Constraint:   c.Constraint,
			ForeignKey:   c.ForeignKey,
			PointerTable: c.PointerTable,
		}
	}
	t := NewTable(dt.Name, cols, dt.IsTemp)
	t.Version = dt.Version
	t.Rows = make([][]any, len(dt.Rows))
	for ri, r := range dt.Rows {
		row := make([]any, len(r))
		for ci, v := range r {
			if v == nil {
				row[ci] = nil
				continue
			}
			if cols[ci].Type == JsonType {
				var anyv any
				switch val := v.(type) {
				case string:
					if json.Unmarshal([]byte(val), &anyv) == nil {
						row[ci] = anyv
					} else {
						row[ci] = val
					}
				default:
					row[ci] = val
				}
			} else {
				row[ci] = v
			}
		}
		t.Rows[ri] = row
	}
	return t
}

func SaveToFile(db *DB, filename string) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	var dump []diskTable
	for tn, tdb := range db.tenants {
		for _, t := range tdb.tables {
			dump = append(dump, tableToDisk(tn, t))
		}
	}

	if err := os.MkdirAll(filepath.Dir(filename), 0o755); err != nil && !errors.Is(err, os.ErrExist) {
		return err
	}
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := gob.NewEncoder(f)
	return enc.Encode(dump)
}

func LoadFromFile(filename string) (*DB, error) {
	f, err := os.Open(filename)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return NewDB(), nil
		}
		return nil, err
	}
	defer f.Close()
	var dump []diskTable
	dec := gob.NewDecoder(f)
	if err := dec.Decode(&dump); err != nil {
		if errors.Is(err, io.EOF) {
			return NewDB(), nil
		}
		return nil, err
	}
	db := NewDB()
	for _, dt := range dump {
		_ = db.Put(dt.Tenant, diskToTable(dt))
	}
	if filename != "" {
		cfg := WALConfig{Path: filename}
		wal, err := OpenWAL(db, cfg)
		if err != nil {
			return nil, err
		}
		db.attachWAL(wal)
	}
	return db, nil
}

type walRecordType uint8

const (
	walRecordBegin walRecordType = iota + 1
	walRecordApplyTable
	walRecordDropTable
	walRecordCommit
)

type walRecord struct {
	Seq       uint64
	TxID      uint64
	Tenant    string
	TableName string
	Table     *diskTable
	Type      walRecordType
	WrittenAt int64
}

type walOperation struct {
	tenant string
	name   string
	drop   bool
	table  *diskTable
}

// WALChange beschreibt eine persistente Änderung, die in das WAL geschrieben wird.
type WALChange struct {
	Tenant string
	Name   string
	Table  *Table
	Drop   bool
}

// CollectWALChanges berechnet die Delta-Menge zwischen zwei MVCC-Snapshots.
func CollectWALChanges(prev, next *DB) []WALChange {
	if prev == nil || next == nil {
		return nil
	}
	changes := make([]WALChange, 0)
	for tn, nextTenant := range next.tenants {
		prevTenant := prev.tenants[tn]
		for key, nt := range nextTenant.tables {
			var pv *Table
			if prevTenant != nil {
				pv = prevTenant.tables[key]
			}
			if pv == nil || pv.Version != nt.Version {
				changes = append(changes, WALChange{Tenant: tn, Name: nt.Name, Table: nt})
			}
		}
	}
	for tn, prevTenant := range prev.tenants {
		nextTenant := next.tenants[tn]
		for key, pt := range prevTenant.tables {
			if nextTenant == nil || nextTenant.tables[key] == nil {
				changes = append(changes, WALChange{Tenant: tn, Name: pt.Name, Drop: true})
			}
		}
	}
	if len(changes) <= 1 {
		return changes
	}
	sort.SliceStable(changes, func(i, j int) bool {
		if changes[i].Tenant == changes[j].Tenant {
			return strings.ToLower(changes[i].Name) < strings.ToLower(changes[j].Name)
		}
		return strings.ToLower(changes[i].Tenant) < strings.ToLower(changes[j].Tenant)
	})
	return changes
}

// WALConfig steuert WAL- und Checkpoint-Verhalten.
type WALConfig struct {
	Path               string
	CheckpointEvery    uint64
	CheckpointInterval time.Duration
}

// WALManager kapselt WAL-Append, Recovery und Checkpoints.
type WALManager struct {
	mu                 sync.Mutex
	path               string
	checkpointPath     string
	checkpointEvery    uint64
	checkpointInterval time.Duration
	file               *os.File
	writer             *bufio.Writer
	encoder            *gob.Encoder
	nextSeq            uint64
	nextTxID           uint64
	txSinceCheckpoint  uint64
	lastCheckpoint     time.Time
}

func (db *DB) attachWAL(wal *WALManager) {
	db.mu.Lock()
	db.wal = wal
	db.mu.Unlock()
}

// WAL liefert den konfigurierten WAL-Manager (kann nil sein).
func (db *DB) WAL() *WALManager {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.wal
}

func (db *DB) upsertTable(tn string, t *Table) {
	td := db.getTenant(tn)
	td.tables[strings.ToLower(t.Name)] = t
}

// OpenWAL stellt sicher, dass ein WAL existiert, re-appliest vorhandene
// Committed-Records und liefert einen einsatzbereiten Manager.
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
	walPath := cfg.Path + ".wal"
	nextSeq, nextTxID, committed, err := replayWAL(db, walPath)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(cfg.Path), 0o755); err != nil && !errors.Is(err, os.ErrExist) {
		return nil, err
	}
	f, err := os.OpenFile(walPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		f.Close()
		return nil, err
	}
	writer := bufio.NewWriter(f)
	wm := &WALManager{
		path:               walPath,
		checkpointPath:     cfg.Path,
		checkpointEvery:    cfg.CheckpointEvery,
		checkpointInterval: cfg.CheckpointInterval,
		file:               f,
		writer:             writer,
		nextSeq:            nextSeq,
		nextTxID:           nextTxID,
		txSinceCheckpoint:  committed,
		lastCheckpoint:     time.Now(),
	}
	wm.encoder = gob.NewEncoder(writer)
	return wm, nil
}

// LogTransaction schreibt alle Änderungen atomar ins WAL; liefert true, wenn
// anschließend ein Checkpoint empfohlen ist.
func (w *WALManager) LogTransaction(changes []WALChange) (bool, error) {
	if w == nil || len(changes) == 0 {
		return false, nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
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
			dt := tableToDisk(ch.Tenant, ch.Table)
			rec.Type = walRecordApplyTable
			rec.Table = &dt
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
	return need, nil
}

// Checkpoint schreibt den aktuellen DB-Snapshot und leert das WAL.
func (w *WALManager) Checkpoint(db *DB) error {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.checkpointPath == "" {
		return nil
	}
	if err := SaveToFile(db, w.checkpointPath); err != nil {
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
		f.Close()
		return err
	}
	w.file = f
	w.writer = bufio.NewWriter(f)
	w.encoder = gob.NewEncoder(w.writer)
	w.nextSeq = 1
	w.txSinceCheckpoint = 0
	w.lastCheckpoint = time.Now()
	return nil
}

// Close sorgt für ein sauberes Flushen/Syncen.
func (w *WALManager) Close() error {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.writer != nil {
		if err := w.writer.Flush(); err != nil {
			return err
		}
	}
	if w.file != nil {
		if err := w.file.Sync(); err != nil {
			return err
		}
		return w.file.Close()
	}
	return nil
}

func (w *WALManager) writeRecord(rec *walRecord) error {
	if err := w.encoder.Encode(rec); err != nil {
		return err
	}
	return nil
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

func replayWAL(db *DB, walPath string) (nextSeq, nextTxID, committed uint64, err error) {
	f, err := os.Open(walPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 1, 1, 0, nil
		}
		return 0, 0, 0, err
	}
	cr := &countingReader{r: f}
	dec := gob.NewDecoder(cr)
	pending := make(map[uint64][]walOperation)
	var lastSeq uint64
	var lastTx uint64
	var lastGood int64
	for {
		var rec walRecord
		if err := dec.Decode(&rec); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.ErrNoProgress) || strings.Contains(err.Error(), "EOF") {
				f.Close()
				if lastGood >= 0 {
					_ = os.Truncate(walPath, lastGood)
				}
				return lastSeq + 1, lastTx + 1, committed, nil
			}
			f.Close()
			return 0, 0, 0, err
		}
		lastGood = cr.n
		if rec.Seq > lastSeq {
			lastSeq = rec.Seq
		}
		if rec.TxID > lastTx {
			lastTx = rec.TxID
		}
		switch rec.Type {
		case walRecordBegin:
			pending[rec.TxID] = nil
		case walRecordApplyTable:
			if rec.Table == nil {
				continue
			}
			dt := *rec.Table
			pending[rec.TxID] = append(pending[rec.TxID], walOperation{tenant: rec.Tenant, name: dt.Name, table: &dt})
		case walRecordDropTable:
			pending[rec.TxID] = append(pending[rec.TxID], walOperation{tenant: rec.Tenant, name: rec.TableName, drop: true})
		case walRecordCommit:
			ops := pending[rec.TxID]
			for _, op := range ops {
				if op.drop {
					_ = db.Drop(op.tenant, op.name)
					continue
				}
				db.upsertTable(op.tenant, diskToTable(*op.table))
			}
			delete(pending, rec.TxID)
			committed++
		}
	}
	f.Close()
	return lastSeq + 1, lastTx + 1, committed, nil
}

type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}
