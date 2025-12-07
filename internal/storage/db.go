// Package storage provides the durable data structures for tinySQL.
//
// What: An in-memory multi-tenant catalog of tables with column metadata,
// rows, and basic typing. It includes snapshot cloning for MVCC-light,
// GOB-based checkpoints, and an append-only Write-Ahead Log (WAL) for crash
// recovery and durability.
// How: Tables store rows as [][]any for compactness; a lower-cased column
// index accelerates name lookups. Save/Load serialize the catalog to a file,
// writing JSON for JSON columns. The WAL logs whole-table changes and drops;
// recovery replays committed records and truncates partial tails.
// Why: Favor a simple, explicit model over complex page managers: it keeps the
// code understandable, testable, and sufficient for embedded/edge use cases.
package storage

import (
	"bufio"
	"bytes"
	"compress/gzip"
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

// safeGobRegister registers a type with encoding/gob but recovers from the
// known "registering duplicate names" panic which can occur when the same
// type is registered via different import paths in a multi-package build
// (for example when using Wails and building bindings). Ignoring that
// specific panic is safe for our use-case.
func safeGobRegister(v any) {
	defer func() {
		if r := recover(); r != nil {
			// If the panic is the duplicate registration panic from gob,
			// ignore it. Otherwise re-panic to avoid hiding real problems.
			if errStr, ok := r.(string); ok {
				if strings.Contains(errStr, "registering duplicate names") {
					return
				}
			}
			panic(r)
		}
	}()
	gob.Register(v)
}

func init() {
	// Register common storage types used in serialized snapshots. Use the
	// safe register helper to avoid build-time panics when types are
	// registered multiple times under different package paths.
	safeGobRegister(diskTable{})
	safeGobRegister(&diskTable{})
	safeGobRegister(Table{})
	safeGobRegister(&Table{})
}

// ColType enumerates supported column data types.
type ColType int

const (
	// Integer types
	IntType ColType = iota
	Int8Type
	Int16Type
	Int32Type
	Int64Type
	UintType
	Uint8Type
	Uint16Type
	Uint32Type
	Uint64Type

	// Floating point types
	Float32Type
	Float64Type
	FloatType // alias for Float64Type

	// String and character types
	StringType
	TextType // alias for StringType
	RuneType
	ByteType

	// Boolean type
	BoolType

	// Time types
	TimeType
	DateType
	DateTimeType
	TimestampType
	DurationType

	// Complex types
	JsonType
	JsonbType
	MapType
	SliceType
	ArrayType

	// Advanced types
	Complex64Type
	Complex128Type
	ComplexType // alias for Complex128Type
	PointerType
	InterfaceType
)

var colTypeToString = map[ColType]string{
	IntType:        "INT",
	Int8Type:       "INT8",
	Int16Type:      "INT16",
	Int32Type:      "INT32",
	Int64Type:      "INT64",
	UintType:       "UINT",
	Uint8Type:      "UINT8",
	Uint16Type:     "UINT16",
	Uint32Type:     "UINT32",
	Uint64Type:     "UINT64",
	Float32Type:    "FLOAT32",
	Float64Type:    "FLOAT64",
	FloatType:      "FLOAT64",
	StringType:     "STRING",
	TextType:       "TEXT",
	RuneType:       "RUNE",
	ByteType:       "BYTE",
	BoolType:       "BOOL",
	TimeType:       "TIME",
	DateType:       "DATE",
	DateTimeType:   "DATETIME",
	TimestampType:  "TIMESTAMP",
	DurationType:   "DURATION",
	JsonType:       "JSON",
	JsonbType:      "JSONB",
	MapType:        "MAP",
	SliceType:      "SLICE",
	ArrayType:      "ARRAY",
	Complex64Type:  "COMPLEX64",
	Complex128Type: "COMPLEX",
	ComplexType:    "COMPLEX",
	PointerType:    "POINTER",
	InterfaceType:  "INTERFACE",
}

func (t ColType) String() string {
	if s, ok := colTypeToString[t]; ok {
		return s
	}
	return "UNKNOWN"
}

// ConstraintType enumerates supported column constraints.
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

// ForeignKeyRef describes a foreign key reference target.
type ForeignKeyRef struct {
	Table  string
	Column string
}

// Column holds column schema information in a table.
type Column struct {
	Name         string
	Type         ColType
	Constraint   ConstraintType
	ForeignKey   *ForeignKeyRef // Only used if Constraint == ForeignKey
	PointerTable string         // Target table for POINTER type
}

// Table stores rows along with column metadata and indexes.
type Table struct {
	Name    string
	Cols    []Column
	Rows    [][]any
	IsTemp  bool
	colPos  map[string]int
	Version int
}

// NewTable creates a new Table with case-insensitive column lookup indices.
func NewTable(name string, cols []Column, isTemp bool) *Table {
	pos := make(map[string]int, len(cols))
	for i, c := range cols {
		pos[strings.ToLower(c.Name)] = i
	}
	return &Table{Name: name, Cols: cols, colPos: pos, IsTemp: isTemp}
}

// ColIndex returns the zero-based index of the named column.
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

// DB is an in-memory, multi-tenant database catalog with full MVCC support.
type DB struct {
	mu      sync.RWMutex
	tenants map[string]*tenantDB
	wal     *WALManager

	// MVCC coordinator
	mvcc *MVCCManager

	// Advanced WAL (optional - replaces basic WAL when enabled)
	advancedWAL *AdvancedWAL

	// System catalog for metadata and job scheduling
	catalog *CatalogManager
}

// NewDB creates a new empty database catalog with MVCC support.
func NewDB() *DB {
	return &DB{
		tenants: map[string]*tenantDB{},
		mvcc:    NewMVCCManager(),
	}
}

func (db *DB) getTenant(tn string) *tenantDB {
	tn = strings.ToLower(tn)
	td := db.tenants[tn]
	if td == nil {
		td = &tenantDB{tables: map[string]*Table{}}
		db.tenants[tn] = td
	}
	return td
}

// Get returns a table by name for the given tenant.
func (db *DB) Get(tn, name string) (*Table, error) {
	td := db.getTenant(tn)
	t, ok := td.tables[strings.ToLower(name)]
	if !ok {
		return nil, fmt.Errorf("no such table %q (tenant %q)", name, tn)
	}
	return t, nil
}

// Put adds a new table to the tenant; returns error if it already exists.
func (db *DB) Put(tn string, t *Table) error {
	td := db.getTenant(tn)
	lc := strings.ToLower(t.Name)
	if _, exists := td.tables[lc]; exists {
		return fmt.Errorf("table %q already exists (tenant %q)", t.Name, tn)
	}
	td.tables[lc] = t
	return nil
}

// Drop removes a table from the tenant.
func (db *DB) Drop(tn, name string) error {
	td := db.getTenant(tn)
	lc := strings.ToLower(name)
	if _, ok := td.tables[lc]; !ok {
		return fmt.Errorf("no such table %q (tenant %q)", name, tn)
	}
	delete(td.tables, lc)
	return nil
}

// ListTables returns the tables in a tenant sorted by name.
func (db *DB) ListTables(tn string) []*Table {
	td := db.getTenant(tn)
	if len(td.tables) == 0 {
		return nil
	}
	names := make([]string, 0, len(td.tables))
	for k := range td.tables {
		names = append(names, k)
	}
	sort.Strings(names)
	out := make([]*Table, len(names))
	for i, n := range names {
		out[i] = td.tables[n]
	}
	return out
}

// DeepClone creates a full copy of the database (MVCC-light snapshot).
// Note: This is not copy-on-write; it creates a full copy (simple but O(n)).
func (db *DB) DeepClone() *DB {
	if len(db.tenants) == 0 {
		return NewDB()
	}
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
	Rows    [][]any // JSON columns stored as strings
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
				switch vv := v.(type) {
				case string:
					// Already a JSON/text representation; keep as-is to avoid double encoding.
					row[j] = vv
				default:
					b, _ := json.Marshal(v)
					row[j] = string(b)
				}
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
			if ci >= len(cols) {
				break // Skip extra columns beyond schema
			}
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

// SaveToFile writes a snapshot of the database to a file. If the filename
// ends with .gz, the snapshot is gzip-compressed to reduce size.
func SaveToFile(db *DB, filename string) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	// Pre-allocate dump slice with estimated capacity
	var totalTables int
	for _, tdb := range db.tenants {
		totalTables += len(tdb.tables)
	}
	dump := make([]diskTable, 0, totalTables)
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

	var w io.Writer = bufio.NewWriter(f)
	// Enable gzip compression based on file extension.
	var gz *gzip.Writer
	if strings.HasSuffix(strings.ToLower(filename), ".gz") {
		gz = gzip.NewWriter(w)
		w = gz
	}
	enc := gob.NewEncoder(w)
	if err := enc.Encode(dump); err != nil {
		if gz != nil {
			_ = gz.Close()
		}
		if bw, ok := w.(*bufio.Writer); ok {
			_ = bw.Flush()
		}
		return err
	}
	if gz != nil {
		if err := gz.Close(); err != nil {
			return err
		}
	}
	// w is bufio.Writer when gz is nil, otherwise gz wraps it
	if bw, ok := w.(*bufio.Writer); ok {
		return bw.Flush()
	}
	return nil
}

// LoadFromFile loads a database snapshot from a file. It auto-detects gzip
// compression based on the .gz suffix and attaches a WAL if a path is given.
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
	var r io.Reader = bufio.NewReader(f)
	if strings.HasSuffix(strings.ToLower(filename), ".gz") {
		gr, gzErr := gzip.NewReader(r)
		if gzErr != nil {
			return nil, gzErr
		}
		defer gr.Close()
		r = gr
	}
	dec := gob.NewDecoder(r)
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

// SaveToWriter writes a snapshot of the database to an arbitrary writer.
// It does not attach or alter WAL configuration.
func SaveToWriter(db *DB, w io.Writer) error {
	db.mu.RLock()
	defer db.mu.RUnlock()
	// Pre-allocate dump slice with estimated capacity
	var totalTables int
	for _, tdb := range db.tenants {
		totalTables += len(tdb.tables)
	}
	dump := make([]diskTable, 0, totalTables)
	for tn, tdb := range db.tenants {
		for _, t := range tdb.tables {
			dump = append(dump, tableToDisk(tn, t))
		}
	}
	bw := bufio.NewWriter(w)
	enc := gob.NewEncoder(bw)
	if err := enc.Encode(dump); err != nil {
		return err
	}
	return bw.Flush()
}

// LoadFromReader loads a database snapshot from an arbitrary reader.
// The returned DB has no WAL attached.
func LoadFromReader(r io.Reader) (*DB, error) {
	dec := gob.NewDecoder(bufio.NewReader(r))
	var dump []diskTable
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
	return db, nil
}

// SaveToBytes serializes the database snapshot to a byte slice.
func SaveToBytes(db *DB) ([]byte, error) {
	var buf bytes.Buffer
	if err := SaveToWriter(db, &buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// LoadFromBytes loads a database from a byte slice.
func LoadFromBytes(b []byte) (*DB, error) {
	return LoadFromReader(bytes.NewReader(b))
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

// WALChange describes a persistent change that will be written to the WAL.
type WALChange struct {
	Tenant string
	Name   string
	Table  *Table
	Drop   bool
}

// CollectWALChanges computes the delta between two MVCC snapshots.
func CollectWALChanges(prev, next *DB) []WALChange {
	if prev == nil || next == nil {
		return nil
	}
	// Estimate capacity based on number of tables in next
	var estCapacity int
	for _, tdb := range next.tenants {
		estCapacity += len(tdb.tables)
	}
	changes := make([]WALChange, 0, estCapacity)
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

// WALConfig configures WAL and checkpoint behavior.
type WALConfig struct {
	Path               string
	CheckpointEvery    uint64
	CheckpointInterval time.Duration
}

// WALManager encapsulates WAL append, recovery, and checkpoints.
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

// AttachAdvancedWAL attaches an advanced WAL to the database.
func (db *DB) AttachAdvancedWAL(wal *AdvancedWAL) {
	db.mu.Lock()
	db.advancedWAL = wal
	db.mu.Unlock()
}

// AdvancedWAL returns the configured advanced WAL manager (may be nil).
func (db *DB) AdvancedWAL() *AdvancedWAL {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.advancedWAL
}

// MVCC returns the MVCC manager.
func (db *DB) MVCC() *MVCCManager {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.mvcc
}

// WAL returns the configured WAL manager (may be nil).
func (db *DB) WAL() *WALManager {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.wal
}

func (db *DB) upsertTable(tn string, t *Table) {
	td := db.getTenant(tn)
	td.tables[strings.ToLower(t.Name)] = t
}

// OpenWAL ensures a WAL exists, replays committed records, and returns a
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

// LogTransaction appends all changes atomically to the WAL.
// It returns true when a checkpoint is recommended.
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

// Checkpoint writes a DB snapshot and resets the WAL file.
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

// Close flushes, syncs, and closes the WAL resources.
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

func replayWAL(db *DB, walPath string) (nextSeq, nextTxID, committed uint64, err error) {
	f, err := os.Open(walPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 1, 1, 0, nil
		}
		return 0, 0, 0, err
	}
	defer f.Close()
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
				if lastGood >= 0 {
					_ = os.Truncate(walPath, lastGood)
				}
				return lastSeq + 1, lastTx + 1, committed, nil
			}
			return 0, 0, 0, err
		}
		lastGood = cr.n
		if rec.Seq > lastSeq {
			lastSeq = rec.Seq
		}
		if rec.TxID > lastTx {
			lastTx = rec.TxID
		}
		handleWalRecord(db, rec, pending, &committed)
	}
	return lastSeq + 1, lastTx + 1, committed, nil
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
		*committed++
	}
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
