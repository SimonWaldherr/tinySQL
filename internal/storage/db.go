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
	"sync/atomic"
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
			if strings.Contains(fmt.Sprint(r), "registering duplicate names") {
				return
			}
			panic(r)
		}
	}()
	gob.Register(v)
}

// ColType enumerates supported column data types.
type ColType int

const (
	// IntType is a generic integer column type.
	IntType ColType = iota
	// Int8Type is an 8-bit signed integer column type.
	Int8Type
	// Int16Type is a 16-bit signed integer column type.
	Int16Type
	// Int32Type is a 32-bit signed integer column type.
	Int32Type
	// Int64Type is a 64-bit signed integer column type.
	Int64Type
	// UintType is an unsigned integer column type.
	UintType
	// Uint8Type is an 8-bit unsigned integer column type.
	Uint8Type
	// Uint16Type is a 16-bit unsigned integer column type.
	Uint16Type
	// Uint32Type is a 32-bit unsigned integer column type.
	Uint32Type
	// Uint64Type is a 64-bit unsigned integer column type.
	Uint64Type

	// Float32Type is a 32-bit floating point column type.
	Float32Type
	// Float64Type is a 64-bit floating point column type.
	Float64Type
	// FloatType is an alias for Float64Type.
	FloatType // alias for Float64Type

	// StringType represents a variable-length UTF-8 string column.
	StringType
	// TextType is an alias for StringType intended for long text.
	TextType // alias for StringType
	// RuneType stores single Unicode code points.
	RuneType
	// ByteType stores raw byte data.
	ByteType

	// BoolType represents a boolean column (true/false).
	BoolType

	// TimeType stores time-of-day values.
	TimeType
	// DateType stores date-only values.
	DateType
	// DateTimeType stores combined date and time values.
	DateTimeType
	// TimestampType stores an absolute point in time.
	TimestampType
	// DurationType stores a time duration.
	DurationType

	// DecimalType stores arbitrary-precision decimal numbers.
	DecimalType
	// MoneyType is a convenience alias for DecimalType used for monetary values.
	MoneyType
	// UUIDType stores RFC-4122 UUID values.
	UUIDType
	// BlobType stores binary large objects.
	BlobType
	// XMLType stores XML text.
	XMLType
	// IntervalType stores SQL-like intervals (parsed to time.Duration when possible).
	IntervalType

	// JsonType stores JSON text.
	JsonType
	// JsonbType stores binary JSON representations.
	JsonbType
	// MapType stores map-like complex values.
	MapType
	// SliceType stores slice-like complex values.
	SliceType
	// ArrayType stores array-like complex values.
	ArrayType

	// Complex64Type stores complex64 numeric values.
	Complex64Type
	// Complex128Type stores complex128 numeric values.
	Complex128Type
	// ComplexType is an alias for Complex128Type.
	ComplexType // alias for Complex128Type
	// PointerType represents a pointer/reference to another object.
	PointerType
	// InterfaceType represents an arbitrary Go interface value.
	InterfaceType

	// VectorType represents a vector/embedding column used by RAG features.
	VectorType
	// GeometryType stores spatial geometry values (GeoJSON/WKB) as JSONB or binary payload.
	GeometryType

	// YAMLType stores YAML-formatted text data.
	YAMLType
	// URLType stores URL/URI values with optional validation.
	URLType
	// HASHType stores cryptographic hash digests (hex-encoded).
	HASHType
	// BitmapType stores roaring-bitmap or bitset values as a byte slice.
	BitmapType
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
	VectorType:     "VECTOR",
	GeometryType:   "GEOMETRY",
	YAMLType:       "YAML",
	URLType:        "URL",
	HASHType:       "HASH",
	BitmapType:     "BITMAP",
	// Additional types
	DecimalType:  "DECIMAL",
	MoneyType:    "MONEY",
	UUIDType:     "UUID",
	BlobType:     "BLOB",
	XMLType:      "XML",
	IntervalType: "INTERVAL",
}

// SQLiteAffinity is the five-class type system used by SQLite declarations.
// It is schema metadata, not another runtime value type: tinySQL continues to
// store NULL, integer, real, text and binary values directly. Keeping the
// declared affinity separate lets imported SQLite schemas retain their
// lossless-coercion behaviour without multiplying ColType values.
type SQLiteAffinity uint8

const (
	// AffinityDefault retains tinySQL's native, strongly typed coercion rules.
	AffinityDefault SQLiteAffinity = iota
	AffinityInteger
	AffinityText
	AffinityNumeric
	AffinityReal
	AffinityBlob
)

func (a SQLiteAffinity) String() string {
	switch a {
	case AffinityInteger:
		return "INTEGER"
	case AffinityText:
		return "TEXT"
	case AffinityNumeric:
		return "NUMERIC"
	case AffinityReal:
		return "REAL"
	case AffinityBlob:
		return "BLOB"
	default:
		return ""
	}
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

// ReferentialAction enumerates what happens to a child row when the parent
// row it references is deleted (ON DELETE) or its referenced column value
// changes (ON UPDATE).
type ReferentialAction int

const (
	// NoAction means no ON DELETE/ON UPDATE clause was given. tinySQL checks
	// foreign key constraints immediately (it has no deferred-constraint
	// mode), so NoAction and Restrict behave identically here — the SQL
	// standard only distinguishes them for deferred checking.
	NoAction ReferentialAction = iota
	// Restrict blocks the parent-side mutation while a referencing child row exists.
	Restrict
	// Cascade propagates the parent-side delete/update to matching child rows.
	Cascade
	// SetNull nulls the child row's foreign key column instead of deleting/blocking.
	SetNull
)

func (a ReferentialAction) String() string {
	switch a {
	case Restrict:
		return "RESTRICT"
	case Cascade:
		return "CASCADE"
	case SetNull:
		return "SET NULL"
	default:
		return "NO ACTION"
	}
}

// ForeignKeyRef describes a foreign key reference target.
type ForeignKeyRef struct {
	Table    string
	Column   string
	OnDelete ReferentialAction
	OnUpdate ReferentialAction
}

// Column holds column schema information in a table.
type Column struct {
	Name string
	Type ColType
	// DeclaredType retains the source SQL spelling (for example VARCHAR(80)
	// or DOUBLE PRECISION). It is intentionally metadata; the physical value
	// representation remains ColType plus SQLiteAffinity.
	DeclaredType string
	// Affinity is populated for SQLite-style declarations. AffinityDefault
	// means this column was declared using a native tinySQL type.
	Affinity SQLiteAffinity
	// NotNull and default metadata are independent of Constraint because SQL
	// permits combinations such as "PRIMARY KEY NOT NULL DEFAULT 0".
	NotNull      bool
	HasDefault   bool
	DefaultValue any
	Constraint   ConstraintType
	ForeignKey   *ForeignKeyRef // Only used if Constraint == ForeignKey
	PointerTable string         // Target table for POINTER type
}

// Table stores rows along with column metadata and indexes.
type Table struct {
	Name string
	Cols []Column
	Rows [][]any
	// Indexes contains materialized secondary and composite indexes keyed by
	// lower-case SQL index name. Unlike catalog metadata these entries are
	// used by the executor and persisted with table snapshots.
	Indexes map[string]*SecondaryIndex
	IsTemp  bool
	colPos  map[string]int
	Version int
	// Stats is populated by ANALYZE and persisted with the table. DML marks it
	// stale rather than trying to estimate distinct values incrementally.
	Stats *TableStats
	// dirtyFrom tracks the first row index modified since the last
	// WAL checkpoint. -1 means no dirty rows (full table must be logged).
	// For append-only workloads (INSERT without UPDATE/DELETE), this
	// enables the WAL to log only new rows instead of the entire table.
	dirtyFrom int
}

// ColumnStats summarizes one column as of TableStats.AnalyzedAt. Min and Max
// are display values for introspection; the planner currently uses row and
// distinct counts, which remain meaningful across all supported column types.
type ColumnStats struct {
	NullCount     int
	DistinctCount int
	Min           string
	Max           string
	HasMinMax     bool
}

// TableStats is the persisted result of ANALYZE for one table.
type TableStats struct {
	RowCount   int
	Columns    map[string]ColumnStats // lower-cased column name → statistics
	AnalyzedAt time.Time
	Stale      bool
}

// NewTable creates a new Table with case-insensitive column lookup indices.
func NewTable(name string, cols []Column, isTemp bool) *Table {
	pos := make(map[string]int, len(cols))
	for i, c := range cols {
		pos[strings.ToLower(c.Name)] = i
	}
	return &Table{Name: name, Cols: cols, colPos: pos, IsTemp: isTemp, dirtyFrom: -1, Indexes: make(map[string]*SecondaryIndex)}
}

// Analyze computes exact cardinality, null and simple range summaries for the
// current table contents. The first statistics implementation scans all rows
// deliberately: transparent and correct inputs are more useful than a sampled
// model whose accuracy would need separate policy and tuning.
func (t *Table) Analyze() *TableStats {
	stats := &TableStats{
		RowCount:   len(t.Rows),
		Columns:    make(map[string]ColumnStats, len(t.Cols)),
		AnalyzedAt: time.Now().UTC(),
	}
	for colIdx, column := range t.Cols {
		columnStats := ColumnStats{}
		distinct := make(map[string]struct{})
		var minValue, maxValue any
		for _, row := range t.Rows {
			if colIdx >= len(row) || row[colIdx] == nil {
				columnStats.NullCount++
				continue
			}
			value := row[colIdx]
			distinct[string(CanonicalIndexKey([]any{value}))] = struct{}{}
			if !columnStats.HasMinMax || statsLess(value, minValue) {
				minValue = value
			}
			if !columnStats.HasMinMax || statsLess(maxValue, value) {
				maxValue = value
			}
			columnStats.HasMinMax = true
		}
		columnStats.DistinctCount = len(distinct)
		if columnStats.HasMinMax {
			columnStats.Min = fmt.Sprint(minValue)
			columnStats.Max = fmt.Sprint(maxValue)
		}
		stats.Columns[strings.ToLower(column.Name)] = columnStats
	}
	t.Stats = stats
	return cloneTableStats(stats)
}

// InvalidateStats marks the previous ANALYZE result stale after a mutation.
// RowCount remains useful for observability while distinct/range values are
// excluded from planner decisions until ANALYZE is run again.
func (t *Table) InvalidateStats() {
	if t.Stats == nil {
		return
	}
	t.Stats.RowCount = len(t.Rows)
	t.Stats.Stale = true
}

// Statistics returns a defensive copy of the latest ANALYZE result.
func (t *Table) Statistics() *TableStats { return cloneTableStats(t.Stats) }

func cloneTableStats(stats *TableStats) *TableStats {
	if stats == nil {
		return nil
	}
	copy := *stats
	copy.Columns = make(map[string]ColumnStats, len(stats.Columns))
	for name, column := range stats.Columns {
		copy.Columns[name] = column
	}
	return &copy
}

func statsLess(left, right any) bool {
	if right == nil {
		return true
	}
	leftNumber, leftIsNumber := statsNumber(left)
	rightNumber, rightIsNumber := statsNumber(right)
	if leftIsNumber && rightIsNumber {
		return leftNumber < rightNumber
	}
	if leftTime, ok := left.(time.Time); ok {
		if rightTime, ok := right.(time.Time); ok {
			return leftTime.Before(rightTime)
		}
	}
	if leftDuration, ok := left.(time.Duration); ok {
		if rightDuration, ok := right.(time.Duration); ok {
			return leftDuration < rightDuration
		}
	}
	return fmt.Sprint(left) < fmt.Sprint(right)
}

func statsNumber(value any) (float64, bool) {
	switch value := value.(type) {
	case int:
		return float64(value), true
	case int8:
		return float64(value), true
	case int16:
		return float64(value), true
	case int32:
		return float64(value), true
	case int64:
		return float64(value), true
	case uint:
		return float64(value), true
	case uint8:
		return float64(value), true
	case uint16:
		return float64(value), true
	case uint32:
		return float64(value), true
	case uint64:
		return float64(value), true
	case float32:
		return float64(value), true
	case float64:
		return value, true
	default:
		return 0, false
	}
}

// MarkDirtyFrom records the first row index that was modified. If an earlier
// index is already set, it is kept. Use -1 for non-append mutations (UPDATE,
// DELETE) to force a full-table WAL entry.
//
// The -1 (full-table) sentinel is sticky: once a mutation within a transaction
// forces a full-table entry, a later append-only INSERT must not downgrade it
// to a delta, or the earlier UPDATE/DELETE would be lost on WAL recovery.
func (t *Table) MarkDirtyFrom(idx int) {
	if idx < 0 {
		t.dirtyFrom = -1
		return
	}
	if t.dirtyFrom < 0 {
		return // full-table WAL entry already forced; keep the sentinel
	}
	if t.dirtyFrom <= idx {
		return // already tracking earlier rows
	}
	t.dirtyFrom = idx
}

// DirtyFrom returns the first dirty row index, or -1 if non-append-only.
func (t *Table) DirtyFrom() int { return t.dirtyFrom }

// ResetDirty marks the table as clean (called after WAL checkpoint).
func (t *Table) ResetDirty() { t.dirtyFrom = len(t.Rows) }

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

// RecoveryStatus describes the last recovery pass performed while opening a DB.
type RecoveryStatus struct {
	Mode                  StorageMode
	Path                  string
	CheckpointLoaded      bool
	RecoveredTransactions uint64
	RecoveredOperations   int
	Truncated             bool
	RecoveredAt           time.Time
}

// DBHealth is a point-in-time operational snapshot for production probes.
type DBHealth struct {
	OK                bool
	Mode              StorageMode
	ModeName          string
	Path              string
	Closed            bool
	Closing           bool
	ReadOnly          bool
	SchedulerRunning  bool
	WALActive         bool
	AdvancedWALActive bool
	Tenants           int
	Tables            int
	BackendStats      BackendStats
	LastSyncAt        time.Time
	LastCloseAt       time.Time
	Recovery          RecoveryStatus
	Error             string
}

// DB is an in-memory, multi-tenant database catalog with full MVCC support.
// It optionally delegates storage to a StorageBackend for disk-based or
// hybrid persistence strategies.
type DB struct {
	mu      sync.RWMutex
	tenants map[string]*tenantDB
	wal     *WALManager

	// extensions contains the statically linked Go extensions activated for
	// this database instance. It deliberately lives outside the persisted
	// catalog: an extension's executable code must be linked into the current
	// process and explicitly activated again after a restart.
	extensionsMu      sync.RWMutex
	extensions        map[string]ExtensionInfo
	loadingExtensions map[string]struct{}

	// contentMu guards the contents of Table values (Rows, Cols, Version,
	// dirtyFrom) reached through a *Table pointer returned by Get/Put/etc.
	// mu only protects the tenant->table map structure itself; once a
	// caller holds a *Table, nothing previously serialized reads of
	// t.Rows against concurrent INSERT/UPDATE/DELETE appends/mutations of
	// the same slice. The engine's Execute() takes contentMu for read
	// (SELECT/EXPLAIN/PRAGMA) or write (everything else) for the duration
	// of a whole statement, which is coarser than per-table locking but
	// closes the race with a single, easy-to-audit choke point.
	contentMu sync.RWMutex

	// MVCC coordinator
	mvcc *MVCCManager

	// Advanced WAL (optional - replaces basic WAL when enabled)
	advancedWAL *AdvancedWAL

	// Optional tamper-evident audit log; see AttachAuditLog.
	auditLog *AuditLog

	// System catalog for metadata and job scheduling
	catalogMu sync.RWMutex
	catalog   *CatalogManager

	// Optional job scheduler/agent.
	scheduler *Scheduler

	// Pluggable storage backend (nil = pure in-memory, the legacy default).
	backend StorageBackend

	// Active storage mode. ModeMemory when no backend is attached.
	storageMode StorageMode

	// Configuration used to open this database (may be nil).
	config *StorageConfig

	closing      bool
	closed       bool
	lastSyncAt   time.Time
	lastCloseAt  time.Time
	lastRecovery RecoveryStatus
	lastError    string

	// readOnly rejects all mutating statements at the engine level when set.
	// Serving-only deployments (e.g. nightly bulk load, read-only during the
	// day) use this to guarantee cache/index stability: no write can invalidate
	// vector index or column caches, and the WAL is never appended to.
	readOnly atomic.Bool
}

// SetReadOnly toggles read-only mode. While enabled, the SQL engine rejects
// INSERT/UPDATE/DELETE/DDL with an error; SELECT, EXPLAIN, and PRAGMA still
// work. Safe to call concurrently with running queries.
func (db *DB) SetReadOnly(ro bool) {
	if db == nil {
		return
	}
	db.readOnly.Store(ro)
}

// IsReadOnly reports whether the database is in read-only mode.
func (db *DB) IsReadOnly() bool {
	if db == nil {
		return false
	}
	return db.readOnly.Load()
}

// SetRBACEnabled overrides RBAC's default opt-in-via-CreateUser behavior;
// see CatalogManager.SetRBACEnabled for the full explanation. A convenience
// delegate to db.Catalog().SetRBACEnabled, provided directly on DB to
// mirror SetReadOnly above (the same "toggle a behavior" shape).
func (db *DB) SetRBACEnabled(enabled bool) {
	if db == nil {
		return
	}
	db.Catalog().SetRBACEnabled(enabled)
}

// IsRBACEnabled reports whether Execute currently enforces RBAC
// permissions. A convenience delegate to db.Catalog().IsRBACEnabled.
func (db *DB) IsRBACEnabled() bool {
	if db == nil {
		return false
	}
	return db.Catalog().IsRBACEnabled()
}

// LockContentForRead acquires the database's content lock for a read-only
// statement (SELECT/EXPLAIN/PRAGMA). Multiple readers may hold this
// concurrently; it excludes concurrent LockContentForWrite callers. Callers
// must call UnlockContentForRead exactly once, typically via defer, and must
// not call it again re-entrantly on the same goroutine (sync.RWMutex is not
// reentrant) — nested statement execution within the engine must bypass
// this lock rather than re-acquire it.
func (db *DB) LockContentForRead() {
	db.contentMu.RLock()
}

// UnlockContentForRead releases a lock taken by LockContentForRead.
func (db *DB) UnlockContentForRead() {
	db.contentMu.RUnlock()
}

// LockContentForWrite acquires the database's content lock exclusively, for
// any statement that may mutate table rows/columns (INSERT/UPDATE/DELETE and
// all DDL). See LockContentForRead for the re-entrancy caveat.
func (db *DB) LockContentForWrite() {
	db.contentMu.Lock()
}

// UnlockContentForWrite releases a lock taken by LockContentForWrite.
func (db *DB) UnlockContentForWrite() {
	db.contentMu.Unlock()
}

// NewDB creates a new empty database catalog with MVCC support.
// The database operates in ModeMemory (pure in-memory) by default.
// Use OpenDB for disk-backed or hybrid storage modes.
func NewDB() *DB {
	return &DB{
		tenants:           map[string]*tenantDB{},
		mvcc:              NewMVCCManager(),
		storageMode:       ModeMemory,
		extensions:        map[string]ExtensionInfo{},
		loadingExtensions: map[string]struct{}{},
	}
}

// applyEncryptionKey enables AES-256-GCM encryption at rest on backend when
// key is non-empty, validating its length immediately with a clear error —
// rather than letting a wrong-size key surface later as an opaque failure
// the first time SaveTable/LoadTable actually tries to use it.
func applyEncryptionKey(backend *DiskBackend, key []byte) error {
	if len(key) == 0 {
		return nil
	}
	enc, err := NewEncryptor(key)
	if err != nil {
		return fmt.Errorf("storage encryption: %w", err)
	}
	backend.SetEncryptor(enc)
	return nil
}

// OpenDB creates or opens a database with the specified storage configuration.
// For ModeMemory this is equivalent to NewDB (with optional save-on-close).
// For ModeDisk/ModeJSON/ModeHybrid/ModeIndex, tables are stored as individual
// files in the configured directory. For ModeWAL, the existing WAL mechanism is
// configured automatically.
func OpenDB(cfg StorageConfig) (*DB, error) {
	// WAL open/recovery currently opens its log read-write and can truncate a
	// torn tail during recovery. Reject it rather than claiming that a
	// read-only open is safe while creating or modifying a WAL sidecar.
	if cfg.ReadOnly && (cfg.Mode == ModeWAL || cfg.Mode == ModeAdvancedWAL) {
		return nil, fmt.Errorf("read-only open is not supported for %s; use a checkpointed disk, index, hybrid, or json artifact", cfg.Mode)
	}
	// Persistent read-only modes must never turn a typo or missing artifact
	// into a newly created directory. NewDiskBackend normally creates its root
	// for import workflows, so validate before it is constructed.
	if cfg.ReadOnly && (cfg.Mode == ModeDisk || cfg.Mode == ModeJSON || cfg.Mode == ModeIndex || cfg.Mode == ModeHybrid || cfg.Mode == ModePagedIndex) {
		info, err := os.Stat(cfg.Path)
		if err != nil {
			return nil, fmt.Errorf("read-only open requires an existing storage directory %q: %w", cfg.Path, err)
		}
		if !info.IsDir() {
			return nil, fmt.Errorf("read-only open requires a storage directory, got %q", cfg.Path)
		}
	}

	db := &DB{
		tenants:     map[string]*tenantDB{},
		mvcc:        NewMVCCManager(),
		storageMode: cfg.Mode,
		config:      &cfg,
	}

	switch cfg.Mode {
	case ModeMemory:
		mb := NewMemoryBackend(cfg.Path)
		mb.setDB(db)
		db.backend = mb
		// If a path is given, try loading an existing GOB file.
		if cfg.Path != "" {
			if loaded, err := loadGOBInto(db, cfg.Path); err != nil {
				return nil, fmt.Errorf("open memory db: %w", err)
			} else if loaded {
				// Update the back-pointer after loading
				mb.setDB(db)
			}
		}

	case ModeWAL:
		if cfg.Path == "" {
			return nil, fmt.Errorf("ModeWAL requires a Path")
		}
		// Load checkpoint if exists
		checkpointLoaded, err := loadGOBInto(db, cfg.Path)
		if err != nil {
			return nil, fmt.Errorf("open wal db: %w", err)
		}
		// Attach WAL
		walCfg := WALConfig{
			Path:               cfg.Path,
			CheckpointEvery:    cfg.CheckpointEvery,
			CheckpointInterval: cfg.CheckpointInterval,
			CheckpointMaxBytes: cfg.CheckpointMaxBytes,
		}
		wal, err := OpenWAL(db, walCfg)
		if err != nil {
			return nil, fmt.Errorf("open wal: %w", err)
		}
		wal.recovery.CheckpointLoaded = checkpointLoaded
		db.attachWAL(wal)
		db.recordRecovery(wal.recovery)

	case ModeAdvancedWAL:
		if cfg.Path == "" {
			return nil, fmt.Errorf("ModeAdvancedWAL requires a Path")
		}
		checkpointPath := cfg.Path + ".checkpoint"
		checkpointLoaded, err := loadGOBInto(db, checkpointPath)
		if err != nil {
			return nil, fmt.Errorf("open advanced wal checkpoint: %w", err)
		}
		walCfg := AdvancedWALConfig{
			Path:               cfg.Path,
			CheckpointPath:     checkpointPath,
			CheckpointEvery:    cfg.CheckpointEvery,
			CheckpointInterval: cfg.CheckpointInterval,
			CheckpointMaxBytes: cfg.CheckpointMaxBytes,
			Compress:           cfg.CompressFiles,
			BufferSize:         64 * 1024,
		}
		wal, err := OpenAdvancedWAL(walCfg)
		if err != nil {
			return nil, fmt.Errorf("open advanced wal: %w", err)
		}
		// Recover pending WAL operations
		recovered, err := wal.Recover(db)
		if err != nil {
			return nil, fmt.Errorf("recover advanced wal: %w", err)
		}
		db.AttachAdvancedWAL(wal)
		db.recordRecovery(RecoveryStatus{
			Mode:                ModeAdvancedWAL,
			Path:                cfg.Path,
			CheckpointLoaded:    checkpointLoaded,
			RecoveredOperations: recovered,
			RecoveredAt:         time.Now(),
		})

	case ModeDisk:
		if cfg.Path == "" {
			return nil, fmt.Errorf("ModeDisk requires a Path")
		}
		backend, err := NewDiskBackend(cfg.Path, cfg.CompressFiles)
		if err != nil {
			return nil, fmt.Errorf("open disk db: %w", err)
		}
		if err := applyEncryptionKey(backend, cfg.EncryptionKey); err != nil {
			return nil, err
		}
		backend.SetReadOnly(cfg.ReadOnly)
		db.backend = backend

	case ModeJSON:
		if cfg.Path == "" {
			return nil, fmt.Errorf("ModeJSON requires a Path")
		}
		backend, err := NewJSONBackend(cfg.Path, cfg.CompressFiles)
		if err != nil {
			return nil, fmt.Errorf("open json db: %w", err)
		}
		if err := applyEncryptionKey(backend, cfg.EncryptionKey); err != nil {
			return nil, err
		}
		backend.SetReadOnly(cfg.ReadOnly)
		db.backend = backend

	case ModeIndex:
		if cfg.Path == "" {
			return nil, fmt.Errorf("ModeIndex requires a Path")
		}
		mem := cfg.MaxMemoryBytes
		if mem <= 0 {
			mem = 64 * 1024 * 1024 // 64 MB
		}
		backend, err := NewHybridBackend(cfg.Path, mem, cfg.CompressFiles, ModeIndex)
		if err != nil {
			return nil, fmt.Errorf("open index db: %w", err)
		}
		if err := applyEncryptionKey(backend.Disk(), cfg.EncryptionKey); err != nil {
			return nil, err
		}
		backend.SetReadOnly(cfg.ReadOnly)
		db.backend = backend

	case ModeHybrid:
		if cfg.Path == "" {
			return nil, fmt.Errorf("ModeHybrid requires a Path")
		}
		mem := cfg.MaxMemoryBytes
		if mem <= 0 {
			mem = 256 * 1024 * 1024 // 256 MB
		}
		backend, err := NewHybridBackend(cfg.Path, mem, cfg.CompressFiles, ModeHybrid)
		if err != nil {
			return nil, fmt.Errorf("open hybrid db: %w", err)
		}
		if err := applyEncryptionKey(backend.Disk(), cfg.EncryptionKey); err != nil {
			return nil, err
		}
		backend.SetReadOnly(cfg.ReadOnly)
		db.backend = backend

	case ModePagedIndex:
		if cfg.Path == "" {
			return nil, fmt.Errorf("ModePagedIndex requires a Path")
		}
		mem := cfg.MaxMemoryBytes
		if mem <= 0 {
			mem = 64 * 1024 * 1024
		}
		backend, err := NewPagedIndexBackend(cfg.Path, mem, cfg.ReadOnly)
		if err != nil {
			return nil, fmt.Errorf("open paged index db: %w", err)
		}
		db.backend = backend

	default:
		return nil, fmt.Errorf("unsupported storage mode: %v", cfg.Mode)
	}

	if err := db.loadBackendCatalog(); err != nil {
		return nil, err
	}

	if cfg.ReadOnly {
		db.SetReadOnly(true)
	}

	return db, nil
}

// loadGOBInto loads a GOB checkpoint file into an existing DB. It returns
// true if data was actually loaded (file existed and was non-empty).
func loadGOBInto(db *DB, filename string) (bool, error) {
	f, err := os.Open(filename)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, err
	}
	defer func() { _ = f.Close() }()

	var dump []diskTable
	var r io.Reader = bufio.NewReader(f)
	if strings.HasSuffix(strings.ToLower(filename), ".gz") {
		gr, gzErr := gzip.NewReader(r)
		if gzErr != nil {
			return false, gzErr
		}
		defer func() { _ = gr.Close() }()
		r = gr
	}
	dec := gob.NewDecoder(r)
	if err := dec.Decode(&dump); err != nil {
		if errors.Is(err, io.EOF) {
			return false, nil
		}
		return false, err
	}
	for _, dt := range dump {
		_ = db.Put(dt.Tenant, diskToTable(dt))
	}
	loadedCatalog := false
	var dc diskCatalog
	if err := dec.Decode(&dc); err == nil {
		db.setCatalog(diskToCatalog(dc))
		loadedCatalog = true
	} else if !errors.Is(err, io.EOF) {
		return false, err
	}
	return len(dump) > 0 || loadedCatalog, nil
}

// ReadCheckpointWatermark reads the trailing uint64 a checkpoint file may
// carry after its table dump and catalog (see SaveToFile's extra
// parameter) — the LSN/Seq up to which that checkpoint already reflects
// every operation, used by AdvancedWAL/WALManager to skip re-applying
// already-checkpointed WAL records on recovery (see each Checkpoint/Recover
// pair). Returns 0 with no error if the file doesn't exist, is empty, or
// simply predates this watermark (an older checkpoint format) — all of
// which mean "nothing to skip," the safe default.
func ReadCheckpointWatermark(filename string) (uint64, error) {
	f, err := os.Open(filename)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, nil
		}
		return 0, err
	}
	defer func() { _ = f.Close() }()

	var r io.Reader = bufio.NewReader(f)
	if strings.HasSuffix(strings.ToLower(filename), ".gz") {
		gr, gzErr := gzip.NewReader(r)
		if gzErr != nil {
			return 0, gzErr
		}
		defer func() { _ = gr.Close() }()
		r = gr
	}
	dec := gob.NewDecoder(r)
	var dump []diskTable
	if err := dec.Decode(&dump); err != nil {
		return 0, nil // empty or unreadable as a snapshot: nothing to skip
	}
	var dc diskCatalog
	if err := dec.Decode(&dc); err != nil {
		return 0, nil // no catalog section: predates any watermark too
	}
	var watermark uint64
	if err := dec.Decode(&watermark); err != nil {
		return 0, nil // predates the watermark being written: nothing to skip
	}
	return watermark, nil
}

// getTenant returns the tenantDB for the given tenant name, creating it
// if necessary. Callers must hold db.mu (at least read-locked when only
// reading, write-locked when creating/modifying).
func (db *DB) getTenant(tn string) *tenantDB {
	tn = strings.ToLower(tn)
	td := db.tenants[tn]
	if td == nil {
		td = &tenantDB{tables: map[string]*Table{}}
		db.tenants[tn] = td
	}
	return td
}

// getTenantRO returns the tenantDB for reading. Returns nil if it does not
// exist (no allocation). Caller must hold db.mu.RLock().
func (db *DB) getTenantRO(tn string) *tenantDB {
	return db.tenants[strings.ToLower(tn)]
}

// Get returns a table by name for the given tenant.
// When a StorageBackend is attached, tables not found in memory are loaded
// from the backend on demand (lazy loading).
func (db *DB) Get(tn, name string) (*Table, error) {
	t, found := func() (*Table, bool) {
		db.mu.RLock()
		defer db.mu.RUnlock()
		td := db.getTenantRO(tn)
		if td == nil {
			return nil, false
		}
		t, ok := td.tables[strings.ToLower(name)]
		return t, ok
	}()
	if found {
		return t, nil
	}

	// Not in memory – try the backend.
	if db.backend != nil {
		t, err := db.backend.LoadTable(tn, name)
		if err != nil {
			return nil, fmt.Errorf("backend load %s/%s: %w", tn, name, err)
		}
		if t != nil {
			// ModeIndex, ModeHybrid, and ModePagedIndex own loaded tables
			// through their own bounded pool rather than DB.tenants.
			// Retaining another pointer in DB.tenants would turn a cache
			// eviction into a no-op and make memory grow with every table
			// ever queried. This does not put mutations at risk: the
			// returned *Table is the very same pointer the backend's pool
			// holds, so an in-place INSERT/UPDATE/DELETE stays visible to
			// that pool. DB.Sync and DB.Close additionally consult the
			// backend's PooledTables (when it implements pooledTableSource)
			// to find and flush exactly these leases, and the pool itself
			// flushes a dirty table before dropping it under memory
			// pressure (see BufferPool.evictionSaver). So the caller's
			// reference is a query-scoped lease for memory-retention
			// purposes only — it remains valid while that statement holds
			// DB.contentMu and becomes collectible once both caller and
			// pool release it — never for durability.
			if !db.backendTablesEvictable() {
				db.mu.Lock()
				db.getTenant(tn).tables[strings.ToLower(t.Name)] = t
				db.mu.Unlock()
			}
			return t, nil
		}
	}

	return nil, db.noSuchTableError(tn, name)
}

// noSuchTableError builds the "no such table" error for Get, adding a
// "did you mean ...?" hint when an existing table name is a close typo
// match. This is a plain edit-distance heuristic (see suggestSimilar), not
// an AI feature — it only fires on the already-slow not-found path.
func (db *DB) noSuchTableError(tn, name string) error {
	if suggestion := suggestSimilar(name, db.candidateTableNames(tn)); suggestion != "" {
		return fmt.Errorf("no such table %q (tenant %q) - did you mean %q?", name, tn, suggestion)
	}
	return fmt.Errorf("no such table %q (tenant %q)", name, tn)
}

// candidateTableNames lists table names known for the tenant, both resident
// in memory and (if a backend is attached) on disk, for typo suggestions.
func (db *DB) candidateTableNames(tn string) []string {
	db.mu.RLock()
	td := db.getTenantRO(tn)
	var names []string
	if td != nil {
		names = make([]string, 0, len(td.tables))
		for _, t := range td.tables {
			names = append(names, t.Name)
		}
	}
	db.mu.RUnlock()
	if db.backend != nil {
		if diskNames, err := db.backend.ListTableNames(tn); err == nil {
			names = append(names, diskNames...)
		}
	}
	return names
}

// backendTablesEvictable reports modes whose backend, rather than DB.tenants,
// is the owner of lazily loaded tables. Keeping this policy explicit is
// important: schemas and manifest metadata stay resident, row payloads do
// not. Mutable tables created in the current process remain in the catalog
// until they are explicitly Evict'ed or the DB is reopened.
func (db *DB) backendTablesEvictable() bool {
	return db.backend != nil && (db.storageMode == ModeIndex || db.storageMode == ModeHybrid || db.storageMode == ModePagedIndex)
}

// Put adds a new table to the tenant; returns error if it already exists.
// When a StorageBackend is attached, the table is also checked against the
// backend to prevent duplicates, and optionally persisted immediately when
// SyncOnMutate is configured.
func (db *DB) Put(tn string, t *Table) error {
	if db.IsReadOnly() {
		return ErrReadOnlyStorage
	}
	exists := func() bool {
		db.mu.Lock()
		defer db.mu.Unlock()
		td := db.getTenant(tn)
		lc := strings.ToLower(t.Name)
		if _, exists := td.tables[lc]; exists {
			return true
		}
		// Also check the backend for tables that may be on disk but not loaded.
		if db.backend != nil && db.backend.TableExists(tn, t.Name) {
			return true
		}
		td.tables[lc] = t
		return false
	}()
	if exists {
		return fmt.Errorf("table %q already exists (tenant %q)", t.Name, tn)
	}

	// Persist to backend if configured.
	if db.backend != nil {
		if err := db.backend.SaveTable(tn, t); err != nil {
			return fmt.Errorf("backend save %s/%s: %w", tn, t.Name, err)
		}
	}
	return nil
}

// Drop removes a table from the tenant (and from the backend if attached).
func (db *DB) Drop(tn, name string) error {
	if db.IsReadOnly() {
		return ErrReadOnlyStorage
	}
	onDisk, found := func() (bool, bool) {
		db.mu.Lock()
		defer db.mu.Unlock()
		td := db.getTenant(tn)
		lc := strings.ToLower(name)
		_, inMemory := td.tables[lc]
		onDisk := db.backend != nil && db.backend.TableExists(tn, name)
		if !inMemory && !onDisk {
			return false, false
		}
		delete(td.tables, lc)
		return onDisk, true
	}()
	if !found {
		return db.noSuchTableError(tn, name)
	}

	if db.backend != nil && onDisk {
		if err := db.backend.DeleteTable(tn, name); err != nil {
			return fmt.Errorf("backend delete %s/%s: %w", tn, name, err)
		}
	}
	return nil
}

// ListTables returns the tables in a tenant sorted by name.
// When a StorageBackend is attached, tables that exist on disk but are not
// currently loaded into memory are loaded on demand.
func (db *DB) ListTables(tn string) []*Table {
	// In the evictable modes the tenant catalog deliberately does not own
	// backend-loaded row data. ListTables is an explicit all-table operation,
	// so return transient table leases rather than repopulating that catalog.
	if db.backendTablesEvictable() {
		names, err := db.backend.ListTableNames(tn)
		if err != nil {
			return nil
		}
		out := make([]*Table, 0, len(names))
		for _, name := range names {
			if t, err := db.Get(tn, name); err == nil && t != nil {
				out = append(out, t)
			}
		}
		return out
	}

	// If a backend is attached, ensure we know about all tables on disk.
	if db.backend != nil {
		if diskNames, err := db.backend.ListTableNames(tn); err == nil {
			for _, n := range diskNames {
				lc := strings.ToLower(n)
				db.mu.RLock()
				td := db.getTenantRO(tn)
				inMem := td != nil && td.tables[lc] != nil
				db.mu.RUnlock()
				if !inMem {
					// Load from backend
					if t, err := db.backend.LoadTable(tn, n); err == nil && t != nil {
						if !db.backendTablesEvictable() {
							db.mu.Lock()
							db.getTenant(tn).tables[lc] = t
							db.mu.Unlock()
						}
					}
				}
			}
		}
	}

	db.mu.RLock()
	td := db.getTenantRO(tn)
	if td == nil || len(td.tables) == 0 {
		db.mu.RUnlock()
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
	db.mu.RUnlock()
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
			_ = out.Put(tn, cloneTable(t))
		}
	}
	return out
}

// DeepClonePair creates two independent full copies in one traversal. The SQL
// driver uses this for transaction begin: one immutable base snapshot for
// conflict detection and one mutable shadow that receives transaction writes.
func (db *DB) DeepClonePair() (*DB, *DB) {
	base := NewDB()
	shadow := NewDB()
	base.wal = db.wal
	shadow.wal = db.wal
	for tn, tdb := range db.tenants {
		for _, t := range tdb.tables {
			base.upsertTable(tn, cloneTable(t))
			shadow.upsertTable(tn, cloneTable(t))
		}
	}
	return base, shadow
}

// SnapshotForTx creates the pair of snapshots a SQL transaction needs while
// copying row data only once instead of twice.
//
// shadow is a full deep clone that receives the transaction's writes. base is
// a lightweight snapshot that records each table's identity and Version but no
// rows: the only consumers of the base — CollectWALChanges and the driver's
// conflict detection — read Table.Version and table existence exclusively and
// never inspect rows. Copying rows into the base (as DeepClonePair does) would
// therefore waste memory proportional to the entire database on every Begin.
func (db *DB) SnapshotForTx() (base *DB, shadow *DB) {
	base = NewDB()
	shadow = NewDB()
	base.wal = db.wal
	shadow.wal = db.wal
	for tn, tdb := range db.tenants {
		for _, t := range tdb.tables {
			base.upsertTable(tn, cloneTableMeta(t))
			shadow.upsertTable(tn, cloneTable(t))
		}
	}
	return base, shadow
}

// cloneTableMeta copies a table's identity, schema and Version but not its
// rows. It backs the transaction base snapshot from SnapshotForTx, where only
// Version and existence are ever read. Cols are shared by reference: the base
// is never mutated, and any schema change bumps Version, so a stale shared
// header cannot cause a missed conflict.
func cloneTableMeta(t *Table) *Table {
	nt := NewTable(t.Name, t.Cols, t.IsTemp)
	nt.Version = t.Version
	return nt
}

func cloneTable(t *Table) *Table {
	cols := make([]Column, len(t.Cols))
	copy(cols, t.Cols)
	nt := NewTable(t.Name, cols, t.IsTemp)
	nt.Version = t.Version
	nt.Indexes = cloneSecondaryIndexes(t.Indexes)
	nt.Stats = cloneTableStats(t.Stats)
	nt.dirtyFrom = t.dirtyFrom
	nt.Rows = cloneRows(t.Rows)
	return nt
}

// cloneRows copies all row headers into a single backing array. A statement
// snapshot commonly clones tens of thousands of rows; keeping the cells
// contiguous avoids one allocation per row while preserving the original
// per-row append semantics through a full slice expression.
func cloneRows(rows [][]any) [][]any {
	cloned := make([][]any, len(rows))
	maxInt := int(^uint(0) >> 1)
	totalCells := 0
	for _, row := range rows {
		if len(row) > maxInt-totalCells {
			// The contiguous allocation cannot be represented. This is only
			// reachable for an impossibly large in-memory table on supported
			// platforms, but retain the safe per-row behavior rather than
			// overflowing the allocation size.
			return cloneRowsIndividually(rows)
		}
		totalCells += len(row)
	}

	cells := make([]any, totalCells)
	offset := 0
	for i, row := range rows {
		end := offset + len(row)
		// Restrict capacity to the row length. Before this optimization each
		// row was independently allocated with cap == len, so append must not
		// be able to overwrite the next row in the shared backing array.
		copyRow := cells[offset:end:end]
		for j, value := range row {
			copyRow[j] = cloneCell(value)
		}
		cloned[i] = copyRow
		offset = end
	}
	return cloned
}

func cloneRowsIndividually(rows [][]any) [][]any {
	cloned := make([][]any, len(rows))
	for i, row := range rows {
		copyRow := make([]any, len(row))
		for j, value := range row {
			copyRow[j] = cloneCell(value)
		}
		cloned[i] = copyRow
	}
	return cloned
}

// cloneCell preserves snapshot isolation for mutable binary values. Other
// scalar values are immutable/value types at the storage boundary.
func cloneCell(v any) any {
	if b, ok := v.([]byte); ok {
		return append([]byte(nil), b...)
	}
	return v
}

// ShallowCloneForTable creates a lightweight copy of the database that
// deep-copies only the specified table and shares all others by reference.
// This is safe when the caller knows only the target table will be mutated
// (single-statement DML). For a database with many tables, this is
// dramatically cheaper than DeepClone — O(rows in target table) instead of
// O(rows in all tables).
func (db *DB) ShallowCloneForTable(tenant, tableName string) *DB {
	if len(db.tenants) == 0 {
		return NewDB()
	}
	out := NewDB()
	out.wal = db.wal
	targetTenant := strings.ToLower(tenant)
	targetKey := strings.ToLower(tableName)
	for tn, tdb := range db.tenants {
		for _, t := range tdb.tables {
			key := strings.ToLower(t.Name)
			if tn == targetTenant && key == targetKey {
				// Deep-copy the target table that will be mutated.
				out.upsertTable(tn, cloneTable(t))
			} else {
				// Share by reference — these tables are read-only in this operation.
				out.upsertTable(tn, t)
			}
		}
	}
	return out
}

// ------------------------ GOB Checkpoint (Load/Save) ------------------------

type diskColumn struct {
	Name         string
	Type         ColType
	DeclaredType string
	Affinity     SQLiteAffinity
	NotNull      bool
	HasDefault   bool
	DefaultValue any
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
	Indexes map[string]*SecondaryIndex
	Stats   *TableStats
}

type diskCatalog struct {
	Tables       []*CatalogTable
	Columns      map[string][]CatalogColumn
	Views        []*CatalogView
	MViews       []*CatalogMaterializedView
	Dependencies []CatalogDependency
	Indexes      []*CatalogIndex
	Funcs        []*CatalogFunction
	Jobs         []*CatalogJob
	JobRuns      []*CatalogJobHistory
	NextRun      int64
	Triggers     []*CatalogTrigger
}

func catalogToDisk(c *CatalogManager) diskCatalog {
	if c == nil {
		return diskCatalog{Columns: make(map[string][]CatalogColumn), NextRun: 1}
	}
	c.mu.RLock()
	defer c.mu.RUnlock()

	dc := diskCatalog{
		Tables:       make([]*CatalogTable, 0, len(c.tables)),
		Columns:      make(map[string][]CatalogColumn, len(c.columns)),
		Views:        make([]*CatalogView, 0, len(c.views)),
		MViews:       make([]*CatalogMaterializedView, 0, len(c.mviews)),
		Dependencies: make([]CatalogDependency, 0),
		Indexes:      make([]*CatalogIndex, 0, len(c.indexes)),
		Funcs:        make([]*CatalogFunction, 0, len(c.funcs)),
		Jobs:         make([]*CatalogJob, 0, len(c.jobs)),
		JobRuns:      make([]*CatalogJobHistory, 0, len(c.jobRuns)),
		NextRun:      c.nextRun,
		Triggers:     make([]*CatalogTrigger, 0, len(c.triggers)),
	}
	if dc.NextRun == 0 {
		dc.NextRun = 1
	}
	for _, t := range c.tables {
		cp := *t
		dc.Tables = append(dc.Tables, &cp)
	}
	for k, cols := range c.columns {
		cp := make([]CatalogColumn, len(cols))
		copy(cp, cols)
		dc.Columns[k] = cp
	}
	for _, v := range c.views {
		cp := *v
		dc.Views = append(dc.Views, &cp)
	}
	for _, mv := range c.mviews {
		cp := *mv
		dc.MViews = append(dc.MViews, &cp)
	}
	for _, deps := range c.dependencies {
		dc.Dependencies = append(dc.Dependencies, deps...)
	}
	for _, idx := range c.indexes {
		cp := *idx
		cp.Columns = append([]string(nil), idx.Columns...)
		dc.Indexes = append(dc.Indexes, &cp)
	}
	for _, f := range c.funcs {
		cp := *f
		if f.ArgTypes != nil {
			cp.ArgTypes = append([]string(nil), f.ArgTypes...)
		}
		dc.Funcs = append(dc.Funcs, &cp)
	}
	for _, j := range c.jobs {
		cp := *j
		dc.Jobs = append(dc.Jobs, &cp)
	}
	for _, run := range c.jobRuns {
		cp := *run
		dc.JobRuns = append(dc.JobRuns, &cp)
	}
	for _, t := range c.triggers {
		cp := *t
		dc.Triggers = append(dc.Triggers, &cp)
	}
	return dc
}

func diskToCatalog(dc diskCatalog) *CatalogManager {
	c := NewCatalogManager()
	for _, t := range dc.Tables {
		if t == nil {
			continue
		}
		cp := *t
		c.tables[cp.Schema+"."+cp.Name] = &cp
	}
	for k, cols := range dc.Columns {
		cp := make([]CatalogColumn, len(cols))
		copy(cp, cols)
		c.columns[k] = cp
	}
	for _, v := range dc.Views {
		if v == nil {
			continue
		}
		cp := *v
		c.views[cp.Schema+"."+cp.Name] = &cp
	}
	for _, mv := range dc.MViews {
		if mv == nil {
			continue
		}
		cp := *mv
		c.mviews[cp.Schema+"."+cp.Name] = &cp
	}
	for _, dep := range dc.Dependencies {
		key := dep.Schema + "." + dep.ObjectName
		c.dependencies[key] = append(c.dependencies[key], dep)
	}
	for _, idx := range dc.Indexes {
		if idx == nil {
			continue
		}
		cp := *idx
		cp.Columns = append([]string(nil), idx.Columns...)
		if cp.Tenant == "" {
			// Snapshots created before tenant-scoped index metadata cannot
			// identify the owning tenant. Preserve them for administrative
			// inspection, but do not expose them to a tenant by guessing.
			c.indexes[legacyCatalogIndexKey(cp.Schema, cp.Name)] = &cp
			continue
		}
		cp.Tenant = normalizeCatalogTenant(cp.Tenant)
		c.indexes[catalogIndexKey(cp.Tenant, cp.Schema, cp.Name)] = &cp
	}
	for _, f := range dc.Funcs {
		if f == nil {
			continue
		}
		cp := *f
		if f.ArgTypes != nil {
			cp.ArgTypes = append([]string(nil), f.ArgTypes...)
		}
		c.funcs[cp.Schema+"."+cp.Name] = &cp
	}
	for _, j := range dc.Jobs {
		if j == nil {
			continue
		}
		cp := *j
		c.jobs[cp.Name] = &cp
	}
	for _, run := range dc.JobRuns {
		if run == nil {
			continue
		}
		cp := *run
		c.jobRuns = append(c.jobRuns, &cp)
		if cp.RunID >= c.nextRun {
			c.nextRun = cp.RunID + 1
		}
	}
	if dc.NextRun > c.nextRun {
		c.nextRun = dc.NextRun
	}
	if c.nextRun == 0 {
		c.nextRun = 1
	}
	for _, t := range dc.Triggers {
		if t == nil {
			continue
		}
		cp := *t
		c.triggers[cp.Name] = &cp
	}
	return c
}

func (db *DB) backendCatalogPath() (string, bool) {
	if db == nil || db.config == nil || db.config.Path == "" {
		return "", false
	}
	switch db.storageMode {
	case ModeDisk, ModeHybrid, ModeIndex, ModeJSON:
		return filepath.Join(db.config.Path, ".catalog.gob"), true
	default:
		return "", false
	}
}

func (db *DB) loadBackendCatalog() error {
	path, ok := db.backendCatalogPath()
	if !ok {
		return nil
	}
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	defer func() { _ = f.Close() }()

	var dc diskCatalog
	if err := gob.NewDecoder(bufio.NewReader(f)).Decode(&dc); err != nil {
		if errors.Is(err, io.EOF) {
			return nil
		}
		return err
	}
	db.setCatalog(diskToCatalog(dc))
	return nil
}

func (db *DB) saveBackendCatalog() error {
	if db.IsReadOnly() {
		return nil
	}
	path, ok := db.backendCatalogPath()
	if !ok {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	bw := bufio.NewWriter(f)
	encErr := gob.NewEncoder(bw).Encode(catalogToDisk(db.Catalog()))
	flushErr := bw.Flush()
	closeErr := f.Close()
	if encErr != nil {
		return encErr
	}
	if flushErr != nil {
		return flushErr
	}
	return closeErr
}

func tableToDisk(tn string, t *Table) diskTable {
	return tableToDiskRange(tn, t, 0, len(t.Rows))
}

// tableToDiskRange serializes the table schema and rows in [from, to).
// Used by the WAL to write only newly appended rows.
func tableToDiskRange(tn string, t *Table, from, to int) diskTable {
	if from < 0 {
		from = 0
	}
	if to > len(t.Rows) {
		to = len(t.Rows)
	}
	dt := diskTable{
		Tenant:  tn,
		Name:    t.Name,
		IsTemp:  t.IsTemp,
		Version: t.Version,
		Cols:    make([]diskColumn, len(t.Cols)),
		Rows:    make([][]any, to-from),
		Indexes: cloneSecondaryIndexes(t.Indexes),
		Stats:   cloneTableStats(t.Stats),
	}
	for i, c := range t.Cols {
		dt.Cols[i] = diskColumn(c)
	}
	for i := from; i < to; i++ {
		r := t.Rows[i]
		row := make([]any, len(r))
		for j, v := range r {
			if v == nil {
				row[j] = nil
				continue
			}
			if t.Cols[j].Type == JsonType {
				switch vv := v.(type) {
				case string:
					row[j] = vv
				default:
					b, _ := JSONMarshal(v)
					row[j] = string(b)
				}
			} else {
				row[j] = v
			}
		}
		dt.Rows[i-from] = row
	}
	return dt
}

// normalizeVectorValue coerces a decoded vector cell back into []float64.
// GOB round-trips []float64 exactly; JSON round-trips it as []any (each
// element a float64, or a json.Number if a decoder used UseNumber()).
func normalizeVectorValue(v any) any {
	switch vv := v.(type) {
	case []float64:
		return vv
	case []any:
		out := make([]float64, len(vv))
		for i, e := range vv {
			switch n := e.(type) {
			case float64:
				out[i] = n
			case json.Number:
				f, _ := n.Float64()
				out[i] = f
			case int:
				out[i] = float64(n)
			case int64:
				out[i] = float64(n)
			}
		}
		return out
	default:
		return v
	}
}

func diskToTable(dt diskTable) *Table {
	cols := make([]Column, len(dt.Cols))
	for i, c := range dt.Cols {
		cols[i] = Column(c)
	}
	t := NewTable(dt.Name, cols, dt.IsTemp)
	t.Version = dt.Version
	t.Indexes = cloneSecondaryIndexes(dt.Indexes)
	t.Stats = cloneTableStats(dt.Stats)
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
			switch cols[ci].Type {
			case JsonType:
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
			case VectorType:
				// GOB preserves []float64 exactly; JSON-based backends decode
				// a JSON number array into []any (each element boxed as
				// float64). Normalize both to []float64 so vector functions
				// (which type-switch on []float64) work regardless of the
				// backend that produced this row.
				row[ci] = normalizeVectorValue(v)
			default:
				row[ci] = v
			}
		}
		t.Rows[ri] = row
	}
	return t
}

// SaveToFile writes a snapshot of the database to a file. If the filename
// ends with .gz, the snapshot is gzip-compressed to reduce size.
//
// The snapshot is written atomically: data goes to a temporary file in the
// same directory, is fsynced, and is then renamed over the target. A crash
// mid-checkpoint therefore never corrupts or truncates the previous snapshot.
// SaveToFile writes an atomic snapshot of db (every table plus the catalog)
// to filename, then gob-encodes each value in extra, in order, immediately
// after — letting a caller persist small auxiliary state (e.g. a WAL
// checkpoint's last-applied LSN/Seq watermark, see
// AdvancedWAL.Checkpoint/WALManager.Checkpoint) atomically with the
// snapshot itself, via the same temp-file-then-rename step, rather than a
// separate file whose write could complete independently of this one and
// leave the two inconsistent after a crash. Existing callers that pass no
// extra values are unaffected; the file format is unchanged for them.
func SaveToFile(db *DB, filename string, extra ...any) error {
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
	tmp := filename + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	fail := func(err error) error {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}

	bw := bufio.NewWriter(f)
	var w io.Writer = bw
	// Enable gzip compression based on file extension.
	var gz *gzip.Writer
	if strings.HasSuffix(strings.ToLower(filename), ".gz") {
		gz = gzip.NewWriter(w)
		w = gz
	}
	enc := gob.NewEncoder(w)
	if err := enc.Encode(dump); err != nil {
		return fail(err)
	}
	if err := enc.Encode(catalogToDisk(db.Catalog())); err != nil {
		return fail(err)
	}
	for _, v := range extra {
		if err := enc.Encode(v); err != nil {
			return fail(err)
		}
	}
	if gz != nil {
		if err := gz.Close(); err != nil {
			return fail(err)
		}
	}
	if err := bw.Flush(); err != nil {
		return fail(err)
	}
	if err := f.Sync(); err != nil {
		return fail(err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, filename); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	// Make the rename durable across power loss (no-op on Windows).
	return syncDir(filepath.Dir(filename))
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
	defer func() { _ = f.Close() }()
	var dump []diskTable
	var r io.Reader = bufio.NewReader(f)
	if strings.HasSuffix(strings.ToLower(filename), ".gz") {
		gr, gzErr := gzip.NewReader(r)
		if gzErr != nil {
			return nil, gzErr
		}
		defer func() { _ = gr.Close() }()
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
	var dc diskCatalog
	if err := dec.Decode(&dc); err == nil {
		db.setCatalog(diskToCatalog(dc))
	} else if !errors.Is(err, io.EOF) {
		return nil, err
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
	if err := enc.Encode(catalogToDisk(db.Catalog())); err != nil {
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
	var dc diskCatalog
	if err := dec.Decode(&dc); err == nil {
		db.setCatalog(diskToCatalog(dc))
	} else if !errors.Is(err, io.EOF) {
		return nil, err
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
	walRecordAppendRows // delta: only the new rows appended by INSERT
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
	tenant     string
	name       string
	drop       bool
	appendOnly bool
	table      *diskTable
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

// ApplyWALChanges applies a set of table-level changes to the database. It is
// used by the SQL driver to commit a transaction by merging the transaction's
// delta into the latest shared database instead of replacing the whole DB with
// an older snapshot.
func (db *DB) ApplyWALChanges(changes []WALChange) error {
	for _, ch := range changes {
		if ch.Drop {
			db.mu.Lock()
			td := db.getTenant(ch.Tenant)
			delete(td.tables, strings.ToLower(ch.Name))
			db.mu.Unlock()
			if db.backend != nil && db.backend.TableExists(ch.Tenant, ch.Name) {
				if err := db.backend.DeleteTable(ch.Tenant, ch.Name); err != nil {
					return fmt.Errorf("backend delete %s/%s: %w", ch.Tenant, ch.Name, err)
				}
			}
			continue
		}
		if ch.Table == nil {
			continue
		}
		db.mu.Lock()
		db.getTenant(ch.Tenant).tables[strings.ToLower(ch.Name)] = ch.Table
		db.mu.Unlock()
	}
	return nil
}

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

// AttachAuditLog attaches a tamper-evident audit log to the database. Once
// attached, internal/engine.Execute records every statement to it (see
// internal/engine/audit.go). Pass nil to detach (stop logging); Execute
// treats a nil audit log as "logging disabled", matching the opt-in
// pattern used throughout this session's hardening work (RBAC, read-only
// mode, the security warning in cmd/server).
func (db *DB) AttachAuditLog(log *AuditLog) {
	db.mu.Lock()
	db.auditLog = log
	db.mu.Unlock()
}

// AuditLog returns the attached audit log, or nil if none is configured.
func (db *DB) AuditLog() *AuditLog {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.auditLog
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

// ───────────────────────────────── Backend integration ──────────────────────

// Backend returns the attached StorageBackend (may be nil for pure in-memory
// databases created with NewDB).
func (db *DB) Backend() StorageBackend {
	return db.backend
}

// PagedIndexMetadata returns a schema-only table for the immutable
// ModePagedIndex backend. The returned table has column and secondary-index
// metadata but no rows; it lets a planner select an on-disk index before a
// full-table compatibility load is considered.
func (db *DB) PagedIndexMetadata(tenant, table string) (*Table, bool, error) {
	backend, ok := db.backend.(*PagedIndexBackend)
	if !ok {
		return nil, false, nil
	}
	t, err := backend.IndexMetadata(tenant, table)
	if err != nil {
		return nil, false, err
	}
	if t == nil {
		return nil, false, nil
	}
	return t, true, nil
}

// PagedIndexRows performs an exact composite seek in a ModePagedIndex
// artifact. The boolean is false when no physical index with that name exists;
// a true result with an empty row slice is a valid negative lookup.
func (db *DB) PagedIndexRows(tenant, table, indexName string, values []any) ([][]any, bool, error) {
	backend, ok := db.backend.(*PagedIndexBackend)
	if !ok {
		return nil, false, nil
	}
	return backend.LookupIndexRows(tenant, table, indexName, values)
}

// SetBackend attaches a StorageBackend and sets the storage mode. This is
// primarily used internally by OpenDB; calling it on a running database
// should be done with care.
func (db *DB) SetBackend(b StorageBackend) {
	db.mu.Lock()
	db.backend = b
	if b != nil {
		db.storageMode = b.Mode()
	}
	db.mu.Unlock()
}

// StorageMode returns the active storage mode.
func (db *DB) StorageMode() StorageMode {
	return db.storageMode
}

// ListTenants returns the names of all tenants that have at least one table.
func (db *DB) ListTenants() []string {
	db.mu.RLock()
	defer db.mu.RUnlock()
	out := make([]string, 0, len(db.tenants))
	for tn := range db.tenants {
		out = append(out, tn)
	}
	sort.Strings(out)
	return out
}

// Config returns the StorageConfig used to open this database.
// Returns nil for databases created with NewDB().
func (db *DB) Config() *StorageConfig {
	return db.config
}

func (db *DB) recordRecovery(status RecoveryStatus) {
	if status.RecoveredAt.IsZero() {
		status.RecoveredAt = time.Now()
	}
	db.mu.Lock()
	db.lastRecovery = status
	db.mu.Unlock()
}

func (db *DB) markSynced() {
	db.mu.Lock()
	db.lastSyncAt = time.Now()
	db.lastError = ""
	db.mu.Unlock()
}

func (db *DB) markError(err error) error {
	if err == nil {
		return nil
	}
	db.mu.Lock()
	db.lastError = err.Error()
	db.mu.Unlock()
	return err
}

// HealthCheck returns a production-oriented lifecycle and storage snapshot.
func (db *DB) HealthCheck() DBHealth {
	if db == nil {
		return DBHealth{OK: false, Error: "nil DB"}
	}

	db.mu.RLock()
	path := ""
	if db.config != nil {
		path = db.config.Path
	}
	tableCount := 0
	for _, tdb := range db.tenants {
		tableCount += len(tdb.tables)
	}
	health := DBHealth{
		OK:                !db.closed && !db.closing,
		ReadOnly:          db.IsReadOnly(),
		Mode:              db.storageMode,
		ModeName:          db.storageMode.String(),
		Path:              path,
		Closed:            db.closed,
		Closing:           db.closing,
		SchedulerRunning:  db.scheduler != nil,
		WALActive:         db.wal != nil,
		AdvancedWALActive: db.advancedWAL != nil,
		Tenants:           len(db.tenants),
		Tables:            tableCount,
		LastSyncAt:        db.lastSyncAt,
		LastCloseAt:       db.lastCloseAt,
		Recovery:          db.lastRecovery,
		Error:             db.lastError,
	}
	db.mu.RUnlock()

	health.BackendStats = db.BackendStats()
	if health.Error == "" {
		switch {
		case health.Closed:
			health.Error = "database closed"
		case health.Closing:
			health.Error = "database closing"
		}
	}
	return health
}

// Sync flushes all dirty in-memory tables to the storage backend. For
// ModeMemory and ModeWAL this is a no-op (those modes use SaveToFile /
// WAL checkpoints respectively). For ModeDisk, ModeJSON, ModeHybrid, and
// ModeIndex, tables whose version has changed since the last save are
// written to disk.
func (db *DB) Sync() error {
	if db.IsReadOnly() {
		db.markSynced()
		return nil
	}
	if db.backend == nil {
		db.markSynced()
		return nil
	}

	// For disk/hybrid/paged-index backends, save all resident tables that
	// are dirty. "Resident" is two sets: db.tenants (every non-evictable
	// mode, plus any table Put into this process) and — for the evictable
	// modes ModeIndex/ModeHybrid/ModePagedIndex — whatever the backend's own
	// bounded pool currently holds. DB.Get returns a query-scoped lease for
	// the latter without ever registering it in db.tenants (see
	// backendTablesEvictable), so skipping pooledTableSource here would
	// silently drop any mutation made on such a lease: Sync/Close would
	// keep returning nil while the table was never actually re-saved.
	if db.storageMode == ModeDisk || db.storageMode == ModeJSON || db.storageMode == ModeHybrid || db.storageMode == ModeIndex || db.storageMode == ModePagedIndex {
		dc, hasDirtyTracker := db.backend.(dirtyTracker)

		type entry struct {
			tenant string
			table  *Table
		}
		var toSave []entry
		seen := make(map[string]bool) // tenant\x00lower(table name)

		db.mu.RLock()
		for tn, tdb := range db.tenants {
			for _, t := range tdb.tables {
				seen[tn+"\x00"+strings.ToLower(t.Name)] = true
				if hasDirtyTracker && !dc.IsDirty(tn, t.Name, t.Version) {
					continue
				}
				toSave = append(toSave, entry{tn, t})
			}
		}
		db.mu.RUnlock()

		// db.tenants entries take precedence: a table already covered above
		// (e.g. HybridBackend/PagedIndexBackend's SaveTable mirrors a Put
		// into their own pool too) must not be saved twice.
		if ps, ok := db.backend.(pooledTableSource); ok {
			for _, ref := range ps.PooledTables() {
				key := strings.ToLower(ref.Tenant) + "\x00" + strings.ToLower(ref.Table.Name)
				if seen[key] {
					continue
				}
				seen[key] = true
				if hasDirtyTracker && !dc.IsDirty(ref.Tenant, ref.Table.Name, ref.Table.Version) {
					continue
				}
				toSave = append(toSave, entry{ref.Tenant, ref.Table})
			}
		}

		for _, e := range toSave {
			if err := db.backend.SaveTable(e.tenant, e.table); err != nil {
				return db.markError(err)
			}
		}
	}

	if err := db.backend.Sync(); err != nil {
		return db.markError(err)
	}
	if err := db.saveBackendCatalog(); err != nil {
		return db.markError(err)
	}
	db.markSynced()
	return nil
}

// Close persists all data and releases resources. For ModeMemory with a
// configured path, this saves a final GOB snapshot. For ModeDisk/ModeJSON/
// ModeHybrid, dirty tables are flushed. WAL and Advanced WAL resources are closed.
func (db *DB) Close() error {
	if db == nil {
		return nil
	}
	shouldClose := func() bool {
		db.mu.Lock()
		defer db.mu.Unlock()
		if db.closed || db.closing {
			return false
		}
		db.closing = true
		return true
	}()
	if !shouldClose {
		return nil
	}

	var firstErr error

	db.StopJobScheduler()

	// Sync dirty tables to backend.
	if err := db.Sync(); err != nil && firstErr == nil {
		firstErr = err
	}

	// Close backend (may do its own final save).
	if db.backend != nil {
		if err := db.backend.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	// Close WAL resources.
	if db.wal != nil {
		if err := db.wal.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if db.advancedWAL != nil {
		if err := db.advancedWAL.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	db.mu.Lock()
	db.closing = false
	db.lastCloseAt = time.Now()
	if firstErr == nil {
		db.closed = true
		db.lastError = ""
	} else {
		db.lastError = firstErr.Error()
	}
	db.mu.Unlock()

	return firstErr
}

// Evict removes a table from the in-memory cache without deleting it from
// the backend. This is only meaningful for disk-backed modes; in ModeMemory
// the data would be lost. Returns an error if no backend is attached.
func (db *DB) Evict(tenant, name string) error {
	if db.IsReadOnly() {
		// Eviction must not try to "save before evicting" an immutable artifact.
		// It is already durable; no catalog table is retained on lazy reads in
		// ModeIndex/ModeHybrid, so there is nothing to flush.
		return nil
	}
	if db.backend == nil || db.storageMode == ModeMemory {
		return fmt.Errorf("evict requires a disk-backed storage mode")
	}

	// Ensure the table is saved before evicting.
	db.mu.RLock()
	td := db.getTenantRO(tenant)
	var t *Table
	if td != nil {
		t = td.tables[strings.ToLower(name)]
	}
	db.mu.RUnlock()

	if t != nil {
		if err := db.backend.SaveTable(tenant, t); err != nil {
			return fmt.Errorf("evict save %s/%s: %w", tenant, name, err)
		}
		db.mu.Lock()
		delete(db.getTenant(tenant).tables, strings.ToLower(name))
		db.mu.Unlock()
	}
	return nil
}

// TableExists reports whether the named table exists, checking both in-memory
// tables and the storage backend.
func (db *DB) TableExists(tenant, name string) bool {
	db.mu.RLock()
	td := db.getTenantRO(tenant)
	if td != nil {
		if _, ok := td.tables[strings.ToLower(name)]; ok {
			db.mu.RUnlock()
			return true
		}
	}
	db.mu.RUnlock()

	if db.backend != nil {
		return db.backend.TableExists(tenant, name)
	}
	return false
}

// SyncTable flushes a single table to the backend. This is called by the
// engine after mutations when SyncOnMutate is enabled.
func (db *DB) SyncTable(tenant string, t *Table) error {
	if db.IsReadOnly() {
		return ErrReadOnlyStorage
	}
	if db.backend == nil {
		return nil
	}
	return db.backend.SaveTable(tenant, t)
}

// BackendStats returns statistics from the storage backend. Returns a
// zero-value BackendStats if no backend is attached.
func (db *DB) BackendStats() BackendStats {
	if db.backend == nil {
		return BackendStats{Mode: ModeMemory}
	}
	return db.backend.Stats()
}

// MigrateToBackend copies all in-memory tables to the given backend and
// attaches it. This enables migrating a ModeMemory database to ModeDisk
// or ModeHybrid at runtime.
func (db *DB) MigrateToBackend(b StorageBackend) error {
	if db.IsReadOnly() {
		return ErrReadOnlyStorage
	}
	db.mu.RLock()
	type entry struct {
		tenant string
		table  *Table
	}
	var tables []entry
	for tn, tdb := range db.tenants {
		for _, t := range tdb.tables {
			tables = append(tables, entry{tn, t})
		}
	}
	db.mu.RUnlock()

	for _, e := range tables {
		if err := b.SaveTable(e.tenant, e.table); err != nil {
			return fmt.Errorf("migrate %s/%s: %w", e.tenant, e.table.Name, err)
		}
	}

	db.mu.Lock()
	db.backend = b
	db.storageMode = b.Mode()
	db.mu.Unlock()

	return nil
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
	nextSeq, nextTxID, committed, truncated, err := replayWAL(db, walPath)
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
	size, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	cw := &countingWriter{w: f, n: size}
	writer := bufio.NewWriter(cw)
	wm := &WALManager{
		path:               walPath,
		checkpointPath:     cfg.Path,
		checkpointEvery:    cfg.CheckpointEvery,
		checkpointInterval: cfg.CheckpointInterval,
		checkpointMaxBytes: normalizeCheckpointMaxBytes(cfg.CheckpointMaxBytes),
		file:               f,
		bytes:              cw,
		writer:             writer,
		nextSeq:            nextSeq,
		nextTxID:           nextTxID,
		txSinceCheckpoint:  committed,
		lastCheckpoint:     time.Now(),
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
			if dirty >= 0 && dirty < len(ch.Table.Rows) {
				// Append-only change: write only the new rows.
				dt := tableToDiskRange(ch.Tenant, ch.Table, dirty, len(ch.Table.Rows))
				rec.Type = walRecordAppendRows
				rec.Table = &dt
			} else {
				// Full table change (UPDATE, DELETE, CREATE, or unknown).
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
		_ = f.Close()
		return err
	}
	w.file = f
	w.bytes = &countingWriter{w: f}
	w.writer = bufio.NewWriter(w.bytes)
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

func replayWAL(db *DB, walPath string) (nextSeq, nextTxID, committed uint64, truncated bool, err error) {
	f, err := os.Open(walPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 1, 1, 0, false, nil
		}
		return 0, 0, 0, false, err
	}
	defer func() { _ = f.Close() }()
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
			if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.ErrNoProgress) {
				if lastGood >= 0 {
					_ = os.Truncate(walPath, lastGood)
				}
				return lastSeq + 1, lastTx + 1, committed, true, nil
			}
			return 0, 0, 0, false, err
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
	return lastSeq + 1, lastTx + 1, committed, false, nil
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
