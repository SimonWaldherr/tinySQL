// Package storage – StorageBackend interface and StorageMode definitions.
//
// What: Pluggable storage backends that decouple data management from the
// in-memory catalog. Each backend decides where table data lives
// (RAM, disk, or a combination) and how it is persisted.
// How: The DB struct optionally delegates Get/Put/Drop operations to an
// attached StorageBackend. Backends may lazily load tables, cache hot
// data, and flush dirty tables on Sync/Close.
// Why: Supporting multiple storage modes turns tinySQL into a realistic
// alternative to SQLite – from purely in-memory analytics right through
// to disk-resident databases that exceed available RAM.
package storage

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// ErrReadOnlyStorage is returned by a persistence backend when a caller tries
// to modify an artifact opened for serving. It is deliberately distinct from
// an OS permission error so callers can surface an actionable configuration
// error before any file is touched.
var ErrReadOnlyStorage = errors.New("storage is read-only")

// ───────────────────────────────────────────────────────────────────────────
// Storage mode enumeration
// ───────────────────────────────────────────────────────────────────────────

// StorageMode defines how the database manages data between memory and disk.
type StorageMode int

const (
	// ModeMemory keeps all data in RAM. Persistence only occurs via explicit
	// SaveToFile calls or when DB.Close is invoked. Fastest mode.
	ModeMemory StorageMode = iota

	// ModeWAL keeps all data in RAM and writes a Write-Ahead Log for crash
	// recovery. Periodic checkpoints create full GOB snapshots. Good
	// balance of speed and durability.
	ModeWAL

	// ModeDisk stores each table as a separate GOB file on disk. Tables
	// are loaded into memory on demand and flushed back on Sync/Close.
	// Minimises RAM usage at the cost of disk I/O.
	ModeDisk

	// ModeIndex keeps table schemas (columns, row-counts) permanently in
	// RAM while row data resides on disk. Rows are loaded on demand and
	// evicted aggressively. Memory usage is proportional to schema size,
	// not data size.
	ModeIndex

	// ModeHybrid uses an LRU buffer pool with a configurable memory limit.
	// Hot tables stay in RAM; cold tables spill to disk. Best for mixed
	// workloads where the working set fits in a bounded amount of memory.
	ModeHybrid

	// ModeAdvancedWAL keeps all data in RAM and uses a row-level Write-Ahead Log
	// for full ACID transaction durability, crash recovery, and point-in-time recovery.
	ModeAdvancedWAL

	// ModeJSON stores each table as a separate human-readable JSON file on
	// disk (same lazy-load/dirty-tracking behaviour as ModeDisk, which uses
	// GOB instead). Larger on disk than GOB and big.Rat/uuid.UUID values
	// round-trip as plain strings, but files can be read, diffed, or
	// hand-edited with any text tool.
	ModeJSON

	// ModePagedIndex is an immutable-page-oriented artifact for read-mostly
	// lookups. It stores rows and materialized secondary indexes in separate
	// B+Trees, so an exact index seek can load only index and referenced row
	// pages instead of decoding a complete table file.
	ModePagedIndex

	// ModeSQLite stores every table as a native table in a real SQLite
	// database file (via the pure-Go modernc.org/sqlite driver), so the
	// resulting file is directly readable by any SQLite tool — sqlite3,
	// DB Browser for SQLite, etc. Requires the sqliteimport build tag; see
	// backend_sqlite_unsupported.go for the error returned without it.
	ModeSQLite
)

// String returns a human-readable label for the StorageMode.
func (m StorageMode) String() string {
	switch m {
	case ModeMemory:
		return "memory"
	case ModeWAL:
		return "wal"
	case ModeDisk:
		return "disk"
	case ModeIndex:
		return "index"
	case ModeHybrid:
		return "hybrid"
	case ModeAdvancedWAL:
		return "advanced_wal"
	case ModeJSON:
		return "json"
	case ModePagedIndex:
		return "paged_index"
	case ModeSQLite:
		return "sqlite"
	default:
		return fmt.Sprintf("StorageMode(%d)", int(m))
	}
}

// WALSyncMode controls the durability barrier used for each committed WAL
// transaction. Its zero value deliberately preserves the historical strongest
// available behavior.
type WALSyncMode uint8

const (
	// WALSyncFull asks the operating system for its strongest available file
	// flush. On macOS this is F_FULLFSYNC; on other platforms it is File.Sync.
	// It is the default because it preserves the durability behavior WAL modes
	// exposed before sync policy became configurable.
	WALSyncFull WALSyncMode = iota

	// WALSyncNormal uses a regular fsync. On macOS this matches SQLite's
	// synchronous=FULL behavior unless SQLite's separate fullfsync pragma is
	// enabled. A committed transaction is still flushed before it is
	// acknowledged, but sudden-power-loss guarantees depend on the filesystem
	// and hardware write cache.
	WALSyncNormal
)

// String returns the canonical configuration spelling for a WAL sync mode.
func (m WALSyncMode) String() string {
	switch m {
	case WALSyncFull:
		return "full"
	case WALSyncNormal:
		return "normal"
	default:
		return fmt.Sprintf("WALSyncMode(%d)", m)
	}
}

// ParseWALSyncMode parses a WAL sync policy. Keep this separate from storage
// modes: both affect durability, but one chooses a backend and the other
// chooses its per-commit flush strength.
func ParseWALSyncMode(s string) (WALSyncMode, error) {
	raw := s
	s = strings.TrimSpace(s)
	switch {
	case strings.EqualFold(s, "full"):
		return WALSyncFull, nil
	case strings.EqualFold(s, "normal"):
		return WALSyncNormal, nil
	default:
		return WALSyncFull, fmt.Errorf("unknown WAL sync mode %q (valid: full, normal)", raw)
	}
}

// ParseStorageMode converts a string representation back to a StorageMode.
// It is case-insensitive and returns an error for unknown values.
func ParseStorageMode(s string) (StorageMode, error) {
	raw := s
	s = strings.TrimSpace(s)
	switch {
	// "memory" is the one value that means something in both dialects: it is a
	// real tinySQL storage mode AND a SQLite access mode. It keeps resolving to
	// ModeMemory, which is also what a SQLite user writing mode=memory wants,
	// so it must stay out of the access-mode branch below.
	case s == "", strings.EqualFold(s, "memory"), strings.EqualFold(s, "mem"), strings.EqualFold(s, "ram"):
		return ModeMemory, nil
	case strings.EqualFold(s, "wal"):
		return ModeWAL, nil
	case strings.EqualFold(s, "disk"):
		return ModeDisk, nil
	case strings.EqualFold(s, "index"):
		return ModeIndex, nil
	case strings.EqualFold(s, "hybrid"):
		return ModeHybrid, nil
	case strings.EqualFold(s, "advanced_wal"), strings.EqualFold(s, "advancedwal"):
		return ModeAdvancedWAL, nil
	case strings.EqualFold(s, "json"):
		return ModeJSON, nil
	case strings.EqualFold(s, "paged_index"), strings.EqualFold(s, "pagedindex"), strings.EqualFold(s, "page_index"):
		return ModePagedIndex, nil
	case strings.EqualFold(s, "sqlite"):
		return ModeSQLite, nil
	// SQLite's mode= URI parameter selects an ACCESS mode; tinySQL's mode=
	// selects a storage BACKEND. Same key, entirely different universe, so a
	// migrant's mode=ro used to be answered with the true but useless "unknown
	// storage mode". Name the collision instead.
	//
	// These are deliberately errors and never translated. mode=rwc means
	// "open read/write, create if missing" — that is not a backend choice at
	// all, and guessing one would silently pick a durability policy (and a file
	// layout) the caller never asked for. mode=ro is likewise a request the
	// caller must restate as read_only=1 plus an explicit backend, because
	// "read-only" says nothing about *what* is being read.
	case strings.EqualFold(s, "ro"), strings.EqualFold(s, "rw"), strings.EqualFold(s, "rwc"):
		return ModeMemory, fmt.Errorf("mode=%s is a SQLite access mode; in tinySQL mode= selects a storage backend (memory, wal, disk, index, hybrid, advanced_wal, json, paged_index, sqlite). Request read-only access with read_only=1 and name a persistent backend as well, e.g. mode=disk&read_only=1", strings.ToLower(s))
	default:
		return ModeMemory, fmt.Errorf("unknown storage mode %q (valid: memory, wal, disk, index, hybrid, advanced_wal, json, paged_index, sqlite)", raw)
	}
}

// ───────────────────────────────────────────────────────────────────────────
// Configuration
// ───────────────────────────────────────────────────────────────────────────

// StorageConfig configures database storage behaviour.
type StorageConfig struct {
	// Mode selects the storage strategy. Defaults to ModeMemory.
	Mode StorageMode

	// Path is the root directory (or file path) for persistent storage.
	// Required for all modes except ModeMemory.
	Path string

	// MaxMemoryBytes limits in-memory table data. Used by ModeHybrid and
	// ModeIndex. Zero means use a sensible default (256 MB).
	MaxMemoryBytes int64

	// SyncOnMutate forces a disk write after every INSERT / UPDATE / DELETE.
	// Slower but provides immediate durability for ModeDisk / ModeHybrid.
	SyncOnMutate bool

	// CompressFiles enables gzip compression for table files on disk
	// (ModeDisk, ModeJSON, ModeHybrid, ModeIndex). For ModeAdvancedWAL it
	// instead compresses the periodic checkpoint snapshot only — the live
	// WAL log itself is never compressed, since it is a continuously
	// appended, crash-recoverable stream rather than a point-in-time
	// artifact (see OpenDB's ModeAdvancedWAL case).
	CompressFiles bool

	// CheckpointEvery controls how many WAL work units trigger an automatic
	// checkpoint: committed transactions in ModeWAL and row-operation records
	// in ModeAdvancedWAL. Zero means the mode default (1000).
	CheckpointEvery uint64

	// CheckpointInterval controls the maximum time between checkpoints. Zero
	// means the mode default (30 s for ModeWAL, 5 min for ModeAdvancedWAL).
	CheckpointInterval time.Duration

	// CheckpointMaxBytes forces a checkpoint once the WAL file exceeds this
	// size, bounding WAL growth independently of transaction count and time
	// (ModeWAL / ModeAdvancedWAL). Zero means default (64 MB); negative
	// disables the size trigger.
	CheckpointMaxBytes int64

	// WALSync controls the durability barrier for ModeWAL and ModeAdvancedWAL
	// commits. The default WALSyncFull retains the historic strongest-flush
	// behavior. WALSyncNormal uses a regular fsync, matching SQLite
	// synchronous=FULL on macOS unless SQLite's separate fullfsync pragma is
	// enabled.
	WALSync WALSyncMode

	// ReadOnly opens the database in read-only mode: the SQL engine rejects
	// all mutating statements. Ideal for serve-only phases (e.g. load at
	// night, query during the day) — vector index and column caches can never
	// be invalidated and the WAL is never appended to.
	ReadOnly bool

	// EncryptionKey, if set, must be exactly EncryptionKeySize (32) bytes
	// and enables AES-256-GCM encryption at rest for table files (ModeDisk,
	// ModeJSON, ModeHybrid, ModeIndex). Derive it from a passphrase with
	// DeriveKeyFromPassphrase (persist the salt yourself — this package
	// doesn't manage key storage) or supply 32 cryptographically random
	// bytes directly for a key-file-based setup. Leave nil for the default,
	// unencrypted behavior. Not yet supported for ModeWAL/ModeAdvancedWAL —
	// see README's Limitations section.
	EncryptionKey []byte
}

// DefaultStorageConfig returns a StorageConfig with sensible defaults for
// the given mode. Path must be supplied by the caller afterwards.
func DefaultStorageConfig(mode StorageMode) StorageConfig {
	cfg := StorageConfig{Mode: mode}
	switch mode {
	case ModeHybrid:
		cfg.MaxMemoryBytes = 256 * 1024 * 1024 // 256 MB
	case ModeIndex:
		cfg.MaxMemoryBytes = 64 * 1024 * 1024 // 64 MB (schemas are small)
	case ModePagedIndex:
		cfg.MaxMemoryBytes = 64 * 1024 * 1024
	case ModeWAL:
		// Keep checkpoints infrequent enough that a small write burst does not
		// repeatedly serialize the whole in-memory database. WAL growth remains
		// bounded by the 30-second and 64 MiB defaults.
		cfg.CheckpointEvery = 1000
		cfg.CheckpointInterval = 30 * time.Second
	case ModeAdvancedWAL:
		cfg.CheckpointEvery = 1000
		cfg.CheckpointInterval = 5 * time.Minute
	}
	return cfg
}

// ───────────────────────────────────────────────────────────────────────────
// Backend interface
// ───────────────────────────────────────────────────────────────────────────

// StorageBackend abstracts the underlying table storage mechanism.
//
// Implementations are responsible for loading tables into *Table structs that
// the engine can mutate directly. After mutations, Sync writes dirty tables
// back to durable storage. Close persists pending data and releases resources.
type StorageBackend interface {
	// LoadTable retrieves a table from the backing store. It returns nil, nil
	// when the table does not exist (not an error – the table may simply not
	// have been created yet).
	LoadTable(tenant, name string) (*Table, error)

	// SaveTable persists a single table to the backing store.
	SaveTable(tenant string, t *Table) error

	// DeleteTable removes a table from the backing store.
	DeleteTable(tenant, name string) error

	// ListTableNames returns all table names for a tenant that exist in the
	// backing store (including tables not currently loaded in memory).
	ListTableNames(tenant string) ([]string, error)

	// TableExists reports whether the named table exists in the backing store
	// without loading it into memory.
	TableExists(tenant, name string) bool

	// Sync flushes any pending / dirty data to durable storage.
	Sync() error

	// Close releases all resources. Implementations should call Sync
	// internally if there is unsaved data.
	Close() error

	// Mode returns the StorageMode this backend implements.
	Mode() StorageMode

	// Stats returns operational statistics about the backend.
	Stats() BackendStats
}

// PooledTableRef identifies one table object currently resident in a storage
// backend's own bounded cache — outside DB.tenants — for an evictable
// storage mode (ModeIndex, ModeHybrid, ModePagedIndex; see
// DB.backendTablesEvictable). DB.Get returns exactly this *Table pointer as
// a query-scoped lease without registering it in DB.tenants, so a mutation
// applied in place (as the SQL engine's INSERT/UPDATE/DELETE do) is only
// reachable through the backend's own pool. DB.Sync and DB.Close use
// pooledTableSource.PooledTables to find and flush these leases; without it
// a table that was never re-registered in DB.tenants could be mutated,
// "successfully" synced/closed, and then lose the mutation on next open.
type PooledTableRef struct {
	Tenant string
	Table  *Table
}

// pooledTableSource is implemented by backends that own such a bounded pool
// of loaded tables (HybridBackend for ModeHybrid/ModeIndex, PagedIndexBackend
// for ModePagedIndex).
type pooledTableSource interface {
	PooledTables() []PooledTableRef
}

// dirtyTracker is implemented by backends that can report whether a table's
// current in-memory version has diverged from the version last durably
// saved. DB.Sync uses it — for both DB.tenants entries and pooledTableSource
// leases — to skip re-saving a table that has not actually changed.
type dirtyTracker interface {
	IsDirty(tenant, name string, currentVersion int) bool
}

// BackendStats provides observability into storage backend behaviour.
type BackendStats struct {
	Mode             StorageMode
	TablesInMemory   int
	TablesOnDisk     int
	MemoryUsedBytes  int64
	MemoryLimitBytes int64
	DiskUsedBytes    int64
	CacheHitRate     float64
	SyncCount        int64
	LoadCount        int64
	EvictionCount    int64
	// AdmissionRejects and LargestRejectedBytes are populated by the
	// table-cache backends (ModeHybrid / ModeIndex). A nonzero
	// AdmissionRejects means at least one table does not fit
	// MemoryLimitBytes even after eviction and is therefore decoded from
	// disk on every access; raising the budget past LargestRejectedBytes is
	// what makes it cacheable. They stay zero for backends without a table
	// cache.
	AdmissionRejects     int64
	LargestRejectedBytes int64
	// PageReads and cache counters are populated by page-oriented backends.
	// They are zero for legacy table-file backends which do not expose pages.
	PageReads   int64
	CacheHits   int64
	CacheMisses int64
	CachedPages int
	PinnedPages int
	// Transient pages are read-only query scratch that could not be admitted
	// while all cache frames were pinned. They are never retained by the LRU.
	TransientPages  int
	TransientFrames int
	MaxCachePages   int
}

// ───────────────────────────────────────────────────────────────────────────
// Table metadata (used by disk-backed modes to avoid loading full tables)
// ───────────────────────────────────────────────────────────────────────────

// TableMeta stores lightweight metadata for a table that is (potentially)
// on disk. The disk backend uses this to answer ListTableNames, TableExists,
// and schema-level queries without loading row data.
type TableMeta struct {
	Tenant   string   `json:"tenant"`
	Name     string   `json:"name"`
	Cols     []Column `json:"cols"`
	RowCount int      `json:"row_count"`
	Version  int      `json:"version"`
	// DiskSize is the file size in bytes on the backing store.
	DiskSize int64 `json:"disk_size"`
	// FilePath is the relative path inside the database directory.
	FilePath string `json:"file_path"`
}
