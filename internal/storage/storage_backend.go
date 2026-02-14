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
	"fmt"
	"time"
)

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
	default:
		return fmt.Sprintf("StorageMode(%d)", int(m))
	}
}

// ParseStorageMode converts a string representation back to a StorageMode.
// It is case-insensitive and returns an error for unknown values.
func ParseStorageMode(s string) (StorageMode, error) {
	switch s {
	case "memory", "mem", "ram", "":
		return ModeMemory, nil
	case "wal":
		return ModeWAL, nil
	case "disk":
		return ModeDisk, nil
	case "index":
		return ModeIndex, nil
	case "hybrid":
		return ModeHybrid, nil
	default:
		return ModeMemory, fmt.Errorf("unknown storage mode %q (valid: memory, wal, disk, index, hybrid)", s)
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

	// CompressFiles enables gzip compression for table files on disk.
	CompressFiles bool

	// CheckpointEvery controls how many committed WAL transactions trigger
	// an automatic checkpoint (ModeWAL only). Zero means default (32).
	CheckpointEvery uint64

	// CheckpointInterval controls the maximum time between checkpoints
	// (ModeWAL only). Zero means default (30 s).
	CheckpointInterval time.Duration
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
	case ModeWAL:
		cfg.CheckpointEvery = 32
		cfg.CheckpointInterval = 30 * time.Second
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
