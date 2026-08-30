package driver

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// OpenConfig describes connection, DSN and database/sql settings for OpenWithConfig.
//
// Best-practice split:
//   - DSN fields configure tinySQL driver/server behavior (tenant, autosave, pools, busy_timeout).
//   - database/sql fields configure connection pool behavior (max open/idle/lifetimes).
//   - PingTimeout validates startup connectivity with PingContext.
type OpenConfig struct {
	// Mode controls DSN scheme. Allowed values: "mem" (default) or "file".
	Mode string
	// FilePath is required when Mode is "file".
	FilePath string
	// Tenant maps to DSN option `tenant`. Empty falls back to "default".
	Tenant string
	// Autosave maps to DSN option `autosave=1` for file mode.
	Autosave bool
	// PoolReaders maps to DSN option `pool_readers`.
	PoolReaders int
	// PoolWriters maps to DSN option `pool_writers`.
	PoolWriters int
	// BusyTimeout maps to DSN option `busy_timeout` (e.g. 250ms, 2s).
	BusyTimeout time.Duration
	// StorageMode selects the persistent backend: memory, wal, disk, index,
	// hybrid, advanced_wal, json, paged_index, or sqlite. Empty preserves the
	// legacy file/memory behavior.
	StorageMode string
	// MaxMemoryBytes bounds hybrid/index caches. Zero uses the backend default.
	MaxMemoryBytes int64
	// ReadOnly opens an existing immutable artifact without creating files.
	ReadOnly bool
	// SyncOnMutate flushes disk-backed table mutations immediately.
	SyncOnMutate bool
	// CompressFiles compresses supported table/checkpoint files.
	CompressFiles bool
	// CheckpointEvery and CheckpointInterval control WAL checkpoint cadence.
	CheckpointEvery    uint64
	CheckpointInterval time.Duration
	// CheckpointMaxBytes triggers a checkpoint by WAL size. Zero uses the
	// backend default; -1 disables the size trigger.
	CheckpointMaxBytes int64
	// WALSync is "full" (strongest/default) or "normal" (lower latency).
	WALSync string
	// PersistDebounce coalesces legacy file autosaves. It trades a bounded
	// durability window for higher ingestion throughput.
	PersistDebounce time.Duration

	// database/sql pool settings.
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
	ConnMaxIdleTime time.Duration

	// PingTimeout is used for startup health-check in OpenWithConfig.
	PingTimeout time.Duration
}

// DefaultOpenConfig returns sensible defaults for tinySQL.
func DefaultOpenConfig() OpenConfig {
	return OpenConfig{
		Mode:            "mem",
		Tenant:          "default",
		PoolReaders:     4,
		PoolWriters:     1,
		BusyTimeout:     250 * time.Millisecond,
		MaxOpenConns:    8,
		MaxIdleConns:    4,
		ConnMaxLifetime: 30 * time.Minute,
		ConnMaxIdleTime: 5 * time.Minute,
		PingTimeout:     5 * time.Second,
	}
}

// OfflineNavigationOpenConfig returns a read-only, bounded-memory profile for
// a prebuilt routing/map artifact. ROUTE_WARM should be called at application
// startup to move graph construction before the first navigation request.
func OfflineNavigationOpenConfig(path string) OpenConfig {
	cfg := DefaultOpenConfig()
	cfg.Mode = "file"
	cfg.FilePath = path
	cfg.StorageMode = "index"
	cfg.MaxMemoryBytes = 256 << 20
	cfg.ReadOnly = true
	cfg.PoolReaders = 4
	cfg.MaxOpenConns = 4
	cfg.MaxIdleConns = 4
	cfg.ConnMaxLifetime = 0
	cfg.ConnMaxIdleTime = 0
	return cfg
}

// RAGOpenConfig returns a bounded hybrid-storage profile for vector, FTS and
// hybrid retrieval. RAG_WARM can populate both retrieval structures after
// opening or after a corpus update.
func RAGOpenConfig(path string) OpenConfig {
	cfg := DefaultOpenConfig()
	cfg.Mode = "file"
	cfg.FilePath = path
	cfg.StorageMode = "hybrid"
	cfg.MaxMemoryBytes = 512 << 20
	cfg.SyncOnMutate = true
	cfg.CompressFiles = true
	cfg.PoolReaders = 8
	cfg.BusyTimeout = 2 * time.Second
	cfg.MaxOpenConns = 9
	cfg.MaxIdleConns = 9
	cfg.ConnMaxLifetime = 0
	cfg.ConnMaxIdleTime = 0
	return cfg
}

// EmbeddedToolOpenConfig returns an ACID WAL profile for replacing a remote
// SQL dependency in a single-process Go tool. One writer matches tinySQL's
// serialized mutation path; several readers preserve concurrent queries.
func EmbeddedToolOpenConfig(path string) OpenConfig {
	cfg := DefaultOpenConfig()
	cfg.Mode = "file"
	cfg.FilePath = path
	cfg.StorageMode = "advanced_wal"
	cfg.WALSync = "normal"
	cfg.CheckpointEvery = 1000
	cfg.CheckpointInterval = 5 * time.Minute
	cfg.CheckpointMaxBytes = 64 << 20
	cfg.PoolReaders = 4
	cfg.PoolWriters = 1
	cfg.BusyTimeout = 5 * time.Second
	cfg.MaxOpenConns = 8
	cfg.MaxIdleConns = 8
	cfg.ConnMaxLifetime = 0
	cfg.ConnMaxIdleTime = 0
	return cfg
}

// DSN builds a tinySQL DSN from OpenConfig.
func (c OpenConfig) DSN() (string, error) {
	mode := strings.ToLower(strings.TrimSpace(c.Mode))
	if mode == "" {
		mode = "mem"
	}
	if err := validateOpenConfig(c, mode); err != nil {
		return "", err
	}

	tenant := strings.TrimSpace(c.Tenant)
	if tenant == "" {
		tenant = "default"
	}

	q := url.Values{}
	q.Set("tenant", tenant)
	if c.Autosave && mode == "file" {
		q.Set("autosave", "1")
	}
	if c.PoolReaders > 0 {
		q.Set("pool_readers", strconv.Itoa(c.PoolReaders))
	}
	if c.PoolWriters > 0 {
		q.Set("pool_writers", strconv.Itoa(c.PoolWriters))
	}
	if c.BusyTimeout > 0 {
		q.Set("busy_timeout", c.BusyTimeout.String())
	}
	if c.StorageMode != "" {
		q.Set("mode", strings.ToLower(strings.TrimSpace(c.StorageMode)))
	}
	if c.MaxMemoryBytes > 0 {
		q.Set("max_memory_bytes", strconv.FormatInt(c.MaxMemoryBytes, 10))
	}
	if c.ReadOnly {
		q.Set("read_only", "1")
	}
	if c.SyncOnMutate {
		q.Set("sync_on_mutate", "1")
	}
	if c.CompressFiles {
		q.Set("compress_files", "1")
	}
	if c.CheckpointEvery > 0 {
		q.Set("checkpoint_every", strconv.FormatUint(c.CheckpointEvery, 10))
	}
	if c.CheckpointInterval > 0 {
		q.Set("checkpoint_interval", c.CheckpointInterval.String())
	}
	if c.CheckpointMaxBytes != 0 {
		q.Set("checkpoint_max_bytes", strconv.FormatInt(c.CheckpointMaxBytes, 10))
	}
	if c.WALSync != "" {
		q.Set("wal_sync", strings.ToLower(strings.TrimSpace(c.WALSync)))
	}
	if c.PersistDebounce > 0 {
		q.Set("persist_debounce_ms", strconv.FormatInt(c.PersistDebounce.Milliseconds(), 10))
	}

	if mode == "file" {
		return "file:" + filepath.Clean(c.FilePath) + "?" + q.Encode(), nil
	}
	return "mem://?" + q.Encode(), nil
}

// OpenWithConfig opens a tinySQL database using explicit settings and validates
// connectivity with PingContext.
func OpenWithConfig(ctx context.Context, cfg OpenConfig) (*sql.DB, error) {
	dsn, err := cfg.DSN()
	if err != nil {
		return nil, err
	}
	db, err := Open(dsn)
	if err != nil {
		return nil, err
	}

	if cfg.MaxOpenConns > 0 {
		db.SetMaxOpenConns(cfg.MaxOpenConns)
	}
	if cfg.MaxIdleConns > 0 {
		db.SetMaxIdleConns(cfg.MaxIdleConns)
	}
	if cfg.ConnMaxLifetime > 0 {
		db.SetConnMaxLifetime(cfg.ConnMaxLifetime)
	}
	if cfg.ConnMaxIdleTime > 0 {
		db.SetConnMaxIdleTime(cfg.ConnMaxIdleTime)
	}

	if ctx == nil {
		ctx = context.Background()
	}
	pingCtx := ctx
	cancel := func() {}
	if cfg.PingTimeout > 0 {
		pingCtx, cancel = context.WithTimeout(ctx, cfg.PingTimeout)
	}
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

func validateOpenConfig(c OpenConfig, mode string) error {
	switch mode {
	case "mem", "file":
	default:
		return fmt.Errorf("tinysql: unsupported mode %q (use mem or file)", c.Mode)
	}
	if mode == "file" && strings.TrimSpace(c.FilePath) == "" {
		return fmt.Errorf("tinysql: file mode requires FilePath")
	}
	if c.PoolReaders < 0 {
		return fmt.Errorf("tinysql: PoolReaders must be >= 0")
	}
	if c.PoolWriters < 0 {
		return fmt.Errorf("tinysql: PoolWriters must be >= 0")
	}
	if c.BusyTimeout < 0 {
		return fmt.Errorf("tinysql: BusyTimeout must be >= 0")
	}
	storageMode := strings.ToLower(strings.TrimSpace(c.StorageMode))
	if storageMode != "" {
		// Delegate to the same alias table internal/driver's DSN layer uses at
		// Open time (storage.ParseStorageMode), rather than a second,
		// separately-maintained copy: a mode this switch accepted or rejected
		// used to be able to drift from what the DSN's mode= option actually
		// resolves once the connection opens.
		if _, err := storage.ParseStorageMode(storageMode); err != nil {
			return fmt.Errorf("tinysql: unsupported StorageMode %q", c.StorageMode)
		}
		if mode != "file" && storageMode != "memory" && storageMode != "mem" && storageMode != "ram" {
			return fmt.Errorf("tinysql: StorageMode %q requires file mode and FilePath", c.StorageMode)
		}
		if c.ReadOnly && (storageMode == "wal" || storageMode == "advanced_wal" || storageMode == "advancedwal") {
			return fmt.Errorf("tinysql: ReadOnly is not supported with StorageMode %q", c.StorageMode)
		}
	}
	if c.MaxMemoryBytes < 0 {
		return fmt.Errorf("tinysql: MaxMemoryBytes must be >= 0")
	}
	if c.CheckpointInterval < 0 {
		return fmt.Errorf("tinysql: CheckpointInterval must be >= 0")
	}
	if c.CheckpointMaxBytes < -1 {
		return fmt.Errorf("tinysql: CheckpointMaxBytes must be >= -1")
	}
	if c.PersistDebounce < 0 || c.PersistDebounce%time.Millisecond != 0 {
		return fmt.Errorf("tinysql: PersistDebounce must be a non-negative whole number of milliseconds")
	}
	if walSync := strings.ToLower(strings.TrimSpace(c.WALSync)); walSync != "" && walSync != "full" && walSync != "normal" {
		return fmt.Errorf("tinysql: WALSync must be full or normal")
	}
	if c.MaxOpenConns < 0 {
		return fmt.Errorf("tinysql: MaxOpenConns must be >= 0")
	}
	if c.MaxIdleConns < 0 {
		return fmt.Errorf("tinysql: MaxIdleConns must be >= 0")
	}
	if c.ConnMaxLifetime < 0 {
		return fmt.Errorf("tinysql: ConnMaxLifetime must be >= 0")
	}
	if c.ConnMaxIdleTime < 0 {
		return fmt.Errorf("tinysql: ConnMaxIdleTime must be >= 0")
	}
	if c.PingTimeout < 0 {
		return fmt.Errorf("tinysql: PingTimeout must be >= 0")
	}
	return nil
}
