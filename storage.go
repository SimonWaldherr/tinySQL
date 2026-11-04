package tinysql

import (
	"context"
	"fmt"
	"io"
	"reflect"
	"strings"
	"sync"

	"github.com/SimonWaldherr/tinySQL/internal/engine"
	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// DBHealth describes database lifecycle, storage, scheduler, and recovery state.
type DBHealth = storage.DBHealth

// RecoveryStatus describes the last recovery pass performed while opening a DB.
type RecoveryStatus = storage.RecoveryStatus

// MVCCManager coordinates multi-version concurrency control with snapshot isolation.
// Provides transaction begin/commit/abort operations and visibility checking.
type MVCCManager = storage.MVCCManager

// TxID represents a unique transaction identifier.
type TxID = storage.TxID

// TxContext holds the state of an active transaction including read/write sets
// and isolation level.
type TxContext = storage.TxContext

// TxStatus represents the current state of a transaction.
type TxStatus = storage.TxStatus

// Timestamp represents a logical timestamp for MVCC visibility checks.
type Timestamp = storage.Timestamp

// IsolationLevel defines transaction isolation semantics.
type IsolationLevel = storage.IsolationLevel

// Transaction status constants
const (
	TxStatusInProgress = storage.TxStatusInProgress // Transaction is active
	TxStatusCommitted  = storage.TxStatusCommitted  // Transaction committed successfully
	TxStatusAborted    = storage.TxStatusAborted    // Transaction was aborted
)

// Isolation level constants
const (
	ReadCommitted     IsolationLevel = storage.ReadCommitted     // Read only committed data
	RepeatableRead    IsolationLevel = storage.RepeatableRead    // Repeatable reads within transaction
	SnapshotIsolation IsolationLevel = storage.SnapshotIsolation // Full snapshot isolation
	Serializable      IsolationLevel = storage.Serializable      // Serializable transactions with conflict detection
)

// AdvancedWAL manages row-level write-ahead logging with REDO/UNDO support.
// Provides durability, crash recovery, and point-in-time recovery.
type AdvancedWAL = storage.AdvancedWAL

// AdvancedWALConfig configures the advanced WAL behavior.
type AdvancedWALConfig = storage.AdvancedWALConfig

// LSN (Log Sequence Number) provides total ordering of log records.
type LSN = storage.LSN

// WALRecord represents a single log entry with before/after images.
type WALRecord = storage.WALRecord

// WALOperationType defines the type of WAL operation.
type WALOperationType = storage.WALOperationType

// WAL operation type constants
const (
	WALOpBegin      = storage.WALOpBegin      // Transaction begin
	WALOpInsert     = storage.WALOpInsert     // Row insert
	WALOpUpdate     = storage.WALOpUpdate     // Row update
	WALOpDelete     = storage.WALOpDelete     // Row delete
	WALOpCommit     = storage.WALOpCommit     // Transaction commit
	WALOpAbort      = storage.WALOpAbort      // Transaction abort
	WALOpCheckpoint = storage.WALOpCheckpoint // Checkpoint operation
)

// AuditLog is an append-only, hash-chained log of statement executions. See
// OpenAuditLog/NewAuditLog and DB.AttachAuditLog.
type AuditLog = storage.AuditLog

// AuditEntry is one recorded statement execution, including the hash chain
// fields (PrevHash/Hash) that make tampering detectable via AuditLog.Verify.
type AuditEntry = storage.AuditEntry

// NewAuditLog creates an in-memory-only audit log (not persisted to disk).
func NewAuditLog() *AuditLog {
	return storage.NewAuditLog()
}

// OpenAuditLog opens (creating if needed) a file-backed, append-only audit
// log at path. If the file already contains entries, they're replayed and
// the hash chain is verified as part of opening — a tampered or corrupted
// existing log is rejected immediately rather than silently trusted.
//
// Example:
//
//	auditLog, err := tinysql.OpenAuditLog("data/audit.jsonl")
//	if err != nil {
//	    log.Fatal(err) // includes "tampered with" detail if verification failed
//	}
//	defer auditLog.Close()
//	db.AttachAuditLog(auditLog)
//	...
//	ctx = tinysql.WithAuditText(tinysql.WithUser(ctx, "alice"), sqlText)
//	tinysql.Execute(ctx, db, "default", stmt)
func OpenAuditLog(path string) (*AuditLog, error) {
	return storage.OpenAuditLog(path)
}

// WithAuditText attaches the original SQL text of the statement about to
// run to ctx, so an attached audit log records the real query instead of a
// best-effort statement-type fallback. See OpenAuditLog's example.
func WithAuditText(ctx context.Context, sqlText string) context.Context {
	return engine.WithAuditText(ctx, sqlText)
}

// StorageMode defines how the database manages data between memory and disk.
// Different modes trade off speed, memory usage, and durability.
type StorageMode = storage.StorageMode

// WALSyncMode controls the per-commit flush strength used by WAL storage
// modes. WALSyncFull is the default; WALSyncNormal opts into regular fsync
// where the platform distinguishes it from a full hardware flush.
type WALSyncMode = storage.WALSyncMode

const (
	// WALSyncFull preserves the historic strongest available WAL flush.
	WALSyncFull = storage.WALSyncFull
	// WALSyncNormal uses a regular fsync while retaining synchronous commits.
	WALSyncNormal = storage.WALSyncNormal
)

// StorageConfig configures database storage behaviour. Pass to OpenDB to
// create a database with the desired persistence strategy.
type StorageConfig = storage.StorageConfig

// StorageBackend is the interface implemented by all storage backends.
type StorageBackend = storage.StorageBackend

// BackendStats provides observability into storage backend behaviour.
type BackendStats = storage.BackendStats

// EncryptionKeySize is the required key length, in bytes, for
// StorageConfig.EncryptionKey and NewEncryptor (AES-256).
const EncryptionKeySize = storage.EncryptionKeySize

// EncryptionSaltSize is the recommended salt length, in bytes, for
// DeriveKeyFromPassphrase.
const EncryptionSaltSize = storage.EncryptionSaltSize

// Encryptor performs authenticated AES-256-GCM encryption/decryption.
// StorageConfig.EncryptionKey is the usual way to enable it for disk-backed
// table files; construct one directly only if you need to encrypt/decrypt
// something outside the storage backend's own file I/O.
type Encryptor = storage.Encryptor

// NewEncryptor creates an Encryptor from an EncryptionKeySize-byte key.
func NewEncryptor(key []byte) (*Encryptor, error) {
	return storage.NewEncryptor(key)
}

// DeriveKeyFromPassphrase derives an AES-256 key from a human-chosen
// passphrase using Argon2id (memory-hard, resists large-scale offline
// guessing better than a plain PBKDF2/SHA hash). salt must be a fixed,
// persisted value (see NewEncryptionSalt) — the same passphrase and salt
// always derive the same key, but a different salt derives a different
// key from the same passphrase.
//
// Example:
//
//	salt, _ := tinysql.NewEncryptionSalt()   // generate once, persist it yourself
//	key := tinysql.DeriveKeyFromPassphrase("correct horse battery staple", salt)
//	db, err := tinysql.OpenDB(tinysql.StorageConfig{
//	    Mode: tinysql.ModeDisk, Path: "./data", EncryptionKey: key,
//	})
func DeriveKeyFromPassphrase(passphrase string, salt []byte) []byte {
	return storage.DeriveKeyFromPassphrase(passphrase, salt)
}

// NewEncryptionSalt returns a fresh, cryptographically random salt for
// DeriveKeyFromPassphrase.
func NewEncryptionSalt() ([]byte, error) {
	return storage.NewEncryptionSalt()
}

// Storage mode constants.
const (
	// ModeMemory keeps all data in RAM. Persistence only occurs via explicit
	// SaveToFile calls or when DB.Close is invoked. Fastest mode.
	ModeMemory StorageMode = storage.ModeMemory

	// ModeWAL keeps all data in RAM and writes a Write-Ahead Log for crash
	// recovery. Periodic checkpoints create full GOB snapshots.
	ModeWAL StorageMode = storage.ModeWAL

	// ModeDisk stores each table as a separate GOB file on disk. Tables are
	// loaded into memory on demand and flushed back on Sync/Close.
	ModeDisk StorageMode = storage.ModeDisk

	// ModeJSON stores each table as a separate human-readable JSON file on
	// disk — same lazy-load/dirty-tracking behavior as ModeDisk, which uses
	// GOB instead. Larger on disk and big.Rat/uuid.UUID values round-trip as
	// plain strings, but files can be read, diffed, or hand-edited with any
	// text tool.
	ModeJSON StorageMode = storage.ModeJSON

	// ModeIndex keeps table schemas in RAM while row data resides on disk.
	// Rows are loaded on demand with aggressive eviction. Memory usage is
	// proportional to schema size, not data size.
	ModeIndex StorageMode = storage.ModeIndex

	// ModeHybrid uses an LRU buffer pool with a configurable memory limit.
	// Hot tables stay in RAM; cold tables spill to disk.
	ModeHybrid StorageMode = storage.ModeHybrid
)

// OpenDB creates or opens a database with the specified storage mode and
// configuration. This is the primary entry point for creating databases that
// persist data to disk using different strategies.
//
// Storage Mode Overview:
//
//	ModeMemory  – All data in RAM. Fast. Save on Close if Path is set.
//	ModeWAL     – All data in RAM + Write-Ahead Log for crash recovery.
//	ModeDisk    – Per-table files on disk, lazy-loaded into RAM.
//	ModeIndex   – Schemas in RAM, rows on disk with small LRU cache.
//	ModeHybrid  – Disk-backed with configurable LRU memory limit.
//
// Examples:
//
//	// In-memory with save-on-close
//	db, _ := tinysql.OpenDB(tinysql.StorageConfig{
//	    Mode: tinysql.ModeMemory,
//	    Path: "mydb.gob",
//	})
//	defer db.Close()
//
//	// Disk mode for large databases
//	db, _ := tinysql.OpenDB(tinysql.StorageConfig{
//	    Mode: tinysql.ModeDisk,
//	    Path: "/data/mydb",
//	})
//	defer db.Close()
//
//	// Hybrid mode with 512 MB cache
//	db, _ := tinysql.OpenDB(tinysql.StorageConfig{
//	    Mode:           tinysql.ModeHybrid,
//	    Path:           "/data/mydb",
//	    MaxMemoryBytes: 512 * 1024 * 1024,
//	})
//	defer db.Close()
//
// Always call db.Close() to ensure data is flushed to disk.
func OpenDB(cfg StorageConfig) (*DB, error) {
	return storage.OpenDB(cfg)
}

// ParseStorageMode converts a string like "memory", "wal", "disk", "index",
// or "hybrid" into a StorageMode constant (case-insensitive).
func ParseStorageMode(s string) (StorageMode, error) {
	return storage.ParseStorageMode(s)
}

// ParseWALSyncMode parses "full" or "normal" for StorageConfig.WALSync.
func ParseWALSyncMode(s string) (WALSyncMode, error) {
	return storage.ParseWALSyncMode(s)
}

// DefaultStorageConfig returns a StorageConfig with sensible defaults for
// the given mode. The caller should set Path before passing to OpenDB.
func DefaultStorageConfig(mode StorageMode) StorageConfig {
	return storage.DefaultStorageConfig(mode)
}

// HealthCheck returns a production-oriented database health snapshot.
func HealthCheck(db *DB) DBHealth {
	if db == nil {
		return DBHealth{OK: false, Error: "nil DB"}
	}
	return db.HealthCheck()
}

// SaveToFile serializes the entire database to a GOB file for persistence.
//
// This creates a snapshot of all tables, rows, and metadata for all tenants.
// The file can be loaded later with LoadFromFile to restore the database state.
//
// Example:
//
//	err := tinysql.SaveToFile(db, "mydb.gob")
//	if err != nil {
//	    log.Fatal(err)
//	}
//
// Note: This saves the current state only. For durability during crashes,
// use AttachAdvancedWAL to enable write-ahead logging.
func SaveToFile(db *DB, filename string) error {
	return storage.SaveToFile(db, filename)
}

// SaveToWriter serializes a consistent database snapshot to w. It is useful
// for embedded targets, HTTP responses, and callers that own their storage.
func SaveToWriter(db *DB, w io.Writer) error {
	return storage.SaveToWriter(db, w)
}

// LoadFromFile deserializes a database from a GOB file created by SaveToFile.
//
// This restores all tables, rows, and metadata from the file. The returned
// database instance is ready for use immediately.
//
// Example:
//
//	db, err := tinysql.LoadFromFile("mydb.gob")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer db.Close()
//
// Returns a new DB instance or an error if the file cannot be read.
func LoadFromFile(filename string) (*DB, error) {
	return storage.LoadFromFile(filename)
}

// LoadFromReader restores a database snapshot from r without requiring a
// temporary file. Callers should bound untrusted input before passing it here.
func LoadFromReader(r io.Reader) (*DB, error) {
	return storage.LoadFromReader(r)
}

// SaveToBytes serializes the entire database to an in-memory GOB snapshot.
//
// This is useful for embedding targets such as WebAssembly where writing to a
// filesystem is unavailable or expensive. The returned bytes can be restored
// with LoadFromBytes.
func SaveToBytes(db *DB) ([]byte, error) {
	return storage.SaveToBytes(db)
}

// LoadFromBytes deserializes a database from a GOB snapshot created by
// SaveToBytes.
func LoadFromBytes(snapshot []byte) (*DB, error) {
	return storage.LoadFromBytes(snapshot)
}

// NewAdvancedWAL creates a new write-ahead log manager with default configuration.
//
// The WAL logs all database modifications (INSERT, UPDATE, DELETE) to disk before
// applying them. This enables:
//   - Crash recovery: replay committed transactions after restart
//   - Point-in-time recovery: restore to any previous state
//   - Durability: changes survive system crashes
//
// Example:
//
//	wal, err := tinysql.NewAdvancedWAL("data/wal.log")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer wal.Close()
//
//	db.AttachAdvancedWAL(wal)
//
//	// Now all database modifications are logged
//	stmt, _ := tinysql.ParseSQL("INSERT INTO users VALUES (1, 'Alice')")
//	tinysql.Execute(ctx, db, "default", stmt)
//
// The path parameter specifies the WAL file location.
func NewAdvancedWAL(path string) (*AdvancedWAL, error) {
	return storage.OpenAdvancedWAL(storage.AdvancedWALConfig{
		Path:               path,
		CheckpointPath:     path + ".checkpoint",
		CheckpointEvery:    1000,
		CheckpointInterval: 300000000000, // 5 minutes in nanoseconds
		BufferSize:         65536,        // 64KB
	})
}

// OpenAdvancedWAL creates or opens a WAL with custom configuration.
//
// This provides full control over WAL behavior including checkpoint
// intervals and buffer sizes. Checkpoint-snapshot compression is configured
// separately, via StorageConfig.CompressFiles when opening through OpenDB
// with Mode: ModeAdvancedWAL — the live WAL log itself is never compressed.
//
// Example:
//
//	wal, err := tinysql.OpenAdvancedWAL(tinysql.AdvancedWALConfig{
//	    Path:               "data/wal.log",
//	    CheckpointPath:     "data/checkpoint",
//	    CheckpointEvery:    5000,
//	    CheckpointInterval: 10 * time.Minute,
//	    BufferSize:         128 * 1024,
//	})
//
// Returns the WAL instance or an error if initialization fails.
func OpenAdvancedWAL(config AdvancedWALConfig) (*AdvancedWAL, error) {
	return storage.OpenAdvancedWAL(config)
}

// --- Deployment modes ---

// DeploymentMode describes the intended runtime shape for a tinySQL instance.
// It is additive metadata and does not change existing NewDB/OpenDB behavior.
type DeploymentMode string

const (
	// DeploymentPackage is the lightweight in-process Go package profile.
	DeploymentPackage DeploymentMode = "package"
	// DeploymentEmbedded is the SQLite-like local embedded database profile.
	DeploymentEmbedded DeploymentMode = "embedded"
	// DeploymentServer is the networked DBMS server profile.
	DeploymentServer DeploymentMode = "server"
	// DeploymentEnterprise is the durable server profile with enterprise services enabled by default.
	DeploymentEnterprise DeploymentMode = "enterprise"
)

// String returns the stable string identifier for a deployment mode.
func (m DeploymentMode) String() string {
	if m == "" {
		return string(DeploymentPackage)
	}
	return string(m)
}

// ParseDeploymentMode parses a deployment mode string.
func ParseDeploymentMode(s string) (DeploymentMode, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "package", "pkg", "library", "lib":
		return DeploymentPackage, nil
	case "embedded", "embed", "sqlite", "sqlite-like", "local":
		return DeploymentEmbedded, nil
	case "server", "dbms", "daemon":
		return DeploymentServer, nil
	case "enterprise", "ent":
		return DeploymentEnterprise, nil
	default:
		return DeploymentPackage, fmt.Errorf("unknown deployment mode %q (valid: package, embedded, server, enterprise)", s)
	}
}

// DeploymentConfig configures a higher-level tinySQL product form.
// Existing callers can continue using NewDB/OpenDB directly; this wrapper is
// for applications that want an explicit package/embedded/server/enterprise shape.
type DeploymentConfig struct {
	Mode              DeploymentMode
	Tenant            string
	Storage           StorageConfig
	StartJobScheduler bool
	JobExecutor       JobExecutor
}

// Instance is a thin runtime wrapper around DB with deployment metadata.
type Instance struct {
	mu     sync.Mutex
	DB     *DB
	Mode   DeploymentMode
	Tenant string
	config DeploymentConfig
}

// OpenDeployment opens a tinySQL instance using the supplied deployment config.
func OpenDeployment(cfg DeploymentConfig) (*Instance, error) {
	mode := cfg.Mode
	if mode == "" {
		mode = DeploymentPackage
	}
	tenant := cfg.Tenant
	if tenant == "" {
		tenant = "default"
	}

	cfg.Mode = mode
	cfg.Tenant = tenant

	var db *DB
	var err error
	if mode == DeploymentPackage && reflect.DeepEqual(cfg.Storage, StorageConfig{}) {
		db = NewDB()
	} else {
		db, err = OpenDB(cfg.Storage)
	}
	if err != nil {
		return nil, err
	}

	if cfg.StartJobScheduler {
		executor := cfg.JobExecutor
		if executor == nil {
			executor = NewSQLJobExecutor(db, tenant)
		}
		if err := db.StartJobScheduler(executor); err != nil {
			_ = db.Close()
			return nil, err
		}
	}

	return &Instance{DB: db, Mode: mode, Tenant: tenant, config: cfg}, nil
}

// OpenPackage creates an in-memory package-mode instance.
func OpenPackage(tenant string) *Instance {
	if tenant == "" {
		tenant = "default"
	}
	return &Instance{
		DB:     NewDB(),
		Mode:   DeploymentPackage,
		Tenant: tenant,
		config: DeploymentConfig{Mode: DeploymentPackage, Tenant: tenant},
	}
}

// OpenEmbedded opens a SQLite-like embedded instance at path using WAL storage.
func OpenEmbedded(path, tenant string) (*Instance, error) {
	if path == "" {
		return nil, fmt.Errorf("embedded deployment requires a path")
	}
	return OpenDeployment(DeploymentConfig{
		Mode:   DeploymentEmbedded,
		Tenant: tenant,
		Storage: StorageConfig{
			Mode: ModeWAL,
			Path: path,
		},
	})
}

// OpenServer opens a server-profile instance. The caller decides storage policy.
func OpenServer(cfg StorageConfig, tenant string) (*Instance, error) {
	return OpenDeployment(DeploymentConfig{
		Mode:              DeploymentServer,
		Tenant:            tenant,
		Storage:           cfg,
		StartJobScheduler: true,
	})
}

// OpenEnterprise opens an enterprise-profile instance with durable storage required.
func OpenEnterprise(cfg StorageConfig, tenant string) (*Instance, error) {
	if cfg.Path == "" {
		return nil, fmt.Errorf("enterprise deployment requires durable storage path")
	}
	if cfg.Mode == ModeMemory {
		return nil, fmt.Errorf("enterprise deployment requires durable storage mode")
	}
	return OpenDeployment(DeploymentConfig{
		Mode:              DeploymentEnterprise,
		Tenant:            tenant,
		Storage:           cfg,
		StartJobScheduler: true,
	})
}

// ExecuteSQL parses and executes one SQL statement against the instance tenant.
func (i *Instance) ExecuteSQL(ctx context.Context, sql string) (*ResultSet, error) {
	if i == nil {
		return nil, fmt.Errorf("nil tinySQL instance")
	}
	// Snapshot the DB pointer under the same lock Start/Stop use to mutate it,
	// then release before executing so concurrent queries are not serialized
	// and a concurrent Restart cannot swap the DB out from under us.
	i.mu.Lock()
	db, tenant := i.DB, i.Tenant
	i.mu.Unlock()
	if db == nil {
		return nil, fmt.Errorf("nil tinySQL instance")
	}
	stmt, err := ParseSQL(sql)
	if err != nil {
		return nil, err
	}
	return Execute(ctx, db, tenant, stmt)
}

// Start opens the configured DB if the instance is stopped.
func (i *Instance) Start() error {
	if i == nil {
		return fmt.Errorf("nil tinySQL instance")
	}
	i.mu.Lock()
	defer i.mu.Unlock()

	if i.DB != nil && i.DB.HealthCheck().OK {
		return nil
	}

	cfg := i.config
	if cfg.Mode == "" {
		cfg.Mode = i.Mode
	}
	if cfg.Mode == "" {
		cfg.Mode = DeploymentPackage
	}
	if cfg.Tenant == "" {
		cfg.Tenant = i.Tenant
	}
	if cfg.Tenant == "" {
		cfg.Tenant = "default"
	}

	var db *DB
	var err error
	if cfg.Mode == DeploymentPackage && reflect.DeepEqual(cfg.Storage, StorageConfig{}) {
		db = NewDB()
	} else {
		db, err = OpenDB(cfg.Storage)
	}
	if err != nil {
		return err
	}
	if cfg.StartJobScheduler {
		executor := cfg.JobExecutor
		if executor == nil {
			executor = NewSQLJobExecutor(db, cfg.Tenant)
		}
		if err := db.StartJobScheduler(executor); err != nil {
			_ = db.Close()
			return err
		}
	}

	i.DB = db
	i.Mode = cfg.Mode
	i.Tenant = cfg.Tenant
	i.config = cfg
	return nil
}

// Stop gracefully stops schedulers, syncs storage, and closes DB resources.
func (i *Instance) Stop() error {
	if i == nil {
		return nil
	}
	i.mu.Lock()
	db := i.DB
	i.mu.Unlock()
	// Keep i.DB pointing at the closed DB (do not nil it) so Health() can still
	// report the closed snapshot (Closed, LastCloseAt); Start() replaces it.
	if db == nil {
		return nil
	}
	return db.Close()
}

// Restart gracefully stops and reopens the configured instance.
func (i *Instance) Restart() error {
	if i == nil {
		return fmt.Errorf("nil tinySQL instance")
	}
	if err := i.Stop(); err != nil {
		return err
	}
	return i.Start()
}

// Health returns a production-oriented status snapshot for the instance.
func (i *Instance) Health() DBHealth {
	if i == nil {
		return DBHealth{OK: false, Error: "nil tinySQL instance"}
	}
	i.mu.Lock()
	db := i.DB
	i.mu.Unlock()
	if db == nil {
		return DBHealth{OK: false, Error: "nil tinySQL instance"}
	}
	return db.HealthCheck()
}

// Close releases instance resources.
func (i *Instance) Close() error {
	return i.Stop()
}
