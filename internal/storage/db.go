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
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

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

	// shadow marks this database as an uncommitted working copy — the private
	// snapshot the SQL driver runs a BEGIN…COMMIT block against. StatementWAL
	// reports no log for a shadow, so the engine cannot append a statement
	// that a later ROLLBACK would discard; the driver logs the whole
	// transaction against the live database when it commits. See
	// SnapshotForTx and PromoteShadow.
	shadow atomic.Bool

	// ambientWALTx is the AdvancedWAL transaction that every statement running
	// against this database joins instead of opening its own. The SQL driver
	// sets it on a transaction shadow so a multi-statement BEGIN…COMMIT block
	// becomes one AdvancedWAL transaction, committed or aborted as a unit.
	// Zero means "no ambient transaction: each statement is its own".
	ambientWALTx atomic.Uint64
}

// BeginAmbientWALTx opens one AdvancedWAL transaction that every subsequent
// statement executed against db joins, instead of each statement logging its
// own implicitly-committed transaction. The SQL driver calls it on a
// transaction shadow at BEGIN so that recovery replays the block only if
// CommitAmbientWALTx ran; AbortAmbientWALTx marks it rolled back.
//
// It is idempotent, so the driver can call it lazily before the first write in
// a block instead of writing a begin record for every read-only BEGIN. It is a
// no-op returning false when no AdvancedWAL is attached.
func (db *DB) BeginAmbientWALTx() (bool, error) {
	if db == nil {
		return false, nil
	}
	wal := db.AdvancedWAL()
	if wal == nil {
		return false, nil
	}
	if _, open := db.AmbientWALTx(); open {
		return true, nil
	}
	txID := wal.NewAutoTxID()
	if _, err := wal.LogBegin(txID); err != nil {
		return false, err
	}
	db.ambientWALTx.Store(uint64(txID))
	return true, nil
}

// AmbientWALTx returns the ambient AdvancedWAL transaction, if one is open.
func (db *DB) AmbientWALTx() (TxID, bool) {
	if db == nil {
		return 0, false
	}
	v := db.ambientWALTx.Load()
	return TxID(v), v != 0
}

// CommitAmbientWALTx writes the commit record for the ambient transaction and
// clears it. Recovery replays the transaction's operations only after seeing
// this record.
func (db *DB) CommitAmbientWALTx() error {
	txID, ok := db.AmbientWALTx()
	if !ok {
		return nil
	}
	db.ambientWALTx.Store(0)
	wal := db.AdvancedWAL()
	if wal == nil {
		return nil
	}
	_, err := wal.LogCommit(txID)
	return err
}

// AbortAmbientWALTx writes the abort record for the ambient transaction and
// clears it, so recovery discards whatever the rolled-back block logged.
func (db *DB) AbortAmbientWALTx() error {
	txID, ok := db.AmbientWALTx()
	if !ok {
		return nil
	}
	db.ambientWALTx.Store(0)
	wal := db.AdvancedWAL()
	if wal == nil {
		return nil
	}
	_, err := wal.LogAbort(txID)
	return err
}

// copyRuntimeState copies everything that is not tenant/table data onto out:
// the write-ahead logs, the storage backend, the audit log, the MVCC
// coordinator, the job scheduler, the open configuration, the storage mode and
// the read-only flag.
//
// Every clone constructor must call it. Before it existed each one hand-copied
// only .wal, so a clone that the driver promoted to be the live database
// silently lost its backend, catalog, audit log and scheduler — and a clone of
// an empty database lost the WAL too, which stopped a fresh ModeWAL database
// from ever logging again after its first write.
//
// isolateCatalog selects how catalog-resident state (views, triggers,
// materialized-view freshness, jobs, RBAC) is carried over:
//
//   - true deep-copies it, so the clone can be mutated for the lifetime of a
//     transaction without the live database observing uncommitted DDL. The
//     driver installs the result at COMMIT.
//   - false shares the live instance, which is what a single autocommit
//     statement needs: the engine's own StatementSnapshot already restores the
//     catalog if that statement fails, and the statement holds the writer lock
//     throughout, so no reader can observe the intermediate state.
func (db *DB) copyRuntimeState(out *DB, isolateCatalog bool) {
	if db == nil || out == nil {
		return
	}
	db.mu.RLock()
	out.wal = db.wal
	out.advancedWAL = db.advancedWAL
	out.auditLog = db.auditLog
	out.mvcc = db.mvcc
	out.scheduler = db.scheduler
	out.backend = db.backend
	out.storageMode = db.storageMode
	out.config = db.config
	out.lastRecovery = db.lastRecovery
	db.mu.RUnlock()
	out.readOnly.Store(db.readOnly.Load())

	live := db.Catalog()
	if isolateCatalog {
		copied := diskToCatalog(catalogToDisk(live))
		copied.setRevision(live.Revision())
		out.setCatalog(copied)
	} else {
		out.setCatalog(live)
	}

	db.extensionsMu.RLock()
	if len(db.extensions) > 0 {
		out.extensions = make(map[string]ExtensionInfo, len(db.extensions))
		for k, v := range db.extensions {
			out.extensions[k] = v
		}
	}
	db.extensionsMu.RUnlock()
}

// markShadow flags out as an uncommitted working copy. See DB.shadow.
func markShadow(out *DB) {
	if out != nil {
		out.shadow.Store(true)
	}
}

// PromoteShadow clears the shadow flag, declaring that this database's
// contents are committed and that statements executed against it from now on
// are responsible for their own write-ahead logging. Only the SQL driver calls
// it, after it has durably logged the transaction that produced the shadow.
func (db *DB) PromoteShadow() {
	if db != nil {
		db.shadow.Store(false)
	}
}

// IsShadow reports whether this database is an uncommitted working copy.
func (db *DB) IsShadow() bool {
	return db != nil && db.shadow.Load()
}

// IsClosed reports whether Close has completed for this database. The SQL
// driver uses it to let the connection pool discard connections whose database
// went away underneath them, rather than returning errors from each one.
func (db *DB) IsClosed() bool {
	if db == nil {
		return true
	}
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.closed
}

// StatementWAL returns the WALManager that a statement executing against db
// must append to, or nil when statement-level logging is not this database's
// responsibility.
//
// It differs from WAL in exactly one case: a transaction shadow, where it
// returns nil. A statement inside BEGIN…COMMIT is not durable until the
// transaction commits, so logging it as it runs would leave a committed-looking
// record on disk that recovery replays even when the transaction was rolled
// back. The driver logs the transaction as a whole against the live database
// instead.
func (db *DB) StatementWAL() *WALManager {
	if db == nil || db.shadow.Load() {
		return nil
	}
	return db.WAL()
}

// StatementAdvancedWAL is the AdvancedWAL counterpart of StatementWAL. Unlike
// the basic WALManager, AdvancedWAL records row-level operations grouped into
// explicit transactions, so a shadow does not have to stop logging: it logs
// into the ambient transaction the driver opened at BEGIN (see
// BeginAmbientWALTx), which recovery only replays once a matching commit
// record exists.
func (db *DB) StatementAdvancedWAL() *AdvancedWAL {
	if db == nil {
		return nil
	}
	return db.AdvancedWAL()
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
