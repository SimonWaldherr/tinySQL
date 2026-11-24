// The operational surface: health, recovery status, syncing to the backend,
// closing, evicting a table from memory, and migrating an in-memory database
// onto a backend.
package storage

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

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
//
// For ModeDisk, ModeJSON, ModeHybrid and ModeIndex, Sync does not stop the
// world. It copies each dirty table (row headers, schema, statistics; the
// secondary indexes are lent) under the content lock's read side, in one
// critical section of a few milliseconds even for a million rows, and then
// encodes and writes the copies, atomically replacing each table file, with no
// engine lock held: statements, including DML on the table being written, run
// meanwhile, and what they change stays dirty for the next Sync. A nil return
// means the state each table had when Sync took its snapshot is on disk. Syncs
// are serialized with each other, and Sync must not be called while the caller
// holds the content lock (LockContentForRead/LockContentForWrite) itself.
// Other backends are handed the live table and need the caller to keep writers
// out for the duration, as before.
func (db *DB) Sync() error {
	if db.IsReadOnly() {
		db.markSynced()
		return nil
	}
	if db.backend == nil {
		db.markSynced()
		return nil
	}

	// One Sync at a time: two concurrent ones would snapshot and write the
	// same tables twice, and the second to finish could install the older
	// image. It is the outermost lock of the Sync path.
	db.syncMu.Lock()
	defer db.syncMu.Unlock()

	// For disk/hybrid/paged-index backends, save all resident tables that
	// are dirty (see collectDirtyTables for what "resident" means). A backend
	// that can write snapshots (DiskBackend, HybridBackend) has each dirty
	// table copied under a brief content read lock and written with no engine
	// lock held (see sync_snapshot.go); the others get the live table and need
	// the caller to keep writers out meanwhile, as they always did.
	if db.storageMode == ModeDisk || db.storageMode == ModeJSON || db.storageMode == ModeHybrid || db.storageMode == ModeIndex || db.storageMode == ModePagedIndex || db.storageMode == ModeSQLite {
		if err := db.flushDirtyTables(); err != nil {
			return db.markError(err)
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

// skipsSearchIndexPersistence reports whether the attached backend keeps the
// derived FTS/ANN index images out of its files.
func (db *DB) skipsSearchIndexPersistence() bool {
	s, ok := db.backend.(searchIndexPersistenceSkipper)
	return ok && s.SkipsSearchIndexPersistence()
}

// searchIndexPersistenceState returns the FTS and ANN generations and whether
// each needs persisting. With skip set the answer is "never": the backend
// writes no image, so a generation counter that moved (a search built an
// index) is not a reason to rewrite the table. The generations are still
// returned so the caller can mark them persisted, which keeps the counters
// meaningful should the table later be handed to a backend that writes images.
func (t *Table) searchIndexPersistenceState(skip bool) (ftsGeneration int, ftsDirty bool, vectorGeneration int, vectorDirty bool) {
	ftsGeneration, ftsDirty = t.FTSIndexesPersistenceState()
	vectorGeneration, vectorDirty = t.VectorIndexesPersistenceState()
	if skip {
		return ftsGeneration, false, vectorGeneration, false
	}
	return ftsGeneration, ftsDirty, vectorGeneration, vectorDirty
}

// searchIndexPersistenceStateNoWait is searchIndexPersistenceState for a caller
// that holds the engine's content lock and so must never wait for the table's
// derived-state lock: an FTS or ANN index build holds that lock for its whole
// duration (tens of seconds on a large table), and a Sync that waited for it
// inside its snapshot critical section would park every statement behind the
// first writer that queued up. With skip set nothing is read at all. When the
// lock is busy the answer is "not dirty": an index build is in progress, the
// image is about to change anyway, and the next Sync sees the finished build.
func (t *Table) searchIndexPersistenceStateNoWait(skip bool) (ftsGeneration int, ftsDirty bool, vectorGeneration int, vectorDirty bool) {
	if skip || !t.derivedMu.TryLock() {
		return 0, false, 0, false
	}
	ftsGeneration = t.ftsGeneration
	ftsDirty = ftsGeneration != t.ftsPersistedGeneration
	vectorGeneration = t.vectorGeneration
	vectorDirty = vectorGeneration != t.vectorPersistedGeneration
	t.derivedMu.Unlock()
	return ftsGeneration, ftsDirty, vectorGeneration, vectorDirty
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
	db.closeChangeWatchers()

	var closeErr error

	db.StopJobScheduler()
	// Stop and join the AdvancedWAL checkpoint worker before closing any
	// storage resources. The worker may be in SaveToFile/rotation at this
	// point; letting Close race it could leave a live goroutine with a closed
	// WAL descriptor or a half-observed final checkpoint.
	db.stopAdvancedWALCheckpointScheduler()
	// A schema/catalog-only AdvancedWAL statement requests a checkpoint rather
	// than producing a row log record. If Close follows it immediately, the
	// asynchronous worker may not have had a scheduling turn yet; flush that
	// pending snapshot synchronously before closing the WAL so CREATE/DROP/etc.
	// is never acknowledged and then lost merely because the process exited
	// quickly. Row-backed work is safe in the WAL either way, but checkpointing
	// it here also leaves a compact clean shutdown artifact.
	if wal := db.AdvancedWAL(); wal != nil && wal.checkpointWorkPending() {
		closeErr = errors.Join(closeErr, wal.Checkpoint(db))
	}

	// Sync dirty tables to backend.
	closeErr = errors.Join(closeErr, db.Sync())

	// Close backend (may do its own final save).
	if db.backend != nil {
		closeErr = errors.Join(closeErr, db.backend.Close())
	}

	// Close WAL resources.
	if db.wal != nil {
		closeErr = errors.Join(closeErr, db.wal.Close())
	}
	if db.advancedWAL != nil {
		closeErr = errors.Join(closeErr, db.advancedWAL.Close())
	}

	db.mu.Lock()
	db.closing = false
	db.lastCloseAt = time.Now()
	if closeErr == nil {
		db.closed = true
		db.lastError = ""
	} else {
		db.lastError = closeErr.Error()
	}
	db.mu.Unlock()

	return closeErr
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
	ftsGeneration, _ := t.FTSIndexesPersistenceState()
	vectorGeneration, _ := t.VectorIndexesPersistenceState()
	if err := db.backend.SaveTable(tenant, t); err != nil {
		return err
	}
	t.MarkFTSIndexesPersisted(ftsGeneration)
	t.MarkVectorIndexesPersisted(vectorGeneration)
	return nil
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
