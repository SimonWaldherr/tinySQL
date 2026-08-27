// The operational surface: health, recovery status, syncing to the backend,
// closing, evicting a table from memory, and migrating an in-memory database
// onto a backend.
package storage

import (
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
	if db.storageMode == ModeDisk || db.storageMode == ModeJSON || db.storageMode == ModeHybrid || db.storageMode == ModeIndex || db.storageMode == ModePagedIndex || db.storageMode == ModeSQLite {
		dc, hasDirtyTracker := db.backend.(dirtyTracker)

		type entry struct {
			tenant           string
			table            *Table
			ftsGeneration    int
			vectorGeneration int
		}
		var toSave []entry
		seen := make(map[string]bool) // tenant\x00lower(table name)

		db.mu.RLock()
		for tn, tdb := range db.tenants {
			for _, t := range tdb.tables {
				seen[tn+"\x00"+strings.ToLower(t.Name)] = true
				ftsGeneration, ftsDirty := t.FTSIndexesPersistenceState()
				vectorGeneration, vectorDirty := t.VectorIndexesPersistenceState()
				if hasDirtyTracker && !dc.IsDirty(tn, t.Name, t.Version) && !ftsDirty && !vectorDirty {
					continue
				}
				toSave = append(toSave, entry{tenant: tn, table: t, ftsGeneration: ftsGeneration, vectorGeneration: vectorGeneration})
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
				ftsGeneration, ftsDirty := ref.Table.FTSIndexesPersistenceState()
				vectorGeneration, vectorDirty := ref.Table.VectorIndexesPersistenceState()
				if hasDirtyTracker && !dc.IsDirty(ref.Tenant, ref.Table.Name, ref.Table.Version) && !ftsDirty && !vectorDirty {
					continue
				}
				toSave = append(toSave, entry{tenant: ref.Tenant, table: ref.Table, ftsGeneration: ftsGeneration, vectorGeneration: vectorGeneration})
			}
		}

		for _, e := range toSave {
			if err := db.backend.SaveTable(e.tenant, e.table); err != nil {
				return db.markError(err)
			}
			e.table.MarkFTSIndexesPersisted(e.ftsGeneration)
			e.table.MarkVectorIndexesPersisted(e.vectorGeneration)
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
		if err := wal.Checkpoint(db); err != nil && firstErr == nil {
			firstErr = err
		}
	}

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
