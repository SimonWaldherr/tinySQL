package storage

// StatementSnapshot is an in-memory rollback point for one SQL statement.
// It is deliberately internal storage machinery: the engine holds DB's
// content lock for the whole statement, so a snapshot does not need to solve
// concurrent writers or provide an application-level transaction API.
//
// Table pointers that existed when the snapshot was taken are retained and
// restored in place. This keeps a caller holding a *Table obtained through
// DB.Get from observing a failed statement's half-applied row changes.
type StatementSnapshot struct {
	tables  map[string]map[string]tableState
	catalog diskCatalog
}

type tableState struct {
	table *Table
	state *Table
}

// SnapshotForStatement captures all table and catalog state needed to undo a
// failed mutating statement, including mutations issued by its triggers. The
// caller must already hold DB's content write lock.
func (db *DB) SnapshotForStatement() *StatementSnapshot {
	if db == nil {
		return nil
	}
	snapshot := &StatementSnapshot{
		tables:  make(map[string]map[string]tableState),
		catalog: catalogToDisk(db.Catalog()),
	}
	db.mu.RLock()
	for tenant, tenantDB := range db.tenants {
		tables := make(map[string]tableState, len(tenantDB.tables))
		for name, table := range tenantDB.tables {
			tables[name] = tableState{table: table, state: cloneTable(table)}
		}
		snapshot.tables[tenant] = tables
	}
	db.mu.RUnlock()
	return snapshot
}

// RestoreStatementSnapshot rolls a database back to snapshot. It restores
// pre-existing tables in place and removes tables created by the failed
// statement. The caller must hold DB's content write lock.
func (db *DB) RestoreStatementSnapshot(snapshot *StatementSnapshot) {
	if db == nil || snapshot == nil {
		return
	}

	db.mu.Lock()
	restored := make(map[string]*tenantDB, len(snapshot.tables))
	for tenant, tables := range snapshot.tables {
		tenantDB := &tenantDB{tables: make(map[string]*Table, len(tables))}
		for name, saved := range tables {
			restoreTable(saved.table, saved.state)
			tenantDB.tables[name] = saved.table
		}
		restored[tenant] = tenantDB
	}
	db.tenants = restored
	db.mu.Unlock()

	// CatalogManager has its own lock and includes materialized-view stale
	// state changed by DML. Reconstructing it from the deep-copy disk form is
	// less error-prone than selectively undoing each catalog side effect.
	db.setCatalog(diskToCatalog(snapshot.catalog))
}

// CollectWALChangesFromSnapshot computes the same per-table change set as
// CollectWALChanges, using a StatementSnapshot's pre-statement table clones
// as the "before" side instead of a second live *DB. This lets a single
// statement executed directly through engine.Execute drive WALManager
// logging (see internal/engine/wal_logging.go) without cloning the whole
// database purely to diff it — SnapshotForStatement already captured
// exactly the "before" state this needs, for rollback purposes.
func CollectWALChangesFromSnapshot(snapshot *StatementSnapshot, next *DB) []WALChange {
	if snapshot == nil || next == nil {
		return nil
	}
	tenants := make(map[string]*tenantDB, len(snapshot.tables))
	for tenant, tables := range snapshot.tables {
		td := &tenantDB{tables: make(map[string]*Table, len(tables))}
		for name, saved := range tables {
			td.tables[name] = saved.state
		}
		tenants[tenant] = td
	}
	return CollectWALChanges(&DB{tenants: tenants}, next)
}

func restoreTable(dst, saved *Table) {
	if dst == nil || saved == nil {
		return
	}
	copy := cloneTable(saved)
	dst.Name = copy.Name
	dst.Cols = copy.Cols
	dst.Rows = copy.Rows
	dst.Indexes = copy.Indexes
	dst.IsTemp = copy.IsTemp
	dst.colPos = copy.colPos
	dst.Version = copy.Version
	dst.Stats = copy.Stats
	dst.dirtyFrom = copy.dirtyFrom
}
