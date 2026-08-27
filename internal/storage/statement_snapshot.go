package storage

import (
	"fmt"
	"strings"
	"sync"
)

// StatementSnapshot is an in-memory rollback point for one SQL statement.
// It is deliberately internal storage machinery: the engine holds DB's
// content lock for the whole statement, so a snapshot does not need to solve
// concurrent writers or provide an application-level transaction API.
//
// Table pointers that existed when the snapshot was taken are retained and
// restored in place. This keeps a caller holding a *Table obtained through
// DB.Get from observing a failed statement's half-applied row changes.
type StatementSnapshot struct {
	tables map[string]map[string]tableState
	// tenant is the tenant a compact (single-table) snapshot belongs to; the
	// map-shaped forms carry it as their key instead. See RestoredTables.
	tenant  string
	catalog *catalogRollback
	// appendOnly is the compact rollback state for a triggerless INSERT that
	// can only append rows (secondary indexes included -- see
	// restoreAppendOnlyTable). Keeping it outside tables avoids allocating two
	// small maps for every ordinary INSERT while preserving the same in-place
	// restore path.
	appendOnly *appendOnlyTableState
	// rowUpdate is the compact rollback state for a triggerless UPDATE whose
	// constraint-index seek bounds the only rows it can replace.
	rowUpdate *rowUpdateTableState
	// rowDelete is the compact rollback state for a triggerless point DELETE
	// on a table without secondary indexes.
	rowDelete *rowDeleteTableState
	// full is false for a table-scoped snapshot. Such snapshots restore only
	// the table that the statement can mutate, leaving unrelated tables in
	// place. This avoids cloning an entire database for ordinary DML.
	full bool
}

type tableState struct {
	table *Table
	state *Table
}

// TableRef names one table for a caller that has to act on it by name, such as
// the executor purging its name-keyed caches after a rollback.
type TableRef struct {
	Tenant string
	Table  string
}

// RestoredTables reports the tables RestoreStatementSnapshot can put back.
// ok is false for a whole-database snapshot, where the caller must assume any
// table may have changed.
//
// It exists so a failed statement's cache cleanup is scoped to what that
// statement could actually have touched. The executor used to walk every
// tenant and every table on any failure and drop each one's constraint,
// vector and geo caches — so one rejected INSERT threw away warm caches
// belonging to tables it never looked at, in the process, for every other
// caller. A snapshot narrow enough to restore one table is by construction
// narrow enough to have changed only that one: the compact and table-scoped
// shapes are only chosen when no trigger and no foreign-key action can write
// elsewhere.
func (s *StatementSnapshot) RestoredTables() ([]TableRef, bool) {
	if s == nil {
		return nil, true
	}
	switch {
	case s.appendOnly != nil:
		return []TableRef{{Tenant: s.tenant, Table: s.appendOnly.table.Name}}, true
	case s.rowUpdate != nil:
		return []TableRef{{Tenant: s.tenant, Table: s.rowUpdate.table.Name}}, true
	case s.rowDelete != nil:
		return []TableRef{{Tenant: s.tenant, Table: s.rowDelete.table.Name}}, true
	case s.full:
		return nil, false
	}
	refs := make([]TableRef, 0, len(s.tables))
	for tenant, tables := range s.tables {
		for _, saved := range tables {
			refs = append(refs, TableRef{Tenant: tenant, Table: saved.table.Name})
		}
	}
	return refs, true
}

// catalogRollback is the catalog half of a StatementSnapshot, captured lazily.
//
// The catalog copy it holds is expensive out of proportion to how often it is
// needed: catalogToDisk deep-copies every table, column, view, index,
// function, job, trigger and RBAC record, then sorts ten slices — work
// proportional to the size of the whole database, paid by every mutating
// statement, to undo a change that the overwhelming majority of statements
// never make. An ordinary INSERT/UPDATE/DELETE touches the catalog only
// through MarkMaterializedViewsStaleByDependency, which now returns without
// taking the write lock unless a materialized view actually depends on the
// table.
//
// So the copy is deferred to the first catalog mutation instead. Arming
// installs this on the CatalogManager; CatalogManager.lockWrite and
// lockRBACWrite — the pair every single mutator funnels through — call
// capture() before applying their change. What capture() then reads is by
// construction the pre-statement catalog: it runs before the first mutation.
type catalogRollback struct {
	catalog *CatalogManager
	mu      sync.Mutex
	taken   bool
	// state is allocated only in capture(), not when the rollback point is
	// armed: diskCatalog is a ~240+ byte struct (eight slice headers, a map,
	// an int64 and a pointer), and arming happens on every mutating
	// statement while capture() -- the deep copy -- runs for only the small
	// minority that actually touch the catalog. A pointer keeps the common
	// case (armed, never captured) allocation-free.
	state *diskCatalog
}

// armCatalogRollback installs a fresh, uncaptured rollback point on c.
//
// Only one can be armed at a time, which is all a statement needs:
// executeStatement takes at most one snapshot per statement and holds the
// database's content write lock for the statement's whole duration. Arming
// overwrites rather than stacking, so a snapshot that was somehow never
// released cannot pin the catalog into copying forever.
func armCatalogRollback(c *CatalogManager) *catalogRollback {
	if c == nil {
		return nil
	}
	cr := &catalogRollback{catalog: c}
	c.pending.Store(cr)
	return cr
}

// capture takes the catalog copy, once. Callers reach it from lockWrite
// before c.mu is held, which is required: catalogToDisk read-locks c.mu.
func (cr *catalogRollback) capture() {
	cr.mu.Lock()
	defer cr.mu.Unlock()
	if cr.taken {
		return
	}
	dc := catalogToDisk(cr.catalog)
	cr.state = &dc
	cr.taken = true
}

// release disarms this rollback point so later statements' catalog writes are
// not captured into it. It is idempotent, and deliberately only clears the
// slot when it still holds cr.
func (cr *catalogRollback) release() {
	if cr == nil || cr.catalog == nil {
		return
	}
	cr.catalog.pending.CompareAndSwap(cr, nil)
}

// captured reports whether the statement mutated the catalog at all, i.e.
// whether there is anything to roll back.
func (cr *catalogRollback) captured() bool {
	if cr == nil {
		return false
	}
	cr.mu.Lock()
	defer cr.mu.Unlock()
	return cr.taken
}

// ReleaseStatementSnapshot discards a snapshot that will not be used, because
// its statement succeeded. Callers must invoke it exactly once per snapshot
// they take, whether or not they also called RestoreStatementSnapshot: it is
// what stops the next statement's catalog writes from being captured into
// this statement's rollback point.
func (db *DB) ReleaseStatementSnapshot(snapshot *StatementSnapshot) {
	if snapshot == nil {
		return
	}
	snapshot.catalog.release()
}

// captureDirtyRows returns the rollback value for Table.dirtyRows.
//
// It deliberately keeps the slice header rather than copying the elements.
// The list is append-only within one dirty window: MarkRowUpdated only ever
// appends, and MarkDirtyFrom only ever replaces it with nil. So whatever the
// statement does, the first len(rows) elements of this backing array still
// hold the pre-statement contents when the rollback runs — an append either
// wrote past them or reallocated away from them entirely. Copying them was an
// O(dirty rows) allocation on every mutating statement, which made a run of
// UPDATEs against the same table quadratic.
func captureDirtyRows(t *Table) []int { return t.dirtyRows }

// appendOnlyTableState is the minimal rollback point for an INSERT that can
// only append rows. It deliberately avoids copying existing rows; callers
// must ensure that no secondary index or other side effect is mutated.
type appendOnlyTableState struct {
	table          *Table
	rowCount       int
	version        int
	stats          *TableStats
	dirtyFrom      int
	dirtyRows      []int
	dirtyRowsState dirtyRowsState
}

// rowUpdateTableState stores only the rows a point UPDATE may replace plus the
// table metadata changed by every successful UPDATE. Secondary indexes are
// restored row-locally from these pre-update rows.
type rowUpdateTableState struct {
	table          *Table
	rowIDs         []int
	rows           [][]any
	version        int
	stats          *TableStats
	dirtyFrom      int
	dirtyRows      []int
	dirtyRowsState dirtyRowsState
}

type rowDeleteTableState struct {
	table          *Table
	rowID          int
	row            []any
	rowCount       int
	version        int
	stats          *TableStats
	dirtyFrom      int
	dirtyRows      []int
	dirtyRowsState dirtyRowsState
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
		catalog: armCatalogRollback(db.Catalog()),
		full:    true,
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

// SnapshotForTableStatement captures one table and the catalog state needed
// to roll back a statement known to mutate only that table. Callers must use
// SnapshotForStatement instead when triggers or foreign-key actions could
// affect other tables. The caller must already hold DB's content write lock.
func (db *DB) SnapshotForTableStatement(tenant, name string) (*StatementSnapshot, error) {
	if db == nil {
		return nil, nil
	}
	table, err := db.Get(tenant, name)
	if err != nil {
		return nil, err
	}
	key := strings.ToLower(table.Name)
	return &StatementSnapshot{
		tables: map[string]map[string]tableState{
			tenant: {key: {table: table, state: cloneTable(table)}},
		},
		catalog: armCatalogRollback(db.Catalog()),
	}, nil
}

// SnapshotForAppendOnlyTableStatement captures the lightweight rollback state
// for a statement that can only append rows to one table. It is intended for
// the executor's triggerless INSERT fast path (see appendOnlySnapshotTarget in
// package engine) and works whether or not the table carries secondary
// indexes -- restoreAppendOnlyTable removes an appended row's index entries
// before truncating it away. Other callers must use SnapshotForTableStatement
// or SnapshotForStatement.
func (db *DB) SnapshotForAppendOnlyTableStatement(tenant, name string) (*StatementSnapshot, error) {
	if db == nil {
		return nil, nil
	}
	table, err := db.Get(tenant, name)
	if err != nil {
		return nil, err
	}
	return &StatementSnapshot{
		tenant: tenant,
		appendOnly: &appendOnlyTableState{
			table:          table,
			rowCount:       len(table.Rows),
			version:        table.Version,
			stats:          cloneTableStats(table.Stats),
			dirtyFrom:      table.dirtyFrom,
			dirtyRows:      captureDirtyRows(table),
			dirtyRowsState: table.dirtyRowsState,
		},
		catalog: armCatalogRollback(db.Catalog()),
	}, nil
}

// SnapshotForRowUpdateStatement captures a lightweight rollback point for an
// UPDATE whose candidate row IDs are already known through a PRIMARY KEY or
// UNIQUE constraint seek. Callers must use a table/full snapshot when triggers
// or foreign-key actions can mutate state beyond these rows. Secondary-index
// entries for the bounded rows are restored directly on rollback.
func (db *DB) SnapshotForRowUpdateStatement(tenant, name string, rowIDs []int) (*StatementSnapshot, error) {
	if db == nil {
		return nil, nil
	}
	table, err := db.Get(tenant, name)
	if err != nil {
		return nil, err
	}
	rows := make([][]any, len(rowIDs))
	for i, rowID := range rowIDs {
		if rowID < 0 || rowID >= len(table.Rows) {
			return nil, fmt.Errorf("row %d out of range for table %q", rowID, table.Name)
		}
		rows[i] = table.Rows[rowID]
	}
	return &StatementSnapshot{
		tenant: tenant,
		rowUpdate: &rowUpdateTableState{
			table:  table,
			rowIDs: append([]int(nil), rowIDs...),
			// UPDATE replaces row slices instead of mutating their cells in place,
			// so these original headers are an immutable rollback image.
			rows:           rows,
			version:        table.Version,
			stats:          cloneTableStats(table.Stats),
			dirtyFrom:      table.dirtyFrom,
			dirtyRows:      captureDirtyRows(table),
			dirtyRowsState: table.dirtyRowsState,
		},
		catalog: armCatalogRollback(db.Catalog()),
	}, nil
}

// SnapshotForRowDeleteStatement captures the single candidate row and table
// metadata needed to undo a PRIMARY KEY/UNIQUE point delete. The caller must
// use a table/full snapshot when triggers, foreign keys, or secondary indexes
// can observe or mutate additional state.
func (db *DB) SnapshotForRowDeleteStatement(tenant, name string, rowIDs []int) (*StatementSnapshot, error) {
	if db == nil {
		return nil, nil
	}
	if len(rowIDs) > 1 {
		return nil, fmt.Errorf("row-delete snapshot for table %q has %d candidates, want at most one", name, len(rowIDs))
	}
	table, err := db.Get(tenant, name)
	if err != nil {
		return nil, err
	}
	rowID := -1
	var row []any
	if len(rowIDs) == 1 {
		rowID = rowIDs[0]
		if rowID < 0 || rowID >= len(table.Rows) {
			return nil, fmt.Errorf("row %d out of range for table %q", rowID, table.Name)
		}
		row = cloneRows([][]any{table.Rows[rowID]})[0]
	}
	return &StatementSnapshot{
		tenant: tenant,
		rowDelete: &rowDeleteTableState{
			table:          table,
			rowID:          rowID,
			row:            row,
			rowCount:       len(table.Rows),
			version:        table.Version,
			stats:          cloneTableStats(table.Stats),
			dirtyFrom:      table.dirtyFrom,
			dirtyRows:      captureDirtyRows(table),
			dirtyRowsState: table.dirtyRowsState,
		},
		catalog: armCatalogRollback(db.Catalog()),
	}, nil
}

// RestoreStatementSnapshot rolls a database back to snapshot. It restores
// pre-existing tables in place and removes tables created by the failed
// statement. The caller must hold DB's content write lock.
func (db *DB) RestoreStatementSnapshot(snapshot *StatementSnapshot) {
	if db == nil || snapshot == nil {
		return
	}

	db.mu.Lock()
	if snapshot.appendOnly != nil {
		state := snapshot.appendOnly
		if db.IsTablePinnedForStream(state.table) {
			// A stream can still read state.table after a statement detached it
			// through copy-on-write. Restore into a fresh copy instead of
			// truncating that stream's immutable source in place.
			restored := cloneTable(state.table)
			copyState := *state
			copyState.table = restored
			restoreAppendOnlyTable(&copyState)
			db.getTenant(snapshot.tenant).tables[strings.ToLower(restored.Name)] = restored
		} else {
			restoreAppendOnlyTable(state)
		}
	} else if snapshot.rowUpdate != nil {
		state := snapshot.rowUpdate
		if db.IsTablePinnedForStream(state.table) {
			restored := cloneTable(state.table)
			copyState := *state
			copyState.table = restored
			copyState.rows = cloneRows(state.rows)
			restoreRowUpdateTable(&copyState)
			db.getTenant(snapshot.tenant).tables[strings.ToLower(restored.Name)] = restored
		} else {
			restoreRowUpdateTable(state)
		}
	} else if snapshot.rowDelete != nil {
		state := snapshot.rowDelete
		if db.IsTablePinnedForStream(state.table) {
			restored := cloneTable(state.table)
			copyState := *state
			copyState.table = restored
			if state.row != nil {
				copyState.row = cloneRows([][]any{state.row})[0]
			}
			restoreRowDeleteTable(&copyState)
			db.getTenant(snapshot.tenant).tables[strings.ToLower(restored.Name)] = restored
		} else {
			restoreRowDeleteTable(state)
		}
	} else if snapshot.full {
		restored := make(map[string]*tenantDB, len(snapshot.tables))
		for tenant, tables := range snapshot.tables {
			tenantDB := &tenantDB{tables: make(map[string]*Table, len(tables))}
			for name, saved := range tables {
				if db.IsTablePinnedForStream(saved.table) {
					// saved.table is an active stream source. saved.state is an
					// independent pre-image, so install a fresh copy and leave the
					// pinned table byte-for-byte untouched.
					tenantDB.tables[name] = cloneTable(saved.state)
				} else {
					restoreStatementTable(saved)
					tenantDB.tables[name] = saved.table
				}
			}
			restored[tenant] = tenantDB
		}
		db.tenants = restored
	} else {
		for tenant, tables := range snapshot.tables {
			tenantDB := db.getTenant(tenant)
			for name, saved := range tables {
				if db.IsTablePinnedForStream(saved.table) {
					tenantDB.tables[name] = cloneTable(saved.state)
				} else {
					restoreStatementTable(saved)
					tenantDB.tables[name] = saved.table
				}
			}
		}
	}
	db.mu.Unlock()

	// CatalogManager has its own lock and includes materialized-view stale
	// state changed by DML. Reconstructing it from the deep-copy disk form is
	// less error-prone than selectively undoing each catalog side effect.
	//
	// Nothing to reconstruct unless the statement actually mutated the catalog:
	// the copy is taken on the first such mutation (see catalogRollback), so
	// "not captured" means the catalog is still exactly as this statement
	// found it. Skipping the replacement in that case is not only cheaper, it
	// also leaves the *CatalogManager pointer intact — which matters to any
	// long-lived holder of one, such as Scheduler.
	//
	// The revision counter must keep moving forward across a replacement that
	// does happen: a rollback is itself a change, and a caller comparing
	// revisions to decide whether it has catalog state to commit (see
	// conn.commitTx) would otherwise see the counter reset to zero and
	// conclude nothing happened.
	if snapshot.catalog.captured() {
		restored := diskToCatalog(*snapshot.catalog.state)
		restored.setRevision(db.Catalog().Revision() + 1)
		db.setCatalog(restored)
	}
}

func restoreStatementTable(saved tableState) {
	restoreTable(saved.table, saved.state)
}

func restoreAppendOnlyTable(state *appendOnlyTableState) {
	if state == nil || state.table == nil {
		return
	}
	table := state.table
	removeAppendedIndexEntries(table, state.rowCount)
	table.dropDerived()
	table.Rows = table.Rows[:state.rowCount:state.rowCount]
	table.Version = state.version
	table.Stats = cloneTableStats(state.stats)
	table.dirtyFrom = state.dirtyFrom
	table.dirtyRows = state.dirtyRows
	table.dirtyRowsState = state.dirtyRowsState
}

// removeAppendedIndexEntries undoes InsertSecondaryIndexRow for every row
// appended since rowCount, so a rolled-back append-only INSERT never leaves a
// materialized secondary index pointing at rows about to be truncated away
// (a stale entry would otherwise alias whatever unrelated row a future
// INSERT happens to place at that same position).
//
// This only runs on the rollback path — a failed multi-row INSERT, most
// commonly a later row violating a UNIQUE secondary index against an earlier
// row in the same statement — which the ordinary successful commit never
// reaches. It costs O(rows this statement appended × indexes), not O(table
// size): appendOnlySnapshotTarget (package engine) now allows this fast path
// for any triggerless INSERT regardless of secondary indexes specifically
// because this makes that trade safe, in exchange for the previous
// alternative of a full cloneTable/cloneRows copy on every single INSERT
// into an indexed table, successful or not.
func removeAppendedIndexEntries(table *Table, rowCount int) {
	if len(table.Indexes) == 0 || len(table.Rows) <= rowCount {
		return
	}
	appended := table.Rows[rowCount:]
	for _, idx := range table.Indexes {
		// hydrate() takes idx.mu, so call it once per index rather than once
		// per (index, row) pair: the skip list it returns cannot change out
		// from under this loop, since the caller holds DB's write lock for
		// the whole statement.
		fast := idx.hydrate()
		for i, row := range appended {
			key, err := table.indexKey(idx.Columns, row)
			if err != nil {
				// The row was never actually indexed under a key that no
				// longer resolves (e.g. a column dropped mid-rollback is not
				// a real scenario here, but indexKey's contract already
				// returns an error rather than panic) — nothing to remove.
				continue
			}
			fast.Remove(key, rowCount+i)
		}
	}
}

func restoreRowUpdateTable(state *rowUpdateTableState) {
	if state == nil || state.table == nil {
		return
	}
	table := state.table
	table.dropDerived()
	indexNames := table.SortedIndexNames()
	for i, rowID := range state.rowIDs {
		if len(indexNames) > 0 {
			_ = table.UpdateSecondaryIndexRow(rowID, table.Rows[rowID], state.rows[i], indexNames)
		}
		table.Rows[rowID] = state.rows[i]
	}
	table.Version = state.version
	table.Stats = cloneTableStats(state.stats)
	table.dirtyFrom = state.dirtyFrom
	table.dirtyRows = state.dirtyRows
	table.dirtyRowsState = state.dirtyRowsState
}

func restoreRowDeleteTable(state *rowDeleteTableState) {
	if state == nil || state.table == nil {
		return
	}
	table := state.table
	table.dropDerived()
	if state.rowID >= 0 {
		switch len(table.Rows) {
		case state.rowCount - 1:
			// The point DELETE this undoes removed state.rowID via
			// swap-and-pop: it moved what was then the last row into
			// state.rowID's slot and truncated. Undo that exactly: grow the
			// slice back to its original length, move the row now sitting
			// at state.rowID back out to the end (where it lived before the
			// delete), then restore the deleted row at state.rowID. When
			// state.rowID was itself the last row, "moving it back to the
			// end" and "restoring it at state.rowID" are the same slot, so
			// this degrades to a plain append+overwrite.
			table.Rows = append(table.Rows, nil)
			table.Rows[state.rowCount-1] = table.Rows[state.rowID]
			table.Rows[state.rowID] = state.row
		case state.rowCount:
			table.Rows[state.rowID] = state.row
		}
	}
	table.Version = state.version
	table.Stats = cloneTableStats(state.stats)
	table.dirtyFrom = state.dirtyFrom
	table.dirtyRows = state.dirtyRows
	table.dirtyRowsState = state.dirtyRowsState
}

func restoreTable(dst, saved *Table) {
	if dst == nil || saved == nil {
		return
	}
	// Executor state describes the rows being replaced, not the ones coming
	// back. It rebuilds on next use.
	dst.dropDerived()
	copy := cloneTable(saved)
	dst.Name = copy.Name
	dst.Cols = copy.Cols
	dst.Rows = copy.Rows
	dst.Indexes = copy.Indexes
	dst.FTSIndexes = copy.FTSIndexes
	dst.ftsGeneration = copy.ftsGeneration
	dst.ftsPersistedGeneration = copy.ftsPersistedGeneration
	dst.VectorIndexes = copy.VectorIndexes
	dst.vectorGeneration = copy.vectorGeneration
	dst.vectorPersistedGeneration = copy.vectorPersistedGeneration
	dst.IsTemp = copy.IsTemp
	dst.colPos = copy.colPos
	dst.Version = copy.Version
	dst.structVersion = copy.structVersion
	dst.Stats = copy.Stats
	dst.dirtyFrom = copy.dirtyFrom
	dst.dirtyRows = copy.dirtyRows
	dst.dirtyRowsState = copy.dirtyRowsState
}
