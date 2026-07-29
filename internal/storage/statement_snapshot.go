package storage

import (
	"fmt"
	"strings"
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
	tables  map[string]map[string]tableState
	catalog diskCatalog
	// appendOnly is the compact rollback state for a triggerless, index-free
	// INSERT. Keeping it outside tables avoids allocating two small maps for
	// every ordinary INSERT while preserving the same in-place restore path.
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
// table metadata changed by every successful UPDATE. The caller guarantees
// that the statement cannot mutate a secondary index.
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
		catalog: catalogToDisk(db.Catalog()),
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
		catalog: catalogToDisk(db.Catalog()),
	}, nil
}

// SnapshotForAppendOnlyTableStatement captures the lightweight rollback state
// for a statement that can only append rows to one table. It is intended for
// the executor's triggerless, index-free INSERT fast path; other callers must
// use SnapshotForTableStatement or SnapshotForStatement.
func (db *DB) SnapshotForAppendOnlyTableStatement(tenant, name string) (*StatementSnapshot, error) {
	if db == nil {
		return nil, nil
	}
	table, err := db.Get(tenant, name)
	if err != nil {
		return nil, err
	}
	return &StatementSnapshot{
		appendOnly: &appendOnlyTableState{
			table:          table,
			rowCount:       len(table.Rows),
			version:        table.Version,
			stats:          cloneTableStats(table.Stats),
			dirtyFrom:      table.dirtyFrom,
			dirtyRows:      append([]int(nil), table.dirtyRows...),
			dirtyRowsState: table.dirtyRowsState,
		},
		catalog: catalogToDisk(db.Catalog()),
	}, nil
}

// SnapshotForRowUpdateStatement captures a lightweight rollback point for an
// UPDATE whose candidate row IDs are already known through a PRIMARY KEY or
// UNIQUE constraint seek. Callers must use a table/full snapshot when triggers,
// foreign-key actions, or an assignment to a secondary-indexed column can
// mutate state beyond these rows.
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
		rowUpdate: &rowUpdateTableState{
			table:          table,
			rowIDs:         append([]int(nil), rowIDs...),
			rows:           cloneRows(rows),
			version:        table.Version,
			stats:          cloneTableStats(table.Stats),
			dirtyFrom:      table.dirtyFrom,
			dirtyRows:      append([]int(nil), table.dirtyRows...),
			dirtyRowsState: table.dirtyRowsState,
		},
		catalog: catalogToDisk(db.Catalog()),
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
		rowDelete: &rowDeleteTableState{
			table:          table,
			rowID:          rowID,
			row:            row,
			rowCount:       len(table.Rows),
			version:        table.Version,
			stats:          cloneTableStats(table.Stats),
			dirtyFrom:      table.dirtyFrom,
			dirtyRows:      append([]int(nil), table.dirtyRows...),
			dirtyRowsState: table.dirtyRowsState,
		},
		catalog: catalogToDisk(db.Catalog()),
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
		restoreAppendOnlyTable(snapshot.appendOnly)
	} else if snapshot.rowUpdate != nil {
		restoreRowUpdateTable(snapshot.rowUpdate)
	} else if snapshot.rowDelete != nil {
		restoreRowDeleteTable(snapshot.rowDelete)
	} else if snapshot.full {
		restored := make(map[string]*tenantDB, len(snapshot.tables))
		for tenant, tables := range snapshot.tables {
			tenantDB := &tenantDB{tables: make(map[string]*Table, len(tables))}
			for name, saved := range tables {
				restoreStatementTable(saved)
				tenantDB.tables[name] = saved.table
			}
			restored[tenant] = tenantDB
		}
		db.tenants = restored
	} else {
		for tenant, tables := range snapshot.tables {
			tenantDB := db.getTenant(tenant)
			for name, saved := range tables {
				restoreStatementTable(saved)
				tenantDB.tables[name] = saved.table
			}
		}
	}
	db.mu.Unlock()

	// CatalogManager has its own lock and includes materialized-view stale
	// state changed by DML. Reconstructing it from the deep-copy disk form is
	// less error-prone than selectively undoing each catalog side effect.
	//
	// The revision counter must keep moving forward across the replacement: a
	// rollback is itself a change, and a caller comparing revisions to decide
	// whether it has catalog state to commit (see conn.commitTx) would
	// otherwise see the counter reset to zero and conclude nothing happened.
	restored := diskToCatalog(snapshot.catalog)
	restored.setRevision(db.Catalog().Revision() + 1)
	db.setCatalog(restored)
}

func restoreStatementTable(saved tableState) {
	restoreTable(saved.table, saved.state)
}

func restoreAppendOnlyTable(state *appendOnlyTableState) {
	if state == nil || state.table == nil {
		return
	}
	table := state.table
	table.Rows = table.Rows[:state.rowCount:state.rowCount]
	table.Version = state.version
	table.Stats = cloneTableStats(state.stats)
	table.dirtyFrom = state.dirtyFrom
	table.dirtyRows = append([]int(nil), state.dirtyRows...)
	table.dirtyRowsState = state.dirtyRowsState
}

func restoreRowUpdateTable(state *rowUpdateTableState) {
	if state == nil || state.table == nil {
		return
	}
	table := state.table
	for i, rowID := range state.rowIDs {
		table.Rows[rowID] = state.rows[i]
	}
	table.Version = state.version
	table.Stats = cloneTableStats(state.stats)
	table.dirtyFrom = state.dirtyFrom
	table.dirtyRows = append([]int(nil), state.dirtyRows...)
	table.dirtyRowsState = state.dirtyRowsState
}

func restoreRowDeleteTable(state *rowDeleteTableState) {
	if state == nil || state.table == nil {
		return
	}
	table := state.table
	if state.rowID >= 0 {
		switch len(table.Rows) {
		case state.rowCount - 1:
			table.Rows = table.Rows[:state.rowCount]
			copy(table.Rows[state.rowID+1:], table.Rows[state.rowID:state.rowCount-1])
			table.Rows[state.rowID] = state.row
		case state.rowCount:
			table.Rows[state.rowID] = state.row
		}
	}
	table.Version = state.version
	table.Stats = cloneTableStats(state.stats)
	table.dirtyFrom = state.dirtyFrom
	table.dirtyRows = append([]int(nil), state.dirtyRows...)
	table.dirtyRowsState = state.dirtyRowsState
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
	dst.dirtyRows = copy.dirtyRows
	dst.dirtyRowsState = copy.dirtyRowsState
}
