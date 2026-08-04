package engine

import (
	"context"
	"fmt"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// executeStatement owns the statement-level infrastructure: authorization,
// content locking, atomic-DML rollback, panic isolation, auditing, and WAL
// finalization. Keeping it separate from execStmt lets statement handlers
// focus exclusively on their SQL semantics.
func executeStatement(ctx context.Context, db *storage.DB, tenant string, stmt Statement) (rs *ResultSet, err error) {
	if err := checkPermission(ctx, db, stmt); err != nil {
		recordAudit(ctx, db, tenant, stmt, err)
		return nil, err
	}
	if isReadOnlyStatement(stmt) {
		db.LockContentForRead()
		defer db.UnlockContentForRead()
	} else {
		db.LockContentForWrite()
		defer db.UnlockContentForWrite()
	}

	// walBefore is the pre-image WALManager logging diffs against. It is taken
	// for every mutating statement, not just DML, so that CREATE/DROP/ALTER
	// TABLE are logged too — a schema change that never reaches the log is
	// replayed onto a database that no longer has the table.
	var walBefore *storage.DB
	if !isReadOnlyStatement(stmt) && db.StatementWAL() != nil {
		walBefore = db.MetaSnapshot()
	}

	var snapshot *storage.StatementSnapshot
	switch {
	case isAtomicDML(stmt):
		var snapshotErr error
		if table, rowIDs, ok := rowUpdateSnapshotTarget(db, tenant, stmt); ok {
			snapshot, snapshotErr = db.SnapshotForRowUpdateStatement(tenant, table, rowIDs)
		} else if table, rowIDs, ok := rowDeleteSnapshotTarget(db, tenant, stmt); ok {
			snapshot, snapshotErr = db.SnapshotForRowDeleteStatement(tenant, table, rowIDs)
		} else if table, ok := appendOnlySnapshotTarget(db, tenant, stmt); ok {
			snapshot, snapshotErr = db.SnapshotForAppendOnlyTableStatement(tenant, table)
		} else if table, ok := tableScopedSnapshotTarget(db, tenant, stmt); ok {
			snapshot, snapshotErr = db.SnapshotForTableStatement(tenant, table)
		} else {
			snapshot = db.SnapshotForStatement()
		}
		if snapshotErr != nil {
			recordAudit(ctx, db, tenant, stmt, snapshotErr)
			return nil, snapshotErr
		}
	case walBefore != nil:
		// A statement that will be written to the WAL needs a rollback point
		// even when it is not DML: if the log append fails, the change must not
		// remain in memory, or this process would keep serving a table that
		// recovery will not reconstruct. DDL is rare enough that the full
		// snapshot is the right trade here.
		snapshot = db.SnapshotForStatement()
	}
	defer func() { recordAudit(ctx, db, tenant, stmt, err) }()
	defer func() {
		if err == nil || snapshot == nil {
			return
		}
		db.RestoreStatementSnapshot(snapshot)
		for _, rollbackTenant := range db.ListTenants() {
			for _, table := range db.ListTables(rollbackTenant) {
				invalidateConstraintIndexes(table)
				purgeVectorCachesFor(rollbackTenant, table.Name)
				purgeGeoGridCachesFor(rollbackTenant, table.Name)
				purgeVecQueryCacheFor(rollbackTenant, table.Name)
			}
		}
	}()
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("internal error executing statement: %v", r)
		}
	}()

	statementWAL := newStatementWAL(db)
	rs, err = execStmt(ExecEnv{ctx: ctx, tenant: tenant, db: db, statementWAL: statementWAL, now: time.Now()}, stmt)
	if err == nil {
		err = statementWAL.commit()
	}
	if err == nil {
		err = maybeLogToWALManager(db, walBefore)
	}
	return rs, err
}

// appendOnlySnapshotTarget identifies the narrow INSERT fast path whose
// failed execution can be rolled back by truncating appended rows. Secondary
// indexes can be changed as rows are inserted, so they retain a cloned-table
// snapshot. The same is true for every trigger-capable statement.
func appendOnlySnapshotTarget(db *storage.DB, tenant string, stmt Statement) (string, bool) {
	s, ok := stmt.(*Insert)
	if !ok {
		return "", false
	}
	catalog := db.Catalog()
	before, after := catalog.GetTriggersForEvent(s.Table, storage.TriggerInsert)
	if len(before) > 0 || len(after) > 0 {
		return "", false
	}
	table, err := db.Get(tenant, s.Table)
	if err != nil || len(table.Indexes) > 0 {
		return "", false
	}
	return s.Table, true
}

// rowUpdateSnapshotTarget identifies a point UPDATE whose constraint-index
// candidate set bounds every row the statement can replace. The compact
// snapshot is safe only when no trigger/FK can write elsewhere and none of the
// assigned columns belongs to a secondary index.
func rowUpdateSnapshotTarget(db *storage.DB, tenant string, stmt Statement) (string, []int, bool) {
	s, ok := stmt.(*Update)
	if !ok || tenantHasAnyForeignKeys(ExecEnv{tenant: tenant, db: db}) {
		return "", nil, false
	}
	before, after := db.Catalog().GetTriggersForEvent(s.Table, storage.TriggerUpdate)
	if len(before) > 0 || len(after) > 0 {
		return "", nil, false
	}
	table, err := db.Get(tenant, s.Table)
	if err != nil || updateTouchesSecondaryIndex(table, s) {
		return "", nil, false
	}
	colIndex := simpleColumnIndex(table, s.Table)
	rowIDs, _, _, found := selectConstraintIndex(table, colIndex, s.Where)
	if !found {
		return "", nil, false
	}
	return s.Table, rowIDs, true
}

func updateTouchesSecondaryIndex(table *storage.Table, s *Update) bool {
	if table == nil || len(table.Indexes) == 0 {
		return false
	}
	updated := make(map[int]struct{}, len(s.Sets))
	for name := range s.Sets {
		if col, err := table.ColIndex(name); err == nil {
			updated[col] = struct{}{}
		}
	}
	for _, index := range table.Indexes {
		for _, name := range index.Columns {
			if col, err := table.ColIndex(name); err == nil {
				if _, ok := updated[col]; ok {
					return true
				}
			}
		}
	}
	return false
}

// rowDeleteSnapshotTarget identifies a single-row DELETE whose rollback can
// reinsert one saved row instead of cloning the full table.
func rowDeleteSnapshotTarget(db *storage.DB, tenant string, stmt Statement) (string, []int, bool) {
	s, ok := stmt.(*Delete)
	if !ok || tenantHasAnyForeignKeys(ExecEnv{tenant: tenant, db: db}) {
		return "", nil, false
	}
	before, after := db.Catalog().GetTriggersForEvent(s.Table, storage.TriggerDelete)
	if len(before) > 0 || len(after) > 0 {
		return "", nil, false
	}
	table, err := db.Get(tenant, s.Table)
	if err != nil || len(table.Indexes) > 0 {
		return "", nil, false
	}
	colIndex := simpleColumnIndex(table, s.Table)
	rowIDs, _, _, found := selectDeleteConstraintRows(table, colIndex, s.Where)
	if !found || len(rowIDs) > 1 {
		return "", nil, false
	}
	return s.Table, rowIDs, true
}

// tableScopedSnapshotTarget identifies DML that cannot mutate a table other
// than its target. In that common case a table-scoped rollback point avoids
// cloning every table on each statement. Triggers and FK cascades can write
// elsewhere, so they deliberately retain the full-database snapshot.
func tableScopedSnapshotTarget(db *storage.DB, tenant string, stmt Statement) (string, bool) {
	var table string
	var event storage.TriggerEvent
	switch s := stmt.(type) {
	case *Insert:
		table, event = s.Table, storage.TriggerEvent("INSERT")
	case *Update:
		if tenantHasAnyForeignKeys(ExecEnv{tenant: tenant, db: db}) {
			return "", false
		}
		table, event = s.Table, storage.TriggerEvent("UPDATE")
	case *Delete:
		if tenantHasAnyForeignKeys(ExecEnv{tenant: tenant, db: db}) {
			return "", false
		}
		table, event = s.Table, storage.TriggerEvent("DELETE")
	default:
		return "", false
	}
	catalog := db.Catalog()
	before, after := catalog.GetTriggersForEvent(table, event)
	if len(before) > 0 || len(after) > 0 {
		return "", false
	}
	return table, true
}

func isAtomicDML(stmt Statement) bool {
	switch s := stmt.(type) {
	case *Insert, *Update, *Delete:
		return true
	case *Explain:
		// EXPLAIN ANALYZE executes its inner statement in the outer statement
		// lifecycle, so it needs the same rollback guarantee as direct DML.
		return s.Analyze && isAtomicDML(s.Statement)
	default:
		return false
	}
}

// recordAudit appends one entry to db's audit log, if one is attached.
func recordAudit(ctx context.Context, db *storage.DB, tenant string, stmt Statement, err error) {
	log := db.AuditLog()
	if log == nil {
		return
	}
	text, ok := auditTextFromContext(ctx)
	if !ok {
		text = fmt.Sprintf("<%T>", stmt)
	}
	user, _ := UserFromContext(ctx)
	errMsg := ""
	if err != nil {
		errMsg = err.Error()
	}
	log.Append(tenant, user, text, err == nil, errMsg)
}
