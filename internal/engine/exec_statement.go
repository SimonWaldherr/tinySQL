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

	var snapshot *storage.StatementSnapshot
	if isAtomicDML(stmt) {
		var snapshotErr error
		if table, ok := appendOnlySnapshotTarget(db, tenant, stmt); ok {
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
				purgeVecQueryCacheFor(rollbackTenant, table.Name)
			}
		}
	}()
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("internal error executing statement: %v", r)
		}
	}()

	statementWAL := newStatementWAL(db.AdvancedWAL())
	rs, err = execStmt(ExecEnv{ctx: ctx, tenant: tenant, db: db, statementWAL: statementWAL, now: time.Now()}, stmt)
	if err == nil {
		err = statementWAL.commit()
	}
	if err == nil {
		err = maybeLogToWALManager(db, snapshot)
	}
	return rs, err
}

// appendOnlySnapshotTarget identifies the narrow INSERT fast path whose
// failed execution can be rolled back by truncating appended rows. Secondary
// indexes can be changed as rows are inserted, so they retain a cloned-table
// snapshot. The same is true for every trigger-capable statement.
func appendOnlySnapshotTarget(db *storage.DB, tenant string, stmt Statement) (string, bool) {
	s, ok := stmt.(*Insert)
	if !ok || db.WAL() != nil {
		return "", false
	}
	catalog := db.Catalog()
	if len(catalog.GetTriggers(s.Table, storage.TriggerTiming("BEFORE"), storage.TriggerEvent("INSERT"))) > 0 ||
		len(catalog.GetTriggers(s.Table, storage.TriggerTiming("AFTER"), storage.TriggerEvent("INSERT"))) > 0 {
		return "", false
	}
	table, err := db.Get(tenant, s.Table)
	if err != nil || len(table.Indexes) > 0 {
		return "", false
	}
	return s.Table, true
}

// tableScopedSnapshotTarget identifies DML that cannot mutate a table other
// than its target. In that common case a table-scoped rollback point avoids
// cloning every table on each statement. Triggers and FK cascades can write
// elsewhere, so they deliberately retain the full-database snapshot.
func tableScopedSnapshotTarget(db *storage.DB, tenant string, stmt Statement) (string, bool) {
	// WALManager derives its persistence record by comparing the complete
	// pre-statement database with the current one. Keep its complete snapshot
	// so unrelated tables are not mistaken for newly-created WAL entries.
	if db.WAL() != nil {
		return "", false
	}
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
	if len(catalog.GetTriggers(table, storage.TriggerTiming("BEFORE"), event)) > 0 ||
		len(catalog.GetTriggers(table, storage.TriggerTiming("AFTER"), event)) > 0 {
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
