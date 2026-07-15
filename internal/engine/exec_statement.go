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
	return rs, err
}

func isAtomicDML(stmt Statement) bool {
	switch stmt.(type) {
	case *Insert, *Update, *Delete:
		return true
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
