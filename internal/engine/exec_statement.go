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

	var planStorage dmlPlan
	plan := newDMLPlan(&planStorage, db, tenant, stmt)

	var snapshot *storage.StatementSnapshot
	switch {
	case isAtomicDML(stmt):
		var snapshotErr error
		if table, rowIDs, ok := rowUpdateSnapshotTarget(plan); ok {
			snapshot, snapshotErr = db.SnapshotForRowUpdateStatement(tenant, table, rowIDs)
		} else if table, rowIDs, ok := rowDeleteSnapshotTarget(plan); ok {
			snapshot, snapshotErr = db.SnapshotForRowDeleteStatement(tenant, table, rowIDs)
		} else if table, ok := appendOnlySnapshotTarget(plan); ok {
			snapshot, snapshotErr = db.SnapshotForAppendOnlyTableStatement(tenant, table)
		} else if table, ok := tableScopedSnapshotTarget(plan); ok {
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
	// Runs after the rollback defer below (defers unwind last-in-first-out), so
	// the snapshot is only disarmed once it can no longer be needed. Without
	// it, a statement's armed catalog rollback point would still be installed
	// when the next statement mutated the catalog, and would capture that
	// statement's pre-image instead. See storage.ReleaseStatementSnapshot.
	defer db.ReleaseStatementSnapshot(snapshot)
	defer func() { recordAudit(ctx, db, tenant, stmt, err) }()
	defer func() {
		if err == nil || snapshot == nil {
			return
		}
		db.RestoreStatementSnapshot(snapshot)
		purgeCachesAfterRollback(db, snapshot)
	}()
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("internal error executing statement: %v", r)
		}
	}()

	statementWAL := newStatementWAL(db)
	rs, err = execStmt(ExecEnv{ctx: ctx, tenant: tenant, db: db, statementWAL: statementWAL, now: time.Now(), subqueryCache: newSubqueryResultCache(), dml: plan}, stmt)
	if err == nil {
		err = statementWAL.commit()
	}
	if err == nil {
		err = maybeLogToWALManager(db, walBefore)
	}
	return rs, err
}

// purgeCachesAfterRollback drops the derived state that a rolled-back
// statement may have built from rows that are no longer there: the
// constraint-value index, and the vector/geo/vector-query caches, which are
// keyed by name rather than by table pointer and so cannot notice the restore
// themselves.
//
// It is scoped to the tables the snapshot actually restored. A statement whose
// rollback point covers one table is, by construction, a statement that no
// trigger and no foreign-key action could have taken outside that table — the
// snapshot selectors above only choose those shapes under exactly that
// condition. Purging everything instead meant one rejected INSERT threw away
// every other table's warm caches, for every caller in the process.
func purgeCachesAfterRollback(db *storage.DB, snapshot *storage.StatementSnapshot) {
	refs, scoped := snapshot.RestoredTables()
	if !scoped {
		for _, rollbackTenant := range db.ListTenants() {
			for _, table := range db.ListTables(rollbackTenant) {
				invalidateConstraintIndexes(table)
				purgeVectorCachesFor(rollbackTenant, table.Name)
				purgeGeoGridCachesFor(rollbackTenant, table.Name)
				purgeVecQueryCacheFor(rollbackTenant, table.Name)
			}
		}
		return
	}
	for _, ref := range refs {
		if table, err := db.Get(ref.Tenant, ref.Table); err == nil {
			invalidateConstraintIndexes(table)
		}
		purgeVectorCachesFor(ref.Tenant, ref.Table)
		purgeGeoGridCachesFor(ref.Tenant, ref.Table)
		purgeVecQueryCacheFor(ref.Tenant, ref.Table)
	}
}

// dmlPlan is the planning work executeStatement has to do anyway, in order to
// choose the cheapest rollback snapshot for a mutating statement, kept so the
// DML handler that runs immediately afterwards reuses it instead of computing
// the same values a second time. Before it existed, a point UPDATE built its
// column index and ran its constraint-index seek twice per statement — once
// while picking a snapshot shape, once in buildSimpleUpdatePlan — which
// measured at roughly 30% of the statement's total cost.
//
// Reuse is sound because nothing between those two points can invalidate it:
// executeStatement holds the content write lock across both, and everything it
// does in between (the WAL metadata pre-image, taking the rollback snapshot,
// binding the statement WAL) only reads.
//
// Every field is an optimization with no semantics of its own. A consumer that
// finds no plan — nested DML dispatched through execStmt, a handler called
// directly by a test, or a field this statement shape does not precompute —
// computes the value itself exactly as before.
type dmlPlan struct {
	// stmt is the identity guard: see ExecEnv.planFor.
	stmt Statement
	// table is the statement's target table, or nil when it does not resolve.
	// The handler reports that lookup error itself, with its own wording.
	table *storage.Table
	// tenantFK is tenantHasAnyForeignKeys for this statement's tenant, which
	// costs a full ListTables (two allocations, a sort, and a backend
	// directory listing in the disk-backed modes) per call. tenantFKKnown is
	// false when the statement shape never asks for it, so a future consumer
	// that does cannot silently read the zero value as "no foreign keys".
	tenantFK      bool
	tenantFKKnown bool
	before, after []*storage.CatalogTrigger
	// colIndex and rowIDs are populated only for the statement shapes whose
	// handler is going to need them again; a nil colIndex means "not
	// precomputed". rowIDsFound mirrors the seek's own found result, which is
	// distinct from an empty candidate set (an indexed negative lookup).
	colIndex    map[string]int
	rowIDs      []int
	rowIDsFound bool
}

func (p *dmlPlan) hasTriggers() bool { return len(p.before) > 0 || len(p.after) > 0 }

// planTable returns the target table a plan already resolved, or nil when
// there is no plan or the lookup failed. A nil result means the caller must
// call db.Get itself — which is also how the lookup error reaches the caller
// with the wording it has always had.
func planTable(plan *dmlPlan) *storage.Table {
	if plan == nil {
		return nil
	}
	return plan.table
}

// planTriggers returns the statement's trigger lists, falling back to a
// catalog lookup when nothing was precomputed.
func planTriggers(plan *dmlPlan, env ExecEnv, table string, event storage.TriggerEvent) (before, after []*storage.CatalogTrigger) {
	if plan != nil {
		return plan.before, plan.after
	}
	return env.db.Catalog().GetTriggersForEvent(table, event)
}

// planColumnIndex returns the precomputed lower-cased column index for the
// statement's target table, or nil when the plan did not build one.
func planColumnIndex(plan *dmlPlan) map[string]int {
	if plan == nil {
		return nil
	}
	return plan.colIndex
}

// planConstraintRows returns the precomputed constraint-index candidate set
// and whether the seek found an index to use, falling back to seek() when the
// plan holds no precomputed answer. A non-nil colIndex is what marks the pair
// as precomputed: newDMLPlan only ever fills them in together.
func planConstraintRows(plan *dmlPlan, seek func() ([]int, bool)) ([]int, bool) {
	if plan != nil && plan.colIndex != nil {
		return plan.rowIDs, plan.rowIDsFound
	}
	return seek()
}

// planTenantHasForeignKeys answers tenantHasAnyForeignKeys for stmt, reusing
// the plan's answer when there is one. The uncached call lists every table in
// the tenant — two allocations and a sort, plus a backend directory listing in
// the disk-backed storage modes — so a statement that asks more than once pays
// for it more than once.
func planTenantHasForeignKeys(env ExecEnv, stmt Statement) bool {
	if plan := env.planFor(stmt); plan != nil && plan.tenantFKKnown {
		return plan.tenantFK
	}
	return tenantHasAnyForeignKeys(env)
}

// newDMLPlan precomputes the shared facts about a mutating statement into out,
// returning it. It returns nil for anything that is not INSERT/UPDATE/DELETE,
// which is also what every consumer treats as "compute it yourself".
//
// The caller supplies the storage so an ordinary statement does not pay a heap
// allocation for the plan itself.
func newDMLPlan(out *dmlPlan, db *storage.DB, tenant string, stmt Statement) *dmlPlan {
	var name string
	var event storage.TriggerEvent
	switch s := stmt.(type) {
	case *Insert:
		name, event = s.Table, storage.TriggerInsert
	case *Update:
		name, event = s.Table, storage.TriggerUpdate
	case *Delete:
		name, event = s.Table, storage.TriggerDelete
	default:
		return nil
	}
	plan := out
	plan.stmt = stmt
	plan.before, plan.after = db.Catalog().GetTriggersForEvent(name, event)
	// Deliberately not computed for INSERT: nothing on the INSERT path asks
	// for it (per-column FOREIGN KEY validation goes through
	// validateOneRowConstraint instead), and the answer costs a full
	// ListTables — two allocations, a sort, and a backend directory listing
	// in the disk-backed storage modes.
	if _, isInsert := stmt.(*Insert); !isInsert {
		plan.tenantFK = tenantHasAnyForeignKeys(ExecEnv{tenant: tenant, db: db})
		plan.tenantFKKnown = true
	}
	table, err := db.Get(tenant, name)
	if err != nil {
		return plan
	}
	plan.table = table
	if plan.hasTriggers() {
		// Every consumer of colIndex/rowIDs below is disabled by a trigger.
		return plan
	}
	switch s := stmt.(type) {
	case *Update:
		// Precomputed under exactly the conditions that make both
		// rowUpdateSnapshotTarget and executeSimpleUpdateFastPath want them:
		// a tenant with foreign keys disables both.
		if plan.tenantFK {
			return plan
		}
		plan.colIndex = simpleColumnIndex(table, s.Table)
		if s.Where != nil {
			plan.rowIDs, _, _, plan.rowIDsFound = selectConstraintIndex(table, plan.colIndex, s.Where)
		}
	case *Delete:
		// Both consumers (rowDeleteSnapshotTarget and executeDelete's point
		// path) do nothing without a WHERE clause.
		if s.Where == nil {
			return plan
		}
		plan.colIndex = simpleColumnIndex(table, s.Table)
		plan.rowIDs, _, _, plan.rowIDsFound = selectDeleteConstraintRows(table, plan.colIndex, s.Where)
	}
	return plan
}

// appendOnlySnapshotTarget identifies the narrow INSERT fast path whose
// failed execution can be rolled back by truncating appended rows and, when
// the table carries secondary indexes, removing exactly those rows' entries
// from each one (see restoreAppendOnlyTable). A table with any OTHER
// mutating side effect a plain row-truncate cannot undo — a trigger, most
// prominently, which can write to arbitrary other tables — still needs the
// full cloned-table snapshot; that is the only remaining disqualifier here.
//
// A secondary index used to disqualify a table from this path entirely,
// because rollback only truncated table.Rows and left the index pointing at
// rows about to disappear. That made every INSERT into an indexed table
// (idx_chunks_source_chunk-style corpora especially) pay a full
// cloneTable/cloneRows copy of the table's ENTIRE current row set on every
// single statement, independent of how many rows that statement actually
// inserted -- turning bulk ingestion into an indexed table quadratic in the
// table's final size. restoreAppendOnlyTable now undoes the index inserts
// too, so that cost is gone on the (overwhelmingly common) successful path;
// only an actual rollback pays a now much smaller O(rows this statement
// appended) price instead of the old O(table size) one.
func appendOnlySnapshotTarget(plan *dmlPlan) (string, bool) {
	if plan == nil {
		return "", false
	}
	s, ok := plan.stmt.(*Insert)
	if !ok || plan.hasTriggers() {
		return "", false
	}
	if plan.table == nil {
		return "", false
	}
	return s.Table, true
}

// rowUpdateSnapshotTarget identifies a point UPDATE whose constraint-index
// candidate set bounds every row the statement can replace. The compact
// snapshot is safe only when no trigger/FK can write elsewhere and none of the
// assigned columns belongs to a secondary index.
func rowUpdateSnapshotTarget(plan *dmlPlan) (string, []int, bool) {
	if plan == nil {
		return "", nil, false
	}
	s, ok := plan.stmt.(*Update)
	if !ok || plan.tenantFK || plan.hasTriggers() {
		return "", nil, false
	}
	if plan.table == nil || updateTouchesSecondaryIndex(plan.table, s) {
		return "", nil, false
	}
	if !plan.rowIDsFound {
		return "", nil, false
	}
	return s.Table, plan.rowIDs, true
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
func rowDeleteSnapshotTarget(plan *dmlPlan) (string, []int, bool) {
	if plan == nil {
		return "", nil, false
	}
	s, ok := plan.stmt.(*Delete)
	if !ok || plan.tenantFK || plan.hasTriggers() {
		return "", nil, false
	}
	if plan.table == nil || len(plan.table.Indexes) > 0 {
		return "", nil, false
	}
	if !plan.rowIDsFound || len(plan.rowIDs) > 1 {
		return "", nil, false
	}
	return s.Table, plan.rowIDs, true
}

// tableScopedSnapshotTarget identifies DML that cannot mutate a table other
// than its target. In that common case a table-scoped rollback point avoids
// cloning every table on each statement. Triggers and FK cascades can write
// elsewhere, so they deliberately retain the full-database snapshot.
func tableScopedSnapshotTarget(plan *dmlPlan) (string, bool) {
	if plan == nil || plan.hasTriggers() {
		return "", false
	}
	switch s := plan.stmt.(type) {
	case *Insert:
		return s.Table, true
	case *Update:
		if plan.tenantFK {
			return "", false
		}
		return s.Table, true
	case *Delete:
		if plan.tenantFK {
			return "", false
		}
		return s.Table, true
	default:
		return "", false
	}
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
