// INSERT, the column defaults it applies, and the per-row constraint checks
// every mutating statement shares. The constraint-index cache lives here too:
// it is the lookup structure that makes PRIMARY KEY and UNIQUE enforcement
// something other than a full scan per row.
package engine

import (
	"fmt"
	"sort"
	"strings"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func executeInsert(env ExecEnv, s *Insert) (*ResultSet, error) {
	if len(s.Rows) == 0 {
		return nil, fmt.Errorf("INSERT requires at least one VALUES clause")
	}
	t := planTable(env.planFor(s))
	if t == nil {
		var err error
		if t, err = env.db.Get(env.tenant, s.Table); err != nil {
			return nil, err
		}
	}
	tmp := Row{}
	if len(s.Cols) == 0 {
		return executeInsertAllColumns(env, s, t, tmp)
	}
	return executeInsertSpecificColumns(env, s, t, tmp)
}

func executeInsertAllColumns(env ExecEnv, s *Insert, t *storage.Table, tmp Row) (*ResultSet, error) {
	expected := len(t.Cols)
	beforeTriggers, afterTriggers := planTriggers(env.planFor(s), env, s.Table, storage.TriggerInsert)
	hasBefore := len(beforeTriggers) > 0
	hasAfter := len(afterTriggers) > 0
	// buildTableRow allocates a map(2*len(cols)); resolve both timing lists
	// once before the row loop and skip that map entirely when neither triggers
	// nor RETURNING needs it.
	needsRow := hasBefore || hasAfter || len(s.Returning) > 0
	// Both of these are for RETURNING and trigger rows only. Building them
	// unconditionally charged every plain INSERT a slice sized to its row count
	// plus one string concatenation per column, none of which anything read.
	var returningRows []Row
	if len(s.Returning) > 0 {
		returningRows = make([]Row, 0, len(s.Rows))
	}
	var keys tableRowKeys
	if needsRow {
		keys = newTableRowKeys(t.Cols, strings.ToLower(s.Table)+".")
	}
	// Only safe without triggers: a trigger body can run DDL between rows, so
	// a parent table resolved for row 1 need not still be the table row 2
	// should be checked against. See fkTargetCache.
	var fks *fkTargetCache
	if !hasBefore && !hasAfter {
		fks = newFKTargetCache()
	}
	// The index set cannot change mid-statement, so its sorted name list is
	// computed once here instead of being rebuilt and re-sorted on every row.
	// rawIndexNames is deliberately called (not storage.Table.SortedIndexNames)
	// so this stays inlined and allocation-free at the call site; see its doc
	// comment in exec_dml_update.go.
	indexNames := rawIndexNames(t)
	sort.Strings(indexNames)
	// Which columns can ever fail validateOneRowConstraint is a pure function
	// of the schema; computed once here instead of validateRowConstraintsWith
	// visiting every column of the table (constrained or not) on every row.
	constrainedCols := constrainedColumnIndices(t.Cols)
	wal, err := beginWALAuto(env, s.Table)
	if err != nil {
		return nil, err
	}
	for _, vals := range s.Rows {
		if len(vals) != expected {
			return nil, fmt.Errorf("INSERT expects %d values", expected)
		}
		if err := checkCtx(env.ctx); err != nil {
			return nil, err
		}
		row := make([]any, expected)
		for i, e := range vals {
			v, err := evalExpr(env, e, tmp)
			if err != nil {
				return nil, err
			}
			cv, err := coerceColumnValue(v, t.Cols[i])
			if err != nil {
				return nil, fmt.Errorf("column %q: %w", t.Cols[i].Name, err)
			}
			row[i] = cv
		}
		if err := validateRowConstraintsWith(env, t, row, -1, constrainedCols, fks); err != nil {
			return nil, err
		}
		if err := t.CheckSecondaryIndexConstraints(row, -1); err != nil {
			return nil, err
		}
		var newRow Row
		if needsRow {
			newRow = buildTableRow(keys, row)
		}
		if hasBefore {
			if err := fireTriggerList(env, beforeTriggers, newRow, nil); err != nil {
				return nil, err
			}
		}
		t.Rows = append(t.Rows, row)
		if err := t.InsertSecondaryIndexRow(len(t.Rows)-1, row, indexNames); err != nil {
			return nil, err
		}
		if err := wal.logInsert(env, len(t.Rows)-1, row, t.Cols); err != nil {
			return nil, err
		}
		if hasAfter {
			if err := fireTriggerList(env, afterTriggers, newRow, nil); err != nil {
				return nil, err
			}
		}
		if len(s.Returning) > 0 {
			returningRows = append(returningRows, newRow)
		}
	}
	if err := wal.commit(); err != nil {
		return nil, err
	}
	tryFastPathAppend(env, s.Table, t, len(s.Rows))
	t.Version++
	t.InvalidateStats()
	t.MarkDirtyFrom(len(t.Rows) - len(s.Rows))
	markDependentMaterializedViewsStale(env, s.Table)
	if len(s.Returning) > 0 {
		return projectReturningRows(env, t.Cols, s.Returning, returningRows)
	}
	return nil, nil
}

func executeInsertSpecificColumns(env ExecEnv, s *Insert, t *storage.Table, tmp Row) (*ResultSet, error) {
	colIdx := make([]int, len(s.Cols))
	for i, name := range s.Cols {
		idx, err := t.ColIndex(name)
		if err != nil {
			return nil, err
		}
		colIdx[i] = idx
	}
	beforeTriggers, afterTriggers := planTriggers(env.planFor(s), env, s.Table, storage.TriggerInsert)
	hasBefore := len(beforeTriggers) > 0
	hasAfter := len(afterTriggers) > 0
	needsRow := hasBefore || hasAfter || len(s.Returning) > 0
	// See executeInsertAllColumns: neither is read unless a trigger or a
	// RETURNING clause asks for the row map.
	var returningRows []Row
	if len(s.Returning) > 0 {
		returningRows = make([]Row, 0, len(s.Rows))
	}
	var keys tableRowKeys
	if needsRow {
		keys = newTableRowKeys(t.Cols, strings.ToLower(s.Table)+".")
	}
	// Only safe without triggers: a trigger body can run DDL between rows, so
	// a parent table resolved for row 1 need not still be the table row 2
	// should be checked against. See fkTargetCache.
	var fks *fkTargetCache
	if !hasBefore && !hasAfter {
		fks = newFKTargetCache()
	}
	// The index set cannot change mid-statement, so its sorted name list is
	// computed once here instead of being rebuilt and re-sorted on every row.
	// rawIndexNames is deliberately called (not storage.Table.SortedIndexNames)
	// so this stays inlined and allocation-free at the call site; see its doc
	// comment in exec_dml_update.go.
	indexNames := rawIndexNames(t)
	sort.Strings(indexNames)
	// See executeInsertAllColumns: both are pure functions of the schema,
	// computed once per statement instead of per row.
	constrainedCols := constrainedColumnIndices(t.Cols)
	hasDefaults := tableHasDefaults(t.Cols)
	wal, err := beginWALAuto(env, s.Table)
	if err != nil {
		return nil, err
	}
	for _, vals := range s.Rows {
		if len(vals) != len(s.Cols) {
			return nil, fmt.Errorf("INSERT column/value mismatch")
		}
		if err := checkCtx(env.ctx); err != nil {
			return nil, err
		}
		row := make([]any, len(t.Cols))
		if hasDefaults {
			if err := applyColumnDefaults(row, t.Cols); err != nil {
				return nil, err
			}
		}
		for i, idx := range colIdx {
			v, err := evalExpr(env, vals[i], tmp)
			if err != nil {
				return nil, err
			}
			cv, err := coerceColumnValue(v, t.Cols[idx])
			if err != nil {
				return nil, fmt.Errorf("column %q: %w", t.Cols[idx].Name, err)
			}
			row[idx] = cv
		}
		if err := validateRowConstraintsWith(env, t, row, -1, constrainedCols, fks); err != nil {
			return nil, err
		}
		if err := t.CheckSecondaryIndexConstraints(row, -1); err != nil {
			return nil, err
		}
		var newRow Row
		if needsRow {
			newRow = buildTableRow(keys, row)
		}
		if hasBefore {
			if err := fireTriggerList(env, beforeTriggers, newRow, nil); err != nil {
				return nil, err
			}
		}
		t.Rows = append(t.Rows, row)
		if err := t.InsertSecondaryIndexRow(len(t.Rows)-1, row, indexNames); err != nil {
			return nil, err
		}
		if err := wal.logInsert(env, len(t.Rows)-1, row, t.Cols); err != nil {
			return nil, err
		}
		if hasAfter {
			if err := fireTriggerList(env, afterTriggers, newRow, nil); err != nil {
				return nil, err
			}
		}
		if len(s.Returning) > 0 {
			returningRows = append(returningRows, newRow)
		}
	}
	if err := wal.commit(); err != nil {
		return nil, err
	}
	tryFastPathAppend(env, s.Table, t, len(s.Rows))
	t.Version++
	t.InvalidateStats()
	t.MarkDirtyFrom(len(t.Rows) - len(s.Rows))
	markDependentMaterializedViewsStale(env, s.Table)
	if len(s.Returning) > 0 {
		return projectReturningRows(env, t.Cols, s.Returning, returningRows)
	}
	return nil, nil
}

// tryFastPathAppend opportunistically hands the rows this INSERT statement
// just appended to t.Rows straight to the storage backend via
// db.AppendRowsFast, which -- for a ModePagedIndex backend only -- writes
// them incrementally into the on-disk B+Trees (cost proportional to
// newRowCount, not to the table's existing size) instead of leaving them to
// be picked up by the next db.Sync's full backend.SaveTable rewrite (cost
// proportional to the whole table). See AppendRowsFast's doc comment.
//
// This never affects the statement's outcome or in-memory bookkeeping: the
// rows are already durably reflected in t.Rows and the WAL by the time this
// runs (both happen unchanged, above). ok=false (no paged-index backend
// attached, or the table has no backend catalog entry yet) and ok=true with
// a non-nil err (the fast path was attempted and failed, e.g. a transient
// backend/IO error) are both silently ignored: either way, t.Version keeps
// incrementing as it always did, so the existing Sync()-driven
// backend.SaveTable path -- unchanged -- remains the durability backstop
// for these rows, exactly as if this fast path did not exist. Swallowing
// the error here (rather than failing the statement) preserves INSERT's
// existing error semantics -- constraint/type errors are the only way
// INSERT fails, exactly as before this fast path existed.
func tryFastPathAppend(env ExecEnv, tableName string, t *storage.Table, newRowCount int) {
	if newRowCount <= 0 || newRowCount > len(t.Rows) {
		return
	}
	newRows := t.Rows[len(t.Rows)-newRowCount:]
	_, _ = env.db.AppendRowsFast(env.tenant, tableName, newRows)
}

// applyColumnDefaults initializes an INSERT row before explicitly named
// columns overwrite their positions. Defaults are copied before coercion so a
// BLOB default can never be shared and mutated through a stored row.
// tableHasDefaults reports whether any column of t declares a default value.
// Whether a table has any default-valued column is a pure function of its
// schema, invariant for the whole statement — computed once per INSERT here
// instead of applyColumnDefaults re-scanning every column of the table on
// every single row when the answer is always "no defaults to apply".
func tableHasDefaults(cols []storage.Column) bool {
	for _, c := range cols {
		if c.HasDefault {
			return true
		}
	}
	return false
}

// constrainedColumnIndices returns, in ascending column-index order, every
// column validateOneRowConstraint could ever reject (NOT NULL or a
// PRIMARY KEY/UNIQUE/FOREIGN KEY constraint). INSERT always validates every
// inserted row against the table's full constraint set, but passing this
// instead of a nil changedCols to validateRowConstraintsWith turns that
// per-row loop from O(len(t.Cols)) into O(constrained-column-count): most
// columns of most tables carry no constraint at all, and
// validateOneRowConstraint's own early return for them still costs a
// function call and two field reads per column per row. Ascending order
// preserves validateRowConstraintsWith's existing first-failure-wins error
// semantics, same as ranging over t.Cols directly did.
func constrainedColumnIndices(cols []storage.Column) []int {
	// Always non-nil, even when empty: validateRowConstraintsWith treats a
	// nil changedCols as "no restriction, check every column" -- an empty
	// slice for a table with zero constrained columns must instead mean
	// "nothing to check", not accidentally re-trigger the full scan this
	// function exists to avoid.
	idx := make([]int, 0, len(cols))
	for i, c := range cols {
		if c.NotNull || c.Constraint != storage.NoConstraint {
			idx = append(idx, i)
		}
	}
	return idx
}

func applyColumnDefaults(row []any, cols []storage.Column) error {
	for i, col := range cols {
		if !col.HasDefault {
			continue
		}
		v := col.DefaultValue
		if b, ok := v.([]byte); ok {
			v = append([]byte(nil), b...)
		}
		cv, err := coerceColumnValue(v, col)
		if err != nil {
			return fmt.Errorf("default for column %q: %w", col.Name, err)
		}
		row[i] = cv
	}
	return nil
}

// validateRowConstraints checks row's constrained columns (NOT NULL,
// PRIMARY KEY, UNIQUE, FOREIGN KEY) against t's schema.
//
// changedCols controls which columns are actually checked:
//   - nil means "every constrained column", which INSERT always passes since
//     a freshly-built row has no prior state to diff against and every
//     column's value is new.
//   - a non-nil slice (empty or not) restricts the check to just those column
//     indices. UPDATE call sites use this: a column whose value didn't
//     change by this statement was already valid before it ran (nothing else
//     in the same statement can have invalidated it — see the call sites for
//     why), so re-validating it is redundant work, including the FOREIGN KEY
//     case's db.Get/ColIndex/constraint-index lookup against the referenced
//     table. Byte-identical error behavior versus the pre-scoping full scan
//     requires changedCols to be sorted ascending: this function still visits
//     columns in that order, matching the original range over t.Cols, so
//     when more than one changed column is in violation the same one wins.
func validateRowConstraints(env ExecEnv, t *storage.Table, row []any, excludeRow int, changedCols []int) error {
	return validateRowConstraintsWith(env, t, row, excludeRow, changedCols, nil)
}

// fkTarget is one resolved FOREIGN KEY reference: the parent table and the
// position of the referenced column in it.
type fkTarget struct {
	table  *storage.Table
	colIdx int
	err    error
}

// fkTargetCache resolves each FOREIGN KEY column's parent table and column
// index once per statement rather than once per row.
//
// Both are pure functions of the column's *storage.ForeignKeyRef, which is
// part of the schema, so within one statement the answer is fixed — but
// getting it costs a db.Get, and in the disk-backed storage modes a db.Get for
// a parent table that is not resident is a *backend load of that whole table*.
// Paying it per row turned a bulk INSERT into one parent load per inserted row.
//
// Callers must only supply a cache when no trigger can run between rows: a
// trigger body may execute DDL, and dropping and recreating the parent table
// mid-statement would leave a cached pointer addressing the old one. See
// executeInsertAllColumns for the gate.
type fkTargetCache struct {
	targets map[*storage.ForeignKeyRef]fkTarget
}

func newFKTargetCache() *fkTargetCache {
	return &fkTargetCache{targets: make(map[*storage.ForeignKeyRef]fkTarget, 1)}
}

// resolve returns the parent table and referenced column index for ref,
// consulting the cache when one is supplied.
func (c *fkTargetCache) resolve(env ExecEnv, col storage.Column) fkTarget {
	if c != nil {
		if hit, ok := c.targets[col.ForeignKey]; ok {
			return hit
		}
	}
	var out fkTarget
	refTable, err := env.db.Get(env.tenant, col.ForeignKey.Table)
	if err != nil {
		out.err = fmt.Errorf("FOREIGN KEY column %q references missing table %q", col.Name, col.ForeignKey.Table)
	} else if refIdx, idxErr := refTable.ColIndex(col.ForeignKey.Column); idxErr != nil {
		out.err = fmt.Errorf("FOREIGN KEY column %q references missing column %q.%q", col.Name, col.ForeignKey.Table, col.ForeignKey.Column)
	} else {
		out.table, out.colIdx = refTable, refIdx
	}
	if c != nil {
		c.targets[col.ForeignKey] = out
	}
	return out
}

func validateRowConstraintsWith(env ExecEnv, t *storage.Table, row []any, excludeRow int, changedCols []int, fks *fkTargetCache) error {
	if changedCols == nil {
		for colIdx := range t.Cols {
			if err := validateOneRowConstraint(env, t, row, excludeRow, colIdx, fks); err != nil {
				return err
			}
		}
		return nil
	}
	for _, colIdx := range changedCols {
		if err := validateOneRowConstraint(env, t, row, excludeRow, colIdx, fks); err != nil {
			return err
		}
	}
	return nil
}

// validateOneRowConstraint is validateRowConstraints's per-column body,
// factored out so the "check everything" (INSERT) and "check only the
// changed columns" (UPDATE) loops share identical logic and error text.
func validateOneRowConstraint(env ExecEnv, t *storage.Table, row []any, excludeRow int, colIdx int, fks *fkTargetCache) error {
	col := t.Cols[colIdx]
	if colIdx >= len(row) {
		return fmt.Errorf("row missing constrained column %q", col.Name)
	}
	val := row[colIdx]
	if col.NotNull && isNull(val) {
		return fmt.Errorf("NOT NULL column %q cannot be NULL", col.Name)
	}
	if col.Constraint == storage.NoConstraint {
		return nil
	}
	switch col.Constraint {
	case storage.PrimaryKey:
		if isNull(val) {
			return fmt.Errorf("PRIMARY KEY column %q cannot be NULL", col.Name)
		}
		if constraintValueExists(t, colIdx, val, excludeRow) {
			return fmt.Errorf("duplicate PRIMARY KEY value for column %q", col.Name)
		}
	case storage.Unique:
		if isNull(val) {
			return nil
		}
		if constraintValueExists(t, colIdx, val, excludeRow) {
			return fmt.Errorf("duplicate UNIQUE value for column %q", col.Name)
		}
	case storage.ForeignKey:
		if isNull(val) {
			return nil
		}
		if col.ForeignKey == nil {
			return fmt.Errorf("FOREIGN KEY column %q has no reference target", col.Name)
		}
		target := fks.resolve(env, col)
		if target.err != nil {
			return target.err
		}
		refTable, refIdx := target.table, target.colIdx
		if !constraintValueExists(refTable, refIdx, val, -1) {
			return fmt.Errorf("FOREIGN KEY violation on column %q: value %v not found in %s.%s", col.Name, val, col.ForeignKey.Table, col.ForeignKey.Column)
		}
	}
	return nil
}

// constraintIndexSet caches, per column of one table, a hash map from an
// already-used column value to the row indices holding it. This turns
// PRIMARY KEY / UNIQUE / FOREIGN KEY existence checks from an O(n) scan of
// the whole table (paid on every INSERT/UPDATE) into an O(1) lookup — the
// difference between ~10s and ~10ms when bulk-inserting 10k rows into a
// table that already has 100k.
//
// It is stored on the table itself, through storage.Table's derived-state
// slot, and guarded by that table's own lock. It used to live in a
// process-global map keyed by (*storage.Table, column), which meant that a
// write to any table in the process contended with every other, that
// maintaining one row cost a scan of every cached column of every table, and
// that nothing was ever released — see storage.Table.derived for the full
// account.
//
// Maintenance is incremental rather than invalidate-and-rebuild-on-any-
// change, because a naive "rebuild when table.Version changes" cache would
// pay the full O(n) rebuild on literally every row of a multi-row INSERT
// (each row bumps Version), erasing the benefit entirely:
//   - INSERT only appends, so getConstraintIndex just indexes whatever rows
//     have been added since rowCount was last recorded — including rows
//     added earlier in the very same multi-row INSERT statement.
//   - UPDATE overwrites a row in place without changing the row count, so
//     it can't be detected by growth; patchConstraintIndexRow moves that
//     one row from its old value's bucket to its new one directly.
//   - DELETE removes rows and shifts every subsequent row's index, which
//     the incremental scheme can't reconcile cheaply, so
//     invalidateConstraintIndexes drops the cache outright and the next
//     check rebuilds it from scratch.
type constraintIndexSet struct {
	cols map[int]*constraintIndexEntry
}

type constraintIndexEntry struct {
	rowCount int // rows already reflected in `rows`, i.e. t.Rows[:rowCount]
	rows     map[any][]int
}

// CloneDerived implements storage.DerivedCloner, so a per-transaction table
// clone (storage.cloneTable, invoked by every db.Begin()) can start with an
// already-built constraint index instead of forcing the transaction's first
// constrained INSERT/UPDATE to rebuild it from scratch by rescanning every
// row -- which, across many short transactions against one growing table,
// used to make total constraint-checking work O(transactions x table_size)
// instead of O(table_size) amortized.
//
// Every map (set.cols itself, each entry's rows map, and each rows value's
// []int slice) is copied rather than referenced, matching DerivedCloner's
// contract that the clone and the original must share no mutable state:
// getConstraintIndex/patchConstraintIndexRow/patchConstraintIndexSwapRemove
// all mutate these in place, and the cloned table's rows diverge
// independently from the source's the moment either one is next written.
func (set *constraintIndexSet) CloneDerived() any {
	cloned := &constraintIndexSet{cols: make(map[int]*constraintIndexEntry, len(set.cols))}
	for colIdx, entry := range set.cols {
		rows := make(map[any][]int, len(entry.rows))
		for k, ids := range entry.rows {
			rows[k] = append([]int(nil), ids...)
		}
		cloned.cols[colIdx] = &constraintIndexEntry{rowCount: entry.rowCount, rows: rows}
	}
	return cloned
}

// constraintIndexSetOf returns the table's cached set, creating it when
// create is true. The caller must hold t.DerivedLock.
func constraintIndexSetOf(t *storage.Table, create bool) *constraintIndexSet {
	set, _ := t.Derived().(*constraintIndexSet)
	if set == nil && create {
		set = &constraintIndexSet{cols: make(map[int]*constraintIndexEntry, 1)}
		t.SetDerived(set)
	}
	return set
}

func getConstraintIndex(t *storage.Table, colIdx int) *constraintIndexEntry {
	t.DerivedLock()
	defer t.DerivedUnlock()

	set := constraintIndexSetOf(t, true)
	e := set.cols[colIdx]
	if e == nil || e.rowCount > len(t.Rows) {
		// First use for this column, or the table shrank (DELETE already
		// invalidates explicitly; this is a defensive fallback in case some
		// row-removing path doesn't).
		e = &constraintIndexEntry{rows: make(map[any][]int, len(t.Rows))}
		set.cols[colIdx] = e
	}
	for i := e.rowCount; i < len(t.Rows); i++ {
		r := t.Rows[i]
		if colIdx >= len(r) || r[colIdx] == nil {
			continue
		}
		k := comparableKeyPart(r[colIdx])
		e.rows[k] = append(e.rows[k], i)
	}
	e.rowCount = len(t.Rows)
	return e
}

// currentConstraintIndex returns an already complete cache entry without
// building one. Point DELETE uses this to avoid allocating an O(n) hash map on
// the first operation after loading a table; a cold delete can scan one key
// column once and still avoid the much larger rollback clone.
func currentConstraintIndex(t *storage.Table, colIdx int) *constraintIndexEntry {
	t.DerivedLock()
	defer t.DerivedUnlock()
	set := constraintIndexSetOf(t, false)
	if set == nil {
		return nil
	}
	entry := set.cols[colIdx]
	if entry == nil || entry.rowCount != len(t.Rows) {
		return nil
	}
	return entry
}

// invalidateConstraintIndexes drops every cached constraint index for a
// table. Call before any operation that can remove or reorder existing rows
// (DELETE) or replace the table wholesale (DROP TABLE) — the incremental
// index only knows how to grow by appending or patch a single row in place.
func invalidateConstraintIndexes(t *storage.Table) {
	if t == nil {
		return
	}
	t.DerivedLock()
	t.SetDerived(nil)
	t.DerivedUnlock()
}

// patchConstraintIndexRow updates every cached constraint index for a table
// after row rowIdx is overwritten in place (UPDATE), moving it from its old
// value's bucket to its new one instead of invalidating the whole cache.
func patchConstraintIndexRow(t *storage.Table, rowIdx int, oldRow, newRow []any) {
	t.DerivedLock()
	defer t.DerivedUnlock()
	set := constraintIndexSetOf(t, false)
	if set == nil {
		return
	}
	for colIdx, e := range set.cols {
		if colIdx >= len(oldRow) || colIdx >= len(newRow) {
			continue
		}
		oldVal, newVal := oldRow[colIdx], newRow[colIdx]
		if rawEqual(oldVal, newVal) {
			continue
		}
		if oldVal != nil {
			ok := comparableKeyPart(oldVal)
			bucket := e.rows[ok]
			for i, ri := range bucket {
				if ri == rowIdx {
					e.rows[ok] = append(bucket[:i], bucket[i+1:]...)
					break
				}
			}
		}
		if newVal != nil {
			nk := comparableKeyPart(newVal)
			e.rows[nk] = append(e.rows[nk], rowIdx)
		}
	}
}

// patchConstraintIndexSwapRemove updates every cached constraint index after
// a point DELETE removes deleteRowID via swap-and-pop: lastRow (the table's
// last row, previously at lastRowID) now lives at deleteRowID, and lastRowID
// no longer exists. Call this before table.Rows is mutated, passing the
// table's pre-delete row count as oldLen.
//
// Only an entry that is fully caught up with the table (e.rowCount ==
// oldLen, the same condition currentConstraintIndex checks) can be patched
// from just these two rows' old and new values -- a partial entry may
// already have indexed deleteRowID's old value without having reached
// lastRowID yet, and patching it from these two rows alone would leave it
// internally inconsistent. Such entries are dropped instead, exactly like
// invalidateConstraintIndexes but scoped to the one lagging column rather
// than every column on the table.
func patchConstraintIndexSwapRemove(t *storage.Table, deleteRowID int, deletedRow []any, lastRowID int, lastRow []any, oldLen int) {
	t.DerivedLock()
	defer t.DerivedUnlock()
	set := constraintIndexSetOf(t, false)
	if set == nil {
		return
	}
	for colIdx, e := range set.cols {
		if e.rowCount != oldLen {
			delete(set.cols, colIdx)
			continue
		}
		if colIdx < len(deletedRow) {
			if oldVal := deletedRow[colIdx]; oldVal != nil {
				removeConstraintIndexBucketEntry(e, oldVal, deleteRowID)
			}
		}
		if deleteRowID != lastRowID && colIdx < len(lastRow) {
			if newVal := lastRow[colIdx]; newVal != nil {
				removeConstraintIndexBucketEntry(e, newVal, lastRowID)
				k := comparableKeyPart(newVal)
				e.rows[k] = append(e.rows[k], deleteRowID)
			}
		}
		e.rowCount = oldLen - 1
	}
}

// removeConstraintIndexBucketEntry removes rowIdx from val's bucket in e, if
// present.
func removeConstraintIndexBucketEntry(e *constraintIndexEntry, val any, rowIdx int) {
	k := comparableKeyPart(val)
	bucket := e.rows[k]
	for i, ri := range bucket {
		if ri == rowIdx {
			e.rows[k] = append(bucket[:i], bucket[i+1:]...)
			break
		}
	}
}

func constraintValueExists(t *storage.Table, colIdx int, val any, excludeRow int) bool {
	idx := getConstraintIndex(t, colIdx)
	for _, rowIdx := range idx.rows[comparableKeyPart(val)] {
		if rowIdx != excludeRow {
			return true
		}
	}
	return false
}
