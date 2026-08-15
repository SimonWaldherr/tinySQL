// UPDATE, including the fast path for a triggerless update whose predicate and
// assignments can be evaluated straight off the stored row.
package engine

import (
	"sort"
	"strings"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func executeUpdate(env ExecEnv, s *Update) (*ResultSet, error) {
	if !planTenantHasForeignKeys(env, s) {
		if rs, ok, err := executeSimpleUpdateFastPath(env, s); ok || err != nil {
			return rs, err
		}
	}

	t := planTable(env.planFor(s))
	if t == nil {
		var err error
		if t, err = env.db.Get(env.tenant, s.Table); err != nil {
			return nil, err
		}
	}
	if err := checkForeignKeysBeforeUpdate(env, t, s); err != nil {
		return nil, err
	}
	setIdx := map[int]Expr{}
	for name, ex := range s.Sets {
		i, err := t.ColIndex(name)
		if err != nil {
			return nil, err
		}
		setIdx[i] = ex
	}
	n := 0
	returningRows := make([]Row, 0)
	tablePrefix := strings.ToLower(s.Table) + "."
	keys := newTableRowKeys(t.Cols, tablePrefix)
	beforeTriggers, afterTriggers := planTriggers(env.planFor(s), env, s.Table, storage.TriggerUpdate)
	hasBefore := len(beforeTriggers) > 0
	hasAfter := len(afterTriggers) > 0
	needsNewRow := hasAfter || len(s.Returning) > 0
	// The index set cannot change mid-statement, so its sorted name list is
	// computed once here instead of being rebuilt and re-sorted twice per row
	// (once for the before key, once for the after key).
	indexNames := rawIndexNames(t)
	sort.Strings(indexNames)
	wal, err := beginWALAuto(env, s.Table)
	if err != nil {
		return nil, err
	}
	for ri, r := range t.Rows {
		if err := checkCtx(env.ctx); err != nil {
			return nil, err
		}
		row := buildTableRow(keys, r)
		ok := true
		if s.Where != nil {
			v, err := evalExpr(env, s.Where, row)
			if err != nil {
				return nil, err
			}
			ok = (toTri(v) == tvTrue)
		}
		if ok {
			// r == t.Rows[ri] before any mutation below, so oldRow is
			// identical to row — reuse it instead of rebuilding the same
			// map from the same data.
			oldRow := row
			if hasBefore {
				if err := fireTriggerList(env, beforeTriggers, row, oldRow); err != nil {
					return nil, err
				}
			}
			nextRow := append([]any(nil), t.Rows[ri]...)
			// changedCols scopes constraint validation below to columns whose
			// value this statement actually altered on this row: an untouched
			// column's existing value was already valid before this UPDATE
			// and nothing else in this loop iteration can have invalidated
			// it, so re-checking it (including, for FOREIGN KEY, a lookup
			// against the referenced table) is redundant. Sorted ascending
			// so that when more than one changed column is in violation, the
			// same one wins as when every column was checked in t.Cols order.
			changedCols := make([]int, 0, len(setIdx))
			for i, ex := range setIdx {
				v, err := evalExpr(env, ex, row)
				if err != nil {
					return nil, err
				}
				cv, err := coerceColumnValue(v, t.Cols[i])
				if err != nil {
					return nil, err
				}
				if !rawEqual(t.Rows[ri][i], cv) {
					changedCols = append(changedCols, i)
				}
				nextRow[i] = cv
			}
			sort.Ints(changedCols)
			if err := validateRowConstraints(env, t, nextRow, ri, changedCols); err != nil {
				return nil, err
			}
			if err := t.CheckSecondaryIndexConstraints(nextRow, ri); err != nil {
				return nil, err
			}
			patchConstraintIndexRow(t, ri, t.Rows[ri], nextRow)
			before := r
			t.Rows[ri] = nextRow
			// Report the row so the WAL can log it alone instead of the whole
			// table. Triggers below may write here too; anything that adds or
			// removes rows invalidates the list through MarkDirtyFrom.
			t.MarkRowUpdated(ri)
			if err := t.UpdateSecondaryIndexRow(ri, before, nextRow, indexNames); err != nil {
				return nil, err
			}
			if err := wal.logUpdate(env, ri, before, nextRow, t.Cols); err != nil {
				return nil, err
			}
			var newRow Row
			if needsNewRow {
				newRow = buildTableRow(keys, t.Rows[ri])
			}
			if hasAfter {
				if err := fireTriggerList(env, afterTriggers, newRow, oldRow); err != nil {
					return nil, err
				}
			}
			if len(s.Returning) > 0 {
				returningRows = append(returningRows, newRow)
			}
			n++
		}
	}
	if err := wal.commit(); err != nil {
		return nil, err
	}
	// Only a statement that actually replaced a row has changed this table.
	// Bumping the version unconditionally told CollectWALChanges that the
	// table had changed, so an UPDATE whose WHERE matched nothing wrote a
	// full-table WAL record and fsynced it — and invalidated every cache that
	// keys on Version — for a statement that did nothing.
	if n > 0 {
		t.Version++
		t.InvalidateStats()
		// The per-row MarkRowUpdated calls above already marked the table
		// non-append. Forcing MarkDirtyFrom(-1) here as well would discard the
		// row list they built and send the whole table to the WAL.
		markDependentMaterializedViewsStale(env, s.Table)
	}
	if len(s.Returning) > 0 {
		return projectReturningRows(env, t.Cols, s.Returning, returningRows)
	}
	return &ResultSet{Cols: []string{"updated"}, Rows: []Row{{"updated": n}}}, nil
}

// rawIndexNames collects a table's secondary index names, unsorted. Callers
// are expected to sort.Strings the result themselves inline, right after
// calling this -- NOT through another wrapper function. This one is small
// enough for the compiler to inline at each call site (`go build -gcflags=-m`
// confirms "can inline rawIndexNames"), so the make+append it contains proves
// non-escaping there and stays stack-allocated, exactly like the map-to-slice
// build indexRowKeys used to do inline on every row before this change. A
// wrapper that also calls sort.Strings itself is NOT small enough to inline,
// which forces its returned slice to escape to heap on every call -- that
// was tried and measured (see the stage 2 commit notes) to cost one extra
// heap allocation per statement versus this split form.
func rawIndexNames(t *storage.Table) []string {
	names := make([]string, 0, len(t.Indexes))
	for name := range t.Indexes {
		names = append(names, name)
	}
	return names
}

type simpleUpdatePlan struct {
	table    *storage.Table
	colIndex map[string]int
	where    Expr
	sets     []simpleUpdateSet
	rowIDs   []int
}

type simpleUpdateSet struct {
	col  int
	expr Expr
}

func executeSimpleUpdateFastPath(env ExecEnv, s *Update) (*ResultSet, bool, error) {
	if len(s.Returning) > 0 {
		return nil, false, nil
	}
	plan, ok, err := buildSimpleUpdatePlan(env, s)
	if !ok || err != nil {
		return nil, ok, err
	}

	rawPlan := &simpleSelectPlan{table: plan.table, colIndex: plan.colIndex, where: plan.where, filter: buildRawFilter(plan.colIndex, plan.where)}
	updated := 0
	values := make([]any, len(plan.sets))
	// The index set cannot change mid-statement, so its sorted name list is
	// computed once here instead of being rebuilt and re-sorted twice per row
	// (once for the before key, once for the after key).
	indexNames := rawIndexNames(plan.table)
	sort.Strings(indexNames)
	wal, err := beginWALAuto(env, s.Table)
	if err != nil {
		return nil, true, err
	}
	candidateCount := len(plan.table.Rows)
	indexed := plan.rowIDs != nil
	if indexed {
		candidateCount = len(plan.rowIDs)
	}
	for candidate := 0; candidate < candidateCount; candidate++ {
		ri := candidate
		if indexed {
			ri = plan.rowIDs[candidate]
			if ri < 0 || ri >= len(plan.table.Rows) {
				continue
			}
		}
		raw := plan.table.Rows[ri]
		// Check context cancellation every 64 rows to reduce channel-select overhead.
		if candidate&63 == 0 {
			if err := checkCtx(env.ctx); err != nil {
				return nil, true, err
			}
		}
		match, err := evalRawWhere(rawPlan, raw)
		if err != nil {
			return nil, true, err
		}
		if !match {
			continue
		}

		for i, set := range plan.sets {
			v, err := evalRawExpr(rawPlan, raw, set.expr)
			if err != nil {
				return nil, true, err
			}
			cv, err := coerceColumnValue(v, plan.table.Cols[set.col])
			if err != nil {
				return nil, true, err
			}
			values[i] = cv
		}
		nextRow := append([]any(nil), raw...)
		// See the identical comment in executeUpdate: only columns whose
		// value this row's SET clauses actually changed need re-validating.
		changedCols := make([]int, 0, len(plan.sets))
		for i, set := range plan.sets {
			if !rawEqual(raw[set.col], values[i]) {
				changedCols = append(changedCols, set.col)
			}
			nextRow[set.col] = values[i]
		}
		sort.Ints(changedCols)
		if err := validateRowConstraints(env, plan.table, nextRow, ri, changedCols); err != nil {
			return nil, true, err
		}
		if err := plan.table.CheckSecondaryIndexConstraints(nextRow, ri); err != nil {
			return nil, true, err
		}
		patchConstraintIndexRow(plan.table, ri, plan.table.Rows[ri], nextRow)
		before := raw
		plan.table.Rows[ri] = nextRow
		plan.table.MarkRowUpdated(ri)
		if err := plan.table.UpdateSecondaryIndexRow(ri, before, nextRow, indexNames); err != nil {
			return nil, true, err
		}
		if err := wal.logUpdate(env, ri, before, nextRow, plan.table.Cols); err != nil {
			return nil, true, err
		}
		updated++
	}
	if err := wal.commit(); err != nil {
		return nil, true, err
	}

	// See executeUpdate: a statement that matched no row has not changed the
	// table, and saying otherwise costs a full-table WAL record.
	if updated > 0 {
		plan.table.Version++
		plan.table.InvalidateStats()
		markDependentMaterializedViewsStale(env, s.Table)
	}
	return &ResultSet{Cols: []string{"updated"}, Rows: []Row{{"updated": updated}}}, true, nil
}

func buildSimpleUpdatePlan(env ExecEnv, s *Update) (*simpleUpdatePlan, bool, error) {
	stmtPlan := env.planFor(s)
	before, after := planTriggers(stmtPlan, env, s.Table, storage.TriggerUpdate)
	if len(before) > 0 || len(after) > 0 {
		return nil, false, nil
	}
	if !isSimpleRawPredicate(s.Where) {
		return nil, false, nil
	}

	table := planTable(stmtPlan)
	if table == nil {
		var err error
		if table, err = env.db.Get(env.tenant, s.Table); err != nil {
			return nil, true, err
		}
	}
	// newDMLPlan resolved the column index and the constraint seek already, to
	// decide this statement's rollback snapshot shape; both are pure functions
	// of state that cannot have changed since.
	colIndex := planColumnIndex(stmtPlan)
	if colIndex == nil {
		colIndex = simpleColumnIndex(table, s.Table)
	}
	sets := make([]simpleUpdateSet, 0, len(s.Sets))
	for name, expr := range s.Sets {
		if !isSimpleRawExpr(expr) {
			return nil, false, nil
		}
		col, err := table.ColIndex(name)
		if err != nil {
			return nil, true, err
		}
		sets = append(sets, simpleUpdateSet{col: col, expr: expr})
	}
	var rowIDs []int
	if candidates, found := planConstraintRows(stmtPlan, func() ([]int, bool) {
		rows, _, _, ok := selectConstraintIndex(table, colIndex, s.Where)
		return rows, ok
	}); found {
		rowIDs = candidates
	}
	return &simpleUpdatePlan{
		table:    table,
		colIndex: colIndex,
		where:    s.Where,
		sets:     sets,
		rowIDs:   rowIDs,
	}, true, nil
}
