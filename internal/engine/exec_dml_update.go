// UPDATE, including the fast path for a triggerless update whose predicate and
// assignments can be evaluated straight off the stored row.
package engine

import (
	"strings"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func executeUpdate(env ExecEnv, s *Update) (*ResultSet, error) {
	if !tenantHasAnyForeignKeys(env) {
		if rs, ok, err := executeSimpleUpdateFastPath(env, s); ok || err != nil {
			return rs, err
		}
	}

	t, err := env.db.Get(env.tenant, s.Table)
	if err != nil {
		return nil, err
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
	beforeTriggers, afterTriggers := env.db.Catalog().GetTriggersForEvent(s.Table, storage.TriggerUpdate)
	hasBefore := len(beforeTriggers) > 0
	hasAfter := len(afterTriggers) > 0
	needsNewRow := hasAfter || len(s.Returning) > 0
	wal, err := beginWALAuto(env, s.Table)
	if err != nil {
		return nil, err
	}
	for ri, r := range t.Rows {
		if err := checkCtx(env.ctx); err != nil {
			return nil, err
		}
		row := buildTableRow(t.Cols, tablePrefix, r)
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
			for i, ex := range setIdx {
				v, err := evalExpr(env, ex, row)
				if err != nil {
					return nil, err
				}
				cv, err := coerceColumnValue(v, t.Cols[i])
				if err != nil {
					return nil, err
				}
				nextRow[i] = cv
			}
			if err := validateRowConstraints(env, t, nextRow, ri); err != nil {
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
			if err := t.UpdateSecondaryIndexRow(ri, before, nextRow); err != nil {
				return nil, err
			}
			if err := wal.logUpdate(env, ri, before, nextRow, t.Cols); err != nil {
				return nil, err
			}
			var newRow Row
			if needsNewRow {
				newRow = buildTableRow(t.Cols, tablePrefix, t.Rows[ri])
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
	t.Version++
	if n > 0 {
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

type simpleUpdatePlan struct {
	table    *storage.Table
	colIndex map[string]int
	where    Expr
	sets     []simpleUpdateSet
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

	rawPlan := &simpleSelectPlan{table: plan.table, colIndex: plan.colIndex, where: plan.where, filter: buildRawFilter(plan.colIndex, plan.where), rowTextCols: rawRowTextColumns(plan.colIndex)}
	updated := 0
	values := make([]any, len(plan.sets))
	wal, err := beginWALAuto(env, s.Table)
	if err != nil {
		return nil, true, err
	}
	for ri, raw := range plan.table.Rows {
		// Check context cancellation every 64 rows to reduce channel-select overhead.
		if ri&63 == 0 {
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
		for i, set := range plan.sets {
			nextRow[set.col] = values[i]
		}
		if err := validateRowConstraints(env, plan.table, nextRow, ri); err != nil {
			return nil, true, err
		}
		if err := plan.table.CheckSecondaryIndexConstraints(nextRow, ri); err != nil {
			return nil, true, err
		}
		patchConstraintIndexRow(plan.table, ri, plan.table.Rows[ri], nextRow)
		before := raw
		plan.table.Rows[ri] = nextRow
		plan.table.MarkRowUpdated(ri)
		if err := plan.table.UpdateSecondaryIndexRow(ri, before, nextRow); err != nil {
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

	plan.table.Version++
	if updated > 0 {
		plan.table.InvalidateStats()
		markDependentMaterializedViewsStale(env, s.Table)
	}
	return &ResultSet{Cols: []string{"updated"}, Rows: []Row{{"updated": updated}}}, true, nil
}

func buildSimpleUpdatePlan(env ExecEnv, s *Update) (*simpleUpdatePlan, bool, error) {
	before, after := env.db.Catalog().GetTriggersForEvent(s.Table, storage.TriggerUpdate)
	if len(before) > 0 || len(after) > 0 {
		return nil, false, nil
	}
	if !isSimpleRawPredicate(s.Where) {
		return nil, false, nil
	}

	table, err := env.db.Get(env.tenant, s.Table)
	if err != nil {
		return nil, true, err
	}
	colIndex := simpleColumnIndex(table, s.Table)
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
	return &simpleUpdatePlan{
		table:    table,
		colIndex: colIndex,
		where:    s.Where,
		sets:     sets,
	}, true, nil
}
