// DELETE, plus the helpers shared by all three DML statements: building a row
// map for expression evaluation, marking dependent materialized views stale,
// and projecting a RETURNING clause.
package engine

import (
	"strings"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func executeDelete(env ExecEnv, s *Delete) (*ResultSet, error) {
	t, err := env.db.Get(env.tenant, s.Table)
	if err != nil {
		return nil, err
	}
	// Removing rows shifts every subsequent row's index, which the
	// incremental constraint index can't reconcile cheaply — drop it and
	// let the next INSERT/UPDATE rebuild it from scratch.
	invalidateConstraintIndexes(t)
	if err := checkForeignKeysBeforeDelete(env, t, s.Where); err != nil {
		return nil, err
	}
	wal, err := beginWALAuto(env, s.Table)
	if err != nil {
		return nil, err
	}
	beforeDelTriggers, afterDelTriggers := env.db.Catalog().GetTriggersForEvent(s.Table, storage.TriggerDelete)
	hasTriggers := len(beforeDelTriggers) > 0 || len(afterDelTriggers) > 0
	// A DELETE without WHERE is still row-triggered. Preserve the compact
	// whole-table path only when no trigger can observe individual OLD rows.
	if s.Where == nil && !hasTriggers {
		del := len(t.Rows)
		if len(s.Returning) > 0 {
			tablePrefix := strings.ToLower(s.Table) + "."
			returningRows := make([]Row, 0, len(t.Rows))
			for _, r := range t.Rows {
				returningRows = append(returningRows, buildTableRow(t.Cols, tablePrefix, r))
			}
			if del > 0 {
				for i, r := range t.Rows {
					if err := wal.logDelete(env, i, r, t.Cols); err != nil {
						return nil, err
					}
				}
				if err := wal.commit(); err != nil {
					return nil, err
				}
				t.Rows = nil
				t.Version++
				t.ClearSecondaryIndexes()
				t.InvalidateStats()
				t.MarkDirtyFrom(-1) // DELETE is non-append; force full-table WAL
				markDependentMaterializedViewsStale(env, s.Table)
			}
			return projectReturningRows(env, t.Cols, s.Returning, returningRows)
		}
		if del > 0 {
			for i, r := range t.Rows {
				if err := wal.logDelete(env, i, r, t.Cols); err != nil {
					return nil, err
				}
			}
			if err := wal.commit(); err != nil {
				return nil, err
			}
			t.Rows = nil
			t.Version++
			t.ClearSecondaryIndexes()
			t.InvalidateStats()
			t.MarkDirtyFrom(-1) // DELETE is non-append; force full-table WAL
			markDependentMaterializedViewsStale(env, s.Table)
		}
		return &ResultSet{Cols: []string{"deleted"}, Rows: []Row{{"deleted": del}}}, nil
	}

	// Fast path: no triggers and a simple predicate – skip the full Row map allocation.
	if !hasTriggers && len(s.Returning) == 0 && isSimpleRawPredicate(s.Where) {
		colIndex := simpleColumnIndex(t, s.Table)
		rawPlan := &simpleSelectPlan{table: t, colIndex: colIndex, where: s.Where, filter: buildRawFilter(colIndex, s.Where), rowTextCols: rawRowTextColumns(colIndex)}
		kept := make([][]any, 0, len(t.Rows))
		oldToNew := make(map[int]int, len(t.Rows))
		del := 0
		for i, r := range t.Rows {
			if i&63 == 0 {
				if err := checkCtx(env.ctx); err != nil {
					return nil, err
				}
			}
			match, err := evalRawWhere(rawPlan, r)
			if err != nil {
				return nil, err
			}
			if match {
				if err := wal.logDelete(env, i, r, t.Cols); err != nil {
					return nil, err
				}
				del++
			} else {
				oldToNew[i] = len(kept)
				kept = append(kept, r)
			}
		}
		if err := wal.commit(); err != nil {
			return nil, err
		}
		t.Rows = kept
		t.Version++
		t.ReindexSecondaryIndexRows(oldToNew)
		if del > 0 {
			t.InvalidateStats()
			t.MarkDirtyFrom(-1)
			markDependentMaterializedViewsStale(env, s.Table)
		}
		return &ResultSet{Cols: []string{"deleted"}, Rows: []Row{{"deleted": del}}}, nil
	}

	// Slow path: triggers present or complex predicate – build full Row maps.
	kept := make([][]any, 0, len(t.Rows))
	oldToNew := make(map[int]int, len(t.Rows))
	del := 0
	returningRows := make([]Row, 0)
	tablePrefix := strings.ToLower(s.Table) + "."
	hasBeforeDel := len(beforeDelTriggers) > 0
	hasAfterDel := len(afterDelTriggers) > 0
	for i, r := range t.Rows {
		if err := checkCtx(env.ctx); err != nil {
			return nil, err
		}
		row := buildTableRow(t.Cols, tablePrefix, r)
		match := true
		if s.Where != nil {
			v, err := evalExpr(env, s.Where, row)
			if err != nil {
				return nil, err
			}
			match = toTri(v) == tvTrue
		}
		if !match {
			oldToNew[i] = len(kept)
			kept = append(kept, r)
		} else {
			if hasBeforeDel {
				if err := fireTriggerList(env, beforeDelTriggers, nil, row); err != nil {
					return nil, err
				}
			}
			if err := wal.logDelete(env, i, r, t.Cols); err != nil {
				return nil, err
			}
			if hasAfterDel {
				if err := fireTriggerList(env, afterDelTriggers, nil, row); err != nil {
					return nil, err
				}
			}
			if len(s.Returning) > 0 {
				returningRows = append(returningRows, row)
			}
			del++
		}
	}
	if err := wal.commit(); err != nil {
		return nil, err
	}
	t.Rows = kept
	t.Version++
	t.ReindexSecondaryIndexRows(oldToNew)
	if del > 0 {
		t.InvalidateStats()
		t.MarkDirtyFrom(-1) // DELETE is non-append; force full-table WAL
		markDependentMaterializedViewsStale(env, s.Table)
	}
	if len(s.Returning) > 0 {
		return projectReturningRows(env, t.Cols, s.Returning, returningRows)
	}
	return &ResultSet{Cols: []string{"deleted"}, Rows: []Row{{"deleted": del}}}, nil
}

func buildTableRow(cols []storage.Column, tablePrefix string, values []any) Row {
	row := make(Row, len(cols)*2)
	for i, c := range cols {
		key := strings.ToLower(c.Name)
		val := values[i]
		row[key] = val
		row[tablePrefix+key] = val
	}
	return row
}

func markDependentMaterializedViewsStale(env ExecEnv, tableName string) {
	if strings.HasPrefix(strings.ToLower(tableName), "__mv_") {
		return
	}
	schema, name := splitObjectName(tableName)
	if schema == "" {
		schema = "main"
	}
	_ = env.db.Catalog().MarkMaterializedViewsStaleByDependency(schema, name)
}

func projectReturningRows(env ExecEnv, cols []storage.Column, projs []SelectItem, rows []Row) (*ResultSet, error) {
	outRows := make([]Row, 0, len(rows))
	outCols := returningOutputCols(cols, projs)

	for _, r := range rows {
		if err := checkCtx(env.ctx); err != nil {
			return nil, err
		}
		out := Row{}
		for i, it := range projs {
			if it.Star {
				for _, c := range cols {
					name := c.Name
					lowerName := strings.ToLower(name)
					val, _ := getValLower(r, lowerName)
					putVal(out, name, val)
				}
				continue
			}
			val, err := evalExpr(env, it.Expr, r)
			if err != nil {
				return nil, err
			}
			putVal(out, projName(it, i), val)
		}
		outRows = append(outRows, out)
	}

	return &ResultSet{Cols: outCols, Rows: outRows}, nil
}

func returningOutputCols(cols []storage.Column, projs []SelectItem) []string {
	outCols := make([]string, 0, len(projs))
	seen := make(map[string]struct{}, len(projs))
	for i, it := range projs {
		if it.Star {
			for _, c := range cols {
				key := strings.ToLower(c.Name)
				if _, ok := seen[key]; ok {
					continue
				}
				seen[key] = struct{}{}
				outCols = append(outCols, c.Name)
			}
			continue
		}
		name := projName(it, i)
		key := strings.ToLower(name)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		outCols = append(outCols, name)
	}
	return outCols
}
