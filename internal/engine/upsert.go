package engine

import (
	"fmt"
	"sort"
	"strings"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

type conflictTarget struct {
	colIndices []int
	secIndex   *storage.SecondaryIndex
}

type preparedOnConflict struct {
	clause       *OnConflictClause
	target       conflictTarget
	assignments  []upsertAssign
	changedCols  []int
	excludedKeys []string
	evalRow      Row
	pointRow     [1]int
}

type upsertAssign struct {
	colIdx int
	expr   Expr
}

func prepareInsertOnConflict(env ExecEnv, s *Insert, t *storage.Table) (*preparedOnConflict, *triggerListRunner, *triggerListRunner, error) {
	if s.OnConflict == nil {
		return nil, nil, nil, nil
	}
	prep, err := prepareOnConflict(t, s.OnConflict)
	if err != nil {
		return nil, nil, nil, err
	}
	if s.OnConflict.DoNothing {
		return prep, nil, nil, nil
	}
	var beforeUpdateRunner, afterUpdateRunner *triggerListRunner
	bu, au := planTriggers(nil, env, s.Table, storage.TriggerUpdate)
	if len(bu) > 0 {
		beforeUpdateRunner = &triggerListRunner{triggers: bu}
	}
	if len(au) > 0 {
		afterUpdateRunner = &triggerListRunner{triggers: au}
	}
	return prep, beforeUpdateRunner, afterUpdateRunner, nil
}

func prepareOnConflict(t *storage.Table, oc *OnConflictClause) (*preparedOnConflict, error) {
	if oc == nil {
		return nil, nil
	}
	target, err := resolveConflictTarget(t, oc.TargetCols)
	if err != nil {
		return nil, err
	}
	prep := &preparedOnConflict{
		clause: oc,
		target: target,
	}
	if !oc.DoNothing {
		assigns := make([]upsertAssign, 0, len(oc.DoUpdate))
		for colName, expr := range oc.DoUpdate {
			colIdx, err := t.ColIndex(colName)
			if err != nil {
				return nil, fmt.Errorf("column %q does not exist in table %q", colName, t.Name)
			}
			assigns = append(assigns, upsertAssign{colIdx: colIdx, expr: expr})
		}
		sort.Slice(assigns, func(i, j int) bool {
			return assigns[i].colIdx < assigns[j].colIdx
		})
		changedCols := make([]int, len(assigns))
		for i, a := range assigns {
			changedCols[i] = a.colIdx
		}
		prep.assignments = assigns
		prep.changedCols = changedCols
	}
	return prep, nil
}

func resolveConflictTarget(t *storage.Table, targetCols []string) (conflictTarget, error) {
	if len(targetCols) == 0 {
		return conflictTarget{}, nil
	}
	if len(targetCols) == 1 {
		colIdx, err := t.ColIndex(targetCols[0])
		if err != nil {
			return conflictTarget{}, fmt.Errorf("ON CONFLICT column %q does not exist in table %q", targetCols[0], t.Name)
		}
		c := t.Cols[colIdx].Constraint
		if c == storage.PrimaryKey || c == storage.Unique {
			return conflictTarget{colIndices: []int{colIdx}}, nil
		}
	}
	// Match against a unique secondary index on the exact target columns (in order).
	for _, idx := range t.Indexes {
		if idx == nil || !idx.Unique || len(idx.Columns) != len(targetCols) {
			continue
		}
		match := true
		for i, col := range idx.Columns {
			if !strings.EqualFold(col, targetCols[i]) {
				match = false
				break
			}
		}
		if match {
			colIndices := make([]int, len(idx.Columns))
			for i, col := range idx.Columns {
				pos, err := t.ColIndex(col)
				if err != nil {
					return conflictTarget{}, err
				}
				colIndices[i] = pos
			}
			return conflictTarget{colIndices: colIndices, secIndex: idx}, nil
		}
	}
	return conflictTarget{}, fmt.Errorf("ON CONFLICT target (%s) does not match any PRIMARY KEY or UNIQUE constraint", strings.Join(targetCols, ", "))
}

func (p *preparedOnConflict) findConflictRow(t *storage.Table, row []any) (int, bool, error) {
	if p == nil {
		return -1, false, nil
	}
	if p.target.secIndex != nil {
		idx := p.target.secIndex
		keyVals := make([]any, len(idx.Columns))
		for i, pos := range p.target.colIndices {
			if pos < 0 || pos >= len(row) || isNull(row[pos]) {
				return -1, false, nil // SQL NULLs never conflict in UNIQUE indexes
			}
			keyVals[i] = row[pos]
		}
		rows, err := t.LookupSecondaryIndexPoint(idx, keyVals)
		if err != nil {
			return -1, false, err
		}
		if len(rows) > 0 {
			return rows[0], true, nil
		}
		return -1, false, nil
	}
	if len(p.target.colIndices) == 1 {
		colIdx := p.target.colIndices[0]
		if colIdx < 0 || colIdx >= len(row) || isNull(row[colIdx]) {
			return -1, false, nil
		}
		rows := lookupConstraintIndexRowsInto(getConstraintIndex(t, colIdx), row[colIdx], p.pointRow[:0])
		if len(rows) > 0 {
			return rows[0], true, nil
		}
		return -1, false, nil
	}
	return -1, false, nil
}

func applyOnConflictUpdate(
	env ExecEnv,
	s *Insert,
	t *storage.Table,
	conflictRowID int,
	proposedRow []any,
	prep *preparedOnConflict,
	keys tableRowKeys,
	indexNames []string,
	wal *walAuto,
	fks *fkTargetCache,
	beforeUpdateRunner, afterUpdateRunner *triggerListRunner,
) (bool, Row, error) {
	before := t.Rows[conflictRowID]
	// WHERE and SET consume the bindings synchronously. RETURNING and triggers
	// get their own row maps below.
	if prep.evalRow == nil {
		prep.evalRow = make(Row, len(keys.key)*3)
		prep.excludedKeys = make([]string, len(keys.key))
		for i, name := range keys.key {
			prep.excludedKeys[i] = "excluded." + name
		}
	}
	evalRow := prep.evalRow
	fillTableRow(evalRow, keys, before)
	for i, key := range prep.excludedKeys {
		evalRow[key] = proposedRow[i]
	}
	if prep.clause.Where != nil {
		v, err := evalExpr(env, prep.clause.Where, evalRow)
		if err != nil {
			return false, nil, err
		}
		if toTri(v) != tvTrue {
			return false, nil, nil
		}
	}
	nextRow := make([]any, len(t.Cols))
	copy(nextRow, before)
	for _, a := range prep.assignments {
		v, err := evalExpr(env, a.expr, evalRow)
		if err != nil {
			return false, nil, err
		}
		cv, err := coerceColumnValue(v, t.Cols[a.colIdx])
		if err != nil {
			return false, nil, fmt.Errorf("column %q: %w", t.Cols[a.colIdx].Name, err)
		}
		nextRow[a.colIdx] = cv
	}
	if err := validateRowConstraintsWith(env, t, nextRow, conflictRowID, prep.changedCols, fks); err != nil {
		return false, nil, err
	}
	if err := t.CheckSecondaryIndexConstraints(nextRow, conflictRowID); err != nil {
		return false, nil, err
	}
	var oldRowMap, newRowMap Row
	if beforeUpdateRunner != nil || afterUpdateRunner != nil {
		oldRowMap = buildTableRow(keys, before)
	}
	if oldRowMap != nil || len(s.Returning) > 0 {
		newRowMap = buildTableRow(keys, nextRow)
	}
	if beforeUpdateRunner != nil {
		if err := beforeUpdateRunner.fire(env, newRowMap, oldRowMap); err != nil {
			return false, nil, err
		}
	}
	t.Rows[conflictRowID] = nextRow
	t.MarkRowUpdated(conflictRowID)
	patchConstraintIndexRow(t, conflictRowID, before, nextRow)
	if len(indexNames) > 0 {
		if err := t.UpdateSecondaryIndexRow(conflictRowID, before, nextRow, indexNames); err != nil {
			return false, nil, err
		}
	}
	if err := wal.logUpdate(env, conflictRowID, before, nextRow, t.Cols); err != nil {
		return false, nil, err
	}
	// Install and log the parent first. A self-referencing cascade may
	// update another column in this very row; its write must survive both
	// this assignment and WAL replay.
	if planTenantHasForeignKeys(env, s) {
		for _, col := range prep.changedCols {
			if isNull(before[col]) || rawEqual(before[col], nextRow[col]) {
				continue
			}
			changes := map[any]fkChange{comparableKeyPart(before[col]): {newVal: nextRow[col]}}
			if err := fkOnUpdateCheckColumn(env, t, col, changes); err != nil {
				return false, nil, err
			}
		}
	}
	if afterUpdateRunner != nil {
		if err := afterUpdateRunner.fire(env, newRowMap, oldRowMap); err != nil {
			return false, nil, err
		}
	}
	if len(s.Returning) > 0 {
		return true, newRowMap, nil
	}
	return true, nil, nil
}
