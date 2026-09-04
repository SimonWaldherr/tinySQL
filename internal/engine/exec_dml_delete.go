// DELETE, plus the helpers shared by all three DML statements: building a row
// map for expression evaluation, marking dependent materialized views stale,
// and projecting a RETURNING clause.
package engine

import (
	"sort"
	"strings"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func executeDelete(env ExecEnv, s *Delete) (*ResultSet, error) {
	stmtPlan := env.planFor(s)
	t := planTable(stmtPlan)
	if t == nil {
		var err error
		if t, err = env.db.Get(env.tenant, s.Table); err != nil {
			return nil, err
		}
	}
	if err := checkForeignKeysBeforeDelete(env, t, s); err != nil {
		return nil, err
	}
	wal, err := beginWALAuto(env, s.Table)
	if err != nil {
		return nil, err
	}
	beforeDelTriggers, afterDelTriggers := planTriggers(stmtPlan, env, s.Table, storage.TriggerDelete)
	var beforeDelRunner, afterDelRunner *triggerListRunner
	if len(beforeDelTriggers) > 0 {
		beforeDelRunner = &triggerListRunner{triggers: beforeDelTriggers}
	}
	if len(afterDelTriggers) > 0 {
		afterDelRunner = &triggerListRunner{triggers: afterDelTriggers}
	}
	hasTriggers := len(beforeDelTriggers) > 0 || len(afterDelTriggers) > 0
	if !hasTriggers && len(s.Returning) == 0 && isSimpleRawPredicate(s.Where) {
		// newDMLPlan already resolved both of these while choosing this
		// statement's rollback snapshot shape; see planConstraintRows.
		colIndex := planColumnIndex(stmtPlan)
		if colIndex == nil {
			colIndex = simpleColumnIndex(t, s.Table)
		}
		if rowIDs, found := planConstraintRows(stmtPlan, func() ([]int, bool) {
			rows, _, _, ok := selectDeleteConstraintRows(t, colIndex, s.Where)
			return rows, ok
		}); found {
			return executeConstraintPointDelete(env, s, t, wal, colIndex, rowIDs)
		}
	}

	// Multi-row DELETE shifts every subsequent row's position. Drop the
	// incremental constraint cache and let the next point operation rebuild it.
	// The point path above delays invalidation until it actually removes a row.
	invalidateConstraintIndexes(t)

	// A DELETE without WHERE is still row-triggered. Preserve the compact
	// whole-table path only when no trigger can observe individual OLD rows.
	if s.Where == nil && !hasTriggers {
		del := len(t.Rows)
		if len(s.Returning) > 0 {
			tablePrefix := strings.ToLower(s.Table) + "."
			keys := newTableRowKeys(t.Cols, tablePrefix)
			returningRows := make([]Row, 0, len(t.Rows))
			for _, r := range t.Rows {
				returningRows = append(returningRows, buildTableRow(keys, r))
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
		rawPlan := &simpleSelectPlan{table: t, colIndex: colIndex, where: s.Where, filter: buildRawFilter(colIndex, s.Where)}
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
		t.Version++
		// A predicate that matched nothing left kept byte-for-byte identical
		// to t.Rows and oldToNew as the identity mapping -- skip the row-slice
		// swap and the full index rebuild in that case, the same way UPDATE
		// already avoids re-touching a table it didn't change (see this
		// file's UPDATE path) and executeConstraintPointDelete already gates
		// its own reindex on having actually removed a row.
		if del > 0 {
			t.Rows = kept
			t.ReindexSecondaryIndexRows(oldToNew)
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
	keys := newTableRowKeys(t.Cols, tablePrefix)
	hasBeforeDel := len(beforeDelTriggers) > 0
	hasAfterDel := len(afterDelTriggers) > 0
	for i, r := range t.Rows {
		if err := checkCtx(env.ctx); err != nil {
			return nil, err
		}
		row := buildTableRow(keys, r)
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
				if err := beforeDelRunner.fire(env, nil, row); err != nil {
					return nil, err
				}
			}
			if err := wal.logDelete(env, i, r, t.Cols); err != nil {
				return nil, err
			}
			if hasAfterDel {
				if err := afterDelRunner.fire(env, nil, row); err != nil {
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
	t.Version++
	// See the raw-predicate fast path above for why this is gated on del > 0.
	if del > 0 {
		t.Rows = kept
		t.ReindexSecondaryIndexRows(oldToNew)
		t.InvalidateStats()
		t.MarkDirtyFrom(-1) // DELETE is non-append; force full-table WAL
		markDependentMaterializedViewsStale(env, s.Table)
	}
	if len(s.Returning) > 0 {
		return projectReturningRows(env, t.Cols, s.Returning, returningRows)
	}
	return &ResultSet{Cols: []string{"deleted"}, Rows: []Row{{"deleted": del}}}, nil
}

func executeConstraintPointDelete(env ExecEnv, s *Delete, table *storage.Table, wal *walAuto, colIndex map[string]int, rowIDs []int) (*ResultSet, error) {
	rawPlan := &simpleSelectPlan{
		table:    table,
		colIndex: colIndex,
		where:    s.Where,
		filter:   buildRawFilter(colIndex, s.Where),
	}
	deleteRowID := -1
	for _, rowID := range rowIDs {
		if rowID < 0 || rowID >= len(table.Rows) {
			continue
		}
		if err := checkCtx(env.ctx); err != nil {
			return nil, err
		}
		match, err := evalRawWhere(rawPlan, table.Rows[rowID])
		if err != nil {
			return nil, err
		}
		if !match {
			continue
		}
		if err := wal.logDelete(env, rowID, table.Rows[rowID], table.Cols); err != nil {
			return nil, err
		}
		deleteRowID = rowID
		break
	}
	if err := wal.commit(); err != nil {
		return nil, err
	}

	if deleteRowID >= 0 {
		// Swap-and-pop: move the table's last row into the deleted slot and
		// truncate, instead of shifting every subsequent row down by one.
		// That shift was O(n) on every single-row DELETE and, worse, forced
		// a wholesale constraint-index invalidation afterward because every
		// index entry pointing past deleteRowID went stale. Swap-and-pop
		// moves exactly one row, so only that row's constraint- and
		// secondary-index entries need patching.
		oldLen := len(table.Rows)
		last := oldLen - 1
		deletedRow := table.Rows[deleteRowID]
		lastRow := table.Rows[last]
		patchConstraintIndexSwapRemove(table, deleteRowID, deletedRow, last, lastRow, oldLen)
		indexNames := rawIndexNames(table)
		sort.Strings(indexNames)
		if err := table.SwapRemoveSecondaryIndexRow(deleteRowID, deletedRow, last, lastRow, indexNames); err != nil {
			return nil, err
		}
		table.Rows[deleteRowID] = lastRow
		table.Rows[last] = nil
		table.Rows = table.Rows[:last]
		table.InvalidateStats()
		table.MarkDirtyFrom(-1)
		markDependentMaterializedViewsStale(env, s.Table)
	}
	// Preserve DELETE's established version semantics: a successfully executed
	// predicate advances the table version even when it matches no row.
	table.Version++
	deleted := 0
	if deleteRowID >= 0 {
		deleted = 1
	}
	return &ResultSet{Cols: []string{"deleted"}, Rows: []Row{{"deleted": deleted}}}, nil
}

// selectDeleteConstraintRows reuses a warm PRIMARY KEY/UNIQUE cache when one
// exists. On a cold table it scans only the constrained column and stops at the
// first match, avoiding an O(n) hash-map build immediately before DELETE would
// invalidate that map because row positions shift.
func selectDeleteConstraintRows(table *storage.Table, colIndex map[string]int, where Expr) ([]int, string, bool, bool) {
	if table == nil || where == nil {
		return nil, "", false, false
	}
	equalities := make(map[int]any)
	totalTerms := collectEqualityTerms(where, colIndex, equalities)
	for colIdx, value := range equalities {
		if colIdx < 0 || colIdx >= len(table.Cols) {
			continue
		}
		column := table.Cols[colIdx]
		if column.Constraint != storage.PrimaryKey && column.Constraint != storage.Unique {
			continue
		}
		if index := currentConstraintIndex(table, colIdx); index != nil {
			rows := lookupConstraintIndexRows(index, value)
			if rows == nil {
				rows = []int{}
			}
			return rows, column.Name, totalTerms != 1, true
		}
		for rowID, row := range table.Rows {
			if colIdx < len(row) && rawEqual(row[colIdx], value) {
				return []int{rowID}, column.Name, totalTerms != 1, true
			}
		}
		return []int{}, column.Name, totalTerms != 1, true
	}
	return nil, "", false, false
}

// tableRowKeys precomputes, once per statement, the two Row map keys every
// column needs: its lowercased name and tablePrefix+lowercased name. cols and
// tablePrefix never change from row to row within a single UPDATE/DELETE, so
// recomputing strings.ToLower and the prefix concatenation for every row (as
// buildTableRow used to do inline) was pure per-row waste on statements that
// touch many rows. Callers build this once before their row loop and pass it
// to buildTableRow for every row.
type tableRowKeys struct {
	key    []string // lowercased column name, one per column
	prefix []string // tablePrefix + lowercased column name, one per column
}

func newTableRowKeys(cols []storage.Column, tablePrefix string) tableRowKeys {
	key := make([]string, len(cols))
	prefix := make([]string, len(cols))
	for i, c := range cols {
		k := strings.ToLower(c.Name)
		key[i] = k
		prefix[i] = tablePrefix + k
	}
	return tableRowKeys{key: key, prefix: prefix}
}

func buildTableRow(keys tableRowKeys, values []any) Row {
	row := make(Row, len(keys.key)*2)
	fillTableRow(row, keys, values)
	return row
}

// fillTableRow writes the complete fixed-schema binding. Callers may reuse it
// only when no result row is retained after synchronous trigger execution.
func fillTableRow(row Row, keys tableRowKeys, values []any) {
	for i, key := range keys.key {
		val := values[i]
		row[key] = val
		row[keys.prefix[i]] = val
	}
}

func markDependentMaterializedViewsStale(env ExecEnv, tableName string) {
	catalog := env.db.Catalog()
	// Asked after every successful mutating statement, so the answer for a
	// database with no materialized views has to be free. Everything below —
	// lower-casing the name, splitting it, building the dependency key — is
	// allocation the common case should not do.
	if !catalog.HasMaterializedViews() {
		return
	}
	if strings.HasPrefix(strings.ToLower(tableName), "__mv_") {
		return
	}
	schema, name := splitObjectName(tableName)
	if schema == "" {
		schema = "main"
	}
	_ = catalog.MarkMaterializedViewsStaleByDependency(schema, name)
}

func projectReturningRows(env ExecEnv, cols []storage.Column, projs []SelectItem, rows []Row) (*ResultSet, error) {
	outRows := make([]Row, 0, len(rows))
	outCols := returningOutputCols(cols, projs)

	// Every output key is a pure function of the projection list and the
	// schema, neither of which changes from row to row. Deriving them inside
	// the loop meant two strings.ToLower per column per row for RETURNING *,
	// and an fmt.Sprintf plus a ToLower per row for any unaliased expression
	// (RETURNING id*2), all of it recomputing the same strings.
	lowerCols := make([]string, len(cols))
	for i, c := range cols {
		lowerCols[i] = strings.ToLower(c.Name)
	}
	projKeys := make([]string, len(projs))
	for i, it := range projs {
		if !it.Star {
			projKeys[i] = strings.ToLower(projName(it, i))
		}
	}

	for _, r := range rows {
		if err := checkCtx(env.ctx); err != nil {
			return nil, err
		}
		out := make(Row, len(outCols))
		for i, it := range projs {
			if it.Star {
				for ci := range cols {
					val, _ := getValLower(r, lowerCols[ci])
					out[lowerCols[ci]] = val
				}
				continue
			}
			val, err := evalExpr(env, it.Expr, r)
			if err != nil {
				return nil, err
			}
			out[projKeys[i]] = val
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
