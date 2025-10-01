// Package engine implements the tinySQL execution engine.
//
// What: This module evaluates parsed SQL statements (AST) against the storage
// layer and produces ResultSets. It covers DDL/DML/SELECT, joins, grouping,
// aggregates, expression evaluation, simple functions (JSON_*, DATEDIFF, etc.),
// and a minimal tri-state boolean logic (true/false/unknown).
//
// How: The executor converts tables to row maps with both qualified and
// unqualified column keys, applies WHERE/GROUP/HAVING/ORDER/LIMIT/OFFSET, and
// optionally combines results with UNION/EXCEPT/INTERSECT. Expression
// evaluation is recursive over a small algebra of literals, variables, unary/
// binary ops, IS NULL, and function calls. Aggregate evaluation runs per-
// group with reusable helpers shared with the scalar evaluator.
//
// Why: Keeping execution self-contained and data-structure driven (Row maps
// and simple slices) makes the engine easy to reason about and to extend with
// new functions, operators, and clauses without introducing heavy planners.
package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// Row represents a single result row mapped by lower-cased column name.
// Keys include both qualified (table.column) and unqualified (column) names
// to simplify expression evaluation and projection.
type Row map[string]any

// ResultSet holds the column order and the returned rows from a query.
// Cols preserve the display order; Rows store values in a case-insensitive map.
type ResultSet struct {
	Cols []string
	Rows []Row
}

type ExecEnv struct {
	ctx    context.Context
	tenant string
	db     *storage.DB
	ctes   map[string]*ResultSet // For CTE support
}

// Execute runs a parsed SQL statement against the given storage DB and tenant.
// It dispatches to handlers per statement kind and returns a ResultSet for
// SELECT (nil for DDL/DML). The context is checked at safe points to support
// cancellation.
func Execute(ctx context.Context, db *storage.DB, tenant string, stmt Statement) (*ResultSet, error) {
	env := ExecEnv{ctx: ctx, tenant: tenant, db: db}
	switch s := stmt.(type) {
	case *CreateTable:
		return executeCreateTable(env, s)
	case *DropTable:
		return executeDropTable(env, s)
	case *Insert:
		return executeInsert(env, s)
	case *Update:
		return executeUpdate(env, s)
	case *Delete:
		return executeDelete(env, s)
	case *Select:
		return executeSelect(env, s)
	}
	return nil, fmt.Errorf("unknown statement")
}

// -------------------- Statement Handlers --------------------

func executeCreateTable(env ExecEnv, s *CreateTable) (*ResultSet, error) {
	if s.AsSelect == nil {
		t := storage.NewTable(s.Name, s.Cols, s.IsTemp)
		env.db.Put(env.tenant, t)
		return nil, nil
	}
	rs, err := Execute(env.ctx, env.db, env.tenant, s.AsSelect)
	if err != nil {
		return nil, err
	}
	cols := make([]storage.Column, len(rs.Cols))
	if len(rs.Rows) > 0 {
		for i, c := range rs.Cols {
			cols[i] = storage.Column{Name: c, Type: inferType(rs.Rows[0][strings.ToLower(c)])}
		}
	} else {
		for i, c := range rs.Cols {
			cols[i] = storage.Column{Name: c, Type: storage.TextType}
		}
	}
	t := storage.NewTable(s.Name, cols, s.IsTemp)
	for _, r := range rs.Rows {
		row := make([]any, len(cols))
		for i, c := range cols {
			row[i] = r[strings.ToLower(c.Name)]
		}
		t.Rows = append(t.Rows, row)
	}
	env.db.Put(env.tenant, t)
	return nil, nil
}

func executeDropTable(env ExecEnv, s *DropTable) (*ResultSet, error) {
	return nil, env.db.Drop(env.tenant, s.Name)
}

func executeInsert(env ExecEnv, s *Insert) (*ResultSet, error) {
	t, err := env.db.Get(env.tenant, s.Table)
	if err != nil {
		return nil, err
	}
	if len(s.Cols) > 0 && len(s.Vals) != len(s.Cols) {
		return nil, fmt.Errorf("INSERT column/value mismatch")
	}
	tmp := Row{}
	if len(s.Cols) == 0 {
		return executeInsertAllColumns(env, s, t, tmp)
	}
	return executeInsertSpecificColumns(env, s, t, tmp)
}

func executeInsertAllColumns(env ExecEnv, s *Insert, t *storage.Table, tmp Row) (*ResultSet, error) {
	if len(s.Vals) != len(t.Cols) {
		return nil, fmt.Errorf("INSERT expects %d values", len(t.Cols))
	}
	row := make([]any, len(t.Cols))
	for i, e := range s.Vals {
		if err := checkCtx(env.ctx); err != nil {
			return nil, err
		}
		v, err := evalExpr(env, e, tmp)
		if err != nil {
			return nil, err
		}
		cv, err := coerceToTypeAllowNull(v, t.Cols[i].Type)
		if err != nil {
			return nil, fmt.Errorf("column %q: %w", t.Cols[i].Name, err)
		}
		row[i] = cv
	}
	t.Rows = append(t.Rows, row)
	t.Version++
	return nil, nil
}

func executeInsertSpecificColumns(env ExecEnv, s *Insert, t *storage.Table, tmp Row) (*ResultSet, error) {
	row := make([]any, len(t.Cols))
	for i := range row {
		row[i] = nil
	}
	for i, name := range s.Cols {
		idx, err := t.ColIndex(name)
		if err != nil {
			return nil, err
		}
		v, err := evalExpr(env, s.Vals[i], tmp)
		if err != nil {
			return nil, err
		}
		cv, err := coerceToTypeAllowNull(v, t.Cols[idx].Type)
		if err != nil {
			return nil, fmt.Errorf("column %q: %w", t.Cols[idx].Name, err)
		}
		row[idx] = cv
	}
	t.Rows = append(t.Rows, row)
	t.Version++
	return nil, nil
}

func executeUpdate(env ExecEnv, s *Update) (*ResultSet, error) {
	t, err := env.db.Get(env.tenant, s.Table)
	if err != nil {
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
	for ri, r := range t.Rows {
		if err := checkCtx(env.ctx); err != nil {
			return nil, err
		}
		row := Row{}
		for i, c := range t.Cols {
			putVal(row, c.Name, r[i])
			putVal(row, s.Table+"."+c.Name, r[i])
		}
		ok := true
		if s.Where != nil {
			v, err := evalExpr(env, s.Where, row)
			if err != nil {
				return nil, err
			}
			ok = (toTri(v) == tvTrue)
		}
		if ok {
			for i, ex := range setIdx {
				v, err := evalExpr(env, ex, row)
				if err != nil {
					return nil, err
				}
				cv, err := coerceToTypeAllowNull(v, t.Cols[i].Type)
				if err != nil {
					return nil, err
				}
				t.Rows[ri][i] = cv
			}
			n++
		}
	}
	t.Version++
	return &ResultSet{Cols: []string{"updated"}, Rows: []Row{{"updated": n}}}, nil
}

func executeDelete(env ExecEnv, s *Delete) (*ResultSet, error) {
	t, err := env.db.Get(env.tenant, s.Table)
	if err != nil {
		return nil, err
	}
	var kept [][]any
	del := 0
	for _, r := range t.Rows {
		if err := checkCtx(env.ctx); err != nil {
			return nil, err
		}
		row := Row{}
		for i, c := range t.Cols {
			putVal(row, c.Name, r[i])
			putVal(row, s.Table+"."+c.Name, r[i])
		}
		keep := true
		if s.Where != nil {
			v, err := evalExpr(env, s.Where, row)
			if err != nil {
				return nil, err
			}
			if toTri(v) == tvTrue {
				keep = false
			}
		}
		if keep {
			kept = append(kept, r)
		} else {
			del++
		}
	}
	t.Rows = kept
	t.Version++
	return &ResultSet{Cols: []string{"deleted"}, Rows: []Row{{"deleted": del}}}, nil
}

func executeSelect(env ExecEnv, s *Select) (*ResultSet, error) {
	// Process CTEs first
	cteEnv := env
	if len(s.CTEs) > 0 {
		// Create a new environment with CTE tables
		cteEnv = ExecEnv{
			ctx:    env.ctx,
			db:     env.db,
			tenant: env.tenant,
			ctes:   make(map[string]*ResultSet),
		}

		// Execute each CTE and store the result
		for _, cte := range s.CTEs {
			cteResult, err := executeSelect(env, cte.Select)
			if err != nil {
				return nil, fmt.Errorf("CTE %s: %v", cte.Name, err)
			}
			cteEnv.ctes[cte.Name] = cteResult
		}
	}

	// FROM
	var leftRows []Row

	// Check if FROM table is a CTE
	if cteEnv.ctes != nil {
		if cteResult, exists := cteEnv.ctes[s.From.Table]; exists {
			// Convert CTE result to rows
			leftRows = make([]Row, len(cteResult.Rows))
			for i, row := range cteResult.Rows {
				leftRows[i] = make(Row)
				// Copy all values from CTE result row
				for k, v := range row {
					leftRows[i][k] = v
					// Also add qualified names
					leftRows[i][s.From.Table+"."+k] = v
				}
			}
		} else {
			// Regular table
			leftT, err := cteEnv.db.Get(cteEnv.tenant, s.From.Table)
			if err != nil {
				return nil, err
			}
			leftRows, _ = rowsFromTable(leftT, aliasOr(s.From))
		}
	} else {
		// Regular table
		leftT, err := env.db.Get(env.tenant, s.From.Table)
		if err != nil {
			return nil, err
		}
		leftRows, _ = rowsFromTable(leftT, aliasOr(s.From))
	}

	cur := leftRows

	// JOINs
	cur, err := processJoins(cteEnv, s.Joins, cur)
	if err != nil {
		return nil, err
	}

	// WHERE
	filtered, err := applyWhereClause(cteEnv, s.Where, cur)
	if err != nil {
		return nil, err
	}

	// GROUP/HAVING
	outRows, outCols, err := processGroupByHaving(env, s, filtered)
	if err != nil {
		return nil, err
	}

	// DISTINCT
	if s.Distinct {
		outRows = distinctRows(outRows, outCols)
	}

	// ORDER BY
	if len(s.OrderBy) > 0 {
		sort.SliceStable(outRows, func(i, j int) bool {
			a := outRows[i]
			b := outRows[j]
			for _, oi := range s.OrderBy {
				av, _ := getVal(a, oi.Col)
				bv, _ := getVal(b, oi.Col)
				cmp := compareForOrder(av, bv, oi.Desc)
				if cmp == 0 {
					continue
				}
				if oi.Desc {
					return cmp > 0
				}
				return cmp < 0
			}
			return false
		})
	}

	// OFFSET/LIMIT (applied before UNION to each individual SELECT)
	baseRows := applyOffsetLimit(s, outRows)

	// Handle UNION operations
	resultRows := baseRows
	resultCols := outCols

	if s.Union != nil {
		var err error
		resultRows, resultCols, err = processUnionClauses(env, s.Union, resultRows, resultCols)
		if err != nil {
			return nil, err
		}
	}

	if len(resultCols) == 0 {
		resultCols = columnsFromRows(resultRows)
	}
	return &ResultSet{Cols: resultCols, Rows: resultRows}, nil
}

func processUnionClauses(env ExecEnv, union *UnionClause, leftRows []Row, leftCols []string) ([]Row, []string, error) {
	resultRows := leftRows
	resultCols := leftCols

	current := union
	for current != nil {
		// Execute the right-hand SELECT
		rightResult, err := executeSelect(env, current.Right)
		if err != nil {
			return nil, nil, err
		}

		// Validate column compatibility
		if len(rightResult.Cols) != len(resultCols) {
			return nil, nil, fmt.Errorf("UNION: column count mismatch between queries (%d vs %d)",
				len(resultCols), len(rightResult.Cols))
		}

		// Process the union based on type
		switch current.Type {
		case UnionAll:
			// UNION ALL: Just append all rows
			resultRows = append(resultRows, rightResult.Rows...)

		case UnionDistinct:
			// UNION: Append and then remove duplicates
			resultRows = append(resultRows, rightResult.Rows...)
			resultRows = distinctRows(resultRows, resultCols)

		case Except:
			// EXCEPT: Remove rows that exist in the right result
			resultRows = exceptRows(resultRows, rightResult.Rows, resultCols)

		case Intersect:
			// INTERSECT: Keep only rows that exist in both results
			resultRows = intersectRows(resultRows, rightResult.Rows, resultCols)
		}

		current = current.Next
	}

	return resultRows, resultCols, nil
}

func exceptRows(leftRows, rightRows []Row, cols []string) []Row {
	// Create a set of right rows for fast lookup
	rightSet := make(map[string]bool)
	for _, r := range rightRows {
		key := rowSignature(r, cols)
		rightSet[key] = true
	}

	// Keep only left rows that are not in the right set
	var result []Row
	for _, l := range leftRows {
		key := rowSignature(l, cols)
		if !rightSet[key] {
			result = append(result, l)
		}
	}
	return result
}

func intersectRows(leftRows, rightRows []Row, cols []string) []Row {
	// Create a set of right rows for fast lookup
	rightSet := make(map[string]bool)
	for _, r := range rightRows {
		key := rowSignature(r, cols)
		rightSet[key] = true
	}

	// Keep only left rows that are also in the right set
	var result []Row
	seen := make(map[string]bool)
	for _, l := range leftRows {
		key := rowSignature(l, cols)
		if rightSet[key] && !seen[key] {
			result = append(result, l)
			seen[key] = true
		}
	}
	return result
}

func rowSignature(row Row, cols []string) string {
	var parts []string
	for _, col := range cols {
		val, _ := getVal(row, col)
		parts = append(parts, fmt.Sprintf("%v", val))
	}
	return strings.Join(parts, "|")
}

func processJoins(env ExecEnv, joins []JoinClause, cur []Row) ([]Row, error) {
	for _, j := range joins {
		rt, err := env.db.Get(env.tenant, j.Right.Table)
		if err != nil {
			return nil, err
		}
		rightRows, _ := rowsFromTable(rt, aliasOr(j.Right))

		switch j.Type {
		case JoinInner:
			cur, err = processInnerJoin(env, cur, rightRows, j.On)
		case JoinLeft:
			cur, err = processLeftJoin(env, cur, rightRows, j.On, aliasOr(j.Right), rt)
		case JoinRight:
			cur, err = processRightJoin(env, cur, rightRows, j.On)
		}
		if err != nil {
			return nil, err
		}
	}
	return cur, nil
}

func processInnerJoin(env ExecEnv, leftRows, rightRows []Row, onCondition Expr) ([]Row, error) {
	joined := make([]Row, 0, len(leftRows)*len(rightRows)/4) // Estimate result size
	for _, l := range leftRows {
		if err := checkCtx(env.ctx); err != nil {
			return nil, err
		}
		for _, r := range rightRows {
			m := mergeRows(l, r)
			ok := true
			if onCondition != nil {
				val, err := evalExpr(env, onCondition, m)
				if err != nil {
					return nil, err
				}
				ok = (toTri(val) == tvTrue)
			}
			if ok {
				joined = append(joined, m)
			}
		}
	}
	return joined, nil
}

func processLeftJoin(env ExecEnv, leftRows, rightRows []Row, onCondition Expr, rightAlias string, rightTable *storage.Table) ([]Row, error) {
	joined := make([]Row, 0, len(leftRows)) // At least one row per left row
	for _, l := range leftRows {
		if err := checkCtx(env.ctx); err != nil {
			return nil, err
		}
		matched := false
		for _, r := range rightRows {
			m := mergeRows(l, r)
			ok := true
			if onCondition != nil {
				val, err := evalExpr(env, onCondition, m)
				if err != nil {
					return nil, err
				}
				ok = (toTri(val) == tvTrue)
			}
			if ok {
				joined = append(joined, m)
				matched = true
			}
		}
		if !matched {
			m := cloneRow(l)
			addRightNulls(m, rightAlias, rightTable)
			joined = append(joined, m)
		}
	}
	return joined, nil
}

func processRightJoin(env ExecEnv, leftRows, rightRows []Row, onCondition Expr) ([]Row, error) {
	joined := make([]Row, 0, len(rightRows)) // At least one row per right row
	var leftKeys []string
	if len(leftRows) > 0 {
		leftKeys = keysOfRow(leftRows[0])
	}
	for _, r := range rightRows {
		if err := checkCtx(env.ctx); err != nil {
			return nil, err
		}
		matched := false
		for _, l := range leftRows {
			m := mergeRows(l, r)
			ok := true
			if onCondition != nil {
				val, err := evalExpr(env, onCondition, m)
				if err != nil {
					return nil, err
				}
				ok = (toTri(val) == tvTrue)
			}
			if ok {
				joined = append(joined, m)
				matched = true
			}
		}
		if !matched {
			m := cloneRow(r)
			for _, k := range leftKeys {
				m[k] = nil
			}
			joined = append(joined, m)
		}
	}
	return joined, nil
}

func applyWhereClause(env ExecEnv, where Expr, rows []Row) ([]Row, error) {
	if where == nil {
		return rows, nil
	}
	filtered := make([]Row, 0, len(rows)/2) // Estimate half will match
	for _, r := range rows {
		if err := checkCtx(env.ctx); err != nil {
			return nil, err
		}
		v, err := evalExpr(env, where, r)
		if err != nil {
			return nil, err
		}
		if toTri(v) == tvTrue {
			filtered = append(filtered, r)
		}
	}
	return filtered, nil
}

func processGroupByHaving(env ExecEnv, s *Select, filtered []Row) ([]Row, []string, error) {
	needAgg := len(s.GroupBy) > 0 || anyAggInSelect(s.Projs) || isAggregate(s.Having)

	if needAgg {
		return processAggregateQuery(env, s, filtered)
	}
	return processNonAggregateQuery(env, s, filtered)
}

func processAggregateQuery(env ExecEnv, s *Select, filtered []Row) ([]Row, []string, error) {
	groups := make(map[string][]Row, len(filtered)/2) // Estimate group count
	orderKeys := make([]string, 0, len(filtered)/2)
	outRows := make([]Row, 0, len(filtered)/2)
	outCols := make([]string, 0, len(s.Projs))

	for _, r := range filtered {
		if err := checkCtx(env.ctx); err != nil {
			return nil, nil, err
		}
		var parts []string
		for _, g := range s.GroupBy {
			v, err := evalExpr(env, &g, r)
			if err != nil {
				return nil, nil, err
			}
			parts = append(parts, fmtKeyPart(v))
		}
		ks := strings.Join(parts, "\x1f")
		if _, ok := groups[ks]; !ok {
			orderKeys = append(orderKeys, ks)
		}
		groups[ks] = append(groups[ks], r)
	}

	for _, k := range orderKeys {
		rows := groups[k]
		if s.Having != nil {
			hv, err := evalAggregate(env, s.Having, rows)
			if err != nil {
				return nil, nil, err
			}
			if toTri(hv) != tvTrue {
				continue
			}
		}
		out := Row{}
		for i, it := range s.Projs {
			if it.Star {
				if len(rows) > 0 {
					for col, v := range rows[0] {
						if strings.Contains(col, ".") {
							putVal(out, col, v)
							outCols = appendUnique(outCols, col)
						}
					}
				}
				continue
			}
			name := projName(it, i)
			var val any
			var err error
			if isAggregate(it.Expr) || len(s.GroupBy) > 0 {
				val, err = evalAggregate(env, it.Expr, rows)
			} else {
				val, err = evalExpr(env, it.Expr, rows[0])
			}
			if err != nil {
				return nil, nil, err
			}
			putVal(out, name, val)
			outCols = appendUnique(outCols, name)
		}
		outRows = append(outRows, out)
	}
	return outRows, outCols, nil
}

func processNonAggregateQuery(env ExecEnv, s *Select, filtered []Row) ([]Row, []string, error) {
	outRows := make([]Row, 0, len(filtered))
	outCols := make([]string, 0, len(s.Projs))

	for _, r := range filtered {
		if err := checkCtx(env.ctx); err != nil {
			return nil, nil, err
		}
		out := Row{}
		for i, it := range s.Projs {
			if it.Star {
				for col, v := range r {
					if strings.Contains(col, ".") {
						putVal(out, col, v)
						outCols = appendUnique(outCols, col)
					}
				}
				continue
			}
			val, err := evalExpr(env, it.Expr, r)
			if err != nil {
				return nil, nil, err
			}
			name := projName(it, i)
			putVal(out, name, val)
			outCols = appendUnique(outCols, name)
		}
		outRows = append(outRows, out)
	}
	return outRows, outCols, nil
}

func applyOffsetLimit(s *Select, rows []Row) []Row {
	start := 0
	if s.Offset != nil && *s.Offset > 0 {
		start = *s.Offset
	}
	if start > len(rows) {
		return []Row{}
	}
	rows = rows[start:]

	if s.Limit != nil && *s.Limit < len(rows) {
		rows = rows[:*s.Limit]
	}
	return rows
}

// -------------------- Eval, Aggregates, Helpers --------------------

func getVal(row Row, name string) (any, bool) { v, ok := row[strings.ToLower(name)]; return v, ok }
func putVal(row Row, key string, val any)     { row[strings.ToLower(key)] = val }
func isNull(v any) bool                       { return v == nil }
func numeric(v any) (float64, bool) {
	switch x := v.(type) {
	case int:
		return float64(x), true
	case int64:
		return float64(x), true
	case float64:
		return x, true
	}
	return 0, false
}
func truthy(v any) bool {
	switch x := v.(type) {
	case nil:
		return false
	case bool:
		return x
	case int:
		return x != 0
	case float64:
		return x != 0
	case string:
		return x != ""
	default:
		return false
	}
}

// tri-state
const (
	tvFalse   = 0
	tvTrue    = 1
	tvUnknown = 2
)

func toTri(v any) int {
	if v == nil {
		return tvUnknown
	}
	if truthy(v) {
		return tvTrue
	}
	return tvFalse
}
func triNot(t int) int {
	if t == tvTrue {
		return tvFalse
	}
	if t == tvFalse {
		return tvTrue
	}
	return tvUnknown
}
func triAnd(a, b int) int {
	if a == tvFalse || b == tvFalse {
		return tvFalse
	}
	if a == tvTrue && b == tvTrue {
		return tvTrue
	}
	return tvUnknown
}
func triOr(a, b int) int {
	if a == tvTrue || b == tvTrue {
		return tvTrue
	}
	if a == tvFalse && b == tvFalse {
		return tvFalse
	}
	return tvUnknown
}

func compare(a, b any) (int, error) {
	if a == nil || b == nil {
		return 0, errors.New("cannot compare with NULL")
	}
	switch ax := a.(type) {
	case int:
		return compareInt(ax, b)
	case float64:
		return compareFloat(ax, b)
	case string:
		return compareString(ax, b)
	case bool:
		return compareBool(ax, b)
	}
	if fmt.Sprintf("%v", a) == fmt.Sprintf("%v", b) {
		return 0, nil
	}
	return 0, fmt.Errorf("incomparable %T and %T", a, b)
}

func compareInt(ax int, b any) (int, error) {
	if f, ok := numeric(b); ok {
		af := float64(ax)
		if af < f {
			return -1, nil
		}
		if af > f {
			return 1, nil
		}
		return 0, nil
	}
	return 0, fmt.Errorf("incomparable int and %T", b)
}

func compareFloat(ax float64, b any) (int, error) {
	if f, ok := numeric(b); ok {
		if ax < f {
			return -1, nil
		}
		if ax > f {
			return 1, nil
		}
		return 0, nil
	}
	return 0, fmt.Errorf("incomparable float64 and %T", b)
}

func compareString(ax string, b any) (int, error) {
	if bs, ok := b.(string); ok {
		if ax < bs {
			return -1, nil
		}
		if ax > bs {
			return 1, nil
		}
		return 0, nil
	}
	return 0, fmt.Errorf("incomparable string and %T", b)
}

func compareBool(ax bool, b any) (int, error) {
	if bb, ok := b.(bool); ok {
		if !ax && bb {
			return -1, nil
		}
		if ax && !bb {
			return 1, nil
		}
		return 0, nil
	}
	return 0, fmt.Errorf("incomparable bool and %T", b)
}
func compareForOrder(a, b any, desc bool) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		if desc {
			return -1
		}
		return 1
	}
	if b == nil {
		if desc {
			return 1
		}
		return -1
	}
	c, err := compare(a, b)
	if err != nil {
		return 0
	}
	return c
}

func checkCtx(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

func evalExpr(env ExecEnv, e Expr, row Row) (any, error) {
	if err := checkCtx(env.ctx); err != nil {
		return nil, err
	}
	switch ex := e.(type) {
	case *Literal:
		return ex.Val, nil
	case *VarRef:
		return evalVarRef(ex, row)
	case *IsNull:
		return evalIsNull(env, ex, row)
	case *Unary:
		return evalUnary(env, ex, row)
	case *Binary:
		return evalBinary(env, ex, row)
	case *FuncCall:
		return evalFuncCall(env, ex, row)
	}
	return nil, fmt.Errorf("unknown expression")
}

func evalVarRef(ex *VarRef, row Row) (any, error) {
	if v, ok := getVal(row, ex.Name); ok {
		return v, nil
	}
	if strings.Contains(ex.Name, ".") {
		return nil, fmt.Errorf("unknown column reference %q", ex.Name)
	}
	if v, ok := getVal(row, ex.Name); ok {
		return v, nil
	}
	return nil, fmt.Errorf("unknown column %q", ex.Name)
}

func evalIsNull(env ExecEnv, ex *IsNull, row Row) (any, error) {
	v, err := evalExpr(env, ex.Expr, row)
	if err != nil {
		return nil, err
	}
	is := isNull(v)
	if ex.Negate {
		return !is, nil
	}
	return is, nil
}

func evalUnary(env ExecEnv, ex *Unary, row Row) (any, error) {
	v, err := evalExpr(env, ex.Expr, row)
	if err != nil {
		return nil, err
	}
	switch ex.Op {
	case "+":
		if f, ok := numeric(v); ok {
			return f, nil
		}
		if v == nil {
			return nil, nil
		}
		return nil, fmt.Errorf("unary + non-numeric")
	case "-":
		if f, ok := numeric(v); ok {
			return -f, nil
		}
		if v == nil {
			return nil, nil
		}
		return nil, fmt.Errorf("unary - non-numeric")
	case "NOT":
		return triToValue(triNot(toTri(v))), nil
	}
	return nil, fmt.Errorf("unknown unary operator: %s", ex.Op)
}

func evalBinary(env ExecEnv, ex *Binary, row Row) (any, error) {
	if ex.Op == "AND" || ex.Op == "OR" {
		return evalLogicalBinary(env, ex, row)
	}

	lv, err := evalExpr(env, ex.Left, row)
	if err != nil {
		return nil, err
	}
	rv, err := evalExpr(env, ex.Right, row)
	if err != nil {
		return nil, err
	}

	switch ex.Op {
	case "+", "-", "*", "/":
		return evalArithmeticBinary(ex.Op, lv, rv)
	case "=", "!=", "<>", "<", "<=", ">", ">=":
		return evalComparisonBinary(ex.Op, lv, rv)
	}
	return nil, fmt.Errorf("unknown binary operator: %s", ex.Op)
}

func evalLogicalBinary(env ExecEnv, ex *Binary, row Row) (any, error) {
	lv, err := evalExpr(env, ex.Left, row)
	if err != nil {
		return nil, err
	}
	if ex.Op == "AND" && toTri(lv) == tvFalse {
		return false, nil
	}
	if ex.Op == "OR" && toTri(lv) == tvTrue {
		return true, nil
	}
	rv, err := evalExpr(env, ex.Right, row)
	if err != nil {
		return nil, err
	}
	if ex.Op == "AND" {
		return triToValue(triAnd(toTri(lv), toTri(rv))), nil
	}
	return triToValue(triOr(toTri(lv), toTri(rv))), nil
}

func evalArithmeticBinary(op string, lv, rv any) (any, error) {
	if lv == nil || rv == nil {
		return nil, nil
	}
	lf, lok := numeric(lv)
	rf, rok := numeric(rv)
	if !(lok && rok) {
		return nil, fmt.Errorf("%s expects numeric", op)
	}
	switch op {
	case "+":
		return lf + rf, nil
	case "-":
		return lf - rf, nil
	case "*":
		return lf * rf, nil
	case "/":
		if rf == 0 {
			return nil, errors.New("division by zero")
		}
		return lf / rf, nil
	}
	return nil, fmt.Errorf("unknown arithmetic operator: %s", op)
}

func evalComparisonBinary(op string, lv, rv any) (any, error) {
	if lv == nil || rv == nil {
		return nil, nil
	}
	cmp, err := compare(lv, rv)
	if err != nil {
		return nil, err
	}
	switch op {
	case "=":
		return cmp == 0, nil
	case "!=", "<>":
		return cmp != 0, nil
	case "<":
		return cmp < 0, nil
	case "<=":
		return cmp <= 0, nil
	case ">":
		return cmp > 0, nil
	case ">=":
		return cmp >= 0, nil
	}
	return nil, fmt.Errorf("unknown comparison operator: %s", op)
}

func evalFuncCall(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	switch ex.Name {
	case "COALESCE":
		return evalCoalesce(env, ex.Args, row)
	case "NULLIF":
		return evalNullif(env, ex.Args, row)
	case "ISNULL":
		return evalIsNullFunc(env, ex.Args, row)
	case "JSON_GET":
		return evalJSONGet(env, ex.Args, row)
	case "JSON_SET", "JSON_EXTRACT":
		return evalJSONExtended(env, ex, row)
	case "COUNT":
		return evalCountSingle(env, ex, row)
	case "SUM", "AVG", "MIN", "MAX":
		return evalAggregateSingle(env, ex, row)
	case "NOW", "CURRENT_TIME":
		return time.Now(), nil
	case "CURRENT_DATE":
		return time.Now().Truncate(24 * time.Hour), nil
	case "DATEDIFF":
		return evalDateDiff(env, ex, row)
	case "LTRIM":
		return evalLTrim(env, ex.Args, row)
	case "RTRIM":
		return evalRTrim(env, ex.Args, row)
	case "TRIM":
		return evalTrim(env, ex.Args, row)
	}
	return nil, fmt.Errorf("unknown function: %s", ex.Name)
}

func evalCoalesce(env ExecEnv, args []Expr, row Row) (any, error) {
	for _, a := range args {
		v, err := evalExpr(env, a, row)
		if err != nil {
			return nil, err
		}
		if v != nil {
			return v, nil
		}
	}
	return nil, nil
}

func evalNullif(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("NULLIF expects 2 args")
	}
	lv, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	rv, err := evalExpr(env, args[1], row)
	if err != nil {
		return nil, err
	}
	if lv == nil {
		return nil, nil
	}
	if rv == nil {
		return lv, nil
	}
	cmp, err := compare(lv, rv)
	if err != nil {
		return nil, err
	}
	if cmp == 0 {
		return nil, nil
	}
	return lv, nil
}

func evalJSONGet(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("JSON_GET expects (json, path)")
	}
	jv, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	pv, err := evalExpr(env, args[1], row)
	if err != nil {
		return nil, err
	}
	ps, _ := pv.(string)
	return jsonGet(jv, ps), nil
}

func evalJSONExtended(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	switch ex.Name {
	case "JSON_SET":
		if len(ex.Args) != 3 {
			return nil, fmt.Errorf("JSON_SET expects (json, path, value)")
		}
		jv, err := evalExpr(env, ex.Args[0], row)
		if err != nil {
			return nil, err
		}
		pv, err := evalExpr(env, ex.Args[1], row)
		if err != nil {
			return nil, err
		}
		val, err := evalExpr(env, ex.Args[2], row)
		if err != nil {
			return nil, err
		}
		ps, _ := pv.(string)
		return jsonSet(jv, ps, val), nil

	case "JSON_EXTRACT":
		// Alias for JSON_GET
		if len(ex.Args) != 2 {
			return nil, fmt.Errorf("JSON_EXTRACT expects (json, path)")
		}
		jv, err := evalExpr(env, ex.Args[0], row)
		if err != nil {
			return nil, err
		}
		pv, err := evalExpr(env, ex.Args[1], row)
		if err != nil {
			return nil, err
		}
		ps, _ := pv.(string)
		return jsonGet(jv, ps), nil
	}
	return nil, fmt.Errorf("unknown JSON function: %s", ex.Name)
}

func evalCountSingle(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if ex.Star {
		return 1, nil
	}
	if len(ex.Args) != 1 {
		return nil, fmt.Errorf("COUNT expects 1 arg")
	}
	v, err := evalExpr(env, ex.Args[0], row)
	if err != nil {
		return nil, err
	}
	if v == nil {
		return 0, nil
	}
	return 1, nil
}

func evalAggregateSingle(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if len(ex.Args) != 1 {
		return nil, fmt.Errorf("%s expects 1 arg", ex.Name)
	}
	v, err := evalExpr(env, ex.Args[0], row)
	if err != nil {
		return nil, err
	}
	return v, nil
}

func evalDateDiff(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if len(ex.Args) != 3 {
		return nil, fmt.Errorf("DATEDIFF expects 3 arguments: (unit, start_date, end_date)")
	}

	// Get the unit (HOURS, DAYS, MINUTES, etc.)
	unitVal, err := evalExpr(env, ex.Args[0], row)
	if err != nil {
		return nil, err
	}
	unit, ok := unitVal.(string)
	if !ok {
		return nil, fmt.Errorf("DATEDIFF unit must be a string")
	}

	// Get start date
	startVal, err := evalExpr(env, ex.Args[1], row)
	if err != nil {
		return nil, err
	}

	// Get end date
	endVal, err := evalExpr(env, ex.Args[2], row)
	if err != nil {
		return nil, err
	}

	// Convert values to time.Time
	startTime, err := parseTimeValue(startVal)
	if err != nil {
		return nil, fmt.Errorf("DATEDIFF start_date: %v", err)
	}

	endTime, err := parseTimeValue(endVal)
	if err != nil {
		return nil, fmt.Errorf("DATEDIFF end_date: %v", err)
	}

	// Calculate difference
	diff := endTime.Sub(startTime)

	// Return based on unit
	switch strings.ToUpper(unit) {
	case "HOURS":
		return int(diff.Hours()), nil
	case "MINUTES":
		return int(diff.Minutes()), nil
	case "SECONDS":
		return int(diff.Seconds()), nil
	case "DAYS":
		return int(diff.Hours() / 24), nil
	case "WEEKS":
		return int(diff.Hours() / (24 * 7)), nil
	case "MONTHS":
		// Approximate: 30 days per month
		return int(diff.Hours() / (24 * 30)), nil
	case "YEARS":
		// Approximate: 365 days per year
		return int(diff.Hours() / (24 * 365)), nil
	default:
		return nil, fmt.Errorf("unsupported DATEDIFF unit: %s (supported: HOURS, MINUTES, SECONDS, DAYS, WEEKS, MONTHS, YEARS)", unit)
	}
}

func parseTimeValue(val any) (time.Time, error) {
	if val == nil {
		return time.Time{}, fmt.Errorf("cannot parse nil as time")
	}

	switch v := val.(type) {
	case time.Time:
		return v, nil
	case string:
		// Try various time formats
		formats := []string{
			time.RFC3339,
			"2006-01-02 15:04:05",
			"2006-01-02T15:04:05",
			"2006-01-02 15:04",
			"2006-01-02",
			"15:04:05",
			"15:04",
		}

		for _, format := range formats {
			if t, err := time.Parse(format, v); err == nil {
				return t, nil
			}
		}
		return time.Time{}, fmt.Errorf("cannot parse '%s' as time", v)
	default:
		return time.Time{}, fmt.Errorf("cannot convert %T to time", val)
	}
}

func triToValue(t int) any {
	if t == tvTrue {
		return true
	}
	if t == tvFalse {
		return false
	}
	return nil
}

// String manipulation functions
func evalLTrim(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) < 1 || len(args) > 2 {
		return nil, fmt.Errorf("LTRIM expects 1 or 2 arguments")
	}

	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}

	str, ok := val.(string)
	if !ok {
		return nil, fmt.Errorf("LTRIM expects a string argument")
	}

	// Default: trim whitespace
	cutset := " \t\n\r"

	// If second argument provided, use as cutset
	if len(args) == 2 {
		cutsetVal, err := evalExpr(env, args[1], row)
		if err != nil {
			return nil, err
		}
		if cutsetVal != nil {
			if cutsetStr, ok := cutsetVal.(string); ok {
				cutset = cutsetStr
			} else {
				return nil, fmt.Errorf("LTRIM cutset must be a string")
			}
		}
	}

	return strings.TrimLeft(str, cutset), nil
}

func evalRTrim(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) < 1 || len(args) > 2 {
		return nil, fmt.Errorf("RTRIM expects 1 or 2 arguments")
	}

	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}

	str, ok := val.(string)
	if !ok {
		return nil, fmt.Errorf("RTRIM expects a string argument")
	}

	// Default: trim whitespace
	cutset := " \t\n\r"

	// If second argument provided, use as cutset
	if len(args) == 2 {
		cutsetVal, err := evalExpr(env, args[1], row)
		if err != nil {
			return nil, err
		}
		if cutsetVal != nil {
			if cutsetStr, ok := cutsetVal.(string); ok {
				cutset = cutsetStr
			} else {
				return nil, fmt.Errorf("RTRIM cutset must be a string")
			}
		}
	}

	return strings.TrimRight(str, cutset), nil
}

func evalTrim(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) < 1 || len(args) > 2 {
		return nil, fmt.Errorf("TRIM expects 1 or 2 arguments")
	}

	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}

	str, ok := val.(string)
	if !ok {
		return nil, fmt.Errorf("TRIM expects a string argument")
	}

	// Default: trim whitespace (use TrimSpace for compatibility)
	if len(args) == 1 {
		return strings.TrimSpace(str), nil
	}

	// If second argument provided, use as cutset for both sides
	cutsetVal, err := evalExpr(env, args[1], row)
	if err != nil {
		return nil, err
	}
	if cutsetVal == nil {
		return strings.TrimSpace(str), nil
	}

	cutsetStr, ok := cutsetVal.(string)
	if !ok {
		return nil, fmt.Errorf("TRIM cutset must be a string")
	}

	return strings.Trim(str, cutsetStr), nil
}

// ISNULL function - returns TRUE if argument is NULL, FALSE otherwise
func evalIsNullFunc(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("ISNULL expects 1 argument")
	}

	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}

	return val == nil, nil
}

func isAggregate(e Expr) bool {
	switch ex := e.(type) {
	case *FuncCall:
		switch ex.Name {
		case "COUNT", "SUM", "AVG", "MIN", "MAX":
			return true
		}
	case *Unary:
		return isAggregate(ex.Expr)
	case *Binary:
		return isAggregate(ex.Left) || isAggregate(ex.Right)
	case *IsNull:
		return isAggregate(ex.Expr)
	}
	return false
}

func evalAggregate(env ExecEnv, e Expr, rows []Row) (any, error) {
	switch ex := e.(type) {
	case *FuncCall:
		return evalAggregateFuncCall(env, ex, rows)
	case *Unary:
		return evalAggregateUnary(env, ex, rows)
	case *Binary:
		return evalAggregateBinary(env, ex, rows)
	case *IsNull:
		return evalAggregateIsNull(env, ex, rows)
	default:
		if len(rows) == 0 {
			return nil, nil
		}
		return evalExpr(env, e, rows[0])
	}
}

func evalAggregateFuncCall(env ExecEnv, ex *FuncCall, rows []Row) (any, error) {
	switch ex.Name {
	case "COUNT":
		return evalAggregateCount(env, ex, rows)
	case "SUM", "AVG":
		return evalAggregateSumAvg(env, ex, rows)
	case "MIN", "MAX":
		return evalAggregateMinMax(env, ex, rows)
	}
	return nil, fmt.Errorf("unsupported aggregate function: %s", ex.Name)
}

func evalAggregateCount(env ExecEnv, ex *FuncCall, rows []Row) (any, error) {
	if ex.Star {
		return len(rows), nil
	}
	if len(ex.Args) != 1 {
		return nil, fmt.Errorf("COUNT expects 1 arg")
	}
	cnt := 0
	for _, r := range rows {
		if err := checkCtx(env.ctx); err != nil {
			return nil, err
		}
		v, err := evalExpr(env, ex.Args[0], r)
		if err != nil {
			return nil, err
		}
		if v != nil {
			cnt++
		}
	}
	return cnt, nil
}

func evalAggregateSumAvg(env ExecEnv, ex *FuncCall, rows []Row) (any, error) {
	if len(ex.Args) != 1 {
		return nil, fmt.Errorf("%s expects 1 arg", ex.Name)
	}
	sum := 0.0
	n := 0
	for _, r := range rows {
		if err := checkCtx(env.ctx); err != nil {
			return nil, err
		}
		v, err := evalExpr(env, ex.Args[0], r)
		if err != nil {
			return nil, err
		}
		if f, ok := numeric(v); ok {
			sum += f
			n++
		}
	}
	if ex.Name == "SUM" {
		return sum, nil
	}
	if n == 0 {
		return nil, nil
	}
	return sum / float64(n), nil
}

func evalAggregateMinMax(env ExecEnv, ex *FuncCall, rows []Row) (any, error) {
	if len(ex.Args) != 1 {
		return nil, fmt.Errorf("%s expects 1 arg", ex.Name)
	}
	var have bool
	var best any
	for _, r := range rows {
		if err := checkCtx(env.ctx); err != nil {
			return nil, err
		}
		v, err := evalExpr(env, ex.Args[0], r)
		if err != nil {
			return nil, err
		}
		if v == nil {
			continue
		}
		if !have {
			best = v
			have = true
		} else {
			cmp, err := compare(v, best)
			if err == nil {
				if ex.Name == "MIN" && cmp < 0 {
					best = v
				}
				if ex.Name == "MAX" && cmp > 0 {
					best = v
				}
			}
		}
	}
	if !have {
		return nil, nil
	}
	return best, nil
}

func evalAggregateUnary(env ExecEnv, ex *Unary, rows []Row) (any, error) {
	v, err := evalAggregate(env, ex.Expr, rows)
	if err != nil {
		return nil, err
	}
	switch ex.Op {
	case "+":
		if f, ok := numeric(v); ok {
			return f, nil
		}
		if v == nil {
			return nil, nil
		}
		return nil, fmt.Errorf("unary + non-numeric")
	case "-":
		if f, ok := numeric(v); ok {
			return -f, nil
		}
		if v == nil {
			return nil, nil
		}
		return nil, fmt.Errorf("unary - non-numeric")
	case "NOT":
		return triToValue(triNot(toTri(v))), nil
	}
	return nil, fmt.Errorf("unknown unary operator: %s", ex.Op)
}

func evalAggregateBinary(env ExecEnv, ex *Binary, rows []Row) (any, error) {
	lv, err := evalAggregate(env, ex.Left, rows)
	if err != nil {
		return nil, err
	}
	rv, err := evalAggregate(env, ex.Right, rows)
	if err != nil {
		return nil, err
	}
	return evalExpr(env, &Binary{Op: ex.Op, Left: &Literal{Val: lv}, Right: &Literal{Val: rv}}, Row{})
}

func evalAggregateIsNull(env ExecEnv, ex *IsNull, rows []Row) (any, error) {
	v, err := evalAggregate(env, ex.Expr, rows)
	if err != nil {
		return nil, err
	}
	if ex.Negate {
		return !isNull(v), nil
	}
	return isNull(v), nil
}

func rowsFromTable(t *storage.Table, alias string) ([]Row, []string) {
	cols := make([]string, len(t.Cols))
	for i, c := range t.Cols {
		cols[i] = strings.ToLower(alias + "." + c.Name)
	}
	var out []Row
	for _, r := range t.Rows {
		row := Row{}
		for i, c := range t.Cols {
			putVal(row, alias+"."+c.Name, r[i])
		}
		for i, c := range t.Cols {
			if _, exists := row[strings.ToLower(c.Name)]; !exists {
				putVal(row, c.Name, r[i])
			}
		}
		out = append(out, row)
	}
	return out, cols
}

func keysOfRow(r Row) []string {
	var ks []string
	for k := range r {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}

func aliasOr(f FromItem) string {
	if f.Alias != "" {
		return f.Alias
	}
	return f.Table
}

func mergeRows(l, r Row) Row {
	m := make(Row, len(l)+len(r))
	for k, v := range l {
		m[k] = v
	}
	for k, v := range r {
		m[k] = v
	}
	return m
}
func cloneRow(r Row) Row {
	m := make(Row, len(r))
	for k, v := range r {
		m[k] = v
	}
	return m
}
func addRightNulls(m Row, alias string, t *storage.Table) {
	for _, c := range t.Cols {
		putVal(m, alias+"."+c.Name, nil)
		if _, ex := m[strings.ToLower(c.Name)]; !ex {
			putVal(m, c.Name, nil)
		}
	}
}

func fmtKeyPart(v any) string {
	switch x := v.(type) {
	case nil:
		return "N:"
	case int:
		return "I:" + strconv.Itoa(x)
	case float64:
		return "F:" + strconv.FormatFloat(x, 'g', -1, 64)
	case bool:
		if x {
			return "B:1"
		}
		return "B:0"
	case string:
		return "S:" + x
	default:
		b, _ := json.Marshal(x)
		return "J:" + string(b)
	}
}
func distinctRows(rows []Row, cols []string) []Row {
	seen := map[string]bool{}
	var out []Row
	for _, r := range rows {
		var parts []string
		for _, c := range cols {
			parts = append(parts, fmtKeyPart(r[strings.ToLower(c)]))
		}
		key := strings.Join(parts, "|")
		if !seen[key] {
			seen[key] = true
			out = append(out, r)
		}
	}
	return out
}

func inferType(v any) storage.ColType {
	switch v.(type) {
	case int, int64:
		return storage.IntType
	case float64:
		return storage.FloatType
	case bool:
		return storage.BoolType
	case string:
		return storage.TextType
	default:
		return storage.JsonType
	}
}
func coerceToTypeAllowNull(v any, t storage.ColType) (any, error) {
	if v == nil {
		return nil, nil
	}
	switch t {
	case storage.IntType:
		return coerceToInt(v)
	case storage.FloatType:
		return coerceToFloat(v)
	case storage.TextType:
		return fmt.Sprintf("%v", v), nil
	case storage.BoolType:
		return coerceToBool(v)
	case storage.JsonType:
		return coerceToJson(v)
	default:
		return v, nil
	}
}

func coerceToInt(v any) (any, error) {
	switch x := v.(type) {
	case int:
		return x, nil
	case int64:
		return int(x), nil
	case float64:
		return int(x), nil
	case string:
		n, err := strconv.Atoi(strings.TrimSpace(x))
		if err != nil {
			return nil, fmt.Errorf("cannot convert %q to INT", x)
		}
		return n, nil
	case bool:
		if x {
			return 1, nil
		}
		return 0, nil
	default:
		return nil, fmt.Errorf("cannot convert %T to INT", v)
	}
}

func coerceToFloat(v any) (any, error) {
	switch x := v.(type) {
	case int:
		return float64(x), nil
	case int64:
		return float64(x), nil
	case float64:
		return x, nil
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(x), 64)
		if err != nil {
			return nil, fmt.Errorf("cannot convert %q to FLOAT", x)
		}
		return f, nil
	case bool:
		if x {
			return 1.0, nil
		}
		return 0.0, nil
	default:
		return nil, fmt.Errorf("cannot convert %T to FLOAT", v)
	}
}

func coerceToBool(v any) (any, error) {
	switch x := v.(type) {
	case bool:
		return x, nil
	case int, int64:
		return x != 0, nil
	case float64:
		return x != 0, nil
	case string:
		s := strings.ToLower(strings.TrimSpace(x))
		return s == "true" || s == "1" || s == "t" || s == "yes", nil
	default:
		return nil, fmt.Errorf("cannot convert %T to BOOL", v)
	}
}

func coerceToJson(v any) (any, error) {
	switch x := v.(type) {
	case string:
		var anyv any
		if json.Unmarshal([]byte(x), &anyv) == nil {
			return anyv, nil
		}
		return x, nil
	default:
		return x, nil
	}
}

// JSON helpers

type pathPart struct {
	key string
	idx int
}

func parseJSONPath(s string) []pathPart {
	var out []pathPart
	cur := ""
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '.':
			if cur != "" {
				out = append(out, pathPart{key: cur, idx: -1})
				cur = ""
			}
		case '[':
			if cur != "" {
				out = append(out, pathPart{key: cur, idx: -1})
				cur = ""
			}
			j := i + 1
			for j < len(s) && s[j] != ']' {
				j++
			}
			if j <= len(s)-1 {
				n, _ := strconv.Atoi(s[i+1 : j])
				out = append(out, pathPart{idx: n})
				i = j
			}
		default:
			cur += string(s[i])
		}
	}
	if cur != "" {
		out = append(out, pathPart{key: cur, idx: -1})
	}
	return out
}
func jsonGet(v any, path string) any {
	if v == nil || path == "" {
		return nil
	}
	parts := parseJSONPath(path)
	cur := v
	for _, p := range parts {
		switch c := cur.(type) {
		case map[string]any:
			cur = c[p.key]
		case []any:
			if p.idx >= 0 && p.idx < len(c) {
				cur = c[p.idx]
			} else {
				return nil
			}
		default:
			return nil
		}
	}
	return cur
}

func jsonSet(v any, path string, value any) any {
	if path == "" {
		return value
	}

	parts := parseJSONPath(path)
	if len(parts) == 0 {
		return value
	}

	// If v is nil, create a new structure
	if v == nil {
		if parts[0].idx >= 0 {
			v = make([]any, parts[0].idx+1)
		} else {
			v = make(map[string]any)
		}
	}

	// Navigate to the parent of the target
	cur := v
	for i := 0; i < len(parts)-1; i++ {
		p := parts[i]
		switch c := cur.(type) {
		case map[string]any:
			if p.idx >= 0 {
				// This should be an array access, but we have a map
				return v
			}
			if c[p.key] == nil {
				// Create next level structure
				if parts[i+1].idx >= 0 {
					c[p.key] = make([]any, parts[i+1].idx+1)
				} else {
					c[p.key] = make(map[string]any)
				}
			}
			cur = c[p.key]
		case []any:
			if p.idx < 0 || p.idx >= len(c) {
				return v
			}
			if c[p.idx] == nil {
				// Create next level structure
				if parts[i+1].idx >= 0 {
					c[p.idx] = make([]any, parts[i+1].idx+1)
				} else {
					c[p.idx] = make(map[string]any)
				}
			}
			cur = c[p.idx]
		default:
			return v
		}
	}

	// Set the final value
	lastPart := parts[len(parts)-1]
	switch c := cur.(type) {
	case map[string]any:
		if lastPart.idx >= 0 {
			return v // Invalid: trying to use array index on map
		}
		c[lastPart.key] = value
	case []any:
		if lastPart.idx < 0 {
			return v // Invalid: trying to use string key on array
		}
		// Extend array if needed
		for len(c) <= lastPart.idx {
			c = append(c, nil)
		}
		c[lastPart.idx] = value
		// Update the reference in the parent
		if len(parts) > 1 {
			parentParts := parts[:len(parts)-1]
			parent := v
			for _, p := range parentParts[:len(parentParts)-1] {
				switch pc := parent.(type) {
				case map[string]any:
					parent = pc[p.key]
				case []any:
					parent = pc[p.idx]
				}
			}
			lastParentPart := parentParts[len(parentParts)-1]
			switch pc := parent.(type) {
			case map[string]any:
				pc[lastParentPart.key] = c
			case []any:
				pc[lastParentPart.idx] = c
			}
		} else {
			v = c
		}
	default:
		return v
	}

	return v
}

// -------------------- misc helpers --------------------

func columnsFromRows(rows []Row) []string {
	if len(rows) == 0 {
		return nil
	}
	seen := make(map[string]bool, len(rows[0]))
	cols := make([]string, 0, len(rows[0]))
	for _, r := range rows {
		for k := range r {
			if !seen[k] {
				seen[k] = true
				cols = append(cols, k)
			}
		}
	}
	sort.Strings(cols)
	return cols
}

// Helper functions for projName, anyAggInSelect, and appendUnique

// projName generates the column name for a select item
func projName(it SelectItem, idx int) string {
	if it.Alias != "" {
		return it.Alias
	}
	if ref, ok := it.Expr.(*VarRef); ok {
		return ref.Name
	}
	return fmt.Sprintf("col_%d", idx)
}

// anyAggInSelect checks if any select item contains an aggregate function
func anyAggInSelect(items []SelectItem) bool {
	for _, it := range items {
		if isAggregate(it.Expr) {
			return true
		}
	}
	return false
}

// appendUnique appends a column name to the slice if it's not already present
func appendUnique(cols []string, c string) []string {
	for _, existing := range cols {
		if existing == c {
			return cols
		}
	}
	return append(cols, c)
}
