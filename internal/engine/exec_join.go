// Joins over row maps: inner, left, right and full outer, plus the WHERE
// filter applied to their output. This is the general path; exec_fastpath_join.go
// handles the shape it can compile.
package engine

import (
	"fmt"
	"strings"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func processJoins(env ExecEnv, joins []JoinClause, cur []Row) ([]Row, error) {
	for _, j := range joins {
		var rightRows []Row
		var rightTable *storage.Table
		var err error

		if j.Right.Subquery != nil {
			subRs, err := executeSelect(env, j.Right.Subquery)
			if err != nil {
				return nil, err
			}
			rightRows = make([]Row, len(subRs.Rows))
			for i, row := range subRs.Rows {
				rightRows[i] = make(Row)
				for k, v := range row {
					rightRows[i][strings.ToLower(k)] = v
					if j.Right.Alias != "" {
						rightRows[i][strings.ToLower(j.Right.Alias+"."+k)] = v
					}
				}
			}
			// build synthetic table metadata
			cols := make([]storage.Column, 0, len(subRs.Cols))
			for _, c := range subRs.Cols {
				cols = append(cols, storage.Column{Name: c})
			}
			rightTable = &storage.Table{Name: j.Right.Alias, Cols: cols}
		} else if cteResult, exists := env.ctes[strings.ToLower(j.Right.Table)]; exists {
			rightRows = rowsFromCTEResult(env, cteResult, j.Right)
			rightTable = resultSetTable(aliasOr(j.Right), cteResult.Cols)
		} else if j.Right.TableFunc != nil {
			tf := j.Right.TableFunc
			fn, ok := GetTableFunc(tf.Name)
			if !ok {
				return nil, fmt.Errorf("unknown table function: %s", tf.Name)
			}
			if err := fn.ValidateArgs(tf.Args); err != nil {
				return nil, err
			}
			rs, err := fn.Execute(env.ctx, tf.Args, env, nil)
			if err != nil {
				return nil, err
			}
			rightRows = make([]Row, len(rs.Rows))
			for i, row := range rs.Rows {
				rightRows[i] = make(Row)
				for k, v := range row {
					rightRows[i][strings.ToLower(k)] = v
					if j.Right.Alias != "" {
						rightRows[i][strings.ToLower(j.Right.Alias+"."+k)] = v
					}
				}
			}
			// synthetic table metadata from ResultSet columns
			cols := make([]storage.Column, 0, len(rs.Cols))
			for _, c := range rs.Cols {
				cols = append(cols, storage.Column{Name: c})
			}
			rightTable = &storage.Table{Name: j.Right.Alias, Cols: cols}
		} else {
			schema, name := splitObjectName(j.Right.Table)
			if mv, ok := env.db.Catalog().GetMaterializedView(schema, name); ok {
				rightTable, err = ensureMaterializedViewCache(env, j.Right.Table, mv)
				if err != nil {
					return nil, err
				}
				rightRows, _ = rowsFromTable(rightTable, aliasOr(j.Right))
			} else if rs, found, viewErr := resolveViewResult(env, j.Right); found || viewErr != nil {
				if viewErr != nil {
					return nil, viewErr
				}
				rightRows = rowsFromResultSet(rs, aliasOr(j.Right))
				rightTable = resultSetTable(aliasOr(j.Right), rs.Cols)
			} else {
				rt, err := env.db.Get(env.tenant, j.Right.Table)
				if err != nil {
					return nil, err
				}

				rightRows, _ = rowsFromTable(rt, aliasOr(j.Right))
				rightTable = rt
			}
		}

		switch j.Type {
		case JoinInner:
			cur, err = processInnerJoin(env, cur, rightRows, j.On)
		case JoinLeft:
			cur, err = processLeftJoin(env, cur, rightRows, j.On, aliasOr(j.Right), rightTable)
		case JoinRight:
			cur, err = processRightJoin(env, cur, rightRows, j.On)
		case JoinFull:
			cur, err = processFullOuterJoin(env, cur, rightRows, j.On, aliasOr(j.Right), rightTable)
		case JoinCross:
			// CROSS JOIN has no ON condition by construction, so (like the
			// onCondition == nil case in processInnerJoin) its output size is
			// the full Cartesian product; guard against materializing it here.
			if int64(len(cur))*int64(len(rightRows)) > maxJoinRows {
				return nil, fmt.Errorf("cross join would produce more than %d rows; add a filtering condition or LIMIT the inputs", maxJoinRows)
			}
			optimizer := &HashJoinOptimizer{env: env}
			cur, err = optimizer.processCrossJoin(cur, rightRows, OptimizedJoinTypeInner)
		}
		if err != nil {
			return nil, err
		}
	}
	return cur, nil
}

// maxJoinRows bounds the number of rows a single join step may materialize.
// LIMIT/OFFSET is applied only after all joins (and WHERE, GROUP BY, DISTINCT,
// ORDER BY) run, so an unconditional cross join -- no ON clause, or one with a
// trivially-true condition -- would otherwise fully materialize the Cartesian
// product of its inputs before a later LIMIT ever gets a chance to trim it.
// A var (not const) so tests can lower it temporarily instead of allocating
// millions of rows to exercise the cap.
var maxJoinRows int64 = 5_000_000

func processInnerJoin(env ExecEnv, leftRows, rightRows []Row, onCondition Expr) ([]Row, error) {
	// A missing ON condition means every row pair is kept unconditionally, so
	// the worst-case output size is known up front: guard against it before
	// materializing anything, including before delegating to the hash-join
	// optimizer below.
	if onCondition == nil && int64(len(leftRows))*int64(len(rightRows)) > maxJoinRows {
		return nil, fmt.Errorf("join would produce more than %d rows without a filtering ON condition; add a condition or LIMIT the inputs", maxJoinRows)
	}

	// Use hash join optimization for large datasets
	if len(leftRows) > 500 || len(rightRows) > 500 {
		optimizer := &HashJoinOptimizer{env: env}
		return optimizer.ProcessOptimizedJoin(leftRows, rightRows, onCondition, OptimizedJoinTypeInner)
	}

	// Fall back to original nested loop for small datasets
	joined := make([]Row, 0, len(leftRows)*len(rightRows)/4) // Estimate result size
	for i, l := range leftRows {
		// Check context cancellation every 64 rows to reduce channel-select overhead.
		if i&63 == 0 {
			if err := checkCtx(env.ctx); err != nil {
				return nil, err
			}
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
				if int64(len(joined)) > maxJoinRows {
					return nil, fmt.Errorf("join exceeded row limit %d", maxJoinRows)
				}
			}
		}
	}
	return joined, nil
}

func processLeftJoin(env ExecEnv, leftRows, rightRows []Row, onCondition Expr, rightAlias string, rightTable *storage.Table) ([]Row, error) {
	// Use hash join optimization for large datasets
	if len(leftRows) > 500 || len(rightRows) > 500 {
		optimizer := &HashJoinOptimizer{env: env}
		result, err := optimizer.ProcessOptimizedJoin(leftRows, rightRows, onCondition, OptimizedJoinTypeLeft)
		if err != nil {
			return nil, err
		}

		// Add right nulls for unmatched rows (hash join might not handle all cases)
		for _, row := range result {
			addRightNulls(row, rightAlias, rightTable)
		}
		return result, nil
	}

	// Fall back to original nested loop for small datasets
	joined := make([]Row, 0, len(leftRows)) // At least one row per left row
	for i, l := range leftRows {
		// Check context cancellation every 64 rows to reduce channel-select overhead.
		if i&63 == 0 {
			if err := checkCtx(env.ctx); err != nil {
				return nil, err
			}
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
	for i, r := range rightRows {
		// Check context cancellation every 64 rows to reduce channel-select overhead.
		if i&63 == 0 {
			if err := checkCtx(env.ctx); err != nil {
				return nil, err
			}
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
			addLeftNulls(m, leftKeys)
			joined = append(joined, m)
		}
	}
	return joined, nil
}

// processFullOuterJoin combines every left row (matched or, like LEFT JOIN,
// paired with right-side NULLs when unmatched) with every right row that
// never matched any left row (paired with left-side NULLs, like RIGHT
// JOIN's unmatched case). This was previously entirely unimplemented: FULL
// and CROSS were not lexer keywords, so "FULL OUTER JOIN" silently
// mis-parsed as a table aliased "FULL" with the rest of the clause dropped
// — a query that looked like a two-table join silently ran as a one-table
// scan with no error.
func processFullOuterJoin(env ExecEnv, leftRows, rightRows []Row, onCondition Expr, rightAlias string, rightTable *storage.Table) ([]Row, error) {
	matchedRight := make([]bool, len(rightRows))
	joined := make([]Row, 0, len(leftRows)+len(rightRows))

	var leftKeys []string
	if len(leftRows) > 0 {
		leftKeys = keysOfRow(leftRows[0])
	}

	for i, l := range leftRows {
		// Check context cancellation every 64 rows to reduce channel-select overhead.
		if i&63 == 0 {
			if err := checkCtx(env.ctx); err != nil {
				return nil, err
			}
		}
		matchedAny := false
		for ri, r := range rightRows {
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
				matchedAny = true
				matchedRight[ri] = true
			}
		}
		if !matchedAny {
			m := cloneRow(l)
			addRightNulls(m, rightAlias, rightTable)
			joined = append(joined, m)
		}
	}

	for ri, r := range rightRows {
		if matchedRight[ri] {
			continue
		}
		m := cloneRow(r)
		addLeftNulls(m, leftKeys)
		joined = append(joined, m)
	}
	return joined, nil
}

func applyWhereClause(env ExecEnv, where Expr, rows []Row) ([]Row, error) {
	if where == nil {
		return rows, nil
	}
	if filter := buildRowWhereFilter(where); filter != nil {
		filtered := make([]Row, 0, len(rows)/2)
		for i, r := range rows {
			if i&63 == 0 {
				if err := checkCtx(env.ctx); err != nil {
					return nil, err
				}
			}
			t, err := filter(r)
			if err != nil {
				return nil, err
			}
			if t == tvTrue {
				filtered = append(filtered, r)
			}
		}
		return filtered, nil
	}

	filtered := make([]Row, 0, len(rows)/2) // Estimate half will match
	for i, r := range rows {
		// Check context cancellation every 64 rows to reduce channel-select overhead.
		if i&63 == 0 {
			if err := checkCtx(env.ctx); err != nil {
				return nil, err
			}
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

type rowWhereTriFilter func(Row) (int, error)

func buildRowWhereFilter(where Expr) rowWhereTriFilter {
	switch ex := where.(type) {
	case *VarRef:
		key := ex.Lower
		if key == "" {
			key = strings.ToLower(ex.Name)
		}
		name := ex.Name
		if name == "" {
			name = key
		}
		return func(r Row) (int, error) {
			v, ok := r[key]
			if !ok {
				return tvFalse, unknownColumnErr(name, columnSuggestionFromRow(name, r))
			}
			return toTri(v), nil
		}

	case *Unary:
		if ex.Op != "NOT" {
			return nil
		}
		inner := buildRowWhereFilter(ex.Expr)
		if inner == nil {
			return nil
		}
		return func(r Row) (int, error) {
			t, err := inner(r)
			if err != nil {
				return tvFalse, err
			}
			return triNot(t), nil
		}

	case *IsNull:
		ref, ok := ex.Expr.(*VarRef)
		if !ok {
			return nil
		}
		key := ref.Lower
		if key == "" {
			key = strings.ToLower(ref.Name)
		}
		name := ref.Name
		if name == "" {
			name = key
		}
		return func(r Row) (int, error) {
			v, ok := r[key]
			if !ok {
				return tvFalse, unknownColumnErr(name, columnSuggestionFromRow(name, r))
			}
			res := isNull(v)
			if ex.Negate {
				res = !res
			}
			return toTri(res), nil
		}

	case *Binary:
		switch ex.Op {
		case "AND":
			left := buildRowWhereFilter(ex.Left)
			right := buildRowWhereFilter(ex.Right)
			if left == nil || right == nil {
				return nil
			}
			return func(r Row) (int, error) {
				lt, err := left(r)
				if err != nil || lt == tvFalse {
					return tvFalse, err
				}
				rt, err := right(r)
				if err != nil {
					return tvFalse, err
				}
				return triAnd(lt, rt), nil
			}
		case "OR":
			left := buildRowWhereFilter(ex.Left)
			right := buildRowWhereFilter(ex.Right)
			if left == nil || right == nil {
				return nil
			}
			return func(r Row) (int, error) {
				lt, err := left(r)
				if err != nil {
					return tvFalse, err
				}
				if lt == tvTrue {
					return tvTrue, nil
				}
				rt, err := right(r)
				if err != nil {
					return tvFalse, err
				}
				return triOr(lt, rt), nil
			}
		}
		if isComparisonOp(ex.Op) {
			return buildRowComparisonFilter(ex)
		}
		return nil
	default:
		return nil
	}
}

func buildRowComparisonFilter(ex *Binary) rowWhereTriFilter {
	if leftRef, ok := ex.Left.(*VarRef); ok {
		leftKey := leftRef.Lower
		if leftKey == "" {
			leftKey = strings.ToLower(leftRef.Name)
		}
		leftName := leftRef.Name
		if leftName == "" {
			leftName = leftKey
		}

		if rightLit, ok := ex.Right.(*Literal); ok {
			return func(r Row) (int, error) {
				leftVal, ok := r[leftKey]
				if !ok {
					return tvFalse, unknownColumnErr(leftName, columnSuggestionFromRow(leftName, r))
				}
				return triCompareVals(leftVal, rightLit.Val, ex.Op)
			}
		}

		if rightRef, ok := ex.Right.(*VarRef); ok {
			rightKey := rightRef.Lower
			if rightKey == "" {
				rightKey = strings.ToLower(rightRef.Name)
			}
			rightName := rightRef.Name
			if rightName == "" {
				rightName = rightKey
			}
			return func(r Row) (int, error) {
				leftVal, ok := r[leftKey]
				if !ok {
					return tvFalse, unknownColumnErr(leftName, columnSuggestionFromRow(leftName, r))
				}
				rightVal, ok := r[rightKey]
				if !ok {
					return tvFalse, unknownColumnErr(rightName, columnSuggestionFromRow(rightName, r))
				}
				return triCompareVals(leftVal, rightVal, ex.Op)
			}
		}
	}

	if leftLit, ok := ex.Left.(*Literal); ok {
		if rightRef, ok := ex.Right.(*VarRef); ok {
			rightKey := rightRef.Lower
			if rightKey == "" {
				rightKey = strings.ToLower(rightRef.Name)
			}
			rightName := rightRef.Name
			if rightName == "" {
				rightName = rightKey
			}
			op := reverseComparisonOp(ex.Op)
			return func(r Row) (int, error) {
				rightVal, ok := r[rightKey]
				if !ok {
					return tvFalse, unknownColumnErr(rightName, columnSuggestionFromRow(rightName, r))
				}
				return triCompareVals(leftLit.Val, rightVal, op)
			}
		}
	}

	return nil
}

func triCompareVals(left, right any, op string) (int, error) {
	if left == nil || right == nil {
		return tvUnknown, nil
	}
	switch op {
	case "=":
		return boolToTri(rawEqual(left, right)), nil
	case "!=", "<>":
		return boolToTri(!rawEqual(left, right)), nil
	default:
		cmp, err := compare(left, right)
		if err != nil {
			return tvFalse, err
		}
		switch op {
		case "<":
			return boolToTri(cmp < 0), nil
		case "<=":
			return boolToTri(cmp <= 0), nil
		case ">":
			return boolToTri(cmp > 0), nil
		case ">=":
			return boolToTri(cmp >= 0), nil
		}
		return tvFalse, nil
	}
}

func boolToTri(v bool) int {
	if v {
		return tvTrue
	}
	return tvFalse
}
