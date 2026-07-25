// GROUP BY, HAVING and PIVOT, and the projection of both aggregate and
// non-aggregate queries, followed by OFFSET/LIMIT.
package engine

import (
	"fmt"
	"sort"
	"strings"
)

func processGroupByHaving(env ExecEnv, s *Select, filtered []Row) ([]Row, []string, error) {
	if s.Pivot != nil {
		pivotRows, err := processPivot(env, s.Pivot, filtered)
		if err != nil {
			return nil, nil, err
		}
		// Pivoted rows are a plain row set keyed by group columns + one key
		// per pivot value; run them through the normal (non-aggregate)
		// projection path so SELECT * / explicit column lists / window
		// functions work exactly as they would on any other row set.
		return processNonAggregateQuery(env, s, pivotRows)
	}

	needAgg := len(s.GroupBy) > 0 || anyAggInSelect(s.Projs) || isAggregate(s.Having)

	if needAgg {
		return processAggregateQuery(env, s, filtered)
	}
	return processNonAggregateQuery(env, s, filtered)
}

// processPivot reshapes filtered rows per a PIVOT clause: every column not
// used as the pivot column or the aggregated value column becomes an
// implicit GROUP BY key, and each literal in the PIVOT's IN (...) list
// becomes its own output column holding agg(value_expr) over the rows in
// that group matching that pivot value.
func processPivot(env ExecEnv, pv *PivotClause, filtered []Row) ([]Row, error) {
	pivotColLower := strings.ToLower(pv.PivotCol)
	exclude := map[string]bool{pivotColLower: true}
	for name := range collectVarRefNames(pv.ValueExpr) {
		exclude[name] = true
	}

	// Group-by columns: every unqualified column present in the source rows
	// except the pivot column and the value expression's own column(s).
	var groupCols []string
	if len(filtered) > 0 {
		seen := map[string]bool{}
		for k := range filtered[0] {
			if strings.Contains(k, ".") || exclude[k] || seen[k] {
				continue
			}
			seen[k] = true
			groupCols = append(groupCols, k)
		}
		sort.Strings(groupCols)
	}

	// Evaluate each IN (...) entry once (they must be constant expressions)
	// and determine its output column name.
	type pivotOut struct {
		key  any
		name string
	}
	outs := make([]pivotOut, len(pv.Values))
	for i, v := range pv.Values {
		val, err := evalExpr(env, v.Expr, Row{})
		if err != nil {
			return nil, fmt.Errorf("PIVOT: evaluating IN-list value: %w", err)
		}
		name := v.Alias
		if name == "" {
			name = fmt.Sprint(val)
		}
		outs[i] = pivotOut{key: val, name: name}
	}

	// Group source rows by their group-by column values, preserving first-
	// seen order (matches GROUP BY's existing behavior elsewhere).
	type group struct {
		values []any
		rows   []Row
	}
	groups := make(map[string]*group)
	var order []string
	keyBuf := make([]byte, 0, 64)
	for _, r := range filtered {
		keyBuf = keyBuf[:0]
		for i, c := range groupCols {
			if i > 0 {
				keyBuf = append(keyBuf, '\x1f')
			}
			keyBuf = writeFmtKeyPart(keyBuf, r[c])
		}
		g, ok := groups[string(keyBuf)]
		if !ok {
			gk := string(keyBuf)
			values := make([]any, len(groupCols))
			for i, c := range groupCols {
				values[i] = r[c]
			}
			g = &group{values: values}
			groups[gk] = g
			order = append(order, gk)
		}
		g.rows = append(g.rows, r)
	}

	outRows := make([]Row, 0, len(order))
	for _, gk := range order {
		g := groups[gk]
		out := Row{}
		for i, c := range groupCols {
			putVal(out, c, g.values[i])
		}
		for _, o := range outs {
			var matching []Row
			for _, r := range g.rows {
				pcv, _ := getValLower(r, pivotColLower)
				cmp, err := compare(pcv, o.key)
				if err == nil && cmp == 0 {
					matching = append(matching, r)
				}
			}
			aggCall := &FuncCall{Name: pv.AggFunc, Args: []Expr{pv.ValueExpr}}
			val, err := evalAggregateFuncCall(env, aggCall, matching)
			if err != nil {
				return nil, fmt.Errorf("PIVOT: %s: %w", pv.AggFunc, err)
			}
			putVal(out, o.name, val)
		}
		outRows = append(outRows, out)
	}
	return outRows, nil
}

// collectVarRefNames returns the lowercased names of every column reference
// reachable within e — used by PIVOT to exclude the value expression's own
// column(s) from the implicit GROUP BY key set.
func collectVarRefNames(e Expr) map[string]bool {
	names := make(map[string]bool)
	var walk func(Expr)
	walk = func(e Expr) {
		switch ex := e.(type) {
		case nil:
		case *VarRef:
			key := ex.Lower
			if key == "" {
				key = strings.ToLower(ex.Name)
			}
			names[key] = true
		case *Unary:
			walk(ex.Expr)
		case *Binary:
			walk(ex.Left)
			walk(ex.Right)
		case *IsNull:
			walk(ex.Expr)
		case *FuncCall:
			for _, a := range ex.Args {
				walk(a)
			}
		case *CaseExpr:
			walk(ex.Operand)
			for _, w := range ex.Whens {
				walk(w.When)
				walk(w.Then)
			}
			walk(ex.Else)
		}
	}
	walk(e)
	return names
}

//nolint:gocyclo // Aggregation flow must cover grouping, HAVING, and projection variants.
func processAggregateQuery(env ExecEnv, s *Select, filtered []Row) ([]Row, []string, error) {
	// groups maps a composite key to a *[]Row rather than []Row directly so
	// appending a row to an EXISTING group never needs to write the map again
	// (see below) — only inserting a brand-new group does.
	groups := make(map[string]*[]Row, len(filtered)/2) // Estimate group count
	orderKeys := make([]string, 0, len(filtered)/2)
	outRows := make([]Row, 0, len(filtered)/2)
	outCols := make([]string, 0, len(s.Projs))
	colSet := make(map[string]struct{}, len(s.Projs))

	// keyBuf is reused across rows via keyBuf[:0] (retaining its backing
	// array) rather than resetting a *strings.Builder to nil every row — see
	// the writeFmtKeyPart doc comment. `groups[string(keyBuf)]` for the
	// existence check is a compiler-optimized zero-allocation map lookup (Go
	// elides the []byte->string conversion when the result is only read);
	// materializing a real, independently-owned string via string(keyBuf)
	// only happens for a row that starts a brand-new group. Real GROUP BY
	// workloads have far fewer distinct groups than rows, so this turns
	// per-row key-string allocation into per-distinct-group allocation.
	keyBuf := make([]byte, 0, 64)
	for _, r := range filtered {
		if err := checkCtx(env.ctx); err != nil {
			return nil, nil, err
		}
		keyBuf = keyBuf[:0]
		for i, g := range s.GroupBy {
			v, err := evalExpr(env, g, r)
			if err != nil {
				return nil, nil, err
			}
			if i > 0 {
				keyBuf = append(keyBuf, '\x1f')
			}
			keyBuf = writeFmtKeyPart(keyBuf, v)
		}
		grp, ok := groups[string(keyBuf)]
		if !ok {
			ks := string(keyBuf)
			orderKeys = append(orderKeys, ks)
			rows := make([]Row, 0, 4)
			grp = &rows
			groups[ks] = grp
		}
		*grp = append(*grp, r)
	}

	// A whole-table aggregate (no GROUP BY) always produces exactly one row,
	// even over zero matching input rows — "SELECT COUNT(*) FROM t" on an
	// empty (or fully filtered-out) table must return one row with count 0,
	// not zero rows. Only synthesize that implicit empty group when there's
	// no GROUP BY at all; a real "GROUP BY x" correctly produces zero rows
	// when there's no data to group.
	if len(s.GroupBy) == 0 && len(orderKeys) == 0 {
		orderKeys = append(orderKeys, "")
		groups[""] = nil
	}

	for _, k := range orderKeys {
		var rows []Row
		if grp := groups[k]; grp != nil {
			rows = *grp
		}
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
						putVal(out, col, v)
						if strings.Contains(col, ".") {
							last := strings.LastIndex(col, ".")
							base := col[last+1:]
							putVal(out, base, v)
							if _, seen := colSet[base]; !seen {
								colSet[base] = struct{}{}
								outCols = append(outCols, base)
							}
						} else {
							if _, seen := colSet[col]; !seen {
								colSet[col] = struct{}{}
								outCols = append(outCols, col)
							}
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
			} else if len(rows) > 0 {
				val, err = evalExpr(env, it.Expr, rows[0])
			} else {
				// The implicit empty group for a whole-table aggregate over
				// zero rows (see above): a non-aggregate projection has no
				// row to evaluate against. A literal still evaluates fine;
				// a real column reference will error instead of panicking
				// on rows[0], which is the right failure mode here since
				// such a reference is not meaningfully defined without a
				// GROUP BY or a row to pull it from.
				val, err = evalExpr(env, it.Expr, Row{})
			}
			if err != nil {
				return nil, nil, err
			}
			putVal(out, name, val)
			if _, seen := colSet[name]; !seen {
				colSet[name] = struct{}{}
				outCols = append(outCols, name)
			}
		}
		outRows = append(outRows, out)
	}
	return outRows, outCols, nil
}

func processNonAggregateQuery(env ExecEnv, s *Select, filtered []Row) ([]Row, []string, error) {
	outRows := make([]Row, 0, len(filtered))
	outCols := make([]string, 0, len(s.Projs))
	colSet := make(map[string]struct{}, len(s.Projs))

	// ORDER BY may name a column the SELECT list does not project. Sorting runs
	// on the projected rows, so such a column has to be carried across the
	// projection or the sort finds nothing to compare and silently returns rows
	// in physical order. The values go into the row under the ordering name but
	// deliberately not into outCols, so they order the result without appearing
	// in it — and only when the projection has not already produced that name,
	// since an output alias must win over a same-named source column.
	orderCols := make([]string, 0, len(s.OrderBy))
	for _, oi := range s.OrderBy {
		orderCols = append(orderCols, strings.ToLower(oi.Col))
	}

	// Check if any window functions are used
	hasWindowFunctions := anyWindowInSelect(s.Projs)

	// If window functions are present, set up window context
	if hasWindowFunctions {
		env.windowRows = filtered
	}

	for rowIdx, r := range filtered {
		if err := checkCtx(env.ctx); err != nil {
			return nil, nil, err
		}

		// Set window index for current row
		if hasWindowFunctions {
			env.windowIndex = rowIdx
		}

		out := Row{}
		for i, it := range s.Projs {
			if it.Star {
				for col, v := range r {
					putVal(out, col, v)
					if strings.Contains(col, ".") {
						last := strings.LastIndex(col, ".")
						base := col[last+1:]
						putVal(out, base, v)
						if _, seen := colSet[base]; !seen {
							colSet[base] = struct{}{}
							outCols = append(outCols, base)
						}
					} else {
						if _, seen := colSet[col]; !seen {
							colSet[col] = struct{}{}
							outCols = append(outCols, col)
						}
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
			if _, seen := colSet[name]; !seen {
				colSet[name] = struct{}{}
				outCols = append(outCols, name)
			}
		}
		for _, col := range orderCols {
			if _, projected := out[col]; projected {
				continue
			}
			if v, ok := r[col]; ok {
				out[col] = v
			}
		}
		outRows = append(outRows, out)
	}
	// An ORDER BY term that names neither an output column nor a source column
	// cannot sort anything. Report it instead of returning rows in physical
	// order, which is what a typo in an ORDER BY clause used to produce. With no
	// rows there is nothing to resolve names against, and the result is empty
	// either way.
	if len(outRows) > 0 {
		for _, col := range orderCols {
			if _, ok := outRows[0][col]; !ok {
				return nil, nil, fmt.Errorf("ORDER BY: no such column %q", col)
			}
		}
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
