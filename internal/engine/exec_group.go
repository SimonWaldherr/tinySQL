// GROUP BY, HAVING and PIVOT, and the projection of both aggregate and
// non-aggregate queries, followed by OFFSET/LIMIT.
package engine

import (
	"fmt"
	"math/big"
	"sort"
	"strings"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
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
	if fastRows, fastCols, handled, err := processAggregateQueryFastPath(env, s, filtered); err != nil {
		return nil, nil, err
	} else if handled {
		return fastRows, fastCols, nil
	}

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
	for i, r := range filtered {
		// Check context cancellation every 64 rows to reduce channel-select overhead.
		if i&63 == 0 {
			if err := checkCtx(env.ctx); err != nil {
				return nil, nil, err
			}
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
					// See the identical pattern (and its explanation) in
					// processNonAggregateQuery: base must only be written
					// from a qualified sibling when rows[0] itself has no
					// unqualified key for it, or the value silently depends
					// on Go's randomized map iteration order.
					src := rows[0]
					for col, v := range src {
						putVal(out, col, v)
						if strings.Contains(col, ".") {
							last := strings.LastIndex(col, ".")
							base := col[last+1:]
							if _, hasUnqualified := src[base]; !hasUnqualified {
								putVal(out, base, v)
							}
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

type aggregateProjectionArg struct {
	key        string
	name       string
	literal    any
	hasArg     bool
	hasLiteral bool
}

func processAggregateQueryFastPath(env ExecEnv, s *Select, filtered []Row) ([]Row, []string, bool, error) {
	if s.Having != nil || anyWindowInSelect(s.Projs) {
		return nil, nil, false, nil
	}

	groupKeys := make([]string, len(s.GroupBy))
	groupNames := make([]string, len(s.GroupBy))
	groupPos := make(map[string]int, len(s.GroupBy))
	for i, g := range s.GroupBy {
		ref, ok := g.(*VarRef)
		if !ok {
			return nil, nil, false, nil
		}
		key := ref.Lower
		if key == "" {
			key = strings.ToLower(ref.Name)
		}
		name := ref.Name
		if name == "" {
			name = key
		}
		if key == "" {
			return nil, nil, false, nil
		}
		groupKeys[i] = key
		groupNames[i] = name
		if _, seen := groupPos[key]; !seen {
			groupPos[key] = i
		}
	}

	projs := make([]simpleAggregateProjection, len(s.Projs))
	args := make([]aggregateProjectionArg, len(s.Projs))
	outCols := make([]string, 0, len(s.Projs))
	colSet := make(map[string]struct{}, len(s.Projs))
	hasAgg := false

	for i, it := range s.Projs {
		proj, arg, ok, err := compileAggregateProjectionForFastPath(it, i, groupPos)
		if err != nil {
			return nil, nil, false, err
		}
		if !ok {
			return nil, nil, false, nil
		}
		projs[i] = proj
		args[i] = arg
		if proj.kind != aggGroupCol {
			hasAgg = true
		}
		name := projName(it, i)
		if _, seen := colSet[name]; !seen {
			colSet[name] = struct{}{}
			outCols = append(outCols, name)
		}
	}

	if len(s.GroupBy) == 0 && !hasAgg {
		return nil, nil, false, nil
	}

	groupCapacity := len(filtered) / 2
	if groupCapacity < 1 {
		groupCapacity = 1
	}
	groups := make(map[string]*simpleAggregateState, groupCapacity)
	order := make([]*simpleAggregateState, 0)
	keyBuf := make([]byte, 0, 64)
	groupValues := make([]any, len(groupKeys))

	if len(s.GroupBy) == 0 {
		state := newSimpleAggregateState(nil, len(s.Projs))
		groups[""] = state
		order = append(order, state)
	}

	for rowIdx, r := range filtered {
		if rowIdx&63 == 0 {
			if err := checkCtx(env.ctx); err != nil {
				return nil, nil, false, err
			}
		}
		var state *simpleAggregateState
		if len(groupKeys) > 0 {
			keyBuf = keyBuf[:0]
			for gi, gkey := range groupKeys {
				v, ok := r[gkey]
				if !ok {
					return nil, nil, false, unknownColumnErr(groupNames[gi], columnSuggestionFromRow(groupNames[gi], r))
				}
				groupValues[gi] = v
				if gi > 0 {
					keyBuf = append(keyBuf, '\x1f')
				}
				keyBuf = writeFmtKeyPart(keyBuf, v)
			}
			key := string(keyBuf)
			st, ok := groups[key]
			if !ok {
				values := append(make([]any, 0, len(groupValues)), groupValues...)
				st = newSimpleAggregateState(values, len(s.Projs))
				groups[key] = st
				order = append(order, st)
			}
			state = st
		} else {
			state = groups[""]
		}

		if err := accumulateSimpleAggregateStateFromRow(r, state, projs, args); err != nil {
			return nil, nil, false, err
		}
	}

	result := simpleAggregateResultSet(projs, outCols, order)
	return result.Rows, result.Cols, true, nil
}

func compileAggregateProjectionForFastPath(it SelectItem, idx int, groupPos map[string]int) (simpleAggregateProjection, aggregateProjectionArg, bool, error) {
	name := projName(it, idx)
	if it.Star {
		return simpleAggregateProjection{}, aggregateProjectionArg{}, false, nil
	}

	if ref, ok := it.Expr.(*VarRef); ok {
		key := ref.Lower
		if key == "" {
			key = strings.ToLower(ref.Name)
		}
		if key == "" {
			return simpleAggregateProjection{}, aggregateProjectionArg{}, false, nil
		}
		groupIndex, grouped := groupPos[key]
		if !grouped {
			return simpleAggregateProjection{}, aggregateProjectionArg{}, false, nil
		}
		return simpleAggregateProjection{
			name:       name,
			kind:       aggGroupCol,
			groupIndex: groupIndex,
		}, aggregateProjectionArg{}, true, nil
	}

	fc, ok := it.Expr.(*FuncCall)
	if !ok || fc.Over != nil || fc.Distinct || len(fc.Name) == 0 {
		return simpleAggregateProjection{}, aggregateProjectionArg{}, false, nil
	}

	proj := simpleAggregateProjection{
		name: name,
	}
	switch fc.Name {
	case "COUNT":
		proj.kind = aggCount
		if fc.Star {
			if len(fc.Args) != 0 {
				return simpleAggregateProjection{}, aggregateProjectionArg{}, false, nil
			}
			return proj, aggregateProjectionArg{}, true, nil
		}
		if len(fc.Args) != 1 {
			return simpleAggregateProjection{}, aggregateProjectionArg{}, false, nil
		}
		arg, ok := aggregateArgFromExpr(fc.Args[0])
		if !ok {
			return simpleAggregateProjection{}, aggregateProjectionArg{}, false, nil
		}
		return proj, arg, true, nil
	case "SUM", "AVG", "MIN", "MAX":
		switch fc.Name {
		case "SUM":
			proj.kind = aggSum
		case "AVG":
			proj.kind = aggAvg
		case "MIN":
			proj.kind = aggMin
		case "MAX":
			proj.kind = aggMax
		}
		if fc.Star || len(fc.Args) != 1 {
			return simpleAggregateProjection{}, aggregateProjectionArg{}, false, nil
		}
		arg, ok := aggregateArgFromExpr(fc.Args[0])
		if !ok {
			return simpleAggregateProjection{}, aggregateProjectionArg{}, false, nil
		}
		return proj, arg, true, nil
	default:
		return simpleAggregateProjection{}, aggregateProjectionArg{}, false, nil
	}
}

func aggregateArgFromExpr(e Expr) (aggregateProjectionArg, bool) {
	switch ex := e.(type) {
	case *VarRef:
		key := ex.Lower
		if key == "" {
			key = strings.ToLower(ex.Name)
		}
		name := ex.Name
		if name == "" {
			name = key
		}
		if key == "" {
			return aggregateProjectionArg{}, false
		}
		return aggregateProjectionArg{
			hasArg: true,
			key:    key,
			name:   name,
		}, true
	case *Literal:
		return aggregateProjectionArg{
			hasArg:     true,
			hasLiteral: true,
			literal:    ex.Val,
		}, true
	default:
		return aggregateProjectionArg{}, false
	}
}

func aggregateProjectionArgValue(r Row, arg aggregateProjectionArg) (any, error) {
	if !arg.hasArg {
		return nil, nil
	}
	if arg.hasLiteral {
		return arg.literal, nil
	}
	v, ok := r[arg.key]
	if !ok {
		return nil, unknownColumnErr(arg.name, columnSuggestionFromRow(arg.name, r))
	}
	return v, nil
}

func accumulateSimpleAggregateStateFromRow(r Row, state *simpleAggregateState, projs []simpleAggregateProjection, args []aggregateProjectionArg) error {
	for i, proj := range projs {
		switch proj.kind {
		case aggGroupCol:
			continue
		case aggCount:
			if !args[i].hasArg {
				state.counts[i]++
				continue
			}
			v, err := aggregateProjectionArgValue(r, args[i])
			if err != nil {
				return err
			}
			if v != nil {
				state.counts[i]++
			}
		case aggSum, aggAvg:
			v, err := aggregateProjectionArgValue(r, args[i])
			if err != nil {
				return err
			}
			if v == nil {
				continue
			}
			if f, ok := numeric(v); ok {
				if state.useRat != nil && state.useRat[i] {
					state.sumRat[i].Add(state.sumRat[i], new(big.Rat).SetFloat64(f))
				} else {
					state.sumFloat[i] += f
				}
				state.counts[i]++
				continue
			}
			if rv, ok := storage.DecimalFromAny(v); ok {
				if state.useRat == nil {
					state.useRat = make([]bool, len(projs))
					state.sumRat = make([]*big.Rat, len(projs))
				}
				if !state.useRat[i] {
					state.sumRat[i] = new(big.Rat)
					if state.counts[i] > 0 {
						state.sumRat[i].SetFloat64(state.sumFloat[i])
					}
					state.useRat[i] = true
				}
				state.sumRat[i].Add(state.sumRat[i], new(big.Rat).Set(rv))
				state.counts[i]++
			}
		case aggMin, aggMax:
			v, err := aggregateProjectionArgValue(r, args[i])
			if err != nil {
				return err
			}
			if v == nil {
				continue
			}
			if !state.haveMinMax[i] {
				state.minmax[i] = v
				state.haveMinMax[i] = true
				continue
			}
			cmp, err := compare(v, state.minmax[i])
			if err != nil {
				continue
			}
			if (proj.kind == aggMin && cmp < 0) || (proj.kind == aggMax && cmp > 0) {
				state.minmax[i] = v
			}
		}
	}
	return nil
}

func processNonAggregateQuery(env ExecEnv, s *Select, filtered []Row) ([]Row, []string, error) {
	outRows := make([]Row, 0, len(filtered))
	outCols := make([]string, 0, len(s.Projs))
	colSet := make(map[string]struct{}, len(s.Projs))
	type directSelectProjection struct {
		name string
		key  string
	}

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
	isDirectSelect := !hasWindowFunctions
	directProjs := make([]directSelectProjection, 0, len(s.Projs))
	if isDirectSelect {
		for i, it := range s.Projs {
			ref, ok := it.Expr.(*VarRef)
			if !ok || it.Star {
				isDirectSelect = false
				break
			}
			key := ref.Lower
			if key == "" {
				key = strings.ToLower(ref.Name)
			}
			if key == "" {
				isDirectSelect = false
				break
			}
			directProjs = append(directProjs, directSelectProjection{
				name: projName(it, i),
				key:  key,
			})
		}
	}
	if isDirectSelect {
		for _, p := range directProjs {
			if _, seen := colSet[p.name]; !seen {
				colSet[p.name] = struct{}{}
				outCols = append(outCols, p.name)
			}
		}
	}

	// If window functions are present, set up window context
	if hasWindowFunctions {
		env.windowRows = filtered
		env.windowPartitions = newWindowPartitionCache()
	}

	for rowIdx, r := range filtered {
		// Check context cancellation every 64 rows to reduce channel-select overhead.
		if rowIdx&63 == 0 {
			if err := checkCtx(env.ctx); err != nil {
				return nil, nil, err
			}
		}

		// Set window index for current row
		if hasWindowFunctions {
			env.windowIndex = rowIdx
		}

		out := Row{}
		if isDirectSelect {
			for _, p := range directProjs {
				v, ok := r[p.key]
				if !ok {
					return nil, nil, unknownColumnErr(p.name, columnSuggestionFromRow(p.name, r))
				}
				putVal(out, p.name, v)
			}
		} else {
			for i, it := range s.Projs {
				if it.Star {
					// Range over r's keys to discover them -- Go
					// deliberately randomizes map iteration order, so
					// nothing here may depend on which key is visited
					// first or last.
					//
					// base (the unqualified name a bare "SELECT *" column
					// takes, e.g. "id" for both "a.id" and "b.id") must
					// therefore never be *written* from a qualified
					// sibling's value: whether r itself already has an
					// unqualified "id" key is a static property of r, not
					// of iteration order, and when it does, that value --
					// copied verbatim by the else branch below whenever
					// `range r` happens to visit the unqualified key
					// itself -- is already the one true answer (mergeRows
					// resolved it at merge time, right side overwriting
					// left on a name collision). Deriving it again from an
					// arbitrary qualified key here, unconditionally, meant
					// two joined tables sharing a column name produced a
					// different, non-deterministic answer for that bare
					// column on every single run of the identical query.
					for col, v := range r {
						putVal(out, col, v)
						if strings.Contains(col, ".") {
							last := strings.LastIndex(col, ".")
							base := col[last+1:]
							if _, hasUnqualified := r[base]; !hasUnqualified {
								putVal(out, base, v)
							}
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
