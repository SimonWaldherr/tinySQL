package engine

import (
	"math/big"
	"strings"
	"time"
)

// Aggregate immutable CTE rows without first copying each row to add source
// qualifiers. Keep the existing row aggregate and WHERE semantics; this only
// binds unambiguous source names once per consumer.
func executeCTEAggregateFastPath(env ExecEnv, s *Select) (result *ResultSet, handled bool, err error) {
	if s == nil || len(s.Joins) != 0 || s.Union != nil || s.Pivot != nil ||
		s.Distinct || len(s.DistinctOn) != 0 || s.Having != nil ||
		s.From.Table == "" || s.From.Subquery != nil || s.From.TableFunc != nil ||
		(len(s.GroupBy) == 0 && !anyAggInSelect(s.Projs)) || anyWindowInSelect(s.Projs) {
		return nil, false, nil
	}
	source := env.ctes[strings.ToLower(s.From.Table)]
	if source == nil {
		return nil, false, nil
	}
	if !cteAggregateLiteralsSafe(s.Where) {
		return nil, false, nil
	}
	required, eligible := cteAggregateSourceKeys(s)
	if !eligible {
		return nil, false, nil
	}
	keys := make(map[string]struct{}, len(source.Cols))
	for _, name := range source.Cols {
		if strings.Contains(name, ".") {
			return nil, false, nil
		}
		keys[strings.ToLower(name)] = struct{}{}
	}
	cte, alias := strings.ToLower(s.From.Table), strings.ToLower(s.From.Alias)
	resolve := func(name string) (string, bool) {
		qualifier, column, qualified := strings.Cut(name, ".")
		if qualified {
			if qualifier != cte && (alias == "" || qualifier != alias) {
				return "", false
			}
			name = column
		}
		_, exists := keys[name]
		return name, exists
	}
	bound := *s
	bound.GroupBy = make([]Expr, len(s.GroupBy))
	for i, expr := range s.GroupBy {
		var valid bool
		bound.GroupBy[i], valid = resolveCTEFilter(expr, resolve)
		if !valid {
			return nil, false, nil
		}
	}
	bound.Projs = make([]SelectItem, len(s.Projs))
	for i, item := range s.Projs {
		bound.Projs[i] = item
		if _, direct := item.Expr.(*VarRef); direct {
			var valid bool
			bound.Projs[i].Expr, valid = resolveCTEFilter(item.Expr, resolve)
			if !valid {
				return nil, false, nil
			}
			continue
		}
		call, ok := item.Expr.(*FuncCall)
		if !ok {
			return nil, false, nil
		}
		copy := *call
		if len(call.Args) != 0 {
			arg, valid := resolveCTEFilter(call.Args[0], resolve)
			if !valid {
				return nil, false, nil
			}
			copy.Args = []Expr{arg}
		}
		bound.Projs[i].Expr = &copy
	}
	predicate, valid := resolveCTEFilter(s.Where, resolve)
	if !valid || (predicate != nil && buildRowWhereFilter(predicate) == nil) {
		return nil, false, nil
	}
	// Borrow only callback-free values, checking before any evaluation. Host
	// Stringer/JSON hooks could mutate later source rows; the general path's
	// qualified maps snapshot those values before running such callbacks.
	// Sparse source maps also retain the general path's qualified diagnostics.
	requiredKeys := make([]string, 0, len(required))
	for name := range required {
		key, valid := resolve(name)
		if !valid {
			return nil, false, nil
		}
		requiredKeys = append(requiredKeys, key)
	}
	for name := range collectVarRefNames(predicate) {
		requiredKeys = append(requiredKeys, name)
	}
	for i, row := range source.Rows {
		if i&63 == 0 {
			if err := checkCtx(env.ctx); err != nil {
				return nil, true, err
			}
		}
		for _, key := range requiredKeys {
			value, exists := row[key]
			if !exists || !cteAggregateValueSafe(value) {
				return nil, false, nil
			}
		}
	}
	var phase *queryPhase
	if env.profile != nil {
		env, phase = startQueryPhase(env, "SCAN/FILTER/AGGREGATE", "materialized CTE")
		defer func() {
			var rows int64
			if result != nil {
				rows = int64(len(result.Rows))
			}
			phase.finish(rows)
		}()
	}
	rows, filterErr := applyWhereClause(env, predicate, source.Rows)
	if filterErr != nil {
		return nil, true, filterErr
	}
	rows, columns, ok, aggregateErr := processAggregateQueryFastPath(env, &bound, rows)
	if aggregateErr != nil {
		return nil, true, aggregateErr
	}
	if !ok {
		return nil, false, nil
	}
	if len(s.OrderBy) != 0 {
		rows = applySortOrderWithLimit(s.OrderBy, rows, s.Limit, s.Offset)
	}
	return &ResultSet{Cols: columns, Rows: applyOffsetLimit(s, rows)}, true, nil
}

// Check eligibility using the original names too. Normalizing q.g and c.g
// must not promote a formerly general aggregate, whose empty-result metadata
// can differ from the row aggregate fast path.
func cteAggregateSourceKeys(s *Select) (map[string]struct{}, bool) {
	// Computed arguments retain the general executor. Reject them before
	// allocating a binding or formatting their projection names.
	for _, item := range s.Projs {
		switch expr := item.Expr.(type) {
		case *VarRef:
		case *FuncCall:
			for _, arg := range expr.Args {
				switch arg := arg.(type) {
				case *VarRef:
				case *Literal:
					if !cteAggregateValueSafe(arg.Val) {
						return nil, false
					}
				default:
					return nil, false
				}
			}
		default:
			return nil, false
		}
	}
	var keys map[string]struct{}
	groups := make(map[string]int, len(s.GroupBy))
	for i, expr := range s.GroupBy {
		ref, direct := expr.(*VarRef)
		if !direct {
			return nil, false
		}
		key := ref.Lower
		if key == "" {
			key = strings.ToLower(ref.Name)
		}
		if key == "" {
			return nil, false
		}
		if keys == nil {
			keys = make(map[string]struct{})
		}
		keys[key] = struct{}{}
		if _, exists := groups[key]; !exists {
			groups[key] = i
		}
	}
	for i, item := range s.Projs {
		_, arg, ok, err := compileAggregateProjectionForFastPath(item, i, groups)
		if err != nil || !ok {
			return nil, false
		}
		if arg.hasArg && !arg.hasLiteral {
			if keys == nil {
				keys = make(map[string]struct{})
			}
			keys[arg.key] = struct{}{}
		}
	}
	return keys, true
}

// Arbitrary composites may contain custom formatting/JSON hooks even when
// the outer value itself implements no interface. Keep them on the snapshot
// path rather than inspecting or coercing their contents while planning.
func cteAggregateValueSafe(value any) bool {
	switch value.(type) {
	case nil, bool, string,
		int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, uintptr,
		float32, float64, []byte, []float64, time.Time, big.Rat, *big.Rat:
		return true
	default:
		return false
	}
}

func cteAggregateLiteralsSafe(expr Expr) bool {
	switch expr := expr.(type) {
	case nil, *VarRef:
		return true
	case *Literal:
		return cteAggregateValueSafe(expr.Val)
	case *Binary:
		return cteAggregateLiteralsSafe(expr.Left) && cteAggregateLiteralsSafe(expr.Right)
	case *Unary:
		return cteAggregateLiteralsSafe(expr.Expr)
	case *IsNull:
		return cteAggregateLiteralsSafe(expr.Expr)
	default:
		return false
	}
}
