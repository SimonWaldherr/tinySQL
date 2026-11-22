package engine

import (
	"fmt"
	"maps"
	"strings"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

type applyRowScope struct {
	row          Row
	parent       *applyRowScope
	lookupSelect *Select
	lookupRows   []Row
}

func lookupApplyColumn(env ExecEnv, key string) (any, bool) {
	dot := strings.LastIndexByte(key, '.')
	if dot >= 0 && env.applyLocal != nil {
		qualifier := key[:dot]
		if strings.EqualFold(aliasOr(env.applyLocal.From), qualifier) {
			return nil, false
		}
		for _, join := range env.applyLocal.Joins {
			if strings.EqualFold(aliasOr(join.Right), qualifier) {
				return nil, false
			}
		}
	}
	for scope := env.applyOuter; scope != nil; scope = scope.parent {
		if value, ok := scope.row[key]; ok {
			return value, true
		}
		if dot >= 0 {
			// An intervening scope that binds this qualifier shadows earlier
			// scopes even when it has no column of the requested name.
			for column := range scope.row {
				if strings.HasPrefix(column, key[:dot+1]) {
					return nil, false
				}
			}
		}
	}
	return nil, false
}

func isApply(joinType JoinType) bool {
	return joinType == JoinCrossApply || joinType == JoinOuterApply
}

func processApply(env ExecEnv, join JoinClause, left []Row) (out []Row, err error) {
	source := join.Right
	inner := applyProjectionNames(source.Subquery)
	var fn TableFunction
	if tf := source.TableFunc; tf != nil {
		var ok bool
		fn, ok = GetTableFunc(tf.Name)
		if !ok {
			return nil, fmt.Errorf("unknown table function: %s", tf.Name)
		}
		if err := fn.ValidateArgs(tf.Args); err != nil {
			return nil, err
		}
	}
	var phase *queryPhase
	if env.profile != nil {
		env, phase = startQueryPhase(env, join.Type.String(), aliasOr(source))
		defer func() { phase.finish(int64(len(out))) }()
	}
	if len(left) == 0 {
		if source.Subquery != nil && env.applyColumns != nil {
			cols, err := selectProjectionColumns(env, inner, true)
			if err != nil {
				return nil, err
			}
			env.applyColumns[source.Subquery] = cols
		}
		return left, nil
	}
	if err := checkCtx(env.ctx); err != nil {
		return nil, err
	}

	scope := applyRowScope{parent: env.applyOuter}
	env.applyOuter = &scope
	// A nested scalar subquery can depend on this APPLY's current binding,
	// including through an unqualified name. Reuse the cache only within one
	// right-hand evaluation.
	cache := newSubqueryResultCache()
	defer releaseSubqueryResultCache(cache)
	env.subqueryCache = cache
	env.cteRowCache = nil
	lookup, err := prepareApplyLookup(env, inner, left)
	if err != nil {
		return nil, err
	}
	if lookup != nil {
		inner = &lookup.selectWithoutEquality
		scope.lookupSelect = inner
	}
	alias := strings.ToLower(source.Alias)
	keys := make(map[string]derivedKeys)
	var cols []string
	out = make([]Row, 0, min(len(left), int(maxJoinRows)))
	for i, row := range left {
		if err := checkCtx(env.ctx); err != nil {
			return nil, err
		}
		scope.row = row
		clear(cache.entries)
		var right *ResultSet
		if fn != nil {
			right, err = fn.Execute(env.ctx, source.TableFunc.Args, env, row)
		} else {
			if lookup != nil {
				// Reuse the candidate slice across correlation values.
				scope.lookupRows = lookup.rowsFor(i, scope.lookupRows[:0])
			}
			right, err = executeSelect(env, inner)
		}
		if err != nil {
			return nil, err
		}
		var rows []Row
		if right != nil {
			rows, cols = right.Rows, right.Cols
		}
		if len(rows) == 0 && join.Type == JoinOuterApply {
			if int64(len(out)) >= maxJoinRows {
				return nil, fmt.Errorf("%s exceeded row limit %d", join.Type, maxJoinRows)
			}
			out = append(out, applyNullRow(row, cols, alias, keys))
			continue
		}
		if int64(len(rows)) > maxJoinRows-int64(len(out)) {
			return nil, fmt.Errorf("%s exceeded row limit %d", join.Type, maxJoinRows)
		}
		for ri, r := range rows {
			if ri&255 == 0 {
				if err := checkCtx(env.ctx); err != nil {
					return nil, err
				}
			}
			merged := make(Row, len(row)+2*len(cols))
			maps.Copy(merged, row)
			for _, col := range cols {
				k := applyColumnKey(keys, alias, col)
				value, exists := r[k.plain]
				if !exists && col != k.plain {
					value = r[col]
				}
				merged[k.plain] = value
				if alias != "" {
					merged[k.qualified] = value
				}
			}
			out = append(out, merged)
		}
	}
	if source.Subquery != nil && env.applyColumns != nil {
		env.applyColumns[source.Subquery] = cols
	}
	if source.TableFunc != nil && env.tableFuncColumns != nil {
		env.tableFuncColumns[source.TableFunc] = cols
	}
	return out, nil
}

// A plain column reference contributes its column name to a derived APPLY
// source. Preserve explicit aliases, including quoted names with dots.
func applyProjectionNames(inner *Select) *Select {
	if inner == nil {
		return nil
	}
	var renamed *Select
	for i, project := range inner.Projs {
		ref, ok := project.Expr.(*VarRef)
		if !ok || project.Alias != "" || !strings.Contains(ref.Name, ".") {
			continue
		}
		if renamed == nil {
			copied := *inner
			copied.Projs = append([]SelectItem(nil), inner.Projs...)
			renamed = &copied
		}
		renamed.Projs[i].Alias = ref.Name[strings.LastIndexByte(ref.Name, '.')+1:]
	}
	if renamed != nil {
		return renamed
	}
	return inner
}

func applyColumnKey(keys map[string]derivedKeys, alias, col string) derivedKeys {
	if key, ok := keys[col]; ok {
		return key
	}
	plain := strings.ToLower(col)
	key := derivedKeys{plain: plain}
	if alias != "" {
		key.qualified = alias + "." + plain
	}
	keys[col] = key
	return key
}

func applyNullRow(left Row, cols []string, alias string, keys map[string]derivedKeys) Row {
	out := make(Row, len(left)+2*len(cols))
	maps.Copy(out, left)
	for _, col := range cols {
		key := applyColumnKey(keys, alias, col)
		if _, ok := out[key.plain]; !ok {
			out[key.plain] = nil
		}
		if alias != "" {
			if _, ok := out[key.qualified]; !ok {
				out[key.qualified] = nil
			}
		}
	}
	return out
}

func addStaticApplyNulls(env ExecEnv, left []Row, source FromItem, table *storage.Table) ([]Row, error) {
	if int64(len(left)) > maxJoinRows {
		return nil, fmt.Errorf("OUTER APPLY exceeded row limit %d", maxJoinRows)
	}
	out := make([]Row, 0, len(left))
	for i, row := range left {
		if i&63 == 0 {
			if err := checkCtx(env.ctx); err != nil {
				return nil, err
			}
		}
		merged := cloneRow(row)
		addRightNulls(merged, aliasOr(source), table)
		out = append(out, merged)
	}
	return out, nil
}

type applyLookup struct {
	selectWithoutEquality Select
	right                 []Row
	candidates            *joinCandidateLookup
}

// Precompute candidate rows for a lone correlated equality against a stored
// table. The join lookup proves that its keys obey SQL comparison semantics.
// The right SELECT still projects, sorts and limits the candidates separately
// for each outer row.
func prepareApplyLookup(env ExecEnv, sel *Select, left []Row) (*applyLookup, error) {
	if len(left) < 8 || sel == nil || sel.From.Table == "" || sel.From.Subquery != nil || sel.From.TableFunc != nil ||
		len(sel.CTEs) != 0 || len(sel.Joins) != 0 || sel.Union != nil ||
		isCatalogOrSysTableRef(sel.From.Table) || isSQLiteSchemaTable(sel.From.Table) {
		return nil, nil
	}
	equality := compileJoinEquality(sel.Where)
	if equality == nil || env.ctes[strings.ToLower(sel.From.Table)] != nil {
		return nil, nil
	}
	localPrefix := strings.ToLower(aliasOr(sel.From)) + "."
	if strings.HasPrefix(equality.leftColumn, localPrefix) == strings.HasPrefix(equality.rightColumn, localPrefix) ||
		!strings.Contains(equality.leftColumn, ".") || !strings.Contains(equality.rightColumn, ".") {
		return nil, nil
	}
	table, err := env.db.Get(env.tenant, sel.From.Table)
	if err != nil || len(table.Rows) == 0 || len(left) <= 1024/len(table.Rows) {
		return nil, nil
	}
	var phase *queryPhase
	if env.profile != nil {
		_, phase = startQueryPhase(env, "APPLY LOOKUP", sel.From.Table)
	}
	right, _ := rowsFromTable(table, aliasOr(sel.From))
	lookup, err := buildJoinCandidateLookup(env, equality, left, right)
	phase.finish(int64(len(right)))
	if lookup == nil || err != nil {
		return nil, err
	}
	inner := *sel
	inner.Where = nil
	return &applyLookup{selectWithoutEquality: inner, right: right, candidates: lookup}, nil
}

func (p *applyLookup) rowsFor(index int, rows []Row) []Row {
	for i := p.candidates.firstCandidate(index, len(p.right)); i != 0; i = p.candidates.nextCandidate(i, len(p.right)) {
		rows = append(rows, p.right[i-1])
	}
	return rows
}
