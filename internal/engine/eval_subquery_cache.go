// Caching for WHERE/SELECT-list subqueries: EXISTS (...), scalar (SELECT ...)
// expressions, and the IN (SELECT ...) form. Without this cache, their
// evaluators call executeSelect on the subquery's SELECT every single
// time they are evaluated, which for a subquery inside a WHERE clause means
// once per outer row (applyWhereClause in exec_join.go). Most such
// subqueries do not depend on the outer row at all -- e.g.
// "WHERE x IN (SELECT y FROM small_table)" -- so re-running them per row is
// pure waste.
//
// The unsafe case is a genuinely correlated subquery, e.g.
// "WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.parent_id = outer.id)": caching
// its first result and reusing it for every subsequent outer row would
// silently produce wrong results. isSelectCorrelated below is the
// conservative, lexical safety check that keeps that from happening; see its
// doc comment for exactly what it checks and why unqualified references are
// not part of that check.
package engine

import (
	"fmt"
	"math"
	"strings"
)

// subqueryCacheEntry holds the cached outcome for one *ExistsExpr,
// *SubqueryExpr or direct *InExpr node, plus the correlation verdict that decides
// whether caching applies at all.
type subqueryCacheEntry struct {
	// correlated is computed once, the first time this node is evaluated
	// during the current statement execution (see isSelectCorrelated), and
	// then reused: correlation is a property of the subquery's static AST
	// shape, not of any particular row, so it never needs recomputing.
	correlated bool
	ready      bool
	rs         *ResultSet
	inSet      *inSubquerySet
	err        error
	// execCount counts every actual executeSelect call made for this node,
	// whether from a cache miss or because correlated/env.triggerRow bypassed
	// the cache entirely. It exists so tests can assert "executed exactly
	// once" directly instead of only checking that results are correct (a
	// cache that does nothing still returns correct results, just slowly).
	execCount int
}

// subqueryResultCache memoizes subquery rows or membership sets per AST node for
// one statement execution. Like windowPartitionCache (eval_window.go), it is
// deliberately scoped to a single top-level Execute() call (see its
// construction in exec_statement.go) rather than kept alive across calls:
// the tables a subquery reads can change between two executions of the same
// parsed/compiled statement (e.g. via tinysql.ExecuteCompiled), so a cache
// that outlived one execution would risk serving stale rows.
type subqueryResultCache struct {
	entries map[Expr]*subqueryCacheEntry
}

// newSubqueryResultCache returns an empty cache with entries left nil: most
// statements (plain INSERT/UPDATE/DELETE with no WHERE subquery, and even
// most SELECTs) never call evalCachedSubquery at all, so allocating the map
// unconditionally on every statement execution charged every one of them a
// map allocation nothing would ever read. Reads on a nil map are already
// safe zero-value lookups; evalCachedSubquery allocates the map lazily on
// its first actual write.
func newSubqueryResultCache() *subqueryResultCache {
	return &subqueryResultCache{}
}

// evalCachedSubquery runs sel -- the SELECT owned by node, an *ExistsExpr or
// *SubqueryExpr -- and returns its ResultSet, executing it only once per
// statement execution for nodes proven safe to cache and re-executing on
// every call otherwise. Callers must treat
// the returned *ResultSet as read-only: it may be handed back verbatim on a
// later call for the same node, so mutating it would corrupt the cache.
func evalCachedSubquery(env ExecEnv, node Expr, sel *Select) (*ResultSet, error) {
	cache := env.subqueryCache
	if cache == nil || env.triggerRow != nil {
		// No cache attached -- every ExecEnv reaching a statement body via
		// executeStatement gets one, but tests and a few internal callers
		// build a bare ExecEnv{} directly, and this stays correct (if
		// uncached) for those too -- or currently evaluating a trigger's
		// WHEN clause or body statement: NEW.col/OLD.col/bare-col can
		// resolve through env.triggerRow from inside sel (evalVarRef's
		// fallback, reached when sel's own row map has no matching key), and
		// that binding genuinely differs across firings of the very same
		// trigger AST within one bulk INSERT/UPDATE/DELETE -- a
		// per-invocation variance isSelectCorrelated's purely lexical check
		// cannot see, since it is not a property of sel's text. Always
		// re-execute in both cases.
		return executeSelect(env, sel)
	}
	entry := cache.entry(node, sel)
	if entry.correlated {
		entry.execCount++
		return executeSelect(env, sel)
	}
	if !entry.ready {
		entry.rs, entry.err = executeSelect(env, sel)
		entry.ready = true
		entry.execCount++
	}
	return entry.rs, entry.err
}

func (cache *subqueryResultCache) entry(node Expr, sel *Select) *subqueryCacheEntry {
	entry := cache.entries[node]
	if entry == nil {
		entry = &subqueryCacheEntry{correlated: isSelectCorrelated(sel)}
		if cache.entries == nil {
			cache.entries = make(map[Expr]*subqueryCacheEntry, 1)
		}
		cache.entries[node] = entry
	}
	return entry
}

// A direct IN node owns its membership cache separately from its SubqueryExpr.
// This permits discarding result Row maps without affecting a scalar use of
// the same subquery node in a hand-built AST. Parsed ASTs are never mutated.
func evalCachedInSubquerySet(env ExecEnv, owner *InExpr, sel *Select) (*inSubquerySet, error) {
	cache := env.subqueryCache
	if cache == nil || env.triggerRow != nil {
		rs, err := executeSelect(env, sel)
		if err != nil {
			return nil, err
		}
		return buildInSubquerySet(env, rs, false)
	}
	entry := cache.entry(owner, sel)
	if entry.correlated {
		entry.execCount++
		rs, err := executeSelect(env, sel)
		if err != nil {
			return nil, err
		}
		return buildInSubquerySet(env, rs, false)
	}
	if !entry.ready {
		entry.execCount++
		rs, err := executeSelect(env, sel)
		if err == nil {
			entry.inSet, err = buildInSubquerySet(env, rs, true)
		}
		entry.err, entry.ready = err, true
	}
	return entry.inSet, entry.err
}

// Retain values rather than one Row map per candidate. Unsupported probe
// types continue to use compare in input order; common types use typed sets.
// Integer and REAL sets are separate because int64 equality is exact while
// comparisons involving a REAL intentionally round integers to float64.
type inSubquerySet struct {
	values       []any
	integers     map[int64]struct{}
	reals        map[float64]struct{}
	integerReals map[float64]struct{} // built only for REAL probes
	texts        map[string]struct{}
	booleans     uint8
	hasRows      bool
	hasNull      bool
	hasNaN       bool
	indexed      bool
}

// Tiny sets are cheaper to scan than to index, while still dropping Row maps.
const inSubqueryIndexMinRows = 16

func buildInSubquerySet(env ExecEnv, rs *ResultSet, index bool) (*inSubquerySet, error) {
	set := &inSubquerySet{}
	if rs == nil {
		return set, nil
	}
	if len(rs.Cols) != 1 {
		return nil, fmt.Errorf("IN subquery must return exactly one column, got %d", len(rs.Cols))
	}
	set.hasRows = len(rs.Rows) > 0
	set.indexed = index && len(rs.Rows) >= inSubqueryIndexMinRows
	set.values = make([]any, 0, len(rs.Rows))
	column := strings.ToLower(rs.Cols[0])
	for i, row := range rs.Rows {
		if i&255 == 0 {
			if err := checkCtx(env.ctx); err != nil {
				return nil, err
			}
		}
		value := row[column]
		if value == nil {
			set.hasNull = true
			continue
		}
		set.values = append(set.values, value)
		if set.indexed {
			set.index(value)
		}
	}
	if len(set.values) == 0 {
		set.values = nil
	}
	return set, nil
}

func (s *inSubquerySet) index(value any) {
	switch value := value.(type) {
	case int:
		if s.integers == nil {
			s.integers = make(map[int64]struct{})
		}
		s.integers[int64(value)] = struct{}{}
	case int64:
		if s.integers == nil {
			s.integers = make(map[int64]struct{})
		}
		s.integers[value] = struct{}{}
	case float64:
		if math.IsNaN(value) {
			s.hasNaN = true
		} else {
			if s.reals == nil {
				s.reals = make(map[float64]struct{})
			}
			s.reals[value] = struct{}{}
		}
	case string:
		if s.texts == nil {
			s.texts = make(map[string]struct{})
		}
		s.texts[value] = struct{}{}
	case bool:
		if value {
			s.booleans |= 2
		} else {
			s.booleans |= 1
		}
	}
}

func (s *inSubquerySet) contains(env ExecEnv, value any) (bool, error) {
	if s.indexed {
		switch value := value.(type) {
		case int:
			return s.containsInteger(int64(value)), nil
		case int64:
			return s.containsInteger(value), nil
		case float64:
			// compare currently returns equality for NaN and any numeric
			// operand. Preserve that behavior instead of relying on NaN keys.
			if s.hasNaN {
				return true, nil
			}
			if math.IsNaN(value) {
				return len(s.integers) > 0 || len(s.reals) > 0, nil
			}
			if _, ok := s.reals[value]; ok {
				return true, nil
			}
			if len(s.integers) > 0 && s.integerReals == nil {
				if err := checkCtx(env.ctx); err != nil {
					return false, err
				}
				rounded := make(map[float64]struct{}, len(s.integers))
				processed := 0
				for integer := range s.integers {
					if processed&255 == 0 {
						if err := checkCtx(env.ctx); err != nil {
							return false, err
						}
					}
					rounded[float64(integer)] = struct{}{}
					processed++
				}
				if err := checkCtx(env.ctx); err != nil {
					return false, err
				}
				s.integerReals = rounded
			}
			_, ok := s.integerReals[value]
			return ok, nil
		case string:
			_, ok := s.texts[value]
			return ok, nil
		case bool:
			if value {
				return s.booleans&2 != 0, nil
			}
			return s.booleans&1 != 0, nil
		}
	}
	for _, candidate := range s.values {
		if cmp, err := compare(value, candidate); err == nil && cmp == 0 {
			return true, nil
		}
	}
	return false, nil
}

func (s *inSubquerySet) containsInteger(value int64) bool {
	if s.hasNaN {
		return true
	}
	if _, ok := s.integers[value]; ok {
		return true
	}
	_, ok := s.reals[float64(value)]
	return ok
}

// isSelectCorrelated reports whether sel -- the body of an EXISTS, scalar
// subquery, or IN (SELECT ...) expression -- may depend on state that varies
// across repeated evaluations of the same AST node within one statement
// execution, and therefore must never have its result cached.
//
// The check is conservative and purely lexical: sel, and everything nested
// within it (further sub-subqueries, its own CTEs, JOIN ON conditions, ...),
// is walked for any qualified column/table reference ("alias.col") whose
// qualifier does not name one of sel's own transitively reachable
// FROM/JOIN/CTE sources. Such a reference cannot resolve within sel's own
// scope, so given how evalVarRef actually resolves columns in this codebase
// (a subquery's row is built solely from its own FROM/JOIN sources --
// resolveFromClause never merges in an outer row; see exec_from.go) it
// either (a) genuinely reaches for a column the enclosing query exposes -- a
// correlated subquery, whose result legitimately differs per outer row --
// or (b) is simply unresolvable and errors every time. This static check
// cannot always tell those two apart, so both are conservatively treated as
// "do not cache" (caching case (b) would only cache a deterministic error,
// which would be harmless, but there is no cheap way to be sure it is (b)
// and not (a)).
//
// Unqualified references are deliberately never flagged as correlated: this
// engine has no code path through which an unqualified column name inside a
// subquery's own WHERE/SELECT-list/GROUP BY/etc. resolves against anything
// other than that subquery's own row, or errors -- there is no silent
// fallback to an enclosing row (again, see evalVarRef/resolveFromClause).
// The one real fallback beyond the row map, env.triggerRow (NEW.col/OLD.col/
// bare-col during a trigger's WHEN/body evaluation), is not a lexical
// property of sel's text and is guarded separately in evalCachedSubquery.
//
// Any expression shape this analysis does not explicitly know how to walk
// (see exprHasUnresolvedReference's default case) conservatively counts as
// an unresolved reference too, so a future AST addition this file was not
// updated for fails safe -- treated as correlated -- rather than silently
// falling through unexamined.
func isSelectCorrelated(sel *Select) bool {
	return selectHasUnresolvedReference(sel, map[string]bool{})
}

// selectHasUnresolvedReference walks sel for a reference that cannot be
// proven to resolve within sel's own transitively reachable scope. ownScope
// holds every alias/CTE/table name available to sel from levels *within* the
// subquery tree currently being analyzed (empty at the top-level call from
// isSelectCorrelated: nothing outside the subquery itself is assumed
// available, so any reference that escapes sel's own scope -- at any nesting
// depth -- is conservatively flagged, matching isSelectCorrelated's doc
// comment on why that is safe).
func selectHasUnresolvedReference(sel *Select, ownScope map[string]bool) bool {
	if sel == nil {
		return false
	}

	// CTEs see earlier sibling CTEs in the same WITH clause (the standard
	// "WITH a AS (...), b AS (SELECT * FROM a)" pattern) but not sel's own
	// FROM/JOIN aliases -- a CTE body cannot correlate to the query that
	// uses it. cteScope is grown one sibling at a time so a CTE is checked
	// against exactly what real SQL scoping would let it see.
	cteScope := ownScope
	for _, cte := range sel.CTEs {
		if selectHasUnresolvedReference(cte.Select, cteScope) {
			return true
		}
		if cte.Name != "" {
			extended := make(map[string]bool, len(cteScope)+1)
			for k := range cteScope {
				extended[k] = true
			}
			extended[strings.ToLower(cte.Name)] = true
			cteScope = extended
		}
	}

	scope := extendScope(cteScope, sel)

	if sel.From.Subquery != nil && selectHasUnresolvedReference(sel.From.Subquery, cteScope) {
		return true
	}
	for _, j := range sel.Joins {
		if j.Right.Subquery != nil && selectHasUnresolvedReference(j.Right.Subquery, cteScope) {
			return true
		}
		if exprHasUnresolvedReference(j.On, scope) {
			return true
		}
	}
	for _, p := range sel.Projs {
		if exprHasUnresolvedReference(p.Expr, scope) {
			return true
		}
	}
	if exprHasUnresolvedReference(sel.Where, scope) {
		return true
	}
	for _, g := range sel.GroupBy {
		if exprHasUnresolvedReference(g, scope) {
			return true
		}
	}
	if exprHasUnresolvedReference(sel.Having, scope) {
		return true
	}
	for _, d := range sel.DistinctOn {
		if exprHasUnresolvedReference(d, scope) {
			return true
		}
	}
	for _, o := range sel.OrderBy {
		if nameHasUnresolvedQualifier(strings.ToLower(o.Col), scope) {
			return true
		}
	}
	if sel.Pivot != nil {
		if exprHasUnresolvedReference(sel.Pivot.ValueExpr, scope) {
			return true
		}
		if nameHasUnresolvedQualifier(strings.ToLower(sel.Pivot.PivotCol), scope) {
			return true
		}
		for _, v := range sel.Pivot.Values {
			if exprHasUnresolvedReference(v.Expr, scope) {
				return true
			}
		}
	}
	// UNION branches are independent SELECTs, siblings of sel rather than
	// nested inside it: they see the same CTEs (cteScope) but not sel's own
	// FROM/JOIN aliases (scope).
	for cur := sel.Union; cur != nil; cur = cur.Next {
		if selectHasUnresolvedReference(cur.Right, cteScope) {
			return true
		}
	}
	return false
}

// extendScope returns a new scope containing base plus sel's own FROM/JOIN
// aliases (aliasOr: the AS alias if present, else the bare table name --
// exactly what rowsFromTable/resolveFromClause use to qualify row-map keys,
// so this matches the names a reference inside sel could actually resolve
// against).
func extendScope(base map[string]bool, sel *Select) map[string]bool {
	scope := make(map[string]bool, len(base)+1+len(sel.Joins))
	for k := range base {
		scope[k] = true
	}
	if a := aliasOr(sel.From); a != "" {
		scope[strings.ToLower(a)] = true
	}
	for _, j := range sel.Joins {
		if a := aliasOr(j.Right); a != "" {
			scope[strings.ToLower(a)] = true
		}
	}
	return scope
}

// exprHasUnresolvedReference reports whether e, or anything nested within it
// (including further-nested EXISTS/scalar/IN subqueries), contains a
// qualified column/table reference whose qualifier is not in scope. See
// selectHasUnresolvedReference's doc comment for what "scope" contains and
// isSelectCorrelated's doc comment for why an unqualified reference is never
// flagged here.
//
// Any Expr shape not explicitly recognized below falls to the default case,
// which conservatively reports "unresolved" -- see isSelectCorrelated's doc
// comment on why that fail-safe matters more than exhaustiveness.
func exprHasUnresolvedReference(e Expr, scope map[string]bool) bool {
	switch ex := e.(type) {
	case nil:
		return false
	case *Literal:
		return false
	case *VarRef:
		lower := ex.Lower
		if lower == "" {
			lower = strings.ToLower(ex.Name)
		}
		return nameHasUnresolvedQualifier(lower, scope)
	case *Unary:
		return exprHasUnresolvedReference(ex.Expr, scope)
	case *Binary:
		return exprHasUnresolvedReference(ex.Left, scope) || exprHasUnresolvedReference(ex.Right, scope)
	case *IsNull:
		return exprHasUnresolvedReference(ex.Expr, scope)
	case *FuncCall:
		for _, arg := range ex.Args {
			if exprHasUnresolvedReference(arg, scope) {
				return true
			}
		}
		if ex.Over != nil {
			for _, p := range ex.Over.PartitionBy {
				if exprHasUnresolvedReference(p, scope) {
					return true
				}
			}
			for _, o := range ex.Over.OrderBy {
				if nameHasUnresolvedQualifier(strings.ToLower(o.Col), scope) {
					return true
				}
			}
		}
		return false
	case *InExpr:
		if exprHasUnresolvedReference(ex.Expr, scope) {
			return true
		}
		for _, v := range ex.Values {
			if exprHasUnresolvedReference(v, scope) {
				return true
			}
		}
		return false
	case *LikeExpr:
		return exprHasUnresolvedReference(ex.Expr, scope) ||
			exprHasUnresolvedReference(ex.Pattern, scope) ||
			exprHasUnresolvedReference(ex.Escape, scope)
	case *RegexpExpr:
		return exprHasUnresolvedReference(ex.Expr, scope) || exprHasUnresolvedReference(ex.Pattern, scope)
	case *BetweenExpr:
		return exprHasUnresolvedReference(ex.Expr, scope) ||
			exprHasUnresolvedReference(ex.Lo, scope) ||
			exprHasUnresolvedReference(ex.Hi, scope)
	case *ExistsExpr:
		return selectHasUnresolvedReference(ex.Select, scope)
	case *SubqueryExpr:
		return selectHasUnresolvedReference(ex.Select, scope)
	case *CaseExpr:
		if exprHasUnresolvedReference(ex.Operand, scope) {
			return true
		}
		for _, w := range ex.Whens {
			if exprHasUnresolvedReference(w.When, scope) || exprHasUnresolvedReference(w.Then, scope) {
				return true
			}
		}
		return exprHasUnresolvedReference(ex.Else, scope)
	default:
		return true
	}
}

// nameHasUnresolvedQualifier reports whether lowerName is qualified
// ("alias.col") and its qualifier is absent from scope. Unqualified names
// (no ".") are never flagged -- see isSelectCorrelated's doc comment.
func nameHasUnresolvedQualifier(lowerName string, scope map[string]bool) bool {
	dot := strings.IndexByte(lowerName, '.')
	if dot < 0 {
		return false
	}
	return !scope[lowerName[:dot]]
}
