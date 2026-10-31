// SELECT: the statement entry point, and the plan types the fast paths below
// compile a query into.
package engine

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func executeSelect(env ExecEnv, s *Select) (result *ResultSet, err error) {
	if env.profile != nil {
		var phase *queryPhase
		detail := s.From.Table
		if s.From.Subquery != nil {
			detail = "derived table"
		} else if s.From.TableFunc != nil {
			detail = "table function"
		} else if detail == "" {
			detail = "constant row"
		}
		if s.From.Alias != "" {
			detail += " AS " + s.From.Alias
		}
		env, phase = startQueryPhase(env, "SELECT", detail)
		defer func() {
			var rows int64
			if result != nil {
				rows = int64(len(result.Rows))
			}
			phase.finish(rows)
		}()
	}
	cteInput := env
	var ctePhase *queryPhase
	if len(s.CTEs) > 0 {
		cteInput, ctePhase = startQueryPhase(env, "CTE", "materialize common table expressions")
	}
	cteEnv, err := processCTEs(cteInput, s)
	if ctePhase != nil {
		var rows int64
		for _, cte := range s.CTEs {
			if rs := cteEnv.ctes[strings.ToLower(cte.Name)]; rs != nil {
				rows += int64(len(rs.Rows))
			}
		}
		ctePhase.finish(rows)
		// Keep the bindings created by processCTEs, but make subsequent
		// execution phases siblings of CTE materialization.
		cteEnv.profileParent = env.profileParent
	}
	if err != nil {
		return nil, err
	}
	// Fast paths access physical storage directly. A CTE source exists only in
	// the execution environment, so bypass them whenever FROM/JOIN references
	// an active CTE; otherwise recursive and chained CTEs are treated as
	// missing physical tables.
	referencesCTE := selectReferencesCTE(cteEnv, s)
	if !referencesCTE {
		// Tried before the plain join and plain aggregate fast paths: both of
		// those deliberately reject any query that has both a JOIN and a
		// GROUP BY (see their eligibility checks), so this is the only fast
		// path that can claim that shape. Trying it first is not load-bearing
		// for correctness -- the other two already reject this shape either
		// way -- but keeps the more specific check first.
		if rs, ok, err := executeSimpleJoinAggregateFastPath(cteEnv, s); ok || err != nil {
			return rs, err
		}
		if rs, ok, err := executeSimpleJoinFastPath(cteEnv, s); ok || err != nil {
			return rs, err
		}
		if rs, ok, err := executeSimpleAggregateFastPath(cteEnv, s); ok || err != nil {
			return rs, err
		}
		if rs, ok, err := executeSimpleSelectFastPath(cteEnv, s); ok || err != nil {
			return rs, err
		}
	} else if rs, ok, err := executeSimpleCTESelectFastPath(cteEnv, s); ok || err != nil {
		return rs, err
	}

	// ORDER BY/LIMIT/OFFSET after a set operation belong to the whole compound
	// result, not to its left-most term.  Keep the term's projection free of
	// those global clauses: besides avoiding a premature sort/page, this
	// prevents a non-projected source sort key from leaking into the maps that
	// are later compared positionally by UNION/EXCEPT/INTERSECT.
	compound := s.Union != nil
	term := s
	if compound {
		termCopy := *s
		termCopy.OrderBy = nil
		termCopy.Limit = nil
		termCopy.Offset = nil
		term = &termCopy
	}

	if s.From.TableFunc != nil {
		cteEnv.tableFuncColumns = make(map[*TableFuncCall][]string)
	} else {
		for _, join := range s.Joins {
			if join.Right.TableFunc != nil {
				cteEnv.tableFuncColumns = make(map[*TableFuncCall][]string)
				break
			}
		}
	}
	// FROM (Tabelle, CTE oder Subselect) - now optional
	phaseEnv, phase := startQueryPhase(cteEnv, "SOURCE", "materialize FROM rows")
	leftRows, err := resolveFromClause(phaseEnv, phaseEnv, s)
	phase.finish(int64(len(leftRows)))
	if err != nil {
		return nil, err
	}

	cur := leftRows

	// JOINs
	phaseEnv = cteEnv
	phase = nil
	if len(s.Joins) > 0 {
		phaseEnv, phase = startQueryPhase(cteEnv, "JOIN", "general joins")
	}
	cur, err = processJoins(phaseEnv, s.Joins, cur)
	phase.finish(int64(len(cur)))
	if err != nil {
		return nil, err
	}

	// WHERE
	phaseEnv = cteEnv
	phase = nil
	if s.Where != nil {
		phaseEnv, phase = startQueryPhase(cteEnv, "FILTER", "WHERE")
	}
	filtered, err := applyWhereClause(phaseEnv, s.Where, cur)
	phase.finish(int64(len(filtered)))
	if err != nil {
		return nil, err
	}

	// GROUP/HAVING
	phaseEnv = cteEnv
	phase = nil
	if cteEnv.profile != nil {
		operation, detail := "PROJECT", "expressions and window functions"
		if term.Pivot != nil {
			operation, detail = "PIVOT/PROJECT", "pivot and projection"
		} else if len(term.GroupBy) > 0 || anyAggInSelect(term.Projs) || isAggregate(term.Having) {
			operation, detail = "GROUP/PROJECT/HAVING", "general aggregation"
		}
		phaseEnv, phase = startQueryPhase(cteEnv, operation, detail)
	}
	outRows, outCols, err := processGroupByHaving(phaseEnv, term, filtered)
	phase.finish(int64(len(outRows)))
	if err != nil {
		return nil, err
	}

	// DISTINCT
	if term.Distinct {
		detail := "deduplicate projected rows"
		if len(term.DistinctOn) > 0 {
			detail = "DISTINCT ON including ordering"
		}
		phaseEnv, phase = startQueryPhase(cteEnv, "DISTINCT", detail)
		// If DISTINCT ON (...) was used, apply DISTINCT ON semantics: keep first
		// row per distinct-on key. The ORDER BY clause controls which row is
		// considered "first"; so apply ORDER BY first if present.
		if len(term.DistinctOn) > 0 {
			var err error
			outRows, err = applyDistinctOn(phaseEnv, term, outRows)
			if err != nil {
				phase.finish(int64(len(outRows)))
				return nil, err
			}
		} else {
			outRows = distinctRows(outRows, outCols)
		}
		phase.finish(int64(len(outRows)))
	}

	// A simple SELECT applies its tail directly.  For a compound SELECT the
	// parser attaches that tail to the outer Select, so defer it until every
	// right-hand term has been combined below.
	resultRows := outRows
	resultCols := outCols
	var orderAliases map[string]string
	if compound && len(s.OrderBy) > 0 {
		orderAliases = make(map[string]string, len(resultCols))
		for _, col := range resultCols {
			orderAliases[strings.ToLower(col)] = col
		}
	}
	if !compound {
		if len(s.OrderBy) > 0 {
			_, phase = startQueryPhase(cteEnv, "SORT", "ORDER BY")
			resultRows = applySortOrderWithLimit(s.OrderBy, resultRows, s.Limit, s.Offset)
			phase.finish(int64(len(resultRows)))
		}
		phase = nil
		if s.Limit != nil || s.Offset != nil {
			_, phase = startQueryPhase(cteEnv, "LIMIT", "OFFSET/LIMIT")
		}
		resultRows = applyOffsetLimit(s, resultRows)
		phase.finish(int64(len(resultRows)))
	} else {
		var err error
		phaseEnv, phase = startQueryPhase(cteEnv, "SET OPERATION", "combine query terms")
		resultRows, resultCols, err = processUnionClauses(phaseEnv, s.Union, resultRows, resultCols, orderAliases)
		phase.finish(int64(len(resultRows)))
		if err != nil {
			return nil, err
		}
	}

	if len(resultCols) == 0 {
		resultCols = columnsFromRows(resultRows)
		if orderAliases != nil {
			for _, col := range resultCols {
				if _, exists := orderAliases[strings.ToLower(col)]; !exists {
					orderAliases[strings.ToLower(col)] = col
				}
			}
		}
	}
	if compound {
		if len(s.OrderBy) > 0 {
			_, phase = startQueryPhase(cteEnv, "SORT", "compound ORDER BY")
			compoundOrderBy, err := resolveCompoundOrderBy(s.OrderBy, orderAliases)
			if err != nil {
				phase.finish(0)
				return nil, err
			}
			resultRows = applySortOrderWithLimit(compoundOrderBy, resultRows, s.Limit, s.Offset)
			phase.finish(int64(len(resultRows)))
		}
		phase = nil
		if s.Limit != nil || s.Offset != nil {
			_, phase = startQueryPhase(cteEnv, "LIMIT", "compound OFFSET/LIMIT")
		}
		resultRows = applyOffsetLimit(s, resultRows)
		phase.finish(int64(len(resultRows)))
	}
	return &ResultSet{Cols: resultCols, Rows: resultRows}, nil
}

// executeSimpleCTESelectFastPath fuses a single materialized-CTE scan with a
// simple WHERE, direct-column projection, OFFSET and LIMIT. The general path
// first copies every CTE row to add source qualifiers, then allocates a second
// slice for WHERE and finally projects the survivors. Bare references and
// unambiguous CTE-name/alias-qualified projections can read the immutable
// ResultSet directly. Qualified predicates and dotted output names retain
// the general path and its full lookup semantics.
func executeSimpleCTESelectFastPath(env ExecEnv, s *Select) (result *ResultSet, ok bool, err error) {
	if s == nil || len(s.Joins) > 0 || len(s.GroupBy) > 0 || s.Having != nil ||
		s.Union != nil || s.Pivot != nil || s.Distinct || len(s.OrderBy) > 0 ||
		s.From.Table == "" || s.From.Subquery != nil || s.From.TableFunc != nil ||
		anyAggInSelect(s.Projs) || anyWindowInSelect(s.Projs) {
		return nil, false, nil
	}
	cteResult, ok := env.ctes[strings.ToLower(s.From.Table)]
	if !ok || cteResult == nil {
		return nil, false, nil
	}
	// Resolve source qualifiers once instead of constructing qualified maps
	// for every materialized row. Dotted output column names retain the
	// generic lookup rules, where qualified and literal dotted keys can collide.
	resolveSource := func(name string) (string, bool) {
		if name == "" {
			return "", false
		}
		qualifier, column, qualified := strings.Cut(name, ".")
		if !qualified {
			return name, true
		}
		if qualifier != strings.ToLower(s.From.Table) &&
			(s.From.Alias == "" || qualifier != strings.ToLower(s.From.Alias)) {
			return "", false
		}
		found := false
		for _, col := range cteResult.Cols {
			if strings.Contains(col, ".") {
				return "", false
			}
			if strings.ToLower(col) == column {
				found = true
			}
		}
		return column, found
	}
	// Reject expression/star projections before allocating a candidate plan.
	// Recursive CTE members commonly project expressions and execute hundreds
	// of times, so even two speculative slices here become measurable churn.
	for _, item := range s.Projs {
		ref, direct := item.Expr.(*VarRef)
		if item.Star || !direct {
			return nil, false, nil
		}
		name := ref.Lower
		if name == "" {
			name = strings.ToLower(ref.Name)
		}
		if _, valid := resolveSource(name); !valid {
			return nil, false, nil
		}
	}

	type projection struct {
		source string
		name   string
		key    string
	}
	projections := make([]projection, 0, len(s.Projs))
	outputCols := make([]string, 0, len(s.Projs))
	for i, item := range s.Projs {
		ref := item.Expr.(*VarRef)
		source := ref.Lower
		if source == "" {
			source = strings.ToLower(ref.Name)
		}
		source, _ = resolveSource(source)
		name := projName(item, i)
		projections = append(projections, projection{source: source, name: name, key: strings.ToLower(name)})
		seen := false
		for _, existing := range outputCols {
			if existing == name {
				seen = true
				break
			}
		}
		if !seen {
			outputCols = append(outputCols, name)
		}
	}

	filter := buildRowWhereFilter(s.Where)
	if s.Where != nil && (filter == nil || exprHasQualifiedVarRef(s.Where)) {
		return nil, false, nil
	}
	offset := 0
	if s.Offset != nil && *s.Offset > 0 {
		offset = *s.Offset
	}
	limit := -1
	if s.Limit != nil {
		limit = *s.Limit
	}
	if limit == 0 {
		// The general CTE path still evaluates WHERE for LIMIT 0 and can report
		// expression/type errors. Keep that observable behavior for this edge
		// case instead of returning before the predicate is evaluated.
		return nil, false, nil
	}
	if env.profile != nil {
		var phase *queryPhase
		env, phase = startQueryPhase(env, "SCAN/FILTER/PROJECT", "materialized CTE")
		defer func() {
			var rows int64
			if result != nil {
				rows = int64(len(result.Rows))
			}
			phase.finish(rows)
		}()
	}
	capacity := len(cteResult.Rows) - offset
	if capacity < 0 {
		capacity = 0
	}
	if limit >= 0 && limit < capacity {
		capacity = limit
	} else if filter != nil {
		capacity /= 2
	}
	outRows := make([]Row, 0, capacity)
	matched := 0
	for i, row := range cteResult.Rows {
		if i&63 == 0 {
			if err := checkCtx(env.ctx); err != nil {
				return nil, true, err
			}
		}
		if filter != nil {
			tri, err := filter(row)
			if err != nil {
				return nil, true, err
			}
			if tri != tvTrue {
				continue
			}
		}
		if matched < offset {
			matched++
			continue
		}
		out := make(Row, len(projections))
		for _, p := range projections {
			value, exists := row[p.source]
			if !exists {
				return nil, true, unknownColumnErr(p.name, columnSuggestionFromRow(p.name, row))
			}
			out[p.key] = value
		}
		outRows = append(outRows, out)
		matched++
		if limit >= 0 && len(outRows) >= limit {
			break
		}
	}
	return &ResultSet{Cols: outputCols, Rows: outRows}, true, nil
}

func exprHasQualifiedVarRef(expr Expr) bool {
	switch ex := expr.(type) {
	case nil:
		return false
	case *VarRef:
		name := ex.Lower
		if name == "" {
			name = ex.Name
		}
		return strings.Contains(name, ".")
	case *Literal:
		return false
	case *Unary:
		return exprHasQualifiedVarRef(ex.Expr)
	case *Binary:
		return exprHasQualifiedVarRef(ex.Left) || exprHasQualifiedVarRef(ex.Right)
	case *IsNull:
		return exprHasQualifiedVarRef(ex.Expr)
	default:
		return true
	}
}

// resolveCompoundOrderBy maps every trailing ORDER BY alias to the canonical
// left-most result-column name.  SQLite permits an alias defined by a later
// term as long as it identifies the corresponding result position.
func resolveCompoundOrderBy(orderBy []OrderItem, aliases map[string]string) ([]OrderItem, error) {
	resolved := make([]OrderItem, len(orderBy))
	for i, item := range orderBy {
		canonical, found := aliases[strings.ToLower(item.Col)]
		if !found {
			return nil, fmt.Errorf("ORDER BY: no such column %q", item.Col)
		}
		resolved[i] = item
		resolved[i].Col = canonical
	}
	return resolved, nil
}

// selectReferencesCTE reports whether a SELECT needs rows bound in the active
// CTE environment instead of a physical table lookup.
func selectReferencesCTE(env ExecEnv, s *Select) bool {
	if len(env.ctes) == 0 || s == nil {
		return false
	}
	fromReferencesCTE := func(from FromItem) bool {
		if from.Table == "" {
			return false
		}
		_, ok := env.ctes[strings.ToLower(from.Table)]
		return ok
	}
	if fromReferencesCTE(s.From) {
		return true
	}
	for _, join := range s.Joins {
		if fromReferencesCTE(join.Right) {
			return true
		}
	}
	return false
}

type simpleSelectPlan struct {
	// now belongs to the execution copy, never the cached plan template.
	now        time.Time
	table      *storage.Table
	colIndex   map[string]int
	projs      []simpleProjection
	orderBy    []OrderItem
	orderExprs []Expr
	// orderCols maps each ORDER BY term to its raw column index when every
	// term is a direct column reference; nil when any term needs expression
	// evaluation. With it set, the ordered fast path skips per-row
	// evalRawExpr map lookups and (for multi-column orders) the per-row
	// keys slice.
	orderCols []int
	where     Expr
	// filter is a pre-compiled, allocation-free version of where for the most
	// common patterns (col op literal, boolean column, AND/OR of those). When
	// non-nil it replaces the recursive evalRawWhere call in the hot scan loop.
	filter     func([]any) (bool, error)
	limit      *int
	offset     *int
	outputCols []string
	rowMapCap  int
	// rowTextCols contains raw column positions in the same sorted-name order
	// as ROW_TO_TEXT's Row-map implementation. It is prepared once per plan so
	// ROW_TO_TEXT predicates do not need to materialize a Row map per input.
	// Read it through rowTextColumns, never directly: it is built on first
	// use, because only ROW_TO_TEXT reads it and almost no statement calls
	// ROW_TO_TEXT — building it up front charged every raw-path INSERT,
	// UPDATE, DELETE and SELECT a map walk, an allocation and a sort for a
	// value nothing then looked at.
	rowTextCols []int
	// rowIDs is nil for a table scan. A non-nil slice is a materialized
	// secondary-index point/prefix seek and contains table row positions.
	rowIDs []int
	// rows is a query-private source supplied by page-oriented index seeks.
	// It is never retained in a cached plan, so decoded BLOBs remain bounded
	// to the request that loaded them.
	rows        [][]any
	pagedSource *pagedSimpleSelectSource
	// pagedSingleRow marks a paged point seek on a unique index with every
	// column bound: it can yield at most one row.
	pagedSingleRow  bool
	scanType        string
	indexName       string
	indexPredicates []string
	residualFilter  bool
	// filterFullyCovered marks access paths that already applied every WHERE
	// predicate, avoiding a redundant raw predicate evaluation per matched row.
	filterFullyCovered bool
	coveringIndex      bool
	estimatedRows      int
}

// simpleSelectPlanCache stores the parameter-independent shape of a parsed
// simple SELECT. It deliberately excludes RowIDs and access-path estimates:
// those depend on the current bound values and current index contents. The
// cache lives on the AST, so its lifetime is bounded by the existing parsed
// statement cache or database/sql prepared statement.
type simpleSelectPlanCache struct {
	// entry is immutable after publication, so warm executions need only one
	// atomic load. mu is the singleflight boundary for cold construction and
	// schema replacement; it is never taken by a valid cache hit.
	mu    sync.Mutex
	entry atomic.Pointer[simpleSelectPlanCacheEntry]
}

type simpleSelectPlanCacheEntry struct {
	table    *storage.Table
	colCount int
	plan     *simpleSelectPlan
}

// simpleProjection describes a single SELECT item in the raw fast-path.
// When colIdx >= 0 the projection is a direct column reference: the value is
// taken from raw[colIdx] without going through evalRawExpr, saving a type
// switch, a strings.ToLower call, and a map lookup per row per column.
// key is the pre-lowercased name used as the Row map key (avoids putVal's
// strings.ToLower on every output row).
// side is only meaningful for join projections: 0=left table, 1=right table,
// -1=single-table context or expression that could not be resolved to a simple
// column reference (use expr instead).
type simpleProjection struct {
	name   string // output column name (original case for ResultSet.Cols)
	key    string // strings.ToLower(name) – pre-computed Row map key
	altKey string // optional second Row map key (e.g. "alias.col" for SELECT *,
	// matching rowsFromTable's qualified+unqualified dual keys); empty if unused
	side   int  // 0=left, 1=right (join), -1=single-table or expression
	colIdx int  // >= 0: direct array index into raw/left/right; -1: use expr
	expr   Expr // used when colIdx < 0
}

type simpleJoinPlan struct {
	left        *storage.Table
	right       *storage.Table
	leftIndex   map[string]int
	rightIndex  map[string]int
	leftKey     int
	rightKey    int
	where       Expr
	leftFilter  func([]any) (bool, error)
	rightFilter func([]any) (bool, error)
	projs       []simpleProjection
	outputCols  []string
	// rowMapCap sizes the output Row map, counting the qualified alternate
	// keys that SELECT * projections carry in addition to their primary key.
	rowMapCap int
	// rightLookup is an immutable hash index of the right input for the most
	// recently observed table version. It avoids rebuilding the same index for
	// every execution of a prepared read query; a write to the right table
	// changes Version and forces a rebuild before its next use.
	rightLookup simpleJoinRightLookupCache
}

// simpleJoinPlanCache holds only the compiled, parameter-independent join
// shape. The table pointers are part of the cache key: DDL can replace a table
// object, while ordinary DML safely updates rows on the same object under the
// content lock held by executeStatement.
type simpleJoinPlanCache struct {
	mu    sync.Mutex
	left  *storage.Table
	right *storage.Table
	plan  *simpleJoinPlan
}

// simpleJoinAggregatePlanCache holds a compiled join-and-aggregate shape for
// repeated executions of the same parsed SELECT. Table identities are kept in
// the key for the same reason as simpleJoinPlanCache: DDL may replace either
// table object, whereas row changes do not invalidate column resolution.
type simpleJoinAggregatePlanCache struct {
	mu    sync.Mutex
	left  *storage.Table
	right *storage.Table
	plan  *simpleJoinAggregatePlan
}

type simpleJoinRightLookupCache struct {
	mu      sync.RWMutex
	table   *storage.Table
	version int
	index   *simpleJoinRightIndex
}
