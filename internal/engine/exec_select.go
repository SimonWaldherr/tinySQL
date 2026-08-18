// SELECT: the statement entry point, and the plan types the fast paths below
// compile a query into.
package engine

import (
	"fmt"
	"strings"
	"sync"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func executeSelect(env ExecEnv, s *Select) (*ResultSet, error) {
	cteEnv, err := processCTEs(env, s)
	if err != nil {
		return nil, err
	}
	// Fast paths access physical storage directly. A CTE source exists only in
	// the execution environment, so bypass them whenever FROM/JOIN references
	// an active CTE; otherwise recursive and chained CTEs are treated as
	// missing physical tables.
	if !selectReferencesCTE(cteEnv, s) {
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

	// FROM (Tabelle, CTE oder Subselect) - now optional
	leftRows, err := resolveFromClause(cteEnv, cteEnv, s)
	if err != nil {
		return nil, err
	}

	cur := leftRows

	// JOINs
	cur, err = processJoins(cteEnv, s.Joins, cur)
	if err != nil {
		return nil, err
	}

	// WHERE
	filtered, err := applyWhereClause(cteEnv, s.Where, cur)
	if err != nil {
		return nil, err
	}

	// GROUP/HAVING
	outRows, outCols, err := processGroupByHaving(cteEnv, term, filtered)
	if err != nil {
		return nil, err
	}

	// DISTINCT
	if term.Distinct {
		// If DISTINCT ON (...) was used, apply DISTINCT ON semantics: keep first
		// row per distinct-on key. The ORDER BY clause controls which row is
		// considered "first"; so apply ORDER BY first if present.
		if len(term.DistinctOn) > 0 {
			var err error
			outRows, err = applyDistinctOn(cteEnv, term, outRows)
			if err != nil {
				return nil, err
			}
		} else {
			outRows = distinctRows(outRows, outCols)
		}
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
			resultRows = applySortOrderWithLimit(s.OrderBy, resultRows, s.Limit, s.Offset)
		}
		resultRows = applyOffsetLimit(s, resultRows)
	} else {
		var err error
		resultRows, resultCols, err = processUnionClauses(cteEnv, s.Union, resultRows, resultCols, orderAliases)
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
			compoundOrderBy, err := resolveCompoundOrderBy(s.OrderBy, orderAliases)
			if err != nil {
				return nil, err
			}
			resultRows = applySortOrderWithLimit(compoundOrderBy, resultRows, s.Limit, s.Offset)
		}
		resultRows = applyOffsetLimit(s, resultRows)
	}
	return &ResultSet{Cols: resultCols, Rows: resultRows}, nil
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
	rows            [][]any
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
	mu       sync.Mutex
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
	byKey   map[any][][]any
}
