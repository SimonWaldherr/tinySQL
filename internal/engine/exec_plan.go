// Compiling a SELECT into a plan, and caching that plan across executions of
// the same statement text. Also index selection: which secondary or constraint
// index a predicate can seek, and whether seeking it beats scanning.
package engine

import (
	"errors"
	"math"
	"sort"
	"strings"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func buildSimpleSelectPlan(env ExecEnv, s *Select) (*simpleSelectPlan, bool, error) {
	if !simpleSelectEligible(s) {
		return nil, false, nil
	}

	// ModePagedIndex exposes table schema and index roots without loading row
	// pages. For a complete composite equality predicate, resolve the B+Tree
	// key first and materialize only the located rows. All other query shapes
	// deliberately fall through to the established full-table compatibility
	// path rather than pretending a scan is a page seek.
	if metadata, paged, metaErr := env.db.PagedIndexMetadata(env.tenant, s.From.Table); metaErr != nil {
		return nil, true, metaErr
	} else if paged {
		colIndex := simpleColumnIndex(metadata, aliasOr(s.From))
		if idx, values, predicates, residual := selectSecondaryIndex(metadata, colIndex, s.Where); idx != nil && len(values) == len(idx.Columns) {
			rows, exists, seekErr := env.db.PagedIndexRows(env.tenant, s.From.Table, idx.Name, values)
			if seekErr != nil {
				return nil, true, seekErr
			}
			if exists {
				// Compile/cache from immutable schema metadata. Candidate rows live
				// only on this plan copy, so a template never retains a decoded BLOB.
				template, ok, err := loadSimpleSelectPlanTemplate(metadata, s, true)
				if !ok || err != nil {
					return nil, ok, err
				}
				plan := *template
				plan.table = metadata
				resetSimplePlanAccess(&plan, len(rows))
				plan.rows = rows
				plan.scanType = "PAGED INDEX POINT SEEK"
				plan.indexName = idx.Name
				plan.indexPredicates = predicates
				plan.residualFilter = residual
				plan.coveringIndex = projectionsCoveredByIndex(plan.projs, idx, metadata)
				plan.estimatedRows = len(rows)
				return &plan, true, nil
			}
		}
	}

	table, err := env.db.Get(env.tenant, s.From.Table)
	if err != nil {
		schema, name := splitObjectName(s.From.Table)
		if mv, ok := env.db.Catalog().GetMaterializedView(schema, name); ok {
			table, err = ensureMaterializedViewCache(env, s.From.Table, mv)
			if err != nil {
				return nil, true, err
			}
		} else if isCatalogViewSource(env, s.From.Table) {
			return nil, false, nil
		} else {
			return nil, true, err
		}
	}
	template, ok, err := loadSimpleSelectPlanTemplate(table, s, false)
	if !ok || err != nil {
		return nil, ok, err
	}
	plan := *template
	resetSimplePlanAccess(&plan, len(table.Rows))
	idx, values, predicates, residual := selectSecondaryIndex(table, plan.colIndex, s.Where)
	rangePlan, haveRange := selectRangeIndex(table, plan.colIndex, s.Where)
	// A range plan is preferred only when it matches at least as many equality
	// columns as the equality-only plan would. Then it is the same key prefix plus
	// one bounded column, so it is strictly narrower. When the equality plan
	// matches more columns — its index covers predicates the range index does not —
	// it stays, because comparing across different indexes would need selectivity
	// estimates this planner does not have.
	if haveRange && idx != nil && len(rangePlan.prefix) < len(values) {
		haveRange = false
	}
	if haveRange {
		idx = nil
	}
	if idx != nil {
		var rowIDs []int
		var seekErr error
		if len(values) == len(idx.Columns) {
			rowIDs, seekErr = table.LookupSecondaryIndexPoint(idx, values)
		} else {
			rowIDs, seekErr = table.LookupSecondaryIndexPrefix(idx, values)
		}
		if seekErr != nil {
			return nil, true, seekErr
		}
		plan.rowIDs = rowIDs
		plan.scanType = "INDEX " + seekKind(len(values), len(idx.Columns))
		plan.indexName = idx.Name
		plan.indexPredicates = predicates
		plan.residualFilter = residual
		plan.coveringIndex = projectionsCoveredByIndex(plan.projs, idx, table)
		plan.estimatedRows = len(rowIDs)
	} else if haveRange {
		// Equality prefix plus one range column. The range does not constrain
		// trailing index columns, so the result is a superset and the residual
		// WHERE still runs — which is what lets a two-dimensional predicate such
		// as a bounding box narrow to one band and filter the other axis.
		rowIDs, seekErr := table.LookupSecondaryIndexRange(rangePlan.index, rangePlan.prefix, rangePlan.lo, rangePlan.hi)
		if seekErr != nil {
			if !errors.Is(seekErr, storage.ErrIndexRangeUnsupported) {
				return nil, true, seekErr
			}
			// The index cannot order this range after all; leave the plan as the
			// table scan resetSimplePlanAccess already set up.
		} else {
			plan.rowIDs = rowIDs
			plan.scanType = "INDEX RANGE SCAN"
			plan.indexName = rangePlan.index.Name
			plan.indexPredicates = rangePlan.predicates
			plan.residualFilter = true
			plan.coveringIndex = projectionsCoveredByIndex(plan.projs, rangePlan.index, table)
			plan.estimatedRows = len(rowIDs)
		}
	} else if rowIDs, column, residual, ok := selectConstraintIndex(table, plan.colIndex, s.Where); ok {
		// PRIMARY KEY and UNIQUE enforcement already maintains this hash index
		// incrementally for DML. Reusing it here avoids a full table scan for
		// the common key lookup shape without adding another persistent index.
		plan.rowIDs = rowIDs
		plan.scanType = "CONSTRAINT INDEX POINT SEEK"
		plan.indexName = column
		plan.indexPredicates = []string{column + " = ?"}
		plan.residualFilter = residual
		plan.filterFullyCovered = !residual
		plan.estimatedRows = len(rowIDs)
	}
	return &plan, true, nil
}

// resetSimplePlanAccess clears value-dependent state retained by a cached
// query shape. Prepared parameters and table mutations can change the access
// path on every execution.
func resetSimplePlanAccess(plan *simpleSelectPlan, rows int) {
	plan.rowIDs = nil
	plan.rows = nil
	plan.pagedSource = nil
	plan.scanType = "TABLE SCAN"
	plan.indexName = ""
	plan.indexPredicates = nil
	plan.residualFilter = false
	plan.filterFullyCovered = false
	plan.coveringIndex = false
	plan.estimatedRows = rows
}

func loadSimpleSelectPlanTemplate(table *storage.Table, s *Select, reusableSchema bool) (*simpleSelectPlan, bool, error) {
	cache := s.simplePlanCache
	cacheable := cache != nil && simplePlanCacheSafe(s.Where)
	if cacheable {
		cache.mu.Lock()
		defer cache.mu.Unlock()
		if cache.plan != nil && cache.colCount == len(table.Cols) && (cache.table == table || (reusableSchema && sameSimplePlanSchema(cache.table, table))) {
			return cache.plan, true, nil
		}
	}

	colIndex := simpleColumnIndex(table, aliasOr(s.From))

	var projs []simpleProjection
	var outputCols []string
	if len(s.Projs) == 1 && s.Projs[0].Star && s.Projs[0].Alias == "" {
		// SELECT * FROM t (single table, no join — guaranteed by
		// simpleSelectEligible): expand directly into one raw-column
		// projection per table column instead of falling back to the
		// general Row-map path. Star previously disqualified the fast path
		// entirely, making "SELECT *" tens of times slower than an
		// equivalent narrow SELECT on the same table.
		projs, outputCols = buildSimpleSelectStarProjections(table, aliasOr(s.From))
	} else {
		var projOk bool
		projs, outputCols, projOk = buildSimpleSelectProjections(s.Projs, colIndex)
		if !projOk {
			return nil, false, nil
		}
	}
	orderExprs, ok := buildSimpleSelectOrderExprs(s.OrderBy, projs, colIndex)
	if !ok {
		return nil, false, nil
	}
	filter := buildRawFilter(colIndex, s.Where)
	if filter == nil {
		return nil, false, nil
	}

	plan := &simpleSelectPlan{
		table:         table,
		colIndex:      colIndex,
		projs:         projs,
		orderBy:       s.OrderBy,
		orderExprs:    orderExprs,
		orderCols:     simpleSelectOrderCols(orderExprs, colIndex, len(table.Cols)),
		where:         s.Where,
		filter:        filter,
		limit:         s.Limit,
		offset:        s.Offset,
		outputCols:    outputCols,
		rowMapCap:     simpleProjectionMapCap(projs),
		scanType:      "TABLE SCAN",
		estimatedRows: len(table.Rows),
	}
	if cacheable {
		cache.table = table
		cache.colCount = len(table.Cols)
		cache.plan = plan
	}
	return plan, true, nil
}

// sameSimplePlanSchema is intentionally narrower than a general table
// equality check. It permits page-store queries to reuse a compiled plan
// across independent schema-only Table instances while rejecting anything
// that could change column resolution or raw-value interpretation.
func sameSimplePlanSchema(a, b *storage.Table) bool {
	if a == nil || b == nil || len(a.Cols) != len(b.Cols) {
		return false
	}
	for i := range a.Cols {
		left, right := a.Cols[i], b.Cols[i]
		if !strings.EqualFold(left.Name, right.Name) || left.Type != right.Type || left.DeclaredType != right.DeclaredType || left.Affinity != right.Affinity || left.Constraint != right.Constraint || left.NotNull != right.NotNull {
			return false
		}
	}
	return true
}

// simplePlanCacheSafe rejects bound forms whose compiled filter captures the
// current parameter value (LIKE/IN/regexp). Plain comparisons use a dynamic
// bound-literal filter and are safe to reuse.
func simplePlanCacheSafe(expr Expr) bool {
	switch ex := expr.(type) {
	case nil, *VarRef:
		return true
	case *Literal:
		return !ex.Parameter
	case *Unary:
		return simplePlanCacheSafe(ex.Expr)
	case *IsNull:
		return simplePlanCacheSafe(ex.Expr)
	case *Binary:
		if ex.Op == "AND" || ex.Op == "OR" || isComparisonOp(ex.Op) {
			return simplePlanCacheSafeComparisonSide(ex.Left) && simplePlanCacheSafeComparisonSide(ex.Right)
		}
		return !exprContainsBoundParameter(expr)
	default:
		return !exprContainsBoundParameter(expr)
	}
}

func simplePlanCacheSafeComparisonSide(expr Expr) bool {
	switch ex := expr.(type) {
	case *Literal:
		return true // dynamic comparison filters dereference Parameter literals
	case *Binary:
		return simplePlanCacheSafe(ex)
	default:
		return !exprContainsBoundParameter(expr)
	}
}

func exprContainsBoundParameter(expr Expr) bool {
	switch ex := expr.(type) {
	case nil:
		return false
	case *Literal:
		return ex.Parameter
	case *Unary:
		return exprContainsBoundParameter(ex.Expr)
	case *Binary:
		return exprContainsBoundParameter(ex.Left) || exprContainsBoundParameter(ex.Right)
	case *IsNull:
		return exprContainsBoundParameter(ex.Expr)
	case *LikeExpr:
		return exprContainsBoundParameter(ex.Expr) || exprContainsBoundParameter(ex.Pattern) || exprContainsBoundParameter(ex.Escape)
	case *RegexpExpr:
		return exprContainsBoundParameter(ex.Expr) || exprContainsBoundParameter(ex.Pattern)
	case *InExpr:
		if exprContainsBoundParameter(ex.Expr) {
			return true
		}
		for _, value := range ex.Values {
			if exprContainsBoundParameter(value) {
				return true
			}
		}
	case *FuncCall:
		for _, arg := range ex.Args {
			if exprContainsBoundParameter(arg) {
				return true
			}
		}
	}
	return false
}

func seekKind(prefixCols, allCols int) string {
	if prefixCols == allCols {
		return "POINT SEEK"
	}
	return "PREFIX SEEK"
}

// selectSecondaryIndex extracts equality terms from a simple WHERE tree and
// chooses the cheapest matching index prefix. Fresh ANALYZE statistics estimate
// cardinality from column distinct counts; before ANALYZE (or after DML marked
// statistics stale) it deterministically falls back to the longest prefix.
// Other predicates stay as residual filters and are still evaluated by the
// normal raw evaluator.
func selectSecondaryIndex(table *storage.Table, colIndex map[string]int, where Expr) (*storage.SecondaryIndex, []any, []string, bool) {
	if where == nil || len(table.Indexes) == 0 {
		return nil, nil, nil, false
	}
	equalities := make(map[int]any)
	totalTerms := collectEqualityTerms(where, colIndex, equalities)
	var chosen *storage.SecondaryIndex
	var values []any
	var predicates []string
	bestEstimate := 0.0
	indexNames := make([]string, 0, len(table.Indexes))
	for name := range table.Indexes {
		indexNames = append(indexNames, name)
	}
	sort.Strings(indexNames)
	for _, indexName := range indexNames {
		idx := table.Indexes[indexName]
		candidate := make([]any, 0, len(idx.Columns))
		candidatePredicates := make([]string, 0, len(idx.Columns))
		for _, column := range idx.Columns {
			pos, err := table.ColIndex(column)
			if err != nil {
				break
			}
			value, ok := equalities[pos]
			if !ok {
				break
			}
			// SQL's current int/float comparison semantics intentionally allow
			// values such as 1 and 1.0 to compare equal, while the durable
			// secondary-index encoding keeps their types distinct. A numeric seek
			// is therefore valid only when no differently encoded row can match
			// this literal. Stop before an unsafe numeric component and retain
			// it as a residual filter; a preceding text/bool prefix remains safe.
			if isNumericSQLValue(value) && !numericSecondaryIndexSeekSafe(table, pos, value) {
				break
			}
			candidate = append(candidate, value)
			candidatePredicates = append(candidatePredicates, column+" = ?")
		}
		if len(candidate) == 0 {
			continue
		}
		estimate := estimateSecondaryIndexRows(table, idx, len(candidate))
		if chosen != nil && !preferSecondaryIndex(idx, len(candidate), estimate, chosen, len(values), bestEstimate) {
			continue
		}
		chosen, values, predicates, bestEstimate = idx, candidate, candidatePredicates, estimate
	}
	if chosen == nil {
		return nil, nil, nil, false
	}
	// An index expected to return the full table costs more than a sequential
	// scan because it still has to visit rows through RowIDs.
	if table.Stats != nil && !table.Stats.Stale && len(table.Rows) > 0 && bestEstimate >= float64(len(table.Rows)) {
		return nil, nil, nil, false
	}
	return chosen, values, predicates, totalTerms != len(values)
}

// selectConstraintIndex finds a single-column PRIMARY KEY or UNIQUE equality
// predicate that can reuse the in-memory constraint index. Constraint indexes
// are hash maps, so unlike materialized secondary indexes they only support
// complete point lookups. The normal raw filter still runs over the located
// rows, preserving residual predicates and SQL's three-valued logic.
func selectConstraintIndex(table *storage.Table, colIndex map[string]int, where Expr) ([]int, string, bool, bool) {
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
		rows := lookupConstraintIndexRows(getConstraintIndex(table, colIdx), value)
		// A non-nil empty slice distinguishes an indexed negative lookup from
		// the nil RowID set used for a table scan.
		if rows == nil {
			rows = []int{}
		}
		return rows, column.Name, totalTerms != 1, true
	}
	return nil, "", false, false
}

// lookupConstraintIndexRows returns every bucket that rawEqual could consider
// equal to value. The constraint hash map keeps int, int64, and float64 as
// separate Go keys, while SQL equality deliberately treats their numerically
// equal values as identical. Merging at most three buckets preserves that SQL
// behavior without falling back to a full scan.
func lookupConstraintIndexRows(index *constraintIndexEntry, value any) []int {
	if index == nil {
		return nil
	}
	buckets := make([][]int, 0, 3)
	add := func(v any) {
		if rows := index.rows[comparableKeyPart(v)]; len(rows) > 0 {
			buckets = append(buckets, rows)
		}
	}
	add(value)
	switch v := value.(type) {
	case int:
		add(int64(v))
		add(float64(v))
	case int64:
		if int64(int(v)) == v {
			add(int(v))
		}
		add(float64(v))
	case float64:
		if !math.IsNaN(v) {
			if float64(int(v)) == v {
				add(int(v))
			}
			if float64(int64(v)) == v {
				add(int64(v))
			}
		}
	}
	if len(buckets) == 0 {
		return nil
	}
	if len(buckets) == 1 {
		return buckets[0]
	}
	rowCount := 0
	for _, bucket := range buckets {
		rowCount += len(bucket)
	}
	rows := make([]int, 0, rowCount)
	for _, bucket := range buckets {
		rows = append(rows, bucket...)
	}
	sort.Ints(rows) // preserve the observable table-scan order
	return rows
}

func isNumericSQLValue(value any) bool {
	switch value.(type) {
	case int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64:
		return true
	default:
		return false
	}
}

func numericSecondaryIndexSeekSafe(table *storage.Table, colPos int, value any) bool {
	if table == nil || colPos < 0 || colPos >= len(table.Cols) {
		return false
	}
	// ModePagedIndex supplies schema-only metadata. SQL comparison treats an
	// int and a float such as 1 and 1.0 as equal, while durable index keys keep
	// their storage representations distinct. Permit only literal forms whose
	// declared write coercion gives one canonical representation; all other
	// forms fall through to the compatible loaded-table path.
	if len(table.Rows) == 0 {
		return numericPagedIndexSeekSafe(table.Cols[colPos], value)
	}

	// An integer literal against a column holding no float64 cannot be shadowed
	// by a differently encoded row: anything comparing equal must itself be an
	// integer, and int and int64 encode identically. That verdict comes from a
	// per-(table, version) column summary, so repeated lookups do not re-scan
	// the table — see index_seek_safety.go for why this path exists.
	if isIntegerSQLValue(value) && !numericColumnHasFloat(table, colPos) {
		return true
	}

	// Everything else keeps the exact per-value check. A float literal needs it:
	// -0.0 and 0.0 compare equal but encode differently, so no column-level
	// summary can prove a float seek sound.
	for _, row := range table.Rows {
		if colPos >= len(row) || !isNumericSQLValue(row[colPos]) {
			continue
		}
		cmp, err := compare(row[colPos], value)
		if err != nil || cmp != 0 {
			continue
		}
		if !storage.CanonicalIndexValueEqual(row[colPos], value) {
			return false
		}
	}
	return true
}

func numericPagedIndexSeekSafe(col storage.Column, value any) bool {
	switch col.Affinity {
	case storage.AffinityInteger:
		return numericIntegerStorageSeekSafe(value)
	case storage.AffinityReal:
		return numericRealStorageSeekSafe(value)
	}
	switch col.Type {
	case storage.IntType:
		return numericIntegerStorageSeekSafe(value)
	case storage.FloatType:
		return numericRealStorageSeekSafe(value)
	default:
		// Types such as FLOAT64 intentionally retain their caller-provided
		// representation today, so metadata cannot safely choose one key.
		return false
	}
}

// numericIntegerStorageSeekSafe accepts forms that cannot be numerically
// equal to a differently encoded integer key. Integral floats are excluded:
// 1.0 compares equal to an integer 1 but has a distinct durable key.
func numericIntegerStorageSeekSafe(value any) bool {
	switch v := value.(type) {
	case int, int64:
		return true
	case float64:
		return !math.IsNaN(v) && math.Trunc(v) != v
	default:
		return false
	}
}

// numericRealStorageSeekSafe accepts normal finite, non-zero float literals.
// Integer literals and signed zero can compare equal to a distinct integer or
// opposite-sign floating representation, respectively.
func numericRealStorageSeekSafe(value any) bool {
	v, ok := value.(float64)
	return ok && !math.IsNaN(v) && v != 0
}

func preferSecondaryIndex(candidate *storage.SecondaryIndex, candidatePrefix int, candidateEstimate float64, current *storage.SecondaryIndex, currentPrefix int, currentEstimate float64) bool {
	if candidateEstimate != currentEstimate {
		return candidateEstimate < currentEstimate
	}
	if candidatePrefix != currentPrefix {
		return candidatePrefix > currentPrefix
	}
	if candidate.Unique != current.Unique {
		return candidate.Unique
	}
	return strings.ToLower(candidate.Name) < strings.ToLower(current.Name)
}

func estimateSecondaryIndexRows(table *storage.Table, index *storage.SecondaryIndex, prefixLen int) float64 {
	if stats := table.Stats; stats != nil && !stats.Stale && stats.RowCount > 0 {
		estimate := float64(stats.RowCount)
		for _, column := range index.Columns[:prefixLen] {
			columnStats, ok := stats.Columns[strings.ToLower(column)]
			if !ok || columnStats.DistinctCount == 0 {
				// A conservative default is preferable to claiming a selective
				// lookup when a plugin type has no meaningful cardinality data.
				estimate *= 0.1
				continue
			}
			estimate /= float64(columnStats.DistinctCount)
		}
		if estimate < 1 {
			return 1
		}
		return estimate
	}
	// Without fresh stats, preserve the historical behavior (longest prefix)
	// but keep it deterministic by assigning each same-prefix index the same
	// estimate. The name tie-breaker above then makes EXPLAIN reproducible.
	return float64(len(table.Rows) + len(index.Columns) - prefixLen)
}

func collectEqualityTerms(expr Expr, colIndex map[string]int, out map[int]any) int {
	b, ok := expr.(*Binary)
	if !ok {
		return 0
	}
	if b.Op == "AND" {
		return collectEqualityTerms(b.Left, colIndex, out) + collectEqualityTerms(b.Right, colIndex, out)
	}
	if b.Op != "=" {
		return 0
	}
	if ref, ok := b.Left.(*VarRef); ok {
		if lit, ok := b.Right.(*Literal); ok {
			if pos, found := colIndex[ref.Lower]; found {
				out[pos] = lit.Val
				return 1
			}
		}
	}
	if ref, ok := b.Right.(*VarRef); ok {
		if lit, ok := b.Left.(*Literal); ok {
			if pos, found := colIndex[ref.Lower]; found {
				out[pos] = lit.Val
				return 1
			}
		}
	}
	return 0
}

func projectionsCoveredByIndex(projs []simpleProjection, idx *storage.SecondaryIndex, table *storage.Table) bool {
	covered := make(map[int]struct{}, len(idx.Columns))
	for _, column := range idx.Columns {
		if pos, err := table.ColIndex(column); err == nil {
			covered[pos] = struct{}{}
		}
	}
	for _, proj := range projs {
		if proj.colIdx < 0 {
			return false
		}
		if _, ok := covered[proj.colIdx]; !ok {
			return false
		}
	}
	return true
}

func isCatalogViewSource(env ExecEnv, name string) bool {
	if name == "" {
		return false
	}
	schema, objectName := splitObjectName(name)
	if _, ok := env.db.Catalog().GetView(schema, objectName); ok {
		return true
	}
	if _, ok := env.db.Catalog().GetMaterializedView(schema, objectName); ok {
		return true
	}
	return false
}

func simpleSelectEligible(s *Select) bool {
	// Plain DISTINCT is handled by executeSimpleSelectDistinctFastPath, which
	// dedupes on projected values before materializing a Row map. DISTINCT ON
	// keeps its general-path implementation: its "first row per key" result
	// depends on ORDER BY, which the raw distinct path deliberately declines.
	if len(s.DistinctOn) > 0 || len(s.CTEs) > 0 || len(s.Joins) > 0 ||
		len(s.GroupBy) > 0 || s.Having != nil || s.Union != nil || s.Pivot != nil ||
		s.From.Table == "" || s.From.Subquery != nil || s.From.TableFunc != nil {
		return false
	}
	if isCatalogOrSysTableRef(s.From.Table) {
		return false
	}
	if isSQLiteSchemaTable(s.From.Table) {
		return false
	}
	return !anyAggInSelect(s.Projs) && !anyWindowInSelect(s.Projs)
}
