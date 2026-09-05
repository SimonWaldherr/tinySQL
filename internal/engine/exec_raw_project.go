// Projections and column addressing for the raw-row fast paths, and the test
// for whether an expression can be evaluated against a stored row at all
// (which is what makes a query eligible for those paths).
package engine

import (
	"sort"
	"strings"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func buildSimpleSelectProjections(items []SelectItem, colIndex map[string]int) ([]simpleProjection, []string, bool) {
	projs := make([]simpleProjection, 0, len(items))
	outputCols := make([]string, 0, len(items))
	for i, it := range items {
		proj, ok := buildSimpleSelectProjection(it, i, colIndex)
		if !ok {
			return nil, nil, false
		}
		projs = append(projs, proj)
		outputCols = append(outputCols, proj.name)
	}
	return projs, outputCols, true
}

func simpleProjectionMapCap(projs []simpleProjection) int {
	capHint := len(projs)
	for _, p := range projs {
		if p.altKey != "" {
			capHint++
		}
	}
	return capHint
}

// buildSimpleSelectStarProjections builds one direct-column-reference
// projection per table column, in schema order — the raw fast-path
// equivalent of "expand * to all columns". Each projection has colIdx >= 0,
// so projectRawRow copies straight from raw[] with no evalRawExpr call.
//
// Both the unqualified ("col") and qualified ("alias.col") Row map keys are
// populated via altKey, matching rowsFromTable's dual-key output — callers
// may look up either form (e.g. tsql.GetVal(row, "orders.id")), and several
// tests/public-API examples rely on the qualified form being present even
// for SELECT *.
//
// expr is also populated (as a plain VarRef) purely so ORDER BY on a star
// query can resolve a column name via findSimpleSelectOrderExpr, which
// returns .expr regardless of whether the fast colIdx path was used for
// projection itself.
func buildSimpleSelectStarProjections(table *storage.Table, alias string) ([]simpleProjection, []string) {
	projs := make([]simpleProjection, len(table.Cols))
	outputCols := make([]string, len(table.Cols))
	for i, c := range table.Cols {
		lower := strings.ToLower(c.Name)
		projs[i] = simpleProjection{
			name:   c.Name,
			key:    lower,
			altKey: strings.ToLower(alias) + "." + lower,
			side:   -1,
			colIdx: i,
			expr:   &VarRef{Name: c.Name, Lower: lower},
		}
		outputCols[i] = c.Name
	}
	return projs, outputCols
}

// buildSimpleJoinStarProjections expands SELECT * across a two-table inner
// join into one direct-column-reference projection per column, left table
// first, mirroring what buildSimpleSelectStarProjections does for a single
// table.
//
// Star previously disqualified the join fast path outright
// (buildSimpleJoinProjections rejects it), so `SELECT * FROM a JOIN b` — about
// the most ordinary join there is — fell back to the general path, which
// materializes BOTH inputs as dual-key Row maps via rowsFromTable and then
// allocates another merged map per output row.
//
// Two details of the general path's output are reproduced exactly here:
//
//   - Output columns are the unqualified names, de-duplicated on first
//     occurrence. Two tables that share a column name contribute it once, so
//     `a(id, av) JOIN b(id, bv)` yields [id av bv], not [id av id bv].
//   - Every column is reachable under both its unqualified and its qualified
//     name, and where the unqualified names collide the RIGHT table's value
//     wins. That falls out of projection order: projectJoinRawRow writes left
//     projections first and the right one overwrites the shared key, which is
//     precisely what mergeRows(l, r) does when the general path merges the two
//     row maps.
func buildSimpleJoinStarProjections(left, right *storage.Table, leftAlias, rightAlias string) ([]simpleProjection, []string) {
	total := len(left.Cols) + len(right.Cols)
	projs := make([]simpleProjection, 0, total)
	outputCols := make([]string, 0, total)
	seen := make(map[string]bool, total)

	appendSide := func(t *storage.Table, alias string, side int) {
		lowerAlias := strings.ToLower(alias)
		for i, c := range t.Cols {
			lower := strings.ToLower(c.Name)
			qualified := lowerAlias + "." + lower
			projs = append(projs, simpleProjection{
				name:   c.Name,
				key:    lower,
				altKey: qualified,
				side:   side,
				colIdx: i,
				// Qualified so the reference is unambiguous if it is ever
				// evaluated; on this path colIdx is always >= 0, so
				// projectJoinRawRow reads the column directly and never
				// consults expr.
				expr: &VarRef{Name: alias + "." + c.Name, Lower: qualified},
			})
			if !seen[lower] {
				seen[lower] = true
				outputCols = append(outputCols, c.Name)
			}
		}
	}
	appendSide(left, leftAlias, 0)
	appendSide(right, rightAlias, 1)
	return projs, outputCols
}

func buildSimpleSelectProjection(it SelectItem, idx int, colIndex map[string]int) (simpleProjection, bool) {
	if it.Star || !isSimpleRawExpr(it.Expr) {
		return simpleProjection{}, false
	}
	name := projName(it, idx)
	if name == "" {
		return simpleProjection{}, false
	}
	colIdx := -1
	if ref, ok := it.Expr.(*VarRef); ok {
		if idx, ok2 := colIndex[strings.ToLower(ref.Name)]; ok2 {
			colIdx = idx
		}
	}
	return simpleProjection{
		name:   name,
		key:    strings.ToLower(name),
		side:   -1,
		colIdx: colIdx,
		expr:   it.Expr,
	}, true
}

func buildSimpleSelectOrderExprs(orderBy []OrderItem, projs []simpleProjection, colIndex map[string]int) ([]Expr, bool) {
	orderExprs := make([]Expr, 0, len(orderBy))
	for _, oi := range orderBy {
		expr, ok := findSimpleSelectOrderExpr(oi.Col, projs, colIndex)
		if !ok {
			return nil, false
		}
		orderExprs = append(orderExprs, expr)
	}
	return orderExprs, true
}

// simpleSelectOrderCols resolves every ORDER BY expression to its raw column
// index. It returns nil unless all terms are direct column references — the
// overwhelmingly common shape — in which case the ordered fast path can read
// sort keys straight out of the raw rows instead of calling evalRawExpr (a
// map lookup per term per row) and, for multi-column orders, allocating a
// keys slice per row.
func simpleSelectOrderCols(orderExprs []Expr, colIndex map[string]int, colCount int) []int {
	if len(orderExprs) == 0 {
		return nil
	}
	cols := make([]int, len(orderExprs))
	for i, e := range orderExprs {
		ref, ok := e.(*VarRef)
		if !ok {
			return nil
		}
		key := ref.Lower
		if key == "" {
			key = strings.ToLower(ref.Name)
		}
		idx, found := colIndex[key]
		if !found || idx < 0 || idx >= colCount {
			return nil
		}
		cols[i] = idx
	}
	return cols
}

// findSimpleSelectOrderExpr resolves one ORDER BY term. Output names win, as SQL
// requires — an alias shadows a same-named source column — and a term that names
// no output column falls back to a source column of the scanned table.
//
// That fallback is what makes ORDER BY on a column the SELECT list does not
// project work here. Without it this plan declined the query, and the general
// path it fell back to looked the term up in the already-projected row, found
// nothing, compared every row equal and returned them in physical order — a
// silently unsorted result for a query as ordinary as
// "SELECT name FROM t ORDER BY created_at".
func findSimpleSelectOrderExpr(col string, projs []simpleProjection, colIndex map[string]int) (Expr, bool) {
	for _, p := range projs {
		if strings.EqualFold(p.name, col) {
			return p.expr, true
		}
	}
	if _, ok := colIndex[strings.ToLower(col)]; ok {
		return &VarRef{Name: col}, true
	}
	return nil, false
}

func simpleColumnIndex(t *storage.Table, alias string) map[string]int {
	idx := make(map[string]int, len(t.Cols)*3)
	tableName := strings.ToLower(t.Name)
	aliasName := strings.ToLower(alias)
	for i, c := range t.Cols {
		col := strings.ToLower(c.Name)
		idx[col] = i
		idx[tableName+"."+col] = i
		if aliasName != "" {
			idx[aliasName+"."+col] = i
		}
	}
	return idx
}

// rawRowTextColumns maps ROW_TO_TEXT's sorted unqualified Row-map keys back
// to physical raw-row indexes. simpleColumnIndex already gives the same
// resolution used by the raw fast path; sorting names here matches
// evalRowToTextFunc's observable output order.
// rowTextColumns returns the plan's ROW_TO_TEXT column order, building it on
// first use. See simpleSelectPlan.rowTextCols for why it is not eager.
//
// Building it here needs no lock because every plan this method can be reached
// through is private to one executing statement: the DML fast paths construct
// theirs per statement, and loadSimpleSelectPlanTemplate's cached template is
// only ever used through a `plan := *template` value copy. Plans that are
// genuinely shared — the ones captured by the filter closures in
// exec_raw_filter.go, which travel inside those cached templates — fill this
// field in eagerly at construction instead, and so never reach this code.
func (plan *simpleSelectPlan) rowTextColumns() []int {
	if plan.rowTextCols == nil {
		plan.rowTextCols = rawRowTextColumns(plan.colIndex)
	}
	return plan.rowTextCols
}

func rawRowTextColumns(colIndex map[string]int) []int {
	type column struct {
		name string
		idx  int
	}
	cols := make([]column, 0, len(colIndex))
	for name, idx := range colIndex {
		if !strings.Contains(name, ".") {
			cols = append(cols, column{name: name, idx: idx})
		}
	}
	sort.Slice(cols, func(i, j int) bool { return cols[i].name < cols[j].name })
	indexes := make([]int, len(cols))
	for i, col := range cols {
		indexes[i] = col.idx
	}
	return indexes
}

func simpleSelectInitialCap(plan *simpleSelectPlan) int {
	rows := simplePlanRows(plan)
	if plan.limit != nil {
		capHint := *plan.limit
		if plan.offset != nil {
			capHint += *plan.offset
		}
		if capHint > 0 && capHint < len(rows) {
			return capHint
		}
	}
	if plan.where == nil && len(rows) > 0 {
		return len(rows)
	}
	if len(rows) < 64 {
		return len(rows)
	}
	return 64
}

func simplePlanRows(plan *simpleSelectPlan) [][]any {
	if plan.rows != nil {
		return plan.rows
	}
	return plan.table.Rows
}

func isSimpleRawExpr(e Expr) bool {
	switch ex := e.(type) {
	case nil:
		return true
	case *Literal, *VarRef:
		return true
	case *Unary:
		return (ex.Op == "+" || ex.Op == "-" || ex.Op == "NOT") && isSimpleRawExpr(ex.Expr)
	case *Binary:
		if ex.Op == "AND" || ex.Op == "OR" || isComparisonOp(ex.Op) || isArithmeticOp(ex.Op) {
			return isSimpleRawExpr(ex.Left) && isSimpleRawExpr(ex.Right)
		}
		return false
	case *IsNull:
		return isSimpleRawExpr(ex.Expr)
	case *LikeExpr:
		// LIKE/ILIKE/GLOB with a literal pattern and no dynamic escape is safe in the fast path.
		return isSimpleRawExpr(ex.Expr) && isSimpleRawExpr(ex.Pattern) &&
			(ex.Escape == nil || isSimpleRawExpr(ex.Escape))
	case *RegexpExpr:
		// REGEXP/RLIKE/SIMILAR TO with literal pattern is safe in the fast path.
		return isSimpleRawExpr(ex.Expr) && isSimpleRawExpr(ex.Pattern)
	case *BetweenExpr:
		return isSimpleRawExpr(ex.Expr) && isSimpleRawExpr(ex.Lo) && isSimpleRawExpr(ex.Hi)
	case *InExpr:
		if !isSimpleRawExpr(ex.Expr) {
			return false
		}
		for _, v := range ex.Values {
			if !isSimpleRawExpr(v) {
				return false
			}
		}
		return true
	case *FuncCall:
		if ex.Over != nil {
			return false
		}
		if rowAwareFuncNames[ex.Name] && ex.Name != "ROW_TO_TEXT" {
			// Reads the ambient Row directly; the raw path pre-evaluates
			// args and substitutes an empty Row, which would silently
			// always return "". Must go through the general evaluator.
			return false
		}
		for _, arg := range ex.Args {
			if !isSimpleRawExpr(arg) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

// rowAwareFuncNames lists scalar functions that read the ambient Row map
// directly rather than only their evaluated arguments. ROW_TO_TEXT is the
// one exception: evalRawRowToText supplies it with precomputed raw indexes,
// so it can safely stay on the raw execution path.
var rowAwareFuncNames = map[string]bool{
	"ROW_TO_TEXT": true,
}

// exprHasRowAwareFuncCall reports whether e, or any sub-expression reachable
// through the node kinds evalRawExpr supports, calls a row-aware function.
func exprHasRowAwareFuncCall(e Expr) bool {
	switch ex := e.(type) {
	case nil, *VarRef, *Literal:
		return false
	case *Unary:
		return exprHasRowAwareFuncCall(ex.Expr)
	case *Binary:
		return exprHasRowAwareFuncCall(ex.Left) || exprHasRowAwareFuncCall(ex.Right)
	case *IsNull:
		return exprHasRowAwareFuncCall(ex.Expr)
	case *LikeExpr:
		return exprHasRowAwareFuncCall(ex.Expr) || exprHasRowAwareFuncCall(ex.Pattern) || exprHasRowAwareFuncCall(ex.Escape)
	case *RegexpExpr:
		return exprHasRowAwareFuncCall(ex.Expr) || exprHasRowAwareFuncCall(ex.Pattern)
	case *BetweenExpr:
		return exprHasRowAwareFuncCall(ex.Expr) || exprHasRowAwareFuncCall(ex.Lo) || exprHasRowAwareFuncCall(ex.Hi)
	case *InExpr:
		if exprHasRowAwareFuncCall(ex.Expr) {
			return true
		}
		for _, v := range ex.Values {
			if exprHasRowAwareFuncCall(v) {
				return true
			}
		}
		return false
	case *FuncCall:
		if rowAwareFuncNames[ex.Name] && ex.Name != "ROW_TO_TEXT" {
			return true
		}
		for _, arg := range ex.Args {
			if exprHasRowAwareFuncCall(arg) {
				return true
			}
		}
		return false
	default:
		return false
	}
}

func isSimpleRawPredicate(e Expr) bool {
	if e == nil {
		return true
	}
	return isSimpleRawExpr(e)
}

func isComparisonOp(op string) bool {
	switch op {
	case "=", "!=", "<>", "<", "<=", ">", ">=":
		return true
	default:
		return false
	}
}

func isArithmeticOp(op string) bool {
	switch op {
	case "+", "-", "*", "/", "%":
		return true
	default:
		return false
	}
}

func evalRawWhere(plan *simpleSelectPlan, raw []any) (bool, error) {
	if plan.filter != nil {
		return plan.filter(raw)
	}
	if plan.where == nil {
		return true, nil
	}
	v, err := evalRawExpr(plan, raw, plan.where)
	if err != nil {
		return false, err
	}
	return toTri(v) == tvTrue, nil
}

// buildRawFilter attempts to compile a WHERE expression into a closure that
// operates directly on raw row slices ([]any) without going through the
// general evalRawExpr machinery.  It handles the most common patterns:
//   - col op literal   (equality, inequality, ordering)
//   - boolean_col      (truthy column reference)
//   - NOT boolean_col
//   - AND / OR of the above
//
// Returns nil when the expression is too complex to compile, in which case
// evalRawExpr is used as the fallback.
func buildRawFilter(colIndex map[string]int, e Expr) func([]any) (bool, error) {
	// Column-independent, order-independent multi-term text search — e.g.
	// `WHERE ROW_TO_TEXT() LIKE '%urgent%' AND ROW_TO_TEXT() LIKE '%widget%'`
	// — is the documented idiom for "find rows containing all of these
	// words, regardless of which column or what order" (see
	// TestRowToTextEnablesWholeRowLike). Recognize that shape before the
	// generic compilers below so the whole-row text is built once per row
	// and reused across every term, instead of once per term.
	if f := buildRawRowToTextAndFilter(colIndex, e); f != nil {
		return f
	}
	if f := buildRawFilterSpecialized(colIndex, e); f != nil {
		return f
	}
	// General fallback: predicates the specialized builders don't compile —
	// e.g. function-call comparisons like
	// VEC_COSINE_SIMILARITY(embedding, ...) > 0.5 — can still run on raw
	// rows through evalRawExpr, provided the expression only uses node
	// kinds the raw evaluator supports and no row-aware functions.
	// Previously such WHERE clauses disqualified the entire plan and forced
	// the general Row-map evaluator, which allocates two map entries per
	// column per row; on scoring-heavy RAG scans that map traffic — not the
	// predicate itself — dominated the query cost.
	return buildRawExprFilter(colIndex, e)
}

// flattenAndConjuncts splits a (possibly deeply nested, e.g. left- or
// right-associative) chain of AND-combined Binary nodes into its leaf
// conjuncts, in left-to-right order. A non-AND node returns a single-element
// slice containing itself.
func flattenAndConjuncts(e Expr) []Expr {
	b, ok := e.(*Binary)
	if !ok || b.Op != "AND" {
		return []Expr{e}
	}
	return append(flattenAndConjuncts(b.Left), flattenAndConjuncts(b.Right)...)
}
