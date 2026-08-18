// Fast path for a two-table equi-join: compile the join keys, filters and
// projection once, then evaluate them against stored rows without building a
// row map per input row.
package engine

import (
	"fmt"
	"strings"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func executeSimpleJoinFastPath(env ExecEnv, s *Select) (*ResultSet, bool, error) {
	plan, ok, err := buildSimpleJoinPlan(env, s)
	if !ok || err != nil {
		return nil, ok, err
	}

	rightByKey, err := plan.rightRowsByKey()
	if err != nil {
		return nil, true, err
	}

	outRows := make([]Row, 0, min(len(plan.left.Rows), len(plan.right.Rows)))
	for i, left := range plan.left.Rows {
		// Check context cancellation every 64 rows to reduce channel-select overhead.
		if i&63 == 0 {
			if err := checkCtx(env.ctx); err != nil {
				return nil, true, err
			}
		}
		if plan.leftFilter != nil {
			match, err := plan.leftFilter(left)
			if err != nil {
				return nil, true, err
			}
			if !match {
				continue
			}
		}
		leftKey := left[plan.leftKey]
		if leftKey == nil {
			continue
		}
		matches := rightByKey[comparableKeyPart(leftKey)]
		for _, right := range matches {
			match, err := evalJoinRawWhere(plan, left, right)
			if err != nil {
				return nil, true, err
			}
			if !match {
				continue
			}
			out, err := projectJoinRawRow(plan, left, right)
			if err != nil {
				return nil, true, err
			}
			outRows = append(outRows, out)
		}
	}
	return &ResultSet{Cols: plan.outputCols, Rows: outRows}, true, nil
}

// rightRowsByKey returns a version-validated index of the right join input.
// Queries hold the database content read lock for their full execution, so a
// published index remains immutable while callers use it. Writers obtain the
// exclusive content lock and increment Table.Version, causing the next query
// to rebuild the index from the new rows.
func (p *simpleJoinPlan) rightRowsByKey() (map[any][][]any, error) {
	cache := &p.rightLookup
	cache.mu.RLock()
	if cache.table == p.right && cache.version == p.right.Version {
		byKey := cache.byKey
		cache.mu.RUnlock()
		return byKey, nil
	}
	cache.mu.RUnlock()

	byKey := make(map[any][][]any, len(p.right.Rows))
	for _, right := range p.right.Rows {
		if p.rightFilter != nil {
			match, err := p.rightFilter(right)
			if err != nil {
				return nil, err
			}
			if !match {
				continue
			}
		}
		keyVal := right[p.rightKey]
		// SQL equality never matches NULL. Besides preserving that semantic, not
		// indexing NULL avoids a false match when both join inputs have NULL in
		// their key column.
		if keyVal == nil {
			continue
		}
		key := comparableKeyPart(keyVal)
		byKey[key] = append(byKey[key], right)
	}

	cache.mu.Lock()
	defer cache.mu.Unlock()
	if cache.table == p.right && cache.version == p.right.Version {
		return cache.byKey, nil
	}
	cache.table = p.right
	cache.version = p.right.Version
	cache.byKey = byKey
	return byKey, nil
}

func buildSimpleJoinPlan(env ExecEnv, s *Select) (*simpleJoinPlan, bool, error) {
	if !simpleJoinSelectEligible(s) {
		return nil, false, nil
	}
	if isCatalogViewSource(env, s.From.Table) || isCatalogViewSource(env, s.Joins[0].Right.Table) {
		return nil, false, nil
	}
	if anyAggInSelect(s.Projs) || anyWindowInSelect(s.Projs) || !isSimpleRawPredicate(s.Where) {
		return nil, false, nil
	}

	left, right, err := loadSimpleJoinTables(env, s)
	if err != nil {
		return nil, true, err
	}
	cache := s.simpleJoinPlanCache
	cacheable := cache != nil && !exprContainsBoundParameter(s.Where)
	if cacheable {
		cache.mu.Lock()
		defer cache.mu.Unlock()
		if cache.plan != nil && cache.left == left && cache.right == right {
			return cache.plan, true, nil
		}
	}

	leftIndex := simpleColumnIndex(left, aliasOr(s.From))
	rightIndex := simpleColumnIndex(right, aliasOr(s.Joins[0].Right))
	leftKey, rightKey, ok := simpleJoinKeys(s.Joins[0].On, leftIndex, rightIndex)
	if !ok {
		return nil, false, nil
	}

	projs, outputCols, ok := buildSimpleJoinProjections(s.Projs, leftIndex, rightIndex)
	if !ok {
		return nil, false, nil
	}
	if !simpleJoinExprResolvable(s.Where, leftIndex, rightIndex) {
		return nil, false, nil
	}

	plan := &simpleJoinPlan{
		left:       left,
		right:      right,
		leftIndex:  leftIndex,
		rightIndex: rightIndex,
		leftKey:    leftKey,
		rightKey:   rightKey,
		where:      s.Where,
		projs:      projs,
		outputCols: outputCols,
	}
	plan.leftFilter, plan.rightFilter, plan.where = buildSimpleJoinFilters(s.Where, leftIndex, rightIndex)
	if cacheable {
		cache.left = left
		cache.right = right
		cache.plan = plan
	}
	return plan, true, nil
}

// buildSimpleJoinFilters pushes safe, single-table AND terms below an inner
// hash join. Only specialized raw filters are pushed: they have the same
// "true keeps, false/unknown rejects" behavior as WHERE while avoiding
// user-defined expression errors on rows that would never join. All other
// terms remain in plan.where and are evaluated after a key match.
func buildSimpleJoinFilters(where Expr, leftIndex, rightIndex map[string]int) (func([]any) (bool, error), func([]any) (bool, error), Expr) {
	var leftTerms, rightTerms, residualTerms []Expr
	collectSimpleJoinFilterTerms(where, leftIndex, rightIndex, &leftTerms, &rightTerms, &residualTerms)
	leftFilter, leftResidual := buildSimpleJoinSideFilter(leftIndex, leftTerms)
	rightFilter, rightResidual := buildSimpleJoinSideFilter(rightIndex, rightTerms)
	residualTerms = append(residualTerms, leftResidual...)
	residualTerms = append(residualTerms, rightResidual...)
	return leftFilter, rightFilter, joinAndTerms(residualTerms)
}

// collectSimpleJoinFilterTerms only splits AND expressions. OR and all other
// expression shapes stay intact; a whole expression is pushed only when it
// references exactly one input side.
func collectSimpleJoinFilterTerms(e Expr, leftIndex, rightIndex map[string]int, leftTerms, rightTerms, residualTerms *[]Expr) {
	if e == nil {
		return
	}
	if binary, ok := e.(*Binary); ok && binary.Op == "AND" {
		collectSimpleJoinFilterTerms(binary.Left, leftIndex, rightIndex, leftTerms, rightTerms, residualTerms)
		collectSimpleJoinFilterTerms(binary.Right, leftIndex, rightIndex, leftTerms, rightTerms, residualTerms)
		return
	}
	switch simpleJoinExprSide(e, leftIndex, rightIndex) {
	case 1:
		*leftTerms = append(*leftTerms, e)
	case 2:
		*rightTerms = append(*rightTerms, e)
	default:
		*residualTerms = append(*residualTerms, e)
	}
}

// simpleJoinExprSide returns a bit set: 1=left, 2=right. Literal-only
// expressions return zero and remain residual so their existing evaluation
// timing and errors are unchanged.
func simpleJoinExprSide(e Expr, leftIndex, rightIndex map[string]int) uint8 {
	switch ex := e.(type) {
	case nil, *Literal:
		return 0
	case *VarRef:
		name := ex.Lower
		if name == "" {
			name = strings.ToLower(ex.Name)
		}
		_, left := leftIndex[name]
		_, right := rightIndex[name]
		if left && !right {
			return 1
		}
		if right && !left {
			return 2
		}
		return 3
	case *Unary:
		return simpleJoinExprSide(ex.Expr, leftIndex, rightIndex)
	case *IsNull:
		return simpleJoinExprSide(ex.Expr, leftIndex, rightIndex)
	case *Binary:
		return simpleJoinExprSide(ex.Left, leftIndex, rightIndex) | simpleJoinExprSide(ex.Right, leftIndex, rightIndex)
	case *LikeExpr:
		return simpleJoinExprSide(ex.Expr, leftIndex, rightIndex) |
			simpleJoinExprSide(ex.Pattern, leftIndex, rightIndex) |
			simpleJoinExprSide(ex.Escape, leftIndex, rightIndex)
	case *RegexpExpr:
		return simpleJoinExprSide(ex.Expr, leftIndex, rightIndex) | simpleJoinExprSide(ex.Pattern, leftIndex, rightIndex)
	case *BetweenExpr:
		return simpleJoinExprSide(ex.Expr, leftIndex, rightIndex) |
			simpleJoinExprSide(ex.Lo, leftIndex, rightIndex) |
			simpleJoinExprSide(ex.Hi, leftIndex, rightIndex)
	case *InExpr:
		side := simpleJoinExprSide(ex.Expr, leftIndex, rightIndex)
		for _, value := range ex.Values {
			side |= simpleJoinExprSide(value, leftIndex, rightIndex)
		}
		return side
	default:
		return 3
	}
}

func buildSimpleJoinSideFilter(colIndex map[string]int, terms []Expr) (func([]any) (bool, error), []Expr) {
	filters := make([]func([]any) (bool, error), 0, len(terms))
	residual := make([]Expr, 0, len(terms))
	for _, term := range terms {
		if !simpleJoinPushdownSafe(term) {
			residual = append(residual, term)
			continue
		}
		filter := buildRawFilterSpecialized(colIndex, term)
		if filter == nil {
			residual = append(residual, term)
			continue
		}
		filters = append(filters, filter)
	}
	if len(filters) == 0 {
		return nil, residual
	}
	return func(raw []any) (bool, error) {
		for _, filter := range filters {
			match, err := filter(raw)
			if err != nil || !match {
				return false, err
			}
		}
		return true, nil
	}, residual
}

// simpleJoinPushdownSafe accepts only specialized filters that cannot surface
// a per-row comparison/type error before the join finds a matching pair.
// This keeps error timing and behavior of unusual expressions on the general
// residual path intact.
func simpleJoinPushdownSafe(e Expr) bool {
	switch ex := e.(type) {
	case *VarRef:
		return true
	case *IsNull:
		_, ok := ex.Expr.(*VarRef)
		return ok
	case *Binary:
		switch ex.Op {
		case "AND", "OR":
			return simpleJoinPushdownSafe(ex.Left) && simpleJoinPushdownSafe(ex.Right)
		case "=", "!=", "<>":
			return simpleJoinColumnLiteralComparison(ex.Left, ex.Right) || simpleJoinColumnLiteralComparison(ex.Right, ex.Left)
		case "<", "<=", ">", ">=":
			return simpleJoinOrderComparison(ex.Left, ex.Right) || simpleJoinOrderComparison(ex.Right, ex.Left)
		}
	case *LikeExpr:
		_, ref := ex.Expr.(*VarRef)
		_, pattern := ex.Pattern.(*Literal)
		return ref && pattern && ex.Escape == nil
	case *RegexpExpr:
		_, ref := ex.Expr.(*VarRef)
		_, pattern := ex.Pattern.(*Literal)
		return ref && pattern
	case *InExpr:
		if _, ok := ex.Expr.(*VarRef); !ok {
			return false
		}
		for _, value := range ex.Values {
			if _, ok := value.(*Literal); !ok {
				return false
			}
		}
		return true
	}
	return false
}

func simpleJoinColumnLiteralComparison(left, right Expr) bool {
	_, ref := left.(*VarRef)
	_, literal := right.(*Literal)
	return ref && literal
}

func simpleJoinOrderComparison(left, right Expr) bool {
	_, ref := left.(*VarRef)
	literal, ok := right.(*Literal)
	if !ref || !ok {
		return false
	}
	switch literal.Val.(type) {
	case int, int64, float64, string:
		return true
	default:
		return false
	}
}

func joinAndTerms(terms []Expr) Expr {
	var out Expr
	for _, term := range terms {
		if out == nil {
			out = term
			continue
		}
		out = &Binary{Op: "AND", Left: out, Right: term}
	}
	return out
}

func simpleJoinSelectEligible(s *Select) bool {
	if !(!s.Distinct && len(s.DistinctOn) <= 0 && len(s.CTEs) <= 0 && len(s.GroupBy) <= 0 &&
		s.Having == nil && s.Union == nil && len(s.OrderBy) <= 0 && s.Limit == nil && s.Offset == nil &&
		s.From.Table != "" && s.From.Subquery == nil && s.From.TableFunc == nil && len(s.Joins) == 1 &&
		s.Joins[0].Type == JoinInner && s.Joins[0].Right.Table != "" && s.Pivot == nil &&
		s.Joins[0].Right.Subquery == nil && s.Joins[0].Right.TableFunc == nil && !isSQLiteSchemaTable(s.From.Table) && !isSQLiteSchemaTable(s.Joins[0].Right.Table)) {
		return false
	}
	return !isCatalogOrSysTableRef(s.From.Table) && !isCatalogOrSysTableRef(s.Joins[0].Right.Table)
}

// isCatalogOrSysTableRef reports whether name refers to a virtual catalog.*
// or sys.* table, which env.db.Get cannot resolve as a physical table.
func isCatalogOrSysTableRef(name string) bool {
	lower := strings.ToLower(name)
	return strings.HasPrefix(lower, "catalog.") || strings.HasPrefix(lower, "sys.")
}

func loadSimpleJoinTables(env ExecEnv, s *Select) (*storage.Table, *storage.Table, error) {
	left, err := env.db.Get(env.tenant, s.From.Table)
	if err != nil {
		return nil, nil, err
	}
	right, err := env.db.Get(env.tenant, s.Joins[0].Right.Table)
	if err != nil {
		return nil, nil, err
	}
	return left, right, nil
}

func buildSimpleJoinProjections(items []SelectItem, leftIndex, rightIndex map[string]int) ([]simpleProjection, []string, bool) {
	projs := make([]simpleProjection, 0, len(items))
	outputCols := make([]string, 0, len(items))
	for i, it := range items {
		if it.Star || !isSimpleRawExpr(it.Expr) || !simpleJoinExprResolvable(it.Expr, leftIndex, rightIndex) {
			return nil, nil, false
		}
		name := projName(it, i)
		side, colIdx := resolveSimpleJoinProjectionRef(it.Expr, leftIndex, rightIndex)
		projs = append(projs, simpleProjection{
			name:   name,
			key:    strings.ToLower(name),
			side:   side,
			colIdx: colIdx,
			expr:   it.Expr,
		})
		outputCols = append(outputCols, name)
	}
	return projs, outputCols, true
}

func resolveSimpleJoinProjectionRef(e Expr, leftIndex, rightIndex map[string]int) (int, int) {
	ref, ok := e.(*VarRef)
	if !ok {
		return -1, -1
	}
	refName := strings.ToLower(ref.Name)
	if li, lok := leftIndex[refName]; lok {
		if _, ambig := rightIndex[refName]; !ambig {
			return 0, li
		}
	}
	if ri, rok := rightIndex[refName]; rok {
		return 1, ri
	}
	return -1, -1
}

func simpleJoinKeys(on Expr, leftIndex, rightIndex map[string]int) (int, int, bool) {
	bin, ok := on.(*Binary)
	if !ok || bin.Op != "=" {
		return 0, 0, false
	}
	leftRef, leftOK := bin.Left.(*VarRef)
	rightRef, rightOK := bin.Right.(*VarRef)
	if !leftOK || !rightOK {
		return 0, 0, false
	}
	leftKey := leftRef.Lower
	if leftKey == "" {
		leftKey = strings.ToLower(leftRef.Name)
	}
	rightKey := rightRef.Lower
	if rightKey == "" {
		rightKey = strings.ToLower(rightRef.Name)
	}
	if li, lok := leftIndex[leftKey]; lok {
		if ri, rok := rightIndex[rightKey]; rok {
			return li, ri, true
		}
	}
	if li, lok := leftIndex[rightKey]; lok {
		if ri, rok := rightIndex[leftKey]; rok {
			return li, ri, true
		}
	}
	return 0, 0, false
}

func simpleJoinExprResolvable(e Expr, leftIndex, rightIndex map[string]int) bool {
	switch ex := e.(type) {
	case nil, *Literal:
		return true
	case *VarRef:
		key := ex.Lower
		if key == "" {
			key = strings.ToLower(ex.Name)
		}
		_, lok := leftIndex[key]
		_, rok := rightIndex[key]
		return lok != rok
	case *IsNull:
		return simpleJoinExprResolvable(ex.Expr, leftIndex, rightIndex)
	case *Unary:
		return simpleJoinExprResolvable(ex.Expr, leftIndex, rightIndex)
	case *Binary:
		return simpleJoinExprResolvable(ex.Left, leftIndex, rightIndex) &&
			simpleJoinExprResolvable(ex.Right, leftIndex, rightIndex)
	case *LikeExpr:
		return simpleJoinExprResolvable(ex.Expr, leftIndex, rightIndex) &&
			simpleJoinExprResolvable(ex.Pattern, leftIndex, rightIndex) &&
			(ex.Escape == nil || simpleJoinExprResolvable(ex.Escape, leftIndex, rightIndex))
	case *RegexpExpr:
		return simpleJoinExprResolvable(ex.Expr, leftIndex, rightIndex) &&
			simpleJoinExprResolvable(ex.Pattern, leftIndex, rightIndex)
	case *BetweenExpr:
		return simpleJoinExprResolvable(ex.Expr, leftIndex, rightIndex) &&
			simpleJoinExprResolvable(ex.Lo, leftIndex, rightIndex) &&
			simpleJoinExprResolvable(ex.Hi, leftIndex, rightIndex)
	case *InExpr:
		if !simpleJoinExprResolvable(ex.Expr, leftIndex, rightIndex) {
			return false
		}
		for _, v := range ex.Values {
			if !simpleJoinExprResolvable(v, leftIndex, rightIndex) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func evalJoinRawWhere(plan *simpleJoinPlan, left, right []any) (bool, error) {
	if plan.where == nil {
		return true, nil
	}
	v, err := evalJoinRawExpr(plan, left, right, plan.where)
	if err != nil {
		return false, err
	}
	return toTri(v) == tvTrue, nil
}

func projectJoinRawRow(plan *simpleJoinPlan, left, right []any) (Row, error) {
	out := make(Row, len(plan.projs))
	for _, p := range plan.projs {
		if p.colIdx >= 0 {
			// Direct column reference: read from pre-resolved side and index.
			switch p.side {
			case 0:
				out[p.key] = left[p.colIdx]
			case 1:
				out[p.key] = right[p.colIdx]
			default:
				v, err := evalJoinRawExpr(plan, left, right, p.expr)
				if err != nil {
					return nil, err
				}
				out[p.key] = v
			}
		} else {
			v, err := evalJoinRawExpr(plan, left, right, p.expr)
			if err != nil {
				return nil, err
			}
			out[p.key] = v
		}
	}
	return out, nil
}

func evalJoinRawExpr(plan *simpleJoinPlan, left, right []any, e Expr) (any, error) {
	switch ex := e.(type) {
	case *Literal:
		return ex.Val, nil
	case *VarRef:
		return evalJoinRawVarRef(plan, left, right, ex)
	case *IsNull:
		return evalJoinRawIsNull(plan, left, right, ex)
	case *Unary:
		return evalJoinRawUnary(plan, left, right, ex)
	case *Binary:
		return evalJoinRawBinary(plan, left, right, ex)
	case *LikeExpr:
		return evalJoinRawLike(plan, left, right, ex)
	case *RegexpExpr:
		return evalJoinRawRegexp(plan, left, right, ex)
	case *BetweenExpr:
		return evalJoinRawBetween(plan, left, right, ex)
	case *InExpr:
		return evalJoinRawIn(plan, left, right, ex)
	default:
		return nil, fmt.Errorf("unsupported join fast-path expression %T", e)
	}
}

// evalJoinRawBetween evaluates BETWEEN in the join fast path with a single
// evaluation of the comparand.
func evalJoinRawBetween(plan *simpleJoinPlan, left, right []any, ex *BetweenExpr) (any, error) {
	v, err := evalJoinRawExpr(plan, left, right, ex.Expr)
	if err != nil {
		return nil, err
	}
	lo, err := evalJoinRawExpr(plan, left, right, ex.Lo)
	if err != nil {
		return nil, err
	}
	hi, err := evalJoinRawExpr(plan, left, right, ex.Hi)
	if err != nil {
		return nil, err
	}
	return betweenResult(v, lo, hi, ex.Negate)
}

func evalJoinRawVarRef(plan *simpleJoinPlan, left, right []any, ex *VarRef) (any, error) {
	name := ex.Lower
	if name == "" {
		name = strings.ToLower(ex.Name)
	}
	if i, ok := plan.leftIndex[name]; ok {
		if _, ambiguous := plan.rightIndex[name]; ambiguous {
			return nil, fmt.Errorf("ambiguous column %q", ex.Name)
		}
		return left[i], nil
	}
	if i, ok := plan.rightIndex[name]; ok {
		return right[i], nil
	}
	return nil, unknownColumnErr(ex.Name, columnSuggestion(ex.Name, plan.leftIndex, plan.rightIndex))
}

func evalJoinRawIsNull(plan *simpleJoinPlan, left, right []any, ex *IsNull) (any, error) {
	v, err := evalJoinRawExpr(plan, left, right, ex.Expr)
	if err != nil {
		return nil, err
	}
	is := isNull(v)
	if ex.Negate {
		return !is, nil
	}
	return is, nil
}

func evalJoinRawUnary(plan *simpleJoinPlan, left, right []any, ex *Unary) (any, error) {
	v, err := evalJoinRawExpr(plan, left, right, ex.Expr)
	if err != nil {
		return nil, err
	}
	return applyUnaryOp(ex.Op, v)
}

func evalJoinRawLike(plan *simpleJoinPlan, left, right []any, ex *LikeExpr) (any, error) {
	val, err := evalJoinRawExpr(plan, left, right, ex.Expr)
	if err != nil {
		return nil, err
	}
	patVal, err := evalJoinRawExpr(plan, left, right, ex.Pattern)
	if err != nil {
		return nil, err
	}
	if val == nil || patVal == nil {
		return false, nil
	}
	str := valueText(val)
	pattern := valueText(patVal)
	matched, err := evalJoinRawLikeMatch(plan, left, right, ex, str, pattern)
	if err != nil {
		return nil, err
	}
	if ex.Negate {
		return !matched, nil
	}
	return matched, nil
}

func evalJoinRawLikeMatch(plan *simpleJoinPlan, left, right []any, ex *LikeExpr, str, pattern string) (bool, error) {
	if ex.GlobStyle {
		if ex.CaseInsensitive {
			return matchGlobPattern(strings.ToLower(str), strings.ToLower(pattern)), nil
		}
		return matchGlobPattern(str, pattern), nil
	}
	escapeChar := '\\'
	if ex.Escape != nil {
		escVal, err := evalJoinRawExpr(plan, left, right, ex.Escape)
		if err != nil {
			return false, err
		}
		if escStr, ok := escVal.(string); ok && len(escStr) == 1 {
			escapeChar = rune(escStr[0])
		}
	}
	if ex.Escape == nil {
		return compileCachedLikeMatcher(pattern, ex.CaseInsensitive)(str), nil
	}
	if ex.CaseInsensitive {
		return matchLikePattern(strings.ToLower(str), strings.ToLower(pattern), escapeChar), nil
	}
	return matchLikePattern(str, pattern, escapeChar), nil
}

func evalJoinRawRegexp(plan *simpleJoinPlan, left, right []any, ex *RegexpExpr) (any, error) {
	val, err := evalJoinRawExpr(plan, left, right, ex.Expr)
	if err != nil {
		return nil, err
	}
	patVal, err := evalJoinRawExpr(plan, left, right, ex.Pattern)
	if err != nil {
		return nil, err
	}
	if val == nil || patVal == nil {
		return false, nil
	}
	str := valueText(val)
	pat := valueText(patVal)
	if ex.SimilarTo {
		pat = similarToRegexp(pat)
	}
	re, err := compileCachedRegexp(pat)
	if err != nil {
		return nil, fmt.Errorf("REGEXP: invalid pattern %q: %v", pat, err)
	}
	matched := re.MatchString(str)
	if ex.Negate {
		return !matched, nil
	}
	return matched, nil
}

func evalJoinRawIn(plan *simpleJoinPlan, left, right []any, ex *InExpr) (any, error) {
	val, err := evalJoinRawExpr(plan, left, right, ex.Expr)
	if err != nil {
		return nil, err
	}
	for _, valExpr := range ex.Values {
		listVal, err := evalJoinRawExpr(plan, left, right, valExpr)
		if err != nil {
			return nil, err
		}
		if rawEqual(val, listVal) {
			if ex.Negate {
				return false, nil
			}
			return true, nil
		}
	}
	if ex.Negate {
		return true, nil
	}
	return false, nil
}

func evalJoinRawBinary(plan *simpleJoinPlan, left, right []any, ex *Binary) (any, error) {
	if ex.Op == "AND" || ex.Op == "OR" {
		lv, err := evalJoinRawExpr(plan, left, right, ex.Left)
		if err != nil {
			return nil, err
		}
		if ex.Op == "AND" && toTri(lv) == tvFalse {
			return false, nil
		}
		if ex.Op == "OR" && toTri(lv) == tvTrue {
			return true, nil
		}
		rv, err := evalJoinRawExpr(plan, left, right, ex.Right)
		if err != nil {
			return nil, err
		}
		if ex.Op == "AND" {
			return triToValue(triAnd(toTri(lv), toTri(rv))), nil
		}
		return triToValue(triOr(toTri(lv), toTri(rv))), nil
	}
	lv, err := evalJoinRawExpr(plan, left, right, ex.Left)
	if err != nil {
		return nil, err
	}
	rv, err := evalJoinRawExpr(plan, left, right, ex.Right)
	if err != nil {
		return nil, err
	}
	if isArithmeticOp(ex.Op) {
		return evalArithmeticBinary(ex.Op, lv, rv)
	}
	if isComparisonOp(ex.Op) {
		return evalComparisonBinary(ex.Op, lv, rv)
	}
	return nil, fmt.Errorf("unknown binary operator: %s", ex.Op)
}
