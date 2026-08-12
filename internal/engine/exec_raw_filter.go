// Compiling a WHERE clause into a closure over stored rows.
//
// Each builder returns a filter specialised for the shapes it recognises —
// column against literal, integer comparison, compiled LIKE, IN over a value
// set — and falls back to the generic expression evaluator for anything it does
// not. A nil filter means "not compilable", which disqualifies the fast path
// rather than producing a wrong answer.
package engine

import (
	"fmt"
	"regexp"
	"strings"
)

// combineAndConjuncts is the inverse of flattenAndConjuncts: it rebuilds a
// left-associative AND chain from a conjunct list, or nil for an empty list
// (buildRawFilter/buildRawFilterSpecialized both treat a nil expression as
// "always true").
func combineAndConjuncts(exprs []Expr) Expr {
	if len(exprs) == 0 {
		return nil
	}
	out := exprs[0]
	for _, x := range exprs[1:] {
		out = &Binary{Op: "AND", Left: out, Right: x}
	}
	return out
}

// rowToTextLikeMatcher recognizes `ROW_TO_TEXT() LIKE 'literal'` (optionally
// NOT LIKE / ILIKE) and compiles its pattern into a plain string matcher.
// GLOB patterns and an explicit ESCAPE clause are left to the general path —
// both are rare for this idiom and not worth the extra cases here.
func rowToTextLikeMatcher(e Expr) (func(string) bool, bool) {
	like, ok := e.(*LikeExpr)
	if !ok || like.GlobStyle || like.Escape != nil {
		return nil, false
	}
	call, ok := like.Expr.(*FuncCall)
	if !ok || call.Name != "ROW_TO_TEXT" || len(call.Args) != 0 {
		return nil, false
	}
	lit, ok := like.Pattern.(*Literal)
	if !ok {
		return nil, false
	}
	pattern, ok := lit.Val.(string)
	if !ok {
		return nil, false
	}
	match := compileLikeStringMatcher(pattern, like.CaseInsensitive)
	if like.Negate {
		return func(s string) bool { return !match(s) }, true
	}
	return match, true
}

// compileLikeStringMatcher compiles a literal LIKE/ILIKE pattern (% and _
// wildcards, no ESCAPE/GLOB) into the cheapest possible func(string) bool,
// mirroring the shape-detection buildCompiledLikeFilter/buildCompiledILikeFilter
// use for column LIKE — exact/prefix/suffix/substring patterns skip the
// general backtracking matcher entirely.
func compileLikeStringMatcher(pattern string, caseInsensitive bool) func(string) bool {
	pat := pattern
	if caseInsensitive {
		pat = strings.ToLower(pattern)
	}
	norm := func(s string) string {
		if caseInsensitive {
			return strings.ToLower(s)
		}
		return s
	}
	switch {
	case !strings.ContainsAny(pat, "%_"):
		return func(s string) bool { return norm(s) == pat }
	case strings.HasSuffix(pat, "%") && !strings.ContainsAny(pat[:len(pat)-1], "%_"):
		prefix := pat[:len(pat)-1]
		return func(s string) bool { return strings.HasPrefix(norm(s), prefix) }
	case strings.HasPrefix(pat, "%") && !strings.ContainsAny(pat[1:], "%_"):
		suffix := pat[1:]
		return func(s string) bool { return strings.HasSuffix(norm(s), suffix) }
	case strings.HasPrefix(pat, "%") && strings.HasSuffix(pat, "%") &&
		len(pat) >= 2 && !strings.ContainsAny(pat[1:len(pat)-1], "%_"):
		mid := pat[1 : len(pat)-1]
		return func(s string) bool { return strings.Contains(norm(s), mid) }
	default:
		return func(s string) bool { return matchLikePattern(norm(s), pat, '\\') }
	}
}

// buildRawRowToTextAndFilter detects 2+ `ROW_TO_TEXT() LIKE 'term'` conjuncts
// within an AND chain and compiles them into a single pass: the whole-row
// text is built once per row and checked against every term, rather than
// once per term (each of which previously fell through to
// buildRawExprFilter and independently rebuilt the concatenated row text —
// O(terms) rebuilds of O(columns) work per row instead of one). Any other
// conjuncts in the chain (plain column predicates, etc.) are compiled
// separately through the normal buildRawFilter path and checked afterward.
// Returns nil when fewer than 2 such conjuncts are present, leaving
// buildRawFilterSpecialized/buildRawExprFilter to handle the expression as
// before.
func buildRawRowToTextAndFilter(colIndex map[string]int, e Expr) func([]any) (bool, error) {
	conjuncts := flattenAndConjuncts(e)
	if len(conjuncts) < 2 {
		return nil
	}
	var matchers []func(string) bool
	var rest []Expr
	for _, c := range conjuncts {
		if m, ok := rowToTextLikeMatcher(c); ok {
			matchers = append(matchers, m)
		} else {
			rest = append(rest, c)
		}
	}
	if len(matchers) < 2 {
		return nil
	}
	restFilter := buildRawFilter(colIndex, combineAndConjuncts(rest))
	if restFilter == nil {
		// Some remaining conjunct can't run on the raw path (e.g. a
		// subquery); abandon this optimization and let the caller fall
		// back to the slower but always-correct per-conjunct compilation.
		return nil
	}
	cols := rawRowTextColumns(colIndex)
	return func(raw []any) (bool, error) {
		text := rawRowToText(cols, raw)
		for _, m := range matchers {
			if !m(text) {
				return false, nil
			}
		}
		return restFilter(raw)
	}
}

// buildRawFilterSpecialized compiles the predicate forms with dedicated,
// type-specialized closures (column/literal comparisons, LIKE with literal
// pattern, IN lists, ...). Returns nil for anything else. AND/OR use this
// distinction as a cost signal: a specialized side is cheap (a raw slice
// access plus a comparison) and runs first, while an expression that only
// evaluates through the evalRawExpr fallback (typically a function call
// such as a vector distance) runs second, only on rows that survive.
func buildRawFilterSpecialized(colIndex map[string]int, e Expr) func([]any) (bool, error) {
	if e == nil {
		return func([]any) (bool, error) { return true, nil }
	}
	switch ex := e.(type) {
	case *VarRef:
		return buildRawFilterVarRef(colIndex, ex)
	case *Unary:
		return buildRawFilterUnary(colIndex, ex)
	case *IsNull:
		return buildRawFilterIsNull(colIndex, ex)
	case *Binary:
		return buildRawFilterBinary(colIndex, ex)
	case *LikeExpr:
		return buildRawFilterLike(colIndex, ex)
	case *RegexpExpr:
		return buildRawFilterRegexp(colIndex, ex)
	case *InExpr:
		return buildRawFilterIn(colIndex, ex)
	case *FuncCall:
		return buildRawFilterFuncCall(colIndex, ex)
	}
	return nil
}

// buildRawExprFilter wraps an arbitrary raw-evaluable expression as a filter,
// using the same truthiness conversion (toTri == tvTrue) as the AND/OR
// fallback paths so three-valued logic matches the general evaluator.
// Returns nil when the expression cannot run on raw rows.
func buildRawExprFilter(colIndex map[string]int, e Expr) func([]any) (bool, error) {
	if !isSimpleRawExpr(e) || exprHasRowAwareFuncCall(e) {
		return nil
	}
	plan := &simpleSelectPlan{colIndex: colIndex, rowTextCols: rawRowTextColumns(colIndex)}
	return func(raw []any) (bool, error) {
		v, err := evalRawExpr(plan, raw, e)
		if err != nil {
			return false, err
		}
		return toTri(v) == tvTrue, nil
	}
}

func buildRawFilterVarRef(colIndex map[string]int, ex *VarRef) func([]any) (bool, error) {
	key := ex.Lower
	if key == "" {
		key = strings.ToLower(ex.Name)
	}
	colIdx, ok := colIndex[key]
	if !ok {
		return nil
	}
	return func(raw []any) (bool, error) { return truthy(raw[colIdx]), nil }
}

func buildRawFilterUnary(colIndex map[string]int, ex *Unary) func([]any) (bool, error) {
	if ex.Op != "NOT" {
		return nil
	}
	// NOT must implement three-valued logic: NOT(unknown) stays unknown
	// (row excluded), not "not false" = true. The specialized
	// comparison/LIKE/IN filters compile to a plain bool that intentionally
	// collapses a NULL-involving predicate to false — safe when applied
	// directly (both false and unknown exclude the row the same way), but
	// wrong once negated, since !false == true loses the distinction.
	// evalRawExpr's own NOT case already implements this correctly via
	// toTri/triNot, so delegate to it instead of negating a collapsed bool.
	return buildRawExprFilter(colIndex, ex)
}

func buildRawFilterIsNull(colIndex map[string]int, ex *IsNull) func([]any) (bool, error) {
	innerRef, ok := ex.Expr.(*VarRef)
	if !ok {
		return nil
	}
	colIdx, ok := colIndex[strings.ToLower(innerRef.Name)]
	if !ok {
		return nil
	}
	if ex.Negate {
		return func(raw []any) (bool, error) { return raw[colIdx] != nil, nil }
	}
	return func(raw []any) (bool, error) { return raw[colIdx] == nil, nil }
}

func buildRawFilterBinary(colIndex map[string]int, ex *Binary) func([]any) (bool, error) {
	switch ex.Op {
	case "AND":
		return buildRawAndFilter(colIndex, ex.Left, ex.Right)
	case "OR":
		return buildRawOrFilter(colIndex, ex.Left, ex.Right)
	default:
		if isComparisonOp(ex.Op) {
			return buildRawComparisonFilter(colIndex, ex)
		}
		return nil
	}
}

func buildRawAndFilter(colIndex map[string]int, leftExpr, rightExpr Expr) func([]any) (bool, error) {
	left := buildRawFilterSpecialized(colIndex, leftExpr)
	right := buildRawFilterSpecialized(colIndex, rightExpr)
	if left != nil && right != nil {
		return func(raw []any) (bool, error) {
			l, err := left(raw)
			if err != nil || !l {
				return false, err
			}
			return right(raw)
		}
	}
	if left == nil && right == nil {
		// Neither side compiles to a specialized filter (e.g. two
		// function-call predicates). Run both through the expression
		// fallback, left first, so the plan still avoids the Row-map
		// evaluator.
		lf := buildRawExprFilter(colIndex, leftExpr)
		rf := buildRawExprFilter(colIndex, rightExpr)
		if lf == nil || rf == nil {
			return nil
		}
		return func(raw []any) (bool, error) {
			l, err := lf(raw)
			if err != nil || !l {
				return false, err
			}
			return rf(raw)
		}
	}

	// Exactly one side compiled. The specialized side is cheap (raw slice
	// access + comparison), so it runs first regardless of written order;
	// the expression side runs only on surviving rows. The fallback
	// evaluates via evalRawExpr(plan, raw, expr). Unsupported row-aware
	// functions still force the general evaluator; ROW_TO_TEXT has a raw
	// implementation with precomputed column indexes and is safe here.
	if left == nil {
		if exprHasRowAwareFuncCall(leftExpr) {
			return nil
		}
		return buildRawAndFilterWithFallback(colIndex, leftExpr, right)
	}
	if exprHasRowAwareFuncCall(rightExpr) {
		return nil
	}
	return buildRawAndFilterWithFallback(colIndex, rightExpr, left)
}

func buildRawAndFilterWithFallback(colIndex map[string]int, expr Expr, fastFilter func([]any) (bool, error)) func([]any) (bool, error) {
	plan := &simpleSelectPlan{colIndex: colIndex, rowTextCols: rawRowTextColumns(colIndex)}
	return func(raw []any) (bool, error) {
		fast, err := fastFilter(raw)
		if err != nil || !fast {
			return false, err
		}
		v, err := evalRawExpr(plan, raw, expr)
		if err != nil {
			return false, err
		}
		return toTri(v) == tvTrue, nil
	}
}

func buildRawOrFilter(colIndex map[string]int, leftExpr, rightExpr Expr) func([]any) (bool, error) {
	left := buildRawFilterSpecialized(colIndex, leftExpr)
	right := buildRawFilterSpecialized(colIndex, rightExpr)
	if left != nil && right != nil {
		return func(raw []any) (bool, error) {
			l, err := left(raw)
			if err != nil {
				return false, err
			}
			if l {
				return true, nil
			}
			return right(raw)
		}
	}
	if left == nil && right == nil {
		// See the matching comment in buildRawAndFilter.
		lf := buildRawExprFilter(colIndex, leftExpr)
		rf := buildRawExprFilter(colIndex, rightExpr)
		if lf == nil || rf == nil {
			return nil
		}
		return func(raw []any) (bool, error) {
			l, err := lf(raw)
			if err != nil {
				return false, err
			}
			if l {
				return true, nil
			}
			return rf(raw)
		}
	}

	// See the matching comment in buildRawAndFilter: the specialized (cheap)
	// side short-circuits first. Unsupported row-aware functions still use the
	// general evaluator, while ROW_TO_TEXT is safe on the raw path.
	if left == nil {
		if exprHasRowAwareFuncCall(leftExpr) {
			return nil
		}
		return buildRawOrFilterWithFallback(colIndex, leftExpr, right)
	}
	if exprHasRowAwareFuncCall(rightExpr) {
		return nil
	}
	return buildRawOrFilterWithFallback(colIndex, rightExpr, left)

}

func buildRawOrFilterWithFallback(colIndex map[string]int, expr Expr, fastFilter func([]any) (bool, error)) func([]any) (bool, error) {
	plan := &simpleSelectPlan{colIndex: colIndex, rowTextCols: rawRowTextColumns(colIndex)}
	return func(raw []any) (bool, error) {
		fast, err := fastFilter(raw)
		if err != nil || fast {
			return fast, err
		}
		v, err := evalRawExpr(plan, raw, expr)
		if err != nil {
			return false, err
		}
		return toTri(v) == tvTrue, nil
	}
}

func buildRawComparisonFilter(colIndex map[string]int, ex *Binary) func([]any) (bool, error) {
	if ref, ok := ex.Left.(*VarRef); ok {
		if lit, ok := ex.Right.(*Literal); ok {
			if colIdx, ok := colIndex[strings.ToLower(ref.Name)]; ok {
				if lit.Parameter {
					return buildBoundLiteralFilter(colIdx, ex.Op, lit)
				}
				return buildColLiteralFilter(colIdx, ex.Op, lit.Val)
			}
		}
	}
	if lit, ok := ex.Left.(*Literal); ok {
		if ref, ok := ex.Right.(*VarRef); ok {
			if colIdx, ok := colIndex[strings.ToLower(ref.Name)]; ok {
				if lit.Parameter {
					return buildBoundLiteralFilter(colIdx, reverseComparisonOp(ex.Op), lit)
				}
				return buildColLiteralFilter(colIdx, reverseComparisonOp(ex.Op), lit.Val)
			}
		}
	}
	if lRef, ok := ex.Left.(*VarRef); ok {
		if rRef, ok := ex.Right.(*VarRef); ok {
			lIdx, lok := colIndex[strings.ToLower(lRef.Name)]
			rIdx, rok := colIndex[strings.ToLower(rRef.Name)]
			if lok && rok {
				return buildColColFilter(lIdx, ex.Op, rIdx)
			}
		}
	}
	return nil
}

// buildBoundLiteralFilter reads the current parameter value on every call.
// Prepared statements mutate Literal.Val under their statement mutex, while
// the cached plan shape and closure stay immutable.
func buildBoundLiteralFilter(colIdx int, op string, literal *Literal) func([]any) (bool, error) {
	return func(raw []any) (bool, error) {
		a, b := raw[colIdx], literal.Val
		if a == nil || b == nil {
			return false, nil
		}
		switch op {
		case "=":
			return rawEqual(a, b), nil
		case "!=", "<>":
			return !rawEqual(a, b), nil
		}
		comparison, err := compare(a, b)
		if err != nil {
			return false, err
		}
		switch op {
		case "<":
			return comparison < 0, nil
		case "<=":
			return comparison <= 0, nil
		case ">":
			return comparison > 0, nil
		case ">=":
			return comparison >= 0, nil
		default:
			return false, fmt.Errorf("unsupported comparison operator %q", op)
		}
	}
}

func buildRawFilterLike(colIndex map[string]int, ex *LikeExpr) func([]any) (bool, error) {
	ref, isRef := ex.Expr.(*VarRef)
	if !isRef {
		return nil
	}
	colIdx, ok := colIndex[strings.ToLower(ref.Name)]
	if !ok {
		return nil
	}
	pat, isLit := ex.Pattern.(*Literal)
	if !isLit {
		return nil
	}
	pattern, isStr := pat.Val.(string)
	if !isStr {
		return nil
	}
	if ex.GlobStyle {
		return buildCompiledGlobFilter(colIdx, pattern, ex.CaseInsensitive, ex.Negate)
	}
	if ex.Escape != nil {
		return nil
	}
	if ex.CaseInsensitive {
		return buildCompiledILikeFilter(colIdx, pattern, ex.Negate)
	}
	return buildCompiledLikeFilter(colIdx, pattern, ex.Negate)
}

func buildRawFilterRegexp(colIndex map[string]int, ex *RegexpExpr) func([]any) (bool, error) {
	ref, isRef := ex.Expr.(*VarRef)
	if !isRef {
		return nil
	}
	colIdx, ok := colIndex[strings.ToLower(ref.Name)]
	if !ok {
		return nil
	}
	pat, isLit := ex.Pattern.(*Literal)
	if !isLit {
		return nil
	}
	pattern, isStr := pat.Val.(string)
	if !isStr {
		return nil
	}
	if ex.SimilarTo {
		pattern = similarToRegexp(pattern)
	}
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil
	}
	negate := ex.Negate
	return func(raw []any) (bool, error) {
		s, ok := raw[colIdx].(string)
		if !ok {
			if raw[colIdx] == nil {
				return false, nil
			}
			s = fmt.Sprintf("%v", raw[colIdx])
		}
		matched := re.MatchString(s)
		if negate {
			return !matched, nil
		}
		return matched, nil
	}
}

// buildRawFilterFuncCall dispatches specialized raw-path compilation for
// function-call predicates. Returns nil (falling back to the generic
// evalFuncCall-based path) for any function it doesn't recognize.
func buildRawFilterFuncCall(colIndex map[string]int, ex *FuncCall) func([]any) (bool, error) {
	switch ex.Name {
	case "CONTAINS_ALL", "CONTAINS_ANY":
		return buildRawFilterContains(colIndex, ex)
	}
	return nil
}

// buildRawFilterContains compiles CONTAINS_ALL/CONTAINS_ANY with literal
// string terms into a closure that evaluates the searched text once per row
// (reusing the raw-path evalRawExpr, so a ROW_TO_TEXT() first argument still
// gets its cheap precomputed-column-index handling) and checks it against
// precompiled lowercase terms directly, skipping the generic
// rawCallScratchPool + map-dispatch path used for arbitrary function calls.
// Must stay behaviorally identical to evalContainsAll/evalContainsAny in
// fts.go (same case-insensitive substring semantics, same nil-text handling).
func buildRawFilterContains(colIndex map[string]int, ex *FuncCall) func([]any) (bool, error) {
	if len(ex.Args) < 2 || !isSimpleRawExpr(ex.Args[0]) || exprHasRowAwareFuncCall(ex.Args[0]) {
		return nil
	}
	terms := make([]string, 0, len(ex.Args)-1)
	for _, arg := range ex.Args[1:] {
		lit, ok := arg.(*Literal)
		if !ok {
			return nil
		}
		s, ok := lit.Val.(string)
		if !ok {
			return nil
		}
		terms = append(terms, strings.ToLower(s))
	}
	all := ex.Name == "CONTAINS_ALL"
	plan := &simpleSelectPlan{colIndex: colIndex, rowTextCols: rawRowTextColumns(colIndex)}
	textExpr := ex.Args[0]
	return func(raw []any) (bool, error) {
		v, err := evalRawExpr(plan, raw, textExpr)
		if err != nil {
			return false, err
		}
		if v == nil {
			return false, nil
		}
		text := strings.ToLower(ftsValueToString(v))
		for _, term := range terms {
			found := strings.Contains(text, term)
			if all && !found {
				return false, nil
			}
			if !all && found {
				return true, nil
			}
		}
		return all, nil
	}
}

func buildRawFilterIn(colIndex map[string]int, ex *InExpr) func([]any) (bool, error) {
	ref, isRef := ex.Expr.(*VarRef)
	if !isRef {
		return nil
	}
	colIdx, ok := colIndex[strings.ToLower(ref.Name)]
	if !ok {
		return nil
	}
	litVals := make([]any, 0, len(ex.Values))
	for _, v := range ex.Values {
		lit, ok := v.(*Literal)
		if !ok {
			return nil
		}
		litVals = append(litVals, lit.Val)
	}
	return buildInFilter(colIdx, litVals, ex.Negate)
}

// reverseComparisonOp reverses the direction of a comparison operator,
// so that "literal op col" can be treated as "col reversed_op literal".
func reverseComparisonOp(op string) string {
	switch op {
	case "<":
		return ">"
	case "<=":
		return ">="
	case ">":
		return "<"
	case ">=":
		return "<="
	default:
		return op // "=" and "!=" / "<>" are symmetric
	}
}

// buildColLiteralFilter builds a fast comparison closure for "raw[colIdx] op litVal".
// The returned function is type-specialized for int, float64, and string to avoid
// the overhead of the generic compare() path.
func buildColLiteralFilter(colIdx int, op string, litVal any) func([]any) (bool, error) {
	switch op {
	case "=":
		return func(raw []any) (bool, error) {
			a := raw[colIdx]
			if a == nil || litVal == nil {
				return false, nil
			}
			return rawEqual(a, litVal), nil
		}
	case "!=", "<>":
		return func(raw []any) (bool, error) {
			a := raw[colIdx]
			if a == nil || litVal == nil {
				return false, nil
			}
			return !rawEqual(a, litVal), nil
		}
	case "<", "<=", ">", ">=":
		// Specialize for the three common literal types to avoid compare().
		switch lv := litVal.(type) {
		case int:
			return buildIntCmpFilter(colIdx, op, lv)
		case int64:
			return buildInt64CmpFilter(colIdx, op, lv)
		case float64:
			return buildFloat64CmpFilter(colIdx, op, lv)
		case string:
			return buildStringCmpFilter(colIdx, op, lv)
		}
		// Generic fallback via compare().
		return func(raw []any) (bool, error) {
			a := raw[colIdx]
			if a == nil || litVal == nil {
				return false, nil
			}
			cmp, err := compare(a, litVal)
			if err != nil {
				return false, err
			}
			switch op {
			case "<":
				return cmp < 0, nil
			case "<=":
				return cmp <= 0, nil
			case ">":
				return cmp > 0, nil
			default: // ">="
				return cmp >= 0, nil
			}
		}
	}
	return nil
}

// buildIntCmpFilter builds a specialized ordering filter for an int literal.
func buildIntCmpFilter(colIdx int, op string, lit int) func([]any) (bool, error) {
	switch op {
	case "<":
		return func(raw []any) (bool, error) {
			a := raw[colIdx]
			switch av := a.(type) {
			case int:
				return av < lit, nil
			case int64:
				return av < int64(lit), nil
			case float64:
				return av < float64(lit), nil
			}
			return false, nil
		}
	case "<=":
		return func(raw []any) (bool, error) {
			a := raw[colIdx]
			switch av := a.(type) {
			case int:
				return av <= lit, nil
			case int64:
				return av <= int64(lit), nil
			case float64:
				return av <= float64(lit), nil
			}
			return false, nil
		}
	case ">":
		return func(raw []any) (bool, error) {
			a := raw[colIdx]
			switch av := a.(type) {
			case int:
				return av > lit, nil
			case int64:
				return av > int64(lit), nil
			case float64:
				return av > float64(lit), nil
			}
			return false, nil
		}
	case ">=":
		return func(raw []any) (bool, error) {
			a := raw[colIdx]
			switch av := a.(type) {
			case int:
				return av >= lit, nil
			case int64:
				return av >= int64(lit), nil
			case float64:
				return av >= float64(lit), nil
			}
			return false, nil
		}
	}
	return nil
}

// buildInt64CmpFilter builds a specialized ordering filter for an int64 literal.
func buildInt64CmpFilter(colIdx int, op string, lit int64) func([]any) (bool, error) {
	switch op {
	case "<":
		return func(raw []any) (bool, error) {
			a := raw[colIdx]
			switch av := a.(type) {
			case int:
				return int64(av) < lit, nil
			case int64:
				return av < lit, nil
			case float64:
				return av < float64(lit), nil
			}
			return false, nil
		}
	case "<=":
		return func(raw []any) (bool, error) {
			a := raw[colIdx]
			switch av := a.(type) {
			case int:
				return int64(av) <= lit, nil
			case int64:
				return av <= lit, nil
			case float64:
				return av <= float64(lit), nil
			}
			return false, nil
		}
	case ">":
		return func(raw []any) (bool, error) {
			a := raw[colIdx]
			switch av := a.(type) {
			case int:
				return int64(av) > lit, nil
			case int64:
				return av > lit, nil
			case float64:
				return av > float64(lit), nil
			}
			return false, nil
		}
	case ">=":
		return func(raw []any) (bool, error) {
			a := raw[colIdx]
			switch av := a.(type) {
			case int:
				return int64(av) >= lit, nil
			case int64:
				return av >= lit, nil
			case float64:
				return av >= float64(lit), nil
			}
			return false, nil
		}
	}
	return nil
}

// buildFloat64CmpFilter builds a specialized ordering filter for a float64 literal.
func buildFloat64CmpFilter(colIdx int, op string, lit float64) func([]any) (bool, error) {
	switch op {
	case "<":
		return func(raw []any) (bool, error) {
			a := raw[colIdx]
			if f, ok := numericFast(a); ok {
				return f < lit, nil
			}
			return false, nil
		}
	case "<=":
		return func(raw []any) (bool, error) {
			a := raw[colIdx]
			if f, ok := numericFast(a); ok {
				return f <= lit, nil
			}
			return false, nil
		}
	case ">":
		return func(raw []any) (bool, error) {
			a := raw[colIdx]
			if f, ok := numericFast(a); ok {
				return f > lit, nil
			}
			return false, nil
		}
	case ">=":
		return func(raw []any) (bool, error) {
			a := raw[colIdx]
			if f, ok := numericFast(a); ok {
				return f >= lit, nil
			}
			return false, nil
		}
	}
	return nil
}

// buildStringCmpFilter builds a specialized ordering filter for a string literal.
func buildStringCmpFilter(colIdx int, op string, lit string) func([]any) (bool, error) {
	switch op {
	case "<":
		return func(raw []any) (bool, error) {
			if s, ok := raw[colIdx].(string); ok {
				return s < lit, nil
			}
			return false, nil
		}
	case "<=":
		return func(raw []any) (bool, error) {
			if s, ok := raw[colIdx].(string); ok {
				return s <= lit, nil
			}
			return false, nil
		}
	case ">":
		return func(raw []any) (bool, error) {
			if s, ok := raw[colIdx].(string); ok {
				return s > lit, nil
			}
			return false, nil
		}
	case ">=":
		return func(raw []any) (bool, error) {
			if s, ok := raw[colIdx].(string); ok {
				return s >= lit, nil
			}
			return false, nil
		}
	}
	return nil
}

// buildColColFilter builds a filter for "raw[lIdx] op raw[rIdx]" (col op col).
func buildColColFilter(lIdx int, op string, rIdx int) func([]any) (bool, error) {
	return func(raw []any) (bool, error) {
		a, b := raw[lIdx], raw[rIdx]
		if a == nil || b == nil {
			return false, nil
		}
		switch op {
		case "=":
			return rawEqual(a, b), nil
		case "!=", "<>":
			return !rawEqual(a, b), nil
		}
		cmp, err := compare(a, b)
		if err != nil {
			return false, err
		}
		switch op {
		case "<":
			return cmp < 0, nil
		case "<=":
			return cmp <= 0, nil
		case ">":
			return cmp > 0, nil
		default: // ">="
			return cmp >= 0, nil
		}
	}
}

// numericFast converts the common numeric types to float64 without the decimal
// or string-parsing branches of the general numeric() helper.
func numericFast(v any) (float64, bool) {
	switch n := v.(type) {
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case float64:
		return n, true
	}
	return 0, false
}

// buildCompiledLikeFilter compiles a SQL LIKE pattern (with default escape '\\')
// into a specialized closure. Common patterns are reduced to library calls:
//   - 'exact'       →  s == "exact"
//   - 'prefix%'     →  strings.HasPrefix(s, "prefix")
//   - '%suffix'     →  strings.HasSuffix(s, "suffix")
//   - '%middle%'    →  strings.Contains(s, "middle")
//
// Patterns containing '_' wildcards or multiple '%' anchors fall back to the
// general matchLikePattern function.
func buildCompiledLikeFilter(colIdx int, pattern string, negate bool) func([]any) (bool, error) {
	matchFn := func(match func(string) bool) func([]any) (bool, error) {
		if negate {
			return func(raw []any) (bool, error) {
				s, ok := raw[colIdx].(string)
				if !ok {
					return false, nil
				}
				return !match(s), nil
			}
		}
		return func(raw []any) (bool, error) {
			s, ok := raw[colIdx].(string)
			if !ok {
				return false, nil
			}
			return match(s), nil
		}
	}

	// No wildcards → exact match.
	if !strings.ContainsAny(pattern, "%_") {
		return matchFn(func(s string) bool { return s == pattern })
	}

	// prefix% – no other wildcards.
	if strings.HasSuffix(pattern, "%") && !strings.ContainsAny(pattern[:len(pattern)-1], "%_") {
		prefix := pattern[:len(pattern)-1]
		return matchFn(func(s string) bool { return strings.HasPrefix(s, prefix) })
	}

	// %suffix – no other wildcards.
	if strings.HasPrefix(pattern, "%") && !strings.ContainsAny(pattern[1:], "%_") {
		suffix := pattern[1:]
		return matchFn(func(s string) bool { return strings.HasSuffix(s, suffix) })
	}

	// %middle% – no other wildcards.
	if strings.HasPrefix(pattern, "%") && strings.HasSuffix(pattern, "%") &&
		len(pattern) >= 2 && !strings.ContainsAny(pattern[1:len(pattern)-1], "%_") {
		middle := pattern[1 : len(pattern)-1]
		return matchFn(func(s string) bool { return strings.Contains(s, middle) })
	}

	// Fall back to the general matcher.
	return matchFn(func(s string) bool { return matchLikePattern(s, pattern, '\\') })
}

// buildCompiledILikeFilter compiles an ILIKE / NOT ILIKE pattern into a closure.
// It pre-lowercases the pattern and lowercases each column value at match time,
// without any per-row heap allocations.
func buildCompiledILikeFilter(colIdx int, pattern string, negate bool) func([]any) (bool, error) {
	// Pre-lowercase the pattern once.
	lowerPat := strings.ToLower(pattern)

	// Pick the cheapest sub-matcher based on the lowercased pattern shape.
	var match func(string) bool
	switch {
	case !strings.ContainsAny(lowerPat, "%_"):
		match = func(s string) bool { return strings.ToLower(s) == lowerPat }
	case strings.HasSuffix(lowerPat, "%") && !strings.ContainsAny(lowerPat[:len(lowerPat)-1], "%_"):
		prefix := lowerPat[:len(lowerPat)-1]
		match = func(s string) bool { return strings.HasPrefix(strings.ToLower(s), prefix) }
	case strings.HasPrefix(lowerPat, "%") && !strings.ContainsAny(lowerPat[1:], "%_"):
		suffix := lowerPat[1:]
		match = func(s string) bool { return strings.HasSuffix(strings.ToLower(s), suffix) }
	case strings.HasPrefix(lowerPat, "%") && strings.HasSuffix(lowerPat, "%") &&
		len(lowerPat) >= 2 && !strings.ContainsAny(lowerPat[1:len(lowerPat)-1], "%_"):
		mid := lowerPat[1 : len(lowerPat)-1]
		match = func(s string) bool { return strings.Contains(strings.ToLower(s), mid) }
	default:
		match = func(s string) bool { return matchLikePattern(strings.ToLower(s), lowerPat, '\\') }
	}

	if negate {
		return func(raw []any) (bool, error) {
			s, ok := raw[colIdx].(string)
			if !ok {
				return false, nil
			}
			return !match(s), nil
		}
	}
	return func(raw []any) (bool, error) {
		s, ok := raw[colIdx].(string)
		if !ok {
			return false, nil
		}
		return match(s), nil
	}
}

// buildCompiledGlobFilter compiles a GLOB / NOT GLOB pattern into a closure.
// GLOB wildcards: * matches any sequence, ? matches any single character.
func buildCompiledGlobFilter(colIdx int, pattern string, caseInsensitive, negate bool) func([]any) (bool, error) {
	if caseInsensitive {
		pattern = strings.ToLower(pattern)
	}
	matchFn := func(s string) bool {
		if caseInsensitive {
			s = strings.ToLower(s)
		}
		return matchGlobPattern(s, pattern)
	}
	if negate {
		return func(raw []any) (bool, error) {
			s, ok := raw[colIdx].(string)
			if !ok {
				return false, nil
			}
			return !matchFn(s), nil
		}
	}
	return func(raw []any) (bool, error) {
		s, ok := raw[colIdx].(string)
		if !ok {
			return false, nil
		}
		return matchFn(s), nil
	}
}

// buildInFilter builds a fast set-membership closure for col IN (litVals).
// It pre-builds typed maps for all-int and all-string value sets for O(1) lookup.
func buildInFilter(colIdx int, litVals []any, negate bool) func([]any) (bool, error) {
	// Try to build typed sets for O(1) lookup.
	allInt, allInt64, allFloat64, allStr := true, true, true, true
	for _, v := range litVals {
		if _, ok := v.(int); !ok {
			allInt = false
		}
		if _, ok := v.(int64); !ok {
			allInt64 = false
		}
		if _, ok := v.(float64); !ok {
			allFloat64 = false
		}
		if _, ok := v.(string); !ok {
			allStr = false
		}
	}

	if allInt {
		set := make(map[int]struct{}, len(litVals))
		for _, v := range litVals {
			set[v.(int)] = struct{}{}
		}
		if negate {
			return func(raw []any) (bool, error) {
				a := raw[colIdx]
				if a == nil {
					return false, nil
				}
				if ai, ok := a.(int); ok {
					_, found := set[ai]
					return !found, nil
				}
				// Fall back for type mismatches (e.g., stored as int64).
				for _, v := range litVals {
					if rawEqual(a, v) {
						return false, nil
					}
				}
				return true, nil
			}
		}
		return func(raw []any) (bool, error) {
			a := raw[colIdx]
			if a == nil {
				return false, nil
			}
			if ai, ok := a.(int); ok {
				_, found := set[ai]
				return found, nil
			}
			for _, v := range litVals {
				if rawEqual(a, v) {
					return true, nil
				}
			}
			return false, nil
		}
	}

	if allInt64 {
		set := make(map[int64]struct{}, len(litVals))
		for _, v := range litVals {
			set[v.(int64)] = struct{}{}
		}
		if negate {
			return func(raw []any) (bool, error) {
				a := raw[colIdx]
				if a == nil {
					return false, nil
				}
				if ai, ok := a.(int64); ok {
					_, found := set[ai]
					return !found, nil
				}
				// Fall back for type mismatches (e.g., stored as int).
				for _, v := range litVals {
					if rawEqual(a, v) {
						return false, nil
					}
				}
				return true, nil
			}
		}
		return func(raw []any) (bool, error) {
			a := raw[colIdx]
			if a == nil {
				return false, nil
			}
			if ai, ok := a.(int64); ok {
				_, found := set[ai]
				return found, nil
			}
			for _, v := range litVals {
				if rawEqual(a, v) {
					return true, nil
				}
			}
			return false, nil
		}
	}

	if allFloat64 {
		set := make(map[float64]struct{}, len(litVals))
		for _, v := range litVals {
			set[v.(float64)] = struct{}{}
		}
		if negate {
			return func(raw []any) (bool, error) {
				a := raw[colIdx]
				if a == nil {
					return false, nil
				}
				if af, ok := a.(float64); ok {
					_, found := set[af]
					return !found, nil
				}
				// Fall back for type mismatches (e.g., stored as int).
				for _, v := range litVals {
					if rawEqual(a, v) {
						return false, nil
					}
				}
				return true, nil
			}
		}
		return func(raw []any) (bool, error) {
			a := raw[colIdx]
			if a == nil {
				return false, nil
			}
			if af, ok := a.(float64); ok {
				_, found := set[af]
				return found, nil
			}
			for _, v := range litVals {
				if rawEqual(a, v) {
					return true, nil
				}
			}
			return false, nil
		}
	}

	if allStr {
		set := make(map[string]struct{}, len(litVals))
		for _, v := range litVals {
			set[v.(string)] = struct{}{}
		}
		if negate {
			return func(raw []any) (bool, error) {
				s, ok := raw[colIdx].(string)
				if !ok {
					return false, nil
				}
				_, found := set[s]
				return !found, nil
			}
		}
		return func(raw []any) (bool, error) {
			s, ok := raw[colIdx].(string)
			if !ok {
				return false, nil
			}
			_, found := set[s]
			return found, nil
		}
	}

	// Generic fallback using rawEqual.
	if negate {
		return func(raw []any) (bool, error) {
			a := raw[colIdx]
			for _, v := range litVals {
				if rawEqual(a, v) {
					return false, nil
				}
			}
			return true, nil
		}
	}
	return func(raw []any) (bool, error) {
		a := raw[colIdx]
		for _, v := range litVals {
			if rawEqual(a, v) {
				return true, nil
			}
		}
		return false, nil
	}
}
