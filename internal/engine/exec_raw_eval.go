// Evaluating expressions directly against a stored row, for the fast paths.
// The generic evaluator in eval_expr.go handles the same algebra over row maps.
package engine

import (
	"bytes"
	"fmt"
	"strings"
	"sync"
)

// rawEqual performs a type-aware equality check between two interface values
// without going through the generic compare() function.  It covers the value
// types that tinySQL stores in table rows (int, int64, float64, string, bool,
// and []byte BLOBs).
func rawEqual(a, b any) bool {
	if a == nil {
		return b == nil
	}
	if b == nil {
		return false
	}
	switch av := a.(type) {
	case int:
		switch bv := b.(type) {
		case int:
			return av == bv
		case int64:
			return int64(av) == bv
		case float64:
			return float64(av) == bv
		}
	case int64:
		switch bv := b.(type) {
		case int:
			return av == int64(bv)
		case int64:
			return av == bv
		case float64:
			return float64(av) == bv
		}
	case float64:
		switch bv := b.(type) {
		case int:
			return av == float64(bv)
		case int64:
			return av == float64(bv)
		case float64:
			return av == bv
		}
	case string:
		if bv, ok := b.(string); ok {
			return av == bv
		}
	case bool:
		if bv, ok := b.(bool); ok {
			return av == bv
		}
	case []byte:
		if bv, ok := b.([]byte); ok {
			return bytes.Equal(av, bv)
		}
	}
	return false
}

func projectRawRow(plan *simpleSelectPlan, raw []any) (Row, error) {
	out := make(Row, plan.rowMapCap)
	for _, p := range plan.projs {
		var v any
		if p.colIdx >= 0 {
			// Direct column reference: skip type switch, map lookup, and ToLower.
			v = raw[p.colIdx]
		} else {
			var err error
			v, err = evalRawExpr(plan, raw, p.expr)
			if err != nil {
				return nil, err
			}
		}
		out[p.key] = v
		if p.altKey != "" {
			out[p.altKey] = v
		}
	}
	return out, nil
}

func evalRawExpr(plan *simpleSelectPlan, raw []any, e Expr) (any, error) {
	switch ex := e.(type) {
	case *Literal:
		return ex.Val, nil
	case *VarRef:
		key := ex.Lower
		if key == "" {
			key = strings.ToLower(ex.Name)
		}
		i, ok := plan.colIndex[key]
		if !ok {
			return nil, unknownColumnErr(ex.Name, columnSuggestion(ex.Name, plan.colIndex))
		}
		if i < 0 || i >= len(raw) {
			return nil, fmt.Errorf("column %q is out of range", ex.Name)
		}
		return raw[i], nil
	case *IsNull:
		v, err := evalRawExpr(plan, raw, ex.Expr)
		if err != nil {
			return nil, err
		}
		is := isNull(v)
		if ex.Negate {
			return !is, nil
		}
		return is, nil
	case *Unary:
		return evalRawUnary(plan, raw, ex)
	case *Binary:
		return evalRawBinary(plan, raw, ex)
	case *LikeExpr:
		return evalRawLike(plan, raw, ex)
	case *InExpr:
		return evalRawIn(plan, raw, ex)
	case *RegexpExpr:
		return evalRawRegexp(plan, raw, ex)
	case *BetweenExpr:
		return evalRawBetween(plan, raw, ex)
	case *FuncCall:
		return evalRawFuncCall(plan, raw, ex)
	default:
		return nil, fmt.Errorf("unsupported fast-path expression %T", e)
	}
}

// evalRawBetween evaluates BETWEEN in the raw fast path with a single
// evaluation of the comparand.
func evalRawBetween(plan *simpleSelectPlan, raw []any, ex *BetweenExpr) (any, error) {
	v, err := evalRawExpr(plan, raw, ex.Expr)
	if err != nil {
		return nil, err
	}
	lo, err := evalRawExpr(plan, raw, ex.Lo)
	if err != nil {
		return nil, err
	}
	hi, err := evalRawExpr(plan, raw, ex.Hi)
	if err != nil {
		return nil, err
	}
	return betweenResult(v, lo, hi, ex.Negate)
}

// rawCallScratch holds the reusable argument wrappers for one
// evalRawFuncCall invocation. The raw fast path evaluates function calls
// once per row; allocating the args slice, one Literal per argument, the
// FuncCall copy (which escapes through the map-dispatched handler) and an
// empty Row map on every row made the allocator — not the function body —
// the dominant cost of expression-heavy scans (e.g. per-row
// VEC_COSINE_SIMILARITY in RAG queries). ROW_TO_TEXT is handled separately
// because it intentionally reads every ambient row value. Other handlers
// only read the wrappers to extract argument values and never retain them,
// so recycling the backing structs through a pool is safe; nested calls
// simply draw their own scratch instance.
type rawCallScratch struct {
	call FuncCall
	args []Expr
	lits []Literal
}

var rawCallScratchPool = sync.Pool{
	New: func() any { return new(rawCallScratch) },
}

// rawEmptyRow is the shared no-columns row passed to handlers on the raw
// fast path. All arguments arrive pre-evaluated as literals, so handlers
// never look anything up in it (ROW_TO_TEXT, which reads the ambient row,
// is excluded from this path by isSimpleRawExpr).
var rawEmptyRow = Row{}

func evalRawFuncCall(plan *simpleSelectPlan, raw []any, ex *FuncCall) (any, error) {
	if ex.Over != nil {
		return nil, fmt.Errorf("window function %s is not supported in raw expression evaluation", ex.Name)
	}
	if ex.Name == "ROW_TO_TEXT" {
		return evalRawRowToText(plan, raw, ex)
	}
	sc := rawCallScratchPool.Get().(*rawCallScratch)
	defer rawCallScratchPool.Put(sc)
	if cap(sc.args) < len(ex.Args) {
		sc.args = make([]Expr, len(ex.Args))
		sc.lits = make([]Literal, len(ex.Args))
	}
	args := sc.args[:len(ex.Args)]
	lits := sc.lits[:len(ex.Args)]
	for i, arg := range ex.Args {
		if lit, ok := arg.(*Literal); ok {
			args[i] = lit
			continue
		}
		v, err := evalRawExpr(plan, raw, arg)
		if err != nil {
			return nil, err
		}
		lits[i].Val = v
		args[i] = &lits[i]
	}
	sc.call = FuncCall{Name: ex.Name, Args: args, Star: ex.Star, Distinct: ex.Distinct}
	if h := boundFuncHandler(ex); h != nil {
		// Handler resolved at parse time; call it directly instead of going
		// back through evalFuncCall's registry dispatch. ex.Over was already
		// rejected above, so this cannot bypass the window-function check.
		return h(ExecEnv{}, &sc.call, rawEmptyRow)
	}
	return evalFuncCall(ExecEnv{}, &sc.call, rawEmptyRow)
}

// evalRawRowToText is the raw-row equivalent of evalRowToTextFunc. The
// public function orders unqualified Row-map keys alphabetically, so the
// plan caches the corresponding raw indexes in that same order. This keeps
// whole-row LIKE searches correct without allocating the qualified and
// unqualified map entries that rowsFromTable normally needs.
func evalRawRowToText(plan *simpleSelectPlan, raw []any, ex *FuncCall) (any, error) {
	if len(ex.Args) > 0 {
		return nil, fmt.Errorf("ROW_TO_TEXT expects no arguments")
	}
	return rawRowToText(plan.rowTextColumns(), raw), nil
}

// rawRowToText concatenates the given raw column positions (already in
// ROW_TO_TEXT's sorted-unqualified-name order) into one space-separated
// string. Factored out of evalRawRowToText so buildRawRowToTextAndFilter can
// build the same string once per row and reuse it across every ROW_TO_TEXT()
// LIKE term in an AND chain, instead of each term rebuilding it independently.
func rawRowToText(cols []int, raw []any) string {
	var sb strings.Builder
	for _, col := range cols {
		if col < 0 || col >= len(raw) || raw[col] == nil {
			continue
		}
		if sb.Len() > 0 {
			sb.WriteByte(' ')
		}
		ftsWriteValue(&sb, raw[col])
	}
	return sb.String()
}

func evalRawUnary(plan *simpleSelectPlan, raw []any, ex *Unary) (any, error) {
	v, err := evalRawExpr(plan, raw, ex.Expr)
	if err != nil {
		return nil, err
	}
	return applyUnaryOp(ex.Op, v)
}

// applyUnaryOp applies a unary +/-/NOT operator to an already-evaluated
// operand. Factored out of evalRawUnary so evalJoinRawUnary
// (exec_fastpath_join.go) can reach the same switch directly instead of
// wrapping its operand in a synthetic &simpleSelectPlan{}/&Unary{}/&Literal{}
// trio purely to call evalRawUnary — three heap allocations per row, on a
// join fast path whose whole purpose is avoiding per-row allocation.
func applyUnaryOp(op string, v any) (any, error) {
	switch op {
	case "+":
		if f, ok := numeric(v); ok {
			return f, nil
		}
		if v == nil {
			return nil, nil
		}
		return nil, fmt.Errorf("unary + non-numeric")
	case "-":
		if f, ok := numeric(v); ok {
			return -f, nil
		}
		if v == nil {
			return nil, nil
		}
		return nil, fmt.Errorf("unary - non-numeric")
	case "NOT":
		return triToValue(triNot(toTri(v))), nil
	default:
		return nil, fmt.Errorf("unknown unary operator: %s", op)
	}
}

func evalRawBinary(plan *simpleSelectPlan, raw []any, ex *Binary) (any, error) {
	if ex.Op == "AND" || ex.Op == "OR" {
		lv, err := evalRawExpr(plan, raw, ex.Left)
		if err != nil {
			return nil, err
		}
		if ex.Op == "AND" && toTri(lv) == tvFalse {
			return false, nil
		}
		if ex.Op == "OR" && toTri(lv) == tvTrue {
			return true, nil
		}
		rv, err := evalRawExpr(plan, raw, ex.Right)
		if err != nil {
			return nil, err
		}
		if ex.Op == "AND" {
			return triToValue(triAnd(toTri(lv), toTri(rv))), nil
		}
		return triToValue(triOr(toTri(lv), toTri(rv))), nil
	}

	lv, err := evalRawExpr(plan, raw, ex.Left)
	if err != nil {
		return nil, err
	}
	rv, err := evalRawExpr(plan, raw, ex.Right)
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

// evalRawLike evaluates a LIKE / NOT LIKE expression in the fast raw path.
// Both the subject and the pattern must be simple raw-path expressions.
func evalRawLike(plan *simpleSelectPlan, raw []any, ex *LikeExpr) (any, error) {
	val, err := evalRawExpr(plan, raw, ex.Expr)
	if err != nil {
		return nil, err
	}
	patVal, err := evalRawExpr(plan, raw, ex.Pattern)
	if err != nil {
		return nil, err
	}
	if val == nil || patVal == nil {
		// SQL three-valued logic: a NULL operand makes the predicate
		// unknown, not false — returning false here would be silently
		// wrong once this result is negated by an enclosing NOT (see
		// evalRawUnary's NOT case, which relies on toTri/triNot).
		return nil, nil
	}
	str := valueText(val)
	pattern := valueText(patVal)
	var matched bool
	if ex.GlobStyle {
		if ex.CaseInsensitive {
			matched = matchGlobPattern(strings.ToLower(str), strings.ToLower(pattern))
		} else {
			matched = matchGlobPattern(str, pattern)
		}
	} else {
		escapeChar := '\\'
		if ex.Escape != nil {
			escVal, err := evalRawExpr(plan, raw, ex.Escape)
			if err != nil {
				return nil, err
			}
			if escStr, ok := escVal.(string); ok && len(escStr) == 1 {
				escapeChar = rune(escStr[0])
			}
		}
		if ex.Escape == nil {
			matched = compileCachedLikeMatcher(pattern, ex.CaseInsensitive)(str)
		} else if ex.CaseInsensitive {
			matched = matchLikePattern(strings.ToLower(str), strings.ToLower(pattern), escapeChar)
		} else {
			matched = matchLikePattern(str, pattern, escapeChar)
		}
	}
	if ex.Negate {
		return !matched, nil
	}
	return matched, nil
}

// evalRawRegexp evaluates REGEXP / RLIKE / SIMILAR TO in the raw fast path.
func evalRawRegexp(plan *simpleSelectPlan, raw []any, ex *RegexpExpr) (any, error) {
	val, err := evalRawExpr(plan, raw, ex.Expr)
	if err != nil {
		return nil, err
	}
	patVal, err := evalRawExpr(plan, raw, ex.Pattern)
	if err != nil {
		return nil, err
	}
	if val == nil || patVal == nil {
		// See the matching comment in evalRawLike: NULL makes this unknown,
		// not false, so an enclosing NOT stays excluded rather than flipping.
		return nil, nil
	}
	str := valueText(val)
	pattern := valueText(patVal)
	if ex.SimilarTo {
		pattern = similarToRegexp(pattern)
	}
	re, err := compileCachedRegexp(pattern)
	if err != nil {
		return nil, fmt.Errorf("REGEXP: invalid pattern %q: %v", pattern, err)
	}
	matched := re.MatchString(str)
	if ex.Negate {
		return !matched, nil
	}
	return matched, nil
}

// evalRawIn evaluates an IN / NOT IN expression in the raw fast path.
func evalRawIn(plan *simpleSelectPlan, raw []any, ex *InExpr) (any, error) {
	val, err := evalRawExpr(plan, raw, ex.Expr)
	if err != nil {
		return nil, err
	}
	if val == nil {
		// SQL three-valued logic: NULL IN (...) and NULL NOT IN (...) are
		// both unknown, not a definite true/false — critically, this must
		// be decided before rawEqual, whose nil-vs-nil branch returns true
		// (a Go-equality convention used elsewhere for grouping/dedup, not
		// SQL equality, where NULL never equals NULL).
		return nil, nil
	}
	for _, valExpr := range ex.Values {
		listVal, err := evalRawExpr(plan, raw, valExpr)
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
