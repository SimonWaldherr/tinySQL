// The general expression evaluator: recursive evaluation over literals,
// column references, unary and binary operators, IS NULL, BETWEEN, IN, LIKE,
// GLOB, SIMILAR TO, CASE, EXISTS and subqueries, against a row map.
package engine

import (
	"errors"
	"fmt"
	"math"
	"math/big"
	"strings"
	"unicode/utf8"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func evalExpr(env ExecEnv, e Expr, row Row) (any, error) {
	// Context cancellation is checked at row-level loop boundaries
	// (applyWhereClause, processNonAggregateQuery, UPDATE/DELETE loops, etc.),
	// not per expression node. This avoids O(nodes_per_row) channel selects.
	switch ex := e.(type) {
	case *Literal:
		return ex.Val, nil
	case *VarRef:
		return evalVarRef(env, ex, row)
	case *IsNull:
		return evalIsNull(env, ex, row)
	case *Unary:
		return evalUnary(env, ex, row)
	case *Binary:
		return evalBinary(env, ex, row)
	case *FuncCall:
		return evalFuncCall(env, ex, row)
	case *InExpr:
		return evalIn(env, ex, row)
	case *LikeExpr:
		return evalLike(env, ex, row)
	case *RegexpExpr:
		return evalRegexpExpr(env, ex, row)
	case *BetweenExpr:
		return evalBetween(env, ex, row)
	case *ExistsExpr:
		return evalExistsExpr(env, ex)
	case *CaseExpr:
		return evalCaseExpr(env, ex, row)
	case *SubqueryExpr:
		return evalSubqueryExpr(env, ex)
	}
	return nil, fmt.Errorf("unknown expression")
}

// evalBetween evaluates "expr [NOT] BETWEEN lo AND hi" with a single
// evaluation of expr, using the same three-valued comparison semantics as
// the desugared AND/OR form.
func evalBetween(env ExecEnv, ex *BetweenExpr, row Row) (any, error) {
	v, err := evalExpr(env, ex.Expr, row)
	if err != nil {
		return nil, err
	}
	lo, err := evalExpr(env, ex.Lo, row)
	if err != nil {
		return nil, err
	}
	hi, err := evalExpr(env, ex.Hi, row)
	if err != nil {
		return nil, err
	}
	return betweenResult(v, lo, hi, ex.Negate)
}

// betweenResult combines the boundary comparisons exactly like the desugared
// forms: BETWEEN → (v >= lo AND v <= hi), NOT BETWEEN → (v < lo OR v > hi).
func betweenResult(v, lo, hi any, negate bool) (any, error) {
	// Common homogeneous values need neither coercion nor tri-state dispatch.
	switch x := v.(type) {
	case int:
		l, lok := lo.(int)
		h, hok := hi.(int)
		if lok && hok {
			return (x >= l && x <= h) != negate, nil
		}
	case int64:
		l, lok := lo.(int64)
		h, hok := hi.(int64)
		if lok && hok {
			return (x >= l && x <= h) != negate, nil
		}
	case float64:
		l, lok := lo.(float64)
		h, hok := hi.(float64)
		if lok && hok && !math.IsNaN(x) && !math.IsNaN(l) && !math.IsNaN(h) {
			return (x >= l && x <= h) != negate, nil
		}
	}

	if negate {
		lt, err := evalComparisonBinary("<", v, lo)
		if err != nil {
			return nil, err
		}
		gt, err := evalComparisonBinary(">", v, hi)
		if err != nil {
			return nil, err
		}
		return triToValue(triOr(toTri(lt), toTri(gt))), nil
	}
	ge, err := evalComparisonBinary(">=", v, lo)
	if err != nil {
		return nil, err
	}
	le, err := evalComparisonBinary("<=", v, hi)
	if err != nil {
		return nil, err
	}
	return triToValue(triAnd(toTri(ge), toTri(le))), nil
}

func evalVarRef(env ExecEnv, ex *VarRef, row Row) (any, error) {
	if ex.Lower != "" {
		if v, ok := getValLower(row, ex.Lower); ok {
			return v, nil
		}
	} else if v, ok := getVal(row, ex.Name); ok {
		return v, nil
	}
	// Trigger bodies reference NEW.col/OLD.col, which aren't part of the row
	// being built by the statement the trigger body itself is executing (an
	// INSERT into a different table, say) — env.triggerRow carries them
	// separately. See executeTrigger and triggerRowBinding in triggers.go.
	if env.triggerRow != nil {
		lower := ex.Lower
		if lower == "" {
			lower = strings.ToLower(ex.Name)
		}
		if v, ok := lookupTriggerRow(env.triggerRow, lower); ok {
			return v, nil
		}
	}
	var suggestion string
	if env.triggerRow != nil {
		suggestion = columnSuggestionFromRow(ex.Name, row, triggerRowSuggestionRow(env.triggerRow))
	} else {
		suggestion = columnSuggestionFromRow(ex.Name, row)
	}
	return nil, unknownColumnErr(ex.Name, suggestion)
}

func evalIsNull(env ExecEnv, ex *IsNull, row Row) (any, error) {
	v, err := evalExpr(env, ex.Expr, row)
	if err != nil {
		return nil, err
	}
	is := isNull(v)
	if ex.Negate {
		return !is, nil
	}
	return is, nil
}

func evalIn(env ExecEnv, ex *InExpr, row Row) (any, error) {
	val, err := evalExpr(env, ex.Expr, row)
	if err != nil {
		return nil, err
	}
	if val == nil {
		// SQL three-valued logic: NULL IN (...) and NULL NOT IN (...) are
		// both unknown, not a definite true/false.
		return nil, nil
	}

	// Check against each value in the list
	hasNull := false
	for _, valExpr := range ex.Values {
		listVal, err := evalExpr(env, valExpr, row)
		if err != nil {
			return nil, err
		}

		if listVal == nil {
			hasNull = true
			continue
		}
		// Compare values
		cmp, err := compare(val, listVal)
		if err == nil && cmp == 0 {
			// Found a match
			if ex.Negate {
				return false, nil
			}
			return true, nil
		}
	}

	// No match found
	if hasNull {
		return nil, nil
	}
	if ex.Negate {
		return true, nil
	}
	return false, nil
}

func evalLike(env ExecEnv, ex *LikeExpr, row Row) (any, error) {
	val, err := evalExpr(env, ex.Expr, row)
	if err != nil {
		return nil, err
	}

	patternVal, err := evalExpr(env, ex.Pattern, row)
	if err != nil {
		return nil, err
	}

	// SQL three-valued logic: NULL LIKE ... and ... LIKE NULL are unknown,
	// not false (previously nil was stringified to "<nil>" and could match
	// '%'; returning a definite false here is also wrong once this result
	// is negated by an enclosing NOT — see evalUnary's NOT case).
	if val == nil || patternVal == nil {
		return nil, nil
	}

	// Convert to strings
	str := valueText(val)
	pattern := valueText(patternVal)

	var matched bool
	if ex.GlobStyle {
		// GLOB: case-sensitive, * matches any sequence, ? matches one char
		if ex.CaseInsensitive {
			matched = matchGlobPattern(strings.ToLower(str), strings.ToLower(pattern))
		} else {
			matched = matchGlobPattern(str, pattern)
		}
	} else {
		// LIKE / ILIKE: get optional escape character
		escapeChar := '\\'
		if ex.Escape != nil {
			escapeVal, err := evalExpr(env, ex.Escape, row)
			if err != nil {
				return nil, err
			}
			escapeStr, ok := escapeVal.(string)
			if !ok || len(escapeStr) != 1 {
				return nil, fmt.Errorf("ESCAPE must be a single character")
			}
			escapeChar = rune(escapeStr[0])
		}
		if ex.Escape == nil {
			// Default backslash escape is what compileLikeStringMatcher
			// supports (see its doc comment) and the common case: route
			// through the cached, shape-detected matcher instead of
			// re-lowercasing and re-running the general backtracking
			// matcher on every row.
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

// evalRegexpExpr evaluates REGEXP / RLIKE / SIMILAR TO predicates.
func evalRegexpExpr(env ExecEnv, ex *RegexpExpr, row Row) (any, error) {
	val, err := evalExpr(env, ex.Expr, row)
	if err != nil {
		return nil, err
	}
	patternVal, err := evalExpr(env, ex.Pattern, row)
	if err != nil {
		return nil, err
	}
	if val == nil || patternVal == nil {
		// See the matching comment in evalLike: NULL makes this unknown,
		// not false, so an enclosing NOT stays excluded rather than flipping.
		return nil, nil
	}
	str := valueText(val)
	pattern := valueText(patternVal)
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

// evalExistsExpr evaluates EXISTS (subquery). See evalCachedSubquery for how
// repeated evaluations of the same *ExistsExpr node within one statement
// execution avoid re-running the subquery when it is safe to do so.
func evalExistsExpr(env ExecEnv, ex *ExistsExpr) (any, error) {
	rs, err := evalCachedSubquery(env, ex, ex.Select)
	if err != nil {
		return nil, err
	}
	return rs != nil && len(rs.Rows) > 0, nil
}

// matchLikePattern matches a string against a SQL LIKE pattern.
// % matches zero or more characters, _ matches exactly one character.
//
// The matcher is rune-aware: _ consumes one Unicode code point, not one byte,
// so multi-byte characters (é, 日, …) match _ correctly. Wildcard backtracking
// uses the classic two-pointer greedy algorithm (O(len(str)*len(pattern))
// worst case, linear for typical patterns, zero allocations).
func matchLikePattern(str, pattern string, escape rune) bool {
	sIdx, pIdx := 0, 0
	sLen, pLen := len(str), len(pattern)
	star, match := -1, 0

	for sIdx < sLen {
		if pIdx < pLen {
			pChar, pw := utf8.DecodeRuneInString(pattern[pIdx:])

			switch {
			case pChar == escape && pIdx+pw < pLen:
				// Escaped character matches literally.
				lChar, lw := utf8.DecodeRuneInString(pattern[pIdx+pw:])
				sChar, sw := utf8.DecodeRuneInString(str[sIdx:])
				if sChar == lChar {
					sIdx += sw
					pIdx += pw + lw
					continue
				}
				// Mismatch: fall through to % backtracking (a bare
				// "return false" here would wrongly reject e.g.
				// 'a_b' LIKE '%\_%' at the first position).
			case pChar == '%':
				star = pIdx
				match = sIdx
				pIdx += pw
				continue
			default:
				sChar, sw := utf8.DecodeRuneInString(str[sIdx:])
				if pChar == '_' || sChar == pChar {
					sIdx += sw
					pIdx += pw
					continue
				}
			}
		}

		// No match, backtrack to last % and consume one more source rune.
		if star != -1 {
			pIdx = star + 1 // '%' is a single byte
			_, mw := utf8.DecodeRuneInString(str[match:])
			match += mw
			sIdx = match
			continue
		}
		return false
	}

	// Consume remaining % in pattern
	for pIdx < pLen && pattern[pIdx] == '%' {
		pIdx++
	}

	return pIdx == pLen
}

// matchGlobPattern matches a string against a GLOB pattern.
// * matches zero or more characters, ? matches exactly one character (one
// Unicode code point). Unlike LIKE, GLOB is case-sensitive by default
// (callers may lowercase both strings for case-insensitive behaviour).
func matchGlobPattern(str, pattern string) bool {
	sIdx, pIdx := 0, 0
	sLen, pLen := len(str), len(pattern)
	star, match := -1, 0

	for sIdx < sLen {
		if pIdx < pLen {
			pChar, pw := utf8.DecodeRuneInString(pattern[pIdx:])
			if pChar == '*' {
				star = pIdx
				match = sIdx
				pIdx += pw
				continue
			}
			sChar, sw := utf8.DecodeRuneInString(str[sIdx:])
			if pChar == '?' || sChar == pChar {
				sIdx += sw
				pIdx += pw
				continue
			}
		}
		if star != -1 {
			pIdx = star + 1 // '*' is a single byte
			_, mw := utf8.DecodeRuneInString(str[match:])
			match += mw
			sIdx = match
			continue
		}
		return false
	}
	for pIdx < pLen && pattern[pIdx] == '*' {
		pIdx++
	}
	return pIdx == pLen
}

// similarToRegexp converts a SQL SIMILAR TO pattern to a Go regexp pattern.
// Rules:
//   - % matches any sequence of characters (like .* in regex)
//   - _ matches any single character (like . in regex)
//   - | * + ? ( ) [ ] { } \ work as standard regex metacharacters
//   - The match is anchored (whole string must match)
func similarToRegexp(pattern string) string {
	var b strings.Builder
	b.WriteString("^")
	for i := 0; i < len(pattern); i++ {
		c := pattern[i]
		switch c {
		case '%':
			b.WriteString(".*")
		case '_':
			b.WriteByte('.')
		case '|', '*', '+', '?', '(', ')', '[', ']', '{', '}', '\\':
			// Standard regex metacharacters – pass through as-is
			b.WriteByte(c)
		case '.', '^', '$':
			// Anchor/any-char metacharacters in regex that are literal in SIMILAR TO
			b.WriteByte('\\')
			b.WriteByte(c)
		default:
			b.WriteByte(c)
		}
	}
	b.WriteString("$")
	return b.String()
}

func evalCaseExpr(env ExecEnv, ex *CaseExpr, row Row) (any, error) {
	if ex.Operand != nil {
		target, err := evalExpr(env, ex.Operand, row)
		if err != nil {
			return nil, err
		}
		for _, w := range ex.Whens {
			whenVal, err := evalExpr(env, w.When, row)
			if err != nil {
				return nil, err
			}
			if cmp, err := compare(target, whenVal); err == nil && cmp == 0 {
				return evalExpr(env, w.Then, row)
			}
		}
	} else {
		for _, w := range ex.Whens {
			cond, err := evalExpr(env, w.When, row)
			if err != nil {
				return nil, err
			}
			if toTri(cond) == tvTrue {
				return evalExpr(env, w.Then, row)
			}
		}
	}
	if ex.Else != nil {
		return evalExpr(env, ex.Else, row)
	}
	return nil, nil
}

func evalSubqueryExpr(env ExecEnv, ex *SubqueryExpr) (any, error) {
	// Also the path evalIn (evalIn's IN (SELECT ...) form, eval_expr.go)
	// reaches through evalExpr's generic *SubqueryExpr dispatch: ex.Values
	// holds a single *SubqueryExpr there, so evalExpr calls this function for
	// that case too, and this cache lookup covers it for free. See
	// evalCachedSubquery for how repeated evaluations of the same
	// *SubqueryExpr node within one statement execution avoid re-running the
	// subquery when it is safe to do so.
	rs, err := evalCachedSubquery(env, ex, ex.Select)
	if err != nil {
		return nil, err
	}
	if rs == nil || len(rs.Rows) == 0 {
		return nil, nil
	}
	if len(rs.Rows) > 1 {
		return nil, fmt.Errorf("scalar subquery returned %d rows", len(rs.Rows))
	}
	row := rs.Rows[0]
	if len(rs.Cols) == 1 {
		if v, ok := getValLower(row, strings.ToLower(rs.Cols[0])); ok {
			return v, nil
		}
	}
	if len(row) == 1 {
		for _, v := range row {
			return v, nil
		}
	}
	for _, col := range rs.Cols {
		if v, ok := getValLower(row, strings.ToLower(col)); ok {
			return v, nil
		}
	}
	for _, v := range row {
		return v, nil
	}
	return nil, nil
}

func evalUnary(env ExecEnv, ex *Unary, row Row) (any, error) {
	v, err := evalExpr(env, ex.Expr, row)
	if err != nil {
		return nil, err
	}
	switch ex.Op {
	case "+":
		if f, ok := numeric(v); ok {
			return f, nil
		}
		if r, ok := storage.DecimalFromAny(v); ok {
			return new(big.Rat).Set(r), nil
		}
		if v == nil {
			return nil, nil
		}
		return nil, fmt.Errorf("unary + non-numeric")
	case "-":
		if f, ok := numeric(v); ok {
			return -f, nil
		}
		if r, ok := storage.DecimalFromAny(v); ok {
			neg := new(big.Rat).Set(r)
			neg.Mul(neg, big.NewRat(-1, 1))
			return neg, nil
		}
		if v == nil {
			return nil, nil
		}
		return nil, fmt.Errorf("unary - non-numeric")
	case "NOT":
		return triToValue(triNot(toTri(v))), nil
	}
	return nil, fmt.Errorf("unknown unary operator: %s", ex.Op)
}

func evalBinary(env ExecEnv, ex *Binary, row Row) (any, error) {
	if ex.Op == "AND" || ex.Op == "OR" {
		return evalLogicalBinary(env, ex, row)
	}

	lv, err := evalExpr(env, ex.Left, row)
	if err != nil {
		return nil, err
	}
	rv, err := evalExpr(env, ex.Right, row)
	if err != nil {
		return nil, err
	}

	switch ex.Op {
	case "+", "-", "*", "/", "%":
		return evalArithmeticBinary(ex.Op, lv, rv)
	case "||":
		return evalConcatOperator(lv, rv), nil
	case "=", "!=", "<>", "<", "<=", ">", ">=":
		return evalComparisonBinary(ex.Op, lv, rv)
	}
	return nil, fmt.Errorf("unknown binary operator: %s", ex.Op)
}

// evalConcatOperator implements ||.
//
// It deliberately does NOT reuse the CONCAT builtin (evalConcat), because the
// two disagree about NULL and both are right: SQLite's || yields NULL when
// either operand is NULL, while SQLite's CONCAT() function (3.44+, which
// evalConcat matches) skips NULL arguments. Routing || through CONCAT would
// turn `first_name || middle_name || last_name` from "NULL until every part is
// present" into a silently gap-filled string — the kind of wrong answer that
// only shows up in production data.
//
// Non-text operands are coerced to text with stringifySQLValue rather than
// valueText because it renders []byte as its bytes (SQLite likewise casts a
// BLOB to text for ||), where valueText would produce the Go decimal byte
// list "[65 66]". Both operands are non-nil here, so its nil case is unused.
func evalConcatOperator(lv, rv any) any {
	if lv == nil || rv == nil {
		return nil
	}
	return stringifySQLValue(lv) + stringifySQLValue(rv)
}

func evalLogicalBinary(env ExecEnv, ex *Binary, row Row) (any, error) {
	lv, err := evalExpr(env, ex.Left, row)
	if err != nil {
		return nil, err
	}
	if ex.Op == "AND" && toTri(lv) == tvFalse {
		return false, nil
	}
	if ex.Op == "OR" && toTri(lv) == tvTrue {
		return true, nil
	}
	rv, err := evalExpr(env, ex.Right, row)
	if err != nil {
		return nil, err
	}
	if ex.Op == "AND" {
		return triToValue(triAnd(toTri(lv), toTri(rv))), nil
	}
	return triToValue(triOr(toTri(lv), toTri(rv))), nil
}

func evalArithmeticBinary(op string, lv, rv any) (any, error) {
	if op == "+" {
		if isStringValue(lv) || isStringValue(rv) {
			return stringifySQLValue(lv) + stringifySQLValue(rv), nil
		}
		if lv == nil || rv == nil {
			return nil, nil
		}
	} else if lv == nil || rv == nil {
		return nil, nil
	}
	// If either operand is a decimal (big.Rat), perform high-precision arithmetic
	// Only treat values as decimals for high-precision arithmetic when they
	// are already rational types (i.e. *big.Rat or big.Rat). This preserves
	// existing numeric semantics for plain ints/floats.
	if la, lok := storage.AsBigRat(lv); lok {
		if rb, rok := storage.AsBigRat(rv); rok {
			a := new(big.Rat).Set(la)
			b := new(big.Rat).Set(rb)
			switch op {
			case "+":
				return new(big.Rat).Add(a, b), nil
			case "-":
				return new(big.Rat).Sub(a, b), nil
			case "*":
				return new(big.Rat).Mul(a, b), nil
			case "%":
				if b.Sign() == 0 {
					return nil, errors.New("division by zero")
				}
				quotient := new(big.Rat).Quo(a, b)
				integer := new(big.Int).Quo(quotient.Num(), quotient.Denom())
				return new(big.Rat).Sub(a, new(big.Rat).Mul(new(big.Rat).SetInt(integer), b)), nil
			case "/":
				if b.Sign() == 0 {
					return nil, errors.New("division by zero")
				}
				return new(big.Rat).Quo(a, b), nil
			}
		}
		return nil, fmt.Errorf("%s expects numeric", op)
	}

	lf, lok := numeric(lv)
	rf, rok := numeric(rv)
	if !lok || !rok {
		return nil, fmt.Errorf("%s expects numeric", op)
	}
	switch op {
	case "+":
		return lf + rf, nil
	case "-":
		return lf - rf, nil
	case "*":
		return lf * rf, nil
	case "%":
		if rf == 0 {
			return nil, errors.New("division by zero")
		}
		return math.Mod(lf, rf), nil
	case "/":
		if rf == 0 {
			return nil, errors.New("division by zero")
		}
		return lf / rf, nil
	}
	return nil, fmt.Errorf("unknown arithmetic operator: %s", op)
}

func evalComparisonBinary(op string, lv, rv any) (any, error) {
	if lv == nil || rv == nil {
		return nil, nil
	}
	cmp, err := compare(lv, rv)
	if err != nil {
		return nil, err
	}
	switch op {
	case "=":
		return cmp == 0, nil
	case "!=", "<>":
		return cmp != 0, nil
	case "<":
		return cmp < 0, nil
	case "<=":
		return cmp <= 0, nil
	case ">":
		return cmp > 0, nil
	case ">=":
		return cmp >= 0, nil
	}
	return nil, fmt.Errorf("unknown comparison operator: %s", op)
}
