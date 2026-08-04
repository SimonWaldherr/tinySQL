// String, JSON and conditional scalar functions: case conversion, trimming,
// padding, substring and search, formatting, CAST, COALESCE/NULLIF/IF, and the
// JSON accessors.
package engine

import (
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode"
)

func evalCoalesce(env ExecEnv, args []Expr, row Row) (any, error) {
	for _, a := range args {
		v, err := evalExpr(env, a, row)
		if err != nil {
			return nil, err
		}
		if v != nil {
			return v, nil
		}
	}
	return nil, nil
}

func evalNullif(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("NULLIF expects 2 args")
	}
	lv, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	rv, err := evalExpr(env, args[1], row)
	if err != nil {
		return nil, err
	}
	if lv == nil {
		return nil, nil
	}
	if rv == nil {
		return lv, nil
	}
	cmp, err := compare(lv, rv)
	if err != nil {
		return nil, err
	}
	if cmp == 0 {
		return nil, nil
	}
	return lv, nil
}

func evalJSONGet(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("JSON_GET expects (json, path)")
	}
	jv, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	pv, err := evalExpr(env, args[1], row)
	if err != nil {
		return nil, err
	}
	ps, _ := pv.(string)
	return jsonGet(jv, ps), nil
}

func evalJSONExtended(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	switch ex.Name {
	case "JSON_SET":
		if len(ex.Args) != 3 {
			return nil, fmt.Errorf("JSON_SET expects (json, path, value)")
		}
		jv, err := evalExpr(env, ex.Args[0], row)
		if err != nil {
			return nil, err
		}
		pv, err := evalExpr(env, ex.Args[1], row)
		if err != nil {
			return nil, err
		}
		val, err := evalExpr(env, ex.Args[2], row)
		if err != nil {
			return nil, err
		}
		ps, _ := pv.(string)
		return jsonSet(jv, ps, val), nil

	case "JSON_EXTRACT":
		// Alias for JSON_GET
		if len(ex.Args) != 2 {
			return nil, fmt.Errorf("JSON_EXTRACT expects (json, path)")
		}
		jv, err := evalExpr(env, ex.Args[0], row)
		if err != nil {
			return nil, err
		}
		pv, err := evalExpr(env, ex.Args[1], row)
		if err != nil {
			return nil, err
		}
		ps, _ := pv.(string)
		return jsonGet(jv, ps), nil
	}
	return nil, fmt.Errorf("unknown JSON function: %s", ex.Name)
}

func evalCountSingle(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if ex.Star {
		return 1, nil
	}
	if len(ex.Args) != 1 {
		return nil, fmt.Errorf("COUNT expects 1 arg")
	}
	v, err := evalExpr(env, ex.Args[0], row)
	if err != nil {
		return nil, err
	}
	if v == nil {
		return 0, nil
	}
	return 1, nil
}

func evalAggregateSingle(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if len(ex.Args) != 1 {
		return nil, fmt.Errorf("%s expects 1 arg", ex.Name)
	}
	v, err := evalExpr(env, ex.Args[0], row)
	if err != nil {
		return nil, err
	}
	return v, nil
}

func triToValue(t int) any {
	if t == tvTrue {
		return true
	}
	if t == tvFalse {
		return false
	}
	return nil
}

// String manipulation functions
// trimSide selects which side(s) a trim function removes characters from.
type trimSide uint8

const (
	trimLeft trimSide = 1 << iota
	trimRight
	trimBoth = trimLeft | trimRight
)

// evalTrimCommon implements TRIM/LTRIM/RTRIM(str [, cutset]).
// NULL input yields NULL; non-string inputs are coerced to their text form
// (SQLite/MySQL behaviour, e.g. LTRIM(123) = '123'); the default cutset is
// Unicode whitespace (unicode.IsSpace), consistent across all three functions.
func evalTrimCommon(env ExecEnv, name string, side trimSide, args []Expr, row Row) (any, error) {
	if len(args) < 1 || len(args) > 2 {
		return nil, fmt.Errorf("%s expects 1 or 2 arguments", name)
	}

	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, nil
	}
	str, ok := val.(string)
	if !ok {
		str = fmt.Sprintf("%v", val)
	}

	cutset := ""
	if len(args) == 2 {
		cutsetVal, err := evalExpr(env, args[1], row)
		if err != nil {
			return nil, err
		}
		if cutsetVal != nil {
			if cutsetStr, ok := cutsetVal.(string); ok {
				cutset = cutsetStr
			} else {
				return nil, fmt.Errorf("%s cutset must be a string", name)
			}
		}
	}

	if cutset == "" {
		// Default: Unicode-aware whitespace trimming.
		switch side {
		case trimLeft:
			return strings.TrimLeftFunc(str, unicode.IsSpace), nil
		case trimRight:
			return strings.TrimRightFunc(str, unicode.IsSpace), nil
		default:
			return strings.TrimSpace(str), nil
		}
	}
	switch side {
	case trimLeft:
		return strings.TrimLeft(str, cutset), nil
	case trimRight:
		return strings.TrimRight(str, cutset), nil
	default:
		return strings.Trim(str, cutset), nil
	}
}

func evalLTrim(env ExecEnv, args []Expr, row Row) (any, error) {
	return evalTrimCommon(env, "LTRIM", trimLeft, args, row)
}

func evalRTrim(env ExecEnv, args []Expr, row Row) (any, error) {
	return evalTrimCommon(env, "RTRIM", trimRight, args, row)
}

func evalTrim(env ExecEnv, args []Expr, row Row) (any, error) {
	return evalTrimCommon(env, "TRIM", trimBoth, args, row)
}

// ISNULL function - returns TRUE if argument is NULL, FALSE otherwise
func evalIsNullFunc(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("ISNULL expects 1 argument")
	}

	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}

	return val == nil, nil
}

func evalUpper(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("UPPER expects 1 argument")
	}

	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}

	str, ok := val.(string)
	if !ok {
		str = fmt.Sprintf("%v", val)
	}

	return strings.ToUpper(str), nil
}

func evalLower(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("LOWER expects 1 argument")
	}

	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}

	str, ok := val.(string)
	if !ok {
		str = fmt.Sprintf("%v", val)
	}

	return strings.ToLower(str), nil
}

func evalConcat(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) == 0 {
		return "", nil
	}

	var sb strings.Builder
	for _, arg := range args {
		val, err := evalExpr(env, arg, row)
		if err != nil {
			return nil, err
		}

		if val != nil {
			str, ok := val.(string)
			if !ok {
				str = fmt.Sprintf("%v", val)
			}
			sb.WriteString(str)
		}
	}

	return sb.String(), nil
}

func evalLength(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("LENGTH expects 1 argument")
	}

	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}

	str, ok := val.(string)
	if !ok {
		str = fmt.Sprintf("%v", val)
	}

	return len(str), nil
}

//nolint:gocyclo // SUBSTRING handling covers varying arity, coercion, and bounds checks.
func evalSubstring(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) < 2 || len(args) > 3 {
		return nil, fmt.Errorf("SUBSTRING expects 2 or 3 arguments")
	}

	// Get string value
	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, nil
	}

	str, ok := val.(string)
	if !ok {
		str = fmt.Sprintf("%v", val)
	}

	// Get start position (1-indexed in SQL)
	startVal, err := evalExpr(env, args[1], row)
	if err != nil {
		return nil, err
	}
	startAny, err := coerceToInt(startVal)
	if err != nil {
		return nil, fmt.Errorf("SUBSTRING start position must be numeric")
	}
	start, ok := startAny.(int)
	if !ok {
		return nil, fmt.Errorf("SUBSTRING start position must be an integer")
	}
	start = start - 1 // Convert to 0-indexed
	if start < 0 {
		start = 0
	}
	if start >= len(str) {
		return "", nil
	}

	// Get length if provided
	if len(args) == 3 {
		lengthVal, err := evalExpr(env, args[2], row)
		if err != nil {
			return nil, err
		}
		lengthAny, err := coerceToInt(lengthVal)
		if err != nil {
			return nil, fmt.Errorf("SUBSTRING length must be numeric")
		}
		length, ok := lengthAny.(int)
		if !ok {
			return nil, fmt.Errorf("SUBSTRING length must be an integer")
		}

		end := start + length
		if end > len(str) {
			end = len(str)
		}
		return str[start:end], nil
	}

	return str[start:], nil
}

func evalLeft(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("LEFT expects 2 arguments")
	}

	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, nil
	}

	str, ok := val.(string)
	if !ok {
		str = fmt.Sprintf("%v", val)
	}

	lenVal, err := evalExpr(env, args[1], row)
	if err != nil {
		return nil, err
	}
	lenAny, err := coerceToInt(lenVal)
	if err != nil {
		return nil, fmt.Errorf("LEFT length must be numeric")
	}
	length, ok := lenAny.(int)
	if !ok {
		return nil, fmt.Errorf("LEFT length must be an integer")
	}

	if length < 0 {
		return "", nil
	}
	if length > len(str) {
		return str, nil
	}
	return str[:length], nil
}

func evalRight(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("RIGHT expects 2 arguments")
	}

	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, nil
	}

	str, ok := val.(string)
	if !ok {
		str = fmt.Sprintf("%v", val)
	}

	lenVal, err := evalExpr(env, args[1], row)
	if err != nil {
		return nil, err
	}
	lenAny, err := coerceToInt(lenVal)
	if err != nil {
		return nil, fmt.Errorf("RIGHT length must be numeric")
	}
	length, ok := lenAny.(int)
	if !ok {
		return nil, fmt.Errorf("RIGHT length must be an integer")
	}

	if length < 0 {
		return "", nil
	}
	if length > len(str) {
		return str, nil
	}
	return str[len(str)-length:], nil
}

func evalReplace(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 3 {
		return nil, fmt.Errorf("REPLACE expects 3 arguments: (string, from, to)")
	}
	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, nil
	}
	str := fmt.Sprintf("%v", val)

	fromVal, err := evalExpr(env, args[1], row)
	if err != nil {
		return nil, err
	}
	from := fmt.Sprintf("%v", fromVal)

	toVal, err := evalExpr(env, args[2], row)
	if err != nil {
		return nil, err
	}
	to := fmt.Sprintf("%v", toVal)

	return strings.ReplaceAll(str, from, to), nil
}

func evalInstr(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("INSTR expects 2 arguments: (string, search)")
	}
	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	if val == nil {
		return 0, nil
	}
	str := fmt.Sprintf("%v", val)

	searchVal, err := evalExpr(env, args[1], row)
	if err != nil {
		return nil, err
	}
	search := fmt.Sprintf("%v", searchVal)

	idx := strings.Index(str, search)
	if idx == -1 {
		return 0, nil
	}
	return idx + 1, nil // 1-based index
}

func evalReverse(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("REVERSE expects 1 argument")
	}
	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, nil
	}
	str := fmt.Sprintf("%v", val)
	runes := []rune(str)
	for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
		runes[i], runes[j] = runes[j], runes[i]
	}
	return string(runes), nil
}

func evalRepeat(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("REPEAT expects 2 arguments: (string, count)")
	}
	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, nil
	}
	str := fmt.Sprintf("%v", val)

	countVal, err := evalExpr(env, args[1], row)
	if err != nil {
		return nil, err
	}
	countAny, err := coerceToInt(countVal)
	if err != nil {
		return nil, fmt.Errorf("REPEAT count must be numeric")
	}
	count, ok := countAny.(int)
	if !ok || count < 0 {
		return "", nil
	}
	return strings.Repeat(str, count), nil
}

func evalPrintf(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) < 1 {
		return nil, fmt.Errorf("PRINTF expects at least 1 argument")
	}
	formatVal, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	format := fmt.Sprintf("%v", formatVal)

	fmtArgs := make([]any, len(args)-1)
	for i := 1; i < len(args); i++ {
		v, err := evalExpr(env, args[i], row)
		if err != nil {
			return nil, err
		}
		fmtArgs[i-1] = v
	}
	return fmt.Sprintf(format, fmtArgs...), nil
}

func evalLpad(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) < 2 || len(args) > 3 {
		return nil, fmt.Errorf("LPAD expects 2-3 arguments: (string, length[, pad])")
	}
	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, nil
	}
	str := fmt.Sprintf("%v", val)

	lenVal, err := evalExpr(env, args[1], row)
	if err != nil {
		return nil, err
	}
	lenAny, err := coerceToInt(lenVal)
	if err != nil {
		return nil, fmt.Errorf("LPAD length must be numeric")
	}
	length, ok := lenAny.(int)
	if !ok || length < 0 {
		return str, nil
	}

	pad := " "
	if len(args) == 3 {
		padVal, err := evalExpr(env, args[2], row)
		if err != nil {
			return nil, err
		}
		pad = fmt.Sprintf("%v", padVal)
		if pad == "" {
			pad = " "
		}
	}

	if len(str) >= length {
		return str[:length], nil
	}
	needed := length - len(str)
	padding := strings.Repeat(pad, (needed/len(pad))+1)[:needed]
	return padding + str, nil
}

func evalRpad(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) < 2 || len(args) > 3 {
		return nil, fmt.Errorf("RPAD expects 2-3 arguments: (string, length[, pad])")
	}
	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, nil
	}
	str := fmt.Sprintf("%v", val)

	lenVal, err := evalExpr(env, args[1], row)
	if err != nil {
		return nil, err
	}
	lenAny, err := coerceToInt(lenVal)
	if err != nil {
		return nil, fmt.Errorf("RPAD length must be numeric")
	}
	length, ok := lenAny.(int)
	if !ok || length < 0 {
		return str, nil
	}

	pad := " "
	if len(args) == 3 {
		padVal, err := evalExpr(env, args[2], row)
		if err != nil {
			return nil, err
		}
		pad = fmt.Sprintf("%v", padVal)
		if pad == "" {
			pad = " "
		}
	}

	if len(str) >= length {
		return str[:length], nil
	}
	needed := length - len(str)
	padding := strings.Repeat(pad, (needed/len(pad))+1)[:needed]
	return str + padding, nil
}

func evalIf(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 3 {
		return nil, fmt.Errorf("IF expects 3 arguments: (condition, true_value, false_value)")
	}
	cond, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	// Check if condition is truthy
	isTrue := false
	switch v := cond.(type) {
	case bool:
		isTrue = v
	case int:
		isTrue = v != 0
	case float64:
		isTrue = v != 0
	case string:
		isTrue = v != "" && v != "0" && strings.ToLower(v) != "false"
	default:
		isTrue = cond != nil
	}
	if isTrue {
		return evalExpr(env, args[1], row)
	}
	return evalExpr(env, args[2], row)
}

func evalSpace(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("SPACE expects 1 argument")
	}
	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	n, ok := numeric(val)
	if !ok {
		return nil, fmt.Errorf("SPACE: argument must be numeric")
	}
	if n < 0 {
		return "", nil
	}
	return strings.Repeat(" ", int(n)), nil
}

func evalAscii(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("ASCII expects 1 argument")
	}
	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	s := fmt.Sprintf("%v", val)
	if len(s) == 0 {
		return 0, nil
	}
	return int(s[0]), nil
}

func evalChar(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("CHAR expects 1 argument")
	}
	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	n, ok := numeric(val)
	if !ok {
		return nil, fmt.Errorf("CHAR: argument must be numeric")
	}
	if n < 0 || n > 127 {
		return "", nil
	}
	return string(rune(int(n))), nil
}

func evalInitcap(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("INITCAP expects 1 argument")
	}
	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	s := fmt.Sprintf("%v", val)
	words := strings.Fields(s)
	for i, word := range words {
		if len(word) > 0 {
			words[i] = strings.ToUpper(string(word[0])) + strings.ToLower(word[1:])
		}
	}
	return strings.Join(words, " "), nil
}

func evalSplitPart(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 3 {
		return nil, fmt.Errorf("SPLIT_PART expects 3 arguments: (string, delimiter, part)")
	}
	strVal, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	delimVal, err := evalExpr(env, args[1], row)
	if err != nil {
		return nil, err
	}
	partVal, err := evalExpr(env, args[2], row)
	if err != nil {
		return nil, err
	}
	s := fmt.Sprintf("%v", strVal)
	delim := fmt.Sprintf("%v", delimVal)
	part, ok := numeric(partVal)
	if !ok {
		return nil, fmt.Errorf("SPLIT_PART: part must be numeric")
	}
	parts := strings.Split(s, delim)
	idx := int(part) - 1 // 1-indexed
	if idx < 0 || idx >= len(parts) {
		return "", nil
	}
	return parts[idx], nil
}

func evalQuote(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("QUOTE expects 1 argument")
	}
	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	if val == nil {
		return "NULL", nil
	}
	s := fmt.Sprintf("%v", val)
	escaped := strings.ReplaceAll(s, "'", "''")
	return "'" + escaped + "'", nil
}

func evalConcatWs(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) < 2 {
		return nil, fmt.Errorf("CONCAT_WS expects at least 2 arguments")
	}
	sepVal, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	sep := fmt.Sprintf("%v", sepVal)
	var parts []string
	for _, arg := range args[1:] {
		v, err := evalExpr(env, arg, row)
		if err != nil {
			return nil, err
		}
		if v != nil {
			parts = append(parts, fmt.Sprintf("%v", v))
		}
	}
	return strings.Join(parts, sep), nil
}

func evalPosition(env ExecEnv, args []Expr, row Row) (any, error) {
	// POSITION(substring IN string) - 1-indexed, returns 0 if not found
	if len(args) != 2 {
		return nil, fmt.Errorf("POSITION expects 2 arguments: (substring, string)")
	}
	subVal, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	strVal, err := evalExpr(env, args[1], row)
	if err != nil {
		return nil, err
	}
	sub := fmt.Sprintf("%v", subVal)
	str := fmt.Sprintf("%v", strVal)
	idx := strings.Index(str, sub)
	if idx < 0 {
		return 0, nil
	}
	return idx + 1, nil
}

func evalTypeof(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("TYPEOF expects 1 argument")
	}
	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	if val == nil {
		return "null", nil
	}
	switch val.(type) {
	case int, int8, int16, int32, int64:
		return "integer", nil
	case uint, uint8, uint16, uint32, uint64:
		return "integer", nil
	case float32, float64:
		return "real", nil
	case bool:
		return "boolean", nil
	case string:
		return "text", nil
	case time.Time:
		return "datetime", nil
	case []any, map[string]any:
		return "json", nil
	default:
		return fmt.Sprintf("%T", val), nil
	}
}

func evalCast(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("CAST expects 2 arguments")
	}

	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}

	typeExpr, ok := args[1].(*VarRef)
	if !ok {
		lit, ok := args[1].(*Literal)
		if !ok {
			return nil, fmt.Errorf("CAST type must be a type name")
		}
		typeStr, ok := lit.Val.(string)
		if !ok {
			return nil, fmt.Errorf("CAST type must be a string")
		}
		return castValue(val, strings.ToUpper(typeStr))
	}

	return castValue(val, strings.ToUpper(typeExpr.Name))
}

func castValue(val any, targetType string) (any, error) {
	if val == nil {
		return nil, nil
	}

	switch targetType {
	case "TEXT", "STRING", "VARCHAR", "CHAR":
		return fmt.Sprintf("%v", val), nil
	case "INT", "INTEGER":
		return coerceToInt(val)
	case "FLOAT", "REAL", "DOUBLE", "NUMERIC":
		if f, ok := numeric(val); ok {
			return f, nil
		}
		str := fmt.Sprintf("%v", val)
		return strconv.ParseFloat(str, 64)
	case "BOOL", "BOOLEAN":
		switch v := val.(type) {
		case bool:
			return v, nil
		case int:
			return v != 0, nil
		case float64:
			return v != 0, nil
		case string:
			return strings.ToLower(v) == "true" || v == "1", nil
		}
		return false, nil
	case "GEOMETRY", "GEOM":
		return coerceToGeometry(val)
	default:
		return nil, fmt.Errorf("unsupported cast type: %s", targetType)
	}
}
