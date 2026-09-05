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
	"unicode/utf8"
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
	str := valueText(val)

	cutset := ""
	if len(args) == 2 {
		cutsetVal, err := evalExpr(env, args[1], row)
		if err != nil {
			return nil, err
		}
		if cutsetVal == nil {
			return nil, nil
		}
		if cutsetStr, ok := cutsetVal.(string); ok {
			cutset = cutsetStr
		} else {
			return nil, fmt.Errorf("%s cutset must be a string", name)
		}
	}

	var trimmed string
	if cutset == "" {
		// Default: Unicode-aware whitespace trimming.
		switch side {
		case trimLeft:
			trimmed = strings.TrimLeftFunc(str, unicode.IsSpace)
		case trimRight:
			trimmed = strings.TrimRightFunc(str, unicode.IsSpace)
		default:
			trimmed = strings.TrimSpace(str)
		}
	} else {
		switch side {
		case trimLeft:
			trimmed = strings.TrimLeft(str, cutset)
		case trimRight:
			trimmed = strings.TrimRight(str, cutset)
		default:
			trimmed = strings.Trim(str, cutset)
		}
	}
	// Nothing trimmed from a string argument: hand back the argument's
	// interface value instead of re-boxing the identical string — one heap
	// allocation per row saved on the common no-op trim.
	if _, ok := val.(string); ok && len(trimmed) == len(str) {
		return val, nil
	}
	return trimmed, nil
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

	return caseStringValue(val, true), nil
}

func evalLower(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("LOWER expects 1 argument")
	}

	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}

	return caseStringValue(val, false), nil
}

// caseStringValue preserves the argument box when conversion changes nothing.
func caseStringValue(val any, upper bool) any {
	if val == nil {
		return nil
	}
	str := valueText(val)
	var converted string
	if upper {
		converted = strings.ToUpper(str)
	} else {
		converted = strings.ToLower(str)
	}
	if _, ok := val.(string); ok && converted == str {
		return val
	}
	return converted
}

// stringEdge only decodes the requested edge, rather than allocating every rune.
// Partial invalid UTF-8 retains the normalization of the former []rune path.
func stringEdge(val any, str string, n int, right bool) any {
	if n <= 0 {
		return ""
	}
	boundary := 0
	if right {
		boundary = len(str)
	}
	invalid := false
	for i := 0; i < n; i++ {
		if (!right && boundary == len(str)) || (right && boundary == 0) {
			break
		}
		var r rune
		var width int
		if right {
			r, width = utf8.DecodeLastRuneInString(str[:boundary])
			boundary -= width
		} else {
			r, width = utf8.DecodeRuneInString(str[boundary:])
			boundary += width
		}
		invalid = invalid || (r == utf8.RuneError && width == 1)
	}
	if (!right && boundary == len(str)) || (right && boundary == 0) {
		if _, ok := val.(string); ok {
			return val
		}
		return str
	}
	part := str[:boundary]
	if right {
		part = str[boundary:]
	}
	if invalid {
		return string([]rune(part))
	}
	// A tiny projected value should not retain a large source string.
	if len(str) > 4096 && len(part) < len(str)/4 {
		return strings.Clone(part)
	}
	return part
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
			sb.WriteString(valueText(val))
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

	str := valueText(val)

	return utf8.RuneCountInString(str), nil
}

// stringRunes returns str as characters rather than UTF-8 bytes. SQL string
// positions and lengths are character-based, and byte indexing can otherwise
// split a multi-byte character into invalid UTF-8.
func stringRunes(str string) []rune { return []rune(str) }

func stringIsASCII(str string) bool {
	for i := 0; i < len(str); i++ {
		if str[i] >= utf8.RuneSelf {
			return false
		}
	}
	return true
}

// stringCharCount returns the character count of str without materializing
// a []rune, taking the same ASCII fast path stringPrefix already uses so
// callers that only need a count (not the runes themselves) can avoid the
// allocation stringRunes(str) would otherwise force.
func stringCharCount(str string) int {
	if stringIsASCII(str) {
		return len(str)
	}
	return utf8.RuneCountInString(str)
}

func stringPrefix(str string, length int) string {
	if length <= 0 {
		return ""
	}
	if stringIsASCII(str) {
		if length >= len(str) {
			return str
		}
		return str[:length]
	}
	runes := stringRunes(str)
	if length >= len(runes) {
		return str
	}
	return string(runes[:length])
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

	str := valueText(val)

	// Get start position (1-indexed in SQL)
	startVal, err := evalExpr(env, args[1], row)
	if err != nil {
		return nil, err
	}
	if startVal == nil {
		return nil, nil
	}
	startAny, err := coerceToInt(startVal)
	if err != nil {
		return nil, fmt.Errorf("SUBSTRING start position must be numeric")
	}
	start, ok := startAny.(int)
	if !ok {
		return nil, fmt.Errorf("SUBSTRING start position must be an integer")
	}
	ascii := stringIsASCII(str)
	var runes []rune
	charLen := len(str)
	if !ascii {
		runes = stringRunes(str)
		charLen = len(runes)
	}
	// SQL positions start at one. Negative positions count from the end;
	// zero remains a forgiving alias for the first character.
	switch {
	case start > 0:
		start--
	case start < 0:
		start += charLen
	default:
		start = 0
	}
	if start < 0 {
		start = 0
	}
	if start >= charLen {
		return "", nil
	}

	// Get length if provided
	if len(args) == 3 {
		lengthVal, err := evalExpr(env, args[2], row)
		if err != nil {
			return nil, err
		}
		if lengthVal == nil {
			return nil, nil
		}
		lengthAny, err := coerceToInt(lengthVal)
		if err != nil {
			return nil, fmt.Errorf("SUBSTRING length must be numeric")
		}
		length, ok := lengthAny.(int)
		if !ok {
			return nil, fmt.Errorf("SUBSTRING length must be an integer")
		}

		if length <= 0 {
			return "", nil
		}
		end := start + length
		if end > charLen {
			end = charLen
		}
		if ascii {
			return str[start:end], nil
		}
		return string(runes[start:end]), nil
	}

	if ascii {
		return str[start:], nil
	}
	return string(runes[start:]), nil
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

	str := valueText(val)

	lenVal, err := evalExpr(env, args[1], row)
	if err != nil {
		return nil, err
	}
	if lenVal == nil {
		return nil, nil
	}
	lenAny, err := coerceToInt(lenVal)
	if err != nil {
		return nil, fmt.Errorf("LEFT length must be numeric")
	}
	length, ok := lenAny.(int)
	if !ok {
		return nil, fmt.Errorf("LEFT length must be an integer")
	}

	return stringEdge(val, str, length, false), nil
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

	str := valueText(val)

	lenVal, err := evalExpr(env, args[1], row)
	if err != nil {
		return nil, err
	}
	if lenVal == nil {
		return nil, nil
	}
	lenAny, err := coerceToInt(lenVal)
	if err != nil {
		return nil, fmt.Errorf("RIGHT length must be numeric")
	}
	length, ok := lenAny.(int)
	if !ok {
		return nil, fmt.Errorf("RIGHT length must be an integer")
	}

	return stringEdge(val, str, length, true), nil
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
	str := valueText(val)

	fromVal, err := evalExpr(env, args[1], row)
	if err != nil {
		return nil, err
	}
	if fromVal == nil {
		return nil, nil
	}
	from := valueText(fromVal)

	toVal, err := evalExpr(env, args[2], row)
	if err != nil {
		return nil, err
	}
	if toVal == nil {
		return nil, nil
	}
	to := valueText(toVal)

	if from == "" {
		return str, nil
	}
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
		return nil, nil
	}
	str := valueText(val)

	searchVal, err := evalExpr(env, args[1], row)
	if err != nil {
		return nil, err
	}
	if searchVal == nil {
		return nil, nil
	}
	search := valueText(searchVal)

	idx := strings.Index(str, search)
	if idx == -1 {
		return 0, nil
	}
	return utf8.RuneCountInString(str[:idx]) + 1, nil // 1-based character index
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
	str := valueText(val)
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
	str := valueText(val)

	countVal, err := evalExpr(env, args[1], row)
	if err != nil {
		return nil, err
	}
	if countVal == nil {
		return nil, nil
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
	format := valueText(formatVal)

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
	str := valueText(val)

	lenVal, err := evalExpr(env, args[1], row)
	if err != nil {
		return nil, err
	}
	if lenVal == nil {
		return nil, nil
	}
	lenAny, err := coerceToInt(lenVal)
	if err != nil {
		return nil, fmt.Errorf("LPAD length must be numeric")
	}
	length, ok := lenAny.(int)
	if !ok || length < 0 {
		return "", nil
	}

	pad := " "
	if len(args) == 3 {
		padVal, err := evalExpr(env, args[2], row)
		if err != nil {
			return nil, err
		}
		if padVal == nil {
			return nil, nil
		}
		pad = valueText(padVal)
	}

	count := stringCharCount(str)
	if count >= length {
		return stringPrefix(str, length), nil
	}
	if pad == "" {
		return str, nil
	}
	needed := length - count
	padding := stringPrefix(strings.Repeat(pad, (needed/utf8.RuneCountInString(pad))+1), needed)
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
	str := valueText(val)

	lenVal, err := evalExpr(env, args[1], row)
	if err != nil {
		return nil, err
	}
	if lenVal == nil {
		return nil, nil
	}
	lenAny, err := coerceToInt(lenVal)
	if err != nil {
		return nil, fmt.Errorf("RPAD length must be numeric")
	}
	length, ok := lenAny.(int)
	if !ok || length < 0 {
		return "", nil
	}

	pad := " "
	if len(args) == 3 {
		padVal, err := evalExpr(env, args[2], row)
		if err != nil {
			return nil, err
		}
		if padVal == nil {
			return nil, nil
		}
		pad = valueText(padVal)
	}

	count := stringCharCount(str)
	if count >= length {
		return stringPrefix(str, length), nil
	}
	if pad == "" {
		return str, nil
	}
	needed := length - count
	padding := stringPrefix(strings.Repeat(pad, (needed/utf8.RuneCountInString(pad))+1), needed)
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
	s := valueText(val)
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
	if val == nil {
		return nil, nil
	}
	s := valueText(val)
	words := strings.Fields(s)
	for i, word := range words {
		if first, width := utf8.DecodeRuneInString(word); width > 0 {
			words[i] = strings.ToUpper(string(first)) + strings.ToLower(word[width:])
		}
	}
	return strings.Join(words, " "), nil
}

// splitPartAt returns the 0-indexed idx-th field of s split on delim,
// without materializing every field the way strings.Split(s, delim) would --
// SPLIT_PART only ever needs the one field at idx, and a long delimited
// string (e.g. many CSV-style fields) otherwise pays for a []string entry
// per field just to throw away all but one. ok is false when idx is
// negative or past the last field, matching strings.Split's out-of-range
// behavior. The empty-delimiter case is rare enough here to just defer to
// strings.Split's own per-rune splitting semantics.
func splitPartAt(s, delim string, idx int) (string, bool) {
	if idx < 0 {
		return "", false
	}
	if delim == "" {
		parts := strings.Split(s, delim)
		if idx >= len(parts) {
			return "", false
		}
		return parts[idx], true
	}
	start := 0
	for i := 0; i < idx; i++ {
		pos := strings.Index(s[start:], delim)
		if pos < 0 {
			return "", false
		}
		start += pos + len(delim)
	}
	if end := strings.Index(s[start:], delim); end >= 0 {
		return s[start : start+end], true
	}
	return s[start:], true
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
	if strVal == nil || delimVal == nil || partVal == nil {
		return nil, nil
	}
	s := valueText(strVal)
	delim := valueText(delimVal)
	part, ok := numeric(partVal)
	if !ok {
		return nil, fmt.Errorf("SPLIT_PART: part must be numeric")
	}
	idx := int(part) - 1 // 1-indexed
	result, ok := splitPartAt(s, delim, idx)
	if !ok {
		return "", nil
	}
	return result, nil
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
	s := valueText(val)
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
	if sepVal == nil {
		return nil, nil
	}
	sep := valueText(sepVal)
	var parts []string
	for _, arg := range args[1:] {
		v, err := evalExpr(env, arg, row)
		if err != nil {
			return nil, err
		}
		if v != nil {
			parts = append(parts, valueText(v))
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
	if subVal == nil || strVal == nil {
		return nil, nil
	}
	sub := valueText(subVal)
	str := valueText(strVal)
	idx := strings.Index(str, sub)
	if idx < 0 {
		return 0, nil
	}
	return utf8.RuneCountInString(str[:idx]) + 1, nil
}

func evalLocate(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("LOCATE expects 2 arguments: (substring, string)")
	}
	// LOCATE has the reverse argument order of INSTR.
	return evalInstr(env, []Expr{args[1], args[0]}, row)
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
		return valueText(val), nil
	case "INT", "INTEGER":
		return coerceToInt(val)
	case "FLOAT", "REAL", "DOUBLE", "NUMERIC":
		if f, ok := numeric(val); ok {
			return f, nil
		}
		str := valueText(val)
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
