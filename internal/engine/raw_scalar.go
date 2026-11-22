package engine

import "fmt"

// Prepare only functions with row-independent semantics. Unsupported children,
// invalid arities and statement clocks retain the complete interpreter call.
func compileRawScalar(columns map[string]int, call *FuncCall) rawValueEvaluator {
	if call == nil || call.Over != nil || call.Filter != nil {
		return nil
	}
	switch call.Name {
	case "LOWER", "UPPER":
		return compileRawCaseConversion(columns, call)
	case "LEFT", "RIGHT", "SUBSTR", "SUBSTRING":
		return compileRawStringSlice(columns, call)
	case "REGEXP_MATCH", "REGEXP_EXTRACT", "REGEXP_REPLACE":
		return compileRawRegexp(columns, call)
	case "ISNULL", "LENGTH", "LEN", "CHAR_LENGTH":
		if len(call.Args) != 1 {
			return nil
		}
		if call.Name == "LENGTH" || call.Name == "LEN" || call.Name == "CHAR_LENGTH" {
			if trim, ok := call.Args[0].(*FuncCall); ok {
				switch trim.Name {
				case "TRIM", "LTRIM", "RTRIM":
					return compileRawTrimLength(columns, trim)
				}
			}
		}
		value := compileRawValue(columns, call.Args[0])
		if value == nil {
			return nil
		}
		switch call.Name {
		case "ISNULL":
			return func(raw []any) (any, error) {
				v, err := value(raw)
				if err != nil {
					return nil, err
				}
				return isNull(v), nil
			}
		default:
			return func(raw []any) (any, error) {
				v, err := value(raw)
				if err != nil {
					return nil, err
				}
				return stringLengthValue(v), nil
			}
		}
	case "NULLIF":
		if len(call.Args) != 2 {
			return nil
		}
		left, right := compileRawValue(columns, call.Args[0]), compileRawValue(columns, call.Args[1])
		if left == nil || right == nil {
			return nil
		}
		return func(raw []any) (any, error) {
			a, err := left(raw)
			if err != nil {
				return nil, err
			}
			// The right operand still runs when the left value is NULL.
			b, err := right(raw)
			if err != nil {
				return nil, err
			}
			return nullifValues(a, b)
		}
	case "IF", "IIF":
		if len(call.Args) != 3 {
			return nil
		}
		condition := compileRawValue(columns, call.Args[0])
		yes, no := compileRawValue(columns, call.Args[1]), compileRawValue(columns, call.Args[2])
		if condition == nil || yes == nil || no == nil {
			return nil
		}
		return func(raw []any) (any, error) {
			value, err := condition(raw)
			if err != nil {
				return nil, err
			}
			if ifConditionTrue(value) {
				return yes(raw)
			}
			return no(raw)
		}
	case "TRIM", "LTRIM", "RTRIM":
		return compileRawTrim(columns, call)
	case "JSON_GET", "JSON_EXTRACT":
		if len(call.Args) != 2 {
			return nil
		}
		literal, ok := call.Args[1].(*Literal)
		if !ok || literal.Parameter {
			return nil
		}
		path, ok := literal.Val.(string)
		if !ok || path == "" {
			return nil
		}
		doc := compileRawValue(columns, call.Args[0])
		if doc == nil {
			return nil
		}
		parts := cachedJSONPath(path)
		return func(raw []any) (any, error) {
			value, err := doc(raw)
			if err != nil {
				return nil, err
			}
			return jsonGetByParts(value, parts), nil
		}
	}
	if date := compileRawDateComponent(columns, call); date != nil {
		return func(raw []any) (any, error) {
			value, err := date(raw)
			if err != nil {
				return nil, err
			}
			return value.toAny(), nil
		}
	}
	return nil
}

func compileRawCaseConversion(columns map[string]int, call *FuncCall) rawValueEvaluator {
	if len(call.Args) != 1 {
		return nil
	}
	upper := call.Name == "UPPER"
	if nested, ok := call.Args[0].(*FuncCall); ok && len(nested.Args) == 1 &&
		nested.Over == nil && nested.Filter == nil && (nested.Name == "UPPER" || nested.Name == "LOWER") {
		if value := compileRawValue(columns, nested.Args[0]); value != nil {
			nestedUpper := nested.Name == "UPPER"
			return func(raw []any) (any, error) {
				v, err := value(raw)
				if err != nil || v == nil {
					return nil, err
				}
				if text, ok := v.(string); ok && stringIsASCII(text) {
					return caseStringValue(v, upper), nil
				}
				// Unicode's upper and lower mappings are not inverses. Keep
				// both mappings and pass the intermediate as a plain string.
				return mapStringCase(mapStringCase(valueText(v), nestedUpper), upper), nil
			}
		}
	}
	value := compileRawValue(columns, call.Args[0])
	if value == nil {
		return nil
	}
	return func(raw []any) (any, error) {
		v, err := value(raw)
		if err != nil {
			return nil, err
		}
		return caseStringValue(v, upper), nil
	}
}

// Literal positions cannot fail during evaluation. Bound parameters and
// dynamic positions keep the existing evaluator and argument ordering.
func compileRawStringSlice(columns map[string]int, call *FuncCall) rawValueEvaluator {
	substring := call.Name == "SUBSTR" || call.Name == "SUBSTRING"
	if len(call.Args) != 2 && !(substring && len(call.Args) == 3) {
		return nil
	}
	position, ok := rawStringPosition(call.Args[1])
	if !ok {
		return nil
	}
	limited := len(call.Args) == 3
	var length int
	if limited {
		length, ok = rawStringPosition(call.Args[2])
		if !ok {
			return nil
		}
	}
	value := compileRawValue(columns, call.Args[0])
	if value == nil {
		return nil
	}
	right := call.Name == "RIGHT"
	return func(raw []any) (any, error) {
		v, err := value(raw)
		if err != nil || v == nil {
			return nil, err
		}
		text := valueText(v)
		if substring {
			return substringText(text, position, length, limited), nil
		}
		return stringEdge(v, text, position, right), nil
	}
}

func rawStringPosition(expr Expr) (int, bool) {
	literal, ok := expr.(*Literal)
	if !ok || literal.Parameter {
		return 0, false
	}
	// Do not read mutable decimal values at plan time. Numeric literals
	// parsed from SQL use int, int64 or float64.
	switch val := literal.Val.(type) {
	case int:
		return val, true
	case int64:
		return int(val), true
	case float64:
		return int(val), true
	}
	return 0, false
}

func compileRawTrim(columns map[string]int, call *FuncCall) rawValueEvaluator {
	trim := bindRawTrim(columns, call)
	if trim == nil {
		return nil
	}
	return func(raw []any) (any, error) {
		original, text, err := trim.evaluate(raw)
		if err != nil || original == nil {
			return nil, err
		}
		if str, ok := original.(string); ok && len(str) == len(text) {
			return original, nil
		}
		return text, nil
	}
}

// A length consumer needs the trimmed text, without boxing an intermediate
// string. Ordinary TRIM still returns the original box for unchanged strings.
func compileRawTrimLength(columns map[string]int, call *FuncCall) rawValueEvaluator {
	trim := bindRawTrim(columns, call)
	if trim == nil {
		return nil
	}
	return func(raw []any) (any, error) {
		original, text, err := trim.evaluate(raw)
		if err != nil || original == nil {
			return nil, err
		}
		return stringCharCount(text), nil
	}
}

type rawTrimOperands struct {
	value, cut rawValueEvaluator
	name       string
	side       trimSide
}

func bindRawTrim(columns map[string]int, call *FuncCall) *rawTrimOperands {
	if call.Over != nil || call.Filter != nil || len(call.Args) < 1 || len(call.Args) > 2 {
		return nil
	}
	value := compileRawValue(columns, call.Args[0])
	if value == nil {
		return nil
	}
	var cut rawValueEvaluator
	if len(call.Args) == 2 {
		cut = compileRawValue(columns, call.Args[1])
		if cut == nil {
			return nil
		}
	}
	side := trimBoth
	if call.Name == "LTRIM" {
		side = trimLeft
	} else if call.Name == "RTRIM" {
		side = trimRight
	}
	return &rawTrimOperands{value: value, cut: cut, name: call.Name, side: side}
}

// A nil original represents SQL NULL. Text conversion precedes cutset
// evaluation, even when that cutset is NULL or raises an error.
func (trim *rawTrimOperands) evaluate(raw []any) (original any, text string, err error) {
	value, err := trim.value(raw)
	if err != nil || value == nil {
		return nil, "", err
	}
	text = valueText(value)
	cutset := ""
	if trim.cut != nil {
		cut, err := trim.cut(raw)
		if err != nil || cut == nil {
			return nil, "", err
		}
		var ok bool
		cutset, ok = cut.(string)
		if !ok {
			return nil, "", fmt.Errorf("%s cutset must be a string", trim.name)
		}
		if cutset == "" {
			return value, text, nil
		}
	}
	return value, trimText(text, cutset, trim.side), nil
}

func compileRawBetween(columns map[string]int, expr *BetweenExpr) rawValueEvaluator {
	value := compileRawValue(columns, expr.Expr)
	lo, hi := compileRawValue(columns, expr.Lo), compileRawValue(columns, expr.Hi)
	if value == nil || lo == nil || hi == nil {
		return nil
	}
	negate := expr.Negate
	return func(raw []any) (any, error) {
		v, err := value(raw)
		if err != nil {
			return nil, err
		}
		lower, err := lo(raw)
		if err != nil {
			return nil, err
		}
		upper, err := hi(raw)
		if err != nil {
			return nil, err
		}
		return betweenResult(v, lower, upper, negate)
	}
}
