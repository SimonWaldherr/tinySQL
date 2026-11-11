package engine

import "fmt"

// Prepare only functions with row-independent semantics. Unsupported children,
// invalid arities and statement clocks retain the complete interpreter call.
func compileRawScalar(columns map[string]int, call *FuncCall) rawValueEvaluator {
	if call == nil || call.Over != nil || call.Filter != nil {
		return nil
	}
	switch call.Name {
	case "ISNULL", "LENGTH", "LEN", "CHAR_LENGTH", "LOWER", "UPPER":
		if len(call.Args) != 1 {
			return nil
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
		case "LENGTH", "LEN", "CHAR_LENGTH":
			return func(raw []any) (any, error) {
				v, err := value(raw)
				if err != nil {
					return nil, err
				}
				return stringLengthValue(v), nil
			}
		default:
			upper := call.Name == "UPPER"
			return func(raw []any) (any, error) {
				v, err := value(raw)
				if err != nil {
					return nil, err
				}
				return caseStringValue(v, upper), nil
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
	}
	return nil
}

func compileRawTrim(columns map[string]int, call *FuncCall) rawValueEvaluator {
	if len(call.Args) < 1 || len(call.Args) > 2 {
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
	name := call.Name
	return func(raw []any) (any, error) {
		v, err := value(raw)
		if err != nil || v == nil {
			return nil, err
		}
		text := valueText(v)
		cutset := ""
		if cut != nil {
			c, err := cut(raw)
			if err != nil || c == nil {
				return nil, err
			}
			var ok bool
			cutset, ok = c.(string)
			if !ok {
				return nil, fmt.Errorf("%s cutset must be a string", name)
			}
			if cutset == "" {
				return text, nil
			}
		}
		return trimStringValue(v, text, cutset, side)
	}
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
