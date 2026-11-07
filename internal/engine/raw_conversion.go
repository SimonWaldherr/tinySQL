package engine

import (
	"fmt"
	"strings"
)

type rawValueEvaluator func([]any) (any, error)

// Bind conversion syntax once. Values, including bound parameter literals,
// remain row-time inputs. Invalid or dynamic declarations retain the evaluator's
// validation and error order rather than failing during planning.
func compileRawConversion(columns map[string]int, expr Expr) rawValueEvaluator {
	call, ok := expr.(*FuncCall)
	if !ok || call.Over != nil || call.Filter != nil {
		return nil
	}
	if call.Name == "CAST" {
		if len(call.Args) != 2 || exprContainsBoundParameter(call.Args[1]) {
			return nil
		}
		var target string
		switch arg := call.Args[1].(type) {
		case *VarRef:
			target = arg.Name
		case *Literal:
			var valid bool
			target, valid = arg.Val.(string)
			if !valid {
				return nil
			}
		default:
			return nil
		}
		target = strings.ToUpper(target)
		value := compileRawConversionInput(columns, call.Args[0])
		if value == nil {
			return nil
		}
		return func(raw []any) (any, error) {
			input, err := value(raw)
			if err != nil {
				return nil, err
			}
			return castValue(input, target)
		}
	}
	if call.Name != "CONVERT" && call.Name != "TRY_CONVERT" || len(call.Args) < 2 || len(call.Args) > 3 {
		return nil
	}
	if exprContainsBoundParameter(call.Args[0]) {
		return nil
	}
	target, err := parseConversionTarget(call.Args[0])
	if err != nil {
		return nil
	}
	style := -1
	if len(call.Args) == 3 {
		literal, ok := call.Args[2].(*Literal)
		if !ok || literal.Parameter || literal.Val == nil {
			return nil
		}
		style, err = conversionStyle(call.Name, literal.Val)
		if err != nil {
			return nil
		}
	}
	value := compileRawConversionInput(columns, call.Args[1])
	if value == nil {
		return nil
	}
	name := call.Name
	return func(raw []any) (any, error) {
		input, err := value(raw)
		if err != nil {
			return nil, err
		}
		return convertCallValue(name, input, target, style)
	}
}

func compileRawConversionInput(columns map[string]int, expr Expr) rawValueEvaluator {
	switch ex := expr.(type) {
	case *Literal:
		return func([]any) (any, error) { return ex.Val, nil }
	case *VarRef:
		key := ex.Lower
		if key == "" {
			key = strings.ToLower(ex.Name)
		}
		column, ok := columns[key]
		if !ok {
			return nil
		}
		name := ex.Name
		return func(raw []any) (any, error) {
			if column < 0 || column >= len(raw) {
				return nil, fmt.Errorf("column %q is out of range", name)
			}
			return raw[column], nil
		}
	case *FuncCall:
		return compileRawConversion(columns, expr)
	}
	if value, ok := signedNumericLiteral(expr); ok && !exprContainsBoundParameter(expr) {
		return func([]any) (any, error) { return value, nil }
	}
	return nil
}

// Keep these filters in the expression tier: promoting conversions to the
// cheap column-predicate tier would change AND's existing evaluation order.
func buildRawConversionFilter(columns map[string]int, expr Expr) func([]any) (bool, error) {
	switch ex := expr.(type) {
	case *BetweenExpr:
		value := compileRawConversion(columns, ex.Expr)
		lo := compileRawConversionInput(columns, ex.Lo)
		hi := compileRawConversionInput(columns, ex.Hi)
		if value == nil || lo == nil || hi == nil {
			return nil
		}
		negate := ex.Negate
		return func(raw []any) (bool, error) {
			v, err := value(raw)
			if err != nil {
				return false, err
			}
			lower, err := lo(raw)
			if err != nil {
				return false, err
			}
			upper, err := hi(raw)
			if err != nil {
				return false, err
			}
			result, err := betweenResult(v, lower, upper, negate)
			return toTri(result) == tvTrue, err
		}
	case *Binary:
		if !isComparisonOp(ex.Op) {
			return nil
		}
		left := compileRawConversion(columns, ex.Left)
		right := compileRawConversion(columns, ex.Right)
		if left == nil && right == nil {
			return nil
		}
		if left == nil {
			left = compileRawConversionInput(columns, ex.Left)
		}
		if right == nil {
			right = compileRawConversionInput(columns, ex.Right)
		}
		if left == nil || right == nil {
			return nil
		}
		op := ex.Op
		return func(raw []any) (bool, error) {
			a, err := left(raw)
			if err != nil {
				return false, err
			}
			b, err := right(raw)
			if err != nil {
				return false, err
			}
			result, err := evalComparisonBinary(op, a, b)
			return toTri(result) == tvTrue, err
		}
	}
	return nil
}
