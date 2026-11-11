package engine

// Keep fused text comparisons in the expression tier. Promoting them to cheap
// predicates would change AND/OR error order and JOIN pushdown eligibility.
func buildRawTrimComparisonFilter(columns map[string]int, expr Expr) func([]any) (bool, error) {
	binary, ok := expr.(*Binary)
	if !ok || !isComparisonOp(binary.Op) {
		return nil
	}
	bind := func(expr Expr) *rawTrimOperands {
		call, ok := expr.(*FuncCall)
		if !ok {
			return nil
		}
		switch call.Name {
		case "TRIM", "LTRIM", "RTRIM":
			return bindRawTrim(columns, call)
		}
		return nil
	}
	left, right := bind(binary.Left), bind(binary.Right)
	op := binary.Op
	if left != nil && right != nil {
		return func(raw []any) (bool, error) {
			a, aText, err := left.evaluate(raw)
			if err != nil {
				return false, err
			}
			b, bText, err := right.evaluate(raw)
			if err != nil {
				return false, err
			}
			if a == nil || b == nil {
				return false, nil
			}
			return compareRawStrings(aText, bText, op)
		}
	}
	if left != nil {
		other := compileRawValue(columns, binary.Right)
		if other == nil {
			return nil
		}
		return func(raw []any) (bool, error) {
			a, text, err := left.evaluate(raw)
			if err != nil {
				return false, err
			}
			b, err := other(raw)
			if err != nil {
				return false, err
			}
			if a == nil || b == nil {
				return false, nil
			}
			if bText, ok := b.(string); ok {
				return compareRawStrings(text, bText, op)
			}
			result, err := evalComparisonBinary(op, text, b)
			return toTri(result) == tvTrue, err
		}
	}
	if right != nil {
		other := compileRawValue(columns, binary.Left)
		if other == nil {
			return nil
		}
		return func(raw []any) (bool, error) {
			a, err := other(raw)
			if err != nil {
				return false, err
			}
			b, text, err := right.evaluate(raw)
			if err != nil {
				return false, err
			}
			if a == nil || b == nil {
				return false, nil
			}
			if aText, ok := a.(string); ok {
				return compareRawStrings(aText, text, op)
			}
			// Decimal-left comparisons can coerce text, unlike text-left
			// comparisons. Preserve the written order on the generic path.
			result, err := evalComparisonBinary(op, a, text)
			return toTri(result) == tvTrue, err
		}
	}
	return nil
}

func compareRawStrings(a, b, op string) (bool, error) {
	switch op {
	case "=":
		return a == b, nil
	case "!=", "<>":
		return a != b, nil
	default:
		return orderedCompareOp(a, b, op)
	}
}
