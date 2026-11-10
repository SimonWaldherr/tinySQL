package engine

// Bind column positions and operators once, while reading literals at execution
// time so prepared parameters remain fresh. Operations still use the regular
// evaluator's NULL, overflow, decimal and error rules; nothing is constant-folded.
func compileRawValue(columns map[string]int, expr Expr) rawValueEvaluator {
	switch ex := expr.(type) {
	case *CaseExpr:
		return compileRawCase(columns, ex)
	case *FuncCall:
		if value := compileRawCoalesce(columns, ex); value != nil {
			return value
		}
	case *IsNull:
		value := compileRawValue(columns, ex.Expr)
		if value == nil {
			return nil
		}
		negate := ex.Negate
		return func(raw []any) (any, error) {
			v, err := value(raw)
			if err != nil {
				return nil, err
			}
			if negate {
				return !isNull(v), nil
			}
			return isNull(v), nil
		}
	}
	binary, ok := expr.(*Binary)
	if !ok {
		return compileRawConversionInput(columns, expr)
	}
	arithmetic := isArithmeticOp(binary.Op)
	if !arithmetic && !isComparisonOp(binary.Op) {
		// AND/OR retain the predicate compiler's short-circuit and cost order.
		return nil
	}
	left := compileRawValue(columns, binary.Left)
	right := compileRawValue(columns, binary.Right)
	if left == nil || right == nil {
		return nil
	}
	op := binary.Op
	return func(raw []any) (any, error) {
		a, err := left(raw)
		if err != nil {
			return nil, err
		}
		b, err := right(raw)
		if err != nil {
			return nil, err
		}
		if arithmetic {
			return evalArithmeticBinary(op, a, b)
		}
		return evalComparisonBinary(op, a, b)
	}
}
