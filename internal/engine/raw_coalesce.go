package engine

// Bind operands without evaluating them. NULL functions must still stop at
// the first non-NULL value, including zero, false and typed nil values.
func compileRawCoalesce(columns map[string]int, call *FuncCall) rawValueEvaluator {
	if call == nil || call.Over != nil || call.Filter != nil {
		return nil
	}
	switch call.Name {
	case "COALESCE", "IFNULL", "NVL":
	default:
		return nil
	}
	values := make([]rawValueEvaluator, len(call.Args))
	for i, arg := range call.Args {
		values[i] = compileRawValue(columns, arg)
		if values[i] == nil {
			return nil
		}
	}
	return func(raw []any) (any, error) {
		for _, value := range values {
			v, err := value(raw)
			if err != nil {
				return nil, err
			}
			if v != nil {
				return v, nil
			}
		}
		return nil, nil
	}
}
