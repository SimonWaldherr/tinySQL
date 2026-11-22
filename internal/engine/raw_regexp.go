package engine

// Bind constant patterns once per plan. Invalid patterns and prepared
// parameters retain runtime evaluation, including NULL and error precedence.
func compileRawRegexp(columns map[string]int, call *FuncCall) rawValueEvaluator {
	count := 2
	if call.Name == "REGEXP_REPLACE" {
		count = 3
	}
	if len(call.Args) != count {
		return nil
	}
	literal, ok := call.Args[1].(*Literal)
	if !ok || literal.Parameter {
		return nil
	}
	pattern, ok := literal.Val.(string)
	if !ok {
		return nil
	}
	input := compileRawValue(columns, call.Args[0])
	if input == nil {
		return nil
	}
	if call.Name == "REGEXP_MATCH" {
		match, err := compileCachedRegexpMatcher(pattern)
		if err != nil {
			return nil
		}
		return func(raw []any) (any, error) {
			value, err := input(raw)
			if err != nil {
				return nil, err
			}
			return value != nil && match(valueText(value)), nil
		}
	}
	re, err := loadCachedRegexp(pattern)
	if err != nil {
		return nil
	}
	if count == 2 {
		return func(raw []any) (any, error) {
			value, err := input(raw)
			if err != nil || value == nil {
				return nil, err
			}
			return re.extractText(valueText(value)), nil
		}
	}
	replacement := compileRawValue(columns, call.Args[2])
	if replacement == nil {
		return nil
	}
	return func(raw []any) (any, error) {
		value, err := input(raw)
		if err != nil {
			return nil, err
		}
		// Evaluate the replacement even when the input is NULL, as in the
		// interpreter. An error in that expression takes precedence.
		repl, err := replacement(raw)
		if err != nil || value == nil || repl == nil {
			return nil, err
		}
		return re.replaceText(value, valueText(value), valueText(repl)), nil
	}
}
