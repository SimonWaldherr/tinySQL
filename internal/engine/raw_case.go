package engine

type rawCaseBranch struct {
	when, then rawValueEvaluator
}

// Prepare every arm without evaluating it. An unsupported child leaves the
// entire CASE on its interpreter, including its lazy errors and statement clock.
func compileRawCase(columns map[string]int, expr *CaseExpr) rawValueEvaluator {
	var operand rawValueEvaluator
	if expr.Operand != nil {
		operand = compileRawValue(columns, expr.Operand)
		if operand == nil {
			return nil
		}
	}
	branches := make([]rawCaseBranch, len(expr.Whens))
	for i, branch := range expr.Whens {
		when := compileRawValue(columns, branch.When)
		then := compileRawValue(columns, branch.Then)
		if when == nil || then == nil {
			return nil
		}
		branches[i] = rawCaseBranch{when: when, then: then}
	}
	var otherwise rawValueEvaluator
	if expr.Else != nil {
		otherwise = compileRawValue(columns, expr.Else)
		if otherwise == nil {
			return nil
		}
	}
	return func(raw []any) (any, error) {
		var target any
		if operand != nil {
			var err error
			target, err = operand(raw)
			if err != nil {
				return nil, err
			}
		}
		for _, branch := range branches {
			value, err := branch.when(raw)
			if err != nil {
				return nil, err
			}
			matched := false
			if operand != nil {
				matched = caseValuesEqual(target, value)
			} else {
				matched = toTri(value) == tvTrue
			}
			if matched {
				return branch.then(raw)
			}
		}
		if otherwise != nil {
			return otherwise(raw)
		}
		return nil, nil
	}
}
