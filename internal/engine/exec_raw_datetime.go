package engine

import "fmt"

// Calls with harmless literal suffixes can keep the ordinary raw wrapper:
// evaluating those literals early cannot expose a discarded expression error.
func rawDateCallNeedsOrder(ex *FuncCall) bool {
	switch ex.Name {
	case "DATE", "TIME", "DATETIME", "JULIANDAY", "UNIXEPOCH":
		return len(ex.Args) > 1
	case "STRFTIME":
		if len(ex.Args) == 1 {
			return false
		}
		if len(ex.Args) != 2 {
			return true
		}
		literal, ok := ex.Args[0].(*Literal)
		return !ok || literal.Val == nil
	case "EXTRACT", "DATE_TRUNC":
		if len(ex.Args) != 2 {
			return true
		}
	case "DATE_ADD", "DATE_SUB", "DATEADD", "DATESUB":
		if len(ex.Args) != 2 && len(ex.Args) != 3 {
			return true
		}
	case "OVERLAPS":
		if len(ex.Args) != 4 {
			return true
		}
	case "EOMONTH":
		if len(ex.Args) != 1 && len(ex.Args) != 2 {
			return true
		}
	case "ADD_MONTHS":
		if len(ex.Args) != 2 {
			return true
		}
	}
	switch ex.Name {
	case "EXTRACT", "DATE_TRUNC":
		literal, ok := ex.Args[0].(*Literal)
		if !ok {
			return true
		}
		_, text := literal.Val.(string)
		return !text
	}
	for _, arg := range ex.Args[1:] {
		if _, literal := arg.(*Literal); !literal {
			return true
		}
	}
	return false
}

// These functions validate arity before evaluating arguments, and some stop
// after NULL or a malformed date. Keep that order when wrapping raw values.
func evalRawDateFuncCall(plan *simpleSelectPlan, raw []any, ex *FuncCall) (any, error) {
	env := ExecEnv{now: plan.now}
	name := ex.Name
	if name == "DATEADD" {
		name = "DATE_ADD"
	} else if name == "DATESUB" {
		name = "DATE_SUB"
	}
	switch name {
	case "DATE", "TIME", "DATETIME", "JULIANDAY", "UNIXEPOCH":
		if err := rejectDateModifiers(ex.Name, ex.Args, 1); err != nil {
			return nil, err
		}
	case "STRFTIME":
		if len(ex.Args) == 0 {
			return evalFuncCall(env, ex, rawEmptyRow)
		}
		if err := rejectDateModifiers(ex.Name, ex.Args, 2); err != nil {
			return nil, err
		}
	case "DATE_ADD", "DATE_SUB":
		if len(ex.Args) != 2 && len(ex.Args) != 3 {
			return evalFuncCall(env, ex, rawEmptyRow)
		}
	case "OVERLAPS":
		if len(ex.Args) != 4 {
			return evalFuncCall(env, ex, rawEmptyRow)
		}
	case "EXTRACT", "DATE_TRUNC", "ADD_MONTHS":
		if len(ex.Args) != 2 {
			return evalFuncCall(env, ex, rawEmptyRow)
		}
	case "EOMONTH":
		if len(ex.Args) != 1 && len(ex.Args) != 2 {
			return evalFuncCall(env, ex, rawEmptyRow)
		}
	}
	// Literal arguments cannot fail during evaluation. Leave date parsing in
	// the handler for the common DATE_ADD(column, literal, literal) shape,
	// avoiding another boxed time.Time in the argument scratch space.
	validateDates := false
	switch name {
	case "DATE_ADD", "DATE_SUB", "EOMONTH", "ADD_MONTHS", "OVERLAPS":
		for _, arg := range ex.Args[1:] {
			if _, literal := arg.(*Literal); !literal {
				validateDates = true
				break
			}
		}
	}

	sc := rawCallScratchPool.Get().(*rawCallScratch)
	defer func() { sc.reset(); rawCallScratchPool.Put(sc) }()
	if cap(sc.args) < len(ex.Args) {
		sc.args = make([]Expr, len(ex.Args))
		sc.lits = make([]Literal, len(ex.Args))
	}
	args, lits := sc.args[:len(ex.Args)], sc.lits[:len(ex.Args)]
	for i, arg := range ex.Args {
		value, err := evalRawExpr(plan, raw, arg)
		if err != nil {
			return nil, err
		}
		parsedValue := false
		switch name {
		case "STRFTIME":
			if i == 0 && value == nil {
				return nil, nil
			}
		case "EXTRACT", "DATE_TRUNC":
			if i == 0 {
				if value == nil {
					return nil, nil
				}
				if _, ok := value.(string); !ok {
					field := "unit"
					if name == "EXTRACT" {
						field = "part"
					}
					return nil, fmt.Errorf("%s: %s must be a string", name, field)
				}
			}
		case "EOMONTH", "ADD_MONTHS":
			if i == 0 {
				if value == nil {
					return nil, nil
				}
				if validateDates {
					parsed, err := parseTimeValue(value)
					if err != nil {
						return nil, fmt.Errorf("%s: %v", name, err)
					}
					value, parsedValue = parsed, true
				}
			}
		case "DATE_ADD", "DATE_SUB":
			if i == 0 {
				if value == nil {
					return nil, nil
				}
				if validateDates {
					parsed, err := parseDateTime(value)
					if err != nil {
						return nil, err
					}
					value, parsedValue = parsed, true
				}
			}
		case "OVERLAPS":
			if validateDates {
				if value == nil {
					return nil, nil
				}
				parsed, err := parseDateTime(value)
				if err != nil {
					return nil, fmt.Errorf("OVERLAPS argument %d: %w", i+1, err)
				}
				value, parsedValue = parsed, true
			}
		}
		if literal, ok := arg.(*Literal); ok && !parsedValue {
			args[i] = literal
			continue
		}
		lits[i].Val = value
		args[i] = &lits[i]
	}
	sc.call = FuncCall{Name: ex.Name, Args: args, Star: ex.Star, Distinct: ex.Distinct}
	if handler := boundFuncHandler(ex); handler != nil {
		return handler(env, &sc.call, rawEmptyRow)
	}
	return evalFuncCall(env, &sc.call, rawEmptyRow)
}
