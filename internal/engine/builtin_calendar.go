package engine

import (
	"fmt"
	"math"
	"strings"
	"time"
)

func evalCalendarFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalCalendarCall(ex, func(arg Expr) (any, error) {
		return evalExpr(env, arg, row)
	})
}

// Both evaluators share validation and NULL short-circuiting, including arity
// checks before evaluating any argument.
func evalCalendarCall(ex *FuncCall, eval func(Expr) (any, error)) (any, error) {
	name := strings.ToUpper(ex.Name)
	if name == "MAKE_DATE" || name == "DATEFROMPARTS" {
		if len(ex.Args) != 3 {
			return nil, fmt.Errorf("%s expects 3 arguments: (year, month, day)", ex.Name)
		}
		var parts [3]int
		for i, field := range [...]string{"year", "month", "day"} {
			value, err := eval(ex.Args[i])
			if err != nil || value == nil {
				return nil, err
			}
			number, ok := numeric(value)
			limit := [...]float64{9999, 12, 31}[i]
			if !ok || math.IsNaN(number) || number < 1 || number > limit || math.Trunc(number) != number {
				return nil, fmt.Errorf("%s: %s must be an integer between 1 and %g", ex.Name, field, limit)
			}
			parts[i] = int(number)
		}
		result := time.Date(parts[0], time.Month(parts[1]), parts[2], 0, 0, 0, 0, time.UTC)
		if int(result.Month()) != parts[1] {
			return nil, fmt.Errorf("%s: invalid calendar date", ex.Name)
		}
		return result, nil
	}
	if len(ex.Args) != 1 {
		return nil, fmt.Errorf("%s expects 1 argument: (date)", ex.Name)
	}
	value, err := eval(ex.Args[0])
	if err != nil || value == nil {
		return nil, err
	}
	date, err := parseDateTime(value)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", ex.Name, err)
	}
	switch name {
	case "LAST_DAY":
		year, month, _ := date.Date()
		return time.Date(year, month+1, 0, 0, 0, 0, 0, date.Location()), nil
	case "DAYNAME":
		return date.Weekday().String(), nil
	case "MONTHNAME":
		return date.Month().String(), nil
	default:
		return nil, fmt.Errorf("unknown calendar function %s", ex.Name)
	}
}
