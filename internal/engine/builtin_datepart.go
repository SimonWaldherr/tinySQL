package engine

import (
	"fmt"
	"strings"
	"time"
)

func evalDatePartFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalDatePartCall(ex, func(arg Expr) (any, error) { return evalExpr(env, arg, row) })
}

func evalDatePartCall(ex *FuncCall, eval func(Expr) (any, error)) (any, error) {
	if len(ex.Args) != 2 {
		return nil, fmt.Errorf("DATEPART expects 2 arguments: (part, date)")
	}
	part, err := eval(ex.Args[0])
	if err != nil || part == nil {
		return nil, err
	}
	text, ok := part.(string)
	if !ok {
		return nil, fmt.Errorf("DATEPART: part must be a string")
	}
	value, err := eval(ex.Args[1])
	if err != nil || value == nil {
		return nil, err
	}
	date, err := parseTimeValue(value)
	if err != nil {
		return nil, fmt.Errorf("DATEPART: %w", err)
	}
	switch strings.ToUpper(text) {
	case "YEAR", "YY", "YYYY":
		return date.Year(), nil
	case "QUARTER", "QQ", "Q":
		return (int(date.Month())-1)/3 + 1, nil
	case "MONTH", "MM", "M":
		return int(date.Month()), nil
	case "DAYOFYEAR", "DY", "Y", "DOY":
		return date.YearDay(), nil
	case "DAY", "DD", "D":
		return date.Day(), nil
	case "WEEK", "WK", "WW":
		// Fixed Sunday-first weeks, with January 1 always in week 1.
		jan1 := time.Date(date.Year(), 1, 1, 0, 0, 0, 0, time.UTC)
		return (date.YearDay()-1+int(jan1.Weekday()))/7 + 1, nil
	case "ISO_WEEKDAY", "ISODOW":
		return (int(date.Weekday())+6)%7 + 1, nil
	case "WEEKDAY", "DW":
		return int(date.Weekday()) + 1, nil
	case "HOUR", "HH":
		return date.Hour(), nil
	case "MINUTE", "MI", "N":
		return date.Minute(), nil
	case "SECOND", "SS", "S":
		return date.Second(), nil
	case "MILLISECOND", "MS":
		return date.Nanosecond() / 1e6, nil
	case "MICROSECOND", "MCS":
		return date.Nanosecond() / 1e3, nil
	case "NANOSECOND", "NS":
		return date.Nanosecond(), nil
	case "TZOFFSET", "TZ":
		_, offset := date.Zone()
		return offset / 60, nil
	case "ISO_WEEK", "ISOWK", "ISOWW":
		_, week := date.ISOWeek()
		return week, nil
	case "ISOYEAR", "ISO_YEAR":
		year, _ := date.ISOWeek()
		return year, nil
	default:
		return nil, fmt.Errorf("DATEPART: unsupported part '%s'", text)
	}
}
