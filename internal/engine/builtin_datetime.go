// Date and time functions, and the parsers behind them. Formats are accepted
// explicitly rather than guessed, so an unparseable value is an error instead of
// a silently wrong date.
package engine

import (
	"fmt"
	"strings"
	"time"
)

func evalDateDiff(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if len(ex.Args) != 3 {
		return nil, fmt.Errorf("DATEDIFF expects 3 arguments: (unit, start_date, end_date)")
	}

	// Get the unit (HOURS, DAYS, MINUTES, etc.)
	unitVal, err := evalExpr(env, ex.Args[0], row)
	if err != nil {
		return nil, err
	}
	unit, ok := unitVal.(string)
	if !ok {
		return nil, fmt.Errorf("DATEDIFF unit must be a string")
	}

	// Get start date
	startVal, err := evalExpr(env, ex.Args[1], row)
	if err != nil {
		return nil, err
	}

	// Get end date
	endVal, err := evalExpr(env, ex.Args[2], row)
	if err != nil {
		return nil, err
	}

	// Convert values to time.Time
	startTime, err := parseTimeValue(startVal)
	if err != nil {
		return nil, fmt.Errorf("DATEDIFF start_date: %v", err)
	}

	endTime, err := parseTimeValue(endVal)
	if err != nil {
		return nil, fmt.Errorf("DATEDIFF end_date: %v", err)
	}

	// Calculate difference
	diff := endTime.Sub(startTime)

	// Return based on unit
	switch strings.ToUpper(unit) {
	case "HOURS":
		return int(diff.Hours()), nil
	case "MINUTES":
		return int(diff.Minutes()), nil
	case "SECONDS":
		return int(diff.Seconds()), nil
	case "DAYS":
		return int(diff.Hours() / 24), nil
	case "WEEKS":
		return int(diff.Hours() / (24 * 7)), nil
	case "MONTHS":
		// Approximate: 30 days per month
		return int(diff.Hours() / (24 * 30)), nil
	case "YEARS":
		// Approximate: 365 days per year
		return int(diff.Hours() / (24 * 365)), nil
	default:
		return nil, fmt.Errorf("unsupported DATEDIFF unit: %s (supported: HOURS, MINUTES, SECONDS, DAYS, WEEKS, MONTHS, YEARS)", unit)
	}
}

// parseTimeFixedDigits parses the fixed-width layouts "2006-01-02 15:04:05",
// "2006-01-02T15:04:05" and "2006-01-02" directly, without going through
// time.Parse's layout interpreter. Timestamp columns in analytical/RAG
// queries (RECENCY_SCORE, RAG_HYBRID_SCORE, date functions) are parsed once
// per row, and time.Parse's generality made it a top-3 CPU cost in such
// scans; direct digit slicing is ~15x cheaper. Returns ok=false for
// anything that does not match exactly — including out-of-range components,
// which time.Date would silently normalize (e.g. month 13 → January) where
// time.Parse reports an error — so callers fall back to the general path
// and error behavior is unchanged.
func parseTimeFixedDigits(s string) (time.Time, bool) {
	digit2 := func(i int) (int, bool) {
		c0, c1 := s[i]-'0', s[i+1]-'0'
		if c0 > 9 || c1 > 9 {
			return 0, false
		}
		return int(c0)*10 + int(c1), true
	}
	if len(s) != 10 && len(s) != 19 {
		return time.Time{}, false
	}
	if s[4] != '-' || s[7] != '-' {
		return time.Time{}, false
	}
	yHi, ok1 := digit2(0)
	yLo, ok2 := digit2(2)
	m, ok3 := digit2(5)
	d, ok4 := digit2(8)
	if !ok1 || !ok2 || !ok3 || !ok4 {
		return time.Time{}, false
	}
	y := yHi*100 + yLo
	var hh, mm, ss int
	if len(s) == 19 {
		if (s[10] != ' ' && s[10] != 'T') || s[13] != ':' || s[16] != ':' {
			return time.Time{}, false
		}
		var ok5, ok6, ok7 bool
		hh, ok5 = digit2(11)
		mm, ok6 = digit2(14)
		ss, ok7 = digit2(17)
		if !ok5 || !ok6 || !ok7 || hh > 23 || mm > 59 || ss > 59 {
			return time.Time{}, false
		}
	}
	if m < 1 || m > 12 || d < 1 || d > 31 {
		return time.Time{}, false
	}
	t := time.Date(y, time.Month(m), d, hh, mm, ss, 0, time.UTC)
	// Reject dates time.Date normalized (e.g. Feb 31 → Mar 3) to keep
	// time.Parse's out-of-range error semantics via the fallback path.
	if t.Day() != d {
		return time.Time{}, false
	}
	return t, true
}

func parseTimeValue(val any) (time.Time, error) {
	if val == nil {
		return time.Time{}, fmt.Errorf("cannot parse nil as time")
	}

	switch v := val.(type) {
	case time.Time:
		return v, nil
	case string:
		if t, ok := parseTimeFixedDigits(v); ok {
			return t, nil
		}
		// Select candidate formats by string length to avoid trying all formats on every call.
		var formats []string
		switch len(v) {
		case 5: // "15:04"
			formats = []string{"15:04"}
		case 8: // "15:04:05"
			formats = []string{"15:04:05"}
		case 10: // "2006-01-02"
			formats = []string{"2006-01-02"}
		case 16: // "2006-01-02 15:04"
			formats = []string{"2006-01-02 15:04"}
		case 19: // "2006-01-02 15:04:05" or "2006-01-02T15:04:05"
			formats = []string{"2006-01-02 15:04:05", "2006-01-02T15:04:05"}
		default: // RFC3339 with timezone and other variants
			formats = []string{time.RFC3339, "2006-01-02 15:04:05", "2006-01-02T15:04:05", "2006-01-02 15:04", "2006-01-02", "15:04:05", "15:04"}
		}

		for _, format := range formats {
			if t, err := time.Parse(format, v); err == nil {
				return t, nil
			}
		}
		return time.Time{}, fmt.Errorf("cannot parse '%s' as time", v)
	default:
		return time.Time{}, fmt.Errorf("cannot convert %T to time", val)
	}
}

func evalStrftime(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) < 1 || len(args) > 2 {
		return nil, fmt.Errorf("STRFTIME expects 1-2 arguments: (format[, datetime])")
	}
	formatVal, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	format := valueText(formatVal)

	var t time.Time
	if len(args) == 2 {
		dtVal, err := evalExpr(env, args[1], row)
		if err != nil {
			return nil, err
		}
		t, err = parseDateTime(dtVal)
		if err != nil {
			return nil, err
		}
	} else {
		t = time.Now()
	}

	return strftimeFormat(t, format), nil
}

func evalDate(env ExecEnv, args []Expr, row Row) (any, error) {
	var t time.Time
	if len(args) == 0 {
		t = time.Now()
	} else {
		val, err := evalExpr(env, args[0], row)
		if err != nil {
			return nil, err
		}
		t, err = parseDateTime(val)
		if err != nil {
			return nil, err
		}
	}
	return t.Format("2006-01-02"), nil
}

func evalTime(env ExecEnv, args []Expr, row Row) (any, error) {
	var t time.Time
	if len(args) == 0 {
		t = time.Now()
	} else {
		val, err := evalExpr(env, args[0], row)
		if err != nil {
			return nil, err
		}
		t, err = parseDateTime(val)
		if err != nil {
			return nil, err
		}
	}
	return t.Format("15:04:05"), nil
}

func evalYear(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("YEAR expects 1 argument")
	}
	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	t, err := parseDateTime(val)
	if err != nil {
		return nil, err
	}
	return t.Year(), nil
}

func evalMonth(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("MONTH expects 1 argument")
	}
	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	t, err := parseDateTime(val)
	if err != nil {
		return nil, err
	}
	return int(t.Month()), nil
}

func evalDay(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("DAY expects 1 argument")
	}
	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	t, err := parseDateTime(val)
	if err != nil {
		return nil, err
	}
	return t.Day(), nil
}

func evalHour(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("HOUR expects 1 argument")
	}
	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	t, err := parseDateTime(val)
	if err != nil {
		return nil, err
	}
	return t.Hour(), nil
}

func evalMinute(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("MINUTE expects 1 argument")
	}
	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	t, err := parseDateTime(val)
	if err != nil {
		return nil, err
	}
	return t.Minute(), nil
}

func evalSecond(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("SECOND expects 1 argument")
	}
	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	t, err := parseDateTime(val)
	if err != nil {
		return nil, err
	}
	return t.Second(), nil
}

// parseDateTime tries to parse a value as a time.Time
func parseDateTime(val any) (time.Time, error) {
	if val == nil {
		return time.Time{}, fmt.Errorf("cannot parse nil as datetime")
	}
	if t, ok := val.(time.Time); ok {
		return t, nil
	}
	str := valueText(val)
	formats := []string{
		time.RFC3339,
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05",
		"2006-01-02",
		"01/02/2006",
		"02-Jan-2006",
	}
	for _, f := range formats {
		if t, err := time.Parse(f, str); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("cannot parse '%s' as datetime", str)
}

// strftimeFormat converts SQLite-style strftime format to Go time
func strftimeFormat(t time.Time, format string) string {
	// Map SQLite strftime codes to Go format
	replacements := map[string]string{
		"%Y": "2006",
		"%m": "01",
		"%d": "02",
		"%H": "15",
		"%M": "04",
		"%S": "05",
		"%j": fmt.Sprintf("%03d", t.YearDay()),
		"%W": fmt.Sprintf("%02d", (t.YearDay()-int(t.Weekday())+7)/7),
		"%w": fmt.Sprintf("%d", t.Weekday()),
		"%s": fmt.Sprintf("%d", t.Unix()),
		"%%": "%",
	}
	result := format
	for code, goFmt := range replacements {
		result = strings.ReplaceAll(result, code, goFmt)
	}
	return t.Format(result)
}

func evalDayOfWeek(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("DAYOFWEEK expects 1 argument")
	}
	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	t, err := parseDateTime(val)
	if err != nil {
		return nil, err
	}
	return int(t.Weekday()) + 1, nil // 1=Sunday, 7=Saturday
}

func evalDayOfYear(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("DAYOFYEAR expects 1 argument")
	}
	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	t, err := parseDateTime(val)
	if err != nil {
		return nil, err
	}
	return t.YearDay(), nil
}

func evalWeekOfYear(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("WEEKOFYEAR expects 1 argument")
	}
	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	t, err := parseDateTime(val)
	if err != nil {
		return nil, err
	}
	_, week := t.ISOWeek()
	return week, nil
}

func evalQuarter(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("QUARTER expects 1 argument")
	}
	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	t, err := parseDateTime(val)
	if err != nil {
		return nil, err
	}
	return (int(t.Month())-1)/3 + 1, nil
}

func evalDateAdd(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 3 {
		return nil, fmt.Errorf("DATE_ADD expects 3 arguments: (date, interval, unit)")
	}
	dateVal, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	intervalVal, err := evalExpr(env, args[1], row)
	if err != nil {
		return nil, err
	}
	unitVal, err := evalExpr(env, args[2], row)
	if err != nil {
		return nil, err
	}

	t, err := parseDateTime(dateVal)
	if err != nil {
		return nil, err
	}
	interval, ok := numeric(intervalVal)
	if !ok {
		return nil, fmt.Errorf("DATE_ADD: interval must be numeric")
	}
	unit := strings.ToUpper(valueText(unitVal))

	switch unit {
	case "YEAR", "YEARS":
		t = t.AddDate(int(interval), 0, 0)
	case "MONTH", "MONTHS":
		t = t.AddDate(0, int(interval), 0)
	case "DAY", "DAYS":
		t = t.AddDate(0, 0, int(interval))
	case "HOUR", "HOURS":
		t = t.Add(time.Duration(interval) * time.Hour)
	case "MINUTE", "MINUTES":
		t = t.Add(time.Duration(interval) * time.Minute)
	case "SECOND", "SECONDS":
		t = t.Add(time.Duration(interval) * time.Second)
	default:
		return nil, fmt.Errorf("DATE_ADD: unknown unit '%s'", unit)
	}
	return t, nil
}

func evalDateSub(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 3 {
		return nil, fmt.Errorf("DATE_SUB expects 3 arguments: (date, interval, unit)")
	}
	// DATE_SUB is just DATE_ADD with negated interval
	dateVal, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	intervalVal, err := evalExpr(env, args[1], row)
	if err != nil {
		return nil, err
	}
	unitVal, err := evalExpr(env, args[2], row)
	if err != nil {
		return nil, err
	}

	t, err := parseDateTime(dateVal)
	if err != nil {
		return nil, err
	}
	interval, ok := numeric(intervalVal)
	if !ok {
		return nil, fmt.Errorf("DATE_SUB: interval must be numeric")
	}
	interval = -interval // Negate for subtraction
	unit := strings.ToUpper(valueText(unitVal))

	switch unit {
	case "YEAR", "YEARS":
		t = t.AddDate(int(interval), 0, 0)
	case "MONTH", "MONTHS":
		t = t.AddDate(0, int(interval), 0)
	case "DAY", "DAYS":
		t = t.AddDate(0, 0, int(interval))
	case "HOUR", "HOURS":
		t = t.Add(time.Duration(interval) * time.Hour)
	case "MINUTE", "MINUTES":
		t = t.Add(time.Duration(interval) * time.Minute)
	case "SECOND", "SECONDS":
		t = t.Add(time.Duration(interval) * time.Second)
	default:
		return nil, fmt.Errorf("DATE_SUB: unknown unit '%s'", unit)
	}
	return t, nil
}
