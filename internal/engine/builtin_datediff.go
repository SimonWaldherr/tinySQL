package engine

import (
	"fmt"
	"math"
	"strings"
	"time"
)

func evalDateDiff(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalDateDiffCall(ex, func(arg Expr) (any, error) { return evalExpr(env, arg, row) })
}

func evalDateDiffCall(ex *FuncCall, eval func(Expr) (any, error)) (any, error) {
	if len(ex.Args) != 3 {
		return nil, fmt.Errorf("%s expects 3 arguments: (unit, start_date, end_date)", ex.Name)
	}
	value, err := eval(ex.Args[0])
	if err != nil || value == nil {
		return nil, err
	}
	unit, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("%s unit must be a string", ex.Name)
	}
	start, err := eval(ex.Args[1])
	if err != nil {
		return nil, err
	}
	end, err := eval(ex.Args[2])
	if err != nil || start == nil || end == nil {
		return nil, err
	}
	a, err := parseTimeValue(start)
	if err != nil {
		return nil, fmt.Errorf("%s start_date: %w", ex.Name, err)
	}
	b, err := parseTimeValue(end)
	if err != nil {
		return nil, fmt.Errorf("%s end_date: %w", ex.Name, err)
	}
	// A common timezone makes equivalent instants compare equally, without
	// consulting a connection setting or the machine's local timezone.
	a, b = a.UTC(), b.UTC()
	unit = normalizeDateDiffUnit(unit)
	var n int64
	if ex.Name == "TIMESTAMPDIFF" {
		n, err = elapsedDateDiff(unit, a, b)
	} else {
		n, err = boundaryDateDiff(unit, a, b)
	}
	if err != nil {
		return nil, fmt.Errorf("%s: %w", ex.Name, err)
	}
	if n < math.MinInt || n > math.MaxInt {
		return nil, fmt.Errorf("%s: result out of integer range", ex.Name)
	}
	return int(n), nil
}

func normalizeDateDiffUnit(unit string) string {
	switch strings.ToUpper(unit) {
	case "YEAR", "YEARS", "YY", "YYYY":
		return "YEAR"
	case "QUARTER", "QUARTERS", "QQ", "Q":
		return "QUARTER"
	case "MONTH", "MONTHS", "MM", "M":
		return "MONTH"
	case "WEEK", "WEEKS", "WK", "WW":
		return "WEEK"
	case "ISO_WEEK", "ISOWK", "ISOWW":
		return "ISO_WEEK"
	case "DAY", "DAYS", "DD", "D", "DAYOFYEAR", "DY", "Y":
		return "DAY"
	case "HOUR", "HOURS", "HH":
		return "HOUR"
	case "MINUTE", "MINUTES", "MI", "N":
		return "MINUTE"
	case "SECOND", "SECONDS", "SS", "S":
		return "SECOND"
	case "MILLISECOND", "MILLISECONDS", "MS":
		return "MILLISECOND"
	case "MICROSECOND", "MICROSECONDS", "MCS":
		return "MICROSECOND"
	case "NANOSECOND", "NANOSECONDS", "NS":
		return "NANOSECOND"
	}
	return strings.ToUpper(unit)
}

func dateDayNumber(t time.Time) int64 {
	return gregorianDayNumber(int64(t.Year()), int(t.Month()), t.Day())
}

func floorDateQuotient(n, d int64) int64 {
	q := n / d
	if n%d < 0 {
		q--
	}
	return q
}

// DATEDIFF counts crossed UTC boundaries, including for negative intervals.
// WEEK boundaries are Sundays; ISO_WEEK boundaries are Mondays.
func boundaryDateDiff(unit string, a, b time.Time) (int64, error) {
	months := (int64(b.Year())-int64(a.Year()))*12 + int64(b.Month()-a.Month())
	days := dateDayNumber(b) - dateDayNumber(a)
	clock := func(t time.Time) int64 { return int64(t.Hour()*3600 + t.Minute()*60 + t.Second()) }
	switch unit {
	case "YEAR":
		return int64(b.Year()) - int64(a.Year()), nil
	case "QUARTER":
		return (int64(b.Year())-int64(a.Year()))*4 + int64((int(b.Month())-1)/3-(int(a.Month())-1)/3), nil
	case "MONTH":
		return months, nil
	case "WEEK", "ISO_WEEK":
		anchor := gregorianDayNumber(1970, 1, 4) // Sunday
		if unit == "ISO_WEEK" {
			anchor++
		}
		return floorDateQuotient(dateDayNumber(b)-anchor, 7) - floorDateQuotient(dateDayNumber(a)-anchor, 7), nil
	case "DAY":
		return days, nil
	case "HOUR":
		return scaleDateDiff(days, 24, int64(b.Hour()-a.Hour()))
	case "MINUTE":
		return scaleDateDiff(days, 1440, clock(b)/60-clock(a)/60)
	case "SECOND", "MILLISECOND", "MICROSECOND", "NANOSECOND":
		seconds, err := scaleDateDiff(days, 86400, clock(b)-clock(a))
		if err != nil || unit == "SECOND" {
			return seconds, err
		}
		scale := dateDiffSubsecondScale(unit)
		return scaleDateDiff(seconds, scale, int64(b.Nanosecond())/(1e9/scale)-int64(a.Nanosecond())/(1e9/scale))
	}
	return 0, fmt.Errorf("unsupported unit %q", unit)
}

// TIMESTAMPDIFF truncates elapsed units toward zero. Calendar months are
// complete only once both the day and the time of day have been reached;
// January 31 to February 29 is therefore zero complete months.
func elapsedDateDiff(unit string, a, b time.Time) (int64, error) {
	switch unit {
	case "YEAR", "QUARTER", "MONTH":
		if b.Before(a) {
			n, err := elapsedDateDiff(unit, b, a)
			return -n, err
		}
		n := (int64(b.Year())-int64(a.Year()))*12 + int64(b.Month()-a.Month())
		clock := func(t time.Time) int64 {
			return int64(t.Hour()*3600+t.Minute()*60+t.Second())*1e9 + int64(t.Nanosecond())
		}
		if b.Day() < a.Day() || b.Day() == a.Day() && clock(b) < clock(a) {
			n--
		}
		if unit == "YEAR" {
			n /= 12
		} else if unit == "QUARTER" {
			n /= 3
		}
		return n, nil
	case "WEEK", "ISO_WEEK", "DAY", "HOUR", "MINUTE", "SECOND", "MILLISECOND", "MICROSECOND", "NANOSECOND":
		seconds, err := boundaryDateDiff("SECOND", a, b)
		if err != nil {
			return 0, err
		}
		nanos := int64(b.Nanosecond() - a.Nanosecond())
		if seconds > 0 && nanos < 0 {
			seconds--
			nanos += 1e9
		} else if seconds < 0 && nanos > 0 {
			seconds++
			nanos -= 1e9
		}
		switch unit {
		case "WEEK", "ISO_WEEK":
			return seconds / 604800, nil
		case "DAY":
			return seconds / 86400, nil
		case "HOUR":
			return seconds / 3600, nil
		case "MINUTE":
			return seconds / 60, nil
		case "SECOND":
			return seconds, nil
		}
		scale := dateDiffSubsecondScale(unit)
		return scaleDateDiff(seconds, scale, nanos/(1e9/scale))
	}
	return 0, fmt.Errorf("unsupported unit %q", unit)
}

func dateDiffSubsecondScale(unit string) int64 {
	switch unit {
	case "MILLISECOND":
		return 1000
	case "MICROSECOND":
		return 1000000
	default:
		return 1000000000
	}
}

func scaleDateDiff(n, scale, remainder int64) (int64, error) {
	// Opposite signs can cancel an otherwise overflowing intermediate product.
	// Normalize first so the checked multiply cannot reject a valid result.
	carry := remainder / scale
	remainder %= scale
	var ok bool
	n, ok = addCalendarInt(n, carry)
	if !ok {
		return 0, fmt.Errorf("result out of integer range")
	}
	if n < 0 && remainder > 0 {
		n++
		remainder -= scale
	}
	if n > 0 && remainder < 0 {
		n--
		remainder += scale
	}
	product, ok := mulInt64(n, scale)
	if !ok {
		return 0, fmt.Errorf("result out of integer range")
	}
	value, ok := addCalendarInt(product, remainder)
	if !ok {
		return 0, fmt.Errorf("result out of integer range")
	}
	return value, nil
}

func knownDateDiffUnit(unit string) bool {
	switch normalizeDateDiffUnit(unit) {
	case "YEAR", "QUARTER", "MONTH", "WEEK", "ISO_WEEK", "DAY", "HOUR", "MINUTE", "SECOND", "MILLISECOND", "MICROSECOND", "NANOSECOND":
		return true
	}
	return false
}
