// Date and time functions, and the parsers behind them. Formats are accepted
// explicitly rather than guessed, so an unparseable value is an error instead of
// a silently wrong date.
package engine

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
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

// ───────────────── the SQLite date/time function family ──────────────────────
//
// DATE, TIME, DATETIME, JULIANDAY, UNIXEPOCH and STRFTIME all share one
// calling convention in SQLite: an optional time value (defaulting to 'now'),
// followed by any number of *modifiers* — '+1 day', 'start of month', 'utc',
// 'weekday 0' and so on. This engine implements the time value and none of the
// modifiers, and the three helpers below make that boundary explicit at every
// entry point instead of per function.
//
// Timezone note: SQLite's 'now' and its rendering are UTC unless the
// 'localtime' modifier is given. Here 'now' is the local wall clock, because
// every other clock in this engine already is — NOW(), GETDATE(),
// CURRENT_TIMESTAMP, CURRENT_DATE and TODAY all read time.Now() and keep its
// location. Having datetime('now') disagree with CURRENT_TIMESTAMP inside the
// same query would be a worse surprise than disagreeing with sqlite3 on a
// machine that is not set to UTC, so the family follows the engine.

// rejectDateModifiers errors when a SQLite date/time function is handed
// modifier arguments beyond its time value.
//
// Erroring is the point. evalDate used to read args[0] and return, so
// date('2024-01-01', '+1 day') answered "2024-01-01": a plausible-looking
// wrong date, which is strictly worse than no answer, because nothing in the
// result hints that a day was meant to be added. The modifier language is not
// implemented here; DATE_ADD/DATE_SUB (which take an ISO 8601 duration or an
// interval/unit pair) are this engine's date arithmetic.
func rejectDateModifiers(fn string, args []Expr, accepted int) error {
	if len(args) <= accepted {
		return nil
	}
	return fmt.Errorf("%s: date/time modifiers are not supported (%d extra argument(s) given); "+
		"use DATE_ADD/DATE_SUB for date arithmetic", fn, len(args)-accepted)
}

// parseSQLiteTimeValue parses one time value of the SQLite date/time family:
// parseDateTime, plus SQLite's 'now' literal. 'now' is matched
// case-insensitively and after trimming, the way SQLite treats it, and only in
// this family — parseDateTime itself is shared with YEAR/MONTH/DATEDIFF/
// OVERLAPS and others where a bare 'now' string is far more likely to be
// malformed data than an intentional clock read.
func parseSQLiteTimeValue(val any) (time.Time, error) {
	if s, ok := val.(string); ok && strings.EqualFold(strings.TrimSpace(s), "now") {
		return time.Now(), nil
	}
	return parseDateTime(val)
}

// sqliteTimeArg resolves the time value at args[valueIdx] — or 'now' when the
// argument is absent — and rejects anything after it as an unsupported
// modifier. valueIdx is 0 for DATE/TIME/DATETIME/JULIANDAY/UNIXEPOCH and 1 for
// STRFTIME, whose first argument is the format string.
func sqliteTimeArg(env ExecEnv, fn string, args []Expr, valueIdx int, row Row) (time.Time, error) {
	if err := rejectDateModifiers(fn, args, valueIdx+1); err != nil {
		return time.Time{}, err
	}
	if len(args) <= valueIdx {
		return time.Now(), nil
	}
	val, err := evalExpr(env, args[valueIdx], row)
	if err != nil {
		return time.Time{}, err
	}
	return parseSQLiteTimeValue(val)
}

func evalStrftime(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) < 1 {
		return nil, fmt.Errorf("STRFTIME expects at least 1 argument: (format[, datetime])")
	}
	formatVal, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	format := valueText(formatVal)

	t, err := sqliteTimeArg(env, "STRFTIME", args, 1, row)
	if err != nil {
		return nil, err
	}
	return strftimeFormat(t, format), nil
}

func evalDate(env ExecEnv, args []Expr, row Row) (any, error) {
	t, err := sqliteTimeArg(env, "DATE", args, 0, row)
	if err != nil {
		return nil, err
	}
	return t.Format("2006-01-02"), nil
}

func evalTime(env ExecEnv, args []Expr, row Row) (any, error) {
	t, err := sqliteTimeArg(env, "TIME", args, 0, row)
	if err != nil {
		return nil, err
	}
	return t.Format("15:04:05"), nil
}

// evalDatetime implements SQLite's datetime(): "YYYY-MM-DD HH:MM:SS" text, not
// a time.Time. The distinction matters — NOW() and DATE_ADD() in this engine
// return time.Time values, which render with Go's full nanosecond-and-zone
// form; a caller writing datetime(...) is asking for SQLite's fixed-width
// rendering and gets exactly that.
func evalDatetime(env ExecEnv, args []Expr, row Row) (any, error) {
	t, err := sqliteTimeArg(env, "DATETIME", args, 0, row)
	if err != nil {
		return nil, err
	}
	return t.Format("2006-01-02 15:04:05"), nil
}

// unixEpochJulianDay is the Julian Day Number of 1970-01-01T00:00:00Z. Julian
// days start at noon UT, hence the .5.
const unixEpochJulianDay = 2440587.5

// evalJulianday implements SQLite's julianday(): the number of days since noon
// UT on 24 November 4714 BC, as a float.
//
// Seconds and nanoseconds are scaled separately rather than going through
// UnixNano, which overflows int64 outside roughly 1678–2262 — a range
// julianday() itself has no trouble with, and quietly wrapping would turn a
// far-future date into a far-past one.
func evalJulianday(env ExecEnv, args []Expr, row Row) (any, error) {
	t, err := sqliteTimeArg(env, "JULIANDAY", args, 0, row)
	if err != nil {
		return nil, err
	}
	const secondsPerDay = 86400.0
	return float64(t.Unix())/secondsPerDay +
		float64(t.Nanosecond())/(secondsPerDay*1e9) + unixEpochJulianDay, nil
}

// evalUnixepoch implements SQLite's unixepoch(): whole seconds since the Unix
// epoch, as an integer. Returned as int64 so it lands in an INT column and
// compares as a number rather than as text.
func evalUnixepoch(env ExecEnv, args []Expr, row Row) (any, error) {
	t, err := sqliteTimeArg(env, "UNIXEPOCH", args, 0, row)
	if err != nil {
		return nil, err
	}
	return t.Unix(), nil
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

// applyDateUnitShift adds interval count of unit to t. Calendar units
// (YEAR/MONTH/WEEK/DAY) go through time.AddDate, which correctly accounts for
// variable month lengths and, because it operates on the local calendar date
// rather than a fixed span of nanoseconds, DST transitions ("add 1 day"
// across a spring-forward stays at the same wall-clock hour); clock units
// (HOUR/MINUTE/SECOND) go through time.Add, a fixed-duration shift instead.
// Keeping these two mechanisms separate — never adding "1 day" as
// 24*time.Hour — is what makes DATE_ADD/DATE_SUB correct across a DST
// boundary; the two both used to inline this same switch verbatim.
func applyDateUnitShift(t time.Time, interval float64, unit string) (time.Time, error) {
	switch unit {
	case "YEAR", "YEARS":
		return t.AddDate(int(interval), 0, 0), nil
	case "MONTH", "MONTHS":
		return t.AddDate(0, int(interval), 0), nil
	case "WEEK", "WEEKS":
		// Scale to days BEFORE truncating, not after: int(interval)*7 (the
		// previous version) truncated interval to a whole *week count*
		// first, so 1.9 WEEK became int(1.9)*7 = 7 days instead of the 13
		// days a caller who wrote 13.3 DAY (the same duration) would get --
		// up to nearly 7 days silently discarded, unlike every other unit
		// here, which only ever loses a fraction of one output unit.
		return t.AddDate(0, 0, int(interval*7)), nil
	case "DAY", "DAYS":
		return t.AddDate(0, 0, int(interval)), nil
	case "HOUR", "HOURS":
		return t.Add(time.Duration(interval) * time.Hour), nil
	case "MINUTE", "MINUTES":
		return t.Add(time.Duration(interval) * time.Minute), nil
	case "SECOND", "SECONDS":
		return t.Add(time.Duration(interval) * time.Second), nil
	default:
		return t, fmt.Errorf("unknown unit '%s'", unit)
	}
}

// isoDurationRE matches an ISO 8601 duration: an optional leading '-', then
// "P", an optional date part (years/months/weeks/days), and an optional
// "T"-prefixed time part (hours/minutes/seconds) -- e.g. "P1Y2M3D", "PT1H30M",
// "P1DT12H", "-P1D". All components are plain non-negative integers; ISO 8601
// permits a fractional value on the single lowest-order component present,
// but no caller of DATE_ADD/DATE_SUB/PARSE_DURATION has needed that yet and
// supporting it correctly (only the last component, and only when no
// lower-order one follows) is easy to get subtly wrong, so it is rejected
// with a clear parse error instead of silently truncated.
var isoDurationRE = regexp.MustCompile(
	`^(-)?P(?:(\d+)Y)?(?:(\d+)M)?(?:(\d+)W)?(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?)?$`)

// isoDuration holds the parsed components of an ISO 8601 duration, split
// into calendar (years/months/weeks/days) and clock (hours/minutes/seconds)
// parts -- the same split applyDateUnitShift's per-unit callers already use,
// generalized so every component of one duration string applies through a
// single DST-safe shift instead of requiring one DATE_ADD call per unit.
// Loosely modeled on the idea github.com/senseyeio/duration's Duration type
// uses (parse once, shift via AddDate for the calendar part and Add for the
// clock part), not on its code.
type isoDuration struct {
	years, months, weeks, days int
	hours, minutes, seconds    int
}

// parseISO8601Duration parses s as an ISO 8601 duration. At least one
// component must be present: a bare "P" or "PT" -- syntactically matched by
// isoDurationRE, since every component group is individually optional -- is
// rejected as malformed rather than accepted as a zero-length duration,
// matching every reference implementation's treatment of it.
func parseISO8601Duration(s string) (isoDuration, error) {
	m := isoDurationRE.FindStringSubmatch(s)
	if m == nil {
		return isoDuration{}, fmt.Errorf("invalid ISO 8601 duration %q", s)
	}
	negative := m[1] == "-"
	present := false
	var fieldErr error
	field := func(g string) int {
		if g == "" {
			return 0
		}
		present = true
		// isoDurationRE's digit groups ((\d+)) have no length cap, so a
		// component with enough digits to exceed int's range is
		// syntactically valid input, and strconv.Atoi correctly reports
		// that as a range error rather than silently clamping. An earlier
		// version of this function discarded that error on the mistaken
		// assumption that a regex-captured digit run "cannot fail" to
		// parse -- it can, and doing so let a duration like
		// "P99999999999999999999Y" silently become years=math.MaxInt64,
		// which then overflowed time.Time.AddDate's internal arithmetic and
		// produced a date *before* the input instead of erroring.
		n, err := strconv.Atoi(g)
		if err != nil && fieldErr == nil {
			fieldErr = fmt.Errorf("component %q out of range", g)
		}
		return n
	}
	d := isoDuration{
		years:   field(m[2]),
		months:  field(m[3]),
		weeks:   field(m[4]),
		days:    field(m[5]),
		hours:   field(m[6]),
		minutes: field(m[7]),
		seconds: field(m[8]),
	}
	if fieldErr != nil {
		return isoDuration{}, fmt.Errorf("invalid ISO 8601 duration %q: %w", s, fieldErr)
	}
	if !present {
		return isoDuration{}, fmt.Errorf("invalid ISO 8601 duration %q: no components", s)
	}
	// A "T" designator with nothing after it (e.g. "P1DT") is malformed the
	// same way a bare "P"/"PT" is: isoDurationRE's H/M/S groups inside the T
	// clause are each individually optional, so the regex alone accepts "T"
	// followed by nothing. The match is the whole (anchored) string, so any
	// "T" in it is the designator -- there is no other place the grammar
	// permits a literal "T".
	if strings.Contains(s, "T") && m[6] == "" && m[7] == "" && m[8] == "" {
		return isoDuration{}, fmt.Errorf("invalid ISO 8601 duration %q: 'T' with no time components", s)
	}
	if negative {
		d.years, d.months, d.weeks, d.days = -d.years, -d.months, -d.weeks, -d.days
		d.hours, d.minutes, d.seconds = -d.hours, -d.minutes, -d.seconds
	}
	return d, nil
}

// shift applies d to t, negating every component first when negate is true
// (DATE_SUB's 2-argument form). The calendar components combine into one
// AddDate call and the clock components into one Add call, each applied
// exactly once, rather than one call per component.
func (d isoDuration) shift(t time.Time, negate bool) time.Time {
	sign := 1
	if negate {
		sign = -1
	}
	t = t.AddDate(sign*d.years, sign*d.months, sign*(d.weeks*7+d.days))
	if d.hours != 0 || d.minutes != 0 || d.seconds != 0 {
		clock := time.Duration(d.hours)*time.Hour +
			time.Duration(d.minutes)*time.Minute +
			time.Duration(d.seconds)*time.Second
		t = t.Add(time.Duration(sign) * clock)
	}
	return t
}

// evalDateAdd implements two call forms:
//
//	DATE_ADD(date, interval, unit)     -- e.g. DATE_ADD(created_at, 1, 'DAY')
//	DATE_ADD(date, iso8601_duration)   -- e.g. DATE_ADD(created_at, 'P1Y2M10D')
//
// The 2-argument form applies every component of one ISO 8601 duration
// string in a single shift, instead of requiring one 3-argument call per
// unit.
func evalDateAdd(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 2 && len(args) != 3 {
		return nil, fmt.Errorf("DATE_ADD expects (date, interval, unit) or (date, iso8601_duration)")
	}
	dateVal, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	t, err := parseDateTime(dateVal)
	if err != nil {
		return nil, err
	}

	if len(args) == 2 {
		durVal, err := evalExpr(env, args[1], row)
		if err != nil {
			return nil, err
		}
		durStr, ok := durVal.(string)
		if !ok {
			return nil, fmt.Errorf("DATE_ADD: 2-argument form expects an ISO 8601 duration string, got %T", durVal)
		}
		dur, err := parseISO8601Duration(durStr)
		if err != nil {
			return nil, fmt.Errorf("DATE_ADD: %w", err)
		}
		return dur.shift(t, false), nil
	}

	intervalVal, err := evalExpr(env, args[1], row)
	if err != nil {
		return nil, err
	}
	unitVal, err := evalExpr(env, args[2], row)
	if err != nil {
		return nil, err
	}
	interval, ok := numeric(intervalVal)
	if !ok {
		return nil, fmt.Errorf("DATE_ADD: interval must be numeric")
	}
	result, err := applyDateUnitShift(t, interval, strings.ToUpper(valueText(unitVal)))
	if err != nil {
		return nil, fmt.Errorf("DATE_ADD: %w", err)
	}
	return result, nil
}

// evalDateSub mirrors evalDateAdd's two call forms with the shift negated:
//
//	DATE_SUB(date, interval, unit)
//	DATE_SUB(date, iso8601_duration)
func evalDateSub(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 2 && len(args) != 3 {
		return nil, fmt.Errorf("DATE_SUB expects (date, interval, unit) or (date, iso8601_duration)")
	}
	dateVal, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	t, err := parseDateTime(dateVal)
	if err != nil {
		return nil, err
	}

	if len(args) == 2 {
		durVal, err := evalExpr(env, args[1], row)
		if err != nil {
			return nil, err
		}
		durStr, ok := durVal.(string)
		if !ok {
			return nil, fmt.Errorf("DATE_SUB: 2-argument form expects an ISO 8601 duration string, got %T", durVal)
		}
		dur, err := parseISO8601Duration(durStr)
		if err != nil {
			return nil, fmt.Errorf("DATE_SUB: %w", err)
		}
		return dur.shift(t, true), nil
	}

	intervalVal, err := evalExpr(env, args[1], row)
	if err != nil {
		return nil, err
	}
	unitVal, err := evalExpr(env, args[2], row)
	if err != nil {
		return nil, err
	}
	interval, ok := numeric(intervalVal)
	if !ok {
		return nil, fmt.Errorf("DATE_SUB: interval must be numeric")
	}
	interval = -interval // Negate for subtraction
	result, err := applyDateUnitShift(t, interval, strings.ToUpper(valueText(unitVal)))
	if err != nil {
		return nil, fmt.Errorf("DATE_SUB: %w", err)
	}
	return result, nil
}

// evalOverlaps implements OVERLAPS(start1, end1, start2, end2): whether the
// half-open ranges [start1, end1) and [start2, end2) share any instant.
// Half-open is this engine's existing range convention (used, for instance,
// by the raw-fastpath BETWEEN/range predicates) and is also the SQL:2011
// OVERLAPS predicate's own default: two ranges that only touch at an
// endpoint -- one range's end equal to the other's start -- do not overlap.
// A NULL argument makes the result NULL rather than a comparison error,
// matching this engine's general NULL-propagation rule for scalar
// functions. There is no infix `(start1, end1) OVERLAPS (start2, end2)`
// syntax here -- that needs row-constructor grammar this parser does not
// have -- so this is exposed as a plain 4-argument function instead.
func evalOverlaps(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 4 {
		return nil, fmt.Errorf("OVERLAPS expects 4 arguments: (start1, end1, start2, end2)")
	}
	var times [4]time.Time
	for i, a := range args {
		v, err := evalExpr(env, a, row)
		if err != nil {
			return nil, err
		}
		if v == nil {
			return nil, nil
		}
		t, err := parseDateTime(v)
		if err != nil {
			return nil, fmt.Errorf("OVERLAPS argument %d: %w", i+1, err)
		}
		times[i] = t
	}
	start1, end1, start2, end2 := times[0], times[1], times[2], times[3]
	// A genuinely reversed range (end before start) is almost certainly a
	// caller mistake, so it errors instead of silently normalizing the pair
	// the way some engines do. A zero-width range (start == end) is a valid
	// "instant" -- it is handled explicitly below, not by this check.
	if end1.Before(start1) {
		return nil, fmt.Errorf("OVERLAPS: end1 is before start1")
	}
	if end2.Before(start2) {
		return nil, fmt.Errorf("OVERLAPS: end2 is before start2")
	}

	// A zero-width range needs its own containment check. The general test
	// below (start1 < end2 && start2 < end1) only correctly detects overlap
	// between two positive-width ranges; fed a degenerate one, it silently
	// degrades into "is this point strictly interior to the other range",
	// which disagrees with that range's own half-open convention at its
	// start boundary -- a point exactly at the other range's start IS
	// contained in [start,end), but the general test would say false there.
	// SQL:2011's own OVERLAPS predicate special-cases instants for exactly
	// this reason. Two instants are defined here as never overlapping, even
	// when equal: each represents the empty set [x,x), and the intersection
	// of two empty sets is empty regardless of where they sit.
	if start1.Equal(end1) && start2.Equal(end2) {
		return false, nil
	}
	if start1.Equal(end1) {
		return !start1.Before(start2) && start1.Before(end2), nil
	}
	if start2.Equal(end2) {
		return !start2.Before(start1) && start2.Before(end1), nil
	}
	return start1.Before(end2) && start2.Before(end1), nil
}

// evalRangeMerge implements RANGE_MERGE(ranges): given an array of [start,
// end] pairs, returns the minimal set of non-overlapping [start, end] pairs
// covering the same instants, sorted by start. Touching ranges (one range's
// end equal to the next range's start) merge into one, consistent with
// evalOverlaps' half-open convention -- [1,2) and [2,3) merge into [1,3),
// the same way two contiguous calendar days should coalesce into one
// two-day span rather than being reported as a "gap-free" pair of separate
// ranges.
//
// The merge itself is a sort-by-start-then-sweep, extending the last kept
// range's end whenever the next range starts at or before it -- the same
// algorithm shape github.com/senseyeio/spaniel uses for its Union operation
// (not its code, and without spaniel's per-span configurable open/closed
// boundary type, which this engine has no use for yet).
func evalRangeMerge(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("RANGE_MERGE expects 1 argument: (ranges)")
	}
	arrVal, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	if arrVal == nil {
		return nil, nil
	}
	arr, ok := arrVal.([]any)
	if !ok {
		return nil, fmt.Errorf("RANGE_MERGE: argument must be an array of [start, end] pairs")
	}

	type timeRange struct{ start, end time.Time }
	ranges := make([]timeRange, 0, len(arr))
	for i, elem := range arr {
		pair, ok := elem.([]any)
		if !ok || len(pair) != 2 {
			return nil, fmt.Errorf("RANGE_MERGE: element %d is not a [start, end] pair", i)
		}
		start, err := parseDateTime(pair[0])
		if err != nil {
			return nil, fmt.Errorf("RANGE_MERGE: element %d start: %w", i, err)
		}
		end, err := parseDateTime(pair[1])
		if err != nil {
			return nil, fmt.Errorf("RANGE_MERGE: element %d end: %w", i, err)
		}
		if end.Before(start) {
			return nil, fmt.Errorf("RANGE_MERGE: element %d has end before start", i)
		}
		ranges = append(ranges, timeRange{start, end})
	}
	if len(ranges) == 0 {
		return []any{}, nil
	}

	// Plain sort.Slice, deliberately: RANGE_MERGE's input is one function
	// argument's worth of ranges (dozens at most in any realistic query, not
	// the thousands-of-rows scale the ORDER BY paths elsewhere in this engine
	// optimize for), where reflect.Swapper's overhead is not worth a concrete
	// sort.Interface type to avoid.
	sort.Slice(ranges, func(i, j int) bool { return ranges[i].start.Before(ranges[j].start) })

	merged := make([]timeRange, 0, len(ranges))
	merged = append(merged, ranges[0])
	for _, r := range ranges[1:] {
		last := &merged[len(merged)-1]
		if !r.start.After(last.end) {
			if r.end.After(last.end) {
				last.end = r.end
			}
			continue
		}
		merged = append(merged, r)
	}

	out := make([]any, len(merged))
	for i, r := range merged {
		out[i] = []any{r.start, r.end}
	}
	return out, nil
}
