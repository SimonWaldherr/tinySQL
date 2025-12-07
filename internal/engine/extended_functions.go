package engine

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"
)

// ==================== Date/Time Functions ====================

// evalInPeriodFunc checks if a date falls within a predefined period
func evalInPeriodFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalInPeriod(env, ex.Args, row)
}

func evalInPeriod(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("IN_PERIOD expects 2 arguments: (period, date)")
	}

	periodVal, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	period, ok := periodVal.(string)
	if !ok {
		return nil, fmt.Errorf("IN_PERIOD: period must be a string")
	}

	dateVal, err := evalExpr(env, args[1], row)
	if err != nil {
		return nil, err
	}

	// NULL dates return false
	if dateVal == nil {
		return false, nil
	}

	dateTime, err := parseTimeValue(dateVal)
	if err != nil {
		return nil, fmt.Errorf("IN_PERIOD: %v", err)
	}

	now := time.Now()
	period = strings.ToUpper(period)

	// Handle aliases
	switch period {
	case "YTD":
		period = "YEAR_TO_DATE"
	case "MTD":
		period = "MONTH_TO_DATE"
	case "QTD":
		period = "QUARTER_TO_DATE"
	case "L12M":
		period = "LAST_12_MONTHS"
	}

	switch period {
	case "TODAY":
		start := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
		end := start.AddDate(0, 0, 1)
		return dateTime.After(start) && dateTime.Before(end) || dateTime.Equal(start), nil

	case "YEAR_TO_DATE":
		start := time.Date(now.Year(), 1, 1, 0, 0, 0, 0, now.Location())
		return dateTime.After(start) && dateTime.Before(now) || dateTime.Equal(start), nil

	case "MONTH_TO_DATE":
		start := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, now.Location())
		return dateTime.After(start) && dateTime.Before(now) || dateTime.Equal(start), nil

	case "QUARTER_TO_DATE":
		q := ((int(now.Month()) - 1) / 3)
		start := time.Date(now.Year(), time.Month(q*3+1), 1, 0, 0, 0, 0, now.Location())
		return dateTime.After(start) && dateTime.Before(now) || dateTime.Equal(start), nil

	case "LAST_12_MONTHS":
		start := now.AddDate(0, -12, 0)
		return dateTime.After(start) && dateTime.Before(now) || dateTime.Equal(start), nil

	case "PREVIOUS_12_MONTHS":
		end := now.AddDate(0, -12, 0)
		start := end.AddDate(0, -12, 0)
		return dateTime.After(start) && dateTime.Before(end) || dateTime.Equal(start), nil

	case "CURRENT_QUARTER":
		q := ((int(now.Month()) - 1) / 3)
		start := time.Date(now.Year(), time.Month(q*3+1), 1, 0, 0, 0, 0, now.Location())
		end := start.AddDate(0, 3, 0)
		return dateTime.After(start) && dateTime.Before(end) || dateTime.Equal(start), nil

	case "PREVIOUS_QUARTER":
		q := ((int(now.Month()) - 1) / 3)
		start := time.Date(now.Year(), time.Month(q*3+1), 1, 0, 0, 0, 0, now.Location()).AddDate(0, -3, 0)
		end := start.AddDate(0, 3, 0)
		return dateTime.After(start) && dateTime.Before(end) || dateTime.Equal(start), nil

	case "NEXT_QUARTER":
		q := ((int(now.Month()) - 1) / 3)
		start := time.Date(now.Year(), time.Month(q*3+1), 1, 0, 0, 0, 0, now.Location()).AddDate(0, 3, 0)
		end := start.AddDate(0, 3, 0)
		return dateTime.After(start) && dateTime.Before(end) || dateTime.Equal(start), nil

	case "CURRENT_YEAR_FULL":
		start := time.Date(now.Year(), 1, 1, 0, 0, 0, 0, now.Location())
		end := start.AddDate(1, 0, 0)
		return dateTime.After(start) && dateTime.Before(end) || dateTime.Equal(start), nil

	case "LAST_YEAR_FULL":
		start := time.Date(now.Year()-1, 1, 1, 0, 0, 0, 0, now.Location())
		end := start.AddDate(1, 0, 0)
		return dateTime.After(start) && dateTime.Before(end) || dateTime.Equal(start), nil

	default:
		return nil, fmt.Errorf("IN_PERIOD: unsupported period '%s'", period)
	}
}

// evalExtractFunc extracts a date part from a date
func evalExtractFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalExtract(env, ex.Args, row)
}

func evalExtract(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("EXTRACT expects 2 arguments: (part, date)")
	}

	partVal, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	part, ok := partVal.(string)
	if !ok {
		return nil, fmt.Errorf("EXTRACT: part must be a string")
	}

	dateVal, err := evalExpr(env, args[1], row)
	if err != nil {
		return nil, err
	}

	if dateVal == nil {
		return nil, nil
	}

	dateTime, err := parseTimeValue(dateVal)
	if err != nil {
		return nil, fmt.Errorf("EXTRACT: %v", err)
	}

	part = strings.ToUpper(part)
	switch part {
	case "YEAR":
		return dateTime.Year(), nil
	case "MONTH":
		return int(dateTime.Month()), nil
	case "DAY":
		return dateTime.Day(), nil
	case "HOUR":
		return dateTime.Hour(), nil
	case "MINUTE":
		return dateTime.Minute(), nil
	case "SECOND":
		return dateTime.Second(), nil
	case "QUARTER":
		return ((int(dateTime.Month()) - 1) / 3) + 1, nil
	case "WEEK":
		_, week := dateTime.ISOWeek()
		return week, nil
	case "DAYOFWEEK", "DOW":
		return int(dateTime.Weekday()), nil
	case "DAYOFYEAR", "DOY":
		return dateTime.YearDay(), nil
	default:
		return nil, fmt.Errorf("EXTRACT: unsupported part '%s'", part)
	}
}

// evalDateTruncFunc truncates a date to the beginning of a period
func evalDateTruncFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalDateTrunc(env, ex.Args, row)
}

func evalDateTrunc(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("DATE_TRUNC expects 2 arguments: (unit, date)")
	}

	unitVal, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	unit, ok := unitVal.(string)
	if !ok {
		return nil, fmt.Errorf("DATE_TRUNC: unit must be a string")
	}

	dateVal, err := evalExpr(env, args[1], row)
	if err != nil {
		return nil, err
	}

	if dateVal == nil {
		return nil, nil
	}

	dateTime, err := parseTimeValue(dateVal)
	if err != nil {
		return nil, fmt.Errorf("DATE_TRUNC: %v", err)
	}

	unit = strings.ToUpper(unit)
	switch unit {
	case "YEAR":
		return time.Date(dateTime.Year(), 1, 1, 0, 0, 0, 0, dateTime.Location()), nil
	case "QUARTER":
		q := ((int(dateTime.Month()) - 1) / 3)
		return time.Date(dateTime.Year(), time.Month(q*3+1), 1, 0, 0, 0, 0, dateTime.Location()), nil
	case "MONTH":
		return time.Date(dateTime.Year(), dateTime.Month(), 1, 0, 0, 0, 0, dateTime.Location()), nil
	case "WEEK":
		// Start of week (Monday)
		weekday := int(dateTime.Weekday())
		if weekday == 0 {
			weekday = 7
		}
		daysBack := weekday - 1
		return dateTime.AddDate(0, 0, -daysBack).Truncate(24 * time.Hour), nil
	case "DAY":
		return dateTime.Truncate(24 * time.Hour), nil
	case "HOUR":
		return dateTime.Truncate(time.Hour), nil
	case "MINUTE":
		return dateTime.Truncate(time.Minute), nil
	case "SECOND":
		return dateTime.Truncate(time.Second), nil
	default:
		return nil, fmt.Errorf("DATE_TRUNC: unsupported unit '%s'", unit)
	}
}

// evalEOMonthFunc returns the end of month for a date with optional offset
func evalEOMonthFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalEOMonth(env, ex.Args, row)
}

func evalEOMonth(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) < 1 || len(args) > 2 {
		return nil, fmt.Errorf("EOMONTH expects 1 or 2 arguments: (date [, offset])")
	}

	dateVal, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}

	if dateVal == nil {
		return nil, nil
	}

	dateTime, err := parseTimeValue(dateVal)
	if err != nil {
		return nil, fmt.Errorf("EOMONTH: %v", err)
	}

	offset := 0
	if len(args) == 2 {
		offsetVal, err := evalExpr(env, args[1], row)
		if err != nil {
			return nil, err
		}
		offsetNum, ok := numeric(offsetVal)
		if !ok {
			return nil, fmt.Errorf("EOMONTH: offset must be numeric")
		}
		offset = int(offsetNum)
	}

	// Add offset months
	targetDate := dateTime.AddDate(0, offset, 0)

	// Get first day of next month, then subtract one day
	firstOfNextMonth := time.Date(targetDate.Year(), targetDate.Month()+1, 1, 0, 0, 0, 0, targetDate.Location())
	endOfMonth := firstOfNextMonth.AddDate(0, 0, -1)

	return endOfMonth, nil
}

// evalAddMonthsFunc adds a number of months to a date
func evalAddMonthsFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalAddMonths(env, ex.Args, row)
}

func evalAddMonths(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("ADD_MONTHS expects 2 arguments: (date, months)")
	}

	dateVal, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}

	if dateVal == nil {
		return nil, nil
	}

	dateTime, err := parseTimeValue(dateVal)
	if err != nil {
		return nil, fmt.Errorf("ADD_MONTHS: %v", err)
	}

	monthsVal, err := evalExpr(env, args[1], row)
	if err != nil {
		return nil, err
	}

	months, ok := numeric(monthsVal)
	if !ok {
		return nil, fmt.Errorf("ADD_MONTHS: months must be numeric")
	}

	return dateTime.AddDate(0, int(months), 0), nil
}

// ==================== Regex Functions ====================

// evalRegexpMatchFunc tests if a string matches a regex pattern
func evalRegexpMatchFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalRegexpMatch(env, ex.Args, row)
}

func evalRegexpMatch(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("REGEXP_MATCH expects 2 arguments: (string, pattern)")
	}

	strVal, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}

	patternVal, err := evalExpr(env, args[1], row)
	if err != nil {
		return nil, err
	}

	if strVal == nil || patternVal == nil {
		return false, nil
	}

	str := fmt.Sprintf("%v", strVal)
	pattern := fmt.Sprintf("%v", patternVal)

	matched, err := regexp.MatchString(pattern, str)
	if err != nil {
		return nil, fmt.Errorf("REGEXP_MATCH: %v", err)
	}

	return matched, nil
}

// evalRegexpExtractFunc extracts the first match of a regex pattern
func evalRegexpExtractFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalRegexpExtract(env, ex.Args, row)
}

func evalRegexpExtract(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("REGEXP_EXTRACT expects 2 arguments: (string, pattern)")
	}

	strVal, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}

	patternVal, err := evalExpr(env, args[1], row)
	if err != nil {
		return nil, err
	}

	if strVal == nil || patternVal == nil {
		return nil, nil
	}

	str := fmt.Sprintf("%v", strVal)
	pattern := fmt.Sprintf("%v", patternVal)

	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, fmt.Errorf("REGEXP_EXTRACT: %v", err)
	}

	match := re.FindString(str)
	if match == "" {
		return nil, nil
	}

	return match, nil
}

// evalRegexpReplaceFunc replaces matches of a regex pattern
func evalRegexpReplaceFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalRegexpReplace(env, ex.Args, row)
}

func evalRegexpReplace(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 3 {
		return nil, fmt.Errorf("REGEXP_REPLACE expects 3 arguments: (string, pattern, replacement)")
	}

	strVal, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}

	patternVal, err := evalExpr(env, args[1], row)
	if err != nil {
		return nil, err
	}

	replVal, err := evalExpr(env, args[2], row)
	if err != nil {
		return nil, err
	}

	if strVal == nil {
		return nil, nil
	}

	str := fmt.Sprintf("%v", strVal)
	pattern := fmt.Sprintf("%v", patternVal)
	replacement := fmt.Sprintf("%v", replVal)

	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, fmt.Errorf("REGEXP_REPLACE: %v", err)
	}

	return re.ReplaceAllString(str, replacement), nil
}

// ==================== Array Functions ====================

// evalSplitFunc splits a string into an array
func evalSplitFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalSplit(env, ex.Args, row)
}

func evalSplit(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("SPLIT expects 2 arguments: (string, delimiter)")
	}

	strVal, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}

	delimVal, err := evalExpr(env, args[1], row)
	if err != nil {
		return nil, err
	}

	if strVal == nil {
		return nil, nil
	}

	str := fmt.Sprintf("%v", strVal)
	delim := fmt.Sprintf("%v", delimVal)

	parts := strings.Split(str, delim)
	result := make([]any, len(parts))
	for i, p := range parts {
		result[i] = p
	}

	return result, nil
}

// evalFirstFunc returns the first element of an array
func evalFirstFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalFirst(env, ex.Args, row)
}

func evalFirst(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("FIRST expects 1 argument")
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
		return nil, fmt.Errorf("FIRST: argument must be an array")
	}

	if len(arr) == 0 {
		return nil, nil
	}

	return arr[0], nil
}

// evalLastFunc returns the last element of an array
func evalLastFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalLast(env, ex.Args, row)
}

func evalLast(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("LAST expects 1 argument")
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
		return nil, fmt.Errorf("LAST: argument must be an array")
	}

	if len(arr) == 0 {
		return nil, nil
	}

	return arr[len(arr)-1], nil
}

// evalArrayLengthFunc returns the length of an array
func evalArrayLengthFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalArrayLength(env, ex.Args, row)
}

func evalArrayLength(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("ARRAY_LENGTH expects 1 argument")
	}

	arrVal, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}

	if arrVal == nil {
		return 0, nil
	}

	arr, ok := arrVal.([]any)
	if !ok {
		return nil, fmt.Errorf("ARRAY_LENGTH: argument must be an array")
	}

	return len(arr), nil
}

// evalArrayContainsFunc checks if an array contains a value
func evalArrayContainsFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalArrayContains(env, ex.Args, row)
}

func evalArrayContains(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("ARRAY_CONTAINS expects 2 arguments: (array, value)")
	}

	arrVal, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}

	searchVal, err := evalExpr(env, args[1], row)
	if err != nil {
		return nil, err
	}

	if arrVal == nil {
		return false, nil
	}

	arr, ok := arrVal.([]any)
	if !ok {
		return nil, fmt.Errorf("ARRAY_CONTAINS: first argument must be an array")
	}

	for _, elem := range arr {
		cmp, err := compare(elem, searchVal)
		if err == nil && cmp == 0 {
			return true, nil
		}
	}

	return false, nil
}

// evalArrayJoinFunc joins array elements into a string
func evalArrayJoinFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalArrayJoin(env, ex.Args, row)
}

func evalArrayJoin(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) < 1 || len(args) > 2 {
		return nil, fmt.Errorf("ARRAY_JOIN expects 1 or 2 arguments: (array [, delimiter])")
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
		return nil, fmt.Errorf("ARRAY_JOIN: first argument must be an array")
	}

	delimiter := ","
	if len(args) == 2 {
		delimVal, err := evalExpr(env, args[1], row)
		if err != nil {
			return nil, err
		}
		delimiter = fmt.Sprintf("%v", delimVal)
	}

	parts := make([]string, len(arr))
	for i, elem := range arr {
		parts[i] = fmt.Sprintf("%v", elem)
	}

	return strings.Join(parts, delimiter), nil
}

// evalArrayDistinctFunc removes duplicates from an array
func evalArrayDistinctFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalArrayDistinct(env, ex.Args, row)
}

func evalArrayDistinct(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("ARRAY_DISTINCT expects 1 argument")
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
		return nil, fmt.Errorf("ARRAY_DISTINCT: argument must be an array")
	}

	seen := make(map[string]bool)
	result := make([]any, 0)

	for _, elem := range arr {
		key := fmt.Sprintf("%v", elem)
		if !seen[key] {
			seen[key] = true
			result = append(result, elem)
		}
	}

	return result, nil
}

// evalArraySortFunc sorts an array
func evalArraySortFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalArraySort(env, ex.Args, row)
}

func evalArraySort(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("ARRAY_SORT expects 1 argument")
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
		return nil, fmt.Errorf("ARRAY_SORT: argument must be an array")
	}

	// Copy array to avoid modifying original
	result := make([]any, len(arr))
	copy(result, arr)

	// Sort using compare
	sort.Slice(result, func(i, j int) bool {
		cmp, err := compare(result[i], result[j])
		if err != nil {
			return false // Keep original order if comparison fails
		}
		return cmp < 0
	})

	return result, nil
}

// ==================== Window Functions (Basic Stubs) ====================

// evalRowNumberFunc returns row number (basic implementation without OVER clause)
func evalRowNumberFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if ex.Over == nil {
		return nil, fmt.Errorf("ROW_NUMBER requires OVER clause")
	}
	// Build partition key for each row and ordering within partition
	rows := env.windowRows
	if rows == nil {
		return nil, fmt.Errorf("no window rows available")
	}
	// Compute partition groups
	groups := make(map[string][]int)
	for i, r := range rows {
		var parts []string
		for _, p := range ex.Over.PartitionBy {
			v, err := evalExpr(env, p, r)
			if err != nil {
				return nil, err
			}
			parts = append(parts, fmt.Sprintf("%v", v))
		}
		key := strings.Join(parts, "|")
		groups[key] = append(groups[key], i)
	}
	// For current row, find its group
	var curKey string
	{
		var parts []string
		for _, p := range ex.Over.PartitionBy {
			v, err := evalExpr(env, p, row)
			if err != nil {
				return nil, err
			}
			parts = append(parts, fmt.Sprintf("%v", v))
		}
		curKey = strings.Join(parts, "|")
	}
	idxs := groups[curKey]
	// Order idxs according to ORDER BY if present
	if len(ex.Over.OrderBy) > 0 {
		sort.SliceStable(idxs, func(i, j int) bool {
			a := rows[idxs[i]]
			b := rows[idxs[j]]
			for _, oi := range ex.Over.OrderBy {
				av, _ := getVal(a, oi.Col)
				bv, _ := getVal(b, oi.Col)
				cmp := compareForOrder(av, bv, oi.Desc)
				if cmp == 0 {
					continue
				}
				if oi.Desc {
					return cmp > 0
				}
				return cmp < 0
			}
			return false
		})
	}
	// find position of env.windowIndex in idxs
	curPos := -1
	for pos, ii := range idxs {
		if ii == env.windowIndex {
			curPos = pos
			break
		}
	}
	if curPos == -1 {
		return nil, fmt.Errorf("current row not found in partition")
	}
	return curPos + 1, nil
}

// evalLagFunc returns the value from a previous row (basic stub)
func evalLagFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if ex.Over == nil {
		return nil, fmt.Errorf("LAG requires OVER clause")
	}
	rows := env.windowRows
	if rows == nil {
		return nil, fmt.Errorf("no window rows available")
	}
	// default offset 1
	offset := 1
	if len(ex.Args) >= 2 {
		offVal, err := evalExpr(env, ex.Args[1], row)
		if err != nil {
			return nil, err
		}
		if offVal == nil {
			offset = 1
		} else if n, ok := offVal.(int); ok {
			offset = n
		} else if f, ok := numeric(offVal); ok {
			offset = int(f)
		} else {
			return nil, fmt.Errorf("LAG offset must be numeric")
		}
	}
	// Compute partition groups same as ROW_NUMBER
	groups := make(map[string][]int)
	for i, r := range rows {
		var parts []string
		for _, p := range ex.Over.PartitionBy {
			v, err := evalExpr(env, p, r)
			if err != nil {
				return nil, err
			}
			parts = append(parts, fmt.Sprintf("%v", v))
		}
		key := strings.Join(parts, "|")
		groups[key] = append(groups[key], i)
	}
	var curKey string
	{
		var parts []string
		for _, p := range ex.Over.PartitionBy {
			v, err := evalExpr(env, p, row)
			if err != nil {
				return nil, err
			}
			parts = append(parts, fmt.Sprintf("%v", v))
		}
		curKey = strings.Join(parts, "|")
	}
	idxs := groups[curKey]
	if len(ex.Over.OrderBy) > 0 {
		sort.SliceStable(idxs, func(i, j int) bool {
			a := rows[idxs[i]]
			b := rows[idxs[j]]
			for _, oi := range ex.Over.OrderBy {
				av, _ := getVal(a, oi.Col)
				bv, _ := getVal(b, oi.Col)
				cmp := compareForOrder(av, bv, oi.Desc)
				if cmp == 0 {
					continue
				}
				if oi.Desc {
					return cmp > 0
				}
				return cmp < 0
			}
			return false
		})
	}
	// find current position
	curPos := -1
	for pos, ii := range idxs {
		if ii == env.windowIndex {
			curPos = pos
			break
		}
	}
	if curPos == -1 {
		return nil, fmt.Errorf("current row not found in partition")
	}
	target := curPos - offset
	if target < 0 {
		return nil, nil
	}
	// evaluate value expression (first arg) against target row
	if len(ex.Args) == 0 {
		return nil, fmt.Errorf("LAG requires at least 1 argument")
	}
	targetRow := rows[idxs[target]]
	// ensure env.windowIndex reflects evaluated row
	env.windowIndex = idxs[target]
	return evalExpr(env, ex.Args[0], targetRow)
}

// evalLeadFunc returns the value from a following row (basic stub)
func evalLeadFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if ex.Over == nil {
		return nil, fmt.Errorf("LEAD requires OVER clause")
	}
	rows := env.windowRows
	if rows == nil {
		return nil, fmt.Errorf("no window rows available")
	}
	// default offset 1
	offset := 1
	if len(ex.Args) >= 2 {
		offVal, err := evalExpr(env, ex.Args[1], row)
		if err != nil {
			return nil, err
		}
		if offVal == nil {
			offset = 1
		} else if n, ok := offVal.(int); ok {
			offset = n
		} else if f, ok := numeric(offVal); ok {
			offset = int(f)
		} else {
			return nil, fmt.Errorf("LEAD offset must be numeric")
		}
	}
	// Compute partition groups
	groups := make(map[string][]int)
	for i, r := range rows {
		var parts []string
		for _, p := range ex.Over.PartitionBy {
			v, err := evalExpr(env, p, r)
			if err != nil {
				return nil, err
			}
			parts = append(parts, fmt.Sprintf("%v", v))
		}
		key := strings.Join(parts, "|")
		groups[key] = append(groups[key], i)
	}
	var curKey string
	{
		var parts []string
		for _, p := range ex.Over.PartitionBy {
			v, err := evalExpr(env, p, row)
			if err != nil {
				return nil, err
			}
			parts = append(parts, fmt.Sprintf("%v", v))
		}
		curKey = strings.Join(parts, "|")
	}
	idxs := groups[curKey]
	if len(ex.Over.OrderBy) > 0 {
		sort.SliceStable(idxs, func(i, j int) bool {
			a := rows[idxs[i]]
			b := rows[idxs[j]]
			for _, oi := range ex.Over.OrderBy {
				av, _ := getVal(a, oi.Col)
				bv, _ := getVal(b, oi.Col)
				cmp := compareForOrder(av, bv, oi.Desc)
				if cmp == 0 {
					continue
				}
				if oi.Desc {
					return cmp > 0
				}
				return cmp < 0
			}
			return false
		})
	}
	// find current position
	curPos := -1
	for pos, ii := range idxs {
		if ii == env.windowIndex {
			curPos = pos
			break
		}
	}
	if curPos == -1 {
		return nil, fmt.Errorf("current row not found in partition")
	}
	target := curPos + offset
	if target >= len(idxs) {
		return nil, nil
	}
	if len(ex.Args) == 0 {
		return nil, fmt.Errorf("LEAD requires at least 1 argument")
	}
	targetRow := rows[idxs[target]]
	env.windowIndex = idxs[target]
	return evalExpr(env, ex.Args[0], targetRow)
}

// evalMovingSumFunc calculates moving sum (basic stub)
func evalMovingSumFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	// TODO: Implement proper window function
	return nil, fmt.Errorf("MOVING_SUM requires OVER clause (not yet implemented)")
}

// evalMovingAvgFunc calculates moving average (basic stub)
func evalMovingAvgFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	// TODO: Implement proper window function
	return nil, fmt.Errorf("MOVING_AVG requires OVER clause (not yet implemented)")
}

// ==================== Value at MIN/MAX Functions ====================

// evalMinByFunc returns the value from one column where another column is minimum
// Usage: MIN_BY(value_column, order_column)
func evalMinByFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return nil, fmt.Errorf("MIN_BY is an aggregate function and requires GROUP BY context")
}

// evalMaxByFunc returns the value from one column where another column is maximum
// Usage: MAX_BY(value_column, order_column)
func evalMaxByFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return nil, fmt.Errorf("MAX_BY is an aggregate function and requires GROUP BY context")
}

// evalFirstValueFunc returns the first value in an ordered set
// Usage: FIRST_VALUE(column) - needs proper window function support
func evalFirstValueFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return nil, fmt.Errorf("FIRST_VALUE requires OVER clause (not yet implemented)")
}

// evalLastValueFunc returns the last value in an ordered set
// Usage: LAST_VALUE(column) - needs proper window function support
func evalLastValueFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return nil, fmt.Errorf("LAST_VALUE requires OVER clause (not yet implemented)")
}

// evalArgMinFunc returns value where comparison column is minimum (alias for MIN_BY)
func evalArgMinFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalMinByFunc(env, ex, row)
}

// evalArgMaxFunc returns value where comparison column is maximum (alias for MAX_BY)
func evalArgMaxFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalMaxByFunc(env, ex, row)
}

// getExtendedFunctions returns the map of extended function handlers
func getExtendedFunctions() map[string]funcHandler {
	return map[string]funcHandler{
		// New date functions
		"IN_PERIOD":  evalInPeriodFunc,
		"EXTRACT":    evalExtractFunc,
		"DATE_TRUNC": evalDateTruncFunc,
		"EOMONTH":    evalEOMonthFunc,
		"ADD_MONTHS": evalAddMonthsFunc,
		// Regex functions
		"REGEXP_MATCH":   evalRegexpMatchFunc,
		"REGEXP_EXTRACT": evalRegexpExtractFunc,
		"REGEXP_REPLACE": evalRegexpReplaceFunc,
		// Array functions
		"SPLIT":          evalSplitFunc,
		"FIRST":          evalFirstFunc,
		"LAST":           evalLastFunc,
		"ARRAY_LENGTH":   evalArrayLengthFunc,
		"ARRAY_CONTAINS": evalArrayContainsFunc,
		"IN_ARRAY":       evalArrayContainsFunc,
		"ARRAY_JOIN":     evalArrayJoinFunc,
		"ARRAY_DISTINCT": evalArrayDistinctFunc,
		"ARRAY_SORT":     evalArraySortFunc,
		// Window functions (basic stubs for now)
		"ROW_NUMBER": evalRowNumberFunc,
		"LAG":        evalLagFunc,
		"LEAD":       evalLeadFunc,
		"MOVING_SUM": evalMovingSumFunc,
		"MOVING_AVG": evalMovingAvgFunc,
		// Value at MIN/MAX functions
		"MIN_BY":      evalMinByFunc,
		"MAX_BY":      evalMaxByFunc,
		"ARG_MIN":     evalArgMinFunc,
		"ARG_MAX":     evalArgMaxFunc,
		"FIRST_VALUE": evalFirstValueFunc,
		"LAST_VALUE":  evalLastValueFunc,
		// IO functions
		"FILE": evalFileFunc,
		"HTTP": evalHTTPFunc,
		// Transform functions
		"GUNZIP":        evalGunzipFunc,
		"UNZIP":         evalGunzipFunc,
		"GZIP":          evalGzipFunc,
		"BASE64_ENCODE": evalBase64EncodeFunc,
		// Table-valued function names (scalar stubs)
		"TABLE_FROM_JSON":       evalTableFromJSONScalar,
		"TABLE_FROM_JSON_LINES": evalTableFromJSONLinesScalar,
		"TABLE_FROM_CSV":        evalTableFromCSVScalar,
	}
}
