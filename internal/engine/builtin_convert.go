package engine

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

type conversionTarget struct {
	name   string
	length int
}

func parseConversionTarget(expr Expr) (conversionTarget, error) {
	var name string
	switch target := expr.(type) {
	case *Literal:
		name, _ = target.Val.(string)
	case *VarRef:
		name = target.Name
	}
	name = strings.ToUpper(strings.TrimSpace(name))
	base, suffix, hasLength := strings.Cut(name, "(")
	target := conversionTarget{name: base}
	if hasLength {
		if !strings.HasSuffix(suffix, ")") {
			return target, fmt.Errorf("invalid type length: %s", name)
		}
		length, err := strconv.Atoi(suffix[:len(suffix)-1])
		if err != nil || length < 1 || length > 8000 {
			return target, fmt.Errorf("type length must be between 1 and 8000")
		}
		switch base {
		case "CHAR", "VARCHAR", "NCHAR", "NVARCHAR":
			target.length = length
		default:
			return target, fmt.Errorf("type %s does not support a length", base)
		}
	}
	switch base {
	case "TEXT", "STRING", "VARCHAR", "CHAR", "NVARCHAR", "NCHAR", "INT", "INTEGER", "BIGINT",
		"FLOAT", "REAL", "DOUBLE", "NUMERIC", "BOOL", "BOOLEAN", "GEOMETRY", "GEOM",
		"DATE", "DATETIME", "DATETIME2", "TIMESTAMP", "TIME":
		return target, nil
	default:
		return target, fmt.Errorf("unsupported conversion type: %s", base)
	}
}

func evalConvertFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalConvertCall(ex, func(arg Expr) (any, error) { return evalExpr(env, arg, row) })
}

func evalConvertCall(ex *FuncCall, eval func(Expr) (any, error)) (any, error) {
	if len(ex.Args) != 2 && len(ex.Args) != 3 {
		return nil, fmt.Errorf("%s expects (type, value [, style])", ex.Name)
	}
	target, err := parseConversionTarget(ex.Args[0])
	if err != nil {
		return nil, fmt.Errorf("%s: %w", ex.Name, err)
	}
	value, err := eval(ex.Args[1])
	if err != nil {
		return nil, err // TRY_CONVERT must not hide errors in its expression.
	}
	style := -1
	if len(ex.Args) == 3 {
		input, err := eval(ex.Args[2])
		if err != nil || input == nil {
			return nil, err
		}
		number, ok := numeric(input)
		if !ok || math.IsNaN(number) || number < 0 || number > 127 || math.Trunc(number) != number {
			return nil, fmt.Errorf("%s: style must be a supported integer", ex.Name)
		}
		style = int(number)
		if _, _, ok := conversionDateLayout(style); !ok {
			return nil, fmt.Errorf("%s: unsupported date style %d", ex.Name, style)
		}
	}
	if value == nil {
		return nil, nil
	}
	if strings.EqualFold(ex.Name, "TRY_CONVERT") && style < 0 && (target.name == "INT" || target.name == "INTEGER" || target.name == "BIGINT") {
		result, _ := conversionIntegerValue(value)
		return result, nil
	}
	result, err := convertValue(value, target, style)
	if err != nil {
		if strings.EqualFold(ex.Name, "TRY_CONVERT") {
			return nil, nil
		}
		return nil, fmt.Errorf("%s: %w", ex.Name, err)
	}
	return result, nil
}

func convertValue(value any, target conversionTarget, style int) (any, error) {
	switch target.name {
	case "DATE", "DATETIME", "DATETIME2", "TIMESTAMP", "TIME":
		date, ok := value.(time.Time)
		if !ok {
			text, ok := value.(string)
			if !ok {
				return nil, fmt.Errorf("cannot convert %T to %s", value, target.name)
			}
			var err error
			if style >= 0 {
				layout, _, _ := conversionDateLayout(style)
				date, err = time.Parse(layout, strings.TrimSpace(text))
			} else {
				date, err = parseDateTime(strings.TrimSpace(text))
			}
			if err != nil {
				return nil, err
			}
		}
		if target.name == "DATE" {
			year, month, day := date.Date()
			return time.Date(year, month, day, 0, 0, 0, 0, date.Location()), nil
		}
		if target.name == "TIME" {
			return date.Format("15:04:05.999999999"), nil
		}
		return date, nil
	case "TEXT", "STRING", "VARCHAR", "CHAR", "NVARCHAR", "NCHAR":
		var text string
		if style >= 0 {
			date, ok := value.(time.Time)
			if !ok {
				return nil, fmt.Errorf("date styles require a datetime input when converting to text")
			}
			_, layout, _ := conversionDateLayout(style)
			text = date.Format(layout)
		} else {
			text = valueText(value)
		}
		if target.length != 0 {
			count := 0
			for pos := range text {
				if count == target.length {
					text = strings.Clone(text[:pos])
					break
				}
				count++
			}
			if (target.name == "CHAR" || target.name == "NCHAR") && count < target.length {
				text += strings.Repeat(" ", target.length-count)
			}
		}
		return text, nil
	}
	if style >= 0 {
		return nil, fmt.Errorf("date styles require a date/time or text target")
	}
	switch target.name {
	case "INT", "INTEGER", "BIGINT":
		return convertInteger(value)
	case "FLOAT", "REAL", "DOUBLE", "NUMERIC":
		result, err := coerceToFloat(value)
		if err != nil {
			return nil, err
		}
		f := result.(float64)
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return nil, fmt.Errorf("nonfinite floating-point value")
		}
		return f, nil
	case "BOOL", "BOOLEAN":
		if text, ok := value.(string); ok {
			switch strings.ToLower(strings.TrimSpace(text)) {
			case "true", "1", "t", "yes":
				return true, nil
			case "false", "0", "f", "no":
				return false, nil
			default:
				return nil, fmt.Errorf("invalid boolean value")
			}
		}
		return coerceToBool(value)
	default:
		return castValue(value, target.name)
	}
}

// Check the exclusive float bound before conversion; float64(MaxInt64) rounds
// up to 2^63. Native int64 values avoid a float round trip altogether.
func convertInteger(value any) (any, error) {
	if result, ok := conversionIntegerValue(value); ok {
		return result, nil
	}
	return nil, fmt.Errorf("cannot convert %v to INT: invalid or out of range", value)
}

func conversionIntegerValue(value any) (any, bool) {
	switch v := value.(type) {
	case int, int64:
		return value, true
	case string:
		n, ok := parseConversionInteger(strings.TrimSpace(v))
		if !ok {
			return nil, false
		}
		if n >= math.MinInt && n <= math.MaxInt {
			return int(n), true
		}
		return n, true
	case float64:
		if math.IsNaN(v) || v < -0x1p63 || v >= 0x1p63 {
			return nil, false
		}
		return int64(v), true
	case bool:
		if v {
			return 1, true
		}
		return 0, true
	default:
		return nil, false
	}
}

// Dirty integer columns are a common TRY_CONVERT input. Reject them without
// allocating strconv.NumError values that would immediately be discarded.
func parseConversionInteger(text string) (int64, bool) {
	if text == "" {
		return 0, false
	}
	negative := text[0] == '-'
	if negative || text[0] == '+' {
		text = text[1:]
		if text == "" {
			return 0, false
		}
	}
	limit := uint64(math.MaxInt64)
	if negative {
		limit++
	}
	var value uint64
	for i := range text {
		digit := text[i] - '0'
		if digit > 9 || value > limit/10 || value == limit/10 && uint64(digit) > limit%10 {
			return 0, false
		}
		value = value*10 + uint64(digit)
	}
	if negative {
		return -int64(value), true
	}
	return int64(value), true
}

// A fixed style selects one parser instead of trying unrelated date layouts.
// The subset deliberately excludes two-digit years and alternate calendars.
func conversionDateLayout(style int) (input, output string, ok bool) {
	switch style {
	case 23:
		return "2006-01-02", "2006-01-02", true
	case 101:
		return "01/02/2006", "01/02/2006", true
	case 102:
		return "2006.01.02", "2006.01.02", true
	case 103:
		return "02/01/2006", "02/01/2006", true
	case 104:
		return "02.01.2006", "02.01.2006", true
	case 105:
		return "02-01-2006", "02-01-2006", true
	case 110:
		return "01-02-2006", "01-02-2006", true
	case 111:
		return "2006/01/02", "2006/01/02", true
	case 112:
		return "20060102", "20060102", true
	case 8, 24, 108:
		return "15:04:05", "15:04:05", true
	case 20, 120:
		return "2006-01-02 15:04:05", "2006-01-02 15:04:05", true
	case 21, 25, 121:
		return "2006-01-02 15:04:05.999999999", "2006-01-02 15:04:05.000", true
	case 126:
		return "2006-01-02T15:04:05.999999999", "2006-01-02T15:04:05.999", true
	case 127:
		return time.RFC3339Nano, time.RFC3339Nano, true
	default:
		return "", "", false
	}
}
