// The scalar function registry: the name-to-handler table the evaluator looks
// up, and the thin adapters that give each function a uniform signature. The
// implementations live in the builtin_*.go files.
package engine

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

type funcHandler func(env ExecEnv, ex *FuncCall, row Row) (any, error)

func getBuiltinFunctions() map[string]funcHandler {
	return map[string]funcHandler{
		"ROW_TO_TEXT":       evalRowToTextFunc,
		"COALESCE":          evalCoalesceFunc,
		"NVL":               evalCoalesceFunc,
		"IFNULL":            evalCoalesceFunc,
		"NULLIF":            evalNullifFunc,
		"ISNULL":            evalIsNullFuncWrapper,
		"JSON_GET":          evalJSONGetFunc,
		"JSON_SET":          evalJSONExtendedFunc,
		"JSON_EXTRACT":      evalJSONExtendedFunc,
		"COUNT":             evalCountSingle,
		"SUM":               evalAggregateSingle,
		"AVG":               evalAggregateSingle,
		"MIN":               evalAggregateSingle,
		"MAX":               evalAggregateSingle,
		"NOW":               evalNowFunc,
		"GETDATE":           evalNowFunc,
		"CURRENT_TIME":      evalCurrentTimeFunc,
		"CURRENT_TIMESTAMP": evalNowFunc,
		"CURRENT_DATE":      evalCurrentDateFunc,
		"TODAY":             evalTodayFunc,
		"FROM_TIMESTAMP":    evalFromTimestampFunc,
		"TIMESTAMP":         evalTimestampFunc,
		"DATEDIFF":          evalDateDiff,
		"LTRIM":             evalLTrimFunc,
		"RTRIM":             evalRTrimFunc,
		"TRIM":              evalTrimFunc,
		"UPPER":             evalUpperFunc,
		"LOWER":             evalLowerFunc,
		"CONCAT":            evalConcatFunc,
		"CONCAT_WS":         evalConcatWsFunc,
		"LENGTH":            evalLengthFunc,
		"LEN":               evalLengthFunc,
		"SUBSTRING":         evalSubstringFunc,
		"SUBSTR":            evalSubstringFunc,
		"MD5":               evalMD5Func,
		"SHA1":              evalSHA1Func,
		"SHA256":            evalSHA256Func,
		"SHA512":            evalSHA512Func,
		"BASE64":            evalBase64Func,
		"BASE64_DECODE":     evalBase64DecodeFunc,
		"LEFT":              evalLeftFunc,
		"RIGHT":             evalRightFunc,
		"CAST":              evalCastFunc,
		"REPLACE":           evalReplaceFunc,
		"INSTR":             evalInstrFunc,
		"LOCATE":            evalLocateFunc,
		"POSITION":          evalPositionFunc,
		"ABS":               evalAbsFunc,
		"ROUND":             evalRoundFunc,
		"FLOOR":             evalFloorFunc,
		"CEIL":              evalCeilFunc,
		"CEILING":           evalCeilFunc,
		"REVERSE":           evalReverseFunc,
		"REPEAT":            evalRepeatFunc,
		"PRINTF":            evalPrintfFunc,
		"FORMAT":            evalPrintfFunc,
		"CHAR_LENGTH":       evalLengthFunc,
		"LPAD":              evalLpadFunc,
		"RPAD":              evalRpadFunc,
		"GREATEST":          evalGreatestFunc,
		"LEAST":             evalLeastFunc,
		"IF":                evalIfFunc,
		"IIF":               evalIfFunc,
		"STRFTIME":          evalStrftimeFunc,
		"DATE":              evalDateFunc,
		"TIME":              evalTimeFunc,
		"DATETIME":          evalDatetimeFunc,
		"JULIANDAY":         evalJuliandayFunc,
		"UNIXEPOCH":         evalUnixepochFunc,
		"YEAR":              evalYearFunc,
		"MONTH":             evalMonthFunc,
		"DAY":               evalDayFunc,
		"HOUR":              evalHourFunc,
		"MINUTE":            evalMinuteFunc,
		"SECOND":            evalSecondFunc,
		"RANDOM":            evalRandomFunc,
		"RAND":              evalRandomFunc,
		// Math functions
		"MOD":      evalModFunc,
		"POWER":    evalPowerFunc,
		"POW":      evalPowerFunc,
		"SQRT":     evalSqrtFunc,
		"LOG":      evalLogFunc,
		"LN":       evalLnFunc,
		"LOG10":    evalLog10Func,
		"LOG2":     evalLog2Func,
		"EXP":      evalExpFunc,
		"SIGN":     evalSignFunc,
		"TRUNCATE": evalTruncateFunc,
		"TRUNC":    evalTruncateFunc,
		"PI":       evalPiFunc,
		"SIN":      evalSinFunc,
		"COS":      evalCosFunc,
		"TAN":      evalTanFunc,
		"ASIN":     evalAsinFunc,
		"ACOS":     evalAcosFunc,
		"ATAN":     evalAtanFunc,
		"ATAN2":    evalAtan2Func,
		"DEGREES":  evalDegreesFunc,
		"RADIANS":  evalRadiansFunc,
		// String functions
		"SPACE":      evalSpaceFunc,
		"ASCII":      evalAsciiFunc,
		"CHAR":       evalCharFunc,
		"CHR":        evalCharFunc,
		"INITCAP":    evalInitcapFunc,
		"SPLIT_PART": evalSplitPartFunc,
		"SOUNDEX":    evalSoundexFunc,
		"QUOTE":      evalQuoteFunc,
		"HEX":        evalHexFunc,
		"UNHEX":      evalUnhexFunc,
		// Additional functions
		"UUID":        evalUuidFunc,
		"TYPEOF":      evalTypeofFunc,
		"VERSION":     evalVersionFunc,
		"DAYOFWEEK":   evalDayOfWeekFunc,
		"DAYOFYEAR":   evalDayOfYearFunc,
		"WEEKOFYEAR":  evalWeekOfYearFunc,
		"QUARTER":     evalQuarterFunc,
		"DATE_ADD":    evalDateAddFunc,
		"DATE_SUB":    evalDateSubFunc,
		"DATEADD":     evalDateAddFunc,
		"DATESUB":     evalDateSubFunc,
		"OVERLAPS":    evalOverlapsFunc,
		"RANGE_MERGE": evalRangeMergeFunc,
	}
}

func evalFuncCall(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	// Check if this is a window function call
	if ex.Over != nil {
		// Window functions need access to all rows, not just current row
		// This will be handled specially during SELECT execution
		// For now, return an error if accessed outside window context
		if env.windowRows == nil {
			return nil, fmt.Errorf("window function %s used outside window context", ex.Name)
		}
		return evalWindowFunction(env, ex, row)
	}

	if h := boundFuncHandler(ex); h != nil {
		// Resolved once at parse time; skips the registry lookup per call.
		return h(env, ex, row)
	}
	builtinFunctions := getAllFunctions()
	if handler, ok := builtinFunctions[ex.Name]; ok {
		return handler(env, ex, row)
	}
	if handler, ok := builtinFunctions[strings.ToUpper(ex.Name)]; ok {
		return handler(env, ex, row)
	}
	return nil, fmt.Errorf("unknown function: %s", ex.Name)
}

// bindFuncHandler resolves a parsed call's registry handler once. Parse-time
// resolution keeps evaluation free of both the map lookup and any lazy-write
// race — the field is only ever written here, before the statement is shared.
// Unknown names stay unresolved and produce their error at evaluation, as
// before. See FuncCall.handlerIdx for why this is an index, not a func value.
func bindFuncHandler(fc *FuncCall) *FuncCall {
	if idx, ok := funcHandlerIndex()[fc.Name]; ok {
		fc.handlerIdx = idx
	}
	return fc
}

// boundFuncHandler returns the handler bound at parse time, or nil when the
// call was not bound (hand-constructed node, or an unknown function name).
//
// It reads allFuncTable directly rather than through funcHandlerTable(),
// deliberately: this runs once per function call per row, and going through
// the accessor would re-enter getAllFunctions() — a sync.Once check — on
// every evaluation. A non-zero handlerIdx is itself proof the table is built,
// because only bindFuncHandler sets it and it obtains the index from
// funcHandlerIndex(), which completes the Once first. The table write
// therefore happens-before the handlerIdx write, which happens-before this
// node becoming reachable by any reader.
func boundFuncHandler(ex *FuncCall) funcHandler {
	if ex.handlerIdx <= 0 {
		return nil
	}
	return allFuncTable[ex.handlerIdx-1]
}

// Wrapper functions to match funcHandler signature
func evalCoalesceFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalCoalesce(env, ex.Args, row)
}

func evalNullifFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalNullif(env, ex.Args, row)
}

func evalIsNullFuncWrapper(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalIsNullFunc(env, ex.Args, row)
}

func evalJSONGetFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalJSONGet(env, ex.Args, row)
}

func evalJSONExtendedFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalJSONExtended(env, ex, row)
}

// requireNoArgs guards the clock functions, which take none.
//
// They used to ignore whatever they were given, so NOW('+1 day') returned the
// current instant — the same silent-modifier-swallow that made
// date('2024-01-01', '+1 day') answer with the unmodified date. Rejecting the
// argument is the honest answer; DATE_ADD/DATE_SUB do the arithmetic.
func requireNoArgs(ex *FuncCall) error {
	if len(ex.Args) == 0 {
		return nil
	}
	return fmt.Errorf("%s expects no arguments (date/time modifiers are not supported); "+
		"use DATE_ADD/DATE_SUB for date arithmetic", strings.ToUpper(ex.Name))
}

func evalNowFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if err := requireNoArgs(ex); err != nil {
		return nil, err
	}
	return time.Now(), nil
}

// evalRowToTextFunc implements ROW_TO_TEXT() — concatenates every column
// value of the current row into one space-separated string, for ad-hoc
// whole-row substring search without setting up FTS, e.g.:
//
//	SELECT * FROM orders WHERE ROW_TO_TEXT() LIKE '%acme corp%' AND status = 'open'
//
// The general evaluator reads the ambient Row directly. The raw fast path
// uses evalRawRowToText, which preserves this function's sorted unqualified
// column order while reading physical row values instead.
func evalRowToTextFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if len(ex.Args) > 0 {
		return nil, fmt.Errorf("ROW_TO_TEXT expects no arguments")
	}
	// Row maps store each column under both its qualified ("t.col") and
	// unqualified ("col") key pointing at the same value; skip qualified
	// keys so each value is included exactly once.
	keys := make([]string, 0, len(row))
	for k := range row {
		if !strings.Contains(k, ".") {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	var sb strings.Builder
	for _, k := range keys {
		v := row[k]
		if v == nil {
			continue
		}
		if sb.Len() > 0 {
			sb.WriteByte(' ')
		}
		ftsWriteValue(&sb, v)
	}
	return sb.String(), nil
}

func evalCurrentDateFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if err := requireNoArgs(ex); err != nil {
		return nil, err
	}
	now := time.Now()
	y, m, d := now.Date()
	return time.Date(y, m, d, 0, 0, 0, 0, now.Location()), nil
}

// evalCurrentTimeFunc implements CURRENT_TIME as the time of day, "HH:MM:SS".
//
// It was aliased to evalNowFunc and so returned a full timestamp — a value
// carrying the very date component the name says it drops, which made
// CURRENT_TIME and CURRENT_TIMESTAMP indistinguishable. SQL's CURRENT_TIME and
// SQLite's are both time-of-day only; this now matches TIME(), the function it
// is the zero-argument shorthand for.
func evalCurrentTimeFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if err := requireNoArgs(ex); err != nil {
		return nil, err
	}
	return time.Now().Format("15:04:05"), nil
}

func evalTodayFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if err := requireNoArgs(ex); err != nil {
		return nil, err
	}
	now := time.Now()
	y, m, d := now.Date()
	return time.Date(y, m, d, 0, 0, 0, 0, now.Location()), nil
}

func evalFromTimestampFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if len(ex.Args) != 1 {
		return nil, fmt.Errorf("FROM_TIMESTAMP expects 1 argument")
	}
	val, err := evalExpr(env, ex.Args[0], row)
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, nil
	}
	floatAny, err := coerceToFloat(val)
	if err != nil {
		return nil, fmt.Errorf("FROM_TIMESTAMP expects numeric or string input: %w", err)
	}
	seconds, ok := floatAny.(float64)
	if !ok {
		return nil, fmt.Errorf("FROM_TIMESTAMP expected float result, got %T", floatAny)
	}
	sec, frac := math.Modf(seconds)
	nsec := int64(math.Round(frac * 1e9))
	return time.Unix(int64(sec), nsec), nil
}

func evalTimestampFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if len(ex.Args) != 1 {
		return nil, fmt.Errorf("TIMESTAMP expects 1 argument")
	}
	val, err := evalExpr(env, ex.Args[0], row)
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, nil
	}
	if floatAny, err := coerceToFloat(val); err == nil {
		if seconds, ok := floatAny.(float64); ok {
			return int64(seconds), nil
		}
	}
	t, err := parseTimeValue(val)
	if err != nil {
		return nil, fmt.Errorf("TIMESTAMP: %v", err)
	}
	return t.Unix(), nil
}

func evalLTrimFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalLTrim(env, ex.Args, row)
}

func evalRTrimFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalRTrim(env, ex.Args, row)
}

func evalTrimFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalTrim(env, ex.Args, row)
}

func evalUpperFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalUpper(env, ex.Args, row)
}

func evalLowerFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalLower(env, ex.Args, row)
}

func evalConcatFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalConcat(env, ex.Args, row)
}

func evalLengthFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalLength(env, ex.Args, row)
}

func evalSubstringFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalSubstring(env, ex.Args, row)
}

func evalMD5Func(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalMD5(env, ex.Args, row)
}

func evalSHA1Func(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalSHA1(env, ex.Args, row)
}

func evalSHA256Func(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalSHA256(env, ex.Args, row)
}

func evalSHA512Func(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalSHA512(env, ex.Args, row)
}

func evalBase64Func(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalBase64(env, ex.Args, row)
}

func evalBase64DecodeFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalBase64Decode(env, ex.Args, row)
}

func evalLeftFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalLeft(env, ex.Args, row)
}

func evalRightFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalRight(env, ex.Args, row)
}

func evalCastFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalCast(env, ex.Args, row)
}

func evalReplaceFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalReplace(env, ex.Args, row)
}

func evalInstrFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalInstr(env, ex.Args, row)
}

func evalAbsFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalAbs(env, ex.Args, row)
}

func evalRoundFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalRound(env, ex.Args, row)
}

func evalFloorFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalFloor(env, ex.Args, row)
}

func evalCeilFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalCeil(env, ex.Args, row)
}

func evalReverseFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalReverse(env, ex.Args, row)
}

func evalRepeatFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalRepeat(env, ex.Args, row)
}

func evalPrintfFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalPrintf(env, ex.Args, row)
}

func evalLpadFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalLpad(env, ex.Args, row)
}

func evalRpadFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalRpad(env, ex.Args, row)
}

func evalGreatestFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalGreatest(env, ex.Args, row)
}

func evalLeastFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalLeast(env, ex.Args, row)
}

func evalIfFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalIf(env, ex.Args, row)
}

func evalStrftimeFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalStrftime(env, ex.Args, row)
}

func evalDateFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalDate(env, ex.Args, row)
}

func evalTimeFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalTime(env, ex.Args, row)
}

func evalDatetimeFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalDatetime(env, ex.Args, row)
}

func evalJuliandayFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalJulianday(env, ex.Args, row)
}

func evalUnixepochFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalUnixepoch(env, ex.Args, row)
}

func evalYearFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalYear(env, ex.Args, row)
}

func evalMonthFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalMonth(env, ex.Args, row)
}

func evalDayFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalDay(env, ex.Args, row)
}

func evalHourFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalHour(env, ex.Args, row)
}

func evalMinuteFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalMinute(env, ex.Args, row)
}

func evalSecondFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalSecond(env, ex.Args, row)
}

func evalRandomFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalRandom(env, ex.Args, row)
}

func evalModFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalMod(env, ex.Args, row)
}

func evalPowerFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalPower(env, ex.Args, row)
}

func evalSqrtFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalSqrt(env, ex.Args, row)
}

func evalLogFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalLog(env, ex.Args, row)
}

func evalLnFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalLn(env, ex.Args, row)
}

func evalLog10Func(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalLog10(env, ex.Args, row)
}

func evalLog2Func(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalLog2(env, ex.Args, row)
}

func evalExpFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalExp(env, ex.Args, row)
}

func evalSignFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalSign(env, ex.Args, row)
}

func evalTruncateFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalTruncate(env, ex.Args, row)
}

func evalPiFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return math.Pi, nil
}

func evalSinFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalSin(env, ex.Args, row)
}

func evalCosFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalCos(env, ex.Args, row)
}

func evalTanFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalTan(env, ex.Args, row)
}

func evalAsinFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalAsin(env, ex.Args, row)
}

func evalAcosFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalAcos(env, ex.Args, row)
}

func evalAtanFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalAtan(env, ex.Args, row)
}

func evalAtan2Func(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalAtan2(env, ex.Args, row)
}

func evalDegreesFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalDegrees(env, ex.Args, row)
}

func evalRadiansFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalRadians(env, ex.Args, row)
}

func evalSpaceFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalSpace(env, ex.Args, row)
}

func evalAsciiFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalAscii(env, ex.Args, row)
}

func evalCharFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalChar(env, ex.Args, row)
}

func evalInitcapFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalInitcap(env, ex.Args, row)
}

func evalSplitPartFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalSplitPart(env, ex.Args, row)
}

func evalSoundexFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalSoundex(env, ex.Args, row)
}

func evalQuoteFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalQuote(env, ex.Args, row)
}

func evalHexFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalHex(env, ex.Args, row)
}

func evalUnhexFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalUnhex(env, ex.Args, row)
}

func evalUuidFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalUuid(env, ex.Args, row)
}

func evalTypeofFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalTypeof(env, ex.Args, row)
}

func evalVersionFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return "tinySQL 1.0", nil
}

func evalConcatWsFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalConcatWs(env, ex.Args, row)
}

func evalPositionFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalPosition(env, ex.Args, row)
}

func evalOverlapsFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalOverlaps(env, ex.Args, row)
}

func evalRangeMergeFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalRangeMerge(env, ex.Args, row)
}

func evalLocateFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalLocate(env, ex.Args, row)
}

func evalDayOfWeekFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalDayOfWeek(env, ex.Args, row)
}

func evalDayOfYearFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalDayOfYear(env, ex.Args, row)
}

func evalWeekOfYearFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalWeekOfYear(env, ex.Args, row)
}

func evalQuarterFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalQuarter(env, ex.Args, row)
}

func evalDateAddFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalDateAdd(env, ex.Args, row)
}

func evalDateSubFunc(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalDateSub(env, ex.Args, row)
}
