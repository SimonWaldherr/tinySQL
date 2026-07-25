// Numeric functions. The exact-decimal cases stay on big.Rat; the rest work in
// float64.
package engine

import (
	"fmt"
	"math"
	"math/rand"
)

func evalAbs(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("ABS expects 1 argument")
	}
	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, nil
	}
	f, ok := numeric(val)
	if !ok {
		return nil, fmt.Errorf("ABS requires numeric argument")
	}
	if f < 0 {
		return -f, nil
	}
	return f, nil
}

func evalRound(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) < 1 || len(args) > 2 {
		return nil, fmt.Errorf("ROUND expects 1-2 arguments")
	}
	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, nil
	}
	f, ok := numeric(val)
	if !ok {
		return nil, fmt.Errorf("ROUND requires numeric argument")
	}

	decimals := 0
	if len(args) == 2 {
		decVal, err := evalExpr(env, args[1], row)
		if err != nil {
			return nil, err
		}
		if decVal == nil {
			return nil, nil
		}
		decAny, err := coerceToInt(decVal)
		if err != nil {
			return nil, fmt.Errorf("ROUND decimals must be numeric")
		}
		var ok bool
		decimals, ok = decAny.(int)
		if !ok {
			return nil, fmt.Errorf("ROUND decimals internal error: expected int, got %T", decAny)
		}
	}

	mult := math.Pow(10, float64(decimals))
	return math.Round(f*mult) / mult, nil
}

func evalFloor(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("FLOOR expects 1 argument")
	}
	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, nil
	}
	f, ok := numeric(val)
	if !ok {
		return nil, fmt.Errorf("FLOOR requires numeric argument")
	}
	return math.Floor(f), nil
}

func evalCeil(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("CEIL expects 1 argument")
	}
	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, nil
	}
	f, ok := numeric(val)
	if !ok {
		return nil, fmt.Errorf("CEIL requires numeric argument")
	}
	return math.Ceil(f), nil
}

func evalGreatest(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("GREATEST expects at least 1 argument")
	}
	var result any
	for _, arg := range args {
		val, err := evalExpr(env, arg, row)
		if err != nil {
			return nil, err
		}
		if val == nil {
			continue
		}
		if result == nil {
			result = val
			continue
		}
		cmp, err := compare(val, result)
		if err != nil {
			return nil, err
		}
		if cmp > 0 {
			result = val
		}
	}
	return result, nil
}

func evalLeast(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("LEAST expects at least 1 argument")
	}
	var result any
	for _, arg := range args {
		val, err := evalExpr(env, arg, row)
		if err != nil {
			return nil, err
		}
		if val == nil {
			continue
		}
		if result == nil {
			result = val
			continue
		}
		cmp, err := compare(val, result)
		if err != nil {
			return nil, err
		}
		if cmp < 0 {
			result = val
		}
	}
	return result, nil
}

func evalRandom(env ExecEnv, args []Expr, row Row) (any, error) {
	return rand.Float64(), nil
}

func evalMod(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("MOD expects 2 arguments")
	}
	av, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	bv, err := evalExpr(env, args[1], row)
	if err != nil {
		return nil, err
	}
	a, ok := numeric(av)
	if !ok {
		return nil, fmt.Errorf("MOD: first argument must be numeric")
	}
	b, ok := numeric(bv)
	if !ok {
		return nil, fmt.Errorf("MOD: second argument must be numeric")
	}
	if b == 0 {
		return nil, fmt.Errorf("MOD: division by zero")
	}
	return math.Mod(a, b), nil
}

func evalPower(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("POWER expects 2 arguments")
	}
	baseVal, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	expVal, err := evalExpr(env, args[1], row)
	if err != nil {
		return nil, err
	}
	base, ok := numeric(baseVal)
	if !ok {
		return nil, fmt.Errorf("POWER: base must be numeric")
	}
	exp, ok := numeric(expVal)
	if !ok {
		return nil, fmt.Errorf("POWER: exponent must be numeric")
	}
	return math.Pow(base, exp), nil
}

func evalSqrt(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("SQRT expects 1 argument")
	}
	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	n, ok := numeric(val)
	if !ok {
		return nil, fmt.Errorf("SQRT: argument must be numeric")
	}
	if n < 0 {
		return nil, fmt.Errorf("SQRT: argument must be non-negative")
	}
	return math.Sqrt(n), nil
}

func evalLog(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) < 1 || len(args) > 2 {
		return nil, fmt.Errorf("LOG expects 1 or 2 arguments")
	}
	if len(args) == 1 {
		// LOG(x) = natural log
		val, err := evalExpr(env, args[0], row)
		if err != nil {
			return nil, err
		}
		n, ok := numeric(val)
		if !ok {
			return nil, fmt.Errorf("LOG: argument must be numeric")
		}
		if n <= 0 {
			return nil, fmt.Errorf("LOG: argument must be positive")
		}
		return math.Log(n), nil
	}
	// LOG(base, x)
	baseVal, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	val, err := evalExpr(env, args[1], row)
	if err != nil {
		return nil, err
	}
	base, ok := numeric(baseVal)
	if !ok {
		return nil, fmt.Errorf("LOG: base must be numeric")
	}
	n, ok := numeric(val)
	if !ok {
		return nil, fmt.Errorf("LOG: argument must be numeric")
	}
	if base <= 0 || base == 1 || n <= 0 {
		return nil, fmt.Errorf("LOG: invalid arguments")
	}
	return math.Log(n) / math.Log(base), nil
}

func evalLn(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("LN expects 1 argument")
	}
	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	n, ok := numeric(val)
	if !ok {
		return nil, fmt.Errorf("LN: argument must be numeric")
	}
	if n <= 0 {
		return nil, fmt.Errorf("LN: argument must be positive")
	}
	return math.Log(n), nil
}

func evalLog10(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("LOG10 expects 1 argument")
	}
	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	n, ok := numeric(val)
	if !ok {
		return nil, fmt.Errorf("LOG10: argument must be numeric")
	}
	if n <= 0 {
		return nil, fmt.Errorf("LOG10: argument must be positive")
	}
	return math.Log10(n), nil
}

func evalLog2(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("LOG2 expects 1 argument")
	}
	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	n, ok := numeric(val)
	if !ok {
		return nil, fmt.Errorf("LOG2: argument must be numeric")
	}
	if n <= 0 {
		return nil, fmt.Errorf("LOG2: argument must be positive")
	}
	return math.Log2(n), nil
}

func evalExp(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("EXP expects 1 argument")
	}
	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	n, ok := numeric(val)
	if !ok {
		return nil, fmt.Errorf("EXP: argument must be numeric")
	}
	return math.Exp(n), nil
}

func evalSign(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("SIGN expects 1 argument")
	}
	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	n, ok := numeric(val)
	if !ok {
		return nil, fmt.Errorf("SIGN: argument must be numeric")
	}
	if n > 0 {
		return 1, nil
	} else if n < 0 {
		return -1, nil
	}
	return 0, nil
}

func evalTruncate(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) < 1 || len(args) > 2 {
		return nil, fmt.Errorf("TRUNCATE expects 1-2 arguments")
	}
	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	n, ok := numeric(val)
	if !ok {
		return nil, fmt.Errorf("TRUNCATE: argument must be numeric")
	}
	decimals := 0
	if len(args) == 2 {
		dv, err := evalExpr(env, args[1], row)
		if err != nil {
			return nil, err
		}
		d, ok := numeric(dv)
		if !ok {
			return nil, fmt.Errorf("TRUNCATE: decimals must be numeric")
		}
		decimals = int(d)
	}
	multiplier := math.Pow(10, float64(decimals))
	return math.Trunc(n*multiplier) / multiplier, nil
}

// Trigonometric functions
func evalSin(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("SIN expects 1 argument")
	}
	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	n, ok := numeric(val)
	if !ok {
		return nil, fmt.Errorf("SIN: argument must be numeric")
	}
	return math.Sin(n), nil
}

func evalCos(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("COS expects 1 argument")
	}
	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	n, ok := numeric(val)
	if !ok {
		return nil, fmt.Errorf("COS: argument must be numeric")
	}
	return math.Cos(n), nil
}

func evalTan(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("TAN expects 1 argument")
	}
	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	n, ok := numeric(val)
	if !ok {
		return nil, fmt.Errorf("TAN: argument must be numeric")
	}
	return math.Tan(n), nil
}

func evalAsin(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("ASIN expects 1 argument")
	}
	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	n, ok := numeric(val)
	if !ok {
		return nil, fmt.Errorf("ASIN: argument must be numeric")
	}
	if n < -1 || n > 1 {
		return nil, fmt.Errorf("ASIN: argument must be between -1 and 1")
	}
	return math.Asin(n), nil
}

func evalAcos(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("ACOS expects 1 argument")
	}
	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	n, ok := numeric(val)
	if !ok {
		return nil, fmt.Errorf("ACOS: argument must be numeric")
	}
	if n < -1 || n > 1 {
		return nil, fmt.Errorf("ACOS: argument must be between -1 and 1")
	}
	return math.Acos(n), nil
}

func evalAtan(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("ATAN expects 1 argument")
	}
	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	n, ok := numeric(val)
	if !ok {
		return nil, fmt.Errorf("ATAN: argument must be numeric")
	}
	return math.Atan(n), nil
}

func evalAtan2(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("ATAN2 expects 2 arguments")
	}
	yVal, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	xVal, err := evalExpr(env, args[1], row)
	if err != nil {
		return nil, err
	}
	y, ok := numeric(yVal)
	if !ok {
		return nil, fmt.Errorf("ATAN2: y must be numeric")
	}
	x, ok := numeric(xVal)
	if !ok {
		return nil, fmt.Errorf("ATAN2: x must be numeric")
	}
	return math.Atan2(y, x), nil
}

func evalDegrees(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("DEGREES expects 1 argument")
	}
	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	n, ok := numeric(val)
	if !ok {
		return nil, fmt.Errorf("DEGREES: argument must be numeric")
	}
	return n * 180 / math.Pi, nil
}

func evalRadians(env ExecEnv, args []Expr, row Row) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("RADIANS expects 1 argument")
	}
	val, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, err
	}
	n, ok := numeric(val)
	if !ok {
		return nil, fmt.Errorf("RADIANS: argument must be numeric")
	}
	return n * math.Pi / 180, nil
}
