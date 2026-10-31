// Aggregate evaluation over a group's rows: COUNT, SUM, AVG, MIN, MAX, MEDIAN,
// MIN_BY/MAX_BY and vector averaging, plus recognising which expressions are
// aggregates at all.
package engine

import (
	"fmt"
	"math/big"
	"sort"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// aggregateOperand is execution-local. Scalar handlers evaluate it on demand,
// preserving their arity checks, NULL propagation and discarded branches.
type aggregateOperand struct {
	expr Expr
	rows []Row
}

func isAggregate(e Expr) bool {
	switch ex := e.(type) {
	case *FuncCall:
		if ex.Over != nil {
			return false
		}
		if isAggregateName(ex.Name) && (ex.Name != "VEC_AVG" || len(ex.Args) == 1) {
			return true
		}
		for _, arg := range ex.Args {
			if isAggregate(arg) {
				return true
			}
		}
	case *Unary:
		return isAggregate(ex.Expr)
	case *Binary:
		return isAggregate(ex.Left) || isAggregate(ex.Right)
	case *IsNull:
		return isAggregate(ex.Expr)
	case *CaseExpr:
		if ex.Operand != nil && isAggregate(ex.Operand) {
			return true
		}
		for _, w := range ex.Whens {
			if isAggregate(w.When) || isAggregate(w.Then) {
				return true
			}
		}
		if ex.Else != nil && isAggregate(ex.Else) {
			return true
		}
	}
	return false
}

func isAggregateName(name string) bool {
	switch name {
	case "COUNT", "SUM", "AVG", "MIN", "MAX", "MEDIAN", "VEC_AVG",
		"MIN_BY", "MAX_BY", "ARG_MIN", "ARG_MAX",
		"GEO_DISSOLVE", "GEO_UNION_AGG", "ST_UNION", "GEO_BBOX_AGG", "GEO_CENTROID_AGG":
		return true
	}
	return false
}

func evalAggregate(env ExecEnv, e Expr, rows []Row) (any, error) {
	switch ex := e.(type) {
	case *Literal:
		return ex.Val, nil
	case *FuncCall:
		return evalAggregateFuncCall(env, ex, rows)
	case *Unary:
		return evalAggregateUnary(env, ex, rows)
	case *Binary:
		return evalAggregateBinary(env, ex, rows)
	case *IsNull:
		return evalAggregateIsNull(env, ex, rows)
	case *CaseExpr:
		return evalAggregateCase(env, ex, rows)
	default:
		if len(rows) == 0 {
			return nil, nil
		}
		return evalExpr(env, e, rows[0])
	}
}

func evalAggregateFuncCall(env ExecEnv, ex *FuncCall, rows []Row) (any, error) {
	if ex.Filter != nil {
		var selected []Row
		for _, row := range rows {
			if err := checkCtx(env.ctx); err != nil {
				return nil, err
			}
			value, err := evalExpr(env, ex.Filter, row)
			if err != nil {
				return nil, err
			}
			if toTri(value) == tvTrue {
				selected = append(selected, row)
			}
		}
		rows = selected
	}
	switch ex.Name {
	case "COUNT":
		return evalAggregateCount(env, ex, rows)
	case "SUM", "AVG":
		return evalAggregateSumAvg(env, ex, rows)
	case "MIN", "MAX":
		return evalAggregateMinMax(env, ex, rows)
	case "MEDIAN":
		return evalAggregateMedian(env, ex, rows)
	case "MIN_BY", "ARG_MIN":
		return evalAggregateMinBy(env, ex, rows)
	case "MAX_BY", "ARG_MAX":
		return evalAggregateMaxBy(env, ex, rows)
	case "VEC_AVG":
		return evalAggregateVecAvg(env, ex, rows)
	case "GEO_DISSOLVE", "GEO_UNION_AGG", "ST_UNION":
		return evalAggregateGeoDissolve(env, ex, rows)
	case "GEO_BBOX_AGG":
		return evalAggregateGeoBBoxAgg(env, ex, rows)
	case "GEO_CENTROID_AGG":
		return evalAggregateGeoCentroidAgg(env, ex, rows)
	default:
		call := *ex
		call.Args = make([]Expr, len(ex.Args))
		for i, arg := range ex.Args {
			if _, literal := arg.(*Literal); literal || ex.Name == "CAST" && i == 1 {
				// Preserve syntax operands such as CAST's target declaration.
				call.Args[i] = arg
			} else {
				call.Args[i] = &aggregateOperand{expr: arg, rows: rows}
			}
		}
		var row Row
		if len(rows) != 0 {
			row = rows[0]
		}
		return evalFuncCall(env, &call, row)
	}
}

func evalAggregateCount(env ExecEnv, ex *FuncCall, rows []Row) (any, error) {
	if ex.Star {
		return len(rows), nil
	}
	if len(ex.Args) != 1 {
		return nil, fmt.Errorf("COUNT expects 1 arg")
	}

	// Handle COUNT(DISTINCT col)
	if ex.Distinct {
		var seen distinctCountSet
		cnt := 0
		for _, r := range rows {
			if err := checkCtx(env.ctx); err != nil {
				return nil, err
			}
			v, err := evalExpr(env, ex.Args[0], r)
			if err != nil {
				return nil, err
			}
			if seen.add(v) {
				cnt++
			}
		}
		return cnt, nil
	}

	// Regular COUNT(col)
	cnt := 0
	for _, r := range rows {
		if err := checkCtx(env.ctx); err != nil {
			return nil, err
		}
		v, err := evalExpr(env, ex.Args[0], r)
		if err != nil {
			return nil, err
		}
		if v != nil {
			cnt++
		}
	}
	return cnt, nil
}

func evalAggregateSumAvg(env ExecEnv, ex *FuncCall, rows []Row) (any, error) {
	if len(ex.Args) != 1 {
		return nil, fmt.Errorf("%s expects 1 arg", ex.Name)
	}
	var (
		sum    sumAccumulator
		sumRat *big.Rat
		n      int
		seen   distinctCountSet
	)
	for _, r := range rows {
		if err := checkCtx(env.ctx); err != nil {
			return nil, err
		}
		v, err := evalExpr(env, ex.Args[0], r)
		if err != nil {
			return nil, err
		}
		if v == nil {
			continue
		}
		if ex.Distinct && !seen.add(v) {
			continue
		}
		if sumRat == nil {
			if sum.add(v) {
				n++
				continue
			}
		} else if rv, ok := ratFromNumeric(v); ok {
			sumRat.Add(sumRat, rv)
			n++
			continue
		}
		if rv, ok := storage.DecimalFromAny(v); ok {
			if sumRat == nil {
				// migrate the exact integer/float total into the rational
				sumRat = sum.rat()
			}
			sumRat.Add(sumRat, new(big.Rat).Set(rv))
			n++
			continue
		}
	}
	// SQL aggregates other than COUNT return NULL when there are no non-NULL
	// input values. This covers both a genuinely empty group and a group whose
	// argument evaluates to NULL for every row (for example a PIVOT bucket
	// with no matching source rows).
	if n == 0 {
		return nil, nil
	}
	if ex.Name == "SUM" {
		if sumRat != nil {
			return sumRat, nil
		}
		// Integer inputs produce an integer SUM (REAL after an overflow).
		return sum.sum(), nil
	}
	if sumRat != nil {
		avg := new(big.Rat).Quo(sumRat, big.NewRat(int64(n), 1))
		return avg, nil
	}
	return sum.average(n), nil
}

func evalAggregateMinMax(env ExecEnv, ex *FuncCall, rows []Row) (any, error) {
	if len(ex.Args) != 1 {
		return nil, fmt.Errorf("%s expects 1 arg", ex.Name)
	}
	var have bool
	var best any
	for _, r := range rows {
		if err := checkCtx(env.ctx); err != nil {
			return nil, err
		}
		v, err := evalExpr(env, ex.Args[0], r)
		if err != nil {
			return nil, err
		}
		if v == nil {
			continue
		}
		if !have {
			best = v
			have = true
		} else {
			cmp, err := compare(v, best)
			if err == nil {
				if ex.Name == "MIN" && cmp < 0 {
					best = v
				}
				if ex.Name == "MAX" && cmp > 0 {
					best = v
				}
			}
		}
	}
	if !have {
		return nil, nil
	}
	return best, nil
}

func evalAggregateMedian(env ExecEnv, ex *FuncCall, rows []Row) (any, error) {
	if len(ex.Args) != 1 {
		return nil, fmt.Errorf("MEDIAN expects 1 arg")
	}
	var values []float64
	var seen distinctCountSet
	for _, r := range rows {
		if err := checkCtx(env.ctx); err != nil {
			return nil, err
		}
		v, err := evalExpr(env, ex.Args[0], r)
		if err != nil {
			return nil, err
		}
		if f, ok := numeric(v); ok {
			if ex.Distinct && !seen.add(v) {
				continue
			}
			values = append(values, f)
		}
	}
	if len(values) == 0 {
		return nil, nil
	}

	// Sort values
	sort.Float64s(values)

	// Calculate median
	n := len(values)
	if n%2 == 1 {
		// Odd number of values - return middle value
		return values[n/2], nil
	}
	// Even number of values - return average of two middle values
	return (values[n/2-1] + values[n/2]) / 2.0, nil
}

// evalAggregateMinBy returns the value from first argument where second argument is minimum
// Usage: MIN_BY(value_column, order_column)
func evalAggregateMinBy(env ExecEnv, ex *FuncCall, rows []Row) (any, error) {
	if len(ex.Args) != 2 {
		return nil, fmt.Errorf("MIN_BY expects 2 arguments: (value_column, order_column)")
	}
	if len(rows) == 0 {
		return nil, nil
	}

	var minOrderVal any
	var resultVal any
	first := true

	for _, r := range rows {
		if err := checkCtx(env.ctx); err != nil {
			return nil, err
		}

		// Evaluate the ordering column (second argument)
		orderVal, err := evalExpr(env, ex.Args[1], r)
		if err != nil {
			return nil, err
		}

		// Skip NULL values in comparison
		if orderVal == nil {
			continue
		}

		// First non-NULL value or found a smaller value
		if first {
			minOrderVal = orderVal
			resultVal, err = evalExpr(env, ex.Args[0], r)
			if err != nil {
				return nil, err
			}
			first = false
		} else {
			cmp, err := compare(orderVal, minOrderVal)
			if err == nil && cmp < 0 {
				minOrderVal = orderVal
				resultVal, err = evalExpr(env, ex.Args[0], r)
				if err != nil {
					return nil, err
				}
			}
		}
	}

	return resultVal, nil
}

// evalAggregateMaxBy returns the value from first argument where second argument is maximum
// Usage: MAX_BY(value_column, order_column)
func evalAggregateMaxBy(env ExecEnv, ex *FuncCall, rows []Row) (any, error) {
	if len(ex.Args) != 2 {
		return nil, fmt.Errorf("MAX_BY expects 2 arguments: (value_column, order_column)")
	}
	if len(rows) == 0 {
		return nil, nil
	}

	var maxOrderVal any
	var resultVal any
	first := true

	for _, r := range rows {
		if err := checkCtx(env.ctx); err != nil {
			return nil, err
		}

		// Evaluate the ordering column (second argument)
		orderVal, err := evalExpr(env, ex.Args[1], r)
		if err != nil {
			return nil, err
		}

		// Skip NULL values in comparison
		if orderVal == nil {
			continue
		}

		// First non-NULL value or found a larger value
		if first {
			maxOrderVal = orderVal
			resultVal, err = evalExpr(env, ex.Args[0], r)
			if err != nil {
				return nil, err
			}
			first = false
		} else {
			cmp, err := compare(orderVal, maxOrderVal)
			if err == nil && cmp > 0 {
				maxOrderVal = orderVal
				resultVal, err = evalExpr(env, ex.Args[0], r)
				if err != nil {
					return nil, err
				}
			}
		}
	}

	return resultVal, nil
}

// evalAggregateVecAvg computes the element-wise average of a set of vectors.
func evalAggregateVecAvg(env ExecEnv, ex *FuncCall, rows []Row) (any, error) {
	if len(ex.Args) != 1 {
		return nil, fmt.Errorf("VEC_AVG expects 1 argument")
	}
	if len(rows) == 0 {
		return nil, nil
	}

	var sum []float64
	count := 0

	for _, r := range rows {
		if err := checkCtx(env.ctx); err != nil {
			return nil, err
		}
		val, err := evalExpr(env, ex.Args[0], r)
		if err != nil {
			return nil, err
		}
		if val == nil {
			continue
		}
		var vec []float64
		switch v := val.(type) {
		case []float64:
			vec = v
		default:
			continue
		}
		if sum == nil {
			sum = make([]float64, len(vec))
		}
		if len(vec) != len(sum) {
			return nil, fmt.Errorf("VEC_AVG: dimension mismatch (%d vs %d)", len(vec), len(sum))
		}
		for i, f := range vec {
			sum[i] += f
		}
		count++
	}

	if count == 0 {
		return nil, nil
	}
	avg := make([]float64, len(sum))
	for i := range sum {
		avg[i] = sum[i] / float64(count)
	}
	return avg, nil
}

func evalAggregateUnary(env ExecEnv, ex *Unary, rows []Row) (any, error) {
	v, err := evalAggregate(env, ex.Expr, rows)
	if err != nil {
		return nil, err
	}
	switch ex.Op {
	case "+":
		if n, ok := unaryNumeric("+", v); ok {
			return n, nil
		}
		if r, ok := storage.DecimalFromAny(v); ok {
			return new(big.Rat).Set(r), nil
		}
		if v == nil {
			return nil, nil
		}
		return nil, fmt.Errorf("unary + non-numeric")
	case "-":
		if n, ok := unaryNumeric("-", v); ok {
			return n, nil
		}
		if r, ok := storage.DecimalFromAny(v); ok {
			neg := new(big.Rat).Set(r)
			neg.Mul(neg, big.NewRat(-1, 1))
			return neg, nil
		}
		if v == nil {
			return nil, nil
		}
		return nil, fmt.Errorf("unary - non-numeric")
	case "NOT":
		return triToValue(triNot(toTri(v))), nil
	}
	return nil, fmt.Errorf("unknown unary operator: %s", ex.Op)
}

func evalAggregateBinary(env ExecEnv, ex *Binary, rows []Row) (any, error) {
	return evalExpr(env, &Binary{Op: ex.Op,
		Left:  &aggregateOperand{expr: ex.Left, rows: rows},
		Right: &aggregateOperand{expr: ex.Right, rows: rows}}, nil)
}

func evalAggregateIsNull(env ExecEnv, ex *IsNull, rows []Row) (any, error) {
	v, err := evalAggregate(env, ex.Expr, rows)
	if err != nil {
		return nil, err
	}
	if ex.Negate {
		return !isNull(v), nil
	}
	return isNull(v), nil
}

func evalAggregateCase(env ExecEnv, ex *CaseExpr, rows []Row) (any, error) {
	if ex.Operand != nil {
		target, err := evalAggregate(env, ex.Operand, rows)
		if err != nil {
			return nil, err
		}
		for _, w := range ex.Whens {
			whenVal, err := evalAggregate(env, w.When, rows)
			if err != nil {
				return nil, err
			}
			if caseValuesEqual(target, whenVal) {
				return evalAggregate(env, w.Then, rows)
			}
		}
	} else {
		for _, w := range ex.Whens {
			cond, err := evalAggregate(env, w.When, rows)
			if err != nil {
				return nil, err
			}
			if toTri(cond) == tvTrue {
				return evalAggregate(env, w.Then, rows)
			}
		}
	}
	if ex.Else != nil {
		return evalAggregate(env, ex.Else, rows)
	}
	return nil, nil
}
