package engine

import (
	"context"
	"strings"
)

// Use prefix totals for wide frames over direct columns. Expressions and
// filters keep their normal evaluation order, including errors and effects.
func cachedWindowTotal(env ExecEnv, entry *windowPartitionEntry, rows []Row, start, end int, function string, arg Expr) (any, bool, error) {
	ref, ok := arg.(*VarRef)
	if !ok || entry == nil || env.triggerRow != nil || start < 0 || end > len(rows) || end-start < 64 {
		return nil, false, nil
	}
	name := ref.Lower
	if name == "" {
		name = strings.ToLower(ref.Name)
	}
	switch function {
	case "COUNT":
		prefix, err := windowCountPrefix(env.ctx, entry, rows, name)
		if prefix == nil || err != nil {
			return nil, false, err
		}
		return prefix[end] - prefix[start], true, nil
	case "SUM", "AVG", "MOVING_SUM", "MOVING_AVG":
		prefix, err := windowIntegerPrefix(env.ctx, entry, rows, name)
		if prefix == nil || err != nil {
			return nil, false, err
		}
		total := prefix[end].sum - prefix[start].sum
		count := prefix[end].count - prefix[start].count
		if function == "MOVING_SUM" {
			return intResult(total), true, nil
		}
		if count == 0 {
			return nil, true, nil
		}
		if function == "SUM" {
			return intResult(total), true, nil
		}
		return float64(total) / float64(count), true, nil
	}
	return nil, false, nil
}

func windowCountPrefix(ctx context.Context, entry *windowPartitionEntry, rows []Row, name string) ([]int, error) {
	if prefix, found := entry.countPrefixes[name]; found {
		return prefix, nil
	}
	prefix := make([]int, len(rows)+1)
	for i, row := range rows {
		if i&63 == 0 {
			if err := checkCtx(ctx); err != nil {
				return nil, err
			}
		}
		value, found := getValLower(row, name)
		if !found {
			prefix = nil
			break
		}
		prefix[i+1] = prefix[i]
		if value != nil {
			prefix[i+1]++
		}
	}
	if entry.countPrefixes == nil {
		entry.countPrefixes = make(map[string][]int)
	}
	entry.countPrefixes[name] = prefix
	return prefix, nil
}

type windowIntegerTotal struct {
	sum   int64
	count int
}

func windowIntegerPrefix(ctx context.Context, entry *windowPartitionEntry, rows []Row, name string) ([]windowIntegerTotal, error) {
	if prefix, found := entry.integerTotals[name]; found {
		return prefix, nil
	}
	prefix, err := buildWindowIntegerPrefix(ctx, rows, name)
	if err == nil {
		if entry.integerTotals == nil {
			entry.integerTotals = make(map[string][]windowIntegerTotal)
		}
		entry.integerTotals[name] = prefix
	}
	return prefix, err
}

func buildWindowIntegerPrefix(ctx context.Context, rows []Row, name string) ([]windowIntegerTotal, error) {
	var prefix []windowIntegerTotal
	var positive, negative int64
	for i, row := range rows {
		if i&63 == 0 {
			if err := checkCtx(ctx); err != nil {
				return nil, err
			}
		}
		value, found := getValLower(row, name)
		if !found {
			return nil, nil
		}
		var number int64
		if value != nil {
			var ok bool
			number, ok = integerOperand(value)
			if !ok {
				// Floating point, DECIMAL and other values retain the scalar
				// evaluator's summation and type promotion.
				return nil, nil
			}
			// If each sign fits on its own, so does every intermediate
			// total in every frame. Otherwise a scalar sum can promote to REAL.
			if number < 0 {
				negative, ok = addInt64(negative, number)
			} else {
				positive, ok = addInt64(positive, number)
			}
			if !ok {
				return nil, nil
			}
		}
		if prefix == nil {
			prefix = make([]windowIntegerTotal, len(rows)+1)
		}
		total := prefix[i]
		if value != nil {
			total.sum += number
			total.count++
		}
		prefix[i+1] = total
	}
	return prefix, nil
}
