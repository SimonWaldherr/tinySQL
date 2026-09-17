package engine

import (
	"math"
	"strings"
)

// joinCandidateLookup preserves the nested loop's outer-row order and each
// key's inner-row order. Positions also identify duplicates for FULL JOIN's
// unmatched-right bookkeeping. The lookup belongs to one execution only.
type joinCandidateLookup struct {
	keys      []any
	positions map[any][]int
}

func (lookup *joinCandidateLookup) candidates(row, innerCount int) ([]int, int) {
	if lookup == nil {
		return nil, innerCount
	}
	positions := lookup.positions[lookup.keys[row]]
	return positions, len(positions)
}

// Only hash domains whose equality exactly matches SQL compare are admitted.
// Mixed integer/float comparisons (especially above 2^53), NaN, decimals and
// other types retain the evaluator, including its errors and operand order.
func joinLookupKey(value any) (any, byte) {
	switch v := value.(type) {
	case nil:
		return nil, 0
	case int:
		return int64(v), 1
	case int64:
		return value, 1
	case float64:
		if !math.IsNaN(v) {
			return value, 2
		}
	case string:
		return value, 3
	case bool:
		return value, 4
	}
	return nil, 255
}

func buildJoinCandidateLookup(env ExecEnv, equality *joinEquality, outer, inner []Row) (*joinCandidateLookup, error) {
	// Tiny inputs cost less to compare directly. Avoid overflow in the product.
	if equality == nil || len(inner) == 0 || len(outer) <= 1024/len(inner) {
		return nil, nil
	}
	if !strings.Contains(equality.leftColumn, ".") || !strings.Contains(equality.rightColumn, ".") {
		return nil, nil
	}
	outerColumn, innerColumn := equality.leftColumn, equality.rightColumn
	if _, ok := outer[0][outerColumn]; !ok {
		outerColumn, innerColumn = innerColumn, outerColumn
	}
	keys := make([]any, len(outer)+len(inner))
	var domain byte
	for side, rows := range [][]Row{outer, inner} {
		column, other, offset := outerColumn, innerColumn, 0
		if side == 1 {
			column, other, offset = innerColumn, outerColumn, len(outer)
		}
		for i, row := range rows {
			if i&63 == 0 {
				if err := checkCtx(env.ctx); err != nil {
					return nil, err
				}
			}
			value, found := row[column]
			_, shadowed := row[other]
			if !found || shadowed {
				return nil, nil
			}
			key, kind := joinLookupKey(value)
			if kind == 255 || (kind != 0 && domain != 0 && kind != domain) {
				return nil, nil
			}
			if kind != 0 {
				domain = kind
			}
			keys[offset+i] = key
		}
	}
	positions := make(map[any][]int, len(inner))
	for i, key := range keys[len(outer):] {
		if i&63 == 0 {
			if err := checkCtx(env.ctx); err != nil {
				return nil, err
			}
		}
		if key != nil {
			positions[key] = append(positions[key], i)
		}
	}
	return &joinCandidateLookup{keys: keys[:len(outer)], positions: positions}, nil
}
