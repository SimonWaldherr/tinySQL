package engine

// Choosing a secondary index for a range predicate.
//
// selectSecondaryIndex handles equality only, so `lat BETWEEN ? AND ?` fell
// through to a table scan — and with it every viewport, radius and time-window
// query, which is most of what a map or navigation workload asks. This adds the
// standard B-tree access shape on top of it: an equality prefix followed by one
// range column.
//
// Two constraints come from the key encoding (see
// internal/storage/secondary_index_range.go) and are correctness requirements,
// not tuning choices:
//
//   - the range column must be numeric, because text keys are length-framed and
//     therefore not byte-ordered by value;
//   - the column must hold one numeric kind, because integers and floats carry
//     different type tags and sort as separate blocks. The bounds are converted
//     to that kind so a query written `lat BETWEEN 47 AND 48` still seeks a
//     float column.
//
// A range is a *superset* filter: trailing index columns are not constrained, so
// the residual WHERE still runs. That is what makes a two-dimensional predicate
// work — an index on (lat, lon) narrows to the latitude band and `lon` is then
// filtered per row, which is far less than the whole table without being an
// R-tree.

import (
	"math"
	"sort"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// rangeTerm accumulates the bounds seen for one column.
type rangeTerm struct {
	lo storage.IndexRangeBound
	hi storage.IndexRangeBound
}

func newRangeTerm() rangeTerm {
	return rangeTerm{
		lo: storage.IndexRangeBound{Absent: true},
		hi: storage.IndexRangeBound{Absent: true},
	}
}

// tightenLower keeps the most restrictive lower bound seen.
func (r *rangeTerm) tightenLower(v any, inclusive bool) {
	if r.lo.Absent {
		r.lo = storage.IndexRangeBound{Value: v, Inclusive: inclusive}
		return
	}
	if cmp, err := compare(v, r.lo.Value); err == nil {
		if cmp > 0 || (cmp == 0 && !inclusive) {
			r.lo = storage.IndexRangeBound{Value: v, Inclusive: inclusive}
		}
	}
}

// tightenUpper keeps the most restrictive upper bound seen.
func (r *rangeTerm) tightenUpper(v any, inclusive bool) {
	if r.hi.Absent {
		r.hi = storage.IndexRangeBound{Value: v, Inclusive: inclusive}
		return
	}
	if cmp, err := compare(v, r.hi.Value); err == nil {
		if cmp < 0 || (cmp == 0 && !inclusive) {
			r.hi = storage.IndexRangeBound{Value: v, Inclusive: inclusive}
		}
	}
}

func (r rangeTerm) bounded() bool { return !r.lo.Absent || !r.hi.Absent }

// collectRangeTerms walks an AND-tree collecting comparison predicates against
// literals, keyed by column position.
//
// Only AND is descended. Under OR a term does not constrain the result, so
// treating it as a bound would drop rows.
func collectRangeTerms(expr Expr, colIndex map[string]int, out map[int]*rangeTerm) {
	b, ok := expr.(*Binary)
	if !ok {
		return
	}
	if b.Op == "AND" {
		collectRangeTerms(b.Left, colIndex, out)
		collectRangeTerms(b.Right, colIndex, out)
		return
	}

	// Normalize `literal op column` into `column op' literal` by mirroring the
	// operator, so both spellings are recognized.
	op := b.Op
	ref, refOK := b.Left.(*VarRef)
	lit, litOK := b.Right.(*Literal)
	if !refOK || !litOK {
		ref, refOK = b.Right.(*VarRef)
		lit, litOK = b.Left.(*Literal)
		if !refOK || !litOK {
			return
		}
		switch op {
		case "<":
			op = ">"
		case "<=":
			op = ">="
		case ">":
			op = "<"
		case ">=":
			op = "<="
		default:
			return
		}
	}

	pos, found := colIndex[ref.Lower]
	if !found {
		return
	}
	term, exists := out[pos]
	if !exists {
		t := newRangeTerm()
		term = &t
		out[pos] = term
	}
	switch op {
	case ">":
		term.tightenLower(lit.Val, false)
	case ">=":
		term.tightenLower(lit.Val, true)
	case "<":
		term.tightenUpper(lit.Val, false)
	case "<=":
		term.tightenUpper(lit.Val, true)
	}
}

// rangeIndexPlan describes a chosen equality-prefix-plus-range access path.
type rangeIndexPlan struct {
	index      *storage.SecondaryIndex
	prefix     []any
	lo         storage.IndexRangeBound
	hi         storage.IndexRangeBound
	predicates []string
	rangeCol   string
}

// selectRangeIndex picks the index whose leading columns are equality-matched and
// whose next column carries a usable range, preferring the longest equality
// prefix and then the most constrained range.
func selectRangeIndex(table *storage.Table, colIndex map[string]int, where Expr) (*rangeIndexPlan, bool) {
	if where == nil || table == nil || len(table.Indexes) == 0 {
		return nil, false
	}

	ranges := make(map[int]*rangeTerm)
	collectRangeTerms(where, colIndex, ranges)
	if len(ranges) == 0 {
		return nil, false
	}
	equalities := make(map[int]any)
	collectEqualityTerms(where, colIndex, equalities)

	var best *rangeIndexPlan
	bestScore := -1
	for _, name := range sortedIndexNames(table) {
		idx := table.Indexes[name]
		if idx == nil || len(idx.Columns) == 0 {
			continue
		}
		prefix := make([]any, 0, len(idx.Columns))
		predicates := make([]string, 0, len(idx.Columns))
		usable := false
		var plan *rangeIndexPlan

		for _, column := range idx.Columns {
			pos, err := table.ColIndex(column)
			if err != nil {
				break
			}
			// Equality columns extend the prefix, under the same numeric-tag
			// safety rule the point seek uses.
			if value, ok := equalities[pos]; ok {
				if isNumericSQLValue(value) && !numericSecondaryIndexSeekSafe(table, pos, value) {
					break
				}
				prefix = append(prefix, value)
				predicates = append(predicates, column+" = ?")
				continue
			}
			// Otherwise this column may carry the range, and the walk stops here.
			term, ok := ranges[pos]
			if !ok || !term.bounded() {
				break
			}
			lo, hi, ok := rangeBoundsForColumn(table, pos, *term)
			if !ok {
				break
			}
			plan = &rangeIndexPlan{
				index:      idx,
				prefix:     append([]any(nil), prefix...),
				lo:         lo,
				hi:         hi,
				predicates: append(append([]string(nil), predicates...), rangePredicateText(column, lo, hi)),
				rangeCol:   column,
			}
			usable = true
			break
		}
		if !usable || plan == nil {
			continue
		}
		// Prefer more equality columns, then a two-sided range over a one-sided
		// one: both narrow the walk.
		score := len(plan.prefix) * 2
		if !plan.lo.Absent && !plan.hi.Absent {
			score++
		}
		if score > bestScore {
			best, bestScore = plan, score
		}
	}
	if best == nil {
		return nil, false
	}
	return best, true
}

func sortedIndexNames(table *storage.Table) []string {
	names := make([]string, 0, len(table.Indexes))
	for name := range table.Indexes {
		names = append(names, name)
	}
	// Deterministic index choice for a given shape, matching selectSecondaryIndex.
	sort.Strings(names)
	return names
}

// rangeBoundsForColumn converts a column's collected bounds into encodable
// numeric bounds of the column's own kind, or reports that no range seek is safe.
func rangeBoundsForColumn(table *storage.Table, pos int, term rangeTerm) (storage.IndexRangeBound, storage.IndexRangeBound, bool) {
	lo, hi := term.lo, term.hi

	// Both bounds must be numeric; text ranges cannot use the index because text
	// keys are length-framed rather than value-ordered.
	for _, b := range []storage.IndexRangeBound{lo, hi} {
		if b.Absent {
			continue
		}
		if !isNumericSQLValue(b.Value) {
			return lo, hi, false
		}
	}

	// The column must hold one numeric kind, and the bounds must be expressed in
	// that kind: integers and floats carry different type tags and so sort as
	// separate blocks rather than by value.
	wantFloat := numericColumnHasFloat(table, pos)
	if wantFloat && !numericColumnIsAllFloat(table, pos) {
		return lo, hi, false // genuinely mixed; no ordering to walk
	}
	convert := func(b storage.IndexRangeBound) (storage.IndexRangeBound, bool) {
		if b.Absent {
			return b, true
		}
		v, ok := coerceRangeBound(b.Value, wantFloat)
		if !ok {
			return b, false
		}
		b.Value = v
		return b, storage.IndexRangeComponentEncodable(v)
	}
	var ok bool
	if lo, ok = convert(lo); !ok {
		return lo, hi, false
	}
	if hi, ok = convert(hi); !ok {
		return lo, hi, false
	}
	lo, hi = normalizeSignedZeroBounds(lo, hi)
	return lo, hi, true
}

// normalizeSignedZeroBounds picks the signed zero whose *encoding* matches what
// numeric comparison means at a zero boundary.
//
// -0.0 and 0.0 compare equal, but the float key encoding flips the bits of
// negative values, so -0.0 sorts strictly below +0.0. A byte walk bounded by
// +0.0 therefore disagrees with SQL: `v >= 0.0` would skip a stored -0.0, and
// `v < 0.0` would wrongly include it. Since a missed row cannot be recovered by
// the residual filter, the bound is moved to the zero that brackets both:
//
//	v >= 0.0  ->  lower bound -0.0 inclusive  (admits -0.0 and +0.0)
//	v >  0.0  ->  lower bound +0.0 exclusive  (the seek already skips -0.0)
//	v <= 0.0  ->  upper bound +0.0 inclusive  (-0.0 sorts below, so included)
//	v <  0.0  ->  upper bound -0.0 exclusive  (excludes both zeros)
func normalizeSignedZeroBounds(lo, hi storage.IndexRangeBound) (storage.IndexRangeBound, storage.IndexRangeBound) {
	negZero := math.Copysign(0, -1)
	if f, ok := lo.Value.(float64); ok && !lo.Absent && f == 0 {
		if lo.Inclusive {
			lo.Value = negZero
		} else {
			lo.Value = float64(0)
		}
	}
	if f, ok := hi.Value.(float64); ok && !hi.Absent && f == 0 {
		if hi.Inclusive {
			hi.Value = float64(0)
		} else {
			hi.Value = negZero
		}
	}
	return lo, hi
}

// coerceRangeBound expresses a bound in the column's numeric kind.
//
// An integer bound against a float column widens exactly. A float bound against
// an integer column is *not* narrowed by truncation, which would silently move
// the boundary: `x > 1.5` on an integer column would become `x > 1`, admitting 1.
// Such a bound is refused and the query falls back to a scan.
func coerceRangeBound(v any, wantFloat bool) (any, bool) {
	switch n := v.(type) {
	case int:
		if wantFloat {
			return float64(n), true
		}
		return n, true
	case int64:
		if wantFloat {
			return float64(n), true
		}
		return n, true
	case float64:
		if wantFloat {
			return n, true
		}
		if n == math.Trunc(n) && !math.IsInf(n, 0) {
			return int(n), true
		}
		return nil, false
	default:
		return nil, false
	}
}

func rangePredicateText(column string, lo, hi storage.IndexRangeBound) string {
	switch {
	case !lo.Absent && !hi.Absent:
		return column + " BETWEEN ? AND ?"
	case !lo.Absent:
		if lo.Inclusive {
			return column + " >= ?"
		}
		return column + " > ?"
	default:
		if hi.Inclusive {
			return column + " <= ?"
		}
		return column + " < ?"
	}
}
