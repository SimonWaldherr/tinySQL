package engine

import (
	"slices"
	"strings"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

const maxIndexChoices = 64

// selectIndexChoices handles a bounded OR/IN term, optionally inside AND.
// Every alternative must have a safe index seek. The complete WHERE remains
// the residual, so these row IDs are only a candidate superset.
func selectIndexChoices(table *storage.Table, colIndex map[string]int, where Expr) ([]int, bool, error) {
	if binary, ok := where.(*Binary); ok && binary.Op == "AND" {
		rows, found, err := selectIndexChoices(table, colIndex, binary.Left)
		if found || err != nil {
			return rows, found, err
		}
		return selectIndexChoices(table, colIndex, binary.Right)
	}
	switch ex := where.(type) {
	case *InExpr:
		if ex.Negate {
			return nil, false, nil
		}
	case *Binary:
		if ex.Op != "OR" {
			return nil, false, nil
		}
	default:
		return nil, false, nil
	}
	choices := make([]*Binary, 0, 4)
	if !collectIndexChoices(where, &choices) {
		return nil, false, nil
	}
	// Broad unions lose to a scan and needlessly allocate large row-ID arrays.
	// Keep enough headroom for small tables and cap work for selective queries.
	maxRows := max(64, len(table.Rows)/4)
	rows := make([]int, 0, min(16, maxRows))
	for _, choice := range choices {
		ref, ok := choice.Left.(*VarRef)
		if !ok {
			ref = choice.Right.(*VarRef)
		}
		if _, known := colIndex[strings.ToLower(ref.Name)]; !known {
			return nil, false, nil // preserve unknown-column errors, even for NULL
		}
		// x = NULL cannot be true, even within OR. Dropping this alternative
		// does not turn unknown into true: the original residual still runs.
		if lit, ok := choice.Left.(*Literal); ok && lit.Val == nil {
			continue
		}
		if lit, ok := choice.Right.(*Literal); ok && lit.Val == nil {
			continue
		}
		var candidates []int
		if idx, values, _, _ := selectSecondaryIndex(table, colIndex, choice); idx != nil {
			// A prefix may fan out into many rows before a union budget can be
			// checked. Keep this path to complete point keys; other shapes scan.
			if len(values) != len(idx.Columns) {
				return nil, false, nil
			}
			var err error
			candidates, err = table.LookupSecondaryIndexPoint(idx, values)
			if err != nil {
				return nil, false, err
			}
		} else {
			var ok bool
			candidates, _, _, ok = selectConstraintIndex(table, colIndex, choice)
			if !ok {
				return nil, false, nil
			}
		}
		if len(candidates) > maxRows-len(rows) {
			return nil, false, nil
		}
		rows = append(rows, candidates...)
	}
	slices.Sort(rows)
	rows = slices.Compact(rows)
	return rows, true, nil
}

// collectIndexChoices accepts only literal equality leaves. It never evaluates
// expressions or subqueries while planning, and bound literals are inspected
// afresh on each execution rather than retained as cached candidate IDs.
func collectIndexChoices(expr Expr, out *[]*Binary) bool {
	if len(*out) >= maxIndexChoices {
		return false
	}
	switch ex := expr.(type) {
	case *Binary:
		if ex.Op == "OR" {
			return collectIndexChoices(ex.Left, out) && collectIndexChoices(ex.Right, out)
		}
		if ex.Op != "=" {
			return false
		}
		_, leftRef := ex.Left.(*VarRef)
		_, rightLit := ex.Right.(*Literal)
		_, rightRef := ex.Right.(*VarRef)
		_, leftLit := ex.Left.(*Literal)
		if !(leftRef && rightLit || rightRef && leftLit) {
			return false
		}
		*out = append(*out, ex)
		return true
	case *InExpr:
		if ex.Negate || len(ex.Values) > maxIndexChoices-len(*out) {
			return false
		}
		if _, ok := ex.Expr.(*VarRef); !ok {
			return false
		}
		for _, value := range ex.Values {
			if _, ok := value.(*Literal); !ok {
				return false
			}
			*out = append(*out, &Binary{Op: "=", Left: ex.Expr, Right: value})
		}
		return true
	default:
		return false
	}
}
