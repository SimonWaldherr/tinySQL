package engine

import (
	"sort"
	"strings"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// Index-assisted candidate rows for DELETE.
//
// The multi-row DELETE normally evaluates its predicate on every row of the
// table. For the statement an importer issues over and over (replace the chunks
// of one source: DELETE ... WHERE source_id = 'x' AND load_id <> 'y') a secondary
// index on the leading column already names the few rows that can match, so the
// predicate only has to run on those. The complete predicate still decides: the
// index only narrows the rows it is evaluated on.
//
// That is identical to the scan only when two things hold, and this file offers a
// candidate list only when it can prove both from the statement and the schema,
// otherwise executeDelete keeps scanning:
//
//  1. Completeness. Every row for which the predicate is TRUE is in the list. The
//     list is built from top-level conjuncts "text column = 'literal'" and
//     "text column IN ('literal', ...)", on a TEXT/STRING column (native or with
//     TEXT affinity). A cell of such a column is a string or NULL (a BLOB can only
//     get in through SQLite's rule that TEXT affinity leaves BLOBs alone, and it
//     never equals a string), string equality is byte equality, and the index key
//     of a string is its bytes, so a row that satisfies the conjunct is stored
//     under exactly the key that is looked up. Numbers are deliberately not used:
//     SQL compares 1 and 1.0 as equal while the index keeps them apart.
//  2. No new errors. A scan evaluates the predicate on rows that never match, and
//     an error there (division by zero, a comparison of incomparable types, a
//     function failing) would abort the statement; evaluating fewer rows must not
//     turn such a statement into a success. So the whole predicate must be built
//     of AND, OR, NOT, IS [NOT] NULL, comparisons and IN lists between a column
//     and literals of the column's own type, which cannot fail.

// deleteIndexMaxValues bounds an IN list that is turned into index seeks; a longer
// list scans.
const deleteIndexMaxValues = 4096

// deleteCandidateRows returns the ascending positions of the rows that can satisfy
// where, read from a secondary index, or nil, false when the statement has to
// scan. The caller has already established that where is a simple raw predicate
// and that no trigger or foreign key can observe the rows.
func deleteCandidateRows(t *storage.Table, colIndex map[string]int, where Expr) ([]int, bool) {
	if where == nil || len(t.Indexes) == 0 || len(t.Rows) == 0 || !deletePredicateCannotFail(t, colIndex, where) {
		return nil, false
	}
	terms := make(map[int][]string)
	collectDeleteSeekTerms(t, colIndex, where, terms)
	if len(terms) == 0 {
		return nil, false
	}
	var best *storage.SecondaryIndex
	bestLen := 0
	for _, name := range t.SortedIndexNames() {
		idx := t.Indexes[name]
		if idx == nil {
			continue
		}
		n := 0
		for _, column := range idx.Columns {
			pos, err := t.ColIndex(column)
			if err != nil || len(terms[pos]) == 0 {
				break
			}
			n++
		}
		if n > bestLen {
			best, bestLen = idx, n
		}
	}
	if best == nil {
		return nil, false
	}
	// Expand the prefix into the keys to look up. Every column has one value, or
	// several for an IN list; the product is bounded like an IN list is.
	keys := [][]any{{}}
	for _, column := range best.Columns[:bestLen] {
		pos, _ := t.ColIndex(column)
		values := terms[pos]
		if len(keys)*len(values) > deleteIndexMaxValues {
			return nil, false
		}
		next := make([][]any, 0, len(keys)*len(values))
		for _, key := range keys {
			for _, v := range values {
				next = append(next, append(append(make([]any, 0, len(key)+1), key...), v))
			}
		}
		keys = next
	}
	var out []int
	for _, key := range keys {
		var ids []int
		var err error
		if bestLen == len(best.Columns) {
			ids, err = t.LookupSecondaryIndexPoint(best, key)
		} else {
			ids, err = t.LookupSecondaryIndexPrefix(best, key)
		}
		if err != nil {
			return nil, false
		}
		out = append(out, ids...)
	}
	// out was built by appending the lookups' ids onto a fresh slice, so it does not
	// alias the index runs that the statement is about to renumber.
	if len(keys) > 1 {
		sort.Ints(out)
		out = compactSortedInts(out)
	}
	// A list the index cannot vouch for (a stale or foreign position) is not used.
	prev := -1
	for _, id := range out {
		if id <= prev || id >= len(t.Rows) {
			return nil, false
		}
		prev = id
	}
	if out == nil {
		out = []int{}
	}
	return out, true
}

func compactSortedInts(in []int) []int {
	if len(in) < 2 {
		return in
	}
	n := 1
	for _, v := range in[1:] {
		if v != in[n-1] {
			in[n] = v
			n++
		}
	}
	return in[:n]
}

// deleteColumn resolves a column reference of a DELETE predicate.
func deleteColumn(t *storage.Table, colIndex map[string]int, e Expr) (int, bool) {
	ref, ok := e.(*VarRef)
	if !ok {
		return 0, false
	}
	key := ref.Lower
	if key == "" {
		key = strings.ToLower(ref.Name)
	}
	pos, found := colIndex[key]
	if !found || pos < 0 || pos >= len(t.Cols) {
		return 0, false
	}
	return pos, true
}

// deleteComparisonCannotFail reports whether "cell op v" cannot fail for a cell of
// col: a non-NULL literal of the column's own kind. Text is compared for equality
// only (a plain TEXT column carries TEXT affinity, whose cells are strings, NULL
// or, by SQLite rule, BLOBs that an ordering comparison would reject); numbers
// only in natively typed columns, whose cells are coerced to the column type.
func deleteComparisonCannotFail(col storage.Column, op string, v any) bool {
	if col.StrictJSON {
		return false
	}
	switch col.Type {
	case storage.StringType, storage.TextType:
		if col.Affinity != storage.AffinityDefault && col.Affinity != storage.AffinityText {
			return false
		}
		if _, ok := v.(string); !ok {
			return false
		}
		return op == "=" || op == "<>" || op == "!="
	case storage.IntType:
		if col.Affinity != storage.AffinityDefault {
			return false
		}
		switch v.(type) {
		case int, int64:
			return true
		}
	case storage.FloatType:
		if col.Affinity != storage.AffinityDefault {
			return false
		}
		switch v.(type) {
		case int, int64, float64:
			return true
		}
	}
	return false
}

// deleteTextColumn reports whether equality with a string literal can be answered
// from an index key: the cells are strings or NULL, and a string's key is its bytes.
func deleteTextColumn(col storage.Column) bool {
	return !col.StrictJSON && (col.Type == storage.StringType || col.Type == storage.TextType) &&
		(col.Affinity == storage.AffinityDefault || col.Affinity == storage.AffinityText)
}

// deletePredicateCannotFail implements condition 2 above.
func deletePredicateCannotFail(t *storage.Table, colIndex map[string]int, e Expr) bool {
	switch ex := e.(type) {
	case *Binary:
		switch {
		case ex.Op == "AND" || ex.Op == "OR":
			return deletePredicateCannotFail(t, colIndex, ex.Left) && deletePredicateCannotFail(t, colIndex, ex.Right)
		case isComparisonOp(ex.Op):
			pos, ok := deleteColumn(t, colIndex, ex.Left)
			lit, isLit := ex.Right.(*Literal)
			if !ok || !isLit {
				pos, ok = deleteColumn(t, colIndex, ex.Right)
				lit, isLit = ex.Left.(*Literal)
			}
			return ok && isLit && deleteComparisonCannotFail(t.Cols[pos], ex.Op, lit.Val)
		}
		return false
	case *Unary:
		return ex.Op == "NOT" && deletePredicateCannotFail(t, colIndex, ex.Expr)
	case *IsNull:
		_, ok := deleteColumn(t, colIndex, ex.Expr)
		return ok
	case *InExpr:
		pos, ok := deleteColumn(t, colIndex, ex.Expr)
		if !ok || ex.Subquery || len(ex.Values) == 0 {
			return false
		}
		for _, v := range ex.Values {
			lit, isLit := v.(*Literal)
			if !isLit || !deleteComparisonCannotFail(t.Cols[pos], "=", lit.Val) {
				return false
			}
		}
		return true
	}
	return false
}

// collectDeleteSeekTerms gathers, from the top-level AND chain of e, the values
// that a text column must equal: one for "col = 'x'", several for an IN list.
// Only the first term of a column is used; the others stay in the predicate.
func collectDeleteSeekTerms(t *storage.Table, colIndex map[string]int, e Expr, out map[int][]string) {
	switch ex := e.(type) {
	case *Binary:
		if ex.Op == "AND" {
			collectDeleteSeekTerms(t, colIndex, ex.Left, out)
			collectDeleteSeekTerms(t, colIndex, ex.Right, out)
			return
		}
		if ex.Op != "=" {
			return
		}
		pos, ok := deleteColumn(t, colIndex, ex.Left)
		lit, isLit := ex.Right.(*Literal)
		if !ok || !isLit {
			pos, ok = deleteColumn(t, colIndex, ex.Right)
			lit, isLit = ex.Left.(*Literal)
		}
		if !ok || !isLit || !deleteTextColumn(t.Cols[pos]) {
			return
		}
		if s, isString := lit.Val.(string); isString {
			if _, taken := out[pos]; !taken {
				out[pos] = []string{s}
			}
		}
	case *InExpr:
		pos, ok := deleteColumn(t, colIndex, ex.Expr)
		if !ok || ex.Negate || ex.Subquery || len(ex.Values) > deleteIndexMaxValues || !deleteTextColumn(t.Cols[pos]) {
			return
		}
		if _, taken := out[pos]; taken {
			return
		}
		values := make([]string, 0, len(ex.Values))
		for _, v := range ex.Values {
			lit, isLit := v.(*Literal)
			s, isString := "", false
			if isLit {
				s, isString = lit.Val.(string)
			}
			if !isString {
				return
			}
			values = append(values, s)
		}
		out[pos] = values
	}
}
