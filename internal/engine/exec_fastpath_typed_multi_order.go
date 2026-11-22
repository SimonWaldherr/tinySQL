// Multi-column ORDER BY over direct column references. Each candidate's sort
// keys are encoded once into a byte string whose lexicographic order equals the
// generic comparator's order. A key contributes a NULL marker (placed by NULLS
// FIRST or LAST, never by direction), or a marker followed by an order-preserving
// payload, inverted for DESC. The scan position follows, so ties keep scan
// order. The full sort then compares bytes rather than boxed values.
//
// Every column must hold one family of values: integers (int and int64), floats
// (NaN declines), or strings. Other types, and mixed families, take the generic
// path, which keeps the rules for them.
package engine

import (
	"bytes"
	"encoding/binary"
	"math"
	"slices"
	"strings"
)

type typedOrderFamily uint8

const (
	typedFamilyNone typedOrderFamily = iota
	typedFamilyInteger
	typedFamilyFloat
	typedFamilyText
)

// Markers sort NULL before or after every value, independent of direction.
const (
	typedMarkerNullFirst = 0x00
	typedMarkerValue     = 0x80
	typedMarkerNullLast  = 0xFF
)

// typedMultiOrder holds the ORDER BY terms that the encoder reads.
type typedMultiOrder struct {
	columns    []int
	desc       []bool
	nullsFirst []bool
}

// encodedOrderItem is one candidate whose key bytes include its scan position.
// row indexes the source rows for projection.
type encodedOrderItem struct {
	key []byte
	row int32
}

// simpleTypedMultiOrderColumns returns the raw column of each ORDER BY term when
// there are several and every term names a column directly.
func simpleTypedMultiOrderColumns(plan *simpleSelectPlan) ([]int, bool) {
	if len(plan.orderBy) < 2 || len(plan.orderCols) != len(plan.orderBy) {
		return nil, false
	}
	return plan.orderCols, true
}

// executeTypedMultiOrderedSelect fully sorts the candidates by encoded keys. It
// handles unbounded orders and pages that keep most candidates. Encoding every
// candidate costs more than a bounded heap saves when few are kept, so those
// pages decline to the generic path. It reports handled=false, without error,
// when the generic path must decide.
func executeTypedMultiOrderedSelect(env ExecEnv, plan *simpleSelectPlan, sourceRows [][]any, columns []int, keepCount int) (*ResultSet, bool, error) {
	candidates := len(sourceRows)
	if plan.rowIDs != nil {
		candidates = len(plan.rowIDs)
	}
	if keepCount >= 0 && keepCount <= candidates-candidates/10 {
		return nil, false, nil
	}
	if len(sourceRows) > math.MaxInt32 {
		return nil, false, nil
	}
	supported, err := typedMultiOrderSupported(env, plan, sourceRows, columns, candidates)
	if err != nil {
		return nil, true, err
	}
	if !supported {
		return nil, false, nil
	}

	order := typedMultiOrder{
		columns:    columns,
		desc:       make([]bool, len(columns)),
		nullsFirst: make([]bool, len(columns)),
	}
	for k := range columns {
		order.desc[k] = plan.orderBy[k].Desc
		order.nullsFirst[k] = plan.orderBy[k].Nulls == NullsFirst
	}

	var scanPhase *queryPhase
	if env.profile != nil {
		_, scanPhase = startQueryPhase(env, "SCAN/FILTER", "collect raw ordering keys")
		defer scanPhase.finish(-1)
	}

	// Every key lives in one arena. Items keep capped views into it, so growing
	// the arena never moves bytes that an earlier item still references.
	arena := make([]byte, 0, candidates*(len(columns)*typedKeyBytesHint+typedPosBytes))
	items := make([]encodedOrderItem, 0, candidates)
	for i := 0; i < candidates; i++ {
		rowID := i
		if plan.rowIDs != nil {
			rowID = plan.rowIDs[i]
		}
		raw := sourceRows[rowID]
		if i&63 == 0 {
			if err := checkCtx(env.ctx); err != nil {
				return nil, true, err
			}
		}
		var match bool
		var err error
		if plan.filter != nil {
			match, err = plan.filter(raw)
		} else {
			match, err = evalRawWhere(plan, raw)
		}
		if err != nil {
			return nil, true, err
		}
		if !match {
			continue
		}
		start := len(arena)
		arena = appendTypedMultiOrderKey(arena, raw, &order, int32(i))
		items = append(items, encodedOrderItem{key: arena[start:len(arena):len(arena)], row: int32(rowID)})
	}
	scanPhase.finish(int64(len(items)))

	var sortPhase *queryPhase
	if env.profile != nil {
		_, sortPhase = startQueryPhase(env, "SORT", "order raw candidates")
	}
	slices.SortFunc(items, func(a, b encodedOrderItem) int {
		return bytes.Compare(a.key, b.key)
	})
	sortPhase.finish(int64(len(items)))

	start := 0
	if plan.offset != nil && *plan.offset > 0 {
		start = *plan.offset
	}
	if start > len(items) {
		return &ResultSet{Cols: plan.outputCols, Rows: []Row{}}, true, nil
	}
	items = items[start:]
	if plan.limit != nil && *plan.limit < len(items) {
		items = items[:*plan.limit]
	}

	var projectPhase *queryPhase
	if env.profile != nil {
		_, projectPhase = startQueryPhase(env, "PROJECT", "materialize ordered page")
		defer projectPhase.finish(-1)
	}
	outRows := make([]Row, 0, len(items))
	for _, item := range items {
		out, err := projectRawRow(plan, sourceRows[item.row])
		if err != nil {
			return nil, true, err
		}
		outRows = append(outRows, out)
	}
	projectPhase.finish(int64(len(outRows)))
	return &ResultSet{Cols: plan.outputCols, Rows: outRows}, true, nil
}

// typedMultiOrderSupported reports whether every candidate's ORDER BY columns
// hold one family of values per column. It returns false for a missing column
// or row id, so the generic path reports the same error it always did.
func typedMultiOrderSupported(env ExecEnv, plan *simpleSelectPlan, sourceRows [][]any, columns []int, candidates int) (bool, error) {
	families := make([]typedOrderFamily, len(columns))
	for i := 0; i < candidates; i++ {
		rowID := i
		if plan.rowIDs != nil {
			rowID = plan.rowIDs[i]
		}
		if rowID < 0 || rowID >= len(sourceRows) {
			return false, nil
		}
		if i&63 == 0 {
			if err := checkCtx(env.ctx); err != nil {
				return false, err
			}
		}
		raw := sourceRows[rowID]
		for k, column := range columns {
			if column >= len(raw) {
				return false, nil
			}
			var family typedOrderFamily
			switch value := raw[column].(type) {
			case nil:
				continue
			case int, int64:
				family = typedFamilyInteger
			case float64:
				// NaN is not totally ordered; the generic comparison decides it.
				if math.IsNaN(value) {
					return false, nil
				}
				family = typedFamilyFloat
			case string:
				family = typedFamilyText
			default:
				return false, nil
			}
			if families[k] != typedFamilyNone && families[k] != family {
				return false, nil
			}
			families[k] = family
		}
	}
	return true, nil
}

// appendTypedMultiOrderKey appends the encoded keys of one candidate, then its
// scan position. Callers must have validated the columns with
// typedMultiOrderSupported.
func appendTypedMultiOrderKey(dst []byte, raw []any, order *typedMultiOrder, pos int32) []byte {
	for k, column := range order.columns {
		value := raw[column]
		if value == nil {
			if order.nullsFirst[k] {
				dst = append(dst, typedMarkerNullFirst)
			} else {
				dst = append(dst, typedMarkerNullLast)
			}
			continue
		}
		dst = append(dst, typedMarkerValue)
		desc := order.desc[k]
		switch v := value.(type) {
		case int:
			dst = appendOrderedInteger(dst, int64(v), desc)
		case int64:
			dst = appendOrderedInteger(dst, v, desc)
		case float64:
			dst = appendOrderedFloat(dst, v, desc)
		case string:
			dst = appendOrderedText(dst, v, desc)
		}
	}
	return binary.BigEndian.AppendUint32(dst, uint32(pos))
}

// appendOrderedInteger writes x as eight bytes whose unsigned order matches the
// signed order. DESC inverts every bit, which reverses that order.
func appendOrderedInteger(dst []byte, x int64, desc bool) []byte {
	u := uint64(x) ^ (1 << 63)
	if desc {
		u = ^u
	}
	return binary.BigEndian.AppendUint64(dst, u)
}

// appendOrderedFloat writes f as eight bytes whose unsigned order matches the
// IEEE order for non-NaN values. Negative zero takes the bits of positive zero,
// because the generic comparison treats them as equal.
func appendOrderedFloat(dst []byte, f float64, desc bool) []byte {
	bits := math.Float64bits(f)
	if f == 0 {
		bits = 0
	}
	if bits&(1<<63) != 0 {
		bits = ^bits
	} else {
		bits |= 1 << 63
	}
	if desc {
		bits = ^bits
	}
	return binary.BigEndian.AppendUint64(dst, bits)
}

// appendOrderedText writes s self-terminated, so a shorter string sorts before
// any longer string it prefixes. Zero bytes are escaped as 00 FF, which keeps the
// terminator 00 01 below every escaped byte. DESC inverts the whole segment.
func appendOrderedText(dst []byte, s string, desc bool) []byte {
	start := len(dst)
	for {
		zero := strings.IndexByte(s, 0)
		if zero < 0 {
			dst = append(dst, s...)
			break
		}
		dst = append(dst, s[:zero]...)
		dst = append(dst, 0x00, 0xFF)
		s = s[zero+1:]
	}
	dst = append(dst, 0x00, 0x01)
	if desc {
		for i := start; i < len(dst); i++ {
			dst[i] = ^dst[i]
		}
	}
	return dst
}

// Arena sizing hints: a key term takes a marker and an eight-byte payload, and
// every key ends with the four-byte scan position.
const (
	typedKeyBytesHint = 10
	typedPosBytes     = 4
)
