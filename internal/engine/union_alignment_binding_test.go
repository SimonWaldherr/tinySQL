package engine

import (
	"context"
	"errors"
	"math"
	"math/big"
	"reflect"
	"slices"
	"sync"
	"testing"
)

func legacySetOperationAlignment(rows []Row, source, target []string) []Row {
	if sameSetOperationColumns(source, target) {
		return rows
	}
	result := make([]Row, len(rows))
	for i, row := range rows {
		out := make(Row, len(target))
		for column, name := range source {
			value, _ := getVal(row, name)
			putVal(out, target[column], value)
		}
		result[i] = out
	}
	return result
}

func TestUnionAlignmentBoundKeysMatchLegacy(t *testing.T) {
	rows := []Row{nil, {}, {"a": 1, "b": nil, "ä": "α🙂", "hidden": "ignored"},
		{"a": []byte{0, 255}, "b": big.NewRat(1, 2), "ä": map[string]any{"value": 1}},
		{"A": 99, "b": true, "a�": 7}}
	for _, shape := range []struct{ source, target []string }{
		{[]string{"A", "B", "Ä"}, []string{"Result", "Value", "Unicode"}},
		{[]string{"A", "B"}, []string{"Repeated", "REPEATED"}},
		{[]string{"A", "a"}, []string{"First", "Second"}},
		{[]string{"A\xff"}, []string{"Target\xff"}},
		{[]string{"A", "B"}, []string{"a", "b"}},
	} {
		source, target := slices.Clone(shape.source), slices.Clone(shape.target)
		for _, input := range [][]Row{nil, {}, rows} {
			want := legacySetOperationAlignment(input, source, target)
			got, err := alignSetOperationRowsWithContext(t.Context(), input, source, target)
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("%v -> %v: got %#v / %v, want %#v", source, target, got, err, want)
			}
		}
		if !reflect.DeepEqual(source, shape.source) || !reflect.DeepEqual(target, shape.target) {
			t.Fatal("alignment changed column metadata")
		}
	}
}

func TestUnionAlignmentBoundKeysOwnOutput(t *testing.T) {
	rows := []Row{{"source": "original"}}
	source, target := []string{"Source"}, []string{"Target"}
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			got, err := alignSetOperationRowsWithContext(t.Context(), rows, source, target)
			if err != nil || len(got) != 1 || got[0]["target"] != "original" {
				t.Errorf("alignment: %#v / %v", got, err)
				return
			}
			got[0]["target"] = "changed"
		})
	}
	wg.Wait()
	if rows[0]["source"] != "original" || len(rows[0]) != 1 {
		t.Fatal("alignment modified its source")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := alignSetOperationRowsWithContext(ctx, rows, source, target); !errors.Is(err, context.Canceled) {
		t.Fatal("cancelled alignment", err)
	}
}

func TestUnionDistinctRenamingUsesLastOutputColumn(t *testing.T) {
	left := []Row{{"same": int64(1), "other": "left"}}
	right := &ResultSet{
		Cols: []string{"First", "Last", "Label"},
		Rows: []Row{{"first": 2, "last": 1.0, "label": "left"},
			{"first": 2, "last": nil, "label": "new", "hidden": true},
			{"first": 1, "last": nil, "label": "new"}},
	}
	cols := []string{"SAME", "Same", "Other"}
	acc := newSetOperationDistinctAccumulator(cols, len(left)+len(right.Rows))
	if err := acc.appendRows(t.Context(), left); err != nil {
		t.Fatal(err)
	}
	if err := acc.appendTerm(t.Context(), right, cols); err != nil {
		t.Fatal(err)
	}
	want := []Row{{"same": int64(1), "other": "left"}, {"same": nil, "other": "new"}}
	if !reflect.DeepEqual(acc.rows, want) {
		t.Fatalf("rows = %#v, want %#v", acc.rows, want)
	}
	if len(right.Rows[1]) != 4 || right.Rows[1]["first"] != 2 || right.Rows[1]["hidden"] != true {
		t.Fatalf("remapping changed the right input: %#v", right.Rows)
	}
}

func TestSetOperationNumericSignatureEquivalence(t *testing.T) {
	groups := [][]any{
		{0, int64(0), 0.0, math.Copysign(0, -1)},
		{1, int64(1), 1.0},
		{int64(9007199254740992), int64(9007199254740993), float64(9007199254740992)},
		{math.NaN(), math.Float64frombits(0xfff8000000001234)},
		{math.Inf(1)}, {math.Inf(-1)}, {math.SmallestNonzeroFloat64},
		{nil}, {"1"}, {true}, {[]byte("1")},
	}
	identified := make(map[string]bool)
	cols := []string{"v", "tail"}
	for _, group := range groups {
		var first string
		for i, value := range group {
			key := string(appendSetOperationSignature(nil, Row{"v": value, "tail": "N;|D\x00"}, cols))
			if i == 0 {
				if identified[key] {
					t.Fatalf("signature collision for %T %v", value, value)
				}
				identified[key] = true
				first = key
			} else if key != first {
				t.Fatalf("signature differs for equivalent %T %v and %T %v", group[0], group[0], value, value)
			}
		}
	}
}

func legacyIntersectMembership(left, right []Row, columns []string) []Row {
	keys := setOperationColumnKeys(columns)
	rightSet, seen := make(map[string]bool), make(map[string]bool)
	for _, row := range right {
		rightSet[string(appendSetOperationSignature(nil, row, keys))] = true
	}
	result := make([]Row, 0, len(left))
	for _, row := range left {
		key := string(appendSetOperationSignature(nil, row, keys))
		if rightSet[key] && !seen[key] {
			seen[key] = true
			result = append(result, row)
		}
	}
	return result
}

func TestIntersectConsumesMembershipWithoutChangingRows(t *testing.T) {
	values := []any{nil, false, true, 0, math.Copysign(0, -1), 1, int64(1), 1.0,
		int64(9007199254740993), math.NaN(), math.Inf(1), "1", "a|b", []byte("1"),
		big.NewRat(1, 2), map[string]any{"nested": nil}}
	left := make([]Row, 0, len(values)*3)
	for _, value := range values {
		for range 3 {
			left = append(left, Row{"value": value})
		}
	}
	for _, columns := range [][]string{{"value"}, {}} {
		for _, right := range [][]Row{nil, {}, {{}}, left[:1], left[7:21], left} {
			want := legacyIntersectMembership(left, right, columns)
			for range 2 {
				got, err := intersectRowsWithContext(t.Context(), left, right, columns)
				if err != nil || !reflect.DeepEqual(got, want) {
					t.Fatalf("columns=%v, right=%v: got %#v / %v, want %#v", columns, right, got, err, want)
				}
			}
		}
	}
}

type intersectCancellationProbe struct {
	calls  *int
	cancel context.CancelFunc
}

func (p intersectCancellationProbe) MarshalJSON() ([]byte, error) {
	*p.calls++
	if *p.calls == 66 {
		p.cancel()
	}
	return []byte(`"same"`), nil
}

func TestIntersectConsumedSetStillEvaluatesRemainingRows(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	calls := 0
	value := intersectCancellationProbe{calls: &calls, cancel: cancel}
	left := make([]Row, 150)
	for i := range left {
		left[i] = Row{"value": value}
	}
	// The only membership key is consumed by the first left match. Remaining
	// signatures must still execute and cancellation must remain observable.
	_, err := intersectRowsWithContext(ctx, left, []Row{{"value": value}}, []string{"value"})
	if !errors.Is(err, context.Canceled) || calls < 66 {
		t.Fatalf("got %v after %d callbacks, want cancellation from remaining rows", err, calls)
	}
}
