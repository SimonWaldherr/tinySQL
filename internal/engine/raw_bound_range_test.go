package engine

import (
	"fmt"
	"math"
	"math/big"
	"testing"
	"time"
)

func TestRawBoundRangeFilterRebinding(t *testing.T) {
	columns := map[string]int{"v": 0, "t.v": 0}
	values := []any{
		nil, int(-1), int(0), int(1), int64(0), int64(1),
		int64(math.MinInt64), int64(math.MaxInt64), int64(9007199254740992), int64(9007199254740993),
		float64(-1), float64(0), math.Copysign(0, -1), float64(1), float64(1.5),
		float64(9007199254740992), math.NaN(), math.Inf(-1), math.Inf(1),
		"", "a", "z", "漢", true, false, []byte("a"), time.Unix(0, 0), big.NewRat(1, 2),
	}
	bounds := [][2]any{
		{int(0), int(1)}, {int64(0), int64(1)}, {int(0), int64(1)}, {int64(0), int(1)},
		{int64(9007199254740992), int64(9007199254740993)}, {int64(math.MinInt64), int64(math.MaxInt64)},
		{int(1), int(0)}, {float64(0), float64(1)}, {float64(1), float64(0)},
		{int64(9007199254740993), float64(9007199254740992)}, {float64(9007199254740992), int64(9007199254740993)},
		{math.Inf(-1), math.Inf(1)}, {math.NaN(), float64(1)}, {float64(0), math.NaN()},
		{"a", "z"}, {"z", "a"}, {"", "漢"}, {nil, nil}, {nil, int(0)}, {int(0), nil},
		{int(0), "z"}, {"a", int(0)}, {true, false}, {[]byte("a"), []byte("z")},
		{time.Unix(-1, 0), time.Unix(1, 0)}, {big.NewRat(0, 1), big.NewRat(1, 1)},
	}
	for _, outside := range []bool{false, true} {
		lowerOp, upperOp := ">=", "<="
		if outside {
			lowerOp, upperOp = "<", ">"
		}
		// Compile before binding any values, then repeatedly change their
		// values and types, as a cached prepared statement does.
		lo, hi := &Literal{Parameter: true}, &Literal{Parameter: true}
		left := &Binary{Op: lowerOp, Left: &VarRef{Name: "V"}, Right: lo}
		right := &Binary{Op: upperOp, Left: &VarRef{Name: "t.v"}, Right: hi}
		filter := buildRawRangeFilter(columns, left, right, outside)
		if filter == nil {
			t.Fatal("parameter range did not compile")
		}
		original := separateRangeComparisons(buildRawComparisonFilter(columns, left), buildRawComparisonFilter(columns, right), outside)
		for _, pair := range bounds {
			lo.Val, hi.Val = pair[0], pair[1]
			for _, value := range values {
				// The general decimal comparator cannot represent nonfinite
				// floating-point bounds; those combinations are not supported.
				if _, decimal := value.(*big.Rat); decimal && (nonfiniteRangeBound(pair[0]) || nonfiniteRangeBound(pair[1])) {
					continue
				}
				raw := []any{value}
				want, wantErr := original(raw)
				got, gotErr := filter(raw)
				if got != want || fmt.Sprint(gotErr) != fmt.Sprint(wantErr) {
					t.Fatalf("value=%v (%T), bounds=%v outside=%v: got %v/%v, want %v/%v", value, value, pair, outside, got, gotErr, want, wantErr)
				}
			}
		}
	}
}

func nonfiniteRangeBound(value any) bool {
	v, ok := value.(float64)
	return ok && (math.IsNaN(v) || math.IsInf(v, 0))
}

func TestRawBoundRangeFilterShortRows(t *testing.T) {
	columns := map[string]int{"v": 1}
	for _, outside := range []bool{false, true} {
		lowerOp, upperOp := ">=", "<="
		if outside {
			lowerOp, upperOp = "<", ">"
		}
		lo, hi := &Literal{Parameter: true}, &Literal{Parameter: true}
		left := &Binary{Op: lowerOp, Left: &VarRef{Name: "v"}, Right: lo}
		right := &Binary{Op: upperOp, Left: &VarRef{Name: "v"}, Right: hi}
		filter := buildRawRangeFilter(columns, left, right, outside)
		original := separateRangeComparisons(buildRawComparisonFilter(columns, left), buildRawComparisonFilter(columns, right), outside)
		for _, bounds := range [][2]any{{nil, nil}, {nil, 2}, {1, nil}, {1, 2}, {"a", "z"}} {
			lo.Val, hi.Val = bounds[0], bounds[1]
			for _, row := range [][]any{nil, {1}} {
				want := rangeFilterPanic(original, row)
				got := rangeFilterPanic(filter, row)
				if want == nil || fmt.Sprint(got) != fmt.Sprint(want) {
					t.Fatalf("bounds=%v outside=%v row=%v: got panic %v, want %v", bounds, outside, row, got, want)
				}
			}
		}
	}
}

func rangeFilterPanic(filter func([]any) (bool, error), row []any) (caught any) {
	defer func() { caught = recover() }()
	_, _ = filter(row)
	return nil
}

func BenchmarkRawBoundRangeFilter(b *testing.B) {
	for _, kind := range []string{"Integer", "Float", "Text", "MixedNumeric", "NullLower", "NullUpper"} {
		rows := make([][]any, 100_000)
		for i := range rows {
			switch kind {
			case "Integer", "MixedNumeric", "NullLower", "NullUpper":
				rows[i] = []any{int64(i)}
			case "Float":
				rows[i] = []any{float64(i)}
			case "Text":
				rows[i] = []any{fmt.Sprintf("%05d", i)}
			}
		}
		for _, outside := range []bool{false, true} {
			predicate := "value BETWEEN 45000 AND 55000"
			if outside {
				predicate = "value NOT BETWEEN 45000 AND 55000"
			}
			binary := mustParse("SELECT value FROM t WHERE " + predicate).(*Select).Where.(*Binary)
			lo := binary.Left.(*Binary).Right.(*Literal)
			hi := binary.Right.(*Binary).Right.(*Literal)
			lo.Parameter, hi.Parameter = true, true
			switch kind {
			case "Integer":
				lo.Val, hi.Val = int64(45000), int64(55000)
			case "Float":
				lo.Val, hi.Val = float64(45000), float64(55000)
			case "Text":
				lo.Val, hi.Val = "45000", "55000"
			case "MixedNumeric":
				lo.Val, hi.Val = int64(45000), float64(55000)
			case "NullLower":
				lo.Val, hi.Val = nil, int64(55000)
			case "NullUpper":
				lo.Val, hi.Val = int64(45000), nil
			}
			filter := buildRawFilter(map[string]int{"value": 0}, binary)
			b.Run(kind+"/"+predicate, func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					matches := 0
					for _, row := range rows {
						match, err := filter(row)
						if err != nil {
							b.Fatal(err)
						}
						if match {
							matches++
						}
					}
					want := 10001
					if outside {
						want = len(rows) - want
					}
					if kind == "NullLower" {
						want = 0
						if outside {
							want = 44999
						}
					}
					if kind == "NullUpper" {
						want = 0
						if outside {
							want = 45000
						}
					}
					if matches != want {
						b.Fatalf("got %d matches, want %d", matches, want)
					}
				}
			})
		}
	}
}
