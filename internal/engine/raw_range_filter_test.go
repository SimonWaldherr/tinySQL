package engine

import (
	"context"
	"math"
	"math/big"
	"testing"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestRawRangeFilterMatchesSeparateComparisons(t *testing.T) {
	values := []any{
		nil, int(-1), int(0), int(1), int64(0), int64(1),
		int64(math.MinInt64), int64(math.MaxInt64), int64(9007199254740992), int64(9007199254740993),
		float64(-1), float64(0), math.Copysign(0, -1), float64(1), float64(1.5),
		float64(9007199254740992), math.NaN(), math.Inf(-1), math.Inf(1),
		"", "a", "z", "漢", true, []byte("a"), big.NewRat(1, 2), time.Unix(0, 0),
	}
	bounds := [][2]any{
		{int(0), int(1)}, {int64(0), int64(1)}, {int(0), int64(1)}, {int64(0), int(1)},
		{int64(9007199254740992), int64(9007199254740993)}, {int64(math.MinInt64), int64(math.MaxInt64)},
		{int(1), int(0)}, {float64(0), float64(1)}, {float64(1), float64(0)},
		{math.Inf(-1), math.Inf(1)}, {math.NaN(), float64(1)}, {float64(0), math.NaN()},
		{"a", "z"}, {"z", "a"}, {"", "漢"},
	}
	columns := map[string]int{"v": 0, "t.v": 0}
	for _, pair := range bounds {
		for _, outside := range []bool{false, true} {
			lowerOp, upperOp := ">=", "<="
			if outside {
				lowerOp, upperOp = "<", ">"
			}
			left := &Binary{Op: lowerOp, Left: &VarRef{Name: "V"}, Right: &Literal{Val: pair[0]}}
			right := &Binary{Op: upperOp, Left: &VarRef{Name: "t.v"}, Right: &Literal{Val: pair[1]}}
			filter := buildRawRangeFilter(columns, left, right, outside)
			if filter == nil {
				t.Fatalf("range not compiled: %v outside=%v", pair, outside)
			}
			original := separateRangeComparisons(buildRawComparisonFilter(columns, left), buildRawComparisonFilter(columns, right), outside)
			for _, value := range values {
				raw := []any{value}
				want, wantErr := original(raw)
				got, gotErr := filter(raw)
				if got != want || (gotErr == nil) != (wantErr == nil) {
					t.Fatalf("%v (%T), bounds=%v outside=%v: got %v/%v want %v/%v", value, value, pair, outside, got, gotErr, want, wantErr)
				}
			}
		}
	}
}

func TestRawRangeFilterDeclinesUnsafeShapes(t *testing.T) {
	columns := map[string]int{"v": 0, "w": 1}
	for _, predicate := range []string{
		"v >= 1 AND w <= 3", "v >= 1 AND missing <= 3", "v >= 1 AND v < 3",
		"v > 1 AND v <= 3", "v >= 1 AND v <= 3.0", "v >= 1.0 AND v <= 3",
		"v >= NULL AND v <= 3", "v >= 1 AND v <= NULL", "v >= 1 AND v <= 'a'",
		"v >= 1 AND v <= 1 / 0", "ABS(v) BETWEEN 1 AND 3",
	} {
		expr := mustParse("SELECT v FROM t WHERE " + predicate).(*Select).Where
		if binary, ok := expr.(*Binary); ok && buildRawRangeFilter(columns, binary.Left, binary.Right, false) != nil {
			t.Fatalf("unsafe shape compiled: %s", predicate)
		}
	}
	for _, lowerParameter := range []bool{false, true} {
		expr := mustParse("SELECT v FROM t WHERE v BETWEEN 1 AND 3").(*Select).Where.(*Binary)
		bound := expr.Right.(*Binary).Right.(*Literal)
		if lowerParameter {
			bound = expr.Left.(*Binary).Right.(*Literal)
		}
		bound.Parameter = true
		if buildRawRangeFilter(columns, expr.Left, expr.Right, false) != nil {
			t.Fatal("parameter value captured")
		}
	}
}

func TestRawRangeFilterCachedParameters(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	execSQL(t, db, "CREATE TABLE range_values (v INT)")
	execSQL(t, db, "INSERT INTO range_values VALUES (1), (2), (3), (NULL)")
	for _, predicate := range []string{"v BETWEEN 1 AND 3", "v NOT BETWEEN 1 AND 3"} {
		stmt := mustParse("SELECT COUNT(*) AS n FROM range_values WHERE " + predicate).(*Select)
		binary := stmt.Where.(*Binary)
		lower, upper := binary.Left.(*Binary).Right.(*Literal), binary.Right.(*Binary).Right.(*Literal)
		lower.Parameter, upper.Parameter = true, true
		for _, tc := range []struct {
			lo, hi          any
			inside, outside int
		}{
			{1, 3, 3, 0}, {int64(2), int64(2), 1, 2}, {float64(1.5), float64(2.5), 1, 2},
			{3, 1, 0, 3}, {nil, 2, 0, 1}, {2, nil, 0, 1}, {nil, nil, 0, 0},
		} {
			lower.Val, upper.Val = tc.lo, tc.hi
			result, err := Execute(context.Background(), db, "default", stmt)
			if err != nil {
				t.Fatal(err)
			}
			want := tc.inside
			if binary.Op == "OR" {
				want = tc.outside
			}
			if len(result.Rows) != 1 || result.Rows[0]["n"] != want {
				t.Fatalf("%s [%v,%v]: %v want %d", predicate, tc.lo, tc.hi, result.Rows, want)
			}
		}
	}
}

func TestRawRangeFilterErrorOrdering(t *testing.T) {
	columns := map[string]int{"v": 0}
	for _, tc := range []struct {
		predicate string
		value     any
		want      bool
		wantErr   bool
	}{
		{"v >= 3 AND v <= 1 / 0", 1, false, false},
		{"v >= 3 AND v <= 1 / 0", 3, false, true},
		{"v < 3 OR v > 1 / 0", 1, true, false},
		{"v < 3 OR v > 1 / 0", 3, false, true},
		{"v BETWEEN 1 AND 3 OR 1 / 0 = 0", 2, true, false},
		{"v BETWEEN 1 AND 3 OR 1 / 0 = 0", 4, false, true},
		{"v NOT BETWEEN 1 AND 3 AND 1 / 0 = 0", 2, false, false},
		{"v NOT BETWEEN 1 AND 3 AND 1 / 0 = 0", 4, false, true},
	} {
		expr := mustParse("SELECT v FROM t WHERE " + tc.predicate).(*Select).Where
		filter := buildRawFilter(columns, expr)
		if filter == nil {
			t.Fatalf("missing filter: %s", tc.predicate)
		}
		got, err := filter([]any{tc.value})
		if got != tc.want || (err != nil) != tc.wantErr {
			t.Errorf("%s v=%v: got %v/%v; want %v, error=%v", tc.predicate, tc.value, got, err, tc.want, tc.wantErr)
		}
	}
}
