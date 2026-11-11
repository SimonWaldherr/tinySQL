package engine

import (
	"fmt"
	"math"
	"math/big"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestInFilterDomainsMatchRawEvaluation(t *testing.T) {
	values := []any{
		nil, false, true, -1, 0, 1, int64(1), int64(math.MinInt64), int64(math.MaxInt64),
		int64(9007199254740992), int64(9007199254740993),
		-1.0, 0.0, math.Copysign(0, -1), 1.0, 1.5, float64(9007199254740992),
		math.NaN(), math.Inf(-1), math.Inf(1), "", "1", "x", "y",
		[]byte(nil), []byte("x"), []byte{0, 255}, big.NewRat(1, 2), map[string]any(nil),
	}
	plan := &simpleSelectPlan{colIndex: map[string]int{"v": 0}}
	check := func(literals []any) {
		t.Helper()
		for _, negate := range []bool{false, true} {
			expr := &InExpr{Expr: &VarRef{Name: "v"}, Negate: negate}
			for _, value := range literals {
				expr.Values = append(expr.Values, &Literal{Val: value})
			}
			filter := buildInFilter(0, literals, negate)
			for _, value := range values {
				raw := []any{value}
				want, wantErr := evalRawIn(plan, raw, expr)
				got, err := filter(raw)
				if got != (toTri(want) == tvTrue) || fmt.Sprint(err) != fmt.Sprint(wantErr) {
					t.Fatalf("%T(%v) IN %#v, negate=%v: got %v/%v, want %v/%v", value, value, literals, negate, got, err, want, wantErr)
				}
			}
		}
	}
	check(nil)
	check([]any{nil, nil, nil})
	check([]any{1, int64(9007199254740993), 1.5, "x", true, []byte("x"), nil})
	for _, first := range values {
		for _, second := range values {
			check([]any{first, second})
		}
	}
}

func TestInFilterDomainsCountScans(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	table := storage.NewTable("in_domains", []storage.Column{{Name: "v", Type: storage.InterfaceType}}, false)
	for _, value := range []any{nil, 1, int64(9007199254740993), float64(9007199254740992), "x", true, []byte("x")} {
		table.Rows = append(table.Rows, []any{value})
	}
	if err := db.Put("default", table); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		predicate string
		count     int
	}{
		{"v IN (1,9007199254740993,'x',TRUE)", 5},
		{"v IN ('x','y')", 1}, {"v NOT IN ('x','y')", 5},
		{"v IN (TRUE,FALSE)", 1},
		{"v IN (NULL,NULL)", 0}, {"v NOT IN (NULL,NULL)", 0},
		{"v NOT IN (1,'x',NULL)", 0},
		{"v IN (9007199254740992,9007199254740993)", 2},
	} {
		got := execSQL(t, db, "SELECT COUNT(*) AS n FROM in_domains WHERE "+tc.predicate)
		if len(got.Rows) != 1 || got.Rows[0]["n"] != tc.count {
			t.Fatalf("%s: got %v, want count %d", tc.predicate, got.Rows, tc.count)
		}
	}
}

var inFilterSetupSink func([]any) (bool, error)

func BenchmarkInFilterSetup(b *testing.B) {
	for _, tc := range []struct {
		name   string
		values []any
	}{
		{"Text", []any{"a", "b", "c", "d"}},
		{"Integer", []any{1, 2, int64(9007199254740993)}},
		{"Float", []any{1.0, 2.0, 3.5}},
		{"Other", []any{true, false, []byte("x"), []byte("y")}},
		{"NullOnly", []any{nil, nil}},
		{"Empty", nil},
		{"MixedControl", []any{1, 2.5, "x", true, nil}},
		{"SingletonControl", []any{1}},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				inFilterSetupSink = buildInFilter(0, tc.values, false)
			}
		})
	}
}
