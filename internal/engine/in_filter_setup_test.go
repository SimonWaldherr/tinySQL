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

func TestRawEqualityOrMatchesSeparateComparisons(t *testing.T) {
	columns := map[string]int{"v": 0, "t.v": 0}
	values := []any{
		nil, 1, int64(9007199254740993), float64(9007199254740992), math.Copysign(0, -1),
		math.NaN(), math.Inf(1), "x", true, []byte("x"), big.NewRat(1, 2),
	}
	probes := append(values, int64(9007199254740992), 0.0, "other", false, []byte("other"))
	for _, literals := range [][]any{{nil, 1}, {"x", "y"}, {int64(9007199254740993), 7}, values} {
		var tree *Binary
		separate := make([]func([]any) (bool, error), 0, len(literals))
		for i, value := range literals {
			leaf := &Binary{Op: "=", Left: &Literal{Val: value}, Right: &VarRef{Name: "t.v"}}
			if i%2 != 0 {
				leaf.Left, leaf.Right = &VarRef{Name: "V"}, leaf.Left
			}
			separate = append(separate, buildRawComparisonFilter(columns, leaf))
			if tree == nil {
				tree = leaf
			} else {
				tree = &Binary{Op: "OR", Left: tree, Right: leaf}
			}
		}
		filter := buildRawEqualityOrFilter(columns, tree.Left, tree.Right)
		if filter == nil {
			t.Fatalf("failed to compile equalities for %v", literals)
		}
		for _, value := range probes {
			row := []any{value}
			want := false
			for _, equal := range separate {
				match, err := equal(row)
				if err != nil {
					t.Fatal(err)
				}
				want = want || match
			}
			if got, err := filter(row); err != nil || got != want {
				t.Fatalf("%T(%v) with %v: got %t / %v, want %t", value, value, literals, got, err, want)
			}
		}
	}
}

func TestRawEqualityOrLeavesParametersAndOtherPredicatesDynamic(t *testing.T) {
	columns := map[string]int{"v": 0, "w": 1}
	for _, predicate := range []string{"v = 1 OR w = 2", "v = 1 OR unknown = 2", "v = 1 OR v = 2/0", "v = 1 OR v > 2"} {
		call := mustParse("SELECT v FROM t WHERE " + predicate).(*Select).Where.(*Binary)
		if buildRawEqualityOrFilter(columns, call.Left, call.Right) != nil {
			t.Fatalf("unexpected membership filter for %s", predicate)
		}
	}
	call := mustParse("SELECT v FROM t WHERE v = 1 OR v = 2").(*Select).Where.(*Binary)
	parameter := call.Left.(*Binary).Right.(*Literal)
	parameter.Parameter = true
	if buildRawEqualityOrFilter(columns, call.Left, call.Right) != nil {
		t.Fatal("compiled a bound value into a fixed set")
	}
	filter := buildRawFilter(columns, call)
	for _, value := range []any{1, int64(1), 1.0, nil, 2, "1"} {
		parameter.Val = value
		if got, err := filter([]any{1}); err != nil || got != rawEqual(1, value) {
			t.Fatalf("bound %T(%v): got %t / %v", value, value, got, err)
		}
	}
	// The raw WHERE membership test must not be inverted under NOT.
	for _, predicate := range []string{"NOT (v = 1 OR v = NULL)", "NOT (v = NULL OR v = NULL)"} {
		expr := mustParse("SELECT v FROM t WHERE " + predicate).(*Select).Where
		if got, err := buildRawFilter(columns, expr)([]any{3}); err != nil || got {
			t.Fatalf("%s: got %t / %v, want UNKNOWN excluded", predicate, got, err)
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
