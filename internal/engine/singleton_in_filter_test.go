package engine

import (
	"fmt"
	"math"
	"math/big"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestSingletonInFilterMatchesRawEvaluation(t *testing.T) {
	values := []any{
		nil, false, true, int(-1), int(0), int(1), int64(1),
		int64(math.MinInt64), int64(math.MaxInt64),
		int64(9007199254740992), int64(9007199254740993),
		-1.0, 0.0, 1.0, 1.5, float64(9007199254740992),
		math.NaN(), math.Inf(-1), math.Inf(1), "", "1", "true", "x",
		[]byte{}, []byte("x"), big.NewRat(1, 2),
	}
	plan := &simpleSelectPlan{colIndex: map[string]int{"v": 0}}
	for _, literal := range values {
		for _, negate := range []bool{false, true} {
			filter := buildInFilter(0, []any{literal}, negate)
			expr := &InExpr{Expr: &VarRef{Name: "v"}, Values: []Expr{&Literal{Val: literal}}, Negate: negate}
			for _, value := range values {
				row := []any{value}
				result, wantErr := evalRawIn(plan, row, expr)
				want := toTri(result) == tvTrue
				got, err := filter(row)
				if got != want || fmt.Sprint(err) != fmt.Sprint(wantErr) {
					t.Fatalf("%v (%T) IN (%v (%T)), negate=%v: got %v/%v, want %v/%v", value, value, literal, literal, negate, got, err, want, wantErr)
				}
			}
		}
	}
}

func TestSingletonInCountScan(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	execSQL(t, db, "CREATE TABLE singleton_values (v INT)")
	table, err := db.Get("default", "singleton_values")
	if err != nil {
		t.Fatal(err)
	}
	// Direct storage callers can mix representations in one column.
	for _, value := range []any{nil, int(1), int64(2), 1.0, "1", true, []byte("1")} {
		table.Rows = append(table.Rows, []any{value})
	}
	table.Version++
	for _, tc := range []struct {
		predicate string
		count     int
	}{
		{"v IN (1)", 2}, {"v NOT IN (1)", 4},
		{"v IN (TRUE)", 1}, {"v NOT IN (TRUE)", 5},
		{"v IN (NULL)", 0}, {"v NOT IN (NULL)", 0},
		{"v IN (1, 2)", 3},
	} {
		result := execSQL(t, db, "SELECT COUNT(*) AS n FROM singleton_values WHERE "+tc.predicate)
		if len(result.Rows) != 1 || result.Rows[0]["n"] != tc.count {
			t.Fatalf("%s: got %v, want count %d", tc.predicate, result.Rows, tc.count)
		}
	}
}

var singletonInFilterSink func([]any) (bool, error)

func BenchmarkSingletonInFilterCompile(b *testing.B) {
	for _, tc := range []struct {
		name    string
		literal any
	}{
		{"Integer", int(1)}, {"Float", 1.0}, {"Text", "x"}, {"Boolean", true}, {"Null", nil},
	} {
		b.Run(tc.name, func(b *testing.B) {
			values := []any{tc.literal}
			b.ReportAllocs()
			for b.Loop() {
				singletonInFilterSink = buildInFilter(0, values, false)
			}
		})
	}
}

func BenchmarkSingletonInFilterScan(b *testing.B) {
	for _, tc := range []struct {
		name            string
		literal, missed any
	}{
		{"Integer", int(1), int(2)}, {"Float", 1.0, 2.0}, {"Text", "x", "y"}, {"Boolean", true, false},
	} {
		rows := make([][]any, 100_000)
		values := []any{nil, tc.literal, tc.missed}
		for i := range rows {
			rows[i] = []any{values[i%len(values)]}
		}
		for _, negate := range []bool{false, true} {
			b.Run(fmt.Sprintf("%s/Negate=%v", tc.name, negate), func(b *testing.B) {
				filter := buildInFilter(0, []any{tc.literal}, negate)
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
					if matches != len(rows)/3 {
						b.Fatalf("got %d matches, want %d", matches, len(rows)/3)
					}
				}
			})
		}
	}
}

func BenchmarkSingletonInCountScan(b *testing.B) {
	db := setupPerfTable(b, 100_000)
	b.Cleanup(func() { _ = db.Close() })
	for _, predicate := range []string{"id IN (1)", "id NOT IN (1)", "id IN (NULL)", "id NOT IN (NULL)", "id IN (1, 2)"} {
		b.Run(predicate, func(b *testing.B) {
			runBench(b, db, "SELECT COUNT(*) AS n FROM t WHERE "+predicate)
		})
	}
}
