package engine

import (
	"fmt"
	"math"
	"reflect"
	"slices"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestSmallInFilterMatchesRawEvaluation(t *testing.T) {
	probes := []any{nil, 0, int64(1), int64(9007199254740993), int64(math.MaxInt64), 0.0, math.Copysign(0, -1), float64(9007199254740992), math.NaN(), math.Inf(1), "x", "1", true, false, []byte(nil), []byte("x")}
	lists := [][]any{
		{nil, nil, nil, nil}, {1, 1, 1, nil}, {int64(9007199254740993), float64(9007199254740992), nil, 0},
		{math.NaN(), math.Inf(1), math.Copysign(0, -1), 1.5}, {true, false, []byte("x"), nil}, {"x", "1", "y", "z"},
	}
	plan := &simpleSelectPlan{colIndex: map[string]int{"v": 0}}
	for _, list := range lists {
		for _, negate := range []bool{false, true} {
			filter := buildInFilter(0, list, negate)
			ex := &InExpr{Expr: &VarRef{Name: "v"}, Negate: negate}
			for _, value := range list {
				ex.Values = append(ex.Values, &Literal{Val: value})
			}
			for _, probe := range probes {
				row := []any{probe}
				want, wantErr := evalRawIn(plan, row, ex)
				got, err := filter(row)
				if got != (toTri(want) == tvTrue) || fmt.Sprint(err) != fmt.Sprint(wantErr) {
					t.Fatalf("%#v IN %#v negate=%v: %v/%v, want %v/%v", probe, list, negate, got, err, want, wantErr)
				}
			}
		}
	}
}

func TestSmallInFilterOwnsLiteralSlots(t *testing.T) {
	for _, values := range [][]any{{1, int64(2), nil}, {1.0, 2.0, nil}, {"x", "z", nil}, {1, "x", nil}} {
		original := slices.Clone(values)
		filters := []func([]any) (bool, error){buildInFilter(0, values, false), buildInFilter(0, values, true)}
		for i := range values {
			values[i] = "replacement"
		}
		for i, filter := range filters {
			expr := &InExpr{Expr: &VarRef{Name: "v"}, Negate: i == 1}
			for _, value := range original {
				expr.Values = append(expr.Values, &Literal{Val: value})
			}
			for _, probe := range []any{nil, 1, int64(2), 2.0, "x", "z", "replacement", 3} {
				row := []any{probe}
				want, wantErr := evalRawIn(&simpleSelectPlan{colIndex: map[string]int{"v": 0}}, row, expr)
				got, err := filter(row)
				if err != nil || wantErr != nil || got != (toTri(want) == tvTrue) {
					t.Fatalf("probe %v, list %#v, negate=%v: %v/%v, want %v/%v", probe, original, expr.Negate, got, err, want, wantErr)
				}
			}
		}
	}
}

func TestVectorOrderWithInMatchesBooleanFilter(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	execSQL(t, db, `CREATE TABLE vector_in (id INT PRIMARY KEY, category TEXT, embedding VECTOR)`)
	execSQL(t, db, `INSERT INTO vector_in VALUES (0,'blocked','[1,2]'),(1,'a','[2,2]'),(2,'b','[1,3]'),(3,'c','[3,2]'),(4,NULL,'[1,2]'),(5,'a','[4,2]')`)
	for _, tc := range []struct{ in, reference string }{
		{`category IN ('a','b')`, `category='a' OR category='b'`},
		{`category NOT IN ('a','b')`, `NOT (category='a' OR category='b')`},
		{`category IN ('a',NULL,'b','c')`, `category='a' OR category=NULL OR category='b' OR category='c'`},
		{`category NOT IN ('a',NULL,'b','c')`, `NOT (category='a' OR category=NULL OR category='b' OR category='c')`},
	} {
		query := `SELECT id,VEC_L2_DISTANCE(embedding,'[1,2]') AS distance FROM vector_in WHERE (%s) ORDER BY distance LIMIT 3`
		got := execSQL(t, db, fmt.Sprintf(query, tc.in))
		want := execSQL(t, db, fmt.Sprintf(query, tc.reference))
		if !reflect.DeepEqual(got.Cols, want.Cols) || !reflect.DeepEqual(got.Rows, want.Rows) {
			t.Fatalf("%s: got %#v, want %#v", tc.in, got.Rows, want.Rows)
		}
	}
}
