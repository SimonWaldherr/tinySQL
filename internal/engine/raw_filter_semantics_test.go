package engine

import (
	"fmt"
	"math"
	"math/big"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestRawNotColumnFilterMatchesExpression(t *testing.T) {
	columns := map[string]int{"v": 0, "t.v": 0}
	for _, predicate := range []string{"NOT v", "NOT V", "NOT t.v", "NOT missing", "NOT v AND v IS NOT NULL", "NOT v OR v IS NULL"} {
		expr := mustParse("SELECT v FROM t WHERE " + predicate).(*Select).Where
		filter := buildRawFilter(columns, expr)
		fallback := buildRawExprFilter(columns, expr)
		for _, value := range []any{nil, false, true, 0, 1, -1, int64(0), int64(1), 0.0, 1.5, math.NaN(), "", "false", []byte{}, big.NewRat(0, 1)} {
			row := []any{value}
			got, err := filter(row)
			want, wantErr := fallback(row)
			if got != want || fmt.Sprint(err) != fmt.Sprint(wantErr) {
				t.Errorf("%s, value=%v (%T): got %v/%v, want %v/%v", predicate, value, value, got, err, want, wantErr)
			}
		}
	}
	for _, index := range []int{-1, 0, 1} {
		columns := map[string]int{"v": index}
		expr := &Unary{Op: "NOT", Expr: &VarRef{Name: "v"}}
		_, err := buildRawFilter(columns, expr)(nil)
		_, wantErr := buildRawExprFilter(columns, expr)(nil)
		if err == nil || fmt.Sprint(err) != fmt.Sprint(wantErr) {
			t.Fatalf("short row, column=%d: got %v, want %v", index, err, wantErr)
		}
	}
}

func TestNotColumnFilterSelectAndMutations(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	execSQL(t, db, "CREATE TABLE flags (id INT, active BOOLEAN)")
	execSQL(t, db, "INSERT INTO flags VALUES (1, TRUE), (2, FALSE), (3, NULL), (4, FALSE)")
	rows := execSQL(t, db, "SELECT id FROM flags WHERE NOT active ORDER BY id").Rows
	if len(rows) != 2 || rows[0]["id"] != 2 || rows[1]["id"] != 4 {
		t.Fatalf("NOT active rows: %v", rows)
	}
	count := execSQL(t, db, "SELECT COUNT(*) AS n FROM flags WHERE NOT active").Rows[0]["n"]
	if count != 2 {
		t.Fatalf("NOT active count: %v", count)
	}
	execSQL(t, db, "UPDATE flags SET id = id + 10 WHERE NOT active")
	rows = execSQL(t, db, "SELECT id FROM flags WHERE NOT active ORDER BY id").Rows
	if len(rows) != 2 || rows[0]["id"] != 12 || rows[1]["id"] != 14 {
		t.Fatalf("updated NOT active rows: %v", rows)
	}
	execSQL(t, db, "DELETE FROM flags WHERE NOT active")
	rows = execSQL(t, db, "SELECT id FROM flags ORDER BY id").Rows
	if len(rows) != 2 || rows[0]["id"] != 1 || rows[1]["id"] != 3 {
		t.Fatalf("remaining active and NULL rows: %v", rows)
	}
}

func TestRawBooleanLiteralFilterMatchesBoundFilter(t *testing.T) {
	columns := map[string]int{"v": 0}
	for _, op := range []string{"=", "!=", "<>"} {
		for _, literal := range []bool{false, true} {
			expr := &Binary{Op: op, Left: &VarRef{Name: "v"}, Right: &Literal{Val: literal}}
			filter := buildRawFilter(columns, expr)
			fallback := buildBoundLiteralFilter(0, op, &Literal{Val: literal, Parameter: true})
			for _, value := range []any{nil, false, true, 0, 1, int64(1), float64(1), "true", "false", []byte("true")} {
				row := []any{value}
				got, err := filter(row)
				want, wantErr := fallback(row)
				if got != want || fmt.Sprint(err) != fmt.Sprint(wantErr) {
					t.Errorf("%v (%T) %s %v: got %v/%v, want %v/%v", value, value, op, literal, got, err, want, wantErr)
				}
			}
		}
	}
}

// The parser desugars column BETWEEN into Binary nodes, but callers can also
// construct a BetweenExpr directly. Both forms must retain SQL value semantics.
func TestRawBetweenNodeFilterMatchesExpression(t *testing.T) {
	columns := map[string]int{"v": 0}
	for _, bounds := range [][2]any{{1, 3}, {int64(1), int64(3)}, {1.0, 3.0}, {"a", "z"}} {
		for _, negate := range []bool{false, true} {
			expr := &BetweenExpr{Expr: &VarRef{Name: "v"}, Lo: &Literal{Val: bounds[0]}, Hi: &Literal{Val: bounds[1]}, Negate: negate}
			filter := buildRawFilter(columns, expr)
			for _, value := range []any{nil, 0, 1, 2, 3, 4, int64(2), 2.0, math.NaN(), "b", true, big.NewRat(2, 1)} {
				row := []any{value}
				got, err := filter(row)
				valueResult, wantErr := evalExpr(ExecEnv{}, expr, Row{"v": value})
				want := toTri(valueResult) == tvTrue
				if got != want || fmt.Sprint(err) != fmt.Sprint(wantErr) {
					t.Errorf("value=%v (%T), bounds=%v negate=%v: got %v/%v, want %v/%v", value, value, bounds, negate, got, err, want, wantErr)
				}
			}
		}
	}
}
