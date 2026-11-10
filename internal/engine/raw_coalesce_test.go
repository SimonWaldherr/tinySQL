package engine

import (
	"fmt"
	"math"
	"math/big"
	"reflect"
	"sync"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestCompiledRawCoalesceMatchesEvaluator(t *testing.T) {
	columns := map[string]int{"a": 0, "b": 1}
	plan := &simpleSelectPlan{colIndex: columns}
	values := []any{nil, 0, int64(0), int64(math.MaxInt64), int64(math.MinInt64),
		0.0, math.Copysign(0, -1), math.NaN(), math.Inf(1), false, true, "", "text",
		[]byte(nil), []byte{0, 255}, map[string]any(nil), map[string]any{"v": 1}, big.NewRat(1, 3), (*big.Rat)(nil)}
	for _, name := range []string{"COALESCE", "IFNULL", "NVL"} {
		for _, arguments := range []string{"", "a", "a, b", "NULL, a, b, 'fallback'", "a + 1, b * 2", "a, 1 / 0", "CAST(a AS TEXT), b", "CASE WHEN a IS NULL THEN b ELSE a END, 9"} {
			expr := mustParse("SELECT " + name + "(" + arguments + ")").(*Select).Projs[0].Expr.(*FuncCall)
			compiled := compileRawCoalesce(columns, expr)
			if compiled == nil {
				t.Fatal("NULL function was not compiled", name, arguments)
			}
			for _, a := range values {
				for _, b := range values {
					raw := []any{a, b}
					// A typed nil decimal is valid as a COALESCE value, but the
					// existing arithmetic and CAST handlers cannot consume it.
					if arguments == "a + 1, b * 2" || arguments == "CAST(a AS TEXT), b" {
						if v, ok := a.(*big.Rat); ok && v == nil {
							continue
						}
						if v, ok := b.(*big.Rat); ok && v == nil {
							continue
						}
					}
					want, wantErr := evalRawExpr(plan, raw, expr)
					got, err := compiled(raw)
					if !conversionValuesEqual(got, want) || fmt.Sprint(err) != fmt.Sprint(wantErr) {
						t.Fatalf("%s(%s) (%#v): %T(%v) / %v, want %T(%v) / %v", name, arguments, raw, got, got, err, want, want, wantErr)
					}
				}
			}
		}
	}
}

func TestCompiledRawCoalescePreservesShortCircuitAndParameters(t *testing.T) {
	columns := map[string]int{"a": 0, "b": 1}
	plan := &simpleSelectPlan{colIndex: columns}
	for _, sql := range []string{"COALESCE(a,b)", "IFNULL(a,b)", "NVL(NULL,a,b)", "COALESCE(a,1/0)"} {
		expr := mustParse("SELECT " + sql).(*Select).Projs[0].Expr.(*FuncCall)
		compiled := compileRawCoalesce(columns, expr)
		for _, raw := range [][]any{nil, {nil}, {7}, {false}, {0}, {""}, {[]byte(nil)}, {nil, 9}} {
			want, wantErr := evalRawExpr(plan, raw, expr)
			got, err := compiled(raw)
			if !conversionValuesEqual(got, want) || fmt.Sprint(err) != fmt.Sprint(wantErr) {
				t.Fatalf("%s (%#v): %v / %v, want %v / %v", sql, raw, got, err, want, wantErr)
			}
		}
	}
	parameter := &Literal{Parameter: true}
	call := &FuncCall{Name: "COALESCE", Args: []Expr{&VarRef{Name: "a"}, parameter}}
	compiled := compileRawCoalesce(columns, call)
	for _, value := range []any{7, nil, "text", []byte{0, 255}, false} {
		parameter.Val = value
		got, err := compiled([]any{nil})
		if err != nil || !reflect.DeepEqual(got, value) {
			t.Fatalf("parameter %v: %v / %v", value, got, err)
		}
		got, err = compiled([]any{0})
		if err != nil || got != 0 {
			t.Fatalf("non-NULL value was replaced: %v / %v", got, err)
		}
	}
}

func TestCompiledRawCoalesceLeavesUnsupportedOperandsToEvaluator(t *testing.T) {
	columns := map[string]int{"a": 0}
	for _, sql := range []string{
		"COALESCE(a,missing)", "COALESCE(a,unknown_function())", "COALESCE(a,NOW())",
		"COALESCE(a,CONVERT(DATE,'2024-01-01',a))", "ISNULL(a)",
	} {
		call := mustParse("SELECT " + sql).(*Select).Projs[0].Expr.(*FuncCall)
		if compileRawCoalesce(columns, call) != nil {
			t.Fatal("unsupported NULL function compiled", sql)
		}
	}
	if compileRawCoalesce(columns, nil) != nil {
		t.Fatal("nil function compiled")
	}
}

func TestCompiledRawCoalesceQueryPaths(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	execSQL(t, db, "CREATE TABLE compiled_nulls (id INT, a INT, b INT)")
	execSQL(t, db, "INSERT INTO compiled_nulls VALUES (1,5,2),(2,NULL,7),(3,0,NULL),(4,NULL,NULL)")
	for _, name := range []string{"COALESCE", "IFNULL", "NVL"} {
		query := "SELECT " + name + "(a,b,9) AS value FROM compiled_nulls"
		got := execSQL(t, db, query)
		if !reflect.DeepEqual(got.Rows, []Row{{"value": 5}, {"value": 7}, {"value": 0}, {"value": 9}}) {
			t.Fatal(query, got)
		}
		columnar, err := ExecuteColumnar(t.Context(), db, "default", mustParse(query))
		if err != nil || !reflect.DeepEqual(columnar.Values[0], []any{5, 7, 0, 9}) {
			t.Fatalf("columnar %s: %v / %v", query, columnar, err)
		}
		stream, err := ExecuteStream(t.Context(), db, "default", mustParse(query))
		if err != nil {
			t.Fatal(err)
		}
		var values []any
		for stream.Next() {
			values = append(values, stream.Row()["value"])
			stream.ReleaseRow(stream.Row())
		}
		err = stream.Err()
		stream.Close()
		if err != nil || !reflect.DeepEqual(values, []any{5, 7, 0, 9}) {
			t.Fatalf("stream %s: %v / %v", query, values, err)
		}
	}
	statement := mustParse("SELECT SUM(COALESCE(a,b,0)) AS total, COUNT(COALESCE(a,b)) AS n FROM compiled_nulls")
	want := []Row{{"total": 12, "n": 3}}
	var workers sync.WaitGroup
	for range 8 {
		workers.Go(func() {
			for range 20 {
				got, err := Execute(t.Context(), db, "default", statement)
				if err != nil || !reflect.DeepEqual(got.Rows, want) {
					t.Errorf("shared NULL statement: %v / %v, want %v", got, err, want)
					return
				}
			}
		})
	}
	workers.Wait()
}
