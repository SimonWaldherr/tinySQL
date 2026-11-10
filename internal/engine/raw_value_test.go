package engine

import (
	"fmt"
	"math"
	"math/big"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestCompiledRawValuesMatchEvaluator(t *testing.T) {
	columns := map[string]int{"a": 0, "b": 1}
	plan := &simpleSelectPlan{colIndex: columns}
	values := []any{nil, -1, 0, 7, int64(math.MaxInt64), int64(math.MinInt64),
		int64(9007199254740993), 0.5, math.NaN(), math.Inf(1), "text", "12", true,
		[]byte{0, 255}, big.NewRat(3, 7)}
	for _, expression := range []string{
		"a + b", "a - b", "a * b", "a / b", "a % b", "(a + b) * (a - b)",
		"a + -2", "CAST(a AS FLOAT) + b", "TRY_CONVERT(INT,a) + b",
		"a + b = b", "a + b != b", "a + b <> b", "a + b < b", "a + b <= b",
		"a + b > b", "a + b >= b",
	} {
		expr := mustParse("SELECT " + expression).(*Select).Projs[0].Expr
		compiled := compileRawValue(columns, expr)
		if compiled == nil {
			t.Fatal("expression was not compiled", expression)
		}
		for _, a := range values {
			for _, b := range values {
				raw := []any{a, b}
				want, wantErr := evalRawExpr(plan, raw, expr)
				got, gotErr := compiled(raw)
				if !conversionValuesEqual(got, want) || fmt.Sprint(gotErr) != fmt.Sprint(wantErr) {
					t.Fatalf("%s (%#v): %v / %v, want %v / %v", expression, raw, got, gotErr, want, wantErr)
				}
			}
		}
		for _, raw := range [][]any{nil, {nil}, {"bad"}} {
			_, wantErr := evalRawExpr(plan, raw, expr)
			_, gotErr := compiled(raw)
			if fmt.Sprint(gotErr) != fmt.Sprint(wantErr) {
				t.Fatalf("%s: short-row diagnostics: %v, want %v", expression, gotErr, wantErr)
			}
		}
	}
	for _, expression := range []string{"a AND b", "a OR b", "a + missing", "a + ABS(b)", "NOW() + 1"} {
		expr := mustParse("SELECT " + expression).(*Select).Projs[0].Expr
		if compileRawValue(columns, expr) != nil {
			t.Fatal("unsupported expression was compiled", expression)
		}
	}
}

func TestCompiledRawValuesReadCurrentParameters(t *testing.T) {
	parameter := &Literal{Parameter: true}
	expr := &Binary{Op: "+", Left: &VarRef{Name: "a"}, Right: parameter}
	compiled := compileRawValue(map[string]int{"a": 0}, expr)
	for _, value := range []any{1, int64(math.MaxInt64), 0.5, nil, "suffix"} {
		parameter.Val = value
		got, err := compiled([]any{7})
		want, wantErr := evalArithmeticBinary("+", 7, value)
		if !conversionValuesEqual(got, want) || fmt.Sprint(err) != fmt.Sprint(wantErr) {
			t.Fatalf("parameter %v: %v / %v, want %v / %v", value, got, err, want, wantErr)
		}
	}
}

func TestCompiledRawValueQueryPaths(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	execSQL(t, db, "CREATE TABLE value_source (id INT PRIMARY KEY, a INT, b INT)")
	execSQL(t, db, "INSERT INTO value_source VALUES (1,10,2),(2,NULL,3),(3,10,2),(4,0,0)")
	for _, projection := range []string{"a + b * 2 AS computed", "a + b > 10 AS computed", "CAST(a AS FLOAT) / (b + 1) AS computed"} {
		for _, prefix := range []string{"SELECT ", "SELECT DISTINCT "} {
			query := prefix + projection + " FROM value_source WHERE a + b > 0"
			got := execSQL(t, db, query)
			want := execSQL(t, db, prefix+projection+" FROM (SELECT * FROM value_source) q WHERE a + b > 0")
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("%s: %v, want %v", query, got, want)
			}
			stream, err := ExecuteStream(t.Context(), db, "default", mustParse(query))
			if err != nil {
				t.Fatal(err)
			}
			var rows []Row
			for stream.Next() {
				row := Row{}
				for key, value := range stream.Row() {
					row[key] = value
				}
				rows = append(rows, row)
				stream.ReleaseRow(stream.Row())
			}
			err = stream.Err()
			stream.Close()
			if err != nil || !reflect.DeepEqual(rows, want.Rows) {
				t.Fatalf("stream %s: %v / %v, want %v", query, rows, err, want.Rows)
			}
			columns, err := ExecuteColumnar(t.Context(), db, "default", mustParse(query))
			if err != nil || columns.RowCount != len(want.Rows) {
				t.Fatalf("columnar %s: %v / %v", query, columns, err)
			}
			for i, row := range want.Rows {
				if !conversionValuesEqual(columns.Values[0][i], row["computed"]) {
					t.Fatalf("columnar value %s: %v, want %v", query, columns.Values[0][i], row["computed"])
				}
			}
		}
	}
	// Expressions are evaluated on skipped rows, but never on excluded rows.
	for _, query := range []string{
		"SELECT 1 / b FROM value_source WHERE id = 4 LIMIT 1 OFFSET 1",
		"SELECT id FROM value_source WHERE a / b > 0 AND id = 4",
	} {
		if _, err := Execute(t.Context(), db, "default", mustParse(query)); err == nil {
			t.Fatal("expression error was lost", query)
		}
	}
	for _, query := range []string{
		"SELECT a / b FROM value_source WHERE id < 4",
		"SELECT id FROM value_source WHERE a / b > 0 AND id < 4",
		"SELECT a / b FROM value_source WHERE id = -1",
		"SELECT a / b FROM value_source LIMIT 0",
	} {
		execSQL(t, db, query)
	}
	// A failure after earlier mutations must restore rows and their indexes.
	before := execSQL(t, db, "SELECT * FROM value_source")
	execSQL(t, db, "CREATE INDEX idx_value_a ON value_source(a)")
	if _, err := Execute(t.Context(), db, "default", mustParse("UPDATE value_source SET a = a / b")); err == nil {
		t.Fatal("expected division by zero")
	}
	if got := execSQL(t, db, "SELECT * FROM value_source"); !reflect.DeepEqual(got, before) {
		t.Fatalf("UPDATE rollback changed rows: %v, want %v", got, before)
	}
	if got := execSQL(t, db, "SELECT id FROM value_source WHERE a = 10"); len(got.Rows) != 2 {
		t.Fatal("UPDATE rollback changed the index", got)
	}
	execSQL(t, db, "UPDATE value_source SET a = a + b * 2 WHERE id < 4")
	got := execSQL(t, db, "SELECT a FROM value_source ORDER BY id")
	if !reflect.DeepEqual(got.Rows, []Row{{"a": 14}, {"a": nil}, {"a": 14}, {"a": 0}}) {
		t.Fatal("unexpected updated values", got)
	}
	execSQL(t, db, "DELETE FROM value_source WHERE a + b > 15")
	if got := execSQL(t, db, "SELECT id FROM value_source ORDER BY id"); !reflect.DeepEqual(got.Rows, []Row{{"id": 2}, {"id": 4}}) {
		t.Fatal("unexpected deleted rows", got)
	}
}

func TestCompiledRawValueSharedStatement(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	execSQL(t, db, "CREATE TABLE shared_values (a INT, b INT)")
	execSQL(t, db, "INSERT INTO shared_values VALUES (1,2),(3,4),(NULL,5)")
	statement := mustParse("SELECT a + b * 2 AS computed FROM shared_values WHERE a + b > 0")
	want, err := Execute(t.Context(), db, "default", statement)
	if err != nil {
		t.Fatal(err)
	}
	var group sync.WaitGroup
	for range 8 {
		group.Go(func() {
			for range 20 {
				got, err := Execute(t.Context(), db, "default", statement)
				if err != nil || !reflect.DeepEqual(got, want) {
					t.Errorf("shared statement: %v / %v, want %v", got, err, want)
					return
				}
			}
		})
	}
	group.Wait()
}

func TestCompiledUpdateAcrossBindingThreshold(t *testing.T) {
	for _, size := range []int{63, 64, 65, 130} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			db := storage.NewDB()
			t.Cleanup(func() { _ = db.Close() })
			execSQL(t, db, "CREATE TABLE update_values (id INT PRIMARY KEY, a INT, b INT)")
			values := make([]string, size)
			for i := range values {
				denominator := 2
				if i == size-1 {
					denominator = 0
				}
				values[i] = fmt.Sprintf("(%d,10,%d)", i, denominator)
			}
			execSQL(t, db, "INSERT INTO update_values VALUES "+strings.Join(values, ","))
			execSQL(t, db, "CREATE INDEX idx_update_a ON update_values(a)")
			before := execSQL(t, db, "SELECT * FROM update_values")
			if _, err := Execute(t.Context(), db, "default", mustParse("UPDATE update_values SET a = a / b")); err == nil {
				t.Fatal("expected division by zero after earlier rows changed")
			}
			if got := execSQL(t, db, "SELECT * FROM update_values"); !reflect.DeepEqual(got, before) {
				t.Fatal("failed UPDATE changed rows")
			}
			if got := execSQL(t, db, "SELECT COUNT(*) AS n FROM update_values WHERE a = 10"); got.Rows[0]["n"] != size {
				t.Fatal("failed UPDATE changed index entries", got)
			}
			execSQL(t, db, "UPDATE update_values SET a = a + b * 2")
			if got := execSQL(t, db, "SELECT COUNT(*) AS n FROM update_values WHERE a = 14"); got.Rows[0]["n"] != size-1 {
				t.Fatal("bulk UPDATE changed values", got)
			}
			execSQL(t, db, fmt.Sprintf("UPDATE update_values SET a = a + 1 WHERE id = %d", size-1))
			if got := execSQL(t, db, fmt.Sprintf("SELECT a FROM update_values WHERE id = %d", size-1)); got.Rows[0]["a"] != 11 {
				t.Fatal("point UPDATE changed values", got)
			}
		})
	}
}
