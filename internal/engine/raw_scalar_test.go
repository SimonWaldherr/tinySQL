package engine

import (
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"sync"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestCompiledRawScalarMatchesEvaluator(t *testing.T) {
	columns := map[string]int{"a": 0, "b": 1, "c": 2}
	plan := &simpleSelectPlan{colIndex: columns}
	values := []any{nil, false, true, 0, 1, int64(math.MaxInt64), int64(9007199254740993),
		0.0, 1.5, math.NaN(), math.Inf(1), "", "  α🙂 x  ", "\xffx\x80", "0", "FALSE",
		[]byte(nil), []byte{0, 255}, json.RawMessage(`{"v":1}`), big.NewRat(1, 2)}
	for _, sql := range []string{
		"ISNULL(a)", "ISNULL(NULLIF(a,b))", "NULLIF(a,b)", "COALESCE(NULLIF(a,b),c)",
		"IF(a,b,1/0)", "IIF(a,1/0,b)", "IF(ISNULL(a),b,a)",
		"TRIM(a)", "LTRIM(a,b)", "RTRIM(a,b)", "TRIM(a,CAST(b AS TEXT))",
		"LENGTH(TRIM(a,b))", "LENGTH(a)", "LEN(a)", "CHAR_LENGTH(a)", "LOWER(a)", "UPPER(a)",
		"COALESCE(a,LENGTH(LOWER(b)),0)", "COALESCE(TRIM(a),b)",
		"(a+1) BETWEEN b AND c", "COALESCE(a,b) BETWEEN b AND c",
		"IF(ISNULL(a),b,a) NOT BETWEEN b AND c",
	} {
		expr := mustParse("SELECT " + sql).(*Select).Projs[0].Expr
		compiled := compileRawValue(columns, expr)
		if compiled == nil {
			t.Fatal("expression was not bound", sql)
		}
		check := func(raw []any) {
			t.Helper()
			want, wantErr := evalRawExpr(plan, raw, expr)
			got, err := compiled(raw)
			if !conversionValuesEqual(got, want) || fmt.Sprint(err) != fmt.Sprint(wantErr) {
				t.Fatalf("%s (%#v): %T(%v) / %v, want %T(%v) / %v", sql, raw, got, got, err, want, want, wantErr)
			}
		}
		for _, a := range values {
			for _, b := range values {
				for _, c := range []any{nil, 0, 10, "z", math.NaN()} {
					check([]any{a, b, c})
				}
			}
		}
		for _, raw := range [][]any{nil, {nil}, {0}, {nil, ""}, {nil, "\ufffd"}} {
			check(raw)
		}
	}
}

func TestCompiledRawScalarLazinessAndFallback(t *testing.T) {
	columns := map[string]int{"a": 0}
	for _, sql := range []string{
		"ISNULL()", "ISNULL(a,a)", "NULLIF(a)", "NULLIF(a,a,a)", "IF(a,a)",
		"TRIM()", "LTRIM(a,a,a)", "LENGTH()", "UPPER(a,a)",
		"TRIM(a,missing)", "IF(a,7,ABS(a))", "COALESCE(a,LOWER(NOW()))",
		"NULLIF(a,NOW())", "LENGTH(unknown_function(a))", "(a+1) BETWEEN 0 AND missing",
		"ISNULL(CONVERT(DATE,a,a))",
	} {
		expr := mustParse("SELECT " + sql).(*Select).Projs[0].Expr
		if compileRawValue(columns, expr) != nil {
			t.Fatal("unsupported child or declaration was bound", sql)
		}
	}
	for _, tc := range []struct {
		sql  string
		want any
	}{
		{"TRIM(NULL,1/0)", nil}, {"COALESCE(0,LENGTH(1/0))", 0},
		{"IF(FALSE,1/0,7)", 7}, {"IIF(TRUE,7,1/0)", 7}, {"ISNULL(NULL)", true},
	} {
		expr := mustParse("SELECT " + tc.sql).(*Select).Projs[0].Expr
		compiled := compileRawValue(columns, expr)
		got, err := compiled(nil)
		if err != nil || !reflect.DeepEqual(got, tc.want) {
			t.Fatalf("%s: %v / %v, want %v", tc.sql, got, err, tc.want)
		}
	}
	for _, sql := range []string{"NULLIF(NULL,1/0)", "COALESCE(NULL,1) BETWEEN NULL AND 1/0", "TRY_CONVERT(INT,1/0)"} {
		expr := mustParse("SELECT " + sql).(*Select).Projs[0].Expr
		if _, err := compileRawValue(columns, expr)(nil); err == nil {
			t.Fatal("operand error was hidden", sql)
		}
	}
	// Typed nils retain the engine's interface-NULL semantics.
	for _, value := range []any{[]byte(nil), map[string]any(nil), (*big.Rat)(nil)} {
		expr := &FuncCall{Name: "ISNULL", Args: []Expr{&Literal{Val: value}}}
		if got, err := compileRawValue(columns, expr)(nil); err != nil || got != false {
			t.Fatalf("typed nil %T became NULL: %v / %v", value, got, err)
		}
	}
}

func TestNullifRejectsInvalidDecimalComparisons(t *testing.T) {
	for _, value := range []any{math.NaN(), math.Inf(1), math.Inf(-1), (*big.Rat)(nil)} {
		for _, operands := range [][2]any{{big.NewRat(1, 2), value}, {value, big.NewRat(1, 2)}} {
			call := &FuncCall{Name: "NULLIF", Args: []Expr{&Literal{Val: operands[0]}, &Literal{Val: operands[1]}}}
			compiled := compileRawValue(nil, call)
			if _, err := compiled(nil); err == nil {
				t.Fatalf("NULLIF(%T,%T) must reject an invalid decimal comparison", operands[0], operands[1])
			}
			if _, err := evalNullif(ExecEnv{}, call.Args, nil); err == nil {
				t.Fatal("general evaluator accepted an invalid decimal comparison")
			}
		}
	}
}

func TestCompiledRawScalarParameters(t *testing.T) {
	columns := map[string]int{"a": 0}
	plan := &simpleSelectPlan{colIndex: columns}
	for _, sql := range []string{"TRIM(a,'x')", "NULLIF(a,'x')", "IF(a,'x',7)", "COALESCE(a,'x') BETWEEN 0 AND 10"} {
		expr := mustParse("SELECT " + sql).(*Select).Projs[0].Expr
		var parameter *Literal
		switch ex := expr.(type) {
		case *FuncCall:
			parameter = ex.Args[1].(*Literal)
		case *BetweenExpr:
			parameter = ex.Lo.(*Literal)
		}
		parameter.Parameter = true
		compiled := compileRawValue(columns, expr)
		for _, value := range []any{nil, "", "x", "α🙂", "\ufffd", 0, 4, false} {
			parameter.Val = value
			for _, a := range []any{nil, "xxα🙂xx", "\xffx\x80", 0, 5, true} {
				raw := []any{a}
				want, wantErr := evalRawExpr(plan, raw, expr)
				got, err := compiled(raw)
				if !conversionValuesEqual(got, want) || fmt.Sprint(err) != fmt.Sprint(wantErr) {
					t.Fatalf("%s, parameter %v: %v/%v, want %v/%v", sql, value, got, err, want, wantErr)
				}
			}
		}
	}
}

func TestCompiledRawScalarQueryPaths(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	execSQL(t, db, "CREATE TABLE scalar_values (id INT,v INT,body TEXT,cut TEXT)")
	execSQL(t, db, "INSERT INTO scalar_values VALUES (1,NULL,' xx Alpha xx ',' x'),(2,0,'..β🙂..','.'),(3,2,NULL,'x'),(4,3,'',''),(5,2,'foo','o')")
	query := "SELECT COALESCE(NULLIF(v,0),LENGTH(TRIM(body,cut)),9) AS value FROM scalar_values"
	want := []Row{{"value": 5}, {"value": 2}, {"value": 2}, {"value": 3}, {"value": 2}}
	got := execSQL(t, db, query)
	if !reflect.DeepEqual(got.Rows, want) {
		t.Fatal(got.Rows)
	}
	columnar, err := ExecuteColumnar(t.Context(), db, "default", mustParse(query))
	if err != nil || !reflect.DeepEqual(columnar.Values[0], []any{5, 2, 2, 3, 2}) {
		t.Fatalf("columnar: %v / %v", columnar, err)
	}
	stream, err := ExecuteStream(t.Context(), db, "default", mustParse(query))
	if err != nil {
		t.Fatal(err)
	}
	var streamed []Row
	for stream.Next() {
		streamed = append(streamed, Row{"value": stream.Row()["value"]})
		stream.ReleaseRow(stream.Row())
	}
	err = stream.Err()
	stream.Close()
	if err != nil || !reflect.DeepEqual(streamed, want) {
		t.Fatalf("streamed: %v / %v", streamed, err)
	}
	for _, tc := range []struct {
		sql  string
		want []Row
	}{
		{"SELECT id FROM scalar_values WHERE ISNULL(NULLIF(v,0))", []Row{{"id": 1}, {"id": 2}}},
		{"SELECT id FROM scalar_values WHERE LENGTH(TRIM(body,cut)) BETWEEN 2 AND 5", []Row{{"id": 1}, {"id": 2}}},
		{"SELECT COUNT(NULLIF(v,0)) AS n,COUNT(ISNULL(v)) AS flags,COUNT(v IS NULL) AS tests,COUNT(LENGTH(body) BETWEEN 1 AND 100) AS ranges FROM scalar_values", []Row{{"n": 3, "flags": 5, "tests": 5, "ranges": 4}}},
		{"SELECT COUNT(*) AS n FROM scalar_values WHERE NULLIF(v,1/0) IS NULL AND id=-1", []Row{{"n": 0}}},
		{"SELECT TRIM(NULL,missing) AS value FROM scalar_values LIMIT 1", []Row{{"value": nil}}},
	} {
		if got := execSQL(t, db, tc.sql); !reflect.DeepEqual(got.Rows, tc.want) {
			t.Fatalf("%s: %v, want %v", tc.sql, got.Rows, tc.want)
		}
	}
	statement := mustParse("SELECT SUM(COALESCE(NULLIF(v,0),LENGTH(TRIM(body,cut)),9)) AS total FROM scalar_values")
	var workers sync.WaitGroup
	for range 8 {
		workers.Go(func() {
			for range 20 {
				got, err := Execute(t.Context(), db, "default", statement)
				if err != nil || !reflect.DeepEqual(got.Rows, []Row{{"total": 14}}) {
					t.Errorf("shared expression: %v / %v", got, err)
					return
				}
			}
		})
	}
	workers.Wait()
}
