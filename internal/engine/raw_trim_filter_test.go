package engine

import (
	"fmt"
	"math"
	"math/big"
	"reflect"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestRawTrimComparisonMatchesInterpreter(t *testing.T) {
	columns := map[string]int{"a": 0, "b": 1, "cut": 2}
	plan := &simpleSelectPlan{colIndex: columns}
	values := []any{nil, "", " xxα🙂xx ", "xx1/2xx", "1/2", "\xffx\x80", 0, 1.5, math.NaN(), false,
		[]byte(nil), []byte{255, 0}, big.NewRat(1, 2), (*big.Rat)(nil)}
	for _, op := range []string{"=", "!=", "<>", "<", "<=", ">", ">="} {
		for _, shape := range []string{"TRIM(a,cut) %s b", "a %s RTRIM(b,cut)", "TRIM(a,cut) %s LTRIM(b,cut)"} {
			predicate := fmt.Sprintf(shape, op)
			expr := mustParse("SELECT 1 WHERE " + predicate).(*Select).Where
			filter := buildRawTrimComparisonFilter(columns, expr)
			if filter == nil {
				t.Fatal("comparison did not bind", predicate)
			}
			check := func(raw []any) {
				t.Helper()
				want, wantErr := evalRawExpr(plan, raw, expr)
				got, err := filter(raw)
				if got != (toTri(want) == tvTrue) || fmt.Sprint(err) != fmt.Sprint(wantErr) {
					t.Fatalf("%s (%#v): %v/%v, want %v/%v", predicate, raw, got, err, want, wantErr)
				}
			}
			for _, a := range values {
				for _, b := range values {
					for _, cut := range []any{nil, "", " x", "\ufffd", "\xff", 0} {
						check([]any{a, b, cut})
					}
				}
			}
			for _, raw := range [][]any{nil, {nil}, {"x"}, {nil, "x"}} {
				check(raw)
			}
		}
	}
}

type trimComparisonProbe struct{ parameter *Literal }

func (probe trimComparisonProbe) String() string {
	probe.parameter.Val = "α🙂"
	return "xxα🙂xx"
}

func TestRawTrimComparisonParametersAndOrder(t *testing.T) {
	columns := map[string]int{"a": 0}
	plan := &simpleSelectPlan{colIndex: columns}
	for _, reversed := range []bool{false, true} {
		parameter := &Literal{Parameter: true}
		trim := &FuncCall{Name: "TRIM", Args: []Expr{newVarRef("a"), &Literal{Val: "x"}}}
		expr := &Binary{Op: "=", Left: trim, Right: parameter}
		if reversed {
			expr.Left, expr.Right = expr.Right, expr.Left
		}
		filter := buildRawTrimComparisonFilter(columns, expr)
		for _, value := range []any{nil, "", "α🙂", 0, big.NewRat(1, 2), []byte("1/2")} {
			parameter.Val = value
			for _, a := range []any{nil, "xxα🙂xx", "xx1/2xx"} {
				want, wantErr := evalRawExpr(plan, []any{a}, expr)
				got, err := filter([]any{a})
				if got != (toTri(want) == tvTrue) || fmt.Sprint(err) != fmt.Sprint(wantErr) {
					t.Fatalf("reversed=%v parameter %v: %v/%v, want %v/%v", reversed, value, got, err, want, wantErr)
				}
			}
		}
		// Coercion changes the parameter: it must be read before or after
		// TRIM according to the written operand order.
		raw := []any{trimComparisonProbe{parameter}}
		parameter.Val = 0
		want, wantErr := evalRawExpr(plan, raw, expr)
		parameter.Val = 0
		got, err := filter(raw)
		if got != (toTri(want) == tvTrue) || fmt.Sprint(err) != fmt.Sprint(wantErr) {
			t.Fatalf("coercion order, reversed=%v: %v/%v, want %v/%v", reversed, got, err, want, wantErr)
		}
	}
	for _, sql := range []string{
		"TRIM(NULL)=1/0", "1/0=TRIM(NULL)", "NULL=TRIM('x',1/0)",
		"TRIM('x',1/0)=NULL", "TRIM(NULL)=TRIM('x',1/0)",
	} {
		expr := mustParse("SELECT 1 WHERE " + sql).(*Select).Where
		filter := buildRawTrimComparisonFilter(columns, expr)
		if filter == nil {
			t.Fatal("error case did not bind", sql)
		}
		if _, err := filter(nil); err == nil {
			t.Fatal("NULL hid an operand error", sql)
		}
	}
	for _, sql := range []string{"TRIM(a,missing)='x'", "TRIM(a)=NOW()", "TRIM(a)=ABS(1)", "a='x'"} {
		expr := mustParse("SELECT 1 WHERE " + sql).(*Select).Where
		if buildRawTrimComparisonFilter(columns, expr) != nil {
			t.Fatal("unsupported shape was fused", sql)
		}
	}
}

func TestRawTrimComparisonQueryAndMutationPaths(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	execSQL(t, db, "CREATE TABLE trim_filter_rows (id INT PRIMARY KEY,body TEXT,cut TEXT,target TEXT)")
	execSQL(t, db, "INSERT INTO trim_filter_rows VALUES (1,'  α🙂  ',' ','α🙂'),(2,'xxabcxx','x','abc'),(3,NULL,'x','abc'),(4,'abc',NULL,'abc'),(5,'abc','','abc'),(6,'...abc...','.','abc')")
	execSQL(t, db, "CREATE INDEX trim_filter_target ON trim_filter_rows(target)")
	query := "SELECT id FROM trim_filter_rows WHERE TRIM(body,cut)=target"
	want := []Row{{"id": 1}, {"id": 2}, {"id": 5}, {"id": 6}}
	if got := execSQL(t, db, query); !reflect.DeepEqual(got.Rows, want) {
		t.Fatal("filtered rows differ", got)
	}
	for _, tc := range []struct {
		predicate string
		rows      []Row
	}{
		{"NOT(TRIM(body,cut)='abc')", []Row{{"id": 1}}},
		{"(TRIM(body,cut)='abc') IS NULL", []Row{{"id": 3}, {"id": 4}}},
	} {
		if got := execSQL(t, db, "SELECT id FROM trim_filter_rows WHERE "+tc.predicate); !reflect.DeepEqual(got.Rows, tc.rows) {
			t.Fatalf("%s lost SQL UNKNOWN: %v", tc.predicate, got.Rows)
		}
	}
	columnar, err := ExecuteColumnar(t.Context(), db, "default", mustParse(query))
	if err != nil || !reflect.DeepEqual(columnar.Values[0], []any{1, 2, 5, 6}) {
		t.Fatalf("columnar: %v / %v", columnar, err)
	}
	stream, err := ExecuteStream(t.Context(), db, "default", mustParse(query))
	if err != nil {
		t.Fatal(err)
	}
	var rows []Row
	for stream.Next() {
		rows = append(rows, Row{"id": stream.Row()["id"]})
		stream.ReleaseRow(stream.Row())
	}
	err = stream.Err()
	stream.Close()
	if err != nil || !reflect.DeepEqual(rows, want) {
		t.Fatalf("stream: %v / %v", rows, err)
	}
	for _, suffix := range []string{" AND id=-1", " OR id>=0"} {
		if _, err := Execute(t.Context(), db, "default", mustParse("SELECT COUNT(*) FROM trim_filter_rows WHERE TRIM(body,1/0)='x'"+suffix)); err != nil {
			t.Fatalf("cheap guard no longer ran first: %s: %v", suffix, err)
		}
	}
	execSQL(t, db, "UPDATE trim_filter_rows SET target='updated' WHERE TRIM(body,cut)=target")
	execSQL(t, db, "DELETE FROM trim_filter_rows WHERE TRIM(body,cut)='abc'")
	if got := execSQL(t, db, "SELECT id FROM trim_filter_rows WHERE target='updated'"); !reflect.DeepEqual(got.Rows, []Row{{"id": 1}}) {
		t.Fatal("mutation or index results changed", got)
	}
}

func TestRawTrimComparisonAvoidsStringBoxes(t *testing.T) {
	for _, sql := range []string{"TRIM(a,'x')='α🙂'", "'α🙂'=RTRIM(a,'x')", "LTRIM(a,'x')=RTRIM(a,'x')"} {
		expr := mustParse("SELECT 1 WHERE " + sql).(*Select).Where
		filter := buildRawTrimComparisonFilter(map[string]int{"a": 0}, expr)
		raw := []any{"xxα🙂xx"}
		if allocations := testing.AllocsPerRun(100, func() {
			if _, err := filter(raw); err != nil {
				t.Fatal(err)
			}
		}); allocations != 0 {
			t.Fatalf("%s allocated %v objects per row", sql, allocations)
		}
	}
}
