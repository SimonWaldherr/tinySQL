package engine

import (
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestNullPredicateFiltersMatchExpressions(t *testing.T) {
	columns := map[string]int{"v": 0, "other": 1, "t.v": 0}
	values := []any{nil, false, true, 0, int64(0), 1.5, math.NaN(), "", "NULL", []byte(nil)}
	for _, predicate := range []string{
		"NOT (v IS NULL)", "NOT (v IS NOT NULL)",
		"COALESCE(v, other) IS NULL", "COALESCE(v, other) IS NOT NULL",
		"ISNULL(COALESCE(v, other))", "NOT ISNULL(COALESCE(v, other))",
		"COALESCE(NULL, t.v, other, NULL) IS NULL", "IFNULL(v, other) IS NOT NULL",
		"NVL(v, other) IS NULL", "COALESCE() IS NULL", "NULL IS NOT NULL",
		"COALESCE(v, 1/0) IS NULL", "COALESCE(v, missing) IS NOT NULL",
	} {
		expr := mustParse("SELECT v FROM t WHERE " + predicate).(*Select).Where
		filter := buildRawFilter(columns, expr)
		fallback := buildRawExprFilter(columns, expr)
		for _, value := range values {
			for _, other := range values {
				row := []any{value, other}
				got, err := filter(row)
				want, wantErr := fallback(row)
				if got != want || fmt.Sprint(err) != fmt.Sprint(wantErr) {
					t.Fatalf("%s values=%v/%v: got %v/%v, want %v/%v", predicate, value, other, got, err, want, wantErr)
				}
			}
		}
	}
}

func TestNullPredicateParameterRebinding(t *testing.T) {
	columns := map[string]int{"v": 0}
	literal := &Literal{Parameter: true}
	for _, negate := range []bool{false, true} {
		expr := &IsNull{Expr: &FuncCall{Name: "COALESCE", Args: []Expr{&VarRef{Name: "v"}, literal}}, Negate: negate}
		filter := buildRawFilter(columns, expr)
		fallback := buildRawExprFilter(columns, expr)
		for _, value := range []any{nil, 0, false, "", []byte(nil)} {
			literal.Val = value
			for _, row := range [][]any{{nil}, {1}, nil} {
				got, err := filter(row)
				want, wantErr := fallback(row)
				if got != want || fmt.Sprint(err) != fmt.Sprint(wantErr) {
					t.Fatalf("negate=%v bound=%v row=%v: got %v/%v, want %v/%v", negate, value, row, got, err, want, wantErr)
				}
			}
		}
	}
}

func TestNullPredicatesAcrossQueryPaths(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	execSQL(t, db, "CREATE TABLE nullable (id INT, v INT, other INT, required INT NOT NULL)")
	execSQL(t, db, "INSERT INTO nullable VALUES (1, NULL, NULL, 7), (2, 0, NULL, 7), (3, NULL, 0, 7), (4, 1, 2, 7)")
	for _, tc := range []struct{ predicate, reference string }{
		{"COALESCE(v, other) IS NULL", "v IS NULL AND other IS NULL"},
		{"COALESCE(v, other) IS NOT NULL", "v IS NOT NULL OR other IS NOT NULL"},
		{"NOT (v IS NULL)", "v IS NOT NULL"},
	} {
		query := "SELECT id FROM nullable WHERE " + tc.predicate
		want := execSQL(t, db, "SELECT id FROM nullable WHERE "+tc.reference)
		got := execSQL(t, db, query)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("%s: got %v, want %v", query, got, want)
		}
		stream, err := ExecuteStream(t.Context(), db, "default", mustParse(query))
		if err != nil {
			t.Fatal(err)
		}
		var rows []Row
		for stream.Next() {
			rows = append(rows, stream.Row())
		}
		streamErr := stream.Err()
		_ = stream.Close()
		if streamErr != nil || !reflect.DeepEqual(rows, want.Rows) {
			t.Fatalf("stream %s: rows=%v error=%v", query, rows, streamErr)
		}
		columns, err := ExecuteColumnar(t.Context(), db, "default", mustParse(query))
		if err != nil || !reflect.DeepEqual(*columns, columnarFromRows(want)) {
			t.Fatalf("columnar %s: result=%v error=%v", query, columns, err)
		}
	}
	_, err := Execute(t.Context(), db, "default", mustParse("UPDATE nullable SET required = NULL WHERE COALESCE(v, other) IS NOT NULL"))
	if err == nil || !strings.Contains(err.Error(), "NOT NULL") {
		t.Fatalf("NOT NULL constraint was not enforced: %v", err)
	}
	if result := execSQL(t, db, "SELECT SUM(required) AS n FROM nullable"); result.Rows[0]["n"] != 28 {
		t.Fatalf("failed NOT NULL update changed rows: %v", result.Rows)
	}
	execSQL(t, db, "UPDATE nullable SET v = 9 WHERE COALESCE(v, other) IS NULL")
	if result := execSQL(t, db, "SELECT id FROM nullable WHERE v = 9"); len(result.Rows) != 1 || result.Rows[0]["id"] != 1 {
		t.Fatalf("NULL predicate update: %v", result.Rows)
	}
	execSQL(t, db, "DELETE FROM nullable WHERE NOT (v IS NULL)")
	if result := execSQL(t, db, "SELECT id FROM nullable"); len(result.Rows) != 1 || result.Rows[0]["id"] != 3 {
		t.Fatalf("NOT NULL predicate delete: %v", result.Rows)
	}
}

func BenchmarkNullPredicates(b *testing.B) {
	db := storage.NewDB()
	b.Cleanup(func() { _ = db.Close() })
	table := storage.NewTable("nullable", []storage.Column{{Name: "v", Type: storage.IntType}, {Name: "other", Type: storage.IntType}}, false)
	for i := 0; i < 50_000; i++ {
		var value, other any
		if i%3 != 0 {
			value = i
		}
		if i%5 != 0 {
			other = i
		}
		table.Rows = append(table.Rows, []any{value, other})
	}
	if err := db.Put("default", table); err != nil {
		b.Fatal(err)
	}
	for _, predicate := range []string{"v IS NULL", "v IS NOT NULL", "NOT (v IS NULL)", "COALESCE(v, other) IS NULL", "COALESCE(v, other) IS NOT NULL"} {
		b.Run(predicate, func(b *testing.B) {
			runBench(b, db, "SELECT COUNT(*) AS n FROM nullable WHERE "+predicate)
		})
	}
}
