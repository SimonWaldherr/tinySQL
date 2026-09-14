package engine

import (
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func primaryOptimizationFixture(tb testing.TB, n int) *storage.DB {
	tb.Helper()
	db := storage.NewDB()
	tb.Cleanup(func() { _ = db.Close() })
	benefitExec(tb, db, "CREATE TABLE pk_opt (id INT PRIMARY KEY, uid INT UNIQUE, value INT)")
	table, _ := db.Get("default", "pk_opt")
	for i := 0; i < n; i++ {
		key := i - n/2
		table.Rows = append(table.Rows, []any{key, key, 0})
	}
	table.Version++
	getConstraintIndex(table, 0)
	getConstraintIndex(table, 1)
	return db
}

func BenchmarkPrimaryOptimization(b *testing.B) {
	for _, n := range []int{1000, 20000} {
		for _, c := range []struct{ name, sql string }{
			{"pk_positive", "SELECT value FROM pk_opt WHERE id=17"},
			{"pk_negative", "SELECT value FROM pk_opt WHERE id=-17"},
			{"unique_negative", "SELECT value FROM pk_opt WHERE uid=-17"},
			{"update_positive", "UPDATE pk_opt SET value=1-value WHERE id=17"},
			{"update_negative", "UPDATE pk_opt SET value=1-value WHERE id=-17"},
			{"delete_miss_positive", "DELETE FROM pk_opt WHERE id=30000"},
			{"delete_miss_negative", "DELETE FROM pk_opt WHERE id=-30000"},
		} {
			b.Run(fmt.Sprintf("n=%d/%s", n, c.name), func(b *testing.B) {
				db := primaryOptimizationFixture(b, n)
				stmt := mustParse(c.sql)
				if _, err := Execute(b.Context(), db, "default", stmt); err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				for b.Loop() {
					rs, err := Execute(b.Context(), db, "default", stmt)
					if err != nil || len(rs.Rows) != 1 {
						b.Fatalf("%v / %v", rs, err)
					}
				}
			})
		}
	}
}

func BenchmarkPrimaryDeleteHit(b *testing.B) {
	db := primaryOptimizationFixture(b, 20000)
	stmt := mustParse("DELETE FROM pk_opt WHERE id=-17")
	insert := mustParse("INSERT INTO pk_opt VALUES (-17,-17,0)")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rs, err := Execute(b.Context(), db, "default", stmt)
		if err != nil || rs.Rows[0]["deleted"] != 1 {
			b.Fatalf("%v / %v", rs, err)
		}
		b.StopTimer()
		if _, err := Execute(b.Context(), db, "default", insert); err != nil {
			b.Fatal(err)
		}
		table, _ := db.Get("default", "pk_opt")
		getConstraintIndex(table, 0)
		getConstraintIndex(table, 1)
		b.StartTimer()
	}
}

func BenchmarkPrimaryKeyChurn(b *testing.B) {
	db := primaryOptimizationFixture(b, 2000)
	stmt := mustParse("UPDATE pk_opt SET id=1 WHERE uid=17").(*Update)
	// Bind a fresh unused key each iteration while retaining the same row count.
	value := stmt.Sets["id"].(*Literal)
	value.Parameter = true
	next := -30000
	b.ReportAllocs()
	for b.Loop() {
		value.Val = next
		next--
		if _, err := Execute(b.Context(), db, "default", stmt); err != nil {
			b.Fatal(err)
		}
	}
}

func TestPrimarySignedDMLAndRollback(t *testing.T) {
	db := primaryOptimizationFixture(t, 100)
	for _, column := range []string{"id", "uid"} {
		query := "SELECT value FROM pk_opt WHERE " + column + "=-17"
		if got := benefitExec(t, db, query); len(got.Rows) != 1 {
			t.Fatal(got)
		}
		benefitExec(t, db, "UPDATE pk_opt SET value=1-value WHERE "+column+"=-17")
	}
	before := benefitExec(t, db, "SELECT * FROM pk_opt")
	if _, err := Execute(t.Context(), db, "default", mustParse("UPDATE pk_opt SET id=-18 WHERE id=-17")); err == nil {
		t.Fatal("duplicate primary key accepted")
	}
	if _, err := Execute(t.Context(), db, "default", mustParse("UPDATE pk_opt SET uid=-18 WHERE id=-17")); err == nil {
		t.Fatal("duplicate unique key accepted")
	}
	if after := benefitExec(t, db, "SELECT * FROM pk_opt"); !reflect.DeepEqual(before.Rows, after.Rows) {
		t.Fatal("failed update changed rows")
	}
	benefitExec(t, db, "UPDATE pk_opt SET id=-300 WHERE id=-17")
	if got := benefitExec(t, db, "SELECT value FROM pk_opt WHERE id=-17"); len(got.Rows) != 0 {
		t.Fatal(got)
	}
	if got := benefitExec(t, db, "DELETE FROM pk_opt WHERE id=-300"); got.Rows[0]["deleted"] != 1 {
		t.Fatal(got)
	}
	benefitExec(t, db, "INSERT INTO pk_opt VALUES (-17,-17,7)")
	if got := benefitExec(t, db, "SELECT value FROM pk_opt WHERE id=-17"); len(got.Rows) != 1 || got.Rows[0]["value"] != 7 {
		t.Fatal(got)
	}
}

func TestPrimarySignedPlansAndNumericFallbacks(t *testing.T) {
	db := primaryOptimizationFixture(t, 100)
	for _, predicate := range []string{"id=-17", "-17=id", "uid=-17", "id=+17"} {
		if got := benefitExec(t, db, "EXPLAIN SELECT value FROM pk_opt WHERE "+predicate); !strings.Contains(fmt.Sprint(got.Rows), "CONSTRAINT INDEX POINT SEEK") {
			t.Fatal(got)
		}
	}
	stmt := mustParse("SELECT id FROM pk_opt WHERE id=-17").(*Select)
	parameter := stmt.Where.(*Binary).Right.(*Unary).Expr.(*Literal)
	parameter.Parameter = true
	for _, value := range []any{17, -17, nil, 30000, 17} {
		parameter.Val = value
		rs, err := Execute(t.Context(), db, "default", stmt)
		if err != nil {
			t.Fatal(err)
		}
		if value == nil || value == 30000 {
			if len(rs.Rows) != 0 {
				t.Fatal(rs.Rows)
			}
		} else if len(rs.Rows) != 1 || rs.Rows[0]["id"] != -value.(int) {
			t.Fatal(rs.Rows)
		}
	}
	for _, constraint := range []storage.ConstraintType{storage.PrimaryKey, storage.Unique} {
		for _, values := range [][]any{
			{int(-1), float64(-1), math.NaN(), nil},
			{int64(-9007199254740993), int64(-9007199254740992)},
		} {
			other := signedIndexFixture(t, values, false)
			table, _ := other.Get("default", "signed_keys")
			table.Cols[1].Constraint = constraint
			for _, predicate := range []string{"k=-1", "k=-9007199254740993"} {
				query := mustParse("SELECT id FROM signed_keys WHERE " + predicate).(*Select)
				plan := &simpleSelectPlan{colIndex: map[string]int{"id": 0, "k": 1}}
				var expected []Row
				for i, value := range values {
					v, err := evalRawExpr(plan, []any{i, value}, query.Where)
					if err != nil {
						t.Fatal(err)
					}
					if toTri(v) == tvTrue {
						expected = append(expected, Row{"id": i})
					}
				}
				rs, err := Execute(t.Context(), other, "default", query)
				if err != nil || len(rs.Rows) != len(expected) || (len(expected) > 0 && !reflect.DeepEqual(rs.Rows, expected)) {
					t.Fatalf("%v / %v, want %v", rs, err, expected)
				}
			}
		}
	}
}
