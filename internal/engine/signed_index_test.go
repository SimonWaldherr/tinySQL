package engine

import (
	"fmt"
	"math"
	"math/big"
	"reflect"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestSignedIndexExceptionalComparisons(t *testing.T) {
	for _, values := range [][]any{
		{math.NaN(), math.Inf(1), math.Inf(-1), math.Copysign(0, -1), float64(-1)},
		{big.NewRat(-1, 1), big.NewRat(-3, 2), int(-1)},
		{int(-1), "text"},
	} {
		for _, indexed := range []bool{false, true} {
			db := signedIndexFixture(t, values, indexed)
			for _, predicate := range []string{"k=-1", "-1=k", "k<=-1", "-1<=k", "k<>-1", "k=-0", "k=-'invalid'"} {
				stmt := mustParse("SELECT id FROM signed_keys WHERE " + predicate).(*Select)
				plan := &simpleSelectPlan{colIndex: map[string]int{"id": 0, "k": 1}}
				var want []Row
				var wantErr error
				for i, value := range values {
					v, err := evalRawExpr(plan, []any{i, value}, stmt.Where)
					if err != nil {
						wantErr = err
						break
					}
					if toTri(v) == tvTrue {
						want = append(want, Row{"id": i})
					}
				}
				rs, err := Execute(t.Context(), db, "default", stmt)
				if (err == nil) != (wantErr == nil) {
					t.Fatalf("%s: err %v, want %v", predicate, err, wantErr)
				}
				if err == nil && (len(rs.Rows) != len(want) || (len(want) > 0 && !reflect.DeepEqual(rs.Rows, want))) {
					t.Fatalf("%s: rows %v, want %v", predicate, rs.Rows, want)
				}
			}
		}
	}
}

func signedIndexFixture(tb testing.TB, values []any, indexed bool) *storage.DB {
	tb.Helper()
	db := storage.NewDB()
	tb.Cleanup(func() { _ = db.Close() })
	table := storage.NewTable("signed_keys", []storage.Column{{Name: "id", Type: storage.IntType}, {Name: "k", Type: storage.FloatType}}, false)
	for i, value := range values {
		table.Rows = append(table.Rows, []any{i, value})
	}
	if err := db.Put("default", table); err != nil {
		tb.Fatal(err)
	}
	if indexed {
		benefitExec(tb, db, "CREATE INDEX signed_idx ON signed_keys(k)")
	}
	return db
}

// Use the generic raw evaluator as an independent oracle, including before an
// index exists: comparing two optimized paths would not detect a shared error.
func TestSignedIndexComparisonSemantics(t *testing.T) {
	for _, values := range [][]any{
		{int(-2), int64(-1), int(0), int(1), int64(2), nil},
		{int(-1), float64(-1), math.Copysign(0, -1), float64(0), int(0), float64(1.5), nil},
		{int64(-9007199254740993), int64(-9007199254740992), int64(9007199254740992), int64(9007199254740993)},
	} {
		for _, indexed := range []bool{false, true} {
			db := signedIndexFixture(t, values, indexed)
			for _, predicate := range []string{"k=-1", "-1=k", "k=+1", "k=-0", "k=+NULL", "k=-1.5", "k=1.0", "k=-9007199254740993", "k=+9007199254740993", "k < -1", "-1 <= k", "k != -1", "k=-1 OR k=+1", "k=-1 AND id=0", "k=-(-1)"} {
				stmt := mustParse("SELECT id FROM signed_keys WHERE " + predicate).(*Select)
				plan := &simpleSelectPlan{colIndex: map[string]int{"id": 0, "k": 1}}
				var want []Row
				for i, value := range values {
					v, err := evalRawExpr(plan, []any{i, value}, stmt.Where)
					if err != nil {
						t.Fatal(err)
					}
					if toTri(v) == tvTrue {
						want = append(want, Row{"id": i})
					}
				}
				rs, err := Execute(t.Context(), db, "default", stmt)
				if err != nil || len(rs.Rows) != len(want) || (len(want) > 0 && !reflect.DeepEqual(rs.Rows, want)) {
					t.Fatalf("indexed=%t %s values=%v: got %v / %v, want %v", indexed, predicate, values, rs, err, want)
				}
			}
		}
	}
}

func TestSignedIndexPlanAndParameterChanges(t *testing.T) {
	db := signedIndexFixture(t, []any{int(-1), int(0), int(1)}, true)
	for _, predicate := range []string{"k=-1", "-1=k", "k=+1", "k=1.0", "k=-0"} {
		rs := benefitExec(t, db, "EXPLAIN SELECT id FROM signed_keys WHERE "+predicate)
		if !strings.Contains(fmt.Sprint(rs.Rows), "INDEX POINT SEEK") {
			t.Fatalf("%s: %v", predicate, rs.Rows)
		}
	}
	stmt := mustParse("SELECT id FROM signed_keys WHERE k=-1").(*Select)
	parameter := stmt.Where.(*Binary).Right.(*Unary).Expr.(*Literal)
	parameter.Parameter = true
	for _, value := range []any{1, -1, 0, nil, 1} {
		parameter.Val = value
		rs, err := Execute(t.Context(), db, "default", stmt)
		if err != nil {
			t.Fatal(err)
		}
		if value == nil {
			if len(rs.Rows) != 0 {
				t.Fatal(rs.Rows)
			}
		} else if len(rs.Rows) != 1 || rs.Rows[0]["id"] != 1-value.(int) {
			t.Fatalf("value=%v rows=%v", value, rs.Rows)
		}
	}
	// A later float encoding must invalidate the integer-only seek decision.
	table, _ := db.Get("default", "signed_keys")
	table.Rows = append(table.Rows, []any{3, float64(-1)})
	table.Version++
	if err := table.RebuildSecondaryIndexes(); err != nil {
		t.Fatal(err)
	}
	parameter.Val = 1
	rs, err := Execute(t.Context(), db, "default", stmt)
	if err != nil || len(rs.Rows) != 2 {
		t.Fatalf("mixed numeric encodings: %v / %v", rs, err)
	}
}

func BenchmarkSignedIndex(b *testing.B) {
	values := make([]any, 20000)
	for i := range values {
		values[i] = i - 10000
	}
	for _, c := range []struct {
		name, predicate string
		matches         int
	}{
		{"negative_hit", "k=-17", 1}, {"negative_miss", "k=-20001", 0},
		{"positive_control", "k=17", 1}, {"integral_float", "k=17.0", 1},
	} {
		for _, indexed := range []bool{false, true} {
			b.Run(fmt.Sprintf("%s/indexed=%t", c.name, indexed), func(b *testing.B) {
				db := signedIndexFixture(b, values, indexed)
				stmt := mustParse("SELECT id FROM signed_keys WHERE " + c.predicate)
				if _, err := Execute(b.Context(), db, "default", stmt); err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				for b.Loop() {
					rs, err := Execute(b.Context(), db, "default", stmt)
					if err != nil || len(rs.Rows) != c.matches {
						b.Fatalf("%v / %v", rs, err)
					}
				}
			})
		}
	}
}
