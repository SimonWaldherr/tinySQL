package engine

import (
	"fmt"
	"math"
	"reflect"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func idUniqueFixture(tb testing.TB, n int) *storage.DB {
	tb.Helper()
	db := storage.NewDB()
	tb.Cleanup(func() { _ = db.Close() })
	benefitExec(tb, db, "CREATE TABLE id_unique (id INT PRIMARY KEY, code TEXT UNIQUE)")
	table, _ := db.Get("default", "id_unique")
	for i := 0; i < n; i++ {
		table.Rows = append(table.Rows, []any{i, fmt.Sprintf("code-%d", i)})
	}
	table.Version++
	getConstraintIndex(table, 0)
	getConstraintIndex(table, 1)
	return db
}

func BenchmarkIDUnique(b *testing.B) {
	for _, n := range []int{1000, 20000} {
		for _, c := range []struct{ name, sql string }{
			{"max_id", "SELECT MAX(id) AS value FROM id_unique"},
			{"filtered_max", "SELECT MAX(id) AS value FROM id_unique WHERE id<500"},
			{"unique_lookup", "SELECT id FROM id_unique WHERE code='code-17'"},
			{"unique_conflict", "INSERT INTO id_unique VALUES (100000,'code-17') ON CONFLICT DO NOTHING"},
		} {
			b.Run(fmt.Sprintf("n=%d/%s", n, c.name), func(b *testing.B) {
				db := idUniqueFixture(b, n)
				stmt := mustParse(c.sql)
				b.ReportAllocs()
				for b.Loop() {
					if _, err := Execute(b.Context(), db, "default", stmt); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

// Fixed-size 32-row batches. Reset and warm-up are excluded; normal INSERT
// validation is timed. This prevents the faster version measuring larger tables.
func BenchmarkIDUniqueInsert(b *testing.B) {
	for _, nextID := range []bool{false, true} {
		b.Run(fmt.Sprintf("next_id=%t", nextID), func(b *testing.B) {
			db := idUniqueFixture(b, 2000)
			table, _ := db.Get("default", "id_unique")
			maxStmt := mustParse("SELECT MAX(id) AS value FROM id_unique")
			insert := mustParse("INSERT INTO id_unique VALUES (0,'new')").(*Insert)
			id := insert.Rows[0][0].(*Literal)
			code := insert.Rows[0][1].(*Literal)
			codes := make([]string, 32)
			for i := range codes {
				codes[i] = fmt.Sprintf("new-%d", i)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for k := 0; k < b.N; k++ {
				for i := 0; i < 32; i++ {
					id.Val = 2000 + i
					if nextID {
						rs, err := Execute(b.Context(), db, "default", maxStmt)
						if err != nil {
							b.Fatal(err)
						}
						id.Val = rs.Rows[0]["value"].(int) + 1
					}
					code.Val = codes[i]
					if _, err := Execute(b.Context(), db, "default", insert); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				if len(table.Rows) != 2032 {
					b.Fatal(len(table.Rows))
				}
				clear(table.Rows[2000:])
				table.Rows = table.Rows[:2000]
				table.Version++
				invalidateConstraintIndexes(table)
				getConstraintIndex(table, 0)
				getConstraintIndex(table, 1)
				b.StartTimer()
			}
		})
	}
}

func TestIDMaximumAndUniqueSemantics(t *testing.T) {
	db := idUniqueFixture(t, 20)
	check := func(want any) {
		t.Helper()
		got := benefitExec(t, db, "SELECT MAX(id) AS value FROM id_unique").Rows[0]["value"]
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("MAX=%v (%T), want %v (%T)", got, got, want, want)
		}
	}
	check(19)
	benefitExec(t, db, "INSERT INTO id_unique VALUES (1000,'explicit')")
	check(1000)
	benefitExec(t, db, "INSERT INTO id_unique VALUES (999,'below')")
	check(1000)
	benefitExec(t, db, "UPDATE id_unique SET id=1001 WHERE id=1000")
	check(1001)
	benefitExec(t, db, "UPDATE id_unique SET id=1002 WHERE id=999")
	check(1002)
	benefitExec(t, db, "DELETE FROM id_unique WHERE id=1002")
	check(1001)
	benefitExec(t, db, "DELETE FROM id_unique WHERE id=1001")
	check(19)
	if _, err := Execute(t.Context(), db, "default", mustParse("INSERT INTO id_unique VALUES (2000,'temporary'),(2001,'code-0')")); err == nil {
		t.Fatal("duplicate UNIQUE accepted")
	}
	check(19)
	benefitExec(t, db, "INSERT INTO id_unique VALUES (20,NULL),(21,NULL)")
	check(21)
	benefitExec(t, db, "DELETE FROM id_unique")
	check(nil)
	benefitExec(t, db, "INSERT INTO id_unique VALUES (-7,'negative')")
	check(-7)
}

func TestIDMaximumTypesCloneAndLimits(t *testing.T) {
	for _, values := range [][]any{
		{nil, nil}, {int(-7), int64(-3)},
		{int64(9007199254740993), int64(9007199254740992)},
		{int64(7), int(7), int(2)}, {int(7), int64(7)},
		{float64(3), math.NaN(), math.Inf(1)}, {"x", "y"},
	} {
		db := signedIndexFixture(t, values, false)
		table, _ := db.Get("default", "signed_keys")
		table.Cols[1].Constraint = storage.Unique
		getConstraintIndex(table, 1)
		for _, suffix := range []string{"", " LIMIT 0", " LIMIT 1 OFFSET 1"} {
			got := benefitExec(t, db, "SELECT MAX(k) AS m FROM signed_keys"+suffix)
			want := benefitExec(t, db, "SELECT MAX(k) AS m FROM signed_keys WHERE 1=1"+suffix)
			if !reflect.DeepEqual(got.Rows, want.Rows) {
				t.Fatalf("values=%v: %v / %v", values, got.Rows, want.Rows)
			}
		}
	}
	db := idUniqueFixture(t, 20)
	clone := db.DeepClone()
	t.Cleanup(func() { _ = clone.Close() })
	benefitExec(t, clone, "INSERT INTO id_unique VALUES (2000,'clone')")
	if got := benefitExec(t, clone, "SELECT MAX(id) AS m FROM id_unique"); got.Rows[0]["m"] != 2000 {
		t.Fatal(got)
	}
	if got := benefitExec(t, db, "SELECT MAX(id) AS m FROM id_unique"); got.Rows[0]["m"] != 19 {
		t.Fatal(got)
	}
}
