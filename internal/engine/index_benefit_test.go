package engine

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func indexBenefitFixture(tb testing.TB, n, distinct int) *storage.DB {
	tb.Helper()
	db := storage.NewDB()
	tb.Cleanup(func() { _ = db.Close() })
	if _, err := Execute(tb.Context(), db, "default", mustParse("CREATE TABLE benefit (id INT PRIMARY KEY, lookup INT, payload INT)")); err != nil {
		tb.Fatal(err)
	}
	table, _ := db.Get("default", "benefit")
	for i := 0; i < n; i++ {
		table.Rows = append(table.Rows, []any{i, i % distinct, 0})
	}
	table.Version++
	return db
}

func benefitExec(tb testing.TB, db *storage.DB, sql string) *ResultSet {
	tb.Helper()
	rs, err := Execute(tb.Context(), db, "default", mustParse(sql))
	if err != nil {
		tb.Fatal(err)
	}
	return rs
}

func TestIndexBenefitResultsAndMaintenance(t *testing.T) {
	for _, distinct := range []int{2, 100, 2000} {
		t.Run(fmt.Sprint(distinct), func(t *testing.T) {
			db := indexBenefitFixture(t, 2000, distinct)
			queries := []string{"SELECT id, lookup FROM benefit WHERE lookup=1", "SELECT id FROM benefit WHERE lookup=-1", "SELECT id FROM benefit WHERE lookup=1 AND payload=0"}
			want := make([]*ResultSet, len(queries))
			for i, q := range queries {
				want[i] = benefitExec(t, db, q)
			}
			benefitExec(t, db, "CREATE INDEX lookup_idx ON benefit(lookup)")
			for i, q := range queries {
				if got := benefitExec(t, db, q); !reflect.DeepEqual(got.Rows, want[i].Rows) {
					t.Fatalf("index changed results for %s", q)
				}
			}
			benefitExec(t, db, "UPDATE benefit SET lookup=5000 WHERE id=1")
			benefitExec(t, db, "INSERT INTO benefit VALUES (2000, 5000, 0)")
			benefitExec(t, db, "UPDATE benefit SET lookup=5000 WHERE id=0") // out-of-order posting
			benefitExec(t, db, "DELETE FROM benefit WHERE id=1")
			got := benefitExec(t, db, "SELECT id FROM benefit WHERE lookup=5000")
			benefitExec(t, db, "DROP INDEX lookup_idx ON benefit")
			wantRows := benefitExec(t, db, "SELECT id FROM benefit WHERE lookup=5000")
			if len(got.Rows) != 2 || !reflect.DeepEqual(got.Rows, wantRows.Rows) {
				t.Fatalf("maintenance mismatch: %v / %v", got.Rows, wantRows.Rows)
			}
		})
	}
}

func BenchmarkIndexBenefitRead(b *testing.B) {
	for _, c := range []struct {
		name                        string
		n, distinct, value, matches int
	}{
		{"small_unique", 1000, 1000, 17, 1},
		{"unique", 20000, 20000, 12345, 1},
		{"miss", 20000, 20000, 20001, 0},
		{"negative_miss", 20000, 20000, -1, 0},
		{"one_percent", 20000, 100, 17, 200},
		{"half", 20000, 2, 1, 10000},
	} {
		for _, indexed := range []bool{false, true} {
			b.Run(fmt.Sprintf("%s/indexed=%t", c.name, indexed), func(b *testing.B) {
				db := indexBenefitFixture(b, c.n, c.distinct)
				if indexed {
					benefitExec(b, db, "CREATE INDEX lookup_idx ON benefit(lookup)")
				}
				stmt := mustParse(fmt.Sprintf("SELECT id FROM benefit WHERE lookup=%d", c.value))
				b.ReportAllocs()
				for b.Loop() {
					rs, err := Execute(b.Context(), db, "default", stmt)
					if err != nil || len(rs.Rows) != c.matches {
						b.Fatalf("read: %v, %v", rs, err)
					}
				}
			})
		}
	}
}

func BenchmarkIndexBenefitBuild(b *testing.B) {
	for _, distinct := range []int{20000, 100, 2} {
		b.Run(fmt.Sprintf("distinct=%d", distinct), func(b *testing.B) {
			db := indexBenefitFixture(b, 20000, distinct)
			create := mustParse("CREATE INDEX lookup_idx ON benefit(lookup)")
			drop := mustParse("DROP INDEX lookup_idx ON benefit")
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := Execute(b.Context(), db, "default", create); err != nil {
					b.Fatal(err)
				}
				b.StopTimer()
				if _, err := Execute(b.Context(), db, "default", drop); err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
			}
		})
	}
}

func BenchmarkIndexBenefitWrite(b *testing.B) {
	for _, column := range []string{"payload", "lookup"} {
		for _, indexed := range []bool{false, true} {
			b.Run(fmt.Sprintf("%s/indexed=%t", column, indexed), func(b *testing.B) {
				db := indexBenefitFixture(b, 2000, 100)
				if indexed {
					benefitExec(b, db, "CREATE INDEX lookup_idx ON benefit(lookup)")
				}
				stmt := mustParse(fmt.Sprintf("UPDATE benefit SET %s=1-%s WHERE id=0", column, column))
				if _, err := Execute(b.Context(), db, "default", stmt); err != nil {
					b.Fatal(err)
				} // warm primary-key lookup
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
