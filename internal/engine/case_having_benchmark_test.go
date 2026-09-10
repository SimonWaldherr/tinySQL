package engine

import (
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func BenchmarkCaseHaving(b *testing.B) {
	db := storage.NewDB()
	b.Cleanup(func() { db.Close() })
	table := storage.NewTable("events", []storage.Column{{Name: "bucket", Type: storage.IntType}, {Name: "v", Type: storage.IntType}}, false)
	for i := 0; i < 50000; i++ {
		var value any = i % 100
		if i%10 == 0 {
			value = nil
		}
		table.Rows = append(table.Rows, []any{i % 10000, value})
	}
	if err := db.Put("default", table); err != nil {
		b.Fatal(err)
	}
	for _, tc := range []struct{ name, sql string }{
		{"searched_case", `SELECT SUM(CASE WHEN v > 50 THEN v ELSE 0 END) FROM events`},
		{"simple_case", `SELECT SUM(CASE v WHEN 1 THEN 10 WHEN 2 THEN 20 ELSE 0 END) FROM events`},
		{"case_filter", `SELECT COUNT(*) FROM events WHERE CASE WHEN v IS NULL THEN FALSE ELSE v > 95 END`},
		{"having", `SELECT bucket, COUNT(*) AS n, SUM(v) AS total FROM events GROUP BY bucket HAVING COUNT(*) > 3 AND SUM(v) > 480`},
		{"having_case_sum", `SELECT bucket, SUM(CASE WHEN v > 50 THEN v ELSE 0 END) AS total FROM events GROUP BY bucket HAVING SUM(CASE WHEN v > 50 THEN v ELSE 0 END) > 480`},
	} {
		b.Run(tc.name, func(b *testing.B) {
			stmt := mustParse(tc.sql)
			if _, err := Execute(b.Context(), db, "default", stmt); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			for b.Loop() {
				if _, err := Execute(b.Context(), db, "default", stmt); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
