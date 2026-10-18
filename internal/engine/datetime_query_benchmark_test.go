package engine

import (
	"testing"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func BenchmarkDateTimeQueries(b *testing.B) {
	db := storage.NewDB()
	b.Cleanup(func() { db.Close() })
	table := storage.NewTable("dates", []storage.Column{
		{Name: "iso", Type: storage.TextType},
		{Name: "legacy", Type: storage.TextType},
	}, false)
	for i := 0; i < 10000; i++ {
		date := time.Date(2024, 1, 1+i%366, 12, 34, 56, 0, time.UTC)
		table.Rows = append(table.Rows, []any{date.Format("2006-01-02 15:04:05"), date.Format("01/02/2006")})
	}
	if err := db.Put("default", table); err != nil {
		b.Fatal(err)
	}
	for _, tc := range []struct{ name, sql string }{
		{"LegacyYearFilter", `SELECT COUNT(*) FROM dates WHERE YEAR(legacy)=2024`},
		{"FormattedMonthFilter", `SELECT COUNT(*) FROM dates WHERE STRFTIME('%Y-%m', iso)='2024-03'`},
		{"DateTruncGroups", `SELECT DATE_TRUNC('MONTH', iso) AS month, COUNT(*) FROM dates GROUP BY DATE_TRUNC('MONTH', iso)`},
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
