package engine

import (
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func BenchmarkNullFunctions(b *testing.B) {
	db := storage.NewDB()
	b.Cleanup(func() { db.Close() })
	table := storage.NewTable("items", []storage.Column{{Name: "id", Type: storage.IntType}, {Name: "v", Type: storage.IntType}, {Name: "body", Type: storage.TextType}}, false)
	for i := 0; i < 50000; i++ {
		var v any = i % 100
		if i%10 == 0 {
			v = nil
		}
		table.Rows = append(table.Rows, []any{i, v, strings.Repeat("PAYLOAD ", 32)})
	}
	if err := db.Put("default", table); err != nil {
		b.Fatal(err)
	}
	for _, tc := range []struct{ name, sql string }{
		{"isnull", `SELECT COUNT(*) FROM items WHERE ISNULL(v)`},
		{"is_null", `SELECT COUNT(*) FROM items WHERE v IS NULL`},
		{"not_isnull", `SELECT COUNT(*) FROM items WHERE NOT ISNULL(v)`},
		{"coalesce", `SELECT SUM(COALESCE(v, 0)) FROM items`},
		{"ifnull", `SELECT SUM(IFNULL(v, 0)) FROM items`},
		{"nullif", `SELECT COUNT(NULLIF(v, 1)) FROM items`},
		{"coalesce_fallback", `SELECT SUM(COALESCE(v, LENGTH(LOWER(body)))) FROM items`},
		{"if_fallback", `SELECT SUM(IF(ISNULL(v), LENGTH(LOWER(body)), v)) FROM items`},
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
