package engine

import (
	"fmt"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func BenchmarkInSubqueryManyRows(b *testing.B) {
	for _, text := range []bool{false, true} {
		b.Run(fmt.Sprintf("text=%v", text), func(b *testing.B) {
			db := storage.NewDB()
			b.Cleanup(func() { _ = db.Close() })
			columnType := storage.IntType
			if text {
				columnType = storage.TextType
			}
			outer := storage.NewTable("outer_values", []storage.Column{{Name: "v", Type: columnType}}, false)
			inner := storage.NewTable("inner_values", []storage.Column{{Name: "v", Type: columnType}}, false)
			for i := 0; i < 4000; i++ {
				var value any = i
				if text {
					value = fmt.Sprintf("key-%08d", i)
				}
				outer.Rows = append(outer.Rows, []any{value})
				if i%2 == 0 {
					inner.Rows = append(inner.Rows, []any{value})
				}
			}
			for _, table := range []*storage.Table{outer, inner} {
				if err := db.Put("default", table); err != nil {
					b.Fatal(err)
				}
			}
			stmt := mustParse(`SELECT COUNT(*) AS n FROM outer_values WHERE v IN (SELECT v FROM inner_values)`)
			b.ReportAllocs()
			for b.Loop() {
				rs, err := Execute(b.Context(), db, "default", stmt)
				if err != nil || len(rs.Rows) != 1 || rs.Rows[0]["n"] != 2000 {
					b.Fatalf("got %#v, %v; want 2000 matches", rs, err)
				}
			}
		})
	}
}
