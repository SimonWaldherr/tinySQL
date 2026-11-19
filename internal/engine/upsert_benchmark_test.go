package engine

import (
	"fmt"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// All input keys already exist, as in a repeated inventory import.
func BenchmarkUpsertBatch(b *testing.B) {
	for _, n := range []int{1, 256} {
		values := make([]string, n)
		for i := range n {
			values[i] = fmt.Sprintf("(%d, 1, 'stock')", i)
		}
		insert := "INSERT INTO inventory VALUES " + strings.Join(values, ",")
		for _, tc := range []struct{ name, clause string }{
			{"Update", "DO UPDATE SET qty = qty + excluded.qty, note = excluded.note"},
			{"Skip", "DO UPDATE SET qty = excluded.qty WHERE qty < 0"},
			{"DoNothing", "DO NOTHING"},
		} {
			b.Run(fmt.Sprintf("%s/%d", tc.name, n), func(b *testing.B) {
				db := storage.NewDB()
				b.Cleanup(func() { _ = db.Close() })
				for _, sql := range []string{"CREATE TABLE inventory (id INT PRIMARY KEY, qty INT, note TEXT)", insert} {
					if _, err := Execute(b.Context(), db, "default", mustParse(sql)); err != nil {
						b.Fatal(err)
					}
				}
				stmt := mustParse(insert + " ON CONFLICT (id) " + tc.clause)
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
