package engine

import (
	"fmt"
	"testing"
)

// ORDER BY and outer joins deliberately bypass the raw-row inner-join fast
// path, so these benchmarks measure the general executor and its candidate lookup.
func BenchmarkSmallGeneralJoin(b *testing.B) {
	for _, query := range []struct {
		name string
		sql  string
	}{
		{"InnerOrdered", `SELECT l.id, l.val, r.extra FROM l JOIN r ON l.id = r.id ORDER BY l.id`},
		{"Left", `SELECT l.id, l.val, r.extra FROM l LEFT JOIN r ON l.id = r.id`},
		{"Right", `SELECT l.id, l.val, r.extra FROM l RIGHT JOIN r ON l.id = r.id`},
		{"Full", `SELECT l.id, l.val, r.extra FROM l FULL OUTER JOIN r ON l.id = r.id`},
	} {
		b.Run(query.name, func(b *testing.B) {
			for _, rows := range []int{10, 100, 400} {
				b.Run(fmt.Sprintf("Rows%d", rows), func(b *testing.B) {
					db := setupJoinTables(b, rows, rows)
					runBench(b, db, query.sql)
				})
			}
		})
	}
}
