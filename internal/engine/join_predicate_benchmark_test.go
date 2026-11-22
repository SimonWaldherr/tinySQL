package engine

import (
	"fmt"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func BenchmarkGeneralJoinRangePredicate(b *testing.B) {
	for _, tc := range []struct {
		name     string
		join     string
		leftRows int
	}{
		{"Right", "RIGHT", 225},
		{"Full", "FULL OUTER", 225},
		{"LargeLeft", "LEFT", 525},
	} {
		b.Run(tc.name, func(b *testing.B) {
			db := storage.NewDB()
			b.Cleanup(func() { _ = db.Close() })
			left := storage.NewTable("bands", []storage.Column{{Name: "lo", Type: storage.IntType}, {Name: "hi", Type: storage.IntType}}, false)
			right := storage.NewTable("events", []storage.Column{{Name: "k", Type: storage.IntType}, {Name: "enabled", Type: storage.BoolType}}, false)
			for i := range tc.leftRows {
				left.Rows = append(left.Rows, []any{2 * i, 2*i + 1})
			}
			for i := range 225 {
				right.Rows = append(right.Rows, []any{2 * i, i%2 == 0})
			}
			for _, table := range []*storage.Table{left, right} {
				if err := db.Put("default", table); err != nil {
					b.Fatal(err)
				}
			}
			query := fmt.Sprintf(`SELECT bands.lo, events.k FROM bands %s JOIN events
				ON bands.lo <= events.k AND bands.hi >= events.k AND events.enabled = TRUE`, tc.join)
			runBench(b, db, query)
		})
	}
}
