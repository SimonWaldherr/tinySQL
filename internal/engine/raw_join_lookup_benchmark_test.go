package engine

import (
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func BenchmarkRawJoinLookupBuild(b *testing.B) {
	const size = 50000
	for _, tc := range []struct {
		name   string
		groups int
		filter bool
	}{
		{"Unique", size, false},
		{"Repeated", 500, false},
		{"Selective", size, true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			rows := make([][]any, size)
			for i := range rows {
				rows[i] = []any{i % tc.groups, i}
			}
			table := &storage.Table{Rows: rows, Version: 1}
			plan := &simpleJoinPlan{right: table, rightKey: 0}
			if tc.filter {
				plan.rightFilter = func(row []any) (bool, error) { return row[1].(int)%1000 == 0, nil }
			}
			b.ReportAllocs()
			for b.Loop() {
				// Rebuild a version-invalidated index; fixture construction and
				// projection allocations are outside this index-only benchmark.
				table.Version++
				lookup, err := plan.rightRowsByKey()
				if err != nil || lookup == nil {
					b.Fatalf("lookup = %v, err = %v", lookup, err)
				}
			}
		})
	}
}

func BenchmarkRawJoinLargePrepared(b *testing.B) {
	for _, tc := range []struct {
		name   string
		groups int
		count  int
	}{
		{"Unique", 50000, 1000},
		{"Repeated", 500, 100000},
	} {
		b.Run(tc.name, func(b *testing.B) {
			db := storage.NewDB()
			b.Cleanup(func() { _ = db.Close() })
			left := storage.NewTable("lookup_left", []storage.Column{
				{Name: "id", Type: storage.IntType}, {Name: "grp", Type: storage.IntType},
			}, false)
			right := storage.NewTable("lookup_right", []storage.Column{{Name: "id", Type: storage.IntType}}, false)
			for i := 0; i < 1000; i++ {
				left.Rows = append(left.Rows, []any{i % tc.groups, 0})
			}
			for i := 0; i < 50000; i++ {
				right.Rows = append(right.Rows, []any{i % tc.groups})
			}
			for _, table := range []*storage.Table{left, right} {
				if err := db.Put("default", table); err != nil {
					b.Fatal(err)
				}
			}
			stmt := mustParse(`SELECT l.grp AS g, COUNT(*) AS n FROM lookup_left l JOIN lookup_right r ON l.id = r.id GROUP BY l.grp`)
			if _, err := Execute(b.Context(), db, "default", stmt); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			for b.Loop() {
				result, err := Execute(b.Context(), db, "default", stmt)
				if err != nil || len(result.Rows) != 1 || result.Rows[0]["n"] != tc.count {
					b.Fatalf("prepared join result: %#v, %v", result, err)
				}
			}
		})
	}
}
