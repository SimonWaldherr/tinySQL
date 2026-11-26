package engine

import (
	"fmt"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func BenchmarkEnqueueCachedInsert(b *testing.B) {
	for _, width := range []int{3, 32} {
		b.Run(fmt.Sprintf("columns=%d", width), func(b *testing.B) {
			db := storage.NewDB()
			defer db.Close()
			cols := make([]storage.Column, width)
			values := make([]Expr, width)
			for i := range cols {
				cols[i] = storage.Column{Name: fmt.Sprintf("c%d", i), Type: storage.IntType}
				values[i] = &Literal{Val: i}
			}
			table := storage.NewTable("docs", cols, false)
			if err := db.Put("default", table); err != nil {
				b.Fatal(err)
			}
			insert := &Insert{Table: "docs", Cached: true, Rows: [][]Expr{values}}
			discard := &DiscardInserts{Table: "docs"}
			b.ReportAllocs()
			for i := 1; b.Loop(); i++ {
				if _, err := Execute(b.Context(), db, "default", insert); err != nil {
					b.Fatal(err)
				}
				if i%4096 == 0 {
					b.StopTimer()
					if _, err := Execute(b.Context(), db, "default", discard); err != nil {
						b.Fatal(err)
					}
					b.StartTimer()
				}
			}
		})
	}
}

func BenchmarkFlushCachedInsertsIntoEmptyTable(b *testing.B) {
	benchmarkFlushCachedInserts(b, nil)
}

func BenchmarkFlushCachedInsertsIntoPopulatedTable(b *testing.B) {
	benchmarkFlushCachedInserts(b, [][]any{{-1}})
}

func benchmarkFlushCachedInserts(b *testing.B, existing [][]any) {
	const count = 65536
	db := storage.NewDB()
	defer db.Close()
	table := storage.NewTable("docs", []storage.Column{{Name: "id", Type: storage.IntType}}, false)
	if err := db.Put("default", table); err != nil {
		b.Fatal(err)
	}
	rows := make([][]any, count)
	for i := range rows {
		rows[i] = []any{i}
	}
	stage := func() {
		table.Rows = existing
		if err := db.AppendStagedInsertRows("default", "", table, rows); err != nil {
			b.Fatal(err)
		}
	}
	statement := &FlushInserts{Table: "docs"}
	stage()
	b.ReportAllocs()
	for b.Loop() {
		if _, err := Execute(b.Context(), db, "default", statement); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		if got := len(table.Rows) - len(existing); got != count {
			b.Fatalf("flushed %d rows, want %d", got, count)
		}
		stage()
		b.StartTimer()
	}
}
