package engine

import (
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func BenchmarkFlushCachedInsertsIntoEmptyTable(b *testing.B) {
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
		table.Rows = nil
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
		if len(table.Rows) != count {
			b.Fatalf("flushed %d rows, want %d", len(table.Rows), count)
		}
		stage()
		b.StartTimer()
	}
}
