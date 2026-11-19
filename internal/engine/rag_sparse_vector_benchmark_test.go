package engine

import (
	"encoding/json"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// Warm the row filter independently of the embedding column: switching
// between map/text and vector retrieval need not populate both caches.
func BenchmarkRAGSparseVectorSQL(b *testing.B) {
	const rows, dims = 8192, 128
	table := storage.NewTable("rag_sparse", []storage.Column{
		{Name: "id", Type: storage.IntType},
		{Name: "region", Type: storage.IntType},
		{Name: "embedding", Type: storage.VectorType},
	}, false)
	table.Rows = make([][]any, rows)
	for i := range rows {
		v := make([]float64, dims)
		for d := range v {
			v[d] = float64((i+d*7)%127 - 63)
		}
		table.Rows[i] = []any{i, i % 256, v}
	}
	db := storage.NewDB()
	b.Cleanup(func() { _ = db.Close() })
	b.Cleanup(func() {
		purgeVectorCachesFor("default", table.Name)
		purgeRAGPreFilterCachesFor(table.Name)
	})
	if err := db.Put("default", table); err != nil {
		b.Fatal(err)
	}
	if err := table.CreateSecondaryIndex("ix_region", []string{"region"}, false); err != nil {
		b.Fatal(err)
	}
	query, err := json.Marshal(table.Rows[1][2])
	if err != nil {
		b.Fatal(err)
	}
	stmt := mustParse(`SELECT id, _vec_distance FROM RAG_SEARCH('rag_sparse','embedding','` + string(query) + `',6,'{"pre_filter":{"equals":{"region":1}}}')`)
	run := func(b *testing.B) {
		rs, err := Execute(b.Context(), db, "default", stmt)
		if err != nil || rs == nil || len(rs.Rows) != 6 {
			b.Fatal("RAG_SEARCH result", rs, err)
		}
	}
	run(b)
	key := vecSearchColumnCacheKey{source: table, tenant: "default", table: table.Name, colIdx: 2}
	b.Run("ColdVectorColumn", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			b.StopTimer()
			vecSearchColumnCacheMu.Lock()
			delete(vecSearchColumnCache, key)
			publishVecSearchColumnCacheSnapshotLocked()
			vecSearchColumnCacheMu.Unlock()
			b.StartTimer()
			run(b)
		}
	})
	b.Run("Repeated", func(b *testing.B) {
		run(b)
		b.ReportAllocs()
		for b.Loop() {
			run(b)
		}
	})
	b.Run("WarmedVectorColumn", func(b *testing.B) {
		getVecColumnCache("default", table, 2, true)
		b.ReportAllocs()
		for b.Loop() {
			run(b)
		}
	})
}
