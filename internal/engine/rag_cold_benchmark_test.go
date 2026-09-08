package engine

import (
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// Called between completed queries, outside benchmark timing. Table data and
// parsed SQL remain available; all corpus-specific FTS structures are cold.
func resetColdFTSBenchmark(table *storage.Table) {
	purgeFTSCachesFor("default", table.Name)
	purgeRAGPreFilterCachesFor(table.Name)
	table.DerivedLock()
	table.FTSIndexes = nil
	table.DerivedUnlock()
}

func BenchmarkRAGHybridColdCache(b *testing.B) {
	db := ragBenchCorpus(b)
	b.Cleanup(func() { _ = db.Close() })
	table, err := db.Get("default", "rag_chunks")
	if err != nil {
		b.Fatal(err)
	}
	qv := ragBenchQueryVector(b)
	stmt := mustParse(`SELECT chunk_id, _rrf_rank FROM HYBRID_SEARCH('rag_chunks', 'embedding', 'search_text',
 'term7 term23 term180 needle42', '` + qv + `',6,'{"candidate_k":24,"index":"flat"}')`)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		resetColdFTSBenchmark(table)
		purgeVectorCachesFor("default", table.Name)
		purgeVecQueryCacheFor("default", table.Name)
		b.StartTimer()
		rows, err := Execute(b.Context(), db, "default", stmt)
		if err != nil {
			b.Fatal(err)
		}
		if len(rows.Rows) != 6 {
			b.Fatalf("got %d hits, want 6", len(rows.Rows))
		}
	}
}

// Isolates cold process-local FTS state with an already loaded persistent
// index. Database file I/O is outside this benchmark.
func BenchmarkRAGFTSRehydrate(b *testing.B) {
	db := ragBenchCorpus(b)
	b.Cleanup(func() { _ = db.Close() })
	table, err := db.Get("default", "rag_chunks")
	if err != nil {
		b.Fatal(err)
	}
	stmt := mustParse(`SELECT chunk_id FROM FTS_SEARCH('rag_chunks','needle42',10,'search_text')`)
	if _, err := Execute(b.Context(), db, "default", stmt); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		purgeFTSCachesFor("default", table.Name)
		b.StartTimer()
		rows, err := Execute(b.Context(), db, "default", stmt)
		if err != nil {
			b.Fatal(err)
		}
		if len(rows.Rows) != 3 {
			b.Fatalf("got %d hits, want 3", len(rows.Rows))
		}
	}
}
