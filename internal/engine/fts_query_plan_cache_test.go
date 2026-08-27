package engine

import (
	"context"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestFTSPreparedQueryCacheReusesAndInvalidatesCorpusPlan(t *testing.T) {
	const (
		tenant = "fts-prepared-cache-test"
		name   = "docs"
		query  = "alpha OR bet*"
	)
	db := storage.NewDB()
	table := storage.NewTable(name, []storage.Column{{Name: "body", Type: storage.TextType}}, false)
	table.Rows = [][]any{{"alpha document"}, {"beta document"}, {"other document"}}
	table.Version++
	if err := db.Put(tenant, table); err != nil {
		t.Fatal(err)
	}
	purgeFTSCachesFor(tenant, name)
	t.Cleanup(func() { purgeFTSCachesFor(tenant, name) })

	ctx := context.Background()
	results, err := ftsSearchCandidates(ctx, tenant, table, query, 10, []int{0})
	if err != nil {
		t.Fatalf("first FTS search: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("first FTS search returned %d rows, want 2", len(results))
	}

	key := ftsPreparedQueryCacheKey{doc: ftsDocCacheKey{tenant: tenant, table: name, cols: "0"}, query: query}
	ftsPreparedQueryCacheMu.RLock()
	first, ok := ftsPreparedQueryCache[key]
	ftsPreparedQueryCacheMu.RUnlock()
	if !ok || first.node == nil {
		t.Fatal("first FTS search did not publish a prepared query plan")
	}

	if _, err := ftsSearchCandidates(ctx, tenant, table, query, 1, []int{0}); err != nil {
		t.Fatalf("cached FTS search: %v", err)
	}
	ftsPreparedQueryCacheMu.RLock()
	second := ftsPreparedQueryCache[key]
	ftsPreparedQueryCacheMu.RUnlock()
	if second.node != first.node || second.version != first.version {
		t.Fatalf("prepared query was rebuilt without a table change: first=%p/%d second=%p/%d", first.node, first.version, second.node, second.version)
	}

	// A version change must not reuse BM25 weights or candidate postings from
	// the prior corpus snapshot, even when the table name and query are equal.
	table.Rows = append(table.Rows, []any{"alpha added after warmup"})
	table.Version++
	if _, err := ftsSearchCandidates(ctx, tenant, table, query, 10, []int{0}); err != nil {
		t.Fatalf("FTS search after mutation: %v", err)
	}
	ftsPreparedQueryCacheMu.RLock()
	third := ftsPreparedQueryCache[key]
	ftsPreparedQueryCacheMu.RUnlock()
	if third.version != table.Version || third.node == first.node {
		t.Fatalf("prepared query cache was not rebuilt after version change: first=%p/%d third=%p/%d", first.node, first.version, third.node, third.version)
	}
}

func BenchmarkFTSPreparedQueryPlanWarm(b *testing.B) {
	db := ragBenchCorpus(b)
	table, err := db.Get("default", "rag_chunks")
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	const query = "term7 OR term23 OR term180 OR needle42"
	if _, err := ftsSearchCandidates(ctx, "default", table, query, 24, []int{5}); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := ftsSearchCandidates(ctx, "default", table, query, 24, []int{5}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFTSPreparedQueryPlanCold(b *testing.B) {
	db := ragBenchCorpus(b)
	table, err := db.Get("default", "rag_chunks")
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	const query = "term7 OR term23 OR term180 OR needle42"
	if _, err := ftsSearchCandidates(ctx, "default", table, query, 24, []int{5}); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		purgeFTSPreparedQueryCachesFor("default", table.Name)
		b.StartTimer()
		if _, err := ftsSearchCandidates(ctx, "default", table, query, 24, []int{5}); err != nil {
			b.Fatal(err)
		}
	}
}
