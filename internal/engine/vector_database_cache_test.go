package engine

import (
	"context"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestVectorCachesRetainIndependentDatabaseEntries(t *testing.T) {
	// Reuse is guaranteed below capacity, not after legitimate eviction.
	// Leave room for both databases even when earlier tests filled the caches.
	vecSearchColumnCacheMu.Lock()
	evictOverCap(vecSearchColumnCache, vecColumnCacheMaxEntries-2)
	publishVecSearchColumnCacheSnapshotLocked()
	vecSearchColumnCacheMu.Unlock()
	vecIVFCacheMu.Lock()
	evictOverCap(vecIVFCache, vecIndexCacheMaxEntries-2)
	publishVecIVFCacheSnapshotLocked()
	vecIVFCacheMu.Unlock()
	vecHNSWCacheMu.Lock()
	evictOverCap(vecHNSWCache, vecIndexCacheMaxEntries-2)
	vecHNSWCacheMu.Unlock()
	a, b := makeVecSearchBenchmarkTable(192, 16), makeVecSearchBenchmarkTable(192, 16)
	t.Cleanup(func() { _ = a.Close(); _ = b.Close() })
	ta, _ := a.Get("default", "vec_docs")
	tb, _ := b.Get("default", "vec_docs")
	firstA := getVecColumnCache("default", ta, 1, true)
	firstB := getVecColumnCache("default", tb, 1, true)
	for _, tc := range []struct {
		table *storage.Table
		first vecSearchColumnCacheEntry
	}{{ta, firstA}, {tb, firstB}, {ta, firstA}} {
		cached := getVecColumnCache("default", tc.table, 1, true)
		if &cached.segments[0].data[0] != &tc.first.segments[0].data[0] {
			t.Fatalf("database-local vector buffer for %p was rebuilt", tc.table)
		}
	}
	for _, index := range []string{"ivf", "hnsw"} {
		t.Run(index, func(t *testing.T) {
			get := func(table *storage.Table) any {
				cache := getVecColumnCache("default", table, 1, true)
				if index == "ivf" {
					idx, err := getVecIVFIndex(t.Context(), "default", table, 1, "cosine", 16, cache)
					if err != nil {
						t.Fatal(err)
					}
					return idx
				}
				idx, err := getVecHNSWIndex(t.Context(), "default", table, 1, "cosine", 16, cache)
				if err != nil {
					t.Fatal(err)
				}
				return idx
			}
			firstA, firstB := get(ta), get(tb)
			if get(ta) != firstA || get(tb) != firstB || firstA == firstB {
				t.Fatal("independent databases replaced each other's warm index")
			}
		})
	}
}

// Candidate ranking is the stage reused by RAG before late materialization.
func BenchmarkVecSearchDatabaseReuse(b *testing.B) {
	ConfigureVectorCache(VectorCacheConfig{})
	for _, mode := range []string{"flat", "ivf", "hnsw"} {
		for _, alternating := range []bool{false, true} {
			name := "one_database"
			if alternating {
				name = "alternating_databases"
			}
			b.Run(mode+"/"+name, func(b *testing.B) {
				const rows, dims = 1024, 32
				dbs := []*storage.DB{makeVecSearchBenchmarkTable(rows, dims), makeVecSearchBenchmarkTable(rows, dims)}
				b.Cleanup(func() {
					for _, db := range dbs {
						_ = db.Close()
					}
				})
				query := make([]float64, dims)
				for i := range query {
					query[i] = float64(i%11+1) / 11
				}
				args := vecSearchArgs{tableName: "vec_docs", colName: "embedding", queryVec: query, k: 20, metric: "cosine", indexMode: mode}
				envs := []ExecEnv{{ctx: context.Background(), db: dbs[0], tenant: "default"}, {ctx: context.Background(), db: dbs[1], tenant: "default"}}
				for _, i := range []int{0, 1, 0} {
					if _, _, err := vecSearchCandidates(context.Background(), envs[i], args); err != nil {
						b.Fatal(err)
					}
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					db := 0
					if alternating {
						db = i % 2
					}
					_, results, err := vecSearchCandidates(context.Background(), envs[db], args)
					if err != nil || len(results) != args.k {
						b.Fatalf("results=%d err=%v", len(results), err)
					}
				}
			})
		}
	}
}
