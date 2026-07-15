package engine

import (
	"testing"
	"testing/synctest"
	"time"
)

func TestVecQueryCacheExpiryWithSynctest(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ConfigureVectorCache(VectorCacheConfig{})
		t.Cleanup(func() { ConfigureVectorCache(VectorCacheConfig{}) })
		ConfigureVectorCache(VectorCacheConfig{ResultCacheEntries: 1, ResultCacheTTL: time.Second})

		key := vecQueryCacheKey{tenant: "synctest", table: "cache", version: 1}
		putVecQueryCache(key, []vecScoredRow{{rowIdx: 7, distance: 0.25}})
		if rows, ok := getVecQueryCache(key); !ok || len(rows) != 1 || rows[0].rowIdx != 7 {
			t.Fatalf("cache lookup before expiry = %#v, %v", rows, ok)
		}

		time.Sleep(time.Second)
		if _, ok := getVecQueryCache(key); ok {
			t.Fatal("cache entry survived its TTL")
		}
	})
}
