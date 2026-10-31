package engine

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func enableIsolatedVectorResultCache(t *testing.T) {
	t.Helper()
	ConfigureVectorCache(VectorCacheConfig{})
	ConfigureVectorCache(VectorCacheConfig{ResultCacheEntries: 8, ResultCacheTTL: time.Minute})
	t.Cleanup(func() { ConfigureVectorCache(VectorCacheConfig{}) })
}

func TestVecQueryCacheIsolatesDatabaseInstances(t *testing.T) {
	for _, index := range []string{"flat", "ivf", "hnsw"} {
		t.Run(index, func(t *testing.T) {
			enableIsolatedVectorResultCache(t)
			a, b := storage.NewDB(), storage.NewDB()
			t.Cleanup(func() { _ = a.Close(); _ = b.Close() })
			for _, db := range []*storage.DB{a, b} {
				execSQL(t, db, `CREATE TABLE isolated_vectors (id INT, embedding VECTOR)`)
			}
			execSQL(t, a, `INSERT INTO isolated_vectors VALUES (1,'[1,0]'),(2,'[0,1]')`)
			execSQL(t, b, `INSERT INTO isolated_vectors VALUES (1,'[0,1]'),(2,'[1,0]')`)
			ta, _ := a.Get("default", "isolated_vectors")
			tb, _ := b.Get("default", "isolated_vectors")
			if ta.Version != tb.Version {
				t.Fatal("fixture must collide on table name and version")
			}
			query := fmt.Sprintf(`SELECT id,_vec_distance AS distance FROM VEC_SEARCH('isolated_vectors','embedding','[1,0]',1,'cosine','%s')`, index)
			for _, tc := range []struct {
				db *storage.DB
				id int
			}{{a, 1}, {b, 2}, {a, 1}} {
				rs := execSQL(t, tc.db, query)
				if len(rs.Rows) != 1 || rs.Rows[0]["id"] != tc.id || rs.Rows[0]["distance"] != float64(0) {
					t.Fatalf("database %p: %v, want id=%d distance=0", tc.db, rs.Rows, tc.id)
				}
			}
			stats := VectorCacheAnalytics()
			if stats.Misses != 2 || stats.Hits != 1 || stats.Entries != 2 {
				t.Fatalf("database-local cache entries: %+v", stats)
			}
		})
	}
}

func TestVecQueryCacheIsolatesReplacedTable(t *testing.T) {
	enableIsolatedVectorResultCache(t)
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	execSQL(t, db, `CREATE TABLE replaced_vectors (id INT, embedding VECTOR)`)
	execSQL(t, db, `INSERT INTO replaced_vectors VALUES (1,'[1,0]'),(2,'[0,1]')`)
	query := `SELECT id FROM VEC_SEARCH('replaced_vectors','embedding','[1,0]',1)`
	if rs := execSQL(t, db, query); rs.Rows[0]["id"] != 1 {
		t.Fatal(rs.Rows)
	}
	old, _ := db.Get("default", "replaced_vectors")
	replacement := storage.NewTable(old.Name, old.Cols, false)
	replacement.Rows = [][]any{{1, []float64{0, 1}}, {2, []float64{1, 0}}}
	replacement.Version = old.Version
	// Replace is also used for refreshed materialized tables. It has no SQL
	// DROP hook, so identity must invalidate an equal-version cache entry.
	if err := db.Replace("default", replacement); err != nil {
		t.Fatal(err)
	}
	if rs := execSQL(t, db, query); rs.Rows[0]["id"] != 2 {
		t.Fatalf("replaced table reused old row IDs: %v", rs.Rows)
	}
	if stats := VectorCacheAnalytics(); stats.Misses != 2 || stats.Hits != 0 {
		t.Fatalf("replacement must miss the result cache: %+v", stats)
	}
}

func TestVecQueryCachePurgeAfterRecreateAndRollback(t *testing.T) {
	enableIsolatedVectorResultCache(t)
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	execSQL(t, db, `CREATE TABLE purged_vectors (id INT PRIMARY KEY, embedding VECTOR)`)
	execSQL(t, db, `INSERT INTO purged_vectors VALUES (1,'[1,0]'),(2,'[0,1]')`)
	query := `SELECT id FROM VEC_SEARCH('purged_vectors','embedding','[1,0]',1)`
	execSQL(t, db, query)
	table, _ := db.Get("default", "purged_vectors")
	version := table.Version
	_, err := Execute(t.Context(), db, "default", mustParse(`INSERT INTO purged_vectors VALUES (1,'[0,1]')`))
	if err == nil || !strings.Contains(err.Error(), "PRIMARY KEY") {
		t.Fatalf("expected rejected INSERT: %v", err)
	}
	restored, _ := db.Get("default", "purged_vectors")
	if restored != table || restored.Version != version {
		t.Fatal("fixture must restore the same table identity and version")
	}
	if stats := VectorCacheAnalytics(); stats.Entries != 0 {
		t.Fatalf("rollback retained cached results: %+v", stats)
	}
	if rs := execSQL(t, db, query); rs.Rows[0]["id"] != 1 {
		t.Fatal(rs.Rows)
	}
	execSQL(t, db, `DROP TABLE purged_vectors`)
	if stats := VectorCacheAnalytics(); stats.Entries != 0 {
		t.Fatalf("DROP retained cached results: %+v", stats)
	}
	execSQL(t, db, `CREATE TABLE purged_vectors (id INT PRIMARY KEY, embedding VECTOR)`)
	execSQL(t, db, `INSERT INTO purged_vectors VALUES (1,'[0,1]'),(2,'[1,0]')`)
	if rs := execSQL(t, db, query); rs.Rows[0]["id"] != 2 {
		t.Fatalf("recreated table reused old row IDs: %v", rs.Rows)
	}
}
