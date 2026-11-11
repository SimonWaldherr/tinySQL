package engine

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"slices"
	"sync"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// Retain the prior physical-ID build as a topology and result reference.
func legacyPhysicalFilteredANN(ctx context.Context, table *storage.Table, metric string, dims int, cache vecSearchColumnCacheEntry, filter *ragRowFilter) (*vecHNSWIndex, error) {
	idx := &vecHNSWIndex{
		table: table, version: table.Version, structVersion: table.StructVersion(),
		metric: metric, dims: dims, entry: -1, maxLevel: -1,
		levels: make([]int, cache.rowCount()), neighbors: make([][][]int, cache.rowCount()),
	}
	visited := acquireVisited(cache.rowCount())
	defer releaseVisited(visited)
	scratch := acquireHNSWScratch()
	defer releaseHNSWScratch(scratch)
	for pos, row := range filter.rows {
		if pos&1023 == 0 {
			if err := checkCtx(ctx); err != nil {
				return nil, err
			}
		}
		if validCacheRow(cache, row, dims) {
			idx.insertHNSWNode(row, cache, visited, scratch)
		}
	}
	return idx, nil
}

func TestFilteredANNCompactMatchesPhysicalGraph(t *testing.T) {
	for _, metric := range []string{"cosine", "l2", "manhattan", "dot"} {
		t.Run(metric, func(t *testing.T) {
			table, cache, filter := sparseFilteredANNFixture(10000, 96, 8)
			for _, pos := range []int{3, 5, 7, 9, 11} {
				row := filter.rows[pos]
				value := cache.overrides[row]
				switch pos {
				case 3:
					value.vector = value.vector[:7]
				case 5:
					clear(value.vector)
				case 7:
					value.vector[0] = math.NaN()
				case 9:
					value.valid = false
				case 11:
					value.vector = nil
				}
				value.norm = vectorL2Norm(value.vector)
				cache.overrides[row] = value
			}
			idx, err := buildRAGFilteredANNIndex(t.Context(), table, metric, 8, cache, filter)
			if err != nil {
				t.Fatal(err)
			}
			legacy, err := legacyPhysicalFilteredANN(t.Context(), table, metric, 8, cache, filter)
			if err != nil {
				t.Fatal(err)
			}
			if idx.filteredCache == nil || len(idx.levels) != len(filter.rows) || len(idx.neighbors) != len(filter.rows) || idx.maxLevel != legacy.maxLevel || idx.filteredRows[idx.entry] != legacy.entry {
				t.Fatal("selective graph retained corpus-sized headers or changed its entry")
			}
			if &idx.filteredRows[0] == &filter.rows[0] {
				t.Fatal("graph did not own its physical-ID mapping")
			}
			for local, physical := range filter.rows {
				if idx.levels[local] != legacy.levels[physical] || len(idx.neighbors[local]) != len(legacy.neighbors[physical]) {
					t.Fatalf("node %d changed its physical-ID level", physical)
				}
				for layer, links := range idx.neighbors[local] {
					mapped := make([]int, len(links))
					for i, neighbor := range links {
						mapped[i] = idx.filteredRows[neighbor]
					}
					if !slices.Equal(mapped, legacy.neighbors[physical][layer]) {
						t.Fatalf("node %d layer %d changed neighbors: %v, want %v", physical, layer, mapped, legacy.neighbors[physical][layer])
					}
				}
				vec, _, _ := idx.filteredCache.resolveRow(local, false)
				if len(vec) != 0 && &vec[0] != &cache.overrides[physical].vector[0] {
					t.Fatal("compact cache copied a vector payload")
				}
			}
			queries := [][]float64{make([]float64, 8), {math.NaN(), 0, 0, 0, 0, 0, 0, 0}, {1, 2, 3, 4, 5, 6, 7, 8}, {-1, 2, -3, 4, -5, 6, -7, 8}}
			for _, query := range queries {
				for _, k := range []int{0, 1, 20, 80, 200} {
					got, gotErr := idx.searchFiltered(t.Context(), query, vectorL2Norm(query), k, cache, filter)
					want, wantErr := legacyFilteredANNSelection(t.Context(), legacy, query, vectorL2Norm(query), k, cache, filter)
					if fmt.Sprint(gotErr) != fmt.Sprint(wantErr) || !reflect.DeepEqual(got, want) {
						t.Fatalf("k=%d query=%v: got %v/%v, want %v/%v", k, query, got, gotErr, want, wantErr)
					}
				}
			}
		})
	}
}

func TestFilteredANNCompactEligibilityAndCancellation(t *testing.T) {
	table, cache, _ := sparseFilteredANNFixture(16, 4, 2)
	for _, rows := range [][]int{{0, 4, 8, 12, 15}, {4, 0}, {0, 0}, {-1}, {16}} {
		local, _, err := compactRAGFilterCache(t.Context(), cache, rows, false)
		if err != nil || local != nil {
			t.Fatalf("non-selective or non-canonical filter %v used local IDs: %v", rows, err)
		}
	}
	for _, rows := range [][]int{nil, {}, {0, 4, 8, 12}} {
		idx, err := buildRAGFilteredANNIndex(t.Context(), table, "l2", 2, cache, &ragRowFilter{rows: rows})
		if err != nil || idx.filteredCache == nil || len(idx.levels) != len(rows) {
			t.Fatalf("selective filter %v was not compact: %v", rows, err)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := buildRAGFilteredANNIndex(ctx, table, "l2", 2, cache, &ragRowFilter{rows: []int{0}}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled compact build returned %v", err)
	}
	idx, err := buildRAGFilteredANNIndex(t.Context(), table, "l2", 2, cache, &ragRowFilter{rows: []int{0}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := idx.searchFiltered(ctx, []float64{1, 2}, 0, 1, cache, &ragRowFilter{rows: []int{0}}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled compact search returned %v", err)
	}
}

func TestFilteredANNCompactPublicPermissionsMutationAndConcurrency(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	table := storage.NewTable("compact_acl", []storage.Column{{Name: "id", Type: storage.IntType}, {Name: "scope", Type: storage.TextType}, {Name: "embedding", Type: storage.VectorType}}, false)
	for id := range 4 * vecSearchParallelMinRows {
		scope, vector := "blocked", []float64{0, 0}
		if id%4 == 0 {
			scope, vector = "allowed", []float64{float64(id/8 + 1), 0}
		}
		table.Rows = append(table.Rows, []any{id, scope, vector})
	}
	if err := db.Put("default", table); err != nil {
		t.Fatal(err)
	}
	query := `SELECT id,_vec_rank,_vec_distance FROM RAG_SEARCH('compact_acl','embedding','[0,0]',3,
		'{"metric":"l2","index":"hnsw","pre_filter":{"equals":{"scope":"allowed"}}}')`
	want := []Row{{"id": 0, "_vec_rank": 1, "_vec_distance": float64(1)}, {"id": 4, "_vec_rank": 2, "_vec_distance": float64(1)}, {"id": 8, "_vec_rank": 3, "_vec_distance": float64(2)}}
	for range 2 {
		got := execSQL(t, db, query)
		if !reflect.DeepEqual(got.Rows, want) {
			t.Fatalf("compact ACL result = %v, want %v", got.Rows, want)
		}
		got.Rows[0]["id"] = -1
	}
	execSQL(t, db, `UPDATE compact_acl SET embedding='[10000,0]' WHERE id=0`)
	want = []Row{{"id": 4, "_vec_rank": 1, "_vec_distance": float64(1)}, {"id": 8, "_vec_rank": 2, "_vec_distance": float64(2)}, {"id": 12, "_vec_rank": 3, "_vec_distance": float64(2)}}
	var group sync.WaitGroup
	errs := make(chan error, 8)
	for range 8 {
		group.Go(func() {
			stmt, err := NewParser(query).ParseStatement()
			if err != nil {
				errs <- err
				return
			}
			got, err := Execute(t.Context(), db, "default", stmt)
			if err != nil || !reflect.DeepEqual(got.Rows, want) {
				errs <- fmt.Errorf("concurrent compact ACL result = %v/%v, want %v", got, err, want)
			}
		})
	}
	group.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	// DELETE shifts physical positions. Restore the eligible subset size with
	// an append, then verify local node IDs still materialize the right rows.
	execSQL(t, db, `DELETE FROM compact_acl WHERE id=4`)
	execSQL(t, db, `INSERT INTO compact_acl VALUES (16384,'allowed','[20000,0]')`)
	want = []Row{{"id": 8, "_vec_rank": 1, "_vec_distance": float64(2)}, {"id": 12, "_vec_rank": 2, "_vec_distance": float64(2)}, {"id": 16, "_vec_rank": 3, "_vec_distance": float64(3)}}
	if got := execSQL(t, db, query); !reflect.DeepEqual(got.Rows, want) {
		t.Fatalf("compact ACL after delete/append = %v, want %v", got.Rows, want)
	}
}

func TestFilteredANNCompactRetiresOldCacheGenerations(t *testing.T) {
	const tenant = "compact-generations"
	table, cache, filter := sparseFilteredANNFixture(64, 16, 2)
	t.Cleanup(func() { purgeRAGFilteredANNCachesFor(tenant, table.Name) })
	old, err := getRAGFilteredANNIndex(t.Context(), tenant, table, 0, "l2", 2, filter, cache)
	if err != nil {
		t.Fatal(err)
	}
	physical := filter.rows[0]
	oldVector := old.filteredCache.vector(0)
	want := append([]float64(nil), oldVector...)
	cache.overrides = make(map[int]vecColumnOverride, len(filter.rows))
	for local, row := range filter.rows {
		vector := old.filteredCache.vector(local)
		if row == physical {
			vector = []float64{10000, 10000}
		}
		cache.overrides[row] = vecColumnOverride{vector: vector, norm: vectorL2Norm(vector), valid: true}
	}
	table.Version++
	cache.version = table.Version
	current, err := getRAGFilteredANNIndex(t.Context(), tenant, table, 0, "l2", 2, filter, cache)
	if err != nil || current == old || !slices.Equal(old.filteredCache.vector(0), want) || current.filteredCache.vector(0)[0] != 10000 {
		t.Fatalf("replacement reused or mutated the old graph: %v", err)
	}
	ragFilteredANNCache.RLock()
	defer ragFilteredANNCache.RUnlock()
	for key, idx := range ragFilteredANNCache.entries {
		if key.tenant == tenant && key.table == table && (key.version != table.Version || idx != current) {
			t.Fatal("obsolete graph generation still pins its selected payloads")
		}
	}
}

type filteredANNVersionContext struct {
	context.Context
	table *storage.Table
	once  sync.Once
}

func (c *filteredANNVersionContext) Done() <-chan struct{} {
	// Change the version synchronously at the first build cancellation check,
	// after its publication key was captured. No timer or concurrent mutation.
	c.once.Do(func() { c.table.Version++ })
	return c.Context.Done()
}

func TestFilteredANNCompactDoesNotPublishSupersededBuild(t *testing.T) {
	const tenant = "compact-superseded"
	table, cache, filter := sparseFilteredANNFixture(64, 16, 2)
	t.Cleanup(func() { purgeRAGFilteredANNCachesFor(tenant, table.Name) })
	ctx := &filteredANNVersionContext{Context: t.Context(), table: table}
	idx, err := getRAGFilteredANNIndex(ctx, tenant, table, 0, "l2", 2, filter, cache)
	if err != nil || idx == nil {
		t.Fatalf("private completed build was lost: %v", err)
	}
	if idx.version != cache.version {
		t.Fatal("private graph no longer describes its captured source generation")
	}
	ragFilteredANNCache.RLock()
	defer ragFilteredANNCache.RUnlock()
	for key := range ragFilteredANNCache.entries {
		if key.tenant == tenant && key.table == table {
			t.Fatal("superseded completed build was republished")
		}
	}
	for key := range ragFilteredANNCache.builds {
		if key.tenant == tenant && key.table == table {
			t.Fatal("completed private build remains registered as in flight")
		}
	}
}
