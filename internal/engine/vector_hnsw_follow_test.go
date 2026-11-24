package engine

import (
	"context"
	"fmt"
	"math/rand"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// Tests of the HNSW removal pass (vector_hnsw_follow.go): the graph of a table
// that lost rows must stay a sound graph over the surviving rows, keep its recall,
// never return a dead row, and never be served stale after a reopen.

const (
	hfDims      = 32
	hfEmbedding = 2 // column index of hf.embedding
	hfPerSource = 8
)

// hfFixture is a table hf(id, src, embedding) of clustered embeddings in which
// source s owns the ids s*8 .. s*8+7, together with its database and queries.
type hfFixture struct {
	t       *testing.T
	db      *storage.DB
	table   *storage.Table
	metric  string
	queries [][]float64
	rng     *rand.Rand
	live    []int          // sources that still exist
	deleted map[int]bool   // ids that were deleted
	vectors map[int]string // id -> vector literal of rows added by add
	nextID  int
}

func newHF(t *testing.T, rows int, metric string) *hfFixture {
	t.Helper()
	source, queries := hnswRecallCorpus(rows, hfDims, 60)
	return newHFFrom(t, source, queries, rows, metric)
}

// newHFDiffuse is a table of isotropic Gaussian vectors, the hard case for a graph:
// no clusters, so every neighbourhood is as large as it gets.
func newHFDiffuse(t *testing.T, rows int, metric string) *hfFixture {
	t.Helper()
	rng := rand.New(rand.NewSource(99))
	draw := func() []float64 {
		v := make([]float64, hfDims)
		for i := range v {
			v[i] = rng.NormFloat64()
		}
		return v
	}
	source := storage.NewTable("hf_source", []storage.Column{{Name: "id", Type: storage.IntType}, {Name: "embedding", Type: storage.VectorType}}, false)
	for i := 0; i < rows; i++ {
		source.Rows = append(source.Rows, []any{i, draw()})
	}
	queries := make([][]float64, 60)
	for i := range queries {
		queries[i] = draw()
	}
	return newHFFrom(t, source, queries, rows, metric)
}

func newHFFrom(t *testing.T, source *storage.Table, queries [][]float64, rows int, metric string) *hfFixture {
	t.Helper()
	table := storage.NewTable("hf", []storage.Column{
		{Name: "id", Type: storage.IntType},
		{Name: "src", Type: storage.IntType},
		{Name: "embedding", Type: storage.VectorType},
	}, false)
	f := &hfFixture{t: t, db: storage.NewDB(), table: table, metric: metric, queries: queries,
		rng: rand.New(rand.NewSource(int64(rows)*31 + 5)), deleted: map[int]bool{}, nextID: rows}
	for i, row := range source.Rows {
		table.Rows = append(table.Rows, []any{i, i / hfPerSource, row[1]})
		if i%hfPerSource == 0 {
			f.live = append(f.live, i/hfPerSource)
		}
	}
	table.Version = 1
	if err := f.db.Put("default", table); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { purgeVectorCachesFor("default", "hf"); _ = f.db.Close() })
	return f
}

func (f *hfFixture) exec(sql string) *ResultSet {
	f.t.Helper()
	return execSQL(f.t, f.db, sql)
}

func (f *hfFixture) cache() vecSearchColumnCacheEntry {
	return getVecColumnCache("default", f.table, hfEmbedding, metricNeedsNorms(f.metric))
}

// warm brings the cache and the graph up to date the way a search does.
func (f *hfFixture) warm() (*vecHNSWIndex, vecSearchColumnCacheEntry) {
	f.t.Helper()
	cache := f.cache()
	idx, err := getVecHNSWIndex(context.Background(), "default", f.table, hfEmbedding, f.metric, hfDims, cache)
	if err != nil {
		f.t.Fatal(err)
	}
	return idx, cache
}

// cached returns the graph of the cache without touching it.
func (f *hfFixture) cached() *vecHNSWIndex {
	key := vecIndexCacheKey{source: f.table, tenant: "default", table: "hf", colIdx: hfEmbedding, metric: f.metric}
	vecHNSWCacheMu.RLock()
	defer vecHNSWCacheMu.RUnlock()
	return vecHNSWCache[key]
}

// deleteSources removes n random sources with one statement.
func (f *hfFixture) deleteSources(n int) {
	f.t.Helper()
	n = min(n, len(f.live))
	var list []string
	for i := 0; i < n; i++ {
		at := f.rng.Intn(len(f.live))
		src := f.live[at]
		f.live = slices.Delete(f.live, at, at+1)
		list = append(list, fmt.Sprint(src))
		for id := src * hfPerSource; id < (src+1)*hfPerSource; id++ {
			f.deleted[id] = true
		}
	}
	f.exec(`DELETE FROM hf WHERE src IN (` + strings.Join(list, ",") + `)`)
}

// follow runs the removal pass directly (without the thresholds that make
// getVecHNSWIndex rebuild) on the cached graph, the way getVecHNSWIndex would.
func (f *hfFixture) follow() (*vecHNSWIndex, vecSearchColumnCacheEntry) {
	f.t.Helper()
	cache := f.cache()
	idx := f.cached()
	changes, ok := f.table.ChangesSince(idx.structVersion, len(idx.levels), storage.ColumnBit(hfEmbedding))
	if !ok || len(changes.Dead) == 0 {
		f.t.Fatalf("the change log reports no removal: ok=%v dead=%d", ok, len(changes.Dead))
	}
	followed, err := followVecHNSWIndex(context.Background(), idx, f.table, cache, changes)
	if err != nil || !followed {
		f.t.Fatalf("follow failed: followed=%v err=%v", followed, err)
	}
	return idx, cache
}

// hnswCheckInvariants verifies that idx is a sound graph over the rows of table.
func hnswCheckInvariants(t *testing.T, idx *vecHNSWIndex, table *storage.Table, cache vecSearchColumnCacheEntry, label string) {
	t.Helper()
	fail := func(format string, args ...any) {
		t.Helper()
		t.Fatalf("%s: "+format, append([]any{label}, args...)...)
	}
	n := len(table.Rows)
	if len(idx.levels) != n || len(idx.neighbors) != n {
		fail("graph covers %d/%d rows, table has %d", len(idx.levels), len(idx.neighbors), n)
	}
	if idx.version != table.Version || idx.structVersion != table.StructVersion() {
		fail("stamped %d/%d, table is at %d/%d", idx.version, idx.structVersion, table.Version, table.StructVersion())
	}
	rows := newVecRowResolver(&cache, metricNeedsNorms(idx.metric))
	scorable := func(i int) bool {
		vector, norm, ok := rows.resolve(i)
		return ok && len(vector) == idx.dims && !(rows.needNorm && norm == 0)
	}
	topLevel := -1
	for i := 0; i < n; i++ {
		layers := idx.neighbors[i]
		if len(layers) == 0 {
			continue // not a node of the graph
		}
		if len(layers) != idx.levels[i]+1 {
			fail("node %d has %d layers, level %d", i, len(layers), idx.levels[i])
		}
		if scorable(i) {
			topLevel = max(topLevel, idx.levels[i])
		}
		for layer, links := range layers {
			if len(links) > hnswMaxNeighbors(layer) {
				fail("node %d layer %d has %d links", i, layer, len(links))
			}
			seen := make(map[int]bool, len(links))
			for _, nb := range links {
				switch {
				case nb < 0 || nb >= n:
					fail("node %d layer %d links to %d, out of range (%d rows)", i, layer, nb, n)
				case nb == i:
					fail("node %d links to itself on layer %d", i, layer)
				case seen[nb]:
					fail("node %d links to %d twice on layer %d", i, nb, layer)
				case len(idx.neighbors[nb]) <= layer:
					fail("node %d layer %d links to %d, which has no such layer", i, layer, nb)
				}
				if _, delta := idx.deltaSet[nb]; !delta && !scorable(nb) {
					fail("node %d layer %d links to %d, which is not a live vector", i, layer, nb)
				}
				seen[nb] = true
			}
		}
	}
	if topLevel < 0 {
		if idx.entry != -1 || idx.maxLevel != -1 {
			fail("graph without nodes has entry %d, maxLevel %d", idx.entry, idx.maxLevel)
		}
	} else {
		if idx.entry < 0 || idx.entry >= n || len(idx.neighbors[idx.entry]) == 0 || idx.levels[idx.entry] != idx.maxLevel {
			fail("entry %d (maxLevel %d) is not a node at the top level", idx.entry, idx.maxLevel)
		}
		if topLevel > idx.maxLevel {
			fail("a node has level %d above maxLevel %d", topLevel, idx.maxLevel)
		}
	}
	if len(idx.deltaSet) != len(idx.deltaRows) {
		fail("%d delta rows, %d in the set", len(idx.deltaRows), len(idx.deltaSet))
	}
	for _, row := range idx.deltaRows {
		if _, ok := idx.deltaSet[row]; !ok || row < 0 || row >= n {
			fail("delta row %d is not in the set or out of range", row)
		}
	}
}

// hnswRecall measures recall@k of idx against the exact flat scan and checks that
// no dead row is returned.
func (f *hfFixture) recall(idx *vecHNSWIndex, cache vecSearchColumnCacheEntry, k int) float64 {
	f.t.Helper()
	hits, total := 0, 0
	for _, query := range f.queries {
		norm := vectorL2Norm(query)
		distFn := buildVecDistanceFunc(f.metric, query, norm)
		exact, err := vecSearchTopK(context.Background(), f.table.Rows, hfDims, k, cache, distFn, vecDistanceFuncNeedsNorm(f.metric))
		if err != nil {
			f.t.Fatal(err)
		}
		approx, err := idx.search(context.Background(), query, norm, k, cache)
		if err != nil {
			f.t.Fatal(err)
		}
		want := make(map[int]bool, len(exact))
		for _, r := range exact {
			want[r.rowIdx] = true
		}
		for _, r := range approx {
			if r.rowIdx < 0 || r.rowIdx >= len(f.table.Rows) {
				f.t.Fatalf("search returned row %d of %d", r.rowIdx, len(f.table.Rows))
			}
			if id := f.table.Rows[r.rowIdx][0].(int); f.deleted[id] {
				f.t.Fatalf("search returned the deleted id %d", id)
			}
			if want[r.rowIdx] {
				hits++
			}
		}
		total += len(exact)
	}
	return float64(hits) / float64(total)
}

func (f *hfFixture) freshGraph() (*vecHNSWIndex, vecSearchColumnCacheEntry) {
	f.t.Helper()
	cache := buildVecColumnCache(f.table, hfEmbedding, metricNeedsNorms(f.metric))
	idx, err := buildVecHNSWIndex(context.Background(), f.table, f.metric, hfDims, cache)
	if err != nil {
		f.t.Fatal(err)
	}
	return idx, cache
}

func hfLiteral(v []float64) string {
	parts := make([]string, len(v))
	for i, x := range v {
		parts[i] = fmt.Sprintf("%.5f", x)
	}
	return "[" + strings.Join(parts, ",") + "]"
}

// A removal keeps the graph sound at every size of removal and for both metrics:
// several statements with a pass in between, as the re-import loop produces them.
func TestVecHNSWFollowKeepsTheGraphSound(t *testing.T) {
	for _, metric := range []string{"cosine", "l2"} {
		for _, percent := range []int{1, 5, 20, 60} {
			t.Run(fmt.Sprintf("%s/%dpercent", metric, percent), func(t *testing.T) {
				f := newHF(t, 1600, metric)
				f.warm()
				per := len(f.live) * percent / 100 / 4
				for step := 0; step < 4; step++ {
					f.deleteSources(max(1, per))
					idx, cache := f.follow()
					hnswCheckInvariants(t, idx, f.table, cache, fmt.Sprintf("after statement %d", step+1))
					f.recall(idx, cache, 10)
				}
			})
		}
	}
}

// Recall of the followed graph against a graph built from scratch over the same
// rows (which is what every removal produced before the removal pass existed).
func TestVecHNSWFollowRecall(t *testing.T) {
	percents := []int{1, 3, 5, 10, 20}
	if testing.Short() {
		percents = []int{3, 10}
	}
	for _, metric := range []string{"cosine", "l2"} {
		for _, percent := range percents {
			t.Run(fmt.Sprintf("%s/%dpercent", metric, percent), func(t *testing.T) {
				f := newHF(t, 6000, metric)
				f.warm()
				statements := 4
				per := max(1, len(f.live)*percent/100/statements)
				for step := 0; step < statements; step++ {
					f.deleteSources(per)
					// A pass and a search after each statement, as an application runs them.
					idx, cache := f.follow()
					f.recall(idx, cache, 10)
				}
				followed, followedCache := f.cached(), f.cache()
				hnswCheckInvariants(t, followed, f.table, followedCache, "followed")
				fresh, freshCache := f.freshGraph()
				got, want := f.recall(followed, followedCache, 10), f.recall(fresh, freshCache, 10)
				t.Logf("%s, %d%% of the rows removed in %d statements: recall@10 followed %.4f, fresh build %.4f", metric, percent, statements, got, want)
				if got < want-0.04 || got < 0.9 {
					t.Errorf("recall@10 of the followed graph is %.4f against %.4f for a fresh build", got, want)
				}
			})
		}
	}
}

// The same on isotropic Gaussian vectors, where a fresh graph itself is far from
// perfect and every dropped edge counts for more.
func TestVecHNSWFollowRecallOnDiffuseVectors(t *testing.T) {
	percents := []int{1, 5, 10, 20}
	if testing.Short() {
		percents = []int{5}
	}
	for _, percent := range percents {
		t.Run(fmt.Sprintf("%dpercent", percent), func(t *testing.T) {
			f := newHFDiffuse(t, 6000, "cosine")
			f.warm()
			statements := 4
			per := max(1, len(f.live)*percent/100/statements)
			for step := 0; step < statements; step++ {
				f.deleteSources(per)
				f.follow()
			}
			followed, followedCache := f.cached(), f.cache()
			hnswCheckInvariants(t, followed, f.table, followedCache, "followed")
			fresh, freshCache := f.freshGraph()
			got, want := f.recall(followed, followedCache, 10), f.recall(fresh, freshCache, 10)
			followedReach, followedNodes := hnswReachable(followed)
			freshReach, freshNodes := hnswReachable(fresh)
			t.Logf("diffuse, %d%% of the rows removed in %d statements: reachable from the entry followed %d/%d, fresh %d/%d", percent, statements, followedReach, followedNodes, freshReach, freshNodes)
			if float64(followedReach) < 0.99*float64(followedNodes) {
				t.Errorf("only %d of %d nodes of the followed graph are reachable", followedReach, followedNodes)
			}
			t.Logf("diffuse, %d%% of the rows removed in %d statements: recall followed %.4f, fresh build %.4f", percent, statements, got, want)
			if got < want-0.06 {
				t.Errorf("recall of the followed graph is %.4f against %.4f for a fresh build", got, want)
			}
		})
	}
}

// Rows appended after a removal join the followed graph and are found.
func TestVecHNSWFollowThenAppend(t *testing.T) {
	f := newHF(t, 1200, "cosine")
	f.warm()
	followsBefore := vecHNSWFollowCount.Load()
	f.deleteSources(7)
	// Append rows that are copies of rows of sources that still exist, shifted.
	var added []int
	for k := 0; k < 40; k++ {
		vector := slices.Clone(f.table.Rows[f.rng.Intn(len(f.table.Rows))][hfEmbedding].([]float64))
		vector[k%hfDims] += 3
		f.exec(fmt.Sprintf(`INSERT INTO hf VALUES (%d, %d, '%s')`, f.nextID, 9000+k/hfPerSource, hfLiteral(vector)))
		added = append(added, f.nextID)
		f.nextID++
	}
	idx, cache := f.warm()
	hnswCheckInvariants(t, idx, f.table, cache, "removal and appends")
	if vecHNSWFollowCount.Load() == followsBefore {
		t.Fatal("the removal was not followed")
	}
	for _, id := range added {
		row := len(f.table.Rows) - len(added) + (id - added[0])
		query := f.table.Rows[row][hfEmbedding].([]float64)
		hits, err := idx.search(context.Background(), query, vectorL2Norm(query), 3, cache)
		if err != nil || len(hits) == 0 || hits[0].rowIdx != row {
			t.Fatalf("appended row %d (id %d) was not its own nearest neighbour: %v %v", row, id, hits, err)
		}
	}
}

// A vector that was UPDATEd stays visible through the delta rows, which are
// renumbered with the rows; an updated row that is removed leaves the delta rows.
func TestVecHNSWFollowRenumbersDeltaRows(t *testing.T) {
	f := newHF(t, 1200, "cosine")
	f.warm()
	moved := slices.Clone(f.table.Rows[900][hfEmbedding].([]float64))
	for i := range moved {
		moved[i] = -moved[i] + 0.25
	}
	f.exec(fmt.Sprintf(`UPDATE hf SET embedding = '%s' WHERE id = 900`, hfLiteral(moved)))
	f.exec(fmt.Sprintf(`UPDATE hf SET embedding = '%s' WHERE id = 1000`, hfLiteral(moved)))
	idx, cache := f.warm() // a refresh: two delta rows
	if len(idx.deltaRows) != 2 {
		t.Fatalf("delta rows %v", idx.deltaRows)
	}
	// Remove sources before and including the second updated row.
	f.exec(`DELETE FROM hf WHERE src = 3 OR src = 40 OR src = 125`) // 125*8 = 1000: removes id 1000
	f.deleted[1000] = true
	idx, cache = f.warm()
	hnswCheckInvariants(t, idx, f.table, cache, "removal with delta rows")
	// id 900 was at position 900, 16 rows (src 3 and 40 are before it) are gone.
	wantRow := 900 - 16
	if f.table.Rows[wantRow][0].(int) != 900 {
		t.Fatalf("row %d holds id %v", wantRow, f.table.Rows[wantRow][0])
	}
	if !reflect.DeepEqual(idx.deltaRows, []int{wantRow}) {
		t.Fatalf("delta rows %v, want [%d]", idx.deltaRows, wantRow)
	}
	hits, err := idx.search(context.Background(), moved, vectorL2Norm(moved), 1, cache)
	if err != nil || len(hits) != 1 || hits[0].rowIdx != wantRow {
		t.Fatalf("the updated vector was not found at row %d: %v %v", wantRow, hits, err)
	}

	// An UPDATE in the same window as a removal is registered at the new position.
	moved2 := slices.Clone(f.table.Rows[400][hfEmbedding].([]float64))
	for i := range moved2 {
		moved2[i] = 0.5 - moved2[i]
	}
	f.exec(fmt.Sprintf(`UPDATE hf SET embedding = '%s' WHERE id = 700`, hfLiteral(moved2)))
	f.exec(`DELETE FROM hf WHERE src = 1`) // ids 8..15, before every row updated so far
	idx, cache = f.warm()
	hnswCheckInvariants(t, idx, f.table, cache, "update and removal in one window")
	var ids []int
	for _, row := range idx.deltaRows {
		ids = append(ids, f.table.Rows[row][0].(int))
	}
	slices.Sort(ids)
	if !reflect.DeepEqual(ids, []int{700, 900}) {
		t.Fatalf("delta rows %v hold the ids %v, want [700 900]", idx.deltaRows, ids)
	}
	hits, err = idx.search(context.Background(), moved2, vectorL2Norm(moved2), 1, cache)
	if err != nil || len(hits) != 1 || f.table.Rows[hits[0].rowIdx][0].(int) != 700 {
		t.Fatalf("the vector updated in the window of the removal was not found: %v %v", hits, err)
	}
}

// When the entry point itself is removed another node of the top level takes over.
func TestVecHNSWFollowReplacesARemovedEntryPoint(t *testing.T) {
	f := newHF(t, 1500, "cosine")
	idx, _ := f.warm()
	entry := idx.entry
	oldLevel := idx.maxLevel
	src := f.table.Rows[entry][1].(int)
	f.live = slices.DeleteFunc(f.live, func(s int) bool { return s == src })
	for id := src * hfPerSource; id < (src+1)*hfPerSource; id++ {
		f.deleted[id] = true
	}
	f.exec(fmt.Sprintf(`DELETE FROM hf WHERE src = %d`, src))
	idx, cache := f.follow()
	hnswCheckInvariants(t, idx, f.table, cache, "entry removed")
	if id := f.table.Rows[idx.entry][0].(int); f.deleted[id] {
		t.Fatalf("the entry point is the removed id %d", id)
	}
	if idx.maxLevel > oldLevel {
		t.Fatalf("maxLevel grew from %d to %d", oldLevel, idx.maxLevel)
	}
	f.recall(idx, cache, 10)
}

// Removing nearly everything, then everything, then adding rows again.
func TestVecHNSWFollowDeletesNearlyEverything(t *testing.T) {
	f := newHF(t, 400, "cosine")
	f.warm()
	f.deleteSources(len(f.live) - 1) // one source of eight rows is left
	idx, cache := f.follow()
	hnswCheckInvariants(t, idx, f.table, cache, "one source left")
	if len(f.table.Rows) != hfPerSource {
		t.Fatalf("%d rows left", len(f.table.Rows))
	}
	query := f.table.Rows[3][hfEmbedding].([]float64)
	hits, err := idx.search(context.Background(), query, vectorL2Norm(query), 20, cache)
	if err != nil || len(hits) == 0 || hits[0].rowIdx != 3 {
		t.Fatalf("search over the remainder: %v %v", hits, err)
	}

	f.exec(`DELETE FROM hf WHERE id >= 0`)
	for id := 0; id < 400; id++ {
		f.deleted[id] = true
	}
	idx, cache = f.follow()
	hnswCheckInvariants(t, idx, f.table, cache, "empty table")
	if hits, err := idx.search(context.Background(), query, vectorL2Norm(query), 5, cache); err != nil || len(hits) != 0 {
		t.Fatalf("search over an empty graph: %v %v", hits, err)
	}

	var values []string
	for k := 0; k < 30; k++ {
		vector := slices.Clone(f.queries[k%len(f.queries)])
		values = append(values, fmt.Sprintf("(%d, 7000, '%s')", 5000+k, hfLiteral(vector)))
	}
	f.exec(`INSERT INTO hf VALUES ` + strings.Join(values, ","))
	idx, cache = f.warm()
	hnswCheckInvariants(t, idx, f.table, cache, "refilled")
	hits, err = idx.search(context.Background(), f.queries[5], vectorL2Norm(f.queries[5]), 1, cache)
	if err != nil || len(hits) != 1 || hits[0].rowIdx != 5 {
		t.Fatalf("search over the refilled graph: %v %v", hits, err)
	}
}

// Invalid and unscorable rows are carried along without becoming nodes.
func TestVecHNSWFollowWithNullAndZeroVectors(t *testing.T) {
	f := newHF(t, 800, "cosine")
	zero := make([]float64, hfDims)
	for _, id := range []int{10, 11, 500, 501, 790} {
		f.table.Rows[id][hfEmbedding] = zero
	}
	for _, id := range []int{20, 21, 600} {
		f.table.Rows[id][hfEmbedding] = nil
	}
	f.table.Version++
	f.table.MarkDirtyFrom(-1)
	f.warm()
	f.deleteSources(5)
	f.exec(`DELETE FROM hf WHERE id = 500 OR id = 20`)
	f.deleted[500], f.deleted[20] = true, true
	idx, cache := f.follow()
	hnswCheckInvariants(t, idx, f.table, cache, "with zero and null vectors")
	f.recall(idx, cache, 10)
}

// The thresholds that make getVecHNSWIndex rebuild instead of following.
func TestVecHNSWFollowThresholds(t *testing.T) {
	f := newHF(t, 2000, "cosine")
	first, _ := f.warm()

	// A small removal is followed in place.
	before := vecHNSWFollowCount.Load()
	f.deleteSources(2)
	idx, cache := f.warm()
	if idx != first || vecHNSWFollowCount.Load() != before+1 {
		t.Fatal("a one percent removal was not followed in place")
	}
	hnswCheckInvariants(t, idx, f.table, cache, "small removal")

	// A single removal of more than five percent rebuilds.
	f.deleteSources(len(f.live) * 8 / 100)
	rebuilt, cache := f.warm()
	if rebuilt == first {
		t.Fatal("a removal of eight percent was followed")
	}
	if rebuilt.removed != 0 {
		t.Fatalf("a rebuilt graph counts %d removed nodes", rebuilt.removed)
	}
	hnswCheckInvariants(t, rebuilt, f.table, cache, "rebuilt")

	// Removals of three percent each add up to the ten percent bound.
	current := rebuilt
	followedRuns, rebuiltAt := 0, 0
	for run := 1; run <= 5; run++ {
		f.deleteSources(len(f.live) * 3 / 100)
		next, cache := f.warm()
		hnswCheckInvariants(t, next, f.table, cache, fmt.Sprintf("run %d", run))
		if next == current {
			followedRuns++
			if next.removed == 0 {
				t.Fatalf("run %d: a followed graph counts no removed nodes", run)
			}
		} else if rebuiltAt == 0 {
			rebuiltAt = run
		}
		current = next
	}
	if followedRuns < 2 || rebuiltAt == 0 {
		t.Fatalf("followed %d runs, first rebuild at run %d", followedRuns, rebuiltAt)
	}
}

// Too many UPDATEs of the vector column in one window go to a rebuild.
func TestVecHNSWFollowRebuildsWhenTooManyVectorsWereUpdated(t *testing.T) {
	f := newHF(t, 240, "cosine")
	first, _ := f.warm()
	f.exec(`UPDATE hf SET embedding = '` + hfLiteral(f.queries[0]) + `' WHERE id < 40`)
	f.deleteSources(1)
	idx, cache := f.warm()
	if idx == first {
		t.Fatal("40 updated vectors and a removal were followed, the delta limit is 32")
	}
	hnswCheckInvariants(t, idx, f.table, cache, "rebuilt")
}

// A failure in the middle of the pass can only lead to a rebuild, never to a
// half-renumbered graph being served.
func TestVecHNSWFollowFailureLeavesNoHalfEditedGraph(t *testing.T) {
	for step := 1; step <= 4; step++ {
		t.Run(fmt.Sprintf("panic after step %d", step), func(t *testing.T) {
			f := newHF(t, 1000, "cosine")
			first, _ := f.warm()
			vecHNSWFollowHook = func(n int) {
				if n == step {
					panic("injected")
				}
			}
			t.Cleanup(func() { vecHNSWFollowHook = nil })
			f.deleteSources(2)
			idx, cache := f.warm()
			vecHNSWFollowHook = nil
			if idx == first {
				t.Fatal("the graph that was interrupted halfway was served")
			}
			hnswCheckInvariants(t, idx, f.table, cache, "rebuilt after the failure")
			f.recall(idx, cache, 10)
			if first.version != -1 || first.structVersion != -1 {
				t.Fatalf("the abandoned graph is stamped %d/%d, not as not current", first.version, first.structVersion)
			}
		})
	}
}

// A panic while the rows appended after a removal are inserted must not leave a
// graph that is stamped current but misses those rows: it has to be rebuilt.
func TestVecHNSWFollowPanicInTheExtensionIsNotServed(t *testing.T) {
	f := newHF(t, 1000, "cosine")
	first, _ := f.warm()
	f.deleteSources(2)
	f.exec(fmt.Sprintf(`INSERT INTO hf VALUES (50000, 9000, '%s'), (50001, 9000, '%s')`,
		hfLiteral(f.queries[0]), hfLiteral(f.queries[1])))
	vecHNSWFollowHook = func(n int) {
		if n == 5 {
			panic("injected into the extension")
		}
	}
	t.Cleanup(func() { vecHNSWFollowHook = nil })
	idx, cache := f.warm()
	vecHNSWFollowHook = nil
	if idx == first {
		t.Fatal("the graph that was interrupted while it took in the appended rows was served")
	}
	if first.version != -1 || first.structVersion != -1 {
		t.Fatalf("the abandoned graph is stamped %d/%d, not as not current", first.version, first.structVersion)
	}
	hnswCheckInvariants(t, idx, f.table, cache, "rebuilt after the failure")
	f.recall(idx, cache, 10)
}

// A cancelled extension of the followed graph leaves it consistent and resumable.
func TestVecHNSWFollowCancelledExtensionResumes(t *testing.T) {
	f := newHF(t, 1500, "cosine")
	idx, _ := f.warm()
	f.deleteSources(3)
	var values []string
	for k := 0; k < 2500; k++ {
		values = append(values, fmt.Sprintf("(%d, 8000, '%s')", 20000+k, hfLiteral(f.queries[k%len(f.queries)])))
		if len(values) == 250 {
			f.exec(`INSERT INTO hf VALUES ` + strings.Join(values, ","))
			values = values[:0]
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cache := f.cache()
	if _, err := getVecHNSWIndex(ctx, "default", f.table, hfEmbedding, "cosine", hfDims, cache); err == nil {
		t.Fatal("a cancelled context was not reported")
	}
	if f.cached() != idx {
		t.Fatal("the cancelled follow replaced the graph")
	}
	if len(idx.levels) != len(idx.neighbors) || len(idx.levels) >= len(f.table.Rows) || idx.version == f.table.Version {
		t.Fatalf("after the cancellation the graph covers %d of %d rows, version %d", len(idx.levels), len(f.table.Rows), idx.version)
	}
	resumed, cache := f.warm()
	if resumed != idx {
		t.Fatal("the resumed extension did not continue the same graph")
	}
	hnswCheckInvariants(t, resumed, f.table, cache, "resumed")
}

// Searches that arrive together after a DELETE share one follow; none may see a
// dead row or a half-edited graph.
func TestVecHNSWFollowConcurrentSearches(t *testing.T) {
	f := newHF(t, 3000, "cosine")
	f.warm()
	var (
		mu       sync.Mutex
		deleted  = map[int]bool{}
		failures atomic.Int64
	)
	run := func(sql string) *ResultSet {
		rs, err := Execute(context.Background(), f.db, "default", mustParse(sql))
		if err != nil {
			t.Errorf("%.100s: %v", sql, err)
			failures.Add(1)
			return &ResultSet{}
		}
		return rs
	}
	for round := 0; round < 6; round++ {
		// A write, then several readers that all find the structures stale. Execute
		// takes the content lock itself: exclusive for the DELETE, shared for a search.
		list := make([]string, 0, 3)
		for i := 0; i < 3 && len(f.live) > 0; i++ {
			at := f.rng.Intn(len(f.live))
			src := f.live[at]
			f.live = slices.Delete(f.live, at, at+1)
			list = append(list, fmt.Sprint(src))
			mu.Lock()
			for id := src * hfPerSource; id < (src+1)*hfPerSource; id++ {
				deleted[id] = true
			}
			mu.Unlock()
		}
		run(`DELETE FROM hf WHERE src IN (` + strings.Join(list, ",") + `)`)

		var wg sync.WaitGroup
		for r := 0; r < 6; r++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for q := 0; q < 8; q++ {
					query := f.queries[(r*8+q)%len(f.queries)]
					rs := run(`SELECT id FROM VEC_SEARCH('hf', 'embedding', '` + hfLiteral(query) + `', 10, 'cosine', 'hnsw')`)
					if len(rs.Rows) == 0 {
						t.Errorf("a search returned no rows")
						failures.Add(1)
					}
					mu.Lock()
					for _, row := range rs.Rows {
						var id int
						switch v := row["id"].(type) {
						case int64:
							id = int(v)
						case int:
							id = v
						}
						if deleted[id] {
							t.Errorf("a search returned the deleted id %d", id)
							failures.Add(1)
						}
					}
					mu.Unlock()
				}
			}()
		}
		wg.Wait()
		if failures.Load() != 0 {
			t.FailNow()
		}
	}
	idx, cache := f.warm()
	hnswCheckInvariants(t, idx, f.table, cache, "after the concurrent searches")
}

// The graph persisted after a followed removal is the one a reopen hydrates, and a
// graph persisted before the removal is never served for the rows after it.
func TestVecHNSWFollowPersistsAndNeverServesAStaleGraph(t *testing.T) {
	f := newHF(t, 600, "cosine")
	f.warm()
	key := vecPersistentHNSWKey(hfEmbedding, "cosine")
	if stored := f.table.VectorIndexes[key]; stored == nil || stored.BuiltRows != 600 {
		t.Fatalf("the warmed graph was not persisted: %#v", stored)
	}
	f.deleteSources(3)

	reopen := func(db *storage.DB) (*storage.Table, bool) {
		data, err := storage.SaveToBytes(db)
		if err != nil {
			t.Fatal(err)
		}
		reopened, err := storage.LoadFromBytes(data)
		if err != nil {
			t.Fatal(err)
		}
		table, err := reopened.Get("default", "hf")
		if err != nil {
			t.Fatal(err)
		}
		purgeVectorCachesFor("default", "hf")
		return table, false
	}
	hydratedFlag := false
	previous := vecPersistentHNSWHydrateHook
	vecPersistentHNSWHydrateHook = func() { hydratedFlag = true }
	t.Cleanup(func() { vecPersistentHNSWHydrateHook = previous })

	// Saved before the follow: the stored graph is stale and must be rebuilt.
	table, _ := reopen(f.db)
	cache := getVecColumnCache("default", table, hfEmbedding, true)
	idx, err := getVecHNSWIndex(context.Background(), "default", table, hfEmbedding, "cosine", hfDims, cache)
	if err != nil {
		t.Fatal(err)
	}
	if hydratedFlag {
		t.Fatal("a graph persisted before the removal was served after a reopen")
	}
	hnswCheckInvariants(t, idx, table, cache, "rebuilt after reopen")

	// Followed, then saved: the reopen hydrates the followed graph.
	purgeVectorCachesFor("default", "hf")
	hydratedFlag = false
	followed, followedCache := f.warm()
	if followed != f.cached() {
		t.Fatal("unexpected graph")
	}
	if stored := f.table.VectorIndexes[key]; stored == nil || stored.StructVersion != f.table.StructVersion() || stored.BuiltRows != len(f.table.Rows) {
		t.Fatalf("the followed graph was not persisted: %#v", stored)
	}
	table, _ = reopen(f.db)
	cache = getVecColumnCache("default", table, hfEmbedding, true)
	idx, err = getVecHNSWIndex(context.Background(), "default", table, hfEmbedding, "cosine", hfDims, cache)
	if err != nil {
		t.Fatal(err)
	}
	if !hydratedFlag {
		t.Fatal("the persisted graph was not hydrated after the reopen")
	}
	hnswCheckInvariants(t, idx, table, cache, "hydrated")
	query := f.queries[7]
	want, err := followed.search(context.Background(), query, vectorL2Norm(query), 10, followedCache)
	if err != nil {
		t.Fatal(err)
	}
	got, err := idx.search(context.Background(), query, vectorL2Norm(query), 10, cache)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("the hydrated graph answers differently: %v vs %v (%v)", got, want, err)
	}
}

// hnswReachable counts the graph nodes a search could reach from the entry point
// through layer 0 (links are directed, so a node can lose its way in even though
// its own links are intact), and the number of nodes in the graph.
func hnswReachable(idx *vecHNSWIndex) (reachable, nodes int) {
	for _, layers := range idx.neighbors {
		if len(layers) > 0 {
			nodes++
		}
	}
	if idx.entry < 0 {
		return 0, nodes
	}
	seen := make([]bool, len(idx.neighbors))
	queue := []int{idx.entry}
	seen[idx.entry] = true
	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]
		reachable++
		for _, nb := range idx.neighborLayer(node, 0) {
			if !seen[nb] {
				seen[nb] = true
				queue = append(queue, nb)
			}
		}
	}
	return reachable, nodes
}

// A graph that is asked to follow with a cache that does not cover the table is
// stamped as not current instead of being left as it is, which would have the
// caller ask again for ever.
func TestVecHNSWFollowRefusesAStaleCache(t *testing.T) {
	f := newHF(t, 600, "cosine")
	idx, staleCache := f.warm()
	f.deleteSources(1)
	changes, ok := f.table.ChangesSince(idx.structVersion, len(idx.levels), storage.ColumnBit(hfEmbedding))
	if !ok || len(changes.Dead) == 0 {
		t.Fatalf("the change log reports no removal: ok=%v", ok)
	}
	followed, err := followVecHNSWIndex(context.Background(), idx, f.table, staleCache, changes)
	if followed || err != nil {
		t.Fatalf("followed=%v err=%v with a cache of the old rows", followed, err)
	}
	if idx.version != -1 || idx.structVersion != -1 {
		t.Fatalf("the refused graph is stamped %d/%d", idx.version, idx.structVersion)
	}
	rebuilt, cache := f.warm()
	if rebuilt == idx {
		t.Fatal("the refused graph was served")
	}
	hnswCheckInvariants(t, rebuilt, f.table, cache, "rebuilt")
}

// With a disk-backed database the followed graph is what the next process loads:
// Close flushes the re-persisted topology, and a reopen hydrates it instead of
// rebuilding.
func TestVecHNSWFollowSurvivesADiskReopen(t *testing.T) {
	f := newHF(t, 600, "cosine")
	dir := filepath.Join(t.TempDir(), "db")
	disk, err := storage.OpenDB(storage.StorageConfig{Mode: storage.ModeDisk, Path: dir})
	if err != nil {
		t.Fatal(err)
	}
	if err := disk.Put("default", f.table); err != nil {
		t.Fatal(err)
	}
	f.db = disk
	f.warm()
	f.deleteSources(3)
	followsBefore := vecHNSWFollowCount.Load()
	idx, cache := f.warm()
	if vecHNSWFollowCount.Load() != followsBefore+1 {
		t.Fatal("the removal was not followed")
	}
	wantRemoved := idx.removed
	if wantRemoved == 0 {
		t.Fatal("the followed graph does not count the nodes it dropped")
	}
	query := f.queries[3]
	want, err := idx.search(context.Background(), query, vectorL2Norm(query), 10, cache)
	if err != nil {
		t.Fatal(err)
	}
	purgeVectorCachesFor("default", "hf")
	if err := disk.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := storage.OpenDB(storage.StorageConfig{Mode: storage.ModeDisk, Path: dir})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	table, err := reopened.Get("default", "hf")
	if err != nil {
		t.Fatal(err)
	}
	if len(table.Rows) != 600-3*hfPerSource {
		t.Fatalf("%d rows after the reopen", len(table.Rows))
	}
	hydrated := false
	previous := vecPersistentHNSWHydrateHook
	vecPersistentHNSWHydrateHook = func() { hydrated = true }
	t.Cleanup(func() { vecPersistentHNSWHydrateHook = previous })
	cache = getVecColumnCache("default", table, hfEmbedding, true)
	loaded, err := getVecHNSWIndex(context.Background(), "default", table, hfEmbedding, "cosine", hfDims, cache)
	if err != nil {
		t.Fatal(err)
	}
	if !hydrated {
		t.Fatal("the followed graph was not hydrated after the reopen")
	}
	hnswCheckInvariants(t, loaded, table, cache, "hydrated from disk")
	if loaded.removed != wantRemoved {
		t.Fatalf("the hydrated graph counts %d removed nodes, the stored one %d", loaded.removed, wantRemoved)
	}
	got, err := loaded.search(context.Background(), query, vectorL2Norm(query), 10, cache)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("the reloaded graph answers differently: %v vs %v (%v)", got, want, err)
	}
	purgeVectorCachesFor("default", "hf")
}

// A followed graph that carries delta rows is not persisted (as after an UPDATE),
// and the topology stored before the removal has the old node numbers and an old
// stamp: a reopen must neither hydrate it nor lose the updated vector.
func TestVecHNSWFollowWithDeltaRowsIsNotHydratedStaleAfterAReopen(t *testing.T) {
	f := newHF(t, 1200, "cosine")
	dir := filepath.Join(t.TempDir(), "db")
	disk, err := storage.OpenDB(storage.StorageConfig{Mode: storage.ModeDisk, Path: dir})
	if err != nil {
		t.Fatal(err)
	}
	if err := disk.Put("default", f.table); err != nil {
		t.Fatal(err)
	}
	f.db = disk
	f.warm() // builds and stores the graph with the old numbering
	moved := slices.Clone(f.table.Rows[500][hfEmbedding].([]float64))
	for i := range moved {
		moved[i] = 0.5 - moved[i]
	}
	f.exec(fmt.Sprintf(`UPDATE hf SET embedding = '%s' WHERE id = 500`, hfLiteral(moved)))
	f.deleteSources(3) // before id 500 or not: the numbering of the rows behind moves
	f.exec(`DELETE FROM hf WHERE src = 0`)
	f.deleted[0] = true
	for id := 0; id < hfPerSource; id++ {
		f.deleted[id] = true
	}
	followsBefore := vecHNSWFollowCount.Load()
	idx, cache := f.warm()
	if vecHNSWFollowCount.Load() != followsBefore+1 || len(idx.deltaRows) == 0 {
		t.Fatalf("expected a followed graph with delta rows: follows %d, delta rows %v", vecHNSWFollowCount.Load()-followsBefore, idx.deltaRows)
	}
	wantRow := -1
	for row, r := range f.table.Rows {
		if r[0].(int) == 500 {
			wantRow = row
		}
	}
	purgeVectorCachesFor("default", "hf")
	if err := disk.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := storage.OpenDB(storage.StorageConfig{Mode: storage.ModeDisk, Path: dir})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	table, err := reopened.Get("default", "hf")
	if err != nil {
		t.Fatal(err)
	}
	hydrated := false
	previous := vecPersistentHNSWHydrateHook
	vecPersistentHNSWHydrateHook = func() { hydrated = true }
	t.Cleanup(func() { vecPersistentHNSWHydrateHook = previous })
	cache = getVecColumnCache("default", table, hfEmbedding, true)
	loaded, err := getVecHNSWIndex(context.Background(), "default", table, hfEmbedding, "cosine", hfDims, cache)
	if err != nil {
		t.Fatal(err)
	}
	if hydrated {
		t.Fatal("a graph with the numbering of before the removal was hydrated")
	}
	hnswCheckInvariants(t, loaded, table, cache, "rebuilt after the reopen")
	hits, err := loaded.search(context.Background(), moved, vectorL2Norm(moved), 1, cache)
	if err != nil || len(hits) != 1 || hits[0].rowIdx != wantRow {
		t.Fatalf("the updated vector was not found at row %d after the reopen: %v %v", wantRow, hits, err)
	}
	purgeVectorCachesFor("default", "hf")
}
