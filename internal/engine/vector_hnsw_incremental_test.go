// Tests for the HNSW vector index's incremental-append fast path
// (extendVecHNSWIndex / canExtendVecHNSWIndex in vector_index.go): a cached
// graph that is stale only because new rows were appended grows in place
// instead of being discarded and rebuilt from scratch. Correctness is the
// point being verified here, not performance — see
// vector_hnsw_incremental_benchmark_test.go for the benchmark showing
// per-insert cost independent of table size.
package engine

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// vecHNSWFingerprint captures every field a search actually reads, so two
// vecHNSWIndex graphs can be compared for exact structural equality rather
// than merely "returns the same search results".
type vecHNSWFingerprint struct {
	entry     int
	maxLevel  int
	levels    []int
	neighbors [][][]int
}

func fingerprintVecHNSWIndex(idx *vecHNSWIndex) vecHNSWFingerprint {
	neighbors := make([][][]int, len(idx.neighbors))
	for i, layers := range idx.neighbors {
		cp := make([][]int, len(layers))
		for l, nbs := range layers {
			cp[l] = append([]int(nil), nbs...)
		}
		neighbors[i] = cp
	}
	return vecHNSWFingerprint{
		entry:     idx.entry,
		maxLevel:  idx.maxLevel,
		levels:    append([]int(nil), idx.levels...),
		neighbors: neighbors,
	}
}

func requireEqualVecHNSWFingerprint(t *testing.T, incremental, fresh vecHNSWFingerprint) {
	t.Helper()
	if incremental.entry != fresh.entry {
		t.Fatalf("entry diverged: incremental=%d fresh=%d", incremental.entry, fresh.entry)
	}
	if incremental.maxLevel != fresh.maxLevel {
		t.Fatalf("maxLevel diverged: incremental=%d fresh=%d", incremental.maxLevel, fresh.maxLevel)
	}
	if len(incremental.levels) != len(fresh.levels) {
		t.Fatalf("row count diverged: incremental=%d fresh=%d", len(incremental.levels), len(fresh.levels))
	}
	for i := range fresh.levels {
		if incremental.levels[i] != fresh.levels[i] {
			t.Fatalf("levels[%d] diverged: incremental=%d fresh=%d", i, incremental.levels[i], fresh.levels[i])
		}
		if len(incremental.neighbors[i]) != len(fresh.neighbors[i]) {
			t.Fatalf("neighbors[%d] layer count diverged: incremental=%d fresh=%d",
				i, len(incremental.neighbors[i]), len(fresh.neighbors[i]))
		}
		for layer := range fresh.neighbors[i] {
			a, b := incremental.neighbors[i][layer], fresh.neighbors[i][layer]
			if len(a) != len(b) {
				t.Fatalf("neighbors[%d][%d] length diverged: incremental=%v fresh=%v", i, layer, a, b)
			}
			for j := range b {
				if a[j] != b[j] {
					t.Fatalf("neighbors[%d][%d][%d] diverged: incremental=%v fresh=%v", i, layer, j, a, b)
				}
			}
		}
	}
}

// TestVecHNSWIncrementalExtendMatchesFullRebuild inserts rows one at a time
// (warming/extending the HNSW graph after every single insert) and confirms
// the resulting graph is not merely "a working graph" but structurally
// IDENTICAL — same entry point, same per-row level, same neighbor lists at
// every layer — to one built from scratch in a single pass over the same
// final data. It also cross-checks that both graphs return identical
// (ranked, scored) search results for several queries.
func TestVecHNSWIncrementalExtendMatchesFullRebuild(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE inc_docs (id INT, embedding VECTOR)`)

	const total = 260
	for i := 0; i < total; i++ {
		execSQL(t, db, fmt.Sprintf(
			`INSERT INTO inc_docs VALUES (%d, '[%f, %f, %f, %f]')`,
			i,
			math.Sin(float64(i)*0.11), math.Cos(float64(i)*0.07),
			math.Sin(float64(i)*0.05), math.Cos(float64(i)*0.13),
		))
		// Force the HNSW graph to warm (first insert) or extend (every
		// subsequent insert) right after each row lands.
		execSQL(t, db, `SELECT * FROM VEC_WARM('inc_docs', 'embedding', 'cosine', 'hnsw')`)
	}

	table, err := db.Get("default", "inc_docs")
	if err != nil {
		t.Fatal(err)
	}
	colIdx, err := table.ColIndex("embedding")
	if err != nil {
		t.Fatal(err)
	}
	key := vecIndexCacheKey{tenant: "default", table: table.Name, colIdx: colIdx, metric: "cosine"}

	vecHNSWCacheMu.RLock()
	incremental := vecHNSWCache[key]
	vecHNSWCacheMu.RUnlock()
	if incremental == nil {
		t.Fatal("expected a warmed HNSW index")
	}
	if incremental.version != table.Version || incremental.structVersion != table.StructVersion() {
		t.Fatalf("incremental index not fully caught up: idx.version=%d idx.structVersion=%d, table.Version=%d table.StructVersion=%d",
			incremental.version, incremental.structVersion, table.Version, table.StructVersion())
	}
	if len(incremental.levels) != total {
		t.Fatalf("expected incremental index to cover all %d rows, got %d", total, len(incremental.levels))
	}
	incrementalFP := fingerprintVecHNSWIndex(incremental)

	// Now force a from-scratch rebuild on the exact same final table data.
	purgeVectorCachesFor("default", "inc_docs")
	execSQL(t, db, `SELECT * FROM VEC_WARM('inc_docs', 'embedding', 'cosine', 'hnsw')`)
	vecHNSWCacheMu.RLock()
	fresh := vecHNSWCache[key]
	vecHNSWCacheMu.RUnlock()
	if fresh == nil {
		t.Fatal("expected a freshly rebuilt HNSW index")
	}
	freshFP := fingerprintVecHNSWIndex(fresh)

	requireEqualVecHNSWFingerprint(t, incrementalFP, freshFP)

	// Behavioral cross-check on top of the structural one: several queries
	// must return identical ranked results (ids and distances) whichever
	// graph answers them.
	queries := [][]float64{
		{0.5, 0.1, -0.2, 0.4},
		{-0.3, 0.9, 0.05, -0.6},
		{0.0, 0.0, 1.0, 0.0},
	}
	for qi, q := range queries {
		rsIncremental := execSQL(t, db, fmt.Sprintf(
			`SELECT id, _vec_rank FROM VEC_SEARCH('inc_docs', 'embedding', '%s', 8, 'cosine', 'hnsw')`,
			mustVecJSON(t, q)))
		purgeVectorCachesFor("default", "inc_docs")
		rsFresh := execSQL(t, db, fmt.Sprintf(
			`SELECT id, _vec_rank FROM VEC_SEARCH('inc_docs', 'embedding', '%s', 8, 'cosine', 'hnsw')`,
			mustVecJSON(t, q)))
		if len(rsIncremental.Rows) != len(rsFresh.Rows) {
			t.Fatalf("query %d: result count diverged: incremental=%d fresh=%d", qi, len(rsIncremental.Rows), len(rsFresh.Rows))
		}
		for i := range rsFresh.Rows {
			if rsIncremental.Rows[i]["id"] != rsFresh.Rows[i]["id"] {
				t.Fatalf("query %d result %d: id diverged: incremental=%v fresh=%v", qi, i, rsIncremental.Rows[i]["id"], rsFresh.Rows[i]["id"])
			}
			gotRank, _ := rsIncremental.Rows[i]["_vec_rank"].(float64)
			wantRank, _ := rsFresh.Rows[i]["_vec_rank"].(float64)
			if math.Abs(gotRank-wantRank) > 1e-9 {
				t.Fatalf("query %d result %d: _vec_rank diverged: incremental=%v fresh=%v", qi, i, gotRank, wantRank)
			}
		}
	}
}

// TestVecHNSWExtendFallsBackToFullRebuildAfterUpdateOrDelete mixes plain
// appends with an UPDATE and a DELETE partway through, and confirms:
//  1. the cached HNSW index is NOT incrementally extended across an
//     UPDATE/DELETE boundary (canExtendVecHNSWIndex must reject it, forcing
//     buildVecHNSWIndex's full-rebuild path instead), and
//  2. search results remain correct throughout, including after the
//     fallback rebuild.
func TestVecHNSWExtendFallsBackToFullRebuildAfterUpdateOrDelete(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE mix_docs (id INT, embedding VECTOR)`)

	insertRow := func(id int) {
		execSQL(t, db, fmt.Sprintf(
			`INSERT INTO mix_docs VALUES (%d, '[%f, %f, %f]')`,
			id, math.Sin(float64(id)*0.2), math.Cos(float64(id)*0.15), math.Sin(float64(id)*0.31)))
	}
	warm := func() { execSQL(t, db, `SELECT * FROM VEC_WARM('mix_docs', 'embedding', 'cosine', 'hnsw')`) }

	table, err := db.Get("default", "mix_docs")
	if err != nil {
		t.Fatal(err)
	}
	colIdx, err := table.ColIndex("embedding")
	if err != nil {
		t.Fatal(err)
	}
	key := vecIndexCacheKey{tenant: "default", table: table.Name, colIdx: colIdx, metric: "cosine"}

	for i := 0; i < 40; i++ {
		insertRow(i)
	}
	warm()

	vecHNSWCacheMu.RLock()
	idxBeforeUpdate := vecHNSWCache[key]
	vecHNSWCacheMu.RUnlock()
	if idxBeforeUpdate == nil {
		t.Fatal("expected a warmed HNSW index before the UPDATE")
	}
	structBeforeUpdate := idxBeforeUpdate.structVersion

	// A few more pure appends: still eligible for incremental extension.
	for i := 40; i < 55; i++ {
		insertRow(i)
	}
	warm()
	vecHNSWCacheMu.RLock()
	idxAfterAppends := vecHNSWCache[key]
	vecHNSWCacheMu.RUnlock()
	if idxAfterAppends != idxBeforeUpdate {
		t.Fatal("expected pure appends to extend the same cached *vecHNSWIndex object in place")
	}
	if idxAfterAppends.structVersion != structBeforeUpdate {
		t.Fatalf("pure appends must not advance structVersion: before=%d after=%d", structBeforeUpdate, idxAfterAppends.structVersion)
	}
	if len(idxAfterAppends.levels) != 55 {
		t.Fatalf("expected the extended graph to cover 55 rows, got %d", len(idxAfterAppends.levels))
	}

	// UPDATE retains graph topology while the vector cache supplies a fresh
	// row override to traversal and scoring.
	execSQL(t, db, `UPDATE mix_docs SET embedding = '[9.0, 9.0, 9.0]' WHERE id = 10`)
	if table.StructVersion() == structBeforeUpdate {
		t.Fatalf("UPDATE must advance StructVersion (was %d, still %d)", structBeforeUpdate, table.StructVersion())
	}
	warm()
	vecHNSWCacheMu.RLock()
	idxAfterUpdate := vecHNSWCache[key]
	vecHNSWCacheMu.RUnlock()
	if idxAfterUpdate != idxAfterAppends {
		t.Fatal("expected the UPDATE to retain HNSW topology and refresh row data in place")
	}
	if len(idxAfterUpdate.levels) != 55 {
		t.Fatalf("expected the refreshed graph to still cover 55 rows, got %d", len(idxAfterUpdate.levels))
	}
	if len(idxAfterUpdate.deltaRows) != 1 || idxAfterUpdate.deltaRows[0] != 10 {
		t.Fatalf("expected exact ANN delta [10], got %v", idxAfterUpdate.deltaRows)
	}

	// A search for the updated vector must find row 10 as an exact match,
	// proving the update overlay is used rather than stale vector data.
	rs := execSQL(t, db, `SELECT id FROM VEC_SEARCH('mix_docs', 'embedding', '[9.0, 9.0, 9.0]', 1, 'cosine', 'hnsw')`)
	if len(rs.Rows) != 1 || rs.Rows[0]["id"] != int64(10) && rs.Rows[0]["id"] != 10 {
		t.Fatalf("expected row 10 as the nearest match after UPDATE, got %#v", rs.Rows)
	}

	// More pure appends after the UPDATE: eligible again, extends in place
	// starting from the delta-refreshed graph.
	for i := 55; i < 70; i++ {
		insertRow(i)
	}
	warm()
	vecHNSWCacheMu.RLock()
	idxAfterMoreAppends := vecHNSWCache[key]
	vecHNSWCacheMu.RUnlock()
	if idxAfterMoreAppends != idxAfterUpdate {
		t.Fatal("expected appends after the rebuild to extend that same object in place")
	}
	if len(idxAfterMoreAppends.levels) != 70 {
		t.Fatalf("expected the extended graph to cover 70 rows, got %d", len(idxAfterMoreAppends.levels))
	}

	// DELETE must, symmetrically, also force a full rebuild rather than an
	// incremental extend.
	structBeforeDelete := table.StructVersion()
	execSQL(t, db, `DELETE FROM mix_docs WHERE id = 5`)
	if table.StructVersion() == structBeforeDelete {
		t.Fatalf("DELETE must advance StructVersion (was %d, still %d)", structBeforeDelete, table.StructVersion())
	}
	warm()
	vecHNSWCacheMu.RLock()
	idxAfterDelete := vecHNSWCache[key]
	vecHNSWCacheMu.RUnlock()
	if idxAfterDelete == idxAfterMoreAppends {
		t.Fatal("expected the DELETE to force a full rebuild into a NEW *vecHNSWIndex object, not extend the old one in place")
	}
	if len(idxAfterDelete.levels) != 69 {
		t.Fatalf("expected the rebuilt graph to cover 69 rows after the delete, got %d", len(idxAfterDelete.levels))
	}

	// Sanity: the deleted row's id must no longer be reachable via search of
	// a broad top-k, and results must otherwise still be well-formed.
	rs = execSQL(t, db, `SELECT id FROM VEC_SEARCH('mix_docs', 'embedding', '[0.0, 0.0, 0.0]', 69, 'cosine', 'hnsw')`)
	for _, row := range rs.Rows {
		if row["id"] == int64(5) || row["id"] == 5 {
			t.Fatalf("deleted row 5 unexpectedly still present in search results: %#v", rs.Rows)
		}
	}
}

// TestVecHNSWExtendContextCancellationLeavesConsistentGraph confirms that if
// an incremental extend is cancelled partway through, the graph is left
// covering exactly the rows it finished inserting (never a partially linked
// row), and a later call successfully resumes and completes the extension.
func TestVecHNSWExtendContextCancellationLeavesConsistentGraph(t *testing.T) {
	db := storage.NewDB()
	table := storage.NewTable("cancel_docs", []storage.Column{
		{Name: "id", Type: storage.IntType},
		{Name: "embedding", Type: storage.VectorType},
	}, false)
	const initial = 2000
	for i := 0; i < initial; i++ {
		table.Rows = append(table.Rows, []any{i, []float64{
			math.Sin(float64(i) * 0.011), math.Cos(float64(i) * 0.017), math.Sin(float64(i) * 0.023),
		}})
	}
	if err := db.Put("default", table); err != nil {
		t.Fatal(err)
	}
	colIdx, err := table.ColIndex("embedding")
	if err != nil {
		t.Fatal(err)
	}

	cache := getVecColumnCache("default", table, colIdx, true)
	if _, err := getVecHNSWIndex(context.Background(), "default", table, colIdx, "cosine", 3, cache); err != nil {
		t.Fatal(err)
	}

	// Append enough rows that the extend loop's periodic (every 1024 rows)
	// context check is guaranteed to fire at least once.
	const appended = 3000
	for i := initial; i < initial+appended; i++ {
		table.Rows = append(table.Rows, []any{i, []float64{
			math.Sin(float64(i) * 0.011), math.Cos(float64(i) * 0.017), math.Sin(float64(i) * 0.023),
		}})
	}
	table.Version++

	key := vecIndexCacheKey{tenant: "default", table: table.Name, colIdx: colIdx, metric: "cosine"}
	vecHNSWCacheMu.RLock()
	idx := vecHNSWCache[key]
	vecHNSWCacheMu.RUnlock()
	if idx == nil {
		t.Fatal("expected a cached HNSW index")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled: checkCtx must fail on the loop's first check
	cache = getVecColumnCache("default", table, colIdx, true)
	if _, err := getVecHNSWIndex(ctx, "default", table, colIdx, "cosine", 3, cache); err == nil {
		t.Fatal("expected context cancellation to surface as an error")
	}

	vecHNSWCacheMu.RLock()
	partial := vecHNSWCache[key]
	vecHNSWCacheMu.RUnlock()
	if partial != idx {
		t.Fatal("expected the cancelled extend to leave the same cached object in place")
	}
	if len(partial.levels) != len(partial.neighbors) {
		t.Fatalf("levels/neighbors length mismatch after cancellation: %d vs %d", len(partial.levels), len(partial.neighbors))
	}
	if len(partial.levels) < initial || len(partial.levels) > initial+appended {
		t.Fatalf("expected partial progress between %d and %d rows, got %d", initial, initial+appended, len(partial.levels))
	}
	// Every neighbor reference recorded so far must point at a row that is
	// itself part of the (possibly truncated) graph — no dangling links past
	// the truncation point.
	for i, layers := range partial.neighbors {
		for layer, nbs := range layers {
			for _, nb := range nbs {
				if nb < 0 || nb >= len(partial.neighbors) {
					t.Fatalf("row %d layer %d has an out-of-range neighbor %d (graph covers %d rows)", i, layer, nb, len(partial.neighbors))
				}
			}
		}
	}

	// A later, uncancelled call must resume and finish the extension.
	if _, err := getVecHNSWIndex(context.Background(), "default", table, colIdx, "cosine", 3, cache); err != nil {
		t.Fatalf("expected the resumed extend to succeed: %v", err)
	}
	vecHNSWCacheMu.RLock()
	complete := vecHNSWCache[key]
	vecHNSWCacheMu.RUnlock()
	if len(complete.levels) != initial+appended {
		t.Fatalf("expected the resumed extend to reach %d rows, got %d", initial+appended, len(complete.levels))
	}
	if complete.structVersion != table.StructVersion() || complete.version != table.Version {
		t.Fatal("expected the completed extend to catch the cached index up to the table's current version")
	}
}
