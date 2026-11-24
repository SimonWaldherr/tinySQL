package engine

import (
	"fmt"
	"math"
	"math/rand"
	"slices"
	"testing"
)

// Targeted companions of vector_merge_differential_test.go. Each test covers a
// branch that a mutation of the merged code showed the randomized test misses.

// hnswIndex returns the cached graph of the test table for metric, or nil.
func (h *mdHarness) hnswIndex(metric string) *vecHNSWIndex {
	h.t.Helper()
	tbl, err := h.db.Get("default", mdTable)
	if err != nil {
		h.t.Fatal(err)
	}
	key := vecIndexCacheKey{source: tbl, tenant: "default", table: tbl.Name, colIdx: mdVecCol, metric: metric}
	vecHNSWCacheMu.RLock()
	defer vecHNSWCacheMu.RUnlock()
	return vecHNSWCache[key]
}

// hnswState reports the entry row and the number of delta rows of the cached graph
// (entry -2 when there is none).
func (h *mdHarness) hnswState(metric string) (entry, deltas int) {
	h.t.Helper()
	idx := h.hnswIndex(metric)
	if idx == nil {
		return -2, 0
	}
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.entry, len(idx.deltaRows)
}

// firstHit is the id of the best hit of a k = 1 search, and whether there was one.
func (h *mdHarness) firstHit(metric, index string, q []float64) (id int, ok bool) {
	h.t.Helper()
	rs := execSQL(h.t, h.db, fmt.Sprintf("SELECT id FROM VEC_SEARCH('%s', 'v', %s, 1, '%s', '%s')", mdTable, mdLit(q), metric, index))
	if len(rs.Rows) == 0 {
		return 0, false
	}
	return mdInt(h.t, rs.Rows[0]["id"]), true
}

// TestVecL2FlatScanPruningMatchesGenericOnPrefixHeavyVectors makes the prefix bound of
// the exact scan (dims >= 512) decisive: nearly all of a row's distance sits in its
// first 64 coordinates, and the rows form clusters, so the bound prunes most rows once
// the heap is full. A bound that is not a lower bound, or a retry rule that skips
// rows, shows up as a different top-k than the generic scan's.
func TestVecL2FlatScanPruningMatchesGenericOnPrefixHeavyVectors(t *testing.T) {
	for _, dims := range []int{512, 520, 768} {
		t.Run(fmt.Sprint(dims), func(t *testing.T) {
			rng := rand.New(rand.NewSource(int64(dims)))
			const n, groups = 2500, 8
			centers := make([][]float64, groups)
			for g := range centers {
				centers[g] = make([]float64, dims)
				for d := 0; d < l2PrunePrefix; d++ {
					centers[g][d] = 4 * rng.NormFloat64()
				}
			}
			sample := func(g int) []float64 {
				v := make([]float64, dims)
				for d := range v {
					sigma := 0.02
					if d < l2PrunePrefix {
						sigma = 0.6
					}
					v[d] = centers[g][d] + sigma*rng.NormFloat64()
				}
				return v
			}
			rows := make([][]any, n)
			for i := range rows {
				rows[i] = []any{sample(rng.Intn(groups))}
			}
			cache := segmentScanCache(rows, []int{n}, false)
			for q := 0; q < 8; q++ {
				query := sample(rng.Intn(groups))
				if q%4 == 3 {
					query = slices.Clone(rows[rng.Intn(n)][0].([]float64)) // an exact copy: distance 0
				}
				for _, k := range []int{1, 3, 10, 64} {
					got, err := vecSearchFlat(t.Context(), rows, "l2", query, 0, k, cache)
					want, refErr := vecSearchTopK(t.Context(), rows, dims, k, cache, buildVecDistanceFunc("l2", query, 0), false)
					if err != nil || refErr != nil || !slices.EqualFunc(got, want, func(a, b vecScoredRow) bool {
						return a.rowIdx == b.rowIdx && math.Float64bits(a.distance) == math.Float64bits(b.distance)
					}) {
						t.Fatalf("query %d k=%d: pruned scan %v/%v differs from the generic scan %v/%v", q, k, got, err, want, refErr)
					}
				}
			}
		})
	}
}

// TestVecMergeHNSWDeltaRowsSurviveFollowedRemovals updates one row to be the unique,
// exact match of a query far outside the data, then removes rows in front of it. The
// row's old place in the graph says nothing about its new vector: it is found only
// because the delta set holds its (renumbered) position, so a wrong delta set loses it.
func TestVecMergeHNSWDeltaRowsSurviveFollowedRemovals(t *testing.T) {
	stats := &mdStats{}
	for seed := int64(1); seed <= 3; seed++ {
		t.Run(fmt.Sprintf("seed%d", seed), func(t *testing.T) {
			h := newMD(t, 200+seed, 8, 0, stats)
			h.insert(2500, -1, h.regularVec)
			far := slices.Repeat([]float64{10}, 8)
			h.search("l2", "hnsw", 5, h.query("l2"), false) // builds the graph
			follows := vecHNSWFollowCount.Load()
			h.deleteIDs([]int{5, 17})
			h.search("l2", "hnsw", 5, h.query("l2"), false) // follows the removal
			target := h.rows[len(h.rows)-150].id
			h.setVec(h.indexOf(target), slices.Clone(far))
			want := func(label string) {
				t.Helper()
				if id, ok := h.firstHit("l2", "hnsw", far); !ok || id != target {
					t.Fatalf("%s: the best hit is id %d (found %v), want the updated row %d at distance 0", label, id, ok, target)
				}
			}
			want("after the UPDATE")
			if _, deltas := h.hnswState("l2"); deltas == 0 {
				t.Fatalf("the UPDATE was not absorbed as a delta row")
			}
			h.deleteIDs([]int{h.rows[3].id, h.rows[40].id, h.rows[900].id}) // in front of the updated row
			want("after a followed removal in front of the updated row")
			h.deleteIDs([]int{h.rows[1].id})
			want("after a second followed removal")
			if got := vecHNSWFollowCount.Load() - follows; got < 2 {
				t.Fatalf("the graph followed %d removals, want at least 2", got)
			}
		})
	}
}

// TestVecMergeHNSWFallsBackToExactScanWhenTheGraphCannotAnswer covers searches on a
// graph that cannot start from its entry row or has none.
func TestVecMergeHNSWFallsBackToExactScanWhenTheGraphCannotAnswer(t *testing.T) {
	nilVec := func() []float64 { return nil }

	t.Run("entry row becomes unscorable", func(t *testing.T) {
		h := newMD(t, 301, 8, 0, &mdStats{})
		h.insert(2000, -1, h.regularVec)
		// The query is a unique copy of one row's vector: that row is the only exact match.
		targetPos := -1
		for pos := 1000; pos < len(h.rows) && targetPos < 0; pos++ {
			unique := true
			for other := range h.rows {
				if other != pos && slices.Equal(h.rows[other].vec, h.rows[pos].vec) {
					unique = false
					break
				}
			}
			if unique {
				targetPos = pos
			}
		}
		if targetPos < 0 {
			t.Fatal("no row with a unique vector")
		}
		q := slices.Clone(h.rows[targetPos].vec)
		targetID := h.rows[targetPos].id
		h.search("l2", "hnsw", 3, q, false) // builds the graph
		entry, _ := h.hnswState("l2")
		if entry < 0 || entry == targetPos {
			t.Fatalf("entry row %d (target row %d)", entry, targetPos)
		}
		other := (targetPos + 7) % len(h.rows)
		if other == entry {
			other++
		}
		h.setVec(entry, nil)                            // the graph cannot start from here any more
		h.setVec(other, slices.Repeat([]float64{2}, 8)) // a valid delta row, far from the query
		if _, deltas := h.hnswState("l2"); deltas == 0 {
			// The graph is stamped current lazily: let the next search absorb the UPDATEs.
			if id, ok := h.firstHit("l2", "hnsw", q); !ok || id != targetID {
				t.Fatalf("the best hit is id %d (found %v), want the exact match %d", id, ok, targetID)
			}
		}
		if id, ok := h.firstHit("l2", "hnsw", q); !ok || id != targetID {
			t.Fatalf("the best hit is id %d (found %v), want the exact match %d", id, ok, targetID)
		}
		if _, deltas := h.hnswState("l2"); deltas == 0 {
			t.Fatalf("the UPDATEs were not absorbed as delta rows: the case was not reached")
		}
	})

	t.Run("graph without nodes, rows updated afterwards", func(t *testing.T) {
		h := newMD(t, 302, 4, 0, &mdStats{})
		h.insert(60, 1, nilVec)
		q := h.query("l2")
		h.search("l2", "hnsw", 3, q, false) // no row is searchable: a graph without nodes
		if entry, _ := h.hnswState("l2"); entry != -1 {
			t.Fatalf("entry %d, want a graph without nodes", entry)
		}
		for i := 0; i < 5; i++ {
			h.setVec(i, h.regularVec())
		}
		h.search("l2", "hnsw", 3, q, false)
		h.search("l2", "hnsw", 5, q, true)
		if _, deltas := h.hnswState("l2"); deltas == 0 {
			t.Fatalf("the UPDATEs were not absorbed as delta rows: the case was not reached")
		}
	})

	t.Run("last graph nodes deleted and other rows updated in one window", func(t *testing.T) {
		h := newMD(t, 303, 4, 0, &mdStats{})
		h.insert(95, 1, nilVec)
		h.insert(5, 2, h.regularVec)
		q := h.query("l2")
		h.search("l2", "hnsw", 3, q, false) // a graph of five nodes over 100 rows
		h.deleteGroup(2)                    // all five nodes go, 5 percent of the rows: within the follow bound
		for i := 0; i < 3; i++ {
			h.setVec(i, h.regularVec())
		}
		h.search("l2", "hnsw", 3, q, false)
		if entry, _ := h.hnswState("l2"); entry < 0 {
			t.Fatalf("the graph kept no entry node: the three updated rows were registered as delta rows of an empty graph instead of becoming nodes")
		}
	})
}
