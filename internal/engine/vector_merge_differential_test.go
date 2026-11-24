package engine

import (
	"fmt"
	"math"
	"math/rand"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// Differential test of the merged vector-search paths against a brute-force
// oracle computed from the rows the test itself tracks.
//
// It runs random INSERT / UPDATE of the vector column / UPDATE of another column /
// DELETE by predicate / replace (DELETE + INSERT of a group) / truncate, with
// searches after almost every statement: metric l2 and cosine, index flat, hnsw
// and ivf, k in {1, 3, 10, 70, more than the row count}, both SELECT * and an
// explicit column list (the projected VEC_SEARCH path).
//
// Vectors are small integers, so squared distances and dot products are exact in
// any summation order: the exact l2 scan is compared id by id with the oracle
// (ties break by physical row order), cosine with a tolerance, the approximate
// indexes for soundness (every hit is a live, searchable row with the distance of
// its current vector, ranks are right, the count is min(k, searchable rows)).
//
// What makes the paths of the merge reachable is documented at the counters:
// l2Fast is the exact-scan fast path (one contiguous segment, no overrides),
// l2FastOnFollowed the same on a cache that followed a DELETE, the generic
// counters are the paths with several segments or UPDATE overrides, hnswDelta
// counts hnsw searches that ran with delta rows.

const (
	mdTable  = "md_merge"
	mdVecCol = 2 // column index of md_merge.v
)

type mdRow struct {
	id, grp int
	vec     []float64 // nil is NULL
}

type mdStats struct {
	searches, projected                    int
	l2Fast, l2FastOnFollowed, l2Generic    int
	l2GenericOverrides, l2GenericSegmented int
	hnsw, hnswDelta, ivf                   int
}

type mdCand struct {
	pos int
	row mdRow
	d   float64 // l2: squared distance, cosine: 1 - similarity
}

type mdHarness struct {
	t      *testing.T
	tag    string
	rng    *rand.Rand
	db     *storage.DB
	dims   int
	center []float64
	rows   []mdRow // the oracle, in physical row order
	nextID int
	stats  *mdStats
	recent []string
}

func newMD(t *testing.T, seed int64, dims, rows int, stats *mdStats) *mdHarness {
	t.Helper()
	h := &mdHarness{t: t, tag: fmt.Sprintf("seed %d dims %d", seed, dims), rng: rand.New(rand.NewSource(seed)), db: storage.NewDB(), dims: dims, stats: stats}
	h.center = h.randomVec()
	t.Cleanup(func() {
		purgeVectorCachesFor("default", mdTable)
		_ = h.db.Close()
	})
	execSQL(t, h.db, "CREATE TABLE "+mdTable+" (id INT, grp INT, v VECTOR)")
	if rows > 0 {
		h.insert(rows, -1, h.rowVec)
	}
	return h
}

func mdLit(v []float64) string {
	if v == nil {
		return "NULL"
	}
	parts := make([]string, len(v))
	for i, x := range v {
		parts[i] = strconv.FormatFloat(x, 'f', -1, 64)
	}
	return "'[" + strings.Join(parts, ",") + "]'"
}

func mdInt(t *testing.T, v any) int {
	t.Helper()
	n, err := toInt(v)
	if err != nil {
		t.Fatalf("not an integer: %T %v", v, v)
	}
	return n
}

func mdFloat(t *testing.T, v any) float64 {
	t.Helper()
	f, ok := v.(float64)
	if !ok {
		t.Fatalf("not a float: %T %v", v, v)
	}
	return f
}

// ---------------------------------------------------------------------------
// random data

// randomVec is a vector of integers in [-2, 2]: exact arithmetic, many ties.
func (h *mdHarness) randomVec() []float64 {
	v := make([]float64, h.dims)
	for i := range v {
		v[i] = float64(h.rng.Intn(5) - 2)
	}
	return v
}

// nearCenter is the global center with a few coordinates moved by one. For wide
// vectors most rows and queries are drawn this way, so that the exact scan's
// prefix bound (dims >= 512) has close rows to compare against and far rows to prune.
func (h *mdHarness) nearCenter(changes int) []float64 {
	v := slices.Clone(h.center)
	for i := 0; i < changes; i++ {
		v[h.rng.Intn(h.dims)] += float64(h.rng.Intn(3) - 1)
	}
	return v
}

func (h *mdHarness) regularVec() []float64 {
	if h.dims >= 512 && h.rng.Intn(2) == 0 {
		return h.nearCenter(1 + h.rng.Intn(3))
	}
	return h.randomVec()
}

// rowVec draws a stored embedding: mostly regular, now and then NULL, one
// dimension short or long, all zero, or a copy of an existing row.
func (h *mdHarness) rowVec() []float64 {
	switch k := h.rng.Intn(40); {
	case k < 2:
		return nil
	case k == 2:
		return h.randomVec()[:h.dims-1]
	case k == 3:
		return append(h.randomVec(), 1)
	case k == 4:
		return make([]float64, h.dims)
	case k < 8:
		if len(h.rows) > 0 {
			if v := h.rows[h.rng.Intn(len(h.rows))].vec; len(v) == h.dims {
				return slices.Clone(v)
			}
		}
	}
	return h.regularVec()
}

func (h *mdHarness) query(metric string) []float64 {
	var q []float64
	switch {
	case h.dims >= 512 && h.rng.Intn(4) != 0:
		q = h.nearCenter(h.rng.Intn(3))
	case len(h.rows) > 0 && h.rng.Intn(6) == 0:
		if v := h.rows[h.rng.Intn(len(h.rows))].vec; len(v) == h.dims {
			q = slices.Clone(v)
		}
	}
	if q == nil {
		q = h.randomVec()
	}
	zero := true
	for _, x := range q {
		if x != 0 {
			zero = false
			break
		}
	}
	if zero && metric == "cosine" {
		q[0] = 1
	}
	return q
}

// ---------------------------------------------------------------------------
// statements, each mirrored into the oracle after it succeeded

func (h *mdHarness) note(format string, args ...any) {
	h.recent = append(h.recent, fmt.Sprintf(format, args...))
	if len(h.recent) > 12 {
		h.recent = h.recent[1:]
	}
}

func (h *mdHarness) do(label, sql string) {
	h.t.Helper()
	h.note("%s", label)
	execSQL(h.t, h.db, sql)
}

func (h *mdHarness) indexOf(id int) int {
	for i, r := range h.rows {
		if r.id == id {
			return i
		}
	}
	h.t.Fatalf("%s: id %d is not in the oracle", h.tag, id)
	return -1
}

func (h *mdHarness) pickIDs(n int) []int {
	n = min(n, len(h.rows))
	perm := h.rng.Perm(len(h.rows))[:n]
	ids := make([]int, n)
	for i, p := range perm {
		ids[i] = h.rows[p].id
	}
	sort.Ints(ids)
	return ids
}

// insert appends n rows; grp < 0 draws a random group per row.
func (h *mdHarness) insert(n, grp int, vec func() []float64) {
	h.t.Helper()
	values := make([]string, 0, n)
	added := make([]mdRow, 0, n)
	for i := 0; i < n; i++ {
		g := grp
		if g < 0 {
			g = h.rng.Intn(10)
		}
		row := mdRow{id: h.nextID, grp: g, vec: vec()}
		h.nextID++
		added = append(added, row)
		values = append(values, fmt.Sprintf("(%d, %d, %s)", row.id, row.grp, mdLit(row.vec)))
	}
	h.do(fmt.Sprintf("INSERT %d rows, grp %d", n, grp), "INSERT INTO "+mdTable+" VALUES "+strings.Join(values, ", "))
	h.rows = append(h.rows, added...)
}

func (h *mdHarness) dropRows(match func(mdRow) bool) {
	kept := h.rows[:0]
	for _, r := range h.rows {
		if !match(r) {
			kept = append(kept, r)
		}
	}
	h.rows = kept
}

func (h *mdHarness) deleteIDs(ids []int) {
	h.t.Helper()
	if len(ids) == 0 {
		return
	}
	list := make([]string, len(ids))
	drop := make(map[int]bool, len(ids))
	for i, id := range ids {
		list[i] = strconv.Itoa(id)
		drop[id] = true
	}
	h.do(fmt.Sprintf("DELETE %d ids %v", len(ids), ids[:min(len(ids), 6)]), fmt.Sprintf("DELETE FROM %s WHERE id IN (%s)", mdTable, strings.Join(list, ", ")))
	h.dropRows(func(r mdRow) bool { return drop[r.id] })
}

func (h *mdHarness) deleteGroup(g int) {
	h.t.Helper()
	h.do(fmt.Sprintf("DELETE grp %d", g), fmt.Sprintf("DELETE FROM %s WHERE grp = %d", mdTable, g))
	h.dropRows(func(r mdRow) bool { return r.grp == g })
}

func (h *mdHarness) deleteAll() {
	h.t.Helper()
	h.do("DELETE all rows", "DELETE FROM "+mdTable)
	h.rows = nil
}

func (h *mdHarness) setVec(i int, v []float64) {
	h.t.Helper()
	id := h.rows[i].id
	h.do(fmt.Sprintf("UPDATE v of id %d", id), fmt.Sprintf("UPDATE %s SET v = %s WHERE id = %d", mdTable, mdLit(v), id))
	h.rows[i].vec = v
}

func (h *mdHarness) setGroupVec(g int, v []float64) {
	h.t.Helper()
	h.do(fmt.Sprintf("UPDATE v of grp %d", g), fmt.Sprintf("UPDATE %s SET v = %s WHERE grp = %d", mdTable, mdLit(v), g))
	for i := range h.rows {
		if h.rows[i].grp == g {
			h.rows[i].vec = v
		}
	}
}

func (h *mdHarness) setGrp(i, g int) {
	h.t.Helper()
	id := h.rows[i].id
	h.do(fmt.Sprintf("UPDATE grp of id %d", id), fmt.Sprintf("UPDATE %s SET grp = %d WHERE id = %d", mdTable, g, id))
	h.rows[i].grp = g
}

func (h *mdHarness) renameGroup(from, to int) {
	h.t.Helper()
	h.do(fmt.Sprintf("UPDATE grp %d to %d", from, to), fmt.Sprintf("UPDATE %s SET grp = %d WHERE grp = %d", mdTable, to, from))
	for i := range h.rows {
		if h.rows[i].grp == from {
			h.rows[i].grp = to
		}
	}
}

func (h *mdHarness) randomOp() {
	h.t.Helper()
	if len(h.rows) == 0 {
		h.insert(40, -1, h.regularVec)
		return
	}
	group := func() int { return h.rows[h.rng.Intn(len(h.rows))].grp }
	switch k := h.rng.Intn(100); {
	case k < 22:
		h.insert(1+h.rng.Intn(6), -1, h.rowVec)
	case k < 27:
		h.insert(20+h.rng.Intn(26), -1, h.rowVec)
	case k < 35:
		h.setVec(h.rng.Intn(len(h.rows)), h.rowVec())
	case k < 38:
		h.setGroupVec(group(), h.regularVec())
	case k < 46:
		h.setGrp(h.rng.Intn(len(h.rows)), h.rng.Intn(10))
	case k < 49:
		h.renameGroup(group(), 10+h.rng.Intn(5))
	case k < 62:
		h.deleteIDs(h.pickIDs(1 + h.rng.Intn(4)))
	case k < 70:
		h.deleteGroup(group())
	case k < 75: // an importer replacing a source: DELETE the old rows, INSERT the new ones
		g := group()
		h.deleteGroup(g)
		h.insert(1+h.rng.Intn(8), g, h.rowVec)
	case k < 78:
		h.do("DELETE nothing", "DELETE FROM "+mdTable+" WHERE id = -1")
	case k < 80: // truncate, then refill with searchable rows
		h.deleteAll()
		h.insert(40, -1, h.regularVec)
	default: // no write: searches only
	}
}

// ---------------------------------------------------------------------------
// the oracle

// candidates returns the rows a search can rank, best first (ties by physical
// position): rows with a vector of the query's length, and, for cosine, a
// non-zero norm.
func (h *mdHarness) candidates(metric string, q []float64) []mdCand {
	var qn float64
	for _, x := range q {
		qn += x * x
	}
	qn = math.Sqrt(qn)
	if metric == "cosine" && qn == 0 {
		return nil
	}
	var out []mdCand
	for pos, r := range h.rows {
		if r.vec == nil || len(r.vec) != len(q) {
			continue
		}
		if metric == "l2" {
			var s float64
			for i := range q {
				diff := r.vec[i] - q[i]
				s += diff * diff
			}
			out = append(out, mdCand{pos: pos, row: r, d: s})
			continue
		}
		var dot, rn float64
		for i := range q {
			dot += r.vec[i] * q[i]
			rn += r.vec[i] * r.vec[i]
		}
		rn = math.Sqrt(rn)
		if rn == 0 {
			continue
		}
		out = append(out, mdCand{pos: pos, row: r, d: 1 - dot/(rn*qn)})
	}
	sort.SliceStable(out, func(a, b int) bool { return out[a].d < out[b].d })
	return out
}

func (h *mdHarness) verify(metric, index string, k int, q []float64, rows []Row, sql string) {
	h.t.Helper()
	fail := func(format string, args ...any) {
		h.t.Helper()
		h.t.Fatalf("%s: %s\n  query: %.200s\n  %d rows in the table, recent statements:\n    %s",
			h.tag, fmt.Sprintf(format, args...), sql, len(h.rows), strings.Join(h.recent, "\n    "))
	}
	cands := h.candidates(metric, q)
	byID := make(map[int]mdCand, len(cands))
	for _, c := range cands {
		byID[c.row.id] = c
	}
	want := min(k, len(cands))
	// An IVF list whose cosine centroid is the zero vector is never probed (the
	// centroid has no cosine distance), so cosine IVF may return fewer rows than
	// asked for; everything else must return exactly min(k, searchable rows).
	strict := !(index == "ivf" && metric == "cosine")
	if len(rows) > want || (strict && len(rows) != want) {
		fail("%s %s k=%d returned %d rows, want %d", index, metric, k, len(rows), want)
	}
	seen := make(map[int]bool, len(rows))
	prev := math.Inf(-1)
	worst := math.Inf(-1)
	for i, r := range rows {
		id := mdInt(h.t, r["id"])
		c, ok := byID[id]
		if !ok {
			fail("rank %d: id %d is not a searchable row of the oracle (deleted, NULL, wrong length, or a zero vector under cosine)", i+1, id)
		}
		if seen[id] {
			fail("rank %d: id %d is returned twice", i+1, id)
		}
		seen[id] = true
		expect := c.d
		if metric == "l2" {
			expect = math.Sqrt(c.d)
		}
		dist := mdFloat(h.t, r["_vec_distance"])
		if math.Abs(dist-expect) > 1e-9*math.Max(1, math.Abs(expect)) {
			fail("rank %d: id %d reports distance %v, its vector in the oracle gives %v", i+1, id, dist, expect)
		}
		if dist < prev {
			fail("rank %d: distance %v is smaller than the %v before it", i+1, dist, prev)
		}
		prev = dist
		worst = math.Max(worst, c.d)
		if g, ok := r["grp"]; ok && mdInt(h.t, g) != c.row.grp {
			fail("rank %d: id %d has grp %d in the result and %d in the oracle (a row position drifted)", i+1, id, mdInt(h.t, g), c.row.grp)
		}
		if v, ok := r["v"]; ok {
			gotVec, _ := v.([]float64)
			if !slices.Equal(gotVec, c.row.vec) {
				fail("rank %d: id %d carries a different vector than the oracle has (a row position drifted)", i+1, id)
			}
		}
		if rank, ok := r["_vec_rank"]; ok && mdInt(h.t, rank) != i+1 {
			fail("rank %d is reported as %v", i+1, rank)
		}
		if sim, ok := r["_vec_similarity"]; ok {
			wantSim := -dist
			if metric == "cosine" {
				wantSim = 1 - dist
			}
			if math.Abs(mdFloat(h.t, sim)-wantSim) > 1e-9 {
				fail("rank %d: similarity %v does not follow distance %v", i+1, sim, dist)
			}
		}
	}
	if index != "flat" {
		return
	}
	if metric == "l2" {
		// Exact: squared distances are exact integers, so the order is fully
		// determined: distance, then physical row.
		for i, r := range rows {
			if id := mdInt(h.t, r["id"]); id != cands[i].row.id {
				fail("rank %d is id %d (squared distance %v), the oracle puts id %d (%v) there; ties break by physical row order", i+1, id, byID[id].d, cands[i].row.id, cands[i].d)
			}
		}
		return
	}
	for _, c := range cands {
		if !seen[c.row.id] && len(rows) > 0 && c.d < worst-1e-9 {
			fail("id %d (distance %v) is missing although the result reaches %v", c.row.id, c.d, worst)
		}
	}
}

// ---------------------------------------------------------------------------
// searches

// observeL2Cache looks at the column cache an exact l2 search is about to use, checks
// it against a cache built from scratch, and records which path the search takes.
func (h *mdHarness) observeL2Cache(tbl *storage.Table) {
	h.t.Helper()
	before := vecColumnFollowCount.Load()
	entry := getVecColumnCache("default", tbl, mdVecCol, false)
	followed := vecColumnFollowCount.Load() != before
	label := h.tag + ": the cache before an l2 search"
	vcCheckEntry(h.t, entry, len(tbl.Rows), label)
	vcCompareEntries(h.t, entry, buildVecColumnCache(tbl, mdVecCol, true), label)
	garbage := 0
	for _, s := range entry.segments {
		garbage += s.garbage
	}
	if _, ok := entry.contiguousSegment(); ok && len(tbl.Rows) == entry.rows {
		h.stats.l2Fast++
		if garbage > 0 || followed {
			h.stats.l2FastOnFollowed++
		}
		return
	}
	h.stats.l2Generic++
	if len(entry.overrides) != 0 {
		h.stats.l2GenericOverrides++
	}
	if len(entry.segments) > 1 {
		h.stats.l2GenericSegmented++
	}
}

func (h *mdHarness) search(metric, index string, k int, q []float64, projected bool) {
	h.t.Helper()
	tbl, err := h.db.Get("default", mdTable)
	if err != nil {
		h.t.Fatal(err)
	}
	h.stats.searches++
	if metric == "l2" && index == "flat" {
		h.observeL2Cache(tbl)
	}
	call := fmt.Sprintf("VEC_SEARCH('%s', 'v', %s, %d, '%s', '%s')", mdTable, mdLit(q), k, metric, index)
	sql := "SELECT * FROM " + call
	if projected {
		sql = "SELECT id, grp, _vec_distance, _vec_similarity, _vec_rank FROM " + call
		h.stats.projected++
	}
	h.note("search %s %s k=%d projected=%v", index, metric, k, projected)
	rs := execSQL(h.t, h.db, sql)
	h.verify(metric, index, k, q, rs.Rows, sql)
	switch index {
	case "ivf":
		h.stats.ivf++
	case "hnsw":
		h.stats.hnsw++
		key := vecIndexCacheKey{source: tbl, tenant: "default", table: tbl.Name, colIdx: mdVecCol, metric: metric}
		vecHNSWCacheMu.RLock()
		idx := vecHNSWCache[key]
		vecHNSWCacheMu.RUnlock()
		if idx != nil {
			idx.mu.RLock()
			delta := len(idx.deltaRows)
			idx.mu.RUnlock()
			if delta > 0 {
				h.stats.hnswDelta++
			}
		}
	}
}

func (h *mdHarness) randomSearches() {
	h.t.Helper()
	for n := 1 + h.rng.Intn(3); n > 0; n-- {
		metric := "l2"
		if h.rng.Intn(3) == 0 {
			metric = "cosine"
		}
		index := "flat"
		switch r := h.rng.Intn(100); {
		case r < 28:
			index = "hnsw"
		case r < 40:
			index = "ivf"
		}
		ks := []int{1, 3, 10, 70, len(h.rows) + 1 + h.rng.Intn(5)}
		h.search(metric, index, ks[h.rng.Intn(len(ks))], h.query(metric), h.rng.Intn(2) == 0)
	}
}

func (h *mdHarness) finalSweep() {
	h.t.Helper()
	for _, metric := range []string{"l2", "cosine"} {
		for _, index := range []string{"flat", "hnsw", "ivf"} {
			for i, k := range []int{1, 3, 10, len(h.rows) + 3} {
				h.search(metric, index, k, h.query(metric), i%2 == 0)
			}
		}
	}
}

func (h *mdHarness) run(ops int) {
	h.t.Helper()
	for i := 0; i < ops; i++ {
		h.randomOp()
		h.randomSearches()
	}
	h.finalSweep()
}

// ---------------------------------------------------------------------------
// tests

func TestVecMergeDifferentialAgainstOracle(t *testing.T) {
	seeds := 8
	if testing.Short() {
		seeds = 3
	}
	stats := &mdStats{}
	cacheFollows, hnswFollows := vecColumnFollowCount.Load(), vecHNSWFollowCount.Load()
	for seed := 1; seed <= seeds; seed++ {
		t.Run(fmt.Sprintf("seed%d", seed), func(t *testing.T) {
			h := newMD(t, int64(seed), 4, 120+10*seed, stats)
			h.run(45)
		})
	}
	cacheFollows = vecColumnFollowCount.Load() - cacheFollows
	hnswFollows = vecHNSWFollowCount.Load() - hnswFollows
	t.Logf("%d searches (%d projected): column cache followed %d times, exact l2 fast path %d (on a followed cache %d), generic l2 %d (overrides %d, several segments %d), hnsw %d (with delta rows %d, graph followed %d times), ivf %d",
		stats.searches, stats.projected, cacheFollows, stats.l2Fast, stats.l2FastOnFollowed, stats.l2Generic, stats.l2GenericOverrides, stats.l2GenericSegmented,
		stats.hnsw, stats.hnswDelta, hnswFollows, stats.ivf)
	if t.Failed() {
		return
	}
	if cacheFollows == 0 || stats.l2FastOnFollowed == 0 || hnswFollows == 0 {
		t.Errorf("the workload did not reach the followed paths: cache follows %d, l2 fast path on a followed cache %d, hnsw follows %d", cacheFollows, stats.l2FastOnFollowed, hnswFollows)
	}
}

// TestVecMergeScriptedFollowPaths drives every combination of the merge once, in a
// fixed order, and asserts the counters that prove the path was taken.
func TestVecMergeScriptedFollowPaths(t *testing.T) {
	stats := &mdStats{}
	h := newMD(t, 7, 4, 200, stats) // ids 0..199
	q := []float64{1, 0, -1, 2}
	qc := []float64{2, 1, 0, -1}
	follows := func() (cache, hnsw int64) { return vecColumnFollowCount.Load(), vecHNSWFollowCount.Load() }

	// 1. A cold cache: one segment, no overrides: the exact scan's fast path.
	h.search("l2", "flat", 5, q, false)
	if stats.l2Fast != 1 {
		t.Fatalf("cold cache: fast path taken %d times, want 1", stats.l2Fast)
	}

	// 2. A DELETE of three rows (1.5% of the data, below the garbage bound) is followed;
	// the followed cache is still one segment, so the fast path serves it.
	c0, _ := follows()
	h.deleteIDs([]int{3, 77, 150})
	h.search("l2", "flat", 5, q, false)
	if c1, _ := follows(); c1 != c0+1 || stats.l2FastOnFollowed != 1 {
		t.Fatalf("after DELETE: cache follows %d (want %d), fast path on a followed cache %d (want 1)", c1-c0, 1, stats.l2FastOnFollowed)
	}
	h.search("l2", "flat", 1, q, true)
	h.search("l2", "flat", len(h.rows)+9, q, false)

	// 3. Appended rows make a second segment: the generic scan, on top of a followed segment.
	h.insert(5, -1, h.regularVec) // ids 200..204
	h.search("l2", "flat", 5, q, false)
	if stats.l2Generic != 1 || stats.l2GenericSegmented != 1 {
		t.Fatalf("after an append: generic %d, several segments %d, want 1 and 1", stats.l2Generic, stats.l2GenericSegmented)
	}

	// 4. A DELETE inside the first segment and one inside the appended tail is followed across two segments.
	c0, _ = follows()
	h.deleteIDs([]int{10, 200})
	h.search("l2", "flat", 5, q, true)
	if c1, _ := follows(); c1 != c0+1 || stats.l2GenericSegmented != 2 {
		t.Fatalf("after DELETE across two segments: cache follows %d (want 1), several segments %d (want 2)", c1-c0, stats.l2GenericSegmented)
	}
	// IVF reads the cache of several segments without overrides directly (scoreSegmentedIVFList).
	h.search("l2", "ivf", 5, q, false)
	h.search("cosine", "ivf", 5, qc, true)
	h.search("cosine", "flat", 10, qc, false) // the norms of a followed, segmented cache

	// 5. An UPDATE of a vector is an override; a following DELETE renumbers it.
	h.setVec(h.indexOf(50), q) // distance 0 to the query
	h.search("l2", "flat", 5, q, false)
	if stats.l2GenericOverrides != 1 {
		t.Fatalf("after a vector UPDATE: generic scans with overrides %d, want 1", stats.l2GenericOverrides)
	}
	c0, _ = follows()
	h.deleteIDs([]int{60})
	h.search("l2", "flat", 5, q, true)
	if c1, _ := follows(); c1 != c0+1 || stats.l2GenericOverrides != 2 {
		t.Fatalf("after DELETE with an override in the cache: cache follows %d (want 1), overrides seen %d (want 2)", c1-c0, stats.l2GenericOverrides)
	}
	h.search("cosine", "flat", 10, qc, true)
	h.search("l2", "ivf", 5, q, true)
	h.search("cosine", "ivf", 5, qc, false)

	// 6. HNSW: cold graph, followed removal, delta row from an UPDATE, renumbered by a
	// second followed removal, k == 1, k above the row count, then appended rows.
	for mi, metric := range []string{"cosine", "l2"} {
		qq := q
		if metric == "cosine" {
			qq = qc
		}
		base := 20 + 20*mi // 20, 21, 30 and 40, 41, 50
		h.search(metric, "hnsw", 10, qq, false)
		_, g0 := follows()
		h.deleteIDs([]int{base, base + 1})
		h.search(metric, "hnsw", 10, qq, true)
		if _, g1 := follows(); g1 != g0+1 {
			t.Fatalf("%s: the graph followed %d removals, want 1", metric, g1-g0)
		}
		h.setVec(h.indexOf(100+10*mi), h.regularVec())
		h.search(metric, "hnsw", 10, qq, false)
		deltas := stats.hnswDelta
		_, g0 = follows()
		h.deleteIDs([]int{base + 10})
		h.search(metric, "hnsw", 10, qq, true)
		if _, g1 := follows(); g1 != g0+1 || stats.hnswDelta != deltas+1 {
			t.Fatalf("%s: after a removal with a delta row the graph followed %d times (want 1), searches with delta rows +%d (want 1)", metric, g1-g0, stats.hnswDelta-deltas)
		}
		h.search(metric, "hnsw", 1, qq, false)
		h.search(metric, "hnsw", len(h.rows)+7, qq, true)
		h.insert(3, -1, h.regularVec)
		h.search(metric, "hnsw", 10, qq, false)
		h.search(metric, "hnsw", 1, qq, true)
	}

	// 7. Everything at once, after truncating.
	h.deleteAll()
	h.search("l2", "flat", 3, q, false)
	h.search("cosine", "hnsw", 3, qc, false)
	h.search("l2", "ivf", 3, q, false)
	h.insert(40, -1, h.regularVec)
	h.finalSweep()
}

// TestVecMergeParallelExactScanAfterFollow runs the exact scans over more rows than
// vecSearchParallelMinRows, so that the scan is split over several workers (when
// GOMAXPROCS > 1), with many equal distances across the chunk borders.
func TestVecMergeParallelExactScanAfterFollow(t *testing.T) {
	stats := &mdStats{}
	h := newMD(t, 11, 4, 0, stats)
	for i := 0; i < 9; i++ {
		h.insert(500, -1, h.rowVec)
	}
	if len(h.rows) < vecSearchParallelMinRows {
		t.Fatalf("%d rows, the scan would not be split", len(h.rows))
	}
	cacheFollows := vecColumnFollowCount.Load()
	for _, k := range []int{1, 10, 70, 3000, len(h.rows) + 5} {
		h.search("l2", "flat", k, h.query("l2"), false)
		h.search("cosine", "flat", k, h.query("cosine"), true)
	}
	for round := 0; round < 4; round++ {
		n := 5
		if round == 2 {
			n = 100
		}
		h.deleteIDs(h.pickIDs(n))
		h.search("l2", "flat", 10, h.query("l2"), false) // round 0: one followed segment, the fast path
		h.search("l2", "flat", 3000, h.query("l2"), true)
		h.search("cosine", "flat", 10, h.query("cosine"), false)
		h.insert(7, -1, h.rowVec)
		h.search("l2", "flat", 10, h.query("l2"), false) // two segments: the generic scan
		h.search("cosine", "flat", 70, h.query("cosine"), true)
	}
	if followed := vecColumnFollowCount.Load() - cacheFollows; followed == 0 || stats.l2FastOnFollowed == 0 {
		t.Errorf("the cache followed %d times, the fast path served %d followed caches", followed, stats.l2FastOnFollowed)
	}
}

// TestVecMergeWideExactScanPruning uses 520-dimensional vectors, where the exact scan
// bounds a row by the distance of its first 64 coordinates (l2ScanPruneMinDims),
// on caches that followed removals.
func TestVecMergeWideExactScanPruning(t *testing.T) {
	stats := &mdStats{}
	for seed := int64(1); seed <= 2; seed++ {
		t.Run(fmt.Sprintf("seed%d", seed), func(t *testing.T) {
			h := newMD(t, 100+seed, 520, 90, stats)
			for _, k := range []int{1, 3, 10, 200} {
				h.search("l2", "flat", k, h.query("l2"), k%2 == 0)
			}
			h.run(14)
		})
	}
}
