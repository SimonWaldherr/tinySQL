package engine

import (
	"fmt"
	"math"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// This file checks that the flat vector column cache, followed through DELETEs,
// UPDATEs and appends, is at every moment what a cache built from scratch over
// the very same rows would be, both logically (vector, norm and validity of every
// row position) and in what searches return.

const vcEmbeddingCol = 4 // column index of fz.embedding

// vcVector draws the SQL literal of one embedding: mostly regular four-dimensional
// vectors, now and then NULL, a ragged length, an all-zero vector or a duplicate.
func vcVector(h *fzHarness) string {
	r := h.r
	switch k := r.Intn(40); {
	case k == 0, k == 1:
		return "NULL"
	case k == 2:
		return fmt.Sprintf("'[%.3f,%.3f,%.3f]'", r.Float64(), r.Float64(), r.Float64())
	case k == 3:
		return fmt.Sprintf("'[%.3f,%.3f,%.3f,%.3f,%.3f]'", r.Float64(), r.Float64(), r.Float64(), r.Float64(), r.Float64())
	case k == 4:
		return "'[0,0,0,0]'"
	case k == 5:
		return "'[0.5,0.5,0.5,0.5]'"
	}
	return fzVec(r)
}

func vcInsertOp(h *fzHarness, s *fzState) fzOp {
	k := 1 + h.r.Intn(8)
	if h.r.Intn(10) == 0 {
		k = 60 + h.r.Intn(80)
	}
	src := s.nextSrc
	s.nextSrc++
	var rows []fzRow
	var values []string
	for i := 0; i < k; i++ {
		row := fzRow{id: s.nextID, src: src, body: fzText(h.r, h.vocab), note: fmt.Sprintf("n%d", h.r.Intn(5))}
		s.nextID++
		rows = append(rows, row)
		values = append(values, fmt.Sprintf("(%d, %d, %s, '%s', %s)", row.id, row.src, fzLit(row.body), row.note, vcVector(h)))
	}
	return fzOp{sql: "INSERT INTO fz VALUES " + strings.Join(values, ", "), mirror: func(m *fzState) { m.rows = append(m.rows, rows...) }}
}

// vcUpdateOp rewrites embeddings; the mirror tracks no embeddings, so none changes.
func vcUpdateOp(h *fzHarness, s *fzState) fzOp {
	row, ok := fzPickRow(h.r, s)
	if !ok {
		return fzOp{sql: "UPDATE fz SET note = 'none' WHERE id = -1", mirror: func(*fzState) {}}
	}
	switch h.r.Intn(6) {
	case 0:
		return fzOp{sql: fmt.Sprintf("UPDATE fz SET embedding = %s WHERE id = %d", vcVector(h), row.id), mirror: func(*fzState) {}}
	case 1:
		return fzOp{sql: fmt.Sprintf("UPDATE fz SET embedding = NULL WHERE id = %d", row.id), mirror: func(*fzState) {}}
	case 2:
		return fzOp{sql: fmt.Sprintf("UPDATE fz SET embedding = '[1,2,3]' WHERE src = %d", row.src), mirror: func(*fzState) {}}
	case 3:
		mod := 20 + h.r.Intn(40)
		return fzOp{sql: fmt.Sprintf("UPDATE fz SET embedding = %s WHERE id %% %d = %d", fzVec(h.r), mod, h.r.Intn(mod)), mirror: func(*fzState) {}}
	case 4: // the same vector again
		return fzOp{sql: fmt.Sprintf("UPDATE fz SET embedding = embedding WHERE id = %d", row.id), mirror: func(*fzState) {}}
	}
	return fzOp{sql: fmt.Sprintf("UPDATE fz SET embedding = %s, note = 'v' WHERE src = %d", fzVec(h.r), row.src), mirror: func(m *fzState) {
		for i := range m.rows {
			if m.rows[i].src == row.src {
				m.rows[i].note = "v"
			}
		}
	}}
}

// vcCheckEntry verifies the structural invariants of a cache entry for a table of
// rows rows.
func vcCheckEntry(t *testing.T, e vecSearchColumnCacheEntry, rows int, label string) {
	t.Helper()
	fail := func(format string, args ...any) {
		t.Helper()
		t.Fatalf("%s: "+format, append([]any{label}, args...)...)
	}
	if e.rows != rows {
		fail("entry covers %d rows, the table has %d", e.rows, rows)
	}
	if !e.adjacentSegments(0, e.rows) {
		fail("segments are not adjacent and complete")
	}
	for i, segment := range e.segments {
		if len(segment.valid) != len(segment.vectors) || (e.normsReady && len(segment.norms) != len(segment.vectors)) {
			fail("segment %d: per-row arrays differ in length", i)
		}
		live := 0
		for row, vector := range segment.vectors {
			if segment.valid[row] {
				live += len(vector)
			} else if vector != nil {
				fail("segment %d: invalid row %d holds a vector", i, row)
			}
		}
		if live+segment.garbage != len(segment.data) {
			fail("segment %d: %d live floats + %d garbage != %d packed", i, live, segment.garbage, len(segment.data))
		}
	}
	if len(e.overrides) != len(e.overrideRows) {
		fail("%d overrides but %d override rows", len(e.overrides), len(e.overrideRows))
	}
	for i, row := range e.overrideRows {
		if row < 0 || row >= e.rows || (i > 0 && e.overrideRows[i-1] >= row) {
			fail("override row %d out of range or not ascending", row)
		}
		if _, ok := e.overrides[row]; !ok {
			fail("override row %d has no override", row)
		}
	}
}

// vcCompareEntries checks got against want, a cache built from scratch, row by
// row. The norm of a row is compared bit for bit.
func vcCompareEntries(t *testing.T, got, want vecSearchColumnCacheEntry, label string) {
	t.Helper()
	if got.rows != want.rows {
		t.Fatalf("%s: %d rows against %d", label, got.rows, want.rows)
	}
	for row := 0; row < want.rows; row++ {
		gv, gn, gok := got.resolveRow(row, true)
		wv, wn, wok := want.resolveRow(row, true)
		if gok != wok {
			t.Fatalf("%s: row %d valid %v, a fresh cache says %v", label, row, gok, wok)
		}
		if !gok {
			continue
		}
		// A cache without norms (a non-cosine search built it) reports 0 for an override.
		if !slices.Equal(gv, wv) || (got.normsReady && math.Float64bits(gn) != math.Float64bits(wn)) {
			t.Fatalf("%s: row %d is %v (norm %v), a fresh cache has %v (norm %v)", label, row, gv, gn, wv, wn)
		}
	}
}

type vcHarness struct {
	*fzHarness
	followed int64
	checks   int
}

func newVC(t *testing.T, seed int64, variant fzVariant) *vcHarness {
	t.Helper()
	h := &vcHarness{fzHarness: newFZ(t, seed, false, variant)}
	t.Cleanup(func() { purgeVectorCachesFor("default", "fz"); purgeVectorCachesFor("ref", "fz") })
	return h
}

// touch asks for the cache the way a search does; norms are requested now and then
// only, so a cosine search arrives at a cache without norms.
func (h *vcHarness) touch() {
	h.t.Helper()
	before := vecColumnFollowCount.Load()
	_ = getVecColumnCache("default", h.table(), vcEmbeddingCol, h.r.Intn(2) == 0)
	h.followed += vecColumnFollowCount.Load() - before
}

func (h *vcHarness) transaction() {
	h.touch() // so the copy of the table starts from the same rows
	h.db.LockContentForRead()
	base, shadow, release := h.db.SnapshotForTxShared()
	h.db.UnlockContentForRead()
	shadowTable, err := shadow.Get("default", "fz")
	if err != nil {
		h.t.Fatal(err)
	}
	_ = getVecColumnCache("default", shadowTable, vcEmbeddingCol, h.r.Intn(2) == 0)
	txState := h.state.clone()
	for i, n := 0, 1+h.r.Intn(4); i < n; i++ {
		h.apply(shadow, txState, h.randomOp(txState, h.r.Intn(3) == 0))
		if shadowTable, err = shadow.Get("default", "fz"); err != nil {
			h.t.Fatal(err)
		}
		if h.r.Intn(2) == 0 {
			before := vecColumnFollowCount.Load()
			got := getVecColumnCache("default", shadowTable, vcEmbeddingCol, h.r.Intn(2) == 0)
			h.followed += vecColumnFollowCount.Load() - before
			vcCheckEntry(h.t, got, len(shadowTable.Rows), "in a transaction")
			vcCompareEntries(h.t, got, buildVecColumnCache(shadowTable, vcEmbeddingCol, true), fmt.Sprintf("in a transaction, statement %d", i))
			refDB, _ := h.referenceOf(shadowTable)
			h.compareVectorQueries("in a transaction", shadow, refDB, 3)
		}
	}
	if h.r.Intn(2) == 0 {
		h.db.LockContentForWrite()
		changes := storage.CollectWALChanges(base, shadow)
		err := h.db.ApplyWALChanges(changes)
		h.db.UnlockContentForWrite()
		if err != nil {
			h.t.Fatal(err)
		}
		h.state = txState
	}
	release()
}

// randomOp draws a mutating statement: the full-text harness' mix plus statements
// that write embeddings.
func (h *vcHarness) randomOp(state *fzState, vectorsOnly bool) fzOp {
	switch k := h.r.Intn(24); {
	case vectorsOnly && k < 12:
		return vcUpdateOp(h.fzHarness, state)
	case k < 5:
		return vcInsertOp(h.fzHarness, state)
	case k < 8:
		return vcUpdateOp(h.fzHarness, state)
	case k < 11:
		return fzUpdateOp(h.r, state, h.vocab)
	case k < 20:
		return fzDeleteOp(h.r, state)
	default:
		return fzFailingOp(h.r, state)
	}
}

func (h *vcHarness) compareVectorQueries(label string, db, refDB *storage.DB, queries int) {
	h.t.Helper()
	run := func(db *storage.DB, tenant, sql string) *ResultSet {
		rs, err := Execute(h.t.Context(), db, tenant, mustParse(sql))
		if err != nil {
			h.t.Fatalf("%s: %.200s: %v", label, sql, err)
		}
		return rs
	}
	metrics := []string{"cosine", "cosine", "l2", "dot", "manhattan"}
	for q := 0; q < queries; q++ {
		metric := metrics[h.r.Intn(len(metrics))]
		sql := fmt.Sprintf(`SELECT * FROM VEC_SEARCH('fz', 'embedding', %s, %d, '%s', 'flat')`, fzVec(h.r), 1+h.r.Intn(12), metric)
		got, want := run(db, "default", sql), run(refDB, "ref", sql)
		if !reflect.DeepEqual(got.Rows, want.Rows) {
			h.t.Fatalf("%s: %s differs from a fresh cache:\n got  %v\n want %v", label, sql, got.Rows, want.Rows)
		}
	}
	h.compareQueries(label, db, refDB, queries)
}

// checkpoint compares the maintained cache and the searches it serves with a
// build from scratch over the same rows.
func (h *vcHarness) checkpoint(label string, queries int) {
	h.t.Helper()
	h.checks++
	h.checkRows(label)
	table := h.table()
	before := vecColumnFollowCount.Load()
	got := getVecColumnCache("default", table, vcEmbeddingCol, h.r.Intn(2) == 0)
	h.followed += vecColumnFollowCount.Load() - before
	vcCheckEntry(h.t, got, len(table.Rows), label)
	vcCompareEntries(h.t, got, buildVecColumnCache(table, vcEmbeddingCol, true), label)
	refDB, _ := h.reference()
	h.compareVectorQueries(label, h.db, refDB, queries)
	// The searches may have moved the cache on; it must still be sound.
	vcCheckEntry(h.t, getVecColumnCache("default", table, vcEmbeddingCol, false), len(table.Rows), label+" after queries")
}

func (h *vcHarness) run(ops int) {
	h.touch()
	h.checkpoint("initial", 4)
	for i := 0; i < ops; i++ {
		label := fmt.Sprintf("op %d", i)
		switch k := h.r.Intn(40); {
		case k == 0:
			h.transaction()
		default:
			h.apply(h.db, h.state, h.randomOp(h.state, false))
		}
		if h.r.Intn(3) == 0 {
			h.touch()
		}
		if h.r.Intn(10) == 0 {
			h.checkpoint(label, 5)
		}
	}
	h.checkpoint("final", 20)
}

func TestVecColumnCacheFollowsDeletesLikeAColdBuild(t *testing.T) {
	seeds := 30
	if testing.Short() {
		seeds = 6
	}
	var followed int64
	var checks int
	for seed := 1; seed <= seeds; seed++ {
		t.Run(fmt.Sprintf("seed%d", seed), func(t *testing.T) {
			h := newVC(t, int64(seed), fzVariant{})
			h.run(40 + h.r.Intn(120))
			followed += h.followed
			checks += h.checks
		})
	}
	t.Logf("%d checkpoints; %d caches built by following a removal", checks, followed)
	if followed < 20 {
		t.Fatalf("the follow path was barely exercised: %d", followed)
	}
}

// The same workload against the table shapes an application is likely to have.
func TestVecColumnCacheFollowsOnOtherTableShapes(t *testing.T) {
	seeds := 8
	if testing.Short() {
		seeds = 2
	}
	for name, variant := range map[string]fzVariant{
		"primary key":           {pk: true},
		"primary key and index": {pk: true, srcIndex: true},
		"source index":          {srcIndex: true},
	} {
		t.Run(name, func(t *testing.T) {
			var followed int64
			for seed := 301; seed < 301+seeds; seed++ {
				t.Run(fmt.Sprintf("seed%d", seed), func(t *testing.T) {
					h := newVC(t, int64(seed), variant)
					h.run(30 + h.r.Intn(90))
					followed += h.followed
				})
			}
			t.Logf("%d followed removals", followed)
			// A shape in which no DELETE can be reported (a swap-and-pop point delete, say)
			// would pass every comparison by always rebuilding; require that it follows.
			if !testing.Short() && followed < 3 {
				t.Fatalf("the follow path was barely exercised on this table shape: %d", followed)
			}
		})
	}
}

// vcSimpleTable builds a table of groups sources with perSource rows each (four
// dimensions, a distinct vector per row) in a fresh database.
func vcSimpleTable(t *testing.T, groups, perSource int) (*storage.DB, *storage.Table) {
	t.Helper()
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	execSQL(t, db, `CREATE TABLE fz (id INT, src INT, body TEXT, note TEXT, embedding VECTOR)`)
	var values []string
	for id := 0; id < groups*perSource; id++ {
		values = append(values, fmt.Sprintf("(%d, %d, 'w%d', 'n', '[%d,%d,%d,1]')", id, id/perSource, id, id%7+1, id%11+1, id%13+1))
		if len(values) == 200 || id == groups*perSource-1 {
			execSQL(t, db, "INSERT INTO fz VALUES "+strings.Join(values, ", "))
			values = values[:0]
		}
	}
	table, err := db.Get("default", "fz")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { purgeVectorCachesFor("default", "fz") })
	return db, table
}

// vcSnapshot copies what an entry says about every row.
func vcSnapshot(e vecSearchColumnCacheEntry) (vectors [][]float64, norms []float64, valid []bool) {
	for row := 0; row < e.rows; row++ {
		v, n, ok := e.resolveRow(row, true)
		vectors = append(vectors, slices.Clone(v))
		norms = append(norms, n)
		valid = append(valid, ok)
	}
	return
}

// A search that holds the cache from before a DELETE keeps seeing the rows it saw:
// the followed cache shares the packed arrays but never writes to the old entry.
func TestVecColumnCacheFollowLeavesTheOldEntryUntouched(t *testing.T) {
	db, table := vcSimpleTable(t, 40, 10)
	old := getVecColumnCache("default", table, vcEmbeddingCol, true)
	wantVectors, wantNorms, wantValid := vcSnapshot(old)
	oldSegments := slices.Clone(old.segments)
	oldRows := old.rows

	before := vecColumnFollowCount.Load()
	execSQL(t, db, `DELETE FROM fz WHERE src = 3 OR src = 17`)
	execSQL(t, db, `INSERT INTO fz VALUES (1000, 99, 'x', 'n', '[9,9,9,9]')`)
	execSQL(t, db, `UPDATE fz SET embedding = '[5,5,5,5]' WHERE id = 5`)
	fresh := getVecColumnCache("default", table, vcEmbeddingCol, true)
	if vecColumnFollowCount.Load() != before+1 {
		t.Fatalf("the removal was not followed")
	}
	vcCheckEntry(t, fresh, len(table.Rows), "followed")
	vcCompareEntries(t, fresh, buildVecColumnCache(table, vcEmbeddingCol, true), "followed")

	if old.rows != oldRows || !reflect.DeepEqual(old.segments, oldSegments) {
		t.Fatal("the old entry's segments were modified")
	}
	gotVectors, gotNorms, gotValid := vcSnapshot(old)
	if !reflect.DeepEqual(gotVectors, wantVectors) || !reflect.DeepEqual(gotNorms, wantNorms) || !reflect.DeepEqual(gotValid, wantValid) {
		t.Fatal("the old entry no longer answers what it answered before the removal")
	}
	// Sharing, not copying: the big packed array is the very same memory.
	if &old.segments[0].data[0] != &fresh.segments[0].data[0] {
		t.Fatal("the followed cache copied the packed vector data instead of sharing it")
	}
}

// Removals accumulate dead floats in the shared packed arrays; beyond ten percent
// the cache is rebuilt, which brings the garbage back to zero.
func TestVecColumnCacheFollowRebuildsAboveTheGarbageBound(t *testing.T) {
	db, table := vcSimpleTable(t, 100, 10) // 1000 rows, 4000 floats
	entry := getVecColumnCache("default", table, vcEmbeddingCol, true)
	rebuilds, follows := 0, 0
	lastGarbage := 0
	for src := 0; src < 40; src++ {
		before := vecColumnFollowCount.Load()
		execSQL(t, db, fmt.Sprintf(`DELETE FROM fz WHERE src = %d`, src*2))
		entry = getVecColumnCache("default", table, vcEmbeddingCol, true)
		vcCheckEntry(t, entry, len(table.Rows), fmt.Sprintf("after %d deletes", src+1))
		vcCompareEntries(t, entry, buildVecColumnCache(table, vcEmbeddingCol, true), fmt.Sprintf("after %d deletes", src+1))
		garbage, total := 0, 0
		for _, segment := range entry.segments {
			garbage += segment.garbage
			total += len(segment.data)
		}
		if garbage*100 > total*vecColumnGarbagePercent {
			t.Fatalf("after %d deletes %d of %d packed floats are garbage", src+1, garbage, total)
		}
		if vecColumnFollowCount.Load() > before {
			follows++
			if garbage <= lastGarbage {
				t.Fatalf("a followed removal did not add garbage: %d then %d", lastGarbage, garbage)
			}
		} else {
			rebuilds++
			if garbage != 0 {
				t.Fatalf("a rebuilt cache still carries %d garbage floats", garbage)
			}
		}
		lastGarbage = garbage
	}
	if follows < 5 || rebuilds < 2 {
		t.Fatalf("%d removals followed, %d rebuilt", follows, rebuilds)
	}
}

// Re-importing sources one after the other must not pile up segments: every cycle
// appends a small tail, and the removals keep the tails from being equally long
// (the only case the binary-counter merge joins).
func TestVecColumnCacheFollowKeepsSegmentCountBounded(t *testing.T) {
	db, table := vcSimpleTable(t, 60, 8)
	id := 10_000
	var firstID []int
	maxSeen := 0
	before := vecColumnFollowCount.Load()
	for cycle := 0; cycle < 150; cycle++ {
		firstID = append(firstID, id)
		for k := 0; k < 1+cycle%4; k++ {
			execSQL(t, db, fmt.Sprintf(`INSERT INTO fz VALUES (%d, %d, 'x', 'n', '[%d,2,3,4]')`, id, 1000+cycle, k+cycle%9))
			id++
		}
		if cycle >= 2 {
			execSQL(t, db, fmt.Sprintf(`DELETE FROM fz WHERE id = %d`, firstID[cycle-2]))
		}
		entry := getVecColumnCache("default", table, vcEmbeddingCol, true)
		maxSeen = max(maxSeen, len(entry.segments))
		if len(entry.segments) > vecColumnMaxSegments {
			t.Fatalf("cycle %d: %d segments", cycle, len(entry.segments))
		}
		if cycle%10 == 0 {
			vcCheckEntry(t, entry, len(table.Rows), fmt.Sprintf("cycle %d", cycle))
			vcCompareEntries(t, entry, buildVecColumnCache(table, vcEmbeddingCol, true), fmt.Sprintf("cycle %d", cycle))
		}
	}
	t.Logf("at most %d segments; %d removals followed", maxSeen, vecColumnFollowCount.Load()-before)
}

// limitVecColumnSegments merges the smallest neighbours and keeps the rows.
func TestLimitVecColumnSegmentsMergesSmallestNeighbours(t *testing.T) {
	var segments []vecColumnSegment
	start := 0
	for i := 0; i < 40; i++ {
		rows := 1 + i%5
		if i == 0 {
			rows = 500
		}
		segment := vecColumnSegment{start: start, vectors: make([][]float64, rows), valid: make([]bool, rows), norms: make([]float64, rows)}
		segment.data = make([]float64, 0, rows*2)
		for r := 0; r < rows; r++ {
			v := []float64{float64(start + r), 1}
			segment.data = append(segment.data, v...)
			segment.vectors[r] = segment.data[len(segment.data)-2:]
			segment.valid[r] = true
			segment.norms[r] = vectorL2Norm(v)
		}
		segments = append(segments, segment)
		start += rows
	}
	got := limitVecColumnSegments(slices.Clone(segments), true)
	if len(got) != vecColumnMaxSegments {
		t.Fatalf("%d segments, want %d", len(got), vecColumnMaxSegments)
	}
	if len(got[0].vectors) != 500 || &got[0].data[0] != &segments[0].data[0] {
		t.Fatal("the large first segment was copied or merged")
	}
	next := 0
	for _, segment := range got {
		if segment.start != next {
			t.Fatalf("segment starts at %d, want %d", segment.start, next)
		}
		for r, vector := range segment.vectors {
			if vector[0] != float64(next+r) || segment.norms[r] != vectorL2Norm(vector) {
				t.Fatalf("row %d changed in the merge", next+r)
			}
		}
		next += len(segment.vectors)
	}
	if next != start {
		t.Fatalf("%d rows left of %d", next, start)
	}
}

// Removals in the first, in a later and in a whole segment of a cache that grew by
// appends: later segments shift their start, a segment without rows disappears.
func TestVecColumnCacheFollowsAcrossSegments(t *testing.T) {
	db, table := vcSimpleTable(t, 40, 10) // 400 rows
	_ = getVecColumnCache("default", table, vcEmbeddingCol, true)
	insert := func(from, n int) {
		var values []string
		for k := 0; k < n; k++ {
			values = append(values, fmt.Sprintf("(%d, 99, 'x', 'n', '[%d,3,4,5]')", from+k, from+k))
		}
		execSQL(t, db, "INSERT INTO fz VALUES "+strings.Join(values, ", "))
		_ = getVecColumnCache("default", table, vcEmbeddingCol, true)
	}
	insert(5000, 7)
	insert(5100, 3)
	if got := len(getVecColumnCache("default", table, vcEmbeddingCol, true).segments); got != 3 {
		t.Fatalf("%d segments before the removals, want 3", got)
	}
	for step, statement := range []string{
		`DELETE FROM fz WHERE src = 5`,                       // inside the first segment only
		`DELETE FROM fz WHERE id = 5001 OR src = 6`,          // the first segment and the middle of the second
		`DELETE FROM fz WHERE id >= 5000 AND id < 5007`,      // what is left of the second segment
		`DELETE FROM fz WHERE id = 5101 OR id = 3`,           // the first and the last segment
		`DELETE FROM fz WHERE id >= 5100 OR src = 7 OR id=0`, // all of the last segment
	} {
		before := vecColumnFollowCount.Load()
		execSQL(t, db, statement)
		entry := getVecColumnCache("default", table, vcEmbeddingCol, true)
		if vecColumnFollowCount.Load() != before+1 {
			t.Fatalf("step %d (%s): the removal was not followed", step, statement)
		}
		vcCheckEntry(t, entry, len(table.Rows), statement)
		vcCompareEntries(t, entry, buildVecColumnCache(table, vcEmbeddingCol, true), statement)
	}
	if got := len(getVecColumnCache("default", table, vcEmbeddingCol, true).segments); got != 1 {
		t.Fatalf("%d segments after the tail segments emptied, want 1", got)
	}
}
