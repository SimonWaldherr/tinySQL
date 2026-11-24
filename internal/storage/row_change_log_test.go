package storage

import (
	"fmt"
	"math/rand"
	"reflect"
	"slices"
	"testing"
)

// changeModel is the brute-force reference for the change log: the table as a
// list of row identities, plus every event that happened to it.
type changeModel struct {
	t      *Table
	ids    []int // identity of the row at each position
	nextID int
	events []changeEvent
	// resetAt counts the log resets so far;
	// a consumer built before it can no longer be served.
	resetAt int
	trace   []string
}

type changeEvent struct {
	id   int
	cols uint64 // 0 = unknown
}

func newChangeModel() *changeModel {
	cols := []Column{{Name: "a", Type: IntType}, {Name: "b", Type: IntType}, {Name: "c", Type: IntType}}
	return &changeModel{t: NewTable("t", cols, false)}
}

func (m *changeModel) syncRows() {
	rows := make([][]any, len(m.ids))
	for i, id := range m.ids {
		rows[i] = []any{id}
	}
	m.t.Rows = rows
}

func (m *changeModel) appendRows(k int) {
	for i := 0; i < k; i++ {
		m.ids = append(m.ids, m.nextID)
		m.t.Rows = append(m.t.Rows, []any{m.nextID})
		m.nextID++
	}
	m.t.MarkDirtyFrom(len(m.t.Rows) - k) // an append: no structural change
}

func (m *changeModel) update(p int, cols uint64) {
	m.trace = append(m.trace, fmt.Sprintf("update%d", p))
	m.t.MarkRowUpdatedCols(p, cols)
	m.events = append(m.events, changeEvent{id: m.ids[p], cols: cols})
}

// remove deletes the rows at the strictly ascending positions.
func (m *changeModel) remove(positions []int) {
	m.trace = append(m.trace, fmt.Sprintf("remove%v of %d", positions, len(m.ids)))
	drop := make(map[int]bool, len(positions))
	for _, p := range positions {
		drop[p] = true
	}
	kept := m.ids[:0:0]
	for i, id := range m.ids {
		if !drop[i] {
			kept = append(kept, id)
		}
	}
	m.ids = kept
	m.syncRows()
	m.t.MarkRowsRemoved(positions)
}

func (m *changeModel) reset() {
	m.t.MarkDirtyFrom(-1)
	m.resetAt++
}

// build is a consumer's index: the identities of the first n rows at the time.
type changeBuild struct {
	version  int
	ids      []int
	evFrom   int
	resetAtB int
}

func (m *changeModel) build(n int) changeBuild {
	return changeBuild{version: m.t.StructVersion(), ids: slices.Clone(m.ids[:n]), evFrom: len(m.events), resetAtB: m.resetAt}
}

// check compares ChangesSince for one build and one interest mask with the truth.
func (m *changeModel) check(t *testing.T, b changeBuild, interest uint64, label string) {
	t.Helper()
	got, ok := m.t.ChangesSince(b.version, len(b.ids), interest)
	if m.resetAt != b.resetAtB {
		if ok {
			t.Fatalf("%s: a reset after the build was not reported", label)
		}
		return
	}
	if !ok {
		t.Fatalf("%s: unexpected ok=false (version %d, now %d, index %d): %s", label, b.version, m.t.StructVersion(), len(b.ids), dumpLog(m.t)+" TRACE "+fmt.Sprint(m.trace))
	}
	alive := make(map[int]int, len(m.ids))
	for p, id := range m.ids {
		alive[id] = p
	}
	var wantDead []int
	oldToNew := map[int]int{}
	survivors := 0
	for orig, id := range b.ids {
		if _, present := alive[id]; present {
			oldToNew[orig] = survivors
			survivors++
		} else {
			wantDead = append(wantDead, orig)
		}
	}
	index := make(map[int]int, len(b.ids))
	for orig, id := range b.ids {
		index[id] = orig
	}
	updatedSet := map[int]bool{}
	for _, ev := range m.events[b.evFrom:] {
		orig, indexed := index[ev.id]
		if !indexed {
			continue
		}
		if _, present := alive[ev.id]; !present {
			continue
		}
		if interest == 0 || ev.cols == 0 || ev.cols&interest != 0 {
			updatedSet[orig] = true
		}
	}
	var wantUpdated []int
	for orig := range updatedSet {
		wantUpdated = append(wantUpdated, orig)
	}
	slices.Sort(wantUpdated)
	if !reflect.DeepEqual(append([]int(nil), got.Dead...), append([]int(nil), wantDead...)) {
		t.Fatalf("%s: dead = %v, want %v", label, got.Dead, wantDead)
	}
	if !reflect.DeepEqual(append([]int(nil), got.Updated...), append([]int(nil), wantUpdated...)) {
		t.Fatalf("%s: updated = %v, want %v (interest %b)", label, got.Updated, wantUpdated, interest)
	}
	if got.Survivors != survivors {
		t.Fatalf("%s: survivors = %d, want %d", label, got.Survivors, survivors)
	}
	// The first Survivors rows of the table are the surviving indexed rows in
	// their old order, and NewPosition agrees with that.
	if got.Survivors > len(m.ids) {
		t.Fatalf("%s: more survivors than rows", label)
	}
	for orig, id := range b.ids {
		pos, present := alive[id]
		if !present {
			continue
		}
		if np := got.NewPosition(orig); np != pos || np >= got.Survivors {
			t.Fatalf("%s: row %d (id %d) maps to %d, is at %d (survivors %d)", label, orig, id, np, pos, got.Survivors)
		}
	}
}

var changeInterests = []uint64{0, 1, 2, 4, 1 | 4, 2 | 4, ^uint64(0)}
var changeEventCols = []uint64{0, 1, 2, 4, 1 | 2, 4 | 1, 1 << 63}

func randomAscending(r *rand.Rand, n int) []int {
	if n == 0 {
		return nil
	}
	var out []int
	switch r.Intn(5) {
	case 0: // one row
		out = []int{r.Intn(n)}
	case 1: // an adjacent run (a source worth of rows)
		start := r.Intn(n)
		length := 1 + r.Intn(min(8, n-start))
		for i := 0; i < length; i++ {
			out = append(out, start+i)
		}
	case 2: // the first or last rows
		length := 1 + r.Intn(min(4, n))
		if r.Intn(2) == 0 {
			for i := 0; i < length; i++ {
				out = append(out, i)
			}
		} else {
			for i := n - length; i < n; i++ {
				out = append(out, i)
			}
		}
	case 3: // a random subset
		for i := 0; i < n; i++ {
			if r.Intn(4) == 0 {
				out = append(out, i)
			}
		}
	default: // everything
		if r.Intn(4) == 0 {
			for i := 0; i < n; i++ {
				out = append(out, i)
			}
		} else {
			out = []int{r.Intn(n)}
		}
	}
	return out
}

func TestChangesSinceMatchesBruteForce(t *testing.T) {
	seeds := 3000
	if testing.Short() {
		seeds = 300
	}
	for seed := 0; seed < seeds; seed++ {
		r := rand.New(rand.NewSource(int64(seed)))
		m := newChangeModel()
		m.appendRows(r.Intn(12))
		var builds []changeBuild
		ops := 10 + r.Intn(50)
		for op := 0; op < ops; op++ {
			switch k := r.Intn(12); {
			case k < 2:
				m.appendRows(1 + r.Intn(5))
			case k < 6:
				if len(m.ids) > 0 {
					m.update(r.Intn(len(m.ids)), changeEventCols[r.Intn(len(changeEventCols))])
				}
			case k < 10:
				pos := randomAscending(r, len(m.ids))
				if r.Intn(10) == 0 {
					pos = nil // an empty removal logs nothing
				}
				m.remove(pos)
			case k == 10:
				if r.Intn(6) == 0 {
					m.reset()
				}
			default:
				builds = append(builds, m.build(r.Intn(len(m.ids)+1)))
			}
			if r.Intn(5) == 0 {
				builds = append(builds, m.build(r.Intn(len(m.ids)+1)))
			}
			for bi, b := range builds {
				interest := changeInterests[r.Intn(len(changeInterests))]
				m.check(t, b, interest, fmt.Sprintf("seed %d op %d build %d", seed, op, bi))
			}
		}
		// A fresh build at the end sees nothing.
		b := m.build(len(m.ids))
		got, ok := m.t.ChangesSince(b.version, len(b.ids), 0)
		if !ok || len(got.Dead) != 0 || len(got.Updated) != 0 || got.Survivors != len(b.ids) {
			t.Fatalf("seed %d: fresh build reports changes %+v ok=%v", seed, got, ok)
		}
	}
}

func TestChangesSinceTailAndPrefixRemovalsMix(t *testing.T) {
	m := newChangeModel()
	m.appendRows(10)
	b := m.build(6) // rows 6..9 were appended before the build was recorded
	m.update(7, 1)  // a tail row: not reported
	m.update(2, 1)
	m.remove([]int{1, 5, 6, 9}) // two indexed rows, two tail rows
	m.update(1, 1)              // original row 2 is now at 1
	m.update(4, 1)              // a tail row now
	m.check(t, b, 1, "mix")
	got, ok := m.t.ChangesSince(b.version, 6, 1)
	if !ok || !reflect.DeepEqual(got.Dead, []int{1, 5}) || !reflect.DeepEqual(got.Updated, []int{2}) || got.Survivors != 4 {
		t.Fatalf("mix: %+v ok=%v", got, ok)
	}
	// Update then remove, and the removal of an updated row's neighbour.
	m.update(0, 2)
	m.remove([]int{0})
	m.check(t, b, 0, "update then remove")
}

func TestChangesSinceRefusals(t *testing.T) {
	m := newChangeModel()
	m.appendRows(5)
	b := m.build(5)
	if _, ok := m.t.ChangesSince(b.version+1, 5, 0); ok {
		t.Fatal("a version from the future was accepted")
	}
	if _, ok := m.t.ChangesSince(b.version, 6, 0); ok {
		t.Fatal("an index larger than the table was accepted")
	}
	if _, ok := m.t.ChangesSince(b.version, -1, 0); ok {
		t.Fatal("a negative index size was accepted")
	}
	m.remove([]int{0, 1, 2, 3, 4})
	if got, ok := m.t.ChangesSince(b.version, 5, 0); !ok || got.Survivors != 0 || len(got.Dead) != 5 {
		t.Fatalf("a table emptied by one removal: %+v ok=%v", got, ok)
	}
	// An unreportable change drops the log.
	m.appendRows(3)
	b2 := m.build(3)
	m.t.MarkDirtyFrom(-1)
	if _, ok := m.t.ChangesSince(b2.version, 3, 0); ok {
		t.Fatal("MarkDirtyFrom(-1) kept the log")
	}
	// Bad removal lists are unreportable too.
	for name, positions := range map[string][]int{
		"unsorted": {2, 1}, "duplicate": {1, 1}, "negative": {-1}, "beyond": {7},
	} {
		m2 := newChangeModel()
		m2.appendRows(5)
		b3 := m2.build(5)
		m2.ids = m2.ids[:4]
		m2.syncRows()
		m2.t.MarkRowsRemoved(positions)
		if _, ok := m2.t.ChangesSince(b3.version, 5, 0); ok {
			t.Fatalf("%s removal list was accepted", name)
		}
	}
	// An empty removal is not an event.
	m3 := newChangeModel()
	m3.appendRows(3)
	before := m3.t.StructVersion()
	m3.t.MarkRowsRemoved(nil)
	if m3.t.StructVersion() != before {
		t.Fatal("an empty removal bumped structVersion")
	}
	if m3.t.DirtyFrom() != -1 {
		t.Fatal("MarkRowsRemoved must mark the table dirty for the WAL")
	}
}

func TestLegacyReadersRefuseRemovals(t *testing.T) {
	m := newChangeModel()
	m.appendRows(8)
	version := m.t.StructVersion()
	m.update(1, 1)
	m.remove([]int{3})
	m.update(2, 1)
	if _, ok := m.t.UpdatedRowsSince(version); ok {
		t.Fatal("UpdatedRowsSince followed a removal")
	}
	if _, ok := m.t.UpdatedRowsSinceCols(version, 4); ok {
		t.Fatal("UpdatedRowsSinceCols followed a removal")
	}
	if _, ok := m.t.catchUpRows(slices.Clone(m.t.Rows), version); ok {
		t.Fatal("the row twin followed a removal")
	}
	// Updates after the removal are served again to a reader stamped after it.
	after := m.t.StructVersion()
	m.update(0, 1)
	if rows, ok := m.t.UpdatedRowsSince(after); !ok || !reflect.DeepEqual(rows, []int{0}) {
		t.Fatalf("updates after a removal: %v ok=%v", rows, ok)
	}
}

func TestChangeLogCloneBranchesStayIndependent(t *testing.T) {
	m := newChangeModel()
	m.appendRows(20)
	b := m.build(20)
	m.remove([]int{3, 4})
	branch := cloneTable(m.t)
	branchModel := &changeModel{t: branch, ids: slices.Clone(m.ids), nextID: m.nextID, events: slices.Clone(m.events), resetAt: m.resetAt}
	branchModel.remove([]int{0})
	m.update(5, 1)
	m.remove([]int{10, 11, 12})
	m.check(t, b, 0, "source")
	branchModel.check(t, b, 0, "branch")
	if got, ok := branch.ChangesSince(b.version, 20, 0); !ok || !reflect.DeepEqual(got.Dead, []int{0, 3, 4}) || got.Survivors != 17 {
		t.Fatalf("branch saw the source's later events: %+v ok=%v", got, ok)
	}
	// A metadata clone has no rows but carries the log, removals included.
	meta := cloneTableMeta(m.t)
	if !meta.hasRemovalSince(b.version) || meta.StructVersion() != m.t.StructVersion() {
		t.Fatal("the metadata clone lost the change log")
	}
}

func TestChangeLogOverflow(t *testing.T) {
	// Updates only: the older half is folded into a column summary.
	m := newChangeModel()
	m.appendRows(8)
	b := m.build(8)
	for i := 0; i < maxRowUpdateDeltas+1000; i++ {
		m.update(i%8, 4) // column 2 only
	}
	if got, ok := m.t.ChangesSince(b.version, 8, 1); !ok || len(got.Updated) != 0 || got.Survivors != 8 {
		t.Fatalf("a folded log must still vouch that column 0 is untouched: %+v ok=%v", got, ok)
	}
	if _, ok := m.t.ChangesSince(b.version, 8, 4); ok {
		t.Fatal("a folded log named no rows for a column that changed")
	}
	// A removal in the half that is folded is kept: a stamp from before it is still
	// served, composed with the removal, as long as the UPDATEs behind it left the
	// reader's columns alone.
	m = newChangeModel()
	m.appendRows(8)
	b = m.build(8)
	m.remove([]int{2})
	for i := 0; i < maxRowUpdateDeltas+10; i++ {
		m.update(i%7, 4)
	}
	m.check(t, b, 1, "a removal that was folded")
	if _, ok := m.t.ChangesSince(b.version, 8, 4); ok {
		t.Fatal("a folded log named no rows for a column that changed")
	}
	b2 := m.build(7)
	m.update(1, 1)
	m.remove([]int{0})
	m.check(t, b2, 0, "after the drop")
	// Too many removed positions in total: the limit counts every logged
	// position, whichever statement removed it.
	big := NewTable("big", []Column{{Name: "a", Type: IntType}}, false)
	const rows = 1_200_000
	big.Rows = make([][]any, rows)
	base := big.StructVersion()
	removeFront := func(count int) {
		positions := make([]int, count)
		for i := range positions {
			positions[i] = i
		}
		big.Rows = big.Rows[count:]
		big.MarkRowsRemoved(positions)
	}
	removeFront(600_000)
	if got, ok := big.ChangesSince(base, rows, 0); !ok || len(got.Dead) != 600_000 || got.Survivors != 600_000 {
		t.Fatalf("a large removal: ok=%v dead=%d", ok, len(got.Dead))
	}
	removeFront(maxRowRemovedPositions - 600_000)
	if _, ok := big.ChangesSince(base, rows, 0); !ok {
		t.Fatal("exactly the limit was refused")
	}
	removeFront(1)
	if _, ok := big.ChangesSince(base, rows, 0); ok {
		t.Fatal("a log beyond the removed-position limit was served")
	}
	// A single over-long statement is refused as well.
	huge := NewTable("huge", []Column{{Name: "a", Type: IntType}}, false)
	huge.Rows = make([][]any, 3)
	hugeBase := huge.StructVersion()
	positions := make([]int, maxRowRemovedPositions+1)
	for i := range positions {
		positions[i] = i
	}
	huge.MarkRowsRemoved(positions)
	if _, ok := huge.ChangesSince(hugeBase, maxRowRemovedPositions+4, 0); ok {
		t.Fatal("an over-long removal was logged")
	}
}

func TestChangesSinceComposeBudget(t *testing.T) {
	// Removing the last row of a table again and again inserts every dead row in
	// front of the previous ones, which costs the whole dead list per event.
	m := newChangeModel()
	const rows = 40_000
	m.ids = make([]int, rows)
	for i := range m.ids {
		m.ids[i] = i
	}
	m.nextID = rows
	m.syncRows()
	b := m.build(rows)
	for i := 0; i < 30_000; i++ {
		m.ids = m.ids[:len(m.ids)-1]
		m.t.Rows = m.t.Rows[:len(m.t.Rows)-1]
		m.t.MarkRowsRemoved([]int{len(m.ids)})
	}
	if _, ok := m.t.ChangesSince(b.version, rows, 0); ok {
		t.Fatal("a log that costs more than the compose budget was served")
	}
	// The same shape in small amounts is served.
	m = newChangeModel()
	m.ids = make([]int, 100)
	for i := range m.ids {
		m.ids[i] = i
	}
	m.nextID = 100
	m.syncRows()
	b = m.build(100)
	for i := 0; i < 50; i++ {
		last := len(m.ids) - 1
		m.remove([]int{last})
	}
	m.check(t, b, 0, "tail removals")
}
func dumpLog(t *Table) string {
	s := fmt.Sprintf("struct=%d base=%d rows=%d folds=%v:", t.structVersion, t.rowUpdateBase, len(t.Rows), t.rowUpdateFolds)
	for i := 0; i < t.rowUpdateLog.len(); i++ {
		d := t.rowUpdateLog.at(i)
		if d.removed != nil {
			s += fmt.Sprintf(" [v%d rm%v]", d.structVersion, d.removed.positions)
		} else {
			s += fmt.Sprintf(" [v%d up%d/%b]", d.structVersion, d.row, d.cols)
		}
	}
	return s
}

// A full log used to be dropped as a whole when the half to fold held a removal,
// which sent every consumer to a rebuild, including those that had long seen the
// removal. A removal is kept now, as a fold of its own: every stamp is served as
// long as the UPDATEs folded behind it left the reader's columns alone.
func TestChangeLogOverflowKeepsRemovals(t *testing.T) {
	m := newChangeModel()
	m.appendRows(20)
	b0 := m.build(20)
	m.update(3, 4)
	m.remove([]int{5})
	b1 := m.build(19) // has seen the first removal, not the second
	m.update(1, 1)
	m.remove([]int{0, 7})
	b2 := m.build(17) // has seen both
	for i := 0; i < maxRowUpdateDeltas+1000; i++ {
		m.update(i%17, 4) // column 2 only
	}
	if got := m.t.rowUpdateLog.removedTotal; got != 3 {
		t.Fatalf("the log counts %d removed positions, want the 3 of both removals", got)
	}
	if len(m.t.rowUpdateFolds) == 0 {
		t.Fatal("the log did not fold")
	}
	// Column 1 (bit 2) never changed after the stamps: both removals compose.
	for name, b := range map[string]changeBuild{"before everything": b0, "between the removals": b1, "after both": b2} {
		m.check(t, b, 2, name+", column 1")
	}
	// An UPDATE of column 0 sits behind b0 and b1 inside a fold: which row it was is
	// not recorded any more, so they rebuild; b2 is stamped behind it.
	for name, b := range map[string]changeBuild{"before everything": b0, "between the removals": b1} {
		if _, ok := m.t.ChangesSince(b.version, len(b.ids), 1); ok {
			t.Fatalf("a stamp %s was served although a folded UPDATE of column 0 follows it", name)
		}
	}
	for _, interest := range []uint64{1, 2, 1 | 2} {
		m.check(t, b2, interest, fmt.Sprintf("after the overflow, interest %b", interest))
	}
	if _, ok := m.t.ChangesSince(b2.version, len(b2.ids), 4); ok {
		t.Fatal("a folded log named no rows for a column that changed")
	}
	// The log keeps working: a later removal is composed as usual.
	m.remove([]int{2, 3})
	m.update(4, 1)
	m.check(t, b2, 1, "a removal after the overflow")
	m.check(t, b2, 2, "a removal after the overflow, other column")
	// And it keeps folding: a second overflow behind the removal above.
	b3 := m.build(15)
	for i := 0; i < maxRowUpdateDeltas+1000; i++ {
		m.update(i%15, 4)
	}
	m.check(t, b3, 2, "second overflow")
	m.check(t, b2, 2, "second overflow, older stamp")
	if _, ok := m.t.ChangesSince(b2.version, len(b2.ids), 1); ok {
		t.Fatal("a stamp behind a folded UPDATE of column 0 was served")
	}
}

// The legacy reader (row twins, subscriptions) cannot follow a removal, folded or not.
func TestUpdatedRowsSinceRefusesFoldedRemovals(t *testing.T) {
	m := newChangeModel()
	m.appendRows(20)
	b := m.build(20)
	m.remove([]int{5})
	for i := 0; i < maxRowUpdateDeltas+1000; i++ {
		m.update(i%17, 4)
	}
	if _, ok := m.t.UpdatedRowsSinceCols(b.version, 1); ok {
		t.Fatal("UpdatedRowsSinceCols served a window with a folded removal")
	}
	m.check(t, b, 1, "ChangesSince over the same window")
}

// Past the position bound the OLDEST removals are forgotten and only the stamps
// before them rebuild; the log is no longer dropped as a whole.
func TestChangeLogShedsTheOldestRemovalsAtThePositionBound(t *testing.T) {
	big := NewTable("big", []Column{{Name: "a", Type: IntType}}, false)
	const rows = 1_200_000
	big.Rows = make([][]any, rows)
	remove := func(count int) {
		positions := make([]int, count)
		for i := range positions {
			positions[i] = i
		}
		big.Rows = big.Rows[count:]
		big.MarkRowsRemoved(positions)
	}
	const small, smallEvents = 20, 4 * rowUpdateChunk // four sealed blocks of 1280 positions
	old := big.StructVersion()
	for i := 0; i < smallEvents; i++ {
		remove(small)
	}
	mid := big.StructVersion()
	midRows := len(big.Rows)
	filler := maxRowRemovedPositions - small*smallEvents
	remove(filler) // the bound is exactly reached
	if _, ok := big.ChangesSince(old, rows, 0); !ok {
		t.Fatal("exactly the limit was refused")
	}
	remove(1000) // beyond it: the oldest sealed block goes
	if _, ok := big.ChangesSince(old, rows, 0); ok {
		t.Fatal("a stamp older than the forgotten removals was served")
	}
	got, ok := big.ChangesSince(mid, midRows, 0)
	if !ok || len(got.Dead) != filler+1000 || got.Survivors != midRows-len(got.Dead) {
		t.Fatalf("a stamp behind the forgotten removals: ok=%v dead=%d survivors=%d", ok, len(got.Dead), got.Survivors)
	}
	if big.rowUpdateLog.removedTotal > maxRowRemovedPositions {
		t.Fatalf("the log counts %d removed positions, bound is %d", big.rowUpdateLog.removedTotal, maxRowRemovedPositions)
	}
	// Nothing to forget (everything sits in the open block): the log is dropped.
	one := NewTable("one", []Column{{Name: "a", Type: IntType}}, false)
	one.Rows = make([][]any, rows)
	base := one.StructVersion()
	positions := make([]int, maxRowRemovedPositions)
	for i := range positions {
		positions[i] = i
	}
	one.Rows = one.Rows[len(positions):]
	one.MarkRowsRemoved(positions)
	one.Rows = one.Rows[1:]
	one.MarkRowsRemoved([]int{0})
	if _, ok := one.ChangesSince(base, rows, 0); ok {
		t.Fatal("a log that could not make room was served")
	}
}
