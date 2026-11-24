package storage

import (
	"reflect"
	"testing"
)

func TestColumnMaskBits(t *testing.T) {
	if got := ColumnBit(0); got != 1 {
		t.Fatalf("ColumnBit(0) = %b", got)
	}
	if got := ColumnBit(62); got != 1<<62 {
		t.Fatalf("ColumnBit(62) = %b", got)
	}
	for _, idx := range []int{63, 64, 200, 1 << 20} {
		if got := ColumnBit(idx); got != 1<<63 {
			t.Fatalf("ColumnBit(%d) = %b, want the shared high bit", idx, got)
		}
	}
	if got := ColumnBit(-1); got != ^uint64(0) {
		t.Fatalf("ColumnBit(-1) = %b, want every bit", got)
	}
	if got := ColumnMask(); got != 0 {
		t.Fatalf("empty mask = %b, want 0 (unknown)", got)
	}
	if got := ColumnMask(0, 2, 5, 70); got != 1|1<<2|1<<5|1<<63 {
		t.Fatalf("ColumnMask = %b", got)
	}
}

func updatedColsTable() *Table {
	return NewTable("t", []Column{{Name: "a", Type: IntType}, {Name: "b", Type: IntType}, {Name: "c", Type: IntType}}, false)
}

func TestUpdatedRowsSinceColsFiltersByColumns(t *testing.T) {
	table := updatedColsTable()
	start := table.StructVersion()
	table.MarkRowUpdatedCols(1, ColumnMask(0))    // a
	table.MarkRowUpdatedCols(2, ColumnMask(1))    // b
	table.MarkRowUpdatedCols(3, ColumnMask(0, 2)) // a, c
	table.MarkRowUpdatedCols(4, ColumnMask(2))    // c
	table.MarkRowUpdatedCols(1, ColumnMask(2))    // c, again row 1

	cases := []struct {
		name     string
		interest uint64
		want     []int
	}{
		{"a", ColumnMask(0), []int{1, 3}},
		{"b", ColumnMask(1), []int{2}},
		{"c", ColumnMask(2), []int{3, 4, 1}},
		{"a or b", ColumnMask(0, 1), []int{1, 2, 3}},
		{"unrelated column", ColumnMask(5), nil},
		{"zero interest means every column", 0, []int{1, 2, 3, 4}},
	}
	for _, tc := range cases {
		got, ok := table.UpdatedRowsSinceCols(start, tc.interest)
		if !ok || !reflect.DeepEqual(got, tc.want) {
			t.Errorf("%s: got %v/%v, want %v/true", tc.name, got, ok, tc.want)
		}
	}
	// The legacy reader reports every row, first-seen order, no filtering.
	if got, ok := table.UpdatedRowsSince(start); !ok || !reflect.DeepEqual(got, []int{1, 2, 3, 4}) {
		t.Errorf("UpdatedRowsSince = %v/%v", got, ok)
	}
	// Nothing happened since the current version.
	if got, ok := table.UpdatedRowsSinceCols(table.StructVersion(), ColumnMask(0)); !ok || len(got) != 0 {
		t.Errorf("current version: %v/%v", got, ok)
	}
}

func TestUpdatedRowsSinceColsUnknownMaskAlwaysCounts(t *testing.T) {
	table := updatedColsTable()
	start := table.StructVersion()
	table.MarkRowUpdated(7)                      // unknown
	table.MarkRowUpdatedCols(8, ColumnMask(1))   // b only
	table.MarkRowUpdatedCols(9, 0)               // explicitly unknown
	table.MarkRowUpdatedCols(10, ColumnMask(70)) // a high column shares bit 63
	got, ok := table.UpdatedRowsSinceCols(start, ColumnMask(0))
	if !ok || !reflect.DeepEqual(got, []int{7, 9}) {
		t.Fatalf("interest a: %v/%v, want [7 9]", got, ok)
	}
	got, ok = table.UpdatedRowsSinceCols(start, ColumnMask(99))
	if !ok || !reflect.DeepEqual(got, []int{7, 9, 10}) {
		t.Fatalf("interest high column: %v/%v, want [7 9 10]", got, ok)
	}
}

func TestUpdatedRowsSinceColsValidatesWholeChain(t *testing.T) {
	table := updatedColsTable()
	start := table.StructVersion()
	table.MarkRowUpdatedCols(1, ColumnMask(1))
	mid := table.StructVersion()
	table.MarkRowUpdatedCols(2, ColumnMask(1))
	// A structural change (DELETE, schema change) cuts the chain even though
	// every delta before it was irrelevant to column a.
	table.noteStructuralInvalidation()
	if _, ok := table.UpdatedRowsSinceCols(start, ColumnMask(0)); ok {
		t.Fatal("chain across an invalidation accepted")
	}
	if _, ok := table.UpdatedRowsSinceCols(mid, ColumnMask(0)); ok {
		t.Fatal("chain across an invalidation accepted (mid)")
	}
	after := table.StructVersion()
	table.MarkRowUpdatedCols(3, ColumnMask(1))
	if got, ok := table.UpdatedRowsSinceCols(after, ColumnMask(0)); !ok || len(got) != 0 {
		t.Fatalf("history after the invalidation: %v/%v", got, ok)
	}
	// A stamp from the future (e.g. a counter that was rewound) is refused.
	if _, ok := table.UpdatedRowsSinceCols(table.StructVersion()+3, ColumnMask(0)); ok {
		t.Fatal("future stamp accepted")
	}
	// A hole in the version chain is refused.
	table2 := updatedColsTable()
	table2.MarkRowUpdatedCols(1, ColumnMask(1))
	table2.structVersion += 5 // simulates a mutation that bypassed the log
	table2.MarkRowUpdatedCols(2, ColumnMask(1))
	if _, ok := table2.UpdatedRowsSinceCols(0, ColumnMask(0)); ok {
		t.Fatal("gapped chain accepted")
	}
}

func TestUpdateLogRetainsBatchesButLegacyReaderKeepsItsSpan(t *testing.T) {
	table := updatedColsTable()
	start := table.StructVersion()
	const batch = 6000 // more than the legacy 4096 window, well inside the log
	for i := 0; i < batch; i++ {
		table.MarkRowUpdatedCols(i, ColumnMask(1))
	}
	got, ok := table.UpdatedRowsSinceCols(start, ColumnMask(0))
	if !ok || len(got) != 0 {
		t.Fatalf("irrelevant batch of %d rows: %d rows ok=%v", batch, len(got), ok)
	}
	got, ok = table.UpdatedRowsSinceCols(start, ColumnMask(1))
	if !ok || len(got) != batch {
		t.Fatalf("relevant batch: %d rows ok=%v, want %d", len(got), ok, batch)
	}
	if _, ok := table.UpdatedRowsSince(start); ok {
		t.Fatalf("legacy reader spanned %d versions, its limit is %d", batch, maxLegacyUpdateSpan)
	}
	if _, ok := table.UpdatedRowsSince(table.StructVersion() - maxLegacyUpdateSpan); !ok {
		t.Fatal("legacy reader refused a span of exactly its limit")
	}
	if _, ok := table.UpdatedRowsSince(table.StructVersion() - maxLegacyUpdateSpan - 1); ok {
		t.Fatal("legacy reader accepted a span beyond its limit")
	}
}

func TestUpdateLogEvictionIsBounded(t *testing.T) {
	table := updatedColsTable()
	start := table.StructVersion()
	for i := 0; i < maxRowUpdateDeltas+10; i++ {
		table.MarkRowUpdatedCols(i%100, ColumnMask(1))
	}
	if n := table.rowUpdateLog.len(); n > maxRowUpdateDeltas {
		t.Fatalf("log holds %d deltas, bound is %d", n, maxRowUpdateDeltas)
	}
	if _, ok := table.UpdatedRowsSinceCols(start, ColumnMask(1)); ok {
		t.Fatal("a folded history of the interesting column was reported with row detail")
	}
	recent := table.StructVersion() - 5
	if got, ok := table.UpdatedRowsSinceCols(recent, ColumnMask(0)); !ok || len(got) != 0 {
		t.Fatalf("recent window: %v/%v", got, ok)
	}
}

func TestUpdateLogMasksSurviveClones(t *testing.T) {
	table := updatedColsTable()
	start := table.StructVersion()
	table.MarkRowUpdatedCols(1, ColumnMask(1))
	table.MarkRowUpdatedCols(2, ColumnMask(0))
	for name, clone := range map[string]*Table{"meta": cloneTableMeta(table), "full": cloneTable(table)} {
		got, ok := clone.UpdatedRowsSinceCols(start, ColumnMask(0))
		if !ok || !reflect.DeepEqual(got, []int{2}) {
			t.Errorf("%s clone: %v/%v, want [2]", name, got, ok)
		}
		clone.MarkRowUpdatedCols(3, ColumnMask(0))
		got, ok = clone.UpdatedRowsSinceCols(start, ColumnMask(0))
		if !ok || !reflect.DeepEqual(got, []int{2, 3}) {
			t.Errorf("%s clone after its own update: %v/%v, want [2 3]", name, got, ok)
		}
	}
	// The clones' own updates never reach the source.
	if got, ok := table.UpdatedRowsSinceCols(start, ColumnMask(0)); !ok || !reflect.DeepEqual(got, []int{2}) {
		t.Errorf("source: %v/%v, want [2]", got, ok)
	}
}

// A statement-wide rollback replaces the rows with a pre-image. The UPDATE log
// of the failed statement must not survive it: rewinding structVersion while
// keeping the log let the next UPDATE reuse a version number that a stale
// entry still carried, so a reader was told about the wrong row.
func TestRestoreTableDoesNotLeaveStaleUpdateLog(t *testing.T) {
	table := updatedColsTable()
	for i := 0; i < 4; i++ {
		table.Rows = append(table.Rows, []any{i, i, i})
	}
	table.noteRowUpdatedCols(0, ColumnMask(1))
	stamp := table.StructVersion()
	saved := cloneTable(table)

	table.Rows[1] = []any{1, 100, 1}
	table.MarkRowUpdatedCols(1, ColumnMask(1)) // the failed statement's update of row 1
	restoreTable(table, saved)

	table.Rows[2] = []any{2, 2, 200}
	table.MarkRowUpdatedCols(2, ColumnMask(2)) // the next statement's update of row 2
	got, ok := table.UpdatedRowsSinceCols(stamp, ColumnMask(2))
	if ok && !reflect.DeepEqual(got, []int{2}) {
		t.Fatalf("stale log: reader at %d told about %v, the changed row is 2", stamp, got)
	}
	if table.StructVersion() <= stamp {
		t.Fatalf("structVersion %d did not move forward from %d", table.StructVersion(), stamp)
	}
}

// MarkDirtyKeepingRows forces a full-table record like MarkDirtyFrom(-1) but
// leaves the structural signal and the UPDATE history alone.
func TestMarkDirtyKeepingRows(t *testing.T) {
	table := updatedColsTable()
	for i := 0; i < 5; i++ {
		table.Rows = append(table.Rows, []any{i, i, i})
	}
	table.ResetDirty()
	start := table.StructVersion()
	table.MarkRowUpdatedCols(2, ColumnMask(1))
	if rows, exact := table.DirtyRows(); !exact || len(rows) != 1 {
		t.Fatalf("setup: dirty rows %v/%v", rows, exact)
	}
	table.MarkDirtyKeepingRows()
	if table.DirtyFrom() != -1 {
		t.Fatalf("dirtyFrom = %d, want -1", table.DirtyFrom())
	}
	if rows, exact := table.DirtyRows(); exact {
		t.Fatalf("an exact row list %v survived a full-table mark", rows)
	}
	if got := table.StructVersion(); got != start+1 {
		t.Fatalf("structVersion %d, want %d (only the earlier UPDATE counts)", got, start+1)
	}
	if rows, ok := table.UpdatedRowsSinceCols(start, ColumnMask(1)); !ok || len(rows) != 1 {
		t.Fatalf("UPDATE history lost: %v/%v", rows, ok)
	}
	// The sentinel is sticky against a later append, as for MarkDirtyFrom(-1).
	table.MarkDirtyFrom(5)
	if table.DirtyFrom() != -1 {
		t.Fatalf("a later append downgraded the full-table mark to %d", table.DirtyFrom())
	}
	// Whereas MarkDirtyFrom(-1) invalidates.
	table.MarkDirtyFrom(-1)
	if _, ok := table.UpdatedRowsSinceCols(start, ColumnMask(1)); ok {
		t.Fatal("MarkDirtyFrom(-1) must still cut the UPDATE history")
	}
}

// Past the per-row capacity the oldest deltas are folded into a column summary:
// a reader stamped inside the folded range keeps its answer as long as no folded
// update touched a column it depends on, which is what keeps a bookkeeping UPDATE
// of every row of a big table from invalidating a search structure.
func TestUpdateLogFoldsKeepIrrelevantHistory(t *testing.T) {
	table := updatedColsTable()
	start := table.StructVersion()
	const total = 3 * maxRowUpdateDeltas
	for i := 0; i < total; i++ {
		table.MarkRowUpdatedCols(i%1000, ColumnMask(1))
	}
	if n := table.rowUpdateLog.len(); n > maxRowUpdateDeltas {
		t.Fatalf("log holds %d deltas, bound is %d", n, maxRowUpdateDeltas)
	}
	if len(table.rowUpdateFolds) == 0 {
		t.Fatal("no fold was made")
	}
	if got, ok := table.UpdatedRowsSinceCols(start, ColumnMask(0)); !ok || len(got) != 0 {
		t.Fatalf("irrelevant history across folds: %d rows ok=%v", len(got), ok)
	}
	// The same history is relevant to a reader of column b: it cannot be told
	// which rows, so it must rebuild.
	if _, ok := table.UpdatedRowsSinceCols(start, ColumnMask(1)); ok {
		t.Fatal("a folded relevant history was reported with row detail")
	}
	// A stamp after the last fold still gets exact rows.
	recent := table.StructVersion() - 10
	if got, ok := table.UpdatedRowsSinceCols(recent, ColumnMask(1)); !ok || len(got) == 0 {
		t.Fatalf("recent window: %v/%v", got, ok)
	}
	// The legacy reader never uses a folded range and never spans more than its limit.
	if _, ok := table.UpdatedRowsSince(start); ok {
		t.Fatal("legacy reader accepted a folded range")
	}
	// A schema or DELETE-like invalidation clears everything.
	table.noteStructuralInvalidation()
	if _, ok := table.UpdatedRowsSinceCols(start, ColumnMask(0)); ok {
		t.Fatal("history survived an invalidation")
	}
	if len(table.rowUpdateFolds) != 0 {
		t.Fatalf("%d folds survived an invalidation", len(table.rowUpdateFolds))
	}
}

func TestUpdateLogFoldRecordsRelevantAndUnknownColumns(t *testing.T) {
	table := updatedColsTable()
	for i := 0; i < maxRowUpdateDeltas/2; i++ {
		table.MarkRowUpdatedCols(i, ColumnMask(1))
	}
	mid := table.StructVersion()
	table.MarkRowUpdatedCols(7, ColumnMask(0)) // relevant to column a, inside the first fold
	for i := 0; i < maxRowUpdateDeltas; i++ {
		table.MarkRowUpdatedCols(i, ColumnMask(1))
	}
	if len(table.rowUpdateFolds) == 0 {
		t.Fatal("no fold was made")
	}
	if _, ok := table.UpdatedRowsSinceCols(mid-1, ColumnMask(0)); ok {
		t.Fatal("a fold hid an update of an interesting column")
	}
	// Folds are coarse (they judge by their whole range), so only a reader stamped
	// past the folds that contain the update is told nothing relevant changed.
	afterUpdate := table.rowUpdateRowsFrom() // past every fold: only column b changed since
	if got, ok := table.UpdatedRowsSinceCols(afterUpdate, ColumnMask(0)); !ok || len(got) != 0 {
		t.Fatalf("reader after the update: %v/%v", got, ok)
	}
	// An update with an unknown column set counts for every reader.
	table2 := updatedColsTable()
	start := table2.StructVersion()
	table2.MarkRowUpdated(3)
	for i := 0; i < maxRowUpdateDeltas; i++ {
		table2.MarkRowUpdatedCols(i, ColumnMask(1))
	}
	if _, ok := table2.UpdatedRowsSinceCols(start, ColumnMask(2)); ok {
		t.Fatal("an unknown-column update vanished into a fold")
	}
}

func TestUpdateLogFoldsAreBounded(t *testing.T) {
	table := updatedColsTable()
	start := table.StructVersion()
	n := (maxRowUpdateFolds + 4) * rowUpdateFoldBlocks * rowUpdateChunk
	for i := 0; i < n; i++ {
		table.MarkRowUpdatedCols(i%100, ColumnMask(1))
	}
	if len(table.rowUpdateFolds) > maxRowUpdateFolds {
		t.Fatalf("%d folds kept, bound is %d", len(table.rowUpdateFolds), maxRowUpdateFolds)
	}
	if _, ok := table.UpdatedRowsSinceCols(start, ColumnMask(0)); ok {
		t.Fatal("a stamp older than every retained fold was accepted")
	}
	oldest := table.rowUpdateBase
	if got, ok := table.UpdatedRowsSinceCols(oldest, ColumnMask(0)); !ok || len(got) != 0 {
		t.Fatalf("oldest retained stamp: %v/%v", got, ok)
	}
}

func TestUpdateLogFoldsSurviveClonesAndKeepRowTwinsSafe(t *testing.T) {
	table := updatedColsTable()
	for i := 0; i < 4; i++ {
		table.Rows = append(table.Rows, []any{i, i, i})
	}
	start := table.StructVersion()
	for i := 0; i < maxRowUpdateDeltas+100; i++ {
		table.MarkRowUpdatedCols(i%4, ColumnMask(1))
	}
	for name, clone := range map[string]*Table{"meta": cloneTableMeta(table), "full": cloneTable(table)} {
		if got, ok := clone.UpdatedRowsSinceCols(start, ColumnMask(0)); !ok || len(got) != 0 {
			t.Errorf("%s clone: %v/%v", name, got, ok)
		}
		clone.MarkRowUpdatedCols(1, ColumnMask(0))
		if got, ok := clone.UpdatedRowsSinceCols(start, ColumnMask(0)); ok && len(got) == 0 {
			t.Errorf("%s clone lost its own later update", name)
		}
	}
	if got, ok := table.UpdatedRowsSinceCols(start, ColumnMask(0)); !ok || len(got) != 0 {
		t.Errorf("clones' updates reached the source: %v/%v", got, ok)
	}
	// A row twin cannot be caught up across a folded range: it needs rows.
	buf := append([][]any(nil), table.Rows...)
	if _, ok := table.catchUpRows(buf, start); ok {
		t.Error("catchUpRows used a folded range")
	}
	if _, ok := table.catchUpRows(buf, table.StructVersion()-5); !ok {
		t.Error("catchUpRows refused a stamp inside the retained log")
	}
}

// A failed statement that left no existing row changed must not invalidate the
// log or the structures stamped from it, whether the snapshot was taken for one
// table or for the whole database.
func TestRestoreTableKeepsLogWhenNothingStructural(t *testing.T) {
	table := updatedColsTable()
	for i := 0; i < 4; i++ {
		table.Rows = append(table.Rows, []any{i, i, i})
	}
	start := table.StructVersion()
	table.noteRowUpdatedCols(0, ColumnMask(1))
	saved := cloneTable(table)
	version := table.StructVersion()

	table.Rows = append(table.Rows, []any{9, 9, 9}) // the failed statement only appended
	restoreTable(table, saved)

	if table.StructVersion() != version {
		t.Fatalf("structVersion %d after a restore that changed nothing structural, want %d", table.StructVersion(), version)
	}
	if got, ok := table.UpdatedRowsSinceCols(start, ColumnMask(0)); !ok || len(got) != 0 {
		t.Fatalf("log lost by a harmless restore: %v/%v", got, ok)
	}
	if len(table.Rows) != 4 {
		t.Fatalf("rows not restored: %d", len(table.Rows))
	}
}
