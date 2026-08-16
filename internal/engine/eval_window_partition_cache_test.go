// Whitebox equivalence test for the window-partition memoization in
// eval_window.go (windowPartitionCache / resolveWindowPartition /
// buildWindowPartition / filterPartitionIndexed / sortRowsIndexed).
//
// filterPartition, sortRows and findRowIndex -- the three primitives the old,
// unmemoized evalWindowFunction composed on every single output row -- are
// still present unchanged (evalWindowFunction's defensive fallback path in
// resolveWindowPartition still calls them). That means the pre-fix behavior
// can be reproduced directly, right here, by composing those same three
// primitives per row exactly as evalWindowFunction used to, and comparing
// the result row-for-row against resolveWindowPartition's memoized path --
// without ever touching eval_window.go/exec.go/exec_group.go's shipped code
// or reverting anything in the live tree (this repo has other in-flight,
// uncommitted stage work sharing exec.go, so file-level reverts there are
// unsafe).
package engine

import (
	"testing"
)

// oldStyleResolvePartition reproduces evalWindowFunction's pre-memoization
// logic verbatim: filter allRows down to currentRow's partition, sort by the
// OVER clause's ORDER BY, then locate currentRow by a rowsEqual scan (hint
// then linear search) -- exactly what findRowIndex has always done.
func oldStyleResolvePartition(env ExecEnv, over *OverClause, allRows []Row, currentRow Row, hint int) ([]Row, int) {
	partitionRows := allRows
	if len(over.PartitionBy) > 0 {
		partitionRows = filterPartition(env, allRows, over.PartitionBy, currentRow)
	}
	if len(over.OrderBy) > 0 {
		partitionRows = sortRows(partitionRows, over.OrderBy)
	}
	return partitionRows, findRowIndex(partitionRows, currentRow, hint)
}

// rowIDs extracts the "id" column from a row slice for compact, readable
// test failure output (comparing Row maps directly is noisy).
func rowIDs(rows []Row) []any {
	ids := make([]any, len(rows))
	for i, r := range rows {
		ids[i] = r["id"]
	}
	return ids
}

func idsEqual(a, b []any) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// buildPartitionCacheFixture returns a synthetic window-function row set
// with: multiple partitions (dept A/B/C), ties within a partition (dept A
// has two rows tied at sal=90 and two tied at sal=70; dept B is fully tied
// at sal=50), and a size-1 partition (dept C). Rows are deliberately NOT
// grouped by partition in allRows, so a correct implementation must actually
// filter rather than assume contiguity.
func buildPartitionCacheFixture() []Row {
	mk := func(id int, dept string, sal int) Row {
		return Row{"id": id, "dept": dept, "sal": sal}
	}
	return []Row{
		mk(1, "A", 90),
		mk(2, "B", 50),
		mk(3, "C", 100),
		mk(4, "A", 90),
		mk(5, "B", 50),
		mk(6, "A", 80),
		mk(7, "A", 70),
		mk(8, "A", 70),
		mk(9, "A", 60),
	}
}

// compareOldVsNewForOverClause runs both the old per-row composition and the
// new memoized resolveWindowPartition for every row in allRows (using each
// row's own index as env.windowIndex, matching how exec_group.go's row loop
// always sets it) and fails on any divergence in either the partition's row
// sequence or the resolved current-row position.
func compareOldVsNewForOverClause(t *testing.T, label string, over *OverClause, allRows []Row) {
	t.Helper()
	env := ExecEnv{windowRows: allRows, windowPartitions: newWindowPartitionCache()}
	for i, row := range allRows {
		env.windowIndex = i

		oldRows, oldIdx := oldStyleResolvePartition(env, over, allRows, row, i)
		newRows, newIdx := resolveWindowPartition(env, &FuncCall{Name: "TEST", Over: over}, allRows, row)

		if !idsEqual(rowIDs(oldRows), rowIDs(newRows)) {
			t.Errorf("%s: row id=%v (idx %d): partition row sequence differs\n  old: %v\n  new: %v",
				label, row["id"], i, rowIDs(oldRows), rowIDs(newRows))
		}
		if oldIdx != newIdx {
			t.Errorf("%s: row id=%v (idx %d): current-row position differs: old=%d new=%d",
				label, row["id"], i, oldIdx, newIdx)
		}
		// currentIdx must always point back at the same row content in both
		// paths -- a mismatched index that happens to still pass the above
		// only by coincidence would be a real bug.
		if oldIdx >= 0 && oldIdx < len(oldRows) && newIdx >= 0 && newIdx < len(newRows) {
			if oldRows[oldIdx]["id"] != newRows[newIdx]["id"] {
				t.Errorf("%s: row id=%v (idx %d): resolved row identity differs: old points at id=%v, new points at id=%v",
					label, row["id"], i, oldRows[oldIdx]["id"], newRows[newIdx]["id"])
			}
		}
	}
}

func TestWindowPartitionCacheMatchesOldImplementation_PartitionAndOrder(t *testing.T) {
	allRows := buildPartitionCacheFixture()
	over := &OverClause{
		PartitionBy: []Expr{newVarRef("dept")},
		OrderBy:     []OrderItem{{Col: "sal", Desc: true}},
	}
	compareOldVsNewForOverClause(t, "PARTITION BY dept ORDER BY sal DESC", over, allRows)
}

func TestWindowPartitionCacheMatchesOldImplementation_PartitionOnly(t *testing.T) {
	allRows := buildPartitionCacheFixture()
	over := &OverClause{
		PartitionBy: []Expr{newVarRef("dept")},
	}
	compareOldVsNewForOverClause(t, "PARTITION BY dept (no ORDER BY)", over, allRows)
}

func TestWindowPartitionCacheMatchesOldImplementation_OrderOnlyNoPartition(t *testing.T) {
	allRows := buildPartitionCacheFixture()
	over := &OverClause{
		OrderBy: []OrderItem{{Col: "sal", Desc: false}},
	}
	compareOldVsNewForOverClause(t, "no PARTITION BY, ORDER BY sal ASC", over, allRows)
}

func TestWindowPartitionCacheMatchesOldImplementation_NoPartitionNoOrder(t *testing.T) {
	allRows := buildPartitionCacheFixture()
	over := &OverClause{}
	compareOldVsNewForOverClause(t, "no PARTITION BY, no ORDER BY", over, allRows)
}

// TestWindowPartitionCacheMatchesOldImplementation_MultiColumnPartition
// exercises a composite PARTITION BY key (dept, sal) so every row in the
// fixture except the sal=90 and sal=70/sal=50 tie-pairs ends up in its own
// singleton partition, and the tie-pairs share a partition of size 2 --
// covering a different partition-size distribution than the single-column
// case above.
func TestWindowPartitionCacheMatchesOldImplementation_MultiColumnPartition(t *testing.T) {
	allRows := buildPartitionCacheFixture()
	over := &OverClause{
		PartitionBy: []Expr{newVarRef("dept"), newVarRef("sal")},
		OrderBy:     []OrderItem{{Col: "id", Desc: false}},
	}
	compareOldVsNewForOverClause(t, "PARTITION BY dept, sal ORDER BY id", over, allRows)
}

// TestWindowPartitionCacheReusedAcrossMultipleFuncCalls confirms two different
// window-function call sites with the same PARTITION BY/ORDER BY shape reuse
// one partition build. Frames and function names do not affect the partition
// rows or their ordering, so sharing is safe and avoids a second sort.
func TestWindowPartitionCacheReusedAcrossMultipleFuncCalls(t *testing.T) {
	allRows := buildPartitionCacheFixture()
	over1 := &OverClause{PartitionBy: []Expr{newVarRef("dept")}, OrderBy: []OrderItem{{Col: "sal", Desc: true}}}
	over2 := &OverClause{
		PartitionBy: []Expr{newVarRef("dept")},
		OrderBy:     []OrderItem{{Col: "sal", Desc: true}},
		Frame: &WindowFrame{
			Mode:      "ROWS",
			StartType: "UNBOUNDED_PRECEDING",
			EndType:   "CURRENT",
		},
	}
	ex1 := &FuncCall{Name: "ROW_NUMBER", Over: over1}
	ex2 := &FuncCall{Name: "RANK", Over: over2}

	env := ExecEnv{windowRows: allRows, windowPartitions: newWindowPartitionCache()}
	for i, row := range allRows {
		env.windowIndex = i
		rows1, idx1 := resolveWindowPartition(env, ex1, allRows, row)
		rows2, idx2 := resolveWindowPartition(env, ex2, allRows, row)
		// Same OVER clause shape and same current row -> same partition
		// content and same resolved position from the shared cache entry.
		if !idsEqual(rowIDs(rows1), rowIDs(rows2)) {
			t.Errorf("row id=%v: shared cache entries for equivalent OVER clauses diverged: %v vs %v",
				row["id"], rowIDs(rows1), rowIDs(rows2))
		}
		if idx1 != idx2 {
			t.Errorf("row id=%v: equivalent OVER clauses resolved different positions: %d vs %d", row["id"], idx1, idx2)
		}
	}
	if got := len(env.windowPartitions.entries); got != 3 {
		t.Errorf("expected 3 cache entries (one shared shape x 3 partitions), got %d", got)
	}
}

// TestWindowPartitionCacheKeepsDifferentShapesSeparate ensures sharing is
// based on both PARTITION BY and ORDER BY. The two calls visit the same three
// department partitions, but their opposite sort directions must not reuse a
// row order or current-row position from the other shape.
func TestWindowPartitionCacheKeepsDifferentShapesSeparate(t *testing.T) {
	allRows := buildPartitionCacheFixture()
	desc := &FuncCall{
		Name: "LAG",
		Over: &OverClause{PartitionBy: []Expr{newVarRef("dept")}, OrderBy: []OrderItem{{Col: "sal", Desc: true}}},
	}
	asc := &FuncCall{
		Name: "LEAD",
		Over: &OverClause{PartitionBy: []Expr{newVarRef("dept")}, OrderBy: []OrderItem{{Col: "sal", Desc: false}}},
	}

	env := ExecEnv{windowRows: allRows, windowPartitions: newWindowPartitionCache()}
	sawDifferentOrder := false
	for i, row := range allRows {
		env.windowIndex = i
		descRows, _ := resolveWindowPartition(env, desc, allRows, row)
		ascRows, _ := resolveWindowPartition(env, asc, allRows, row)
		if !idsEqual(rowIDs(descRows), rowIDs(ascRows)) {
			sawDifferentOrder = true
		}
	}
	if !sawDifferentOrder {
		t.Fatal("fixture did not produce a distinguishable ascending/descending partition")
	}
	if got := len(env.windowPartitions.entries); got != 6 {
		t.Errorf("expected 6 cache entries (2 shapes x 3 partitions), got %d", got)
	}
}
