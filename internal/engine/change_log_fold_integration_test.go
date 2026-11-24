package engine

import (
	"fmt"
	"strings"
	"testing"
)

// irLiteral is a SQL vector literal of hfDims components derived from seed.
func irLiteral(seed int) string {
	parts := make([]string, hfDims)
	for i := range parts {
		parts[i] = fmt.Sprintf("%d", (seed*7+i*13)%17-8)
	}
	return "'[" + strings.Join(parts, ",") + "]'"
}

// touchUntilFolded runs bookkeeping UPDATEs (the src column, which no search
// structure indexes) until the table's change log has folded its older half.
func touchUntilFolded(f *hfFixture) {
	f.t.Helper()
	start := f.table.StructVersion()
	for f.table.StructVersion()-start < 70000 {
		f.exec(`UPDATE hf SET src = src + 1`)
	}
}

// A DELETE is not forgotten when bookkeeping UPDATEs fill the change log behind
// it: the flat cache and the graph, both stamped before the DELETE, still follow
// it instead of being rebuilt.
func TestRemovalSurvivesALongRunOfBookkeepingUpdates(t *testing.T) {
	f := newHF(t, 1600, "cosine")
	idx, cache0 := f.warm()
	f.exec(`DELETE FROM hf WHERE src = 3`)
	touchUntilFolded(f)

	flat, graph := vecColumnFollowCount.Load(), vecHNSWFollowCount.Load()
	idx1, cache1 := f.warm()
	if vecColumnFollowCount.Load() != flat+1 {
		t.Fatal("the flat cache was rebuilt instead of following the DELETE")
	}
	if vecHNSWFollowCount.Load() != graph+1 || idx1 != idx {
		t.Fatal("the graph was rebuilt instead of following the DELETE")
	}
	if &cache0.segments[0].data[0] != &cache1.segments[0].data[0] {
		t.Fatal("the flat cache copied its packed vectors")
	}
	vcCheckEntry(t, cache1, len(f.table.Rows), "flat cache")
	vcCompareEntries(t, cache1, buildVecColumnCache(f.table, hfEmbedding, true), "flat cache")
	hnswCheckInvariants(t, idx1, f.table, cache1, "followed graph")
}

// An UPDATE of the indexed vector column that ends up inside a folded range cannot
// be reported row by row any more: the structures must not be served from the log
// then, whatever the DELETE and the bookkeeping UPDATEs around it were.
func TestFoldedUpdateOfTheVectorColumnForcesARebuild(t *testing.T) {
	f := newHF(t, 1600, "cosine")
	f.warm()
	f.exec(`DELETE FROM hf WHERE src = 3`)
	f.exec(fmt.Sprintf(`UPDATE hf SET embedding = %s WHERE id = 500`, irLiteral(42)))
	touchUntilFolded(f)

	idx, cache := f.warm()
	vcCheckEntry(t, cache, len(f.table.Rows), "flat cache")
	vcCompareEntries(t, cache, buildVecColumnCache(f.table, hfEmbedding, true), "flat cache")
	hnswCheckInvariants(t, idx, f.table, cache, "graph")
	// The updated row answers to its new vector.
	row := -1
	for i, r := range f.table.Rows {
		if r[0].(int) == 500 {
			row = i
		}
	}
	if row < 0 {
		t.Fatal("row 500 vanished")
	}
	got, _, ok := cache.resolveRow(row, true)
	want := f.table.Rows[row][hfEmbedding].([]float64)
	if !ok || len(got) != len(want) {
		t.Fatalf("cache row %d: ok=%v len=%d", row, ok, len(got))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("cache row %d component %d is %v, the table has %v", row, i, got[i], want[i])
		}
	}
}
