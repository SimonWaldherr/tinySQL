package engine

import (
	"fmt"
	"testing"
)

// A window that removed only rows the flat cache and the graph never covered (an
// import appended rows, then cleaned them up again before anyone searched) is the
// append-and-update case: both are extended, not rebuilt from scratch.
func TestDeleteOfRowsNoStructureCoveredKeepsFlatCacheAndGraph(t *testing.T) {
	for _, withUpdate := range []bool{false, true} {
		t.Run(fmt.Sprintf("update=%v", withUpdate), func(t *testing.T) {
			f := newHF(t, 1600, "cosine")
			idx0, cache0 := f.warm()
			for i := 0; i < 6; i++ {
				f.exec(fmt.Sprintf(`INSERT INTO hf VALUES (%d, 9999, %s)`, 90000+i, irLiteral(i)))
			}
			if withUpdate {
				f.exec(fmt.Sprintf(`UPDATE hf SET embedding = %s WHERE id = 17`, irLiteral(99)))
			}
			f.exec(`DELETE FROM hf WHERE src = 9999`)
			if got := len(f.table.Rows); got != 1600 {
				t.Fatalf("the table has %d rows, want 1600", got)
			}
			// Another statement that appends, so the extension has rows to read.
			f.exec(fmt.Sprintf(`INSERT INTO hf VALUES (91000, 9998, %s)`, irLiteral(5)))

			followsBefore := vecHNSWFollowCount.Load()
			idx1, cache1 := f.warm()
			if vecHNSWFollowCount.Load() != followsBefore {
				t.Fatal("the graph went through the removal pass although none of its nodes was removed")
			}
			if &cache0.segments[0].data[0] != &cache1.segments[0].data[0] {
				t.Fatal("the flat cache was rebuilt although every row it covered is where it was")
			}
			vcCheckEntry(t, cache1, len(f.table.Rows), "flat cache")
			vcCompareEntries(t, cache1, buildVecColumnCache(f.table, hfEmbedding, true), "flat cache")
			if idx1 != idx0 {
				t.Fatal("the graph was rebuilt although every node it holds is where it was")
			}
			hnswCheckInvariants(t, idx1, f.table, cache1, "extended graph")
			if withUpdate && len(idx1.deltaRows) == 0 {
				t.Fatal("the updated vector was not registered as a delta row")
			}
		})
	}
}
