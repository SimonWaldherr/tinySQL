package engine

import (
	"fmt"
	"math/rand"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func followTestTable(t *testing.T, rows int) (*storage.DB, *storage.Table) {
	t.Helper()
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	execSQL(t, db, `CREATE TABLE follow_t (id INT, src INT, body TEXT)`)
	for i := 0; i < rows; i++ {
		execSQL(t, db, fmt.Sprintf(`INSERT INTO follow_t VALUES (%d, %d, 'common word%d term%d alpha beta gamma delta epsilon zeta eta theta iota kappa lambda mu nu xi omicron')`, i, i/10, i%13, i%7))
	}
	table, err := db.Get("default", "follow_t")
	if err != nil {
		t.Fatal(err)
	}
	return db, table
}

func followFresh(table *storage.Table, cols []int) *storage.FTSIndex {
	cold := storage.NewTable("follow_cold", table.Cols, false)
	cold.Rows = table.Rows
	index := &storage.FTSIndex{}
	ftsExtendPersistent(cold, cols, index)
	return index
}

// A few deleted rows are applied to the index in place; so many that a quarter
// of the arenas would be obsolete rebuild it.
func TestFTSFollowDeleteKeepsIndexUntilTooMuchIsStale(t *testing.T) {
	db, table := followTestTable(t, 400)
	cols := []int{2}
	_ = getFTSDocCache("default", table, cols)
	index := table.FTSIndexes["2"]

	execSQL(t, db, `DELETE FROM follow_t WHERE src = 3`)
	execSQL(t, db, `DELETE FROM follow_t WHERE id = 399 OR id = 0`)
	entry := getFTSDocCache("default", table, cols)
	if table.FTSIndexes["2"] != index {
		t.Fatal("deleting a dozen rows rebuilt the index")
	}
	if len(table.Rows) != 388 || index.BuiltRows != 388 || entry.numDocs != 388 || entry.docFreq("common") != 388 {
		t.Fatalf("rows=%d built=%d docs=%d common=%d", len(table.Rows), index.BuiltRows, entry.numDocs, entry.docFreq("common"))
	}
	if index.StructVersion != table.StructVersion() || index.Version != table.Version {
		t.Fatal("the followed index does not carry the table's stamps")
	}
	ftsCheckInvariants(t, index, 388, "after a small delete")
	ftsCompareCanonical(t, index, followFresh(table, cols), "after a small delete")

	// Rows appended afterwards, then a delete in front of them.
	execSQL(t, db, `INSERT INTO follow_t VALUES (1000, 90, 'common freshterm'), (1001, 90, 'freshterm other')`)
	execSQL(t, db, `DELETE FROM follow_t WHERE id = 5 OR id = 1000`)
	entry = getFTSDocCache("default", table, cols)
	if table.FTSIndexes["2"] != index {
		t.Fatal("a delete behind an append rebuilt the index")
	}
	if entry.docFreq("freshterm") != 1 || entry.docFreq("common") != 387 {
		t.Fatalf("freshterm=%d common=%d", entry.docFreq("freshterm"), entry.docFreq("common"))
	}
	ftsCheckInvariants(t, index, len(table.Rows), "after delete and append")
	ftsCompareCanonical(t, index, followFresh(table, cols), "after delete and append")

	// Almost half the corpus: the obsolete arena entries pass the 25 percent
	// rule, so a rebuild is cheaper than keeping them.
	execSQL(t, db, `DELETE FROM follow_t WHERE src < 17`)
	_ = getFTSDocCache("default", table, cols)
	rebuilt := table.FTSIndexes["2"]
	if rebuilt == index {
		t.Fatal("an index with a quarter of its arenas obsolete was kept")
	}
	if rebuilt.StaleTokens != 0 || rebuilt.StaleTerms != 0 {
		t.Fatalf("the rebuilt index is not compact: %d/%d", rebuilt.StaleTokens, rebuilt.StaleTerms)
	}
	ftsCheckInvariants(t, rebuilt, len(table.Rows), "after a heavy delete")
	ftsCompareCanonical(t, rebuilt, followFresh(table, cols), "after a heavy delete")
}

// An index whose structure does not match what the log says is dropped, never
// patched further: a half-edited index (stamps cleared) and a posting list that
// lacks a document the delete removes.
func TestFTSFollowDiscardsAnIndexThatDoesNotAddUp(t *testing.T) {
	cols := []int{2}

	t.Run("half edited", func(t *testing.T) {
		db, table := followTestTable(t, 120)
		_ = getFTSDocCache("default", table, cols)
		index := table.FTSIndexes["2"]
		index.Version, index.StructVersion = -1, -1 // what an interrupted pass leaves
		index.SetVerified(false)
		execSQL(t, db, `DELETE FROM follow_t WHERE src = 2`)
		_ = getFTSDocCache("default", table, cols)
		if table.FTSIndexes["2"] == index {
			t.Fatal("a half-edited index was reused")
		}
		ftsCheckInvariants(t, table.FTSIndexes["2"], len(table.Rows), "after an interrupted pass")
		ftsCompareCanonical(t, table.FTSIndexes["2"], followFresh(table, cols), "after an interrupted pass")
	})

	t.Run("missing posting", func(t *testing.T) {
		db, table := followTestTable(t, 120)
		_ = getFTSDocCache("default", table, cols)
		index := table.FTSIndexes["2"]
		rows, counts := index.Postings["alpha"], index.PostingCounts["alpha"]
		for i, row := range rows { // drop document 25 from one list, keeping the lists aligned
			if row == 25 {
				index.Postings["alpha"] = append(rows[:i:i], rows[i+1:]...)
				index.PostingCounts["alpha"] = append(counts[:i:i], counts[i+1:]...)
				ftsRebuildPostingBlocks(index, "alpha", 0)
				break
			}
		}
		execSQL(t, db, `DELETE FROM follow_t WHERE id >= 20 AND id < 30`)
		_ = getFTSDocCache("default", table, cols)
		if table.FTSIndexes["2"] == index {
			t.Fatal("an index missing a posting for a deleted document was patched")
		}
		ftsCheckInvariants(t, table.FTSIndexes["2"], len(table.Rows), "after a damaged index")
		ftsCompareCanonical(t, table.FTSIndexes["2"], followFresh(table, cols), "after a damaged index")
	})
}

// UPDATEs and DELETEs between two searches are applied together, including an
// update of a row that is deleted afterwards and of a row that shifted.
func TestFTSFollowMixesUpdatesAndDeletes(t *testing.T) {
	db, table := followTestTable(t, 300)
	cols := []int{2}
	_ = getFTSDocCache("default", table, cols)
	index := table.FTSIndexes["2"]
	execSQL(t, db, `UPDATE follow_t SET body = 'rewritten uniqueone' WHERE id = 150`)
	execSQL(t, db, `UPDATE follow_t SET body = 'rewritten uniquetwo' WHERE id = 20`)
	execSQL(t, db, `DELETE FROM follow_t WHERE id >= 10 AND id < 20`)
	execSQL(t, db, `UPDATE follow_t SET body = 'rewritten uniquethree' WHERE id = 250`)
	execSQL(t, db, `DELETE FROM follow_t WHERE id = 20 OR id = 299`)
	execSQL(t, db, `UPDATE follow_t SET src = 77 WHERE id = 100`)
	entry := getFTSDocCache("default", table, cols)
	if table.FTSIndexes["2"] != index {
		t.Fatal("updates and deletes together rebuilt the index")
	}
	if entry.docFreq("uniqueone") != 1 || entry.docFreq("uniquetwo") != 0 || entry.docFreq("uniquethree") != 1 || entry.docFreq("rewritten") != 2 {
		t.Fatalf("one=%d two=%d three=%d rewritten=%d", entry.docFreq("uniqueone"), entry.docFreq("uniquetwo"), entry.docFreq("uniquethree"), entry.docFreq("rewritten"))
	}
	ftsCheckInvariants(t, index, len(table.Rows), "mixed")
	ftsCompareCanonical(t, index, followFresh(table, cols), "mixed")
}

// A large vocabulary and long lists take the parallel paths of the removal pass
// (renumbering by term ranges, block bounds per list); the result is the same
// index a cold build gives.
func TestFTSApplyRemovalsParallelPathsMatchColdBuild(t *testing.T) {
	previous := runtime.GOMAXPROCS(4)
	defer runtime.GOMAXPROCS(previous)
	rng := rand.New(rand.NewSource(5))
	const docs = 3000
	table := storage.NewTable("fz", []storage.Column{{Name: "body", Type: storage.TextType}}, false)
	for i := 0; i < docs; i++ {
		var sb strings.Builder
		for c := 0; c < 30; c++ { // the same thirty words in every document: long lists
			fmt.Fprintf(&sb, "common%d ", c)
		}
		for k := 0; k < 20; k++ { // plus a long tail of rare words: many short lists
			fmt.Fprintf(&sb, "rare%d ", rng.Intn(40000))
		}
		table.Rows = append(table.Rows, []any{sb.String()})
	}
	cols := []int{0}
	index := &storage.FTSIndex{}
	ftsExtendPersistent(table, cols, index)
	if len(index.Postings) < 8192 {
		t.Fatalf("only %d terms; the parallel renumbering needs more", len(index.Postings))
	}
	var dead []int
	for p := 0; p < docs; p++ {
		if p < 5 || p%37 == 0 || p > docs-4 {
			dead = append(dead, p)
		}
	}
	if !ftsApplyRemovals(index, dead) {
		t.Fatal("removal refused")
	}
	survivors := table.Rows[:0:0]
	for p, row := range table.Rows {
		if _, gone := slices.BinarySearch(dead, p); !gone {
			survivors = append(survivors, row)
		}
	}
	index.BuiltRows = len(survivors)
	index.AvgDocLen = index.TotalDocLen / float64(index.NumDocs)
	index.SetVerified(true)
	cold := storage.NewTable("fz", table.Cols, false)
	cold.Rows = survivors
	fresh := &storage.FTSIndex{}
	ftsExtendPersistent(cold, cols, fresh)
	ftsCheckInvariants(t, index, len(survivors), "parallel removal")
	ftsCompareCanonical(t, index, fresh, "parallel removal")
}
