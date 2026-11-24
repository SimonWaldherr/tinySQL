package engine

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func ftsMaskTestTable(t *testing.T, rows int) (*storage.DB, *storage.Table) {
	t.Helper()
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE fts_mask (id INT, loaded_at INT, body TEXT)`)
	for i := 0; i < rows; i++ {
		execSQL(t, db, fmt.Sprintf(`INSERT INTO fts_mask VALUES (%d, 1, 'common term%d word%d filler text')`, i, i%17, i%5))
	}
	table, err := db.Get("default", "fts_mask")
	if err != nil {
		t.Fatal(err)
	}
	return db, table
}

func ftsIndexShape(index *storage.FTSIndex) [5]int {
	return [5]int{len(index.DocTokenIDs), len(index.DocTermIDs), index.StaleTokens, index.StaleTerms, index.NumDocs}
}

// An UPDATE of a column the index does not cover keeps the index as it is,
// whatever the number of rows, and only moves its stamps.
func TestFTSIndexSurvivesUpdateOfOtherColumns(t *testing.T) {
	db, table := ftsMaskTestTable(t, 300)
	cols := []int{2}
	_ = getFTSDocCache("default", table, cols)
	index := table.FTSIndexes["2"]
	shape := ftsIndexShape(index)

	// More rows than the legacy log held and more than the refresh limit.
	execSQL(t, db, `UPDATE fts_mask SET loaded_at = 2`)
	for i := 0; i < 5000; i++ {
		execSQL(t, db, fmt.Sprintf(`UPDATE fts_mask SET loaded_at = %d WHERE id = %d`, 3+i, i%300))
	}
	entry := getFTSDocCache("default", table, cols)
	if table.FTSIndexes["2"] != index {
		t.Fatal("an update of another column replaced the FTS index")
	}
	if got := ftsIndexShape(index); got != shape {
		t.Fatalf("index arenas changed: %v -> %v", shape, got)
	}
	if index.StructVersion != table.StructVersion() || index.Version != table.Version {
		t.Fatalf("stamps not moved: index (%d,%d) table (%d,%d)", index.Version, index.StructVersion, table.Version, table.StructVersion())
	}
	if entry.docFreq("common") != 300 || entry.numDocs != 300 {
		t.Fatalf("entry wrong: common=%d docs=%d", entry.docFreq("common"), entry.numDocs)
	}
	checkPostingMetadata(t, entry)

	// Appended rows after the touch batch are still picked up.
	execSQL(t, db, `INSERT INTO fts_mask VALUES (1000, 1, 'brandnew common')`)
	execSQL(t, db, `UPDATE fts_mask SET loaded_at = 9 WHERE id < 10`)
	entry = getFTSDocCache("default", table, cols)
	if table.FTSIndexes["2"] != index {
		t.Fatal("append plus touch replaced the FTS index")
	}
	if entry.docFreq("brandnew") != 1 || entry.docFreq("common") != 301 || entry.numDocs != 301 {
		t.Fatalf("appended row missing: brandnew=%d common=%d docs=%d", entry.docFreq("brandnew"), entry.docFreq("common"), entry.numDocs)
	}
	checkPostingMetadata(t, entry)
}

// An UPDATE of an indexed column still reaches the index, incrementally for a
// few rows and by rebuild for many, and a rewrite with the same text is free.
func TestFTSIndexRefreshesUpdatesOfIndexedColumn(t *testing.T) {
	db, table := ftsMaskTestTable(t, 300)
	cols := []int{2}
	_ = getFTSDocCache("default", table, cols)
	index := table.FTSIndexes["2"]

	execSQL(t, db, `UPDATE fts_mask SET body = 'replaced zebra only' WHERE id = 7`)
	entry := getFTSDocCache("default", table, cols)
	if table.FTSIndexes["2"] != index {
		t.Fatal("a one-row update rebuilt the index instead of refreshing it")
	}
	if entry.docFreq("zebra") != 1 || entry.docFreq("common") != 299 {
		t.Fatalf("refresh wrong: zebra=%d common=%d", entry.docFreq("zebra"), entry.docFreq("common"))
	}
	checkPostingMetadata(t, entry)

	// Many rows: the index is rebuilt rather than patched row by row.
	execSQL(t, db, `UPDATE fts_mask SET body = 'wholesale rewrite'`)
	entry = getFTSDocCache("default", table, cols)
	if table.FTSIndexes["2"] == index {
		t.Fatal("a rewrite of every row patched the index instead of rebuilding it")
	}
	if entry.docFreq("wholesale") != 300 || entry.docFreq("common") != 0 || entry.docFreq("zebra") != 0 {
		t.Fatalf("rebuild wrong: wholesale=%d common=%d zebra=%d", entry.docFreq("wholesale"), entry.docFreq("common"), entry.docFreq("zebra"))
	}
	checkPostingMetadata(t, entry)
}

// ftsRefreshUpdatedRows leaves a row alone whose tokens did not change, and keeps
// its counters exact for the rows that did.
func TestFTSRefreshSkipsRowsWithUnchangedTokens(t *testing.T) {
	table := storage.NewTable("fts_skip", []storage.Column{{Name: "body", Type: storage.TextType}}, false)
	table.Rows = [][]any{{"alpha beta"}, {"beta gamma"}, {"gamma delta delta"}, {nil}}
	table.Version = 1
	_ = getFTSDocCache("fts-skip", table, []int{0})
	index := table.FTSIndexes["0"]
	docs := append([]storage.FTSDocument(nil), index.Docs...)

	// Row 0: identical text. Row 1: same terms, different order. Row 2: same
	// tokens with different case and punctuation. Row 3: still nothing.
	table.Rows[0] = []any{"alpha beta"}
	table.Rows[1] = []any{"gamma beta"}
	table.Rows[2] = []any{"GAMMA, delta delta!"}
	table.Rows[3] = []any{""}
	before := ftsIndexShape(index)
	ftsRefreshUpdatedRows(table, []int{0}, index, []int{0, 1, 2, 3})

	if index.Docs[0] != docs[0] || index.Docs[2] != docs[2] || index.Docs[3] != docs[3] {
		t.Fatalf("unchanged rows were re-indexed: %v vs %v", index.Docs, docs)
	}
	if index.Docs[1] == docs[1] {
		t.Fatal("a reordered row was skipped, phrase positions would be stale")
	}
	after := ftsIndexShape(index)
	if after[2] != before[2]+2 || after[3] != before[3]+2 {
		t.Fatalf("stale counters not exact: %v -> %v (row 1 had 2 tokens and 2 terms)", before, after)
	}
	if index.NumDocs != 3 || index.TotalDocLen != 7 {
		t.Fatalf("NumDocs=%d TotalDocLen=%v, want 3 and 7", index.NumDocs, index.TotalDocLen)
	}
	checkPostingMetadata(t, ftsCacheFromPersistent(table, index))

	// The refreshed index equals a cold build in everything a query can see.
	cold := &storage.FTSIndex{}
	ftsExtendPersistent(table, []int{0}, cold)
	got, want := ftsCacheFromPersistent(table, index), ftsCacheFromPersistent(table, cold)
	if !reflect.DeepEqual(got.postings, want.postings) || !reflect.DeepEqual(got.postingCounts, want.postingCounts) ||
		got.numDocs != want.numDocs || got.avgDocLen != want.avgDocLen {
		t.Fatalf("refreshed index differs from a cold build:\n got  %v %v\n want %v %v", got.postings, got.postingCounts, want.postings, want.postingCounts)
	}
}
