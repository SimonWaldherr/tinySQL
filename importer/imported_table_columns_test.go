package importer

// Regression test: a table created by an import must resolve its own column
// names through storage.Table.ColIndex.
//
// createTable used to build the table with a struct literal, which left the
// private column-position map nil. Ordinary SELECT and INSERT still worked
// because the executor resolves names by scanning Cols, so this stayed hidden —
// but ColIndex is how features resolve a column up front, and it returned
// "unknown column" for every column of every imported table. VEC_SEARCH,
// FTS_SEARCH and HYBRID_SEARCH all go through it, so vector and full-text
// queries over imported data failed outright. That combination (imported corpus
// + vector/FTS retrieval) is exactly the RAG and geo workflow the importers
// exist to feed.

import (
	"context"
	"strings"
	"testing"

	tinysql "github.com/SimonWaldherr/tinySQL"
)

func TestImportedTableResolvesColumnsByName(t *testing.T) {
	ctx := context.Background()
	db := tinysql.NewDB()
	csv := "id,name,note\n1,alice,hello world\n2,bob,goodbye world\n"
	if _, err := ImportCSV(ctx, db, "default", "people", strings.NewReader(csv), &ImportOptions{CreateTable: true}); err != nil {
		t.Fatalf("import csv: %v", err)
	}
	tbl, err := db.Get("default", "people")
	if err != nil {
		t.Fatal(err)
	}
	for _, col := range []string{"id", "name", "note", "NAME", "Note"} {
		if _, err := tbl.ColIndex(col); err != nil {
			t.Errorf("ColIndex(%q) on an imported table: %v", col, err)
		}
	}
	if _, err := tbl.ColIndex("absent"); err == nil {
		t.Error("ColIndex should still reject a column that does not exist")
	}

	// FTS_SEARCH resolves its text columns through ColIndex, so this is the
	// user-visible consequence of the bug rather than a restatement of it.
	stmt, err := tinysql.ParseSQL(`SELECT id, _fts_score FROM FTS_SEARCH('people', 'world', 5, 'note')`)
	if err != nil {
		t.Fatal(err)
	}
	rs, err := tinysql.Execute(ctx, db, "default", stmt)
	if err != nil {
		t.Fatalf("FTS_SEARCH over an imported table: %v", err)
	}
	if len(rs.Rows) != 2 {
		t.Errorf("FTS_SEARCH returned %d rows, want 2", len(rs.Rows))
	}
}
