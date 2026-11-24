package engine

import (
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func reviewTable(t *testing.T, docs int, body func(i int) string) (*storage.DB, *storage.Table) {
	t.Helper()
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	execSQL(t, db, `CREATE TABLE ev (id INT, body TEXT)`)
	var values []string
	for i := 0; i < docs; i++ {
		values = append(values, fmt.Sprintf("(%d, '%s')", i, body(i)))
	}
	execSQL(t, db, `INSERT INTO ev VALUES `+strings.Join(values, ", "))
	table, err := db.Get("default", "ev")
	if err != nil {
		t.Fatal(err)
	}
	return db, table
}

// reviewCold builds the index of table's current rows from scratch.
func reviewCold(table *storage.Table, cols []int) *storage.FTSIndex {
	cold := storage.NewTable(table.Name, table.Cols, false)
	for _, row := range table.Rows {
		cold.Rows = append(cold.Rows, slices.Clone(row))
	}
	fresh := &storage.FTSIndex{}
	ftsExtendPersistent(cold, cols, fresh)
	return fresh
}

// Terms that no document has any more keep an empty posting list; once a quarter
// of the vocabulary is dead weight the index is rebuilt instead.
func TestFTSFollowRebuildsWhenAQuarterOfTheVocabularyIsDead(t *testing.T) {
	cols := []int{1}
	key := ftsColsCacheKey(cols)
	body := func(i int) string {
		if i < 2000 {
			var sb strings.Builder
			for k := 0; k < 60; k++ {
				fmt.Fprintf(&sb, "common%d ", (i*7+k*13)%50)
			}
			return sb.String()
		}
		return fmt.Sprintf("unique%d", i) // one term that only this document has
	}
	db, table := reviewTable(t, 3200, body)
	search := func() {
		execSQL(t, db, `SELECT id FROM FTS_SEARCH('ev', 'common3', 5, 'body')`)
	}
	search()
	before := table.FTSIndexes[key]

	// 100 dead terms are tolerated: the index is carried forward.
	execSQL(t, db, `DELETE FROM ev WHERE id >= 3100`)
	search()
	if table.FTSIndexes[key] != before {
		t.Fatal("a handful of dead terms made the index rebuild")
	}
	ftsCheckInvariants(t, before, len(table.Rows), "few dead terms")

	// 1100 more are more than a quarter of the vocabulary: rebuilt.
	execSQL(t, db, `DELETE FROM ev WHERE id >= 2000`)
	search()
	after := table.FTSIndexes[key]
	if after == before {
		t.Fatal("an index with mostly dead terms was kept")
	}
	if len(after.TermIDs) > 60 {
		t.Fatalf("the rebuilt index still holds %d terms", len(after.TermIDs))
	}
	ftsCheckInvariants(t, after, len(table.Rows), "rebuilt")
	ftsCompareCanonical(t, after, reviewCold(table, cols), "rebuilt")
}

// A failure in the middle of the removal pass must neither hold the table's
// derived-data lock nor leave an index that is still believed.
func TestFTSFollowSurvivesAPanicInTheRemovalPass(t *testing.T) {
	cols := []int{1}
	key := ftsColsCacheKey(cols)
	t.Cleanup(func() { ftsApplyRemovalsHook = nil })
	for step := 1; step <= 5; step++ {
		db, table := reviewTable(t, 1500, func(i int) string {
			return fmt.Sprintf("alpha%d beta%d gamma w%d", i%11, i%5, i%97)
		})
		execSQL(t, db, `SELECT id FROM FTS_SEARCH('ev', 'alpha3', 5, 'body')`)
		before := table.FTSIndexes[key]
		execSQL(t, db, `DELETE FROM ev WHERE id % 50 = 7`)

		ftsApplyRemovalsHook = func(n int) {
			if n == step {
				panic("injected failure")
			}
		}
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("step %d: the injected panic did not reach the caller (removal not followed?)", step)
				}
			}()
			getFTSDocCache("default", table, cols)
		}()
		ftsApplyRemovalsHook = nil
		if before.Version != -1 || before.StructVersion != -1 || before.Verified() {
			t.Fatalf("step %d: the half-edited index still claims to be current: version %d/%d verified %v", step, before.Version, before.StructVersion, before.Verified())
		}

		done := make(chan struct{})
		go func() {
			defer close(done)
			getFTSDocCache("default", table, cols)
		}()
		select {
		case <-done:
		case <-time.After(20 * time.Second):
			t.Fatalf("step %d: the derived-data lock is still held after the panic", step)
		}
		index := table.FTSIndexes[key]
		if index == before {
			t.Fatalf("step %d: the half-edited index was reused", step)
		}
		ftsCheckInvariants(t, index, len(table.Rows), fmt.Sprintf("step %d", step))
		ftsCompareCanonical(t, index, reviewCold(table, cols), fmt.Sprintf("step %d", step))
	}
}

// The change log folds its older half when it fills up. A fold of UPDATEs cannot
// name rows, so UPDATEs of the indexed column inside one send the index to a
// rebuild; a removal is kept whole and a fold of UPDATEs to other columns hides
// nothing, so neither does.
func TestFTSFollowAcrossAFoldedChangeLog(t *testing.T) {
	cols := []int{1}
	key := ftsColsCacheKey(cols)
	body := func(i int) string { return fmt.Sprintf("alpha%d beta%d gamma w%d", i%11, i%5, i%97) }
	noise := func(table *storage.Table, col int) {
		mask := storage.ColumnMask(col)
		for i := 0; i < 70_000; i++ {
			table.MarkRowUpdatedCols(i%len(table.Rows), mask)
		}
	}
	for _, tc := range []struct {
		name        string
		order       func(db *storage.DB, table *storage.Table)
		wantFollowd bool
	}{
		{"removal, then enough updates of another column to fold it", func(db *storage.DB, table *storage.Table) {
			execSQL(t, db, `DELETE FROM ev WHERE id % 40 = 3`)
			noise(table, 0)
		}, true},
		{"removal, then enough updates of the indexed column to fold it", func(db *storage.DB, table *storage.Table) {
			execSQL(t, db, `DELETE FROM ev WHERE id % 40 = 3`)
			noise(table, 1)
		}, false},
		{"updates of another column, then a removal", func(db *storage.DB, table *storage.Table) {
			noise(table, 0)
			execSQL(t, db, `DELETE FROM ev WHERE id % 40 = 3`)
		}, true},
		{"updates of the indexed column, then a removal", func(db *storage.DB, table *storage.Table) {
			noise(table, 1)
			execSQL(t, db, `DELETE FROM ev WHERE id % 40 = 3`)
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, table := reviewTable(t, 800, body)
			execSQL(t, db, `SELECT id FROM FTS_SEARCH('ev', 'alpha3', 5, 'body')`)
			before := table.FTSIndexes[key]
			tc.order(db, table)
			execSQL(t, db, `SELECT id FROM FTS_SEARCH('ev', 'alpha3', 5, 'body')`)
			after := table.FTSIndexes[key]
			if followed := after == before; followed != tc.wantFollowd {
				t.Fatalf("index followed = %v, want %v", followed, tc.wantFollowd)
			}
			ftsCheckInvariants(t, after, len(table.Rows), tc.name)
			ftsCompareCanonical(t, after, reviewCold(table, cols), tc.name)
		})
	}
}

func TestFTSRunWorkersReraisesAPanicOnTheCaller(t *testing.T) {
	var recovered any
	func() {
		defer func() { recovered = recover() }()
		var n atomic.Int32
		ftsRunWorkers(4, func() {
			if n.Add(1) == 1 {
				panic("worker failure")
			}
		})
	}()
	if recovered != "worker failure" {
		t.Fatalf("recovered %v", recovered)
	}
}
