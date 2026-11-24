package engine

import (
	"reflect"
	"strconv"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// Producers of UPDATE deltas must name exactly the columns whose cells they
// changed (or none, meaning "unknown"): a derived structure over another column
// relies on it to skip the row.
func TestUpdateRecordsChangedColumnSet(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE mask_docs (id INT, loaded_at INT, content TEXT, tag TEXT)`)
	for i := 0; i < 6; i++ {
		execSQL(t, db, `INSERT INTO mask_docs VALUES (`+strconv.Itoa(i)+`, 1, 'alpha beta', 'x')`)
	}
	table, err := db.Get("default", "mask_docs")
	if err != nil {
		t.Fatal(err)
	}
	colID, _ := table.ColIndex("id")
	colLoaded, _ := table.ColIndex("loaded_at")
	colContent, _ := table.ColIndex("content")
	colTag, _ := table.ColIndex("tag")

	// rowsFor reports what a structure over column col would have to revisit.
	rowsFor := func(since int, col int) []int {
		t.Helper()
		rows, ok := table.UpdatedRowsSinceCols(since, storage.ColumnBit(col))
		if !ok {
			t.Fatalf("history since %d unavailable", since)
		}
		return rows
	}

	// A fast-path point update of one column.
	since := table.StructVersion()
	execSQL(t, db, `UPDATE mask_docs SET loaded_at = 2 WHERE id = 3`)
	if got := rowsFor(since, colLoaded); !reflect.DeepEqual(got, []int{3}) {
		t.Fatalf("loaded_at update: loaded_at rows = %v, want [3]", got)
	}
	if got := rowsFor(since, colContent); len(got) != 0 {
		t.Fatalf("loaded_at update: content rows = %v, want none", got)
	}

	// A whole-table update of two columns (the raw fast path with arena).
	since = table.StructVersion()
	execSQL(t, db, `UPDATE mask_docs SET loaded_at = 3, tag = 'y'`)
	if got := rowsFor(since, colContent); len(got) != 0 {
		t.Fatalf("loaded_at+tag update: content rows = %v, want none", got)
	}
	if got := rowsFor(since, colTag); len(got) != 6 {
		t.Fatalf("loaded_at+tag update: tag rows = %v, want 6 rows", got)
	}
	if got := rowsFor(since, colID); len(got) != 0 {
		t.Fatalf("loaded_at+tag update: id rows = %v, want none", got)
	}

	// An update that sets a column to its present value changes nothing: no
	// delta at all for the unchanged columns, and none for the statement.
	since = table.StructVersion()
	execSQL(t, db, `UPDATE mask_docs SET content = 'alpha beta', loaded_at = 3`)
	if table.StructVersion() != since {
		t.Fatalf("a no-op update advanced structVersion %d -> %d", since, table.StructVersion())
	}

	// A mixed statement: the row whose content really changes is reported for
	// content, the rest only for loaded_at.
	since = table.StructVersion()
	execSQL(t, db, `UPDATE mask_docs SET content = CASE WHEN id = 2 THEN 'gamma' ELSE content END, loaded_at = 4`)
	if got := rowsFor(since, colContent); !reflect.DeepEqual(got, []int{2}) {
		t.Fatalf("mixed update: content rows = %v, want [2]", got)
	}
	if got := rowsFor(since, colLoaded); len(got) != 6 {
		t.Fatalf("mixed update: loaded_at rows = %v, want 6 rows", got)
	}
}

// The general path (a trigger forces it) records the same sets, and a trigger
// that rewrites the row without changing any SET column reports "unknown".
func TestGeneralUpdatePathRecordsChangedColumnSet(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE gmask (id INT, loaded_at INT, content TEXT)`)
	execSQL(t, db, `CREATE TABLE gmask_log (id INT)`)
	execSQL(t, db, `INSERT INTO gmask VALUES (1, 1, 'alpha'), (2, 1, 'beta')`)
	execSQL(t, db, `CREATE TRIGGER gmask_after AFTER UPDATE ON gmask BEGIN INSERT INTO gmask_log VALUES (NEW.id); END`)
	table, err := db.Get("default", "gmask")
	if err != nil {
		t.Fatal(err)
	}
	colLoaded, _ := table.ColIndex("loaded_at")
	colContent, _ := table.ColIndex("content")

	since := table.StructVersion()
	execSQL(t, db, `UPDATE gmask SET loaded_at = 5 WHERE id = 1`)
	rows, ok := table.UpdatedRowsSinceCols(since, storage.ColumnBit(colContent))
	if !ok || len(rows) != 0 {
		t.Fatalf("triggered loaded_at update: content rows = %v/%v, want none", rows, ok)
	}
	rows, ok = table.UpdatedRowsSinceCols(since, storage.ColumnBit(colLoaded))
	if !ok || !reflect.DeepEqual(rows, []int{0}) {
		t.Fatalf("triggered loaded_at update: loaded_at rows = %v/%v, want [0]", rows, ok)
	}

	// Setting a column to its present value still fires the trigger, replaces
	// the row, and so must read as "unknown", never as "no column changed".
	since = table.StructVersion()
	execSQL(t, db, `UPDATE gmask SET loaded_at = 5 WHERE id = 1`)
	rows, ok = table.UpdatedRowsSinceCols(since, storage.ColumnBit(colContent))
	if !ok || !reflect.DeepEqual(rows, []int{0}) {
		t.Fatalf("trigger-only no-op update: content rows = %v/%v, want [0] (unknown mask)", rows, ok)
	}
}

// ON CONFLICT DO UPDATE does not name its columns and stays "unknown".
func TestUpsertUpdateKeepsUnknownColumnSet(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE umask (id INT PRIMARY KEY, loaded_at INT, content TEXT)`)
	execSQL(t, db, `INSERT INTO umask VALUES (1, 1, 'alpha')`)
	table, err := db.Get("default", "umask")
	if err != nil {
		t.Fatal(err)
	}
	colContent, _ := table.ColIndex("content")
	since := table.StructVersion()
	execSQL(t, db, `INSERT INTO umask VALUES (1, 2, 'alpha') ON CONFLICT (id) DO UPDATE SET loaded_at = 2`)
	rows, ok := table.UpdatedRowsSinceCols(since, storage.ColumnBit(colContent))
	if !ok || !reflect.DeepEqual(rows, []int{0}) {
		t.Fatalf("upsert: content rows = %v/%v, want [0] (unknown mask)", rows, ok)
	}
}

// A SET column whose new value merely equals the stored one (an int cell in a
// FLOAT column, 1 and 1.0) is left out of the column mask, so the cell itself
// must stay exactly as stored: a derived index over that column keeps serving
// what it indexed. Both UPDATE paths.
func TestUpdateKeepsStoredCellOfEqualValueInMixedStatement(t *testing.T) {
	for _, tc := range []struct {
		name, sql string
	}{
		{"general path", `UPDATE mask_rep SET f = 1, other = 9 WHERE id >= 0 OR id < 0`},
		{"simple path", `UPDATE mask_rep SET f = 1, other = 9`},
		{"point path", `UPDATE mask_rep SET f = 1, other = 9 WHERE id = 1`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := storage.NewDB()
			execSQL(t, db, `CREATE TABLE mask_rep (id INT, f FLOAT, other INT)`)
			execSQL(t, db, `INSERT INTO mask_rep VALUES (1, 1.5, 0)`)
			table, err := db.Get("default", "mask_rep")
			if err != nil {
				t.Fatal(err)
			}
			table.Rows[0][1] = int(1) // equal to 1.0, but not the same representation
			since := table.StructVersion()
			execSQL(t, db, tc.sql)
			if got, ok := table.Rows[0][1].(int); !ok || got != 1 {
				t.Fatalf("stored cell is %T(%v) after the update, want the untouched int(1)", table.Rows[0][1], table.Rows[0][1])
			}
			colF, _ := table.ColIndex("f")
			colOther, _ := table.ColIndex("other")
			if rows, ok := table.UpdatedRowsSinceCols(since, storage.ColumnBit(colF)); !ok || len(rows) != 0 {
				t.Fatalf("f was reported changed: %v/%v", rows, ok)
			}
			if rows, ok := table.UpdatedRowsSinceCols(since, storage.ColumnBit(colOther)); !ok || len(rows) != 1 {
				t.Fatalf("other not reported: %v/%v", rows, ok)
			}
		})
	}
}
