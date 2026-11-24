package engine

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func deleteLogTable(t *testing.T, rows int) (*storage.DB, *storage.Table) {
	t.Helper()
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	execSQL(t, db, `CREATE TABLE dlog (id INT, grp TEXT, body TEXT)`)
	for i := 0; i < rows; i++ {
		execSQL(t, db, fmt.Sprintf(`INSERT INTO dlog VALUES (%d, '%c', 'row %d')`, i, 'a'+rune(i%3), i))
	}
	table, err := db.Get("default", "dlog")
	if err != nil {
		t.Fatal(err)
	}
	return db, table
}

// A DELETE that names its rows is one event of the change log: the rows it
// removed are Dead, the rest are renumbered, whichever executor path ran it.
func TestDeleteReportsItsRemovedRows(t *testing.T) {
	for _, tc := range []struct {
		name, where string
		indexed     bool
	}{
		{"raw predicate", `grp = 'b'`, false},
		{"raw predicate with an index", `grp = 'c'`, true},
		{"complex predicate", `(id * 2) % 5 = 1 OR id > 17`, false},
		{"range", `id >= 3 AND id < 9`, false},
		{"nothing matches", `id < 0`, false},
		{"everything matches", `id >= 0`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, table := deleteLogTable(t, 20)
			if tc.indexed {
				execSQL(t, db, `CREATE INDEX dlog_grp ON dlog(grp)`)
			}
			var want []int
			for _, row := range execSQL(t, db, `SELECT id FROM dlog WHERE `+tc.where+` ORDER BY id`).Rows {
				want = append(want, row["id"].(int))
			}
			version := table.StructVersion()
			execSQL(t, db, `DELETE FROM dlog WHERE `+tc.where)
			got, ok := table.ChangesSince(version, 20, 0)
			if !ok {
				t.Fatal("the removal was not reported")
			}
			if !reflect.DeepEqual(append([]int(nil), got.Dead...), append([]int(nil), want...)) || got.Survivors != 20-len(want) {
				t.Fatalf("dead = %v (survivors %d), want %v", got.Dead, got.Survivors, want)
			}
			// The first Survivors rows are the surviving rows in their old order.
			pos := 0
			for id := 0; id < 20; id++ {
				if deleteLogContains(want, id) {
					continue
				}
				if table.Rows[pos][0] != id {
					t.Fatalf("row %d is id %v, want %d", pos, table.Rows[pos][0], id)
				}
				pos++
			}
			if len(want) == 0 && table.StructVersion() != version {
				t.Fatal("a DELETE that matched nothing moved structVersion")
			}
		})
	}
}

func deleteLogContains(list []int, v int) bool {
	for _, x := range list {
		if x == v {
			return true
		}
	}
	return false
}

// Deletes that cannot report their rows keep dropping the log.
func TestUnreportableDeletesDropTheChangeLog(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup []string
		stmt  string
	}{
		{"no WHERE", nil, `DELETE FROM dlog`},
		{"trigger", []string{`CREATE TABLE dlog_audit (id INT)`, `CREATE TRIGGER dlog_trg AFTER DELETE ON dlog BEGIN INSERT INTO dlog_audit VALUES (OLD.id); END`}, `DELETE FROM dlog WHERE grp = 'b'`},
		{"trigger and complex predicate", []string{`CREATE TABLE dlog_audit (id INT)`, `CREATE TRIGGER dlog_trg AFTER DELETE ON dlog BEGIN INSERT INTO dlog_audit VALUES (OLD.id); END`}, `DELETE FROM dlog WHERE (id * 2) % 5 = 1`},
		{"primary key point delete", []string{`CREATE TABLE dlog_pk (id INT PRIMARY KEY, grp TEXT)`, `INSERT INTO dlog_pk VALUES (1,'a'),(2,'b'),(3,'c')`}, `DELETE FROM dlog_pk WHERE id = 2`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, table := deleteLogTable(t, 12)
			for _, stmt := range tc.setup {
				execSQL(t, db, stmt)
			}
			if tc.name == "primary key point delete" {
				table, _ = db.Get("default", "dlog_pk")
			}
			version := table.StructVersion()
			rows := len(table.Rows)
			execSQL(t, db, tc.stmt)
			if table.StructVersion() == version {
				t.Fatal("the delete did not move structVersion")
			}
			if _, ok := table.ChangesSince(version, rows, 0); ok {
				t.Fatal("an unreportable delete left a usable change log")
			}
		})
	}
}

// A table with a PRIMARY KEY and no secondary index gets the single-row DELETE
// snapshot for DELETE ... WHERE id = k, although RETURNING (or a predicate that is
// not a plain comparison) sends the statement through the general scan, which
// closes the gap and logs a removal. A rollback must restore the old order and
// leave neither the removal in the log nor a full-text index that follows it.
func TestRolledBackPrimaryKeyDeleteThroughTheGeneralScan(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	execSQL(t, db, `CREATE TABLE pkdel (id INT PRIMARY KEY, body TEXT)`)
	for i := 0; i < 6; i++ {
		execSQL(t, db, fmt.Sprintf(`INSERT INTO pkdel VALUES (%d, 'word%d common')`, i, i))
	}
	table, err := db.Get("default", "pkdel")
	if err != nil {
		t.Fatal(err)
	}
	ids := func() []int {
		var out []int
		for _, r := range table.Rows {
			out = append(out, r[0].(int))
		}
		return out
	}
	search := func(q string) []any {
		var out []any
		for _, row := range execSQL(t, db, fmt.Sprintf(`SELECT id FROM FTS_SEARCH('pkdel', '%s', 10, 'body') ORDER BY id`, q)).Rows {
			out = append(out, row["id"])
		}
		return out
	}
	if got := search("word2"); !reflect.DeepEqual(got, []any{2}) {
		t.Fatalf("setup: %v", got)
	}
	for _, stmt := range []string{
		`DELETE FROM pkdel WHERE id = 2 RETURNING missing`,
		`DELETE FROM pkdel WHERE id = 2 AND LENGTH(body) > 0 RETURNING missing`,
	} {
		version := table.StructVersion()
		if _, err := Execute(t.Context(), db, "default", mustParse(stmt)); err == nil {
			t.Fatalf("%s: expected a projection error", stmt)
		}
		if got := ids(); !reflect.DeepEqual(got, []int{0, 1, 2, 3, 4, 5}) {
			t.Fatalf("%s: rows after the rollback: %v", stmt, got)
		}
		if got, ok := table.ChangesSince(version, 6, 0); ok {
			t.Fatalf("%s: the rolled back removal is still in the log: %+v", stmt, got)
		}
		for i := 0; i < 6; i++ {
			if got := search(fmt.Sprintf("word%d", i)); !reflect.DeepEqual(got, []any{i}) {
				t.Fatalf("%s: word%d finds %v after the rollback", stmt, i, got)
			}
		}
	}
}

// A statement that fails after deleting is rolled back; the removal it logged
// must not describe the restored rows.
func TestRolledBackDeleteLeavesNoRemovalInTheLog(t *testing.T) {
	db, table := deleteLogTable(t, 10)
	version := table.StructVersion()
	if _, err := Execute(t.Context(), db, "default", mustParse(`DELETE FROM dlog WHERE grp = 'a' RETURNING missing`)); err == nil {
		t.Fatal("expected a projection error")
	}
	if len(table.Rows) != 10 {
		t.Fatalf("rollback left %d rows", len(table.Rows))
	}
	if got, ok := table.ChangesSince(version, 10, 0); ok {
		t.Fatalf("the rolled back removal is still in the log: %+v", got)
	}
	// A rolled back UPDATE only over-reports.
	version = table.StructVersion()
	execSQL(t, db, `UPDATE dlog SET body = 'x' WHERE id = 4`)
	got, ok := table.ChangesSince(version, 10, storage.ColumnMask(2))
	if !ok || !reflect.DeepEqual(got.Updated, []int{4}) {
		t.Fatalf("update after the rollback: %+v ok=%v", got, ok)
	}
}
