package engine

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestBoundCaseUpdatePreservesRowsAndIndexes(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	execSQL(t, db, "CREATE TABLE case_updates (id INT PRIMARY KEY, val INT, denominator INT)")
	const size = 130
	values := make([]string, size)
	for i := range values {
		denominator := 2
		if i == size-1 {
			denominator = 0
		}
		values[i] = fmt.Sprintf("(%d,10,%d)", i, denominator)
	}
	execSQL(t, db, "INSERT INTO case_updates VALUES "+strings.Join(values, ","))
	execSQL(t, db, "CREATE INDEX idx_case_val ON case_updates(val)")
	before := execSQL(t, db, "SELECT * FROM case_updates ORDER BY id")
	if _, err := Execute(t.Context(), db, "default", mustParse(
		"UPDATE case_updates SET val = CASE WHEN id = 129 THEN 1 / denominator ELSE val + 1 END")); err == nil {
		t.Fatal("expected a selected branch to fail after earlier rows changed")
	}
	if got := execSQL(t, db, "SELECT * FROM case_updates ORDER BY id"); !reflect.DeepEqual(got, before) {
		t.Fatal("failed CASE update changed stored rows")
	}
	if got := execSQL(t, db, "SELECT COUNT(*) AS n FROM case_updates WHERE val = 10"); got.Rows[0]["n"] != size {
		t.Fatal("failed CASE update changed the secondary index", got)
	}
	// An unsupported unselected arm keeps the interpreter's lazy behavior.
	execSQL(t, db, "UPDATE case_updates SET val = CASE WHEN TRUE THEN val ELSE missing END")
	execSQL(t, db, "UPDATE case_updates SET val = CASE WHEN denominator = 0 THEN val ELSE val / denominator END")
	if got := execSQL(t, db, "SELECT COUNT(*) AS n FROM case_updates WHERE val = 5"); got.Rows[0]["n"] != size-1 {
		t.Fatal("unselected division or coercion changed results", got)
	}
	// Both assignments read the original row, including the original val.
	execSQL(t, db, `UPDATE case_updates SET
		val = CASE WHEN id % 2 = 0 THEN val + 1 ELSE val - 1 END,
		denominator = CASE WHEN val = 5 THEN 1 ELSE 2 END`)
	got := execSQL(t, db, "SELECT id, val, denominator FROM case_updates ORDER BY id")
	for i, row := range got.Rows {
		val, denominator := 5, 1
		if i == size-1 {
			val, denominator = 10, 2
		}
		if i%2 == 0 {
			val++
		} else {
			val--
		}
		if !reflect.DeepEqual(row, Row{"id": i, "val": val, "denominator": denominator}) {
			t.Fatalf("row %d: %v", i, row)
		}
	}
	// A point update still follows the smaller interpreter path.
	execSQL(t, db, "UPDATE case_updates SET val = CASE WHEN denominator = 2 THEN 99 ELSE 0 END WHERE id = 129")
	if got := execSQL(t, db, "SELECT id FROM case_updates WHERE val = 99"); !reflect.DeepEqual(got.Rows, []Row{{"id": 129}}) {
		t.Fatal("point CASE update lost its indexed value", got)
	}
}

func TestBoundCoalesceUpdatePreservesLazinessAndRollback(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	execSQL(t, db, "CREATE TABLE coalesce_updates (id INT PRIMARY KEY, val INT, denominator INT)")
	const size = 130
	values := make([]string, size)
	for i := range values {
		val, denominator := "10", 0
		if i%3 == 0 {
			val, denominator = "NULL", 2
		}
		if i == size-1 {
			denominator = 0
		}
		values[i] = fmt.Sprintf("(%d,%s,%d)", i, val, denominator)
	}
	execSQL(t, db, "INSERT INTO coalesce_updates VALUES "+strings.Join(values, ","))
	execSQL(t, db, "CREATE INDEX idx_coalesce_val ON coalesce_updates(val)")
	before := execSQL(t, db, "SELECT * FROM coalesce_updates ORDER BY id")
	if _, err := Execute(t.Context(), db, "default", mustParse("UPDATE coalesce_updates SET val = COALESCE(val, 10 / denominator)")); err == nil {
		t.Fatal("expected a NULL row's selected fallback to fail")
	}
	if got := execSQL(t, db, "SELECT * FROM coalesce_updates ORDER BY id"); !reflect.DeepEqual(got, before) {
		t.Fatal("failed COALESCE update changed stored rows")
	}
	if got := execSQL(t, db, "SELECT COUNT(*) AS n FROM coalesce_updates WHERE val = 10"); got.Rows[0]["n"] != 86 {
		t.Fatal("failed COALESCE update changed the index", got)
	}
	execSQL(t, db, `UPDATE coalesce_updates SET val =
		COALESCE(val, CASE WHEN denominator = 0 THEN 7 ELSE 10 / denominator END)`)
	got := execSQL(t, db, "SELECT val FROM coalesce_updates ORDER BY id")
	for i, row := range got.Rows {
		want := 10
		if i%3 == 0 {
			want = 5
		}
		if i == size-1 {
			want = 7
		}
		if row["val"] != want {
			t.Fatalf("row %d: got %v, want %d", i, row, want)
		}
	}
	if got := execSQL(t, db, `SELECT SUM(CASE WHEN val IS NULL THEN COALESCE(denominator,1)
		ELSE COALESCE(val,0) END) AS total FROM coalesce_updates`); got.Rows[0]["total"] != 1082 {
		t.Fatal("CASE/COALESCE composition changed the aggregate", got)
	}
}
