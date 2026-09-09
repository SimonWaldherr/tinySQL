package engine

import (
	"reflect"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestOrderedDistinctMatchesSubqueryPath(t *testing.T) {
	db := setupDistinctTable(t)
	t.Cleanup(func() { _ = db.Close() })
	for _, query := range []string{
		"SELECT DISTINCT grp FROM d ORDER BY grp DESC LIMIT 2 OFFSET 1",
		"SELECT DISTINCT grp, sub FROM d ORDER BY grp, sub DESC LIMIT 6 OFFSET 3",
		"SELECT DISTINCT val * 2 AS doubled FROM d WHERE id < 30 ORDER BY doubled DESC LIMIT 2",
		"SELECT DISTINCT grp FROM d ORDER BY grp LIMIT 0",
		"SELECT DISTINCT grp FROM d ORDER BY grp LIMIT 3 OFFSET 1000",
		"SELECT DISTINCT grp FROM d WHERE id < 0 ORDER BY grp",
	} {
		got := execSQL(t, db, query)
		general := strings.Replace(query, "FROM d", "FROM (SELECT * FROM d) AS d", 1)
		want := execSQL(t, db, general)
		if !reflect.DeepEqual(got.Cols, want.Cols) || !reflect.DeepEqual(rowsAsKeys(got), rowsAsKeys(want)) {
			t.Fatalf("%s: got %v want %v", query, got, want)
		}
	}
}

func TestSingleTextDistinctPreservesTypeBoundaries(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	execSQL(t, db, "CREATE TABLE mixed_distinct (value TEXT)")
	table, _ := db.Get("default", "mixed_distinct")
	for _, v := range []any{nil, "N;", 1, "I1;", int64(1), "L1;", true, "B1;", "", "N;", nil, []byte{1}, []byte{1}} {
		table.Rows = append(table.Rows, []any{v})
	}
	table.Version++
	got := execSQL(t, db, "SELECT DISTINCT value FROM mixed_distinct")
	all := execSQL(t, db, "SELECT value FROM mixed_distinct")
	want := distinctRows(all.Rows, all.Cols)
	if !reflect.DeepEqual(got.Rows, want) {
		t.Fatalf("got %#v want %#v", got.Rows, want)
	}
}

func TestFilteredOffsetMatchesFullProjection(t *testing.T) {
	db := setupDistinctTable(t)
	t.Cleanup(func() { _ = db.Close() })
	for _, suffix := range []string{"LIMIT 2 OFFSET 7", "LIMIT 0 OFFSET 3", "LIMIT 100 OFFSET 10", "LIMIT 3 OFFSET 1000"} {
		base := "SELECT id, val * 2 AS doubled FROM d WHERE id < 30"
		got := execSQL(t, db, base+" "+suffix)
		want := execSQL(t, db, strings.Replace(base, "FROM d", "FROM (SELECT * FROM d) AS d", 1)+" "+suffix)
		if !reflect.DeepEqual(rowsAsKeys(got), rowsAsKeys(want)) {
			t.Fatalf("%s: got %v want %v", suffix, got, want)
		}
	}
	// Even skipped matches must evaluate projections and report errors.
	if _, err := Execute(t.Context(), db, "default", mustParse("SELECT 1 / id AS quotient FROM d WHERE id < 30 LIMIT 1 OFFSET 10")); err == nil || !strings.Contains(err.Error(), "division by zero") {
		t.Fatalf("skipped projection error: %v", err)
	}
}

func TestRowNumberUnorderedDuplicateRows(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	execSQL(t, db, "CREATE TABLE duplicate_numbers (value INT)")
	execSQL(t, db, "INSERT INTO duplicate_numbers VALUES (1), (1), (1), (1)")
	got := execSQL(t, db, "SELECT ROW_NUMBER() OVER () AS rn FROM duplicate_numbers LIMIT 2 OFFSET 1")
	if len(got.Rows) != 2 || got.Rows[0]["rn"] != 2 || got.Rows[1]["rn"] != 3 {
		t.Fatal(got.Rows)
	}
}

func TestTextPartitionIndexFallbacks(t *testing.T) {
	for _, values := range [][]any{{"a", "b", "a", ""}, {1, int64(1), 2, 1}, {"a", nil, "b", "a"}} {
		rows := make([]Row, len(values))
		for i, v := range values {
			rows[i] = Row{"id": i, "key": v}
		}
		compareOldVsNewForOverClause(t, "partition index", &OverClause{PartitionBy: []Expr{newVarRef("key")}, OrderBy: []OrderItem{{Col: "id", Desc: true}}}, rows)
	}
}
