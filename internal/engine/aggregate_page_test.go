package engine

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func BenchmarkGroupedAggregatePage(b *testing.B) {
	for _, tc := range []struct{ name, suffix string }{
		{"Unordered", ""},
		{"Having", " HAVING SUM(val) > 10000"},
		{"OrderedControl", " ORDER BY id DESC"},
	} {
		b.Run(tc.name, func(b *testing.B) {
			db := setupPerfTable(b, 20000)
			b.Cleanup(func() { _ = db.Close() })
			runBench(b, db, "SELECT id, COUNT(*) AS n, SUM(val) AS total FROM t GROUP BY id"+tc.suffix+" LIMIT 20 OFFSET 10000")
		})
	}
}

func TestGroupedAggregatePagesMatchCompleteResult(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	execSQL(t, db, "CREATE TABLE group_pages (id INT, grp INT, val INT)")
	table, err := db.Get("default", "group_pages")
	if err != nil {
		t.Fatal(err)
	}
	for i := range 4099 {
		var value any = i
		if i%11 == 0 {
			value = nil
		}
		table.Rows = append(table.Rows, []any{i, i % 17, value})
	}
	table.Version++
	for _, query := range []string{
		"SELECT grp, COUNT(*) AS n, SUM(val) AS total, AVG(val) AS mean FROM group_pages GROUP BY grp",
		"SELECT grp, MIN(val) AS lo, MAX(val) AS hi FROM group_pages GROUP BY grp",
		"SELECT grp, COUNT(*) AS n FROM group_pages GROUP BY grp HAVING COUNT(*) > 241",
		"SELECT grp, SUM(val) AS total FROM group_pages GROUP BY grp HAVING SUM(val) > 0 ORDER BY total DESC",
		"SELECT COUNT(*) AS n FROM group_pages WHERE id < 0",
		"SELECT grp, COUNT(*) AS n FROM group_pages WHERE id < 0 GROUP BY grp",
	} {
		complete := execSQL(t, db, query)
		for _, bounds := range [][2]int{{0, 0}, {1, 0}, {3, 2}, {20, 0}, {3, 17}, {3, 10000}} {
			limit, offset := bounds[0], bounds[1]
			page := execSQL(t, db, fmt.Sprintf("%s LIMIT %d OFFSET %d", query, limit, offset))
			start := min(offset, len(complete.Rows))
			end := start + min(limit, len(complete.Rows)-start)
			if !reflect.DeepEqual(page.Cols, complete.Cols) || !reflect.DeepEqual(page.Rows, complete.Rows[start:end]) {
				t.Fatalf("%s bounds=%v: got %#v, want %#v", query, bounds, page.Rows, complete.Rows[start:end])
			}
		}
		page := execSQL(t, db, query+" OFFSET 2")
		if !reflect.DeepEqual(page.Rows, complete.Rows[min(2, len(complete.Rows)):]) {
			t.Fatalf("offset-only query %s: got %#v", query, page.Rows)
		}
	}
}

func TestGroupedAggregatePageStillEvaluatesAllInput(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	execSQL(t, db, "CREATE TABLE group_page_errors (grp INT, val INT)")
	execSQL(t, db, "INSERT INTO group_page_errors VALUES (1, 2), (2, 0)")
	for _, query := range []string{
		"SELECT grp, SUM(1 / val) FROM group_page_errors GROUP BY grp",
		"SELECT grp, SUM(val) FROM group_page_errors GROUP BY grp HAVING 1 / SUM(val) > 0",
	} {
		for _, page := range []string{"LIMIT 0", "LIMIT 1", "LIMIT 1 OFFSET 100"} {
			sql := query + " " + page
			_, err := Execute(t.Context(), db, "default", mustParse(sql))
			if err == nil {
				t.Fatalf("%s suppressed the error from a later group", sql)
			}
		}
	}
}
