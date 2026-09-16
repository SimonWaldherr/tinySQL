package engine

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestCountStarCardinality(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE counts (id INT, val FLOAT)`)
	execSQL(t, db, `INSERT INTO counts VALUES (1, 2), (2, NULL), (3, 2)`)
	execSQL(t, db, `CREATE TABLE empty_counts (id INT, val FLOAT)`)

	for _, tt := range []struct {
		name string
		sql  string
		rows []Row
	}{
		{"empty", `SELECT COUNT(*) AS n FROM empty_counts`, []Row{{"n": 0}}},
		{"all-rows-including-null", `SELECT COUNT(*) AS n FROM counts`, []Row{{"n": 3}}},
		{"multiple-counts", `SELECT COUNT(*) AS n, COUNT(*) AS total FROM counts`, []Row{{"n": 3, "total": 3}}},
		{"having-match", `SELECT COUNT(*) AS n FROM counts HAVING COUNT(*) = 3`, []Row{{"n": 3}}},
		{"having-no-match", `SELECT COUNT(*) AS n FROM counts HAVING COUNT(*) > 3`, nil},
		{"having-empty-match", `SELECT COUNT(*) AS n FROM empty_counts HAVING COUNT(*) = 0`, []Row{{"n": 0}}},
		{"having-empty-no-match", `SELECT COUNT(*) AS n FROM empty_counts HAVING COUNT(*) > 0`, nil},
		{"order-limit", `SELECT COUNT(*) AS n FROM counts ORDER BY n DESC LIMIT 1`, []Row{{"n": 3}}},
		{"limit-zero", `SELECT COUNT(*) AS n FROM counts LIMIT 0`, nil},
		{"offset", `SELECT COUNT(*) AS n FROM counts LIMIT 1 OFFSET 1`, nil},
		{"offset-empty", `SELECT COUNT(*) AS n FROM empty_counts LIMIT 1 OFFSET 1`, nil},
		{"filtered", `SELECT COUNT(*) AS n FROM counts WHERE id > 1`, []Row{{"n": 2}}},
		{"filtered-empty", `SELECT COUNT(*) AS n FROM counts WHERE id > 3`, []Row{{"n": 0}}},
		{"grouped", `SELECT id, COUNT(*) AS n FROM counts GROUP BY id ORDER BY id`, []Row{{"id": 1, "n": 1}, {"id": 2, "n": 1}, {"id": 3, "n": 1}}},
		{"mixed-counts", `SELECT COUNT(*) AS n, COUNT(val) AS populated, COUNT(DISTINCT val) AS unique_values FROM counts`, []Row{{"n": 3, "populated": 2, "unique_values": 1}}},
		{"mixed-aggregates", `SELECT COUNT(*) AS n, SUM(val) AS total, AVG(val) AS mean FROM counts`, []Row{{"n": 3, "total": float64(4), "mean": float64(2)}}},
		{"mixed-empty", `SELECT COUNT(*) AS n, SUM(val) AS total FROM empty_counts`, []Row{{"n": 0, "total": nil}}},
		{"count-null-expression", `SELECT COUNT(*) AS n, COUNT(NULL) AS missing FROM counts`, []Row{{"n": 3, "missing": 0}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			rs := execSQL(t, db, tt.sql)
			if len(rs.Rows) != len(tt.rows) {
				t.Fatalf("rows = %#v, want %#v", rs.Rows, tt.rows)
			}
			for i := range tt.rows {
				if len(rs.Rows[i]) != len(tt.rows[i]) {
					t.Fatalf("row %d = %#v, want %#v", i, rs.Rows[i], tt.rows[i])
				}
				for col, want := range tt.rows[i] {
					got, present := rs.Rows[i][col]
					equal := reflect.DeepEqual(got, want)
					if col == "id" {
						// The grouped source key has SQL numeric semantics;
						// this test concerns counts, not its Go integer width.
						number, ok := numeric(got)
						equal = ok && number == float64(want.(int))
					}
					if !present || !equal {
						t.Errorf("row %d column %q = %v (%T), want %v (%T)", i, col, got, got, want, want)
					}
				}
			}
		})
	}
}

func TestCountStarCardinalityPreparedAfterChanges(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE counts (id INT, val FLOAT)`)
	stmt := mustParse(`SELECT COUNT(*) AS n FROM counts`)

	for _, tt := range []struct {
		change string
		count  int
	}{
		{"", 0},
		{`INSERT INTO counts VALUES (1, 2), (2, NULL), (3, 4)`, 3},
		{`UPDATE counts SET val = 5 WHERE id = 1`, 3},
		{`DELETE FROM counts WHERE id = 2`, 2},
		{`INSERT INTO counts VALUES (4, NULL)`, 3},
		{`DELETE FROM counts`, 0},
	} {
		if tt.change != "" {
			execSQL(t, db, tt.change)
		}
		rs, err := Execute(context.Background(), db, "default", stmt)
		if err != nil {
			t.Fatal(err)
		}
		if got := firstRow(t, rs)["n"]; got != tt.count {
			t.Errorf("after %q: count = %v, want %d", tt.change, got, tt.count)
		}
	}
}

func TestCountStarCardinalityHonorsCanceledContext(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE counts (id INT)`)
	stmt := mustParse(`SELECT COUNT(*) AS n FROM counts`).(*Select)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	env := ExecEnv{ctx: ctx, db: db, tenant: "default"}

	// Exercise the aggregate executor itself: bypassing its old scan must not
	// bypass cancellation, even when the table contains no records.
	_, used, err := executeSimpleAggregateFastPath(env, stmt)
	if !used || !errors.Is(err, context.Canceled) {
		t.Fatalf("used = %v, error = %v; want handled context.Canceled", used, err)
	}
}
