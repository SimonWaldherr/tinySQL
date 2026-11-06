package engine

import (
	"database/sql"
	"fmt"
	"math"
	"slices"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
	_ "modernc.org/sqlite"
)

func TestWindowMetricCountsAgreeWithSQLite(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	ref, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ref.Close() })
	create := `CREATE TABLE metrics_window (id INT, series INT, value INT)`
	execSQL(t, db, create)
	if _, err := ref.Exec(create); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 320; i++ {
		value := fmt.Sprint(i % 17)
		if i%5 == 0 {
			value = "NULL"
		}
		query := fmt.Sprintf("INSERT INTO metrics_window VALUES (%d,%d,%s)", i, i%2, value)
		execSQL(t, db, query)
		if _, err := ref.Exec(query); err != nil {
			t.Fatal(err)
		}
	}
	for _, frame := range []string{
		"ROWS BETWEEN 99 PRECEDING AND CURRENT ROW",
		"ROWS BETWEEN 80 PRECEDING AND 10 FOLLOWING",
		"ROWS BETWEEN 100 FOLLOWING AND 110 FOLLOWING",
		"ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING",
		"RANGE BETWEEN 99 PRECEDING AND CURRENT ROW",
		"RANGE BETWEEN 80 PRECEDING AND 10 FOLLOWING",
		"RANGE BETWEEN 100 FOLLOWING AND 110 FOLLOWING",
	} {
		for _, direction := range []string{"ASC", "DESC"} {
			query := `SELECT id, COUNT(value) OVER (PARTITION BY series ORDER BY id ` + direction + ` ` + frame + `) AS n FROM metrics_window ORDER BY id`
			got := execSQL(t, db, query)
			rows, err := ref.Query(query)
			if err != nil {
				t.Fatal(err)
			}
			i := 0
			for rows.Next() {
				var id, n int
				if err := rows.Scan(&id, &n); err != nil {
					t.Fatal(err)
				}
				if got.Rows[i]["id"] != id || got.Rows[i]["n"] != n {
					t.Fatalf("%s row %d: %v want %d", query, i, got.Rows[i], n)
				}
				i++
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			rows.Close()
			if i != len(got.Rows) {
				t.Fatalf("row count %d vs %d", i, len(got.Rows))
			}
		}
	}
}

func TestIntegerRangeBoundsMatchGeneral(t *testing.T) {
	for _, desc := range []bool{false, true} {
		values := []int64{math.MinInt64, math.MinInt64 + 1, -5, 0, 0, 5, math.MaxInt64 - 1, math.MaxInt64}
		if desc {
			slices.Reverse(values)
		}
		rows := make([]Row, len(values))
		for i, v := range values {
			rows[i] = Row{"time": v}
		}
		order := []OrderItem{{Col: "time", Desc: desc}}
		for i := range values {
			for _, kind := range []string{"UNBOUNDED_PRECEDING", "OFFSET_PRECEDING", "CURRENT", "OFFSET_FOLLOWING", "UNBOUNDED_FOLLOWING"} {
				for _, offset := range []int{0, 1, 7, math.MaxInt} {
					for _, start := range []bool{false, true} {
						got := intRangeBound(values, i, desc, kind, offset, start)
						want, err := rangeWindowBound(ExecEnv{ctx: t.Context()}, rows, i, order, kind, offset, start)
						if err != nil || got != want {
							t.Fatalf("desc=%v i=%d kind=%s offset=%d start=%v: got %d want %d err %v", desc, i, kind, offset, start, got, want, err)
						}
					}
				}
			}
		}
	}
}
