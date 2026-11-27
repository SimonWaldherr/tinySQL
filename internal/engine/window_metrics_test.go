package engine

import (
	"database/sql"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"slices"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
	_ "modernc.org/sqlite"
)

func TestWindowMetricTotalsMatchScalar(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	execSQL(t, db, `CREATE TABLE metric_totals (id INT, series TEXT, value INT)`)
	table, err := db.Get("default", "metric_totals")
	if err != nil {
		t.Fatal(err)
	}
	addRows := func(series string, values ...any) {
		for i := range 160 {
			table.Rows = append(table.Rows, []any{i, series, values[i%len(values)]})
		}
	}
	addRows("integer", -7, int64(5), 3, nil, -2)
	addRows("null", nil)
	// An intermediate overflow promotes SUM even if later terms undo it.
	addRows("overflow", int64(math.MaxInt64), 1, -1, nil)
	addRows("float", 1e16, 1.0, -1e16)
	addRows("decimal", big.NewRat(1, 3), 1, nil)
	table.Version++

	checkRows := func(label string, got, want []Row) {
		t.Helper()
		if len(got) != len(want) {
			t.Fatalf("%s: got %d rows, want %d", label, len(got), len(want))
		}
		for i := range got {
			if !reflect.DeepEqual(got[i], want[i]) {
				t.Fatalf("%s row %d: got %v, want %v", label, i, got[i], want[i])
			}
		}
	}
	for _, frame := range []string{
		"ROWS BETWEEN 75 PRECEDING AND 7 FOLLOWING",
		"ROWS BETWEEN 65 FOLLOWING AND 140 FOLLOWING",
		"RANGE BETWEEN 75 PRECEDING AND 7 FOLLOWING",
	} {
		for _, direction := range []string{"ASC", "DESC"} {
			over := " OVER (PARTITION BY series ORDER BY id " + direction + " " + frame + ")"
			got := execSQL(t, db, "SELECT SUM(value)"+over+" AS total, AVG(value)"+over+" AS mean FROM metric_totals")
			want := execSQL(t, db, "SELECT SUM(COALESCE(value, NULL))"+over+" AS total, AVG(COALESCE(value, NULL))"+over+" AS mean FROM metric_totals")
			checkRows(direction+" "+frame, got.Rows, want.Rows)
		}
	}
	const over = " OVER (PARTITION BY series ORDER BY id DESC)"
	got := execSQL(t, db, "SELECT MOVING_SUM(100, value)"+over+" AS total, MOVING_AVG(100, value)"+over+" AS mean FROM metric_totals")
	want := execSQL(t, db, "SELECT MOVING_SUM(100, COALESCE(value, NULL))"+over+" AS total, MOVING_AVG(100, COALESCE(value, NULL))"+over+" AS mean FROM metric_totals")
	checkRows("moving totals", got.Rows, want.Rows)
}

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

func TestWindowValuePeerFrameSkipsIntegerRangeCache(t *testing.T) {
	rows := []Row{{"time": 1, "value": 3}, {"time": 1, "value": 4}, {"time": 2, "value": 5}}
	entry := &windowPartitionEntry{rows: rows}
	call := &FuncCall{
		Args: []Expr{newVarRef("value")},
		Over: &OverClause{
			OrderBy: []OrderItem{{Col: "time"}},
			Frame:   &WindowFrame{Mode: "RANGE", StartType: "CURRENT", EndType: "CURRENT"},
		},
	}
	env := ExecEnv{ctx: t.Context()}
	first, firstErr := evalWindowFrameValue(env, call, rows, 1, entry, false)
	last, lastErr := evalWindowFrameValue(env, call, rows, 0, entry, true)
	if first != 3 || last != 4 || firstErr != nil || lastErr != nil {
		t.Fatalf("peer frame values = %v, %v (%v, %v)", first, last, firstErr, lastErr)
	}
	if entry.integerRangeChecked {
		t.Fatal("peer frame prepared unused integer range keys")
	}
}
