package engine

import (
	"database/sql"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
	_ "modernc.org/sqlite"
)

func TestWindowFramesAgreeWithSQLite(t *testing.T) {
	db := storage.NewDB()
	defer db.Close()
	reference, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer reference.Close()
	reference.SetMaxOpenConns(1)
	setup := []string{`CREATE TABLE frame_oracle (id INT, grp INT, k INT, v INT, keep INT)`}
	for i := 0; i < 16; i++ {
		key := fmt.Sprint(i * 7 % 9)
		value := fmt.Sprint(i * 3 % 11)
		if i%5 == 0 {
			key = "NULL"
		}
		if i%7 == 0 {
			value = "NULL"
		}
		setup = append(setup, fmt.Sprintf("INSERT INTO frame_oracle VALUES (%d,%d,%s,%s,%d)", i, i%2, key, value, i%3))
	}
	for _, s := range setup {
		execSQL(t, db, s)
		if _, err := reference.Exec(s); err != nil {
			t.Fatal(err)
		}
	}
	for _, order := range []string{"k ASC NULLS FIRST", "k ASC NULLS LAST", "k DESC NULLS FIRST", "k DESC NULLS LAST"} {
		for _, frame := range []string{
			"", "RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW",
			"RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING",
			"RANGE BETWEEN 2 PRECEDING AND 1 FOLLOWING",
			"RANGE BETWEEN 1 FOLLOWING AND 2 FOLLOWING",
			"RANGE BETWEEN 2 PRECEDING AND 1 PRECEDING",
			"ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING",
			"ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING",
			"ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING",
		} {
			over := "OVER (PARTITION BY grp ORDER BY " + order + " " + frame + ")"
			query := "SELECT id,COUNT(*) " + over + " AS n,SUM(v) FILTER (WHERE keep=1) " + over + " AS s," +
				"FIRST_VALUE(v) " + over + " AS f,LAST_VALUE(v) " + over + " AS l FROM frame_oracle ORDER BY id"
			got := execSQL(t, db, query)
			rows, err := reference.Query(query)
			if err != nil {
				t.Fatal(err)
			}
			i := 0
			for rows.Next() {
				var values [5]any
				if err := rows.Scan(&values[0], &values[1], &values[2], &values[3], &values[4]); err != nil {
					t.Fatal(err)
				}
				if i >= len(got.Rows) {
					t.Fatalf("missing row for %s", query)
				}
				for j, col := range []string{"id", "n", "s", "f", "l"} {
					if fmt.Sprint(got.Rows[i][col]) != fmt.Sprint(values[j]) {
						t.Fatalf("%s row %d %s: got %v, SQLite %v", query, i, col, got.Rows[i][col], values[j])
					}
				}
				i++
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			rows.Close()
			if i != len(got.Rows) {
				t.Fatalf("row count for %s: %d != %d", query, len(got.Rows), i)
			}
		}
	}
}

func TestWindowRangeRejectsNonfiniteKeys(t *testing.T) {
	db := storage.NewDB()
	defer db.Close()
	execSQL(t, db, `CREATE TABLE invalid_range (k FLOAT, v INT)`)
	table, _ := db.Get("default", "invalid_range")
	for _, key := range []any{math.NaN(), math.Inf(1), math.Inf(-1), "text"} {
		table.Rows = [][]any{{key, 1}}
		table.Version++
		_, err := Execute(t.Context(), db, "default", mustParse(`SELECT SUM(v) OVER (ORDER BY k RANGE 1 PRECEDING) FROM invalid_range`))
		if err == nil || !strings.Contains(err.Error(), "numeric ORDER BY") {
			t.Fatalf("key %v: %v", key, err)
		}
	}
}
