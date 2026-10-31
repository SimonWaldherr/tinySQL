package driver

import (
	"database/sql"
	"testing"
)

func TestPreparedFilteredWindowAndDateDiff(t *testing.T) {
	db := orderByFixture(t)
	stmt, err := db.Prepare(`SELECT COUNT(*) FILTER (WHERE score>=?) AS n,
		SUM(id) FILTER (WHERE score>=?) AS total FROM t`)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	for _, tc := range []struct {
		threshold, n, total int
		valid               bool
	}{
		{0, 3, 6, true}, {20, 2, 4, true}, {99, 0, 0, false}, {10, 3, 6, true},
	} {
		var n int
		var total sql.NullInt64
		if err := stmt.QueryRow(tc.threshold, tc.threshold).Scan(&n, &total); err != nil {
			t.Fatal(err)
		}
		if n != tc.n || total.Valid != tc.valid || total.Valid && total.Int64 != int64(tc.total) {
			t.Fatalf("threshold %d: %d %v", tc.threshold, n, total)
		}
	}
	window, err := db.Prepare(`SELECT id,COUNT(DISTINCT score) FILTER (WHERE score>=?) OVER () AS n
		FROM t ORDER BY id NULLS LAST`)
	if err != nil {
		t.Fatal(err)
	}
	defer window.Close()
	for _, threshold := range []int{0, 20, 99, 0} {
		rows, err := window.Query(threshold)
		if err != nil {
			t.Fatal(err)
		}
		count := 0
		for rows.Next() {
			var id, n int
			if err := rows.Scan(&id, &n); err != nil {
				t.Fatal(err)
			}
			want := 3
			if threshold == 20 {
				want = 2
			}
			if threshold == 99 {
				want = 0
			}
			if n != want {
				t.Fatalf("window threshold %d: %d", threshold, n)
			}
			count++
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		rows.Close()
		if count != 3 {
			t.Fatal(count)
		}
	}
	dates, err := db.Prepare(`SELECT DATEDIFF(MONTH,?,?),TIMESTAMPDIFF(MONTH,?,?),ISODOW(?)`)
	if err != nil {
		t.Fatal(err)
	}
	defer dates.Close()
	var boundaries, elapsed, weekday int
	if err := dates.QueryRow("2024-01-31", "2024-02-29", "2024-01-31", "2024-02-29", "2024-03-01").Scan(&boundaries, &elapsed, &weekday); err != nil {
		t.Fatal(err)
	}
	if boundaries != 1 || elapsed != 0 || weekday != 5 {
		t.Fatalf("dates: %d %d %d", boundaries, elapsed, weekday)
	}
}
