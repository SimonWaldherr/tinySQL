package driver

import (
	"testing"
	"time"
)

func TestSQLPreparedRawClockRefreshes(t *testing.T) {
	db := orderByFixture(t)
	stmt, err := db.Prepare(`SELECT NOW() AS n, CURRENT_TIMESTAMP() AS c, STRFTIME('%F', ?) AS d FROM t WHERE id > ? AND NOW() = CURRENT_TIMESTAMP()`)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	var previous time.Time
	for _, value := range []string{"2000-01-01", "now", "2024-03-05", "now"} {
		rows, err := stmt.Query(value, 0)
		if err != nil {
			t.Fatal(err)
		}
		var statementTime time.Time
		count := 0
		for rows.Next() {
			var now, current time.Time
			var date string
			if err := rows.Scan(&now, &current, &date); err != nil {
				t.Fatal(err)
			}
			if count == 0 {
				statementTime = now
			}
			if !now.Equal(statementTime) || !current.Equal(statementTime) {
				t.Fatalf("inconsistent clocks: %v, %v, want %v", now, current, statementTime)
			}
			wantDate := value
			if value == "now" {
				wantDate = now.Format("2006-01-02")
			}
			if date != wantDate {
				t.Fatalf("bound date %q formatted as %q, want %q", value, date, wantDate)
			}
			count++
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		rows.Close()
		if count != 3 {
			t.Fatalf("got %d rows, want 3", count)
		}
		if !previous.IsZero() && !statementTime.After(previous) {
			t.Fatalf("clock did not refresh: %v then %v", previous, statementTime)
		}
		previous = statementTime
	}
}
