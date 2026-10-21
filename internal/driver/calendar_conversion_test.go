package driver

import (
	"database/sql"
	"testing"
	"time"
)

func TestSQLPreparedCalendarConversion(t *testing.T) {
	db := orderByFixture(t)
	stmt, err := db.Prepare(`SELECT FROM_CALENDAR(?, ?), CALENDAR_CONVERT(?, ?, ?), TO_CALENDAR(?, ?) FROM t WHERE id = 1`)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	for _, tc := range []struct {
		input, source, target, want string
	}{
		{"2024-02-16", "julian", "discordian", "3190-StTibs"},
		{"3190-StTibs", "discordian", "ordinal", "2024-060"},
		{"1445-08-19", "islamic_civil", "gregorian", "2024-02-29"},
	} {
		var date time.Time
		var converted, formatted string
		err := stmt.QueryRow(tc.input, tc.source, tc.input, tc.source, tc.target, "2024-02-29", tc.target).Scan(&date, &converted, &formatted)
		if err != nil {
			t.Fatal(err)
		}
		if date.Format(time.RFC3339) != "2024-02-29T00:00:00Z" || converted != tc.want || formatted != tc.want {
			t.Fatalf("%s: date=%v converted=%q formatted=%q; want %q", tc.source, date, converted, formatted, tc.want)
		}
	}
	var date sql.NullTime
	var converted, formatted sql.NullString
	if err := stmt.QueryRow(nil, "julian", nil, "julian", "gregorian", nil, "gregorian").Scan(&date, &converted, &formatted); err != nil {
		t.Fatal(err)
	}
	if date.Valid || converted.Valid || formatted.Valid {
		t.Fatal("NULL calendar dates must remain NULL")
	}
}
