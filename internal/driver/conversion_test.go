package driver

import (
	"database/sql"
	"math"
	"testing"
)

func TestSQLPreparedConversions(t *testing.T) {
	db := orderByFixture(t)
	stmt, err := db.Prepare(`SELECT CONVERT(TEXT, TRY_CONVERT(DATE, ?, ?), 23), TRY_CONVERT(BIGINT, ?), DATEPART(iso_year, CONVERT(DATE, ?, 23)) FROM t WHERE id = 1`)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	for _, tc := range []struct {
		date, number, isoDate string
		style                 int
		wantDate              sql.NullString
		wantNumber            sql.NullInt64
		wantISOYear           int
	}{
		{"31.12.2024", "42", "2021-01-01", 104, sql.NullString{String: "2024-12-31", Valid: true}, sql.NullInt64{Int64: 42, Valid: true}, 2020},
		{"bad", "bad", "2025-01-01", 104, sql.NullString{}, sql.NullInt64{}, 2025},
		{"02/29/2024", "9223372036854775807", "2024-12-31", 101, sql.NullString{String: "2024-02-29", Valid: true}, sql.NullInt64{Int64: math.MaxInt64, Valid: true}, 2025},
	} {
		var date sql.NullString
		var number sql.NullInt64
		var isoYear int
		if err := stmt.QueryRow(tc.date, tc.style, tc.number, tc.isoDate).Scan(&date, &number, &isoYear); err != nil {
			t.Fatal(err)
		}
		if date != tc.wantDate || number != tc.wantNumber || isoYear != tc.wantISOYear {
			t.Fatalf("date=%v number=%v ISO year=%d; want %v, %v, %d", date, number, isoYear, tc.wantDate, tc.wantNumber, tc.wantISOYear)
		}
	}
}
