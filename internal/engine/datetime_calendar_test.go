package engine

import (
	"context"
	"testing"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestDateTimeStatementClock(t *testing.T) {
	fixed := time.Date(2024, 3, 31, 23, 59, 58, 123456789, time.FixedZone("test", 5*3600+45*60))
	env := ExecEnv{now: fixed}
	for _, name := range []string{"NOW", "GETDATE", "CURRENT_TIMESTAMP"} {
		got, err := evalExpr(env, &FuncCall{Name: name}, nil)
		if err != nil || got != fixed {
			t.Errorf("%s = %v, %v; want %v", name, got, err, fixed)
		}
	}
	for _, name := range []string{"CURRENT_DATE", "TODAY"} {
		got, err := evalExpr(env, &FuncCall{Name: name}, nil)
		want := time.Date(2024, 3, 31, 0, 0, 0, 0, fixed.Location())
		if err != nil || got != want {
			t.Errorf("%s = %v, %v; want %v", name, got, err, want)
		}
	}
	for _, tc := range []struct {
		sql  string
		want any
	}{
		{`SELECT CURRENT_TIME() AS v`, "23:59:58"},
		{`SELECT DATE() AS v`, "2024-03-31"},
		{`SELECT TIME(' now ') AS v`, "23:59:58"},
		{`SELECT DATETIME('NOW') AS v`, "2024-03-31 23:59:58"},
		{`SELECT UNIXEPOCH() AS v`, fixed.Unix()},
		{`SELECT STRFTIME('%Y-%m-%d %T') AS v`, "2024-03-31 23:59:58"},
		{`SELECT IN_PERIOD('TODAY', NOW()) AS v`, true},
	} {
		rs, err := executeSelect(env, mustParse(tc.sql).(*Select))
		if err != nil {
			t.Fatalf("%s: %v", tc.sql, err)
		}
		if got := rs.Rows[0]["v"]; got != tc.want {
			t.Errorf("%s = %v; want %v", tc.sql, got, tc.want)
		}
	}

	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE clock_rows (id INT)`)
	execSQL(t, db, `INSERT INTO clock_rows VALUES (1), (2), (3)`)
	rs := execSQL(t, db, `SELECT NOW() AS a, CURRENT_TIMESTAMP() AS b FROM clock_rows`)
	want := rs.Rows[0]["a"].(time.Time)
	for _, row := range rs.Rows {
		if row["a"] != want || row["b"] != want {
			t.Fatalf("statement clock changed across expressions or rows: %v", rs.Rows)
		}
	}
}

func TestDateTruncLocalCalendar(t *testing.T) {
	for _, tc := range []struct {
		unit, input, want string
	}{
		{"YEAR", "2024-03-06T16:32:45.123+05:45", "2024-01-01T00:00:00+05:45"},
		{"QUARTER", "2024-05-06T16:32:45+05:45", "2024-04-01T00:00:00+05:45"},
		{"MONTH", "2024-03-06T16:32:45+05:45", "2024-03-01T00:00:00+05:45"},
		{"WEEK", "2024-03-10T16:32:45+05:45", "2024-03-04T00:00:00+05:45"},
		{"WEEK", "2024-01-01T00:01:00-03:30", "2024-01-01T00:00:00-03:30"},
		{"DAY", "2024-03-06T16:32:45+05:45", "2024-03-06T00:00:00+05:45"},
		{"HOUR", "2024-03-06T16:32:45+05:45", "2024-03-06T16:00:00+05:45"},
		{"MINUTE", "2024-03-06T16:32:45.123+05:45", "2024-03-06T16:32:00+05:45"},
		{"SECOND", "2024-03-06T16:32:45.123+05:45", "2024-03-06T16:32:45+05:45"},
	} {
		sql := "SELECT DATE_TRUNC('" + tc.unit + "', '" + tc.input + "') AS v"
		got := evalScalarSQL(t, sql).(time.Time)
		if got.Format(time.RFC3339Nano) != tc.want {
			t.Errorf("%s: got %v, want %s", sql, got, tc.want)
		}
	}
}

func TestDateTruncDST(t *testing.T) {
	for _, tc := range []struct {
		zone, unit, input, want string
	}{
		{"Europe/Berlin", "DAY", "2024-03-31T12:45:00+02:00", "2024-03-31T00:00:00+01:00"},
		{"Europe/Berlin", "WEEK", "2024-03-31T12:45:00+02:00", "2024-03-25T00:00:00+01:00"},
		{"Europe/Berlin", "HOUR", "2024-10-27T02:45:12+02:00", "2024-10-27T02:00:00+02:00"},
		{"Europe/Berlin", "HOUR", "2024-10-27T02:45:12+01:00", "2024-10-27T02:00:00+01:00"},
		{"Australia/Lord_Howe", "HOUR", "2024-04-07T01:45:12+10:30", "2024-04-07T01:00:00+11:00"},
	} {
		loc, err := time.LoadLocation(tc.zone)
		if err != nil {
			t.Fatal(err)
		}
		input, err := time.Parse(time.RFC3339, tc.input)
		if err != nil {
			t.Fatal(err)
		}
		got, err := evalDateTrunc(ExecEnv{}, []Expr{&Literal{Val: tc.unit}, &Literal{Val: input.In(loc)}}, nil)
		if err != nil {
			t.Fatal(err)
		}
		if result := got.(time.Time); result.Format(time.RFC3339) != tc.want || result.After(input) {
			t.Errorf("%s %s %s: got %v, want %s", tc.zone, tc.unit, tc.input, result, tc.want)
		}
	}
	odd := time.Date(1900, 1, 1, 12, 34, 56, 789, time.FixedZone("odd", 20730))
	got := truncateDateClock(odd, time.Minute)
	if got.Minute() != 34 || got.Second() != 0 || got.Nanosecond() != 0 {
		t.Fatalf("minute truncation with second-based offset = %v", got)
	}
}

func TestEOMonthCalendarOffset(t *testing.T) {
	for _, tc := range []struct {
		input, offset, want string
	}{
		{"2024-01-31", "1", "2024-02-29"},
		{"2023-01-31", "1", "2023-02-28"},
		{"2024-03-31", "-1", "2024-02-29"},
		{"2024-02-29", "12", "2025-02-28"},
		{"2024-12-31", "1", "2025-01-31"},
		{"2024-01-31", "0", "2024-01-31"},
	} {
		sql := "SELECT EOMONTH('" + tc.input + "', " + tc.offset + ") AS v"
		got := evalScalarSQL(t, sql).(time.Time)
		if got.Format("2006-01-02") != tc.want {
			t.Errorf("%s: got %v, want %s", sql, got, tc.want)
		}
	}
}

func TestDateTimeNullPropagation(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE nullable_dates (v TEXT)`)
	execSQL(t, db, `INSERT INTO nullable_dates VALUES (NULL)`)
	for _, expression := range []string{
		`DATE(v)`, `TIME(v)`, `DATETIME(v)`, `JULIANDAY(v)`, `UNIXEPOCH(v)`,
		`STRFTIME('%Y', v)`, `STRFTIME(v, '2024-01-01')`,
		`YEAR(v)`, `MONTH(v)`, `DAY(v)`, `HOUR(v)`, `MINUTE(v)`, `SECOND(v)`,
		`DAYOFWEEK(v)`, `DAYOFYEAR(v)`, `WEEKOFYEAR(v)`, `QUARTER(v)`,
		`EXTRACT('YEAR', v)`, `EXTRACT(v, '2024-01-01')`,
		`DATE_TRUNC('DAY', v)`, `DATE_TRUNC(v, '2024-01-01')`,
		`DATE_ADD(v, 1, 'DAY')`, `DATE_SUB(v, 'P1D')`,
		`DATE_ADD('2024-01-01', v, 'DAY')`, `DATE_SUB('2024-01-01', 1, v)`,
		`DATE_ADD('2024-01-01', v)`, `DATE_SUB('2024-01-01', v, 'DAY')`,
		`DATEDIFF('DAYS', v, '2024-01-01')`, `DATEDIFF('DAYS', '2024-01-01', v)`,
		`DATEDIFF(v, '2024-01-01', '2024-01-02')`,
		`EOMONTH(v)`, `EOMONTH('2024-01-01', v)`, `ADD_MONTHS('2024-01-01', v)`,
	} {
		rs := execSQL(t, db, "SELECT "+expression+" AS result FROM nullable_dates")
		if got := rs.Rows[0]["result"]; got != nil {
			t.Errorf("%s: got %v, want NULL", expression, got)
		}
	}
	for _, expression := range []string{
		`DATE('invalid')`, `YEAR('2024-02-30')`, `DATE_TRUNC('invalid', '2024-01-01')`,
		`EOMONTH('2024-01-31', POWER(10, 400))`, `ADD_MONTHS('2024-01-31', POWER(-1, 0.5))`,
	} {
		if _, err := Execute(context.Background(), db, "default", mustParse("SELECT "+expression)); err == nil {
			t.Errorf("%s: expected error", expression)
		}
	}
}
