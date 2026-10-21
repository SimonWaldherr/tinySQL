package engine

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestCalendarConversionKnownDates(t *testing.T) {
	for _, tc := range []struct {
		gregorian, calendar, text string
	}{
		{"1582-10-15", "JULIAN", "1582-10-05"},
		{"1900-03-13", "JULIAN", "1900-02-29"},
		{"1900-03-14", "JULIAN", "1900-03-01"},
		{"2000-01-14", "JULIAN", "2000-01-01"},
		{"2024-02-29", "JULIAN", "2024-02-16"},
		{"0001-01-01", "JULIAN", "0001-01-03"},
		{"2024-01-01", "DISCORDIAN", "3190-Chaos-01"},
		{"2024-02-28", "DISCORDIAN", "3190-Chaos-59"},
		{"2024-02-29", "DISCORDIAN", "3190-StTibs"},
		{"2024-03-01", "DISCORDIAN", "3190-Chaos-60"},
		{"2023-03-01", "DISCORDIAN", "3189-Chaos-60"},
		{"1900-03-01", "DISCORDIAN", "3066-Chaos-60"},
		{"2000-02-29", "DISCORDIAN", "3166-StTibs"},
		{"2024-03-15", "DISCORDIAN", "3190-Discord-01"},
		{"2024-05-27", "DISCORDIAN", "3190-Confusion-01"},
		{"2024-08-08", "DISCORDIAN", "3190-Bureaucracy-01"},
		{"2024-10-20", "DISCORDIAN", "3190-Aftermath-01"},
		{"9999-12-31", "DISCORDIAN", "11165-Aftermath-73"},
		{"0622-07-19", "ISLAMIC_CIVIL", "0001-01-01"},
		{"2024-03-11", "ISLAMIC_CIVIL", "1445-09-01"},
		{"2024-07-07", "ISLAMIC_CIVIL", "1445-12-30"},
		{"2024-07-08", "ISLAMIC_CIVIL", "1446-01-01"},
		{"2021-01-01", "ISO_WEEK", "2020-W53-5"},
		{"2024-12-30", "ISO_WEEK", "2025-W01-1"},
		{"2024-02-29", "ORDINAL", "2024-060"},
		{"2024-12-31", "ORDINAL", "2024-366"},
	} {
		t.Run(tc.calendar+"/"+tc.gregorian, func(t *testing.T) {
			got := evalScalarSQL(t, "SELECT TO_CALENDAR('"+tc.gregorian+"', '"+tc.calendar+"') AS v")
			if got != tc.text {
				t.Fatalf("format = %v; want %s", got, tc.text)
			}
			got = evalScalarSQL(t, "SELECT DATE(FROM_CALENDAR('"+tc.text+"', '"+tc.calendar+"')) AS v")
			if got != tc.gregorian {
				t.Fatalf("parse = %v; want %s", got, tc.gregorian)
			}
			got = evalScalarSQL(t, "SELECT CALENDAR_CONVERT('"+tc.text+"', '"+tc.calendar+"', 'GREGORIAN') AS v")
			if got != tc.gregorian {
				t.Fatalf("convert = %v; want %s", got, tc.gregorian)
			}
		})
	}
	for _, tc := range []struct{ expression, want string }{
		{`CALENDAR_CONVERT('2024-02-16', 'julian', 'discordian')`, "3190-StTibs"},
		{`CALENDAR_CONVERT('3190-sttibs', 'discordian', 'julian')`, "2024-02-16"},
		{`CALENDAR_CONVERT('3190-The Aftermath-01', 'discordian', 'ordinal')`, "2024-294"},
		{`TO_CALENDAR('2024-03-01T00:15:00+14:00', 'discordian')`, "3190-Chaos-60"},
		{`TO_CALENDAR('2024-02-29', ' islamic-civil ')`, "1445-08-19"},
		{`TO_CALENDAR('2024-02-29', 'ISO_ORDINAL')`, "2024-060"},
	} {
		if got := evalScalarSQL(t, "SELECT "+tc.expression+" AS v"); got != tc.want {
			t.Errorf("%s = %v; want %s", tc.expression, got, tc.want)
		}
	}
	date := evalScalarSQL(t, `SELECT FROM_CALENDAR('3190-StTibs', 'discordian') AS v`).(time.Time)
	if date.Location() != time.UTC || date.Hour() != 0 || date.Nanosecond() != 0 {
		t.Fatalf("FROM_CALENDAR must return native UTC midnight: %v", date)
	}
}

func TestCalendarConversionRejectsInvalidDates(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	for _, expression := range []string{
		`FROM_CALENDAR('1900-02-29', 'gregorian')`, `FROM_CALENDAR('1901-02-29', 'julian')`,
		`FROM_CALENDAR('2024-04-31', 'julian')`, `FROM_CALENDAR('2024-00-01', 'julian')`,
		`FROM_CALENDAR('0001-01-01', 'julian')`, `FROM_CALENDAR('9999-12-31', 'julian')`,
		`FROM_CALENDAR('0000-01-01', 'gregorian')`, `FROM_CALENDAR('10000-01-01', 'gregorian')`,
		`FROM_CALENDAR('2024-1-01', 'gregorian')`, `FROM_CALENDAR('2024-01-01Z', 'gregorian')`,
		`FROM_CALENDAR('3189-StTibs', 'discordian')`, `FROM_CALENDAR('3066-StTibs', 'discordian')`,
		`FROM_CALENDAR('3266-StTibs', 'discordian')`, `FROM_CALENDAR('3190-Chaos-74', 'discordian')`,
		`FROM_CALENDAR('3190-Chaos-00', 'discordian')`, `FROM_CALENDAR('3190-Unknown-01', 'discordian')`,
		`FROM_CALENDAR('1166-Chaos-01', 'discordian')`, `FROM_CALENDAR('999999999-Chaos-01', 'discordian')`,
		`FROM_CALENDAR('1446-12-30', 'islamic_civil')`, `FROM_CALENDAR('1445-02-30', 'islamic_civil')`,
		`FROM_CALENDAR('1445-13-01', 'islamic_civil')`, `FROM_CALENDAR('9999-01-01', 'islamic_civil')`,
		`TO_CALENDAR('0622-07-18', 'islamic_civil')`, `FROM_CALENDAR('2021-W53-1', 'iso_week')`,
		`FROM_CALENDAR('2020-W00-1', 'iso_week')`, `FROM_CALENDAR('2020-W53-0', 'iso_week')`,
		`FROM_CALENDAR('9999-W52-7', 'iso_week')`, `FROM_CALENDAR('2023-366', 'ordinal')`,
		`FROM_CALENDAR('2024-000', 'ordinal')`, `FROM_CALENDAR('2024-367', 'ordinal')`,
		`TO_CALENDAR('invalid', 'julian')`, `TO_CALENDAR('2024-01-01', 'islamic')`,
		`FROM_CALENDAR(2024, 'julian')`, `TO_CALENDAR('2024-01-01', 123)`,
	} {
		if _, err := Execute(context.Background(), db, "default", mustParse("SELECT "+expression)); err == nil {
			t.Errorf("%s: expected error", expression)
		}
	}
	for _, year := range []int{0, -1, 10000, 1000000} {
		ex := &FuncCall{Name: "TO_CALENDAR", Args: []Expr{&Literal{Val: time.Date(year, 1, 1, 0, 0, 0, 0, time.UTC)}, &Literal{Val: "JULIAN"}}}
		if _, err := evalCalendarConversion(ExecEnv{}, ex, nil); err == nil {
			t.Errorf("year %d: expected range error", year)
		}
	}
}

func TestCalendarConversionRawParity(t *testing.T) {
	plan := &simpleSelectPlan{colIndex: map[string]int{"d": 0, "c": 1, "s": 2, "n": 3}}
	raw := []any{"2024-02-29", "JULIAN", "3190-StTibs", nil}
	row := Row{"d": raw[0], "c": raw[1], "s": raw[2], "n": nil}
	for _, expression := range []string{
		`TO_CALENDAR(d, c)`, `FROM_CALENDAR(s, 'discordian')`,
		`CALENDAR_CONVERT(s, 'discordian', c)`, `TO_CALENDAR(FROM_CALENDAR(s, 'discordian'), c)`,
		`TO_CALENDAR(n, 1/0)`, `FROM_CALENDAR(n, 1/0)`,
		`CALENDAR_CONVERT(n, 1/0, 1/0)`, `CALENDAR_CONVERT(s, n, 1/0)`,
		`TO_CALENDAR(d, 'unknown')`, `CALENDAR_CONVERT(s, 'discordian', 'unknown')`,
		`TO_CALENDAR(1/0)`, `FROM_CALENDAR(1/0, 1/0, 1/0)`, `CALENDAR_CONVERT(1/0)`,
	} {
		expr := mustParse("SELECT " + expression).(*Select).Projs[0].Expr
		want, wantErr := evalExpr(ExecEnv{}, expr, row)
		got, gotErr := evalRawExpr(plan, raw, expr)
		if !reflect.DeepEqual(got, want) || (gotErr == nil) != (wantErr == nil) || gotErr != nil && gotErr.Error() != wantErr.Error() {
			t.Errorf("%s: raw=%v, %v; ordinary=%v, %v", expression, got, gotErr, want, wantErr)
		}
	}
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	execSQL(t, db, `CREATE TABLE calendar_values (v TEXT, c TEXT)`)
	execSQL(t, db, `INSERT INTO calendar_values VALUES ('3190-StTibs', 'discordian'), ('2024-02-16', 'julian'), ('1445-08-19', 'islamic_civil'), (NULL, 'julian')`)
	result := execSQL(t, db, `SELECT CALENDAR_CONVERT(v, c, 'gregorian') AS d, COUNT(*) AS n FROM calendar_values WHERE FROM_CALENDAR(v, c) IS NOT NULL GROUP BY CALENDAR_CONVERT(v, c, 'gregorian')`)
	if len(result.Rows) != 1 || result.Rows[0]["d"] != "2024-02-29" || result.Rows[0]["n"] != 3 {
		t.Fatalf("mixed calendar grouping: %#v", result.Rows)
	}
}

func TestCalendarConversionRoundTrips(t *testing.T) {
	// A complete Gregorian leap cycle exercises every ISO week-year shape,
	// ordinary/century leap boundary and Discordian season boundary.
	for date := time.Date(1600, 1, 1, 0, 0, 0, 0, time.UTC); date.Year() < 2000; date = date.AddDate(0, 0, 1) {
		jdn := calendarDateJDN(date)
		back, err := calendarDateFromJDN(jdn)
		if err != nil || !back.Equal(date) {
			t.Fatalf("JDN %d: %v, %v; want %v", jdn, back, err, date)
		}
		for kind := calendarGregorian; kind <= calendarOrdinal; kind++ {
			text, err := formatCalendarDate(date, kind)
			if err != nil {
				t.Fatal(err)
			}
			got, err := parseCalendarDate(text, kind)
			if err != nil || !got.Equal(date) {
				t.Fatalf("calendar %d, %v -> %q -> %v, %v", kind, date, text, got, err)
			}
		}
	}
	for _, year := range []int{1, 4, 100, 400, 622, 1582, 2100, 2400, 9999} {
		for _, month := range []time.Month{1, 2, 3, 12} {
			date := time.Date(year, month+1, 0, 0, 0, 0, 0, time.UTC)
			for kind := calendarGregorian; kind <= calendarOrdinal; kind++ {
				if kind == calendarIslamicCivil && calendarDateJDN(date) < islamicCivilEpoch {
					continue
				}
				text, err := formatCalendarDate(date, kind)
				if err != nil {
					t.Fatal(err)
				}
				got, err := parseCalendarDate(text, kind)
				if err != nil || !got.Equal(date) {
					t.Fatalf("boundary calendar %d, %v -> %q -> %v, %v", kind, date, text, got, err)
				}
			}
		}
	}
}

func TestIslamicCivilLeapCycle(t *testing.T) {
	leapYears := map[int]bool{2: true, 5: true, 7: true, 10: true, 13: true, 16: true, 18: true, 21: true, 24: true, 26: true, 29: true}
	for year := 1; year <= 30; year++ {
		wantDays := int64(354)
		if leapYears[year] {
			wantDays++
		}
		if islamicCivilLeap(year) != leapYears[year] || islamicCivilJDN(year+1, 1, 1)-islamicCivilJDN(year, 1, 1) != wantDays {
			t.Fatalf("incorrect leap rule for year %d", year)
		}
	}
	if islamicCivilJDN(31, 1, 1)-islamicCivilEpoch != 10631 {
		t.Fatal("incorrect 30-year cycle length")
	}
	for jdn := int64(islamicCivilEpoch); jdn < islamicCivilEpoch+10631; jdn++ {
		year, month, day := islamicCivilFromJDN(jdn)
		if day < 1 || day > calendarMonthDays(year, month, calendarIslamicCivil) || islamicCivilJDN(year, month, day) != jdn {
			t.Fatalf("invalid cycle day %d: %d-%d-%d", jdn, year, month, day)
		}
	}
}

func FuzzCalendarDateParsing(f *testing.F) {
	for kind, text := range []string{"2024-02-29", "1900-02-29", "3190-StTibs", "1445-12-30", "2020-W53-5", "2024-060"} {
		f.Add(byte(kind), text)
	}
	f.Fuzz(func(t *testing.T, inputKind byte, text string) {
		kind := calendarKind(inputKind % 6)
		date, err := parseCalendarDate(text, kind)
		if err != nil {
			return
		}
		if date.Year() < 1 || date.Year() > 9999 || date.Location() != time.UTC {
			t.Fatalf("out-of-range calendar date: %v", date)
		}
		canonical, err := formatCalendarDate(date, kind)
		if err != nil {
			t.Fatal(err)
		}
		got, err := parseCalendarDate(canonical, kind)
		if err != nil || !got.Equal(date) {
			t.Fatalf("%q -> %v -> %q -> %v, %v", text, date, canonical, got, err)
		}
	})
}
