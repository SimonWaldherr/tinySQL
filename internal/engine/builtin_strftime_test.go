package engine

import (
	"math"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestStrftimeSubstitutions(t *testing.T) {
	cases := []struct {
		format string
		want   string
	}{
		{"%Y-%m-%d %H:%M:%S", "2024-03-05 06:07:08"},
		{"%e|%k|%l|%I|%p|%P", " 5| 6| 6|06|AM|am"},
		{"%F|%R|%T|%f", "2024-03-05|06:07|06:07:08|08.123"},
		{"%G|%g|%V|%j|%U|%W|%u|%w", "2024|24|10|065|09|10|2|2"},
		{"", ""},
		{"2006 Jan Mon MST 15:04:05", "2006 Jan Mon MST 15:04:05"},
		{"%%Y|%%j|%%W|%%w|%%s|%%f|%%%%|%%%Y", "%Y|%j|%W|%w|%s|%f|%%|%2024"},
		{"März: %F; 日本語 %%", "März: 2024-03-05; 日本語 %"},
	}
	for _, tc := range cases {
		t.Run(tc.format, func(t *testing.T) {
			sql := "SELECT STRFTIME('" + tc.format + "', '2024-03-05 06:07:08.1234') AS v"
			if got := evalScalarSQL(t, sql); got != tc.want {
				t.Fatalf("got %#v, want %q", got, tc.want)
			}
		})
	}
}

func TestStrftimeCalendarWeeks(t *testing.T) {
	cases := []struct {
		date string
		want string
	}{
		{"2023-01-01", "01|00|2022|22|52|7|0|001"},
		{"2023-01-02", "01|01|2023|23|01|1|1|002"},
		{"2024-01-01", "00|01|2024|24|01|1|1|001"},
		{"2024-01-07", "01|01|2024|24|01|7|0|007"},
		{"2024-01-08", "01|02|2024|24|02|1|1|008"},
		{"2020-12-31", "52|52|2020|20|53|4|4|366"},
		{"2021-01-01", "00|00|2020|20|53|5|5|001"},
		{"2021-01-03", "01|00|2020|20|53|7|0|003"},
		{"2021-01-04", "01|01|2021|21|01|1|1|004"},
		{"2018-12-31", "52|53|2019|19|01|1|1|365"},
		{"2024-12-31", "52|53|2025|25|01|2|2|366"},
		{"2000-12-31", "53|52|2000|00|52|7|0|366"},
	}
	for _, tc := range cases {
		t.Run(tc.date, func(t *testing.T) {
			got := evalScalarSQL(t, "SELECT STRFTIME('%U|%W|%G|%g|%V|%u|%w|%j', '"+tc.date+"') AS v")
			if got != tc.want {
				t.Fatalf("got %#v, want %q", got, tc.want)
			}
		})
	}
}

func TestStrftimeTwelveHourClock(t *testing.T) {
	for _, tc := range []struct {
		hour int
		want string
	}{
		{0, "00| 0|12|12|AM|am"},
		{1, "01| 1|01| 1|AM|am"},
		{12, "12|12|12|12|PM|pm"},
		{13, "13|13|01| 1|PM|pm"},
		{23, "23|23|11|11|PM|pm"},
	} {
		date := time.Date(2024, 1, 1, tc.hour, 0, 0, 0, time.UTC)
		got, ok := strftimeFormat(date, "%H|%k|%I|%l|%p|%P")
		if !ok || got != tc.want {
			t.Errorf("hour %d: got %q/%v, want %q", tc.hour, got, ok, tc.want)
		}
	}
}

func TestStrftimeISOYearZeroBoundary(t *testing.T) {
	for _, tc := range []struct{ date, want string }{
		{"0000-01-01", "-001|-1|52"},
		{"0000-01-03", "0000|00|01"},
	} {
		got := evalScalarSQL(t, "SELECT STRFTIME('%G|%g|%V', '"+tc.date+"') AS v")
		if got != tc.want {
			t.Errorf("ISO year boundary %s: got %v, want %s", tc.date, got, tc.want)
		}
	}
}

func TestStrftimeFractionalSeconds(t *testing.T) {
	for _, tc := range []struct {
		nanos int
		want  string
	}{
		{0, "08.000"},
		{123_400_000, "08.123"},
		{123_900_000, "08.124"},
		{999_900_000, "09.000"},
	} {
		got, ok := strftimeFormat(time.Date(2024, 1, 1, 0, 0, 8, tc.nanos, time.UTC), "%f")
		if !ok || got != tc.want {
			t.Errorf("nanoseconds %d: got %q/%v, want %q", tc.nanos, got, ok, tc.want)
		}
	}
	if got, ok := strftimeFormat(time.Date(2024, 1, 1, 0, 0, 59, 999_900_000, time.UTC), "%f"); !ok || got != "59.999" {
		t.Errorf("rounding at minute boundary: got %q/%v, want 59.999", got, ok)
	}
}

func TestStrftimeUnsupportedSubstitutionsReturnNull(t *testing.T) {
	for _, format := range []string{"%", "%Q", "%z", "%n", "%E", "good %Y bad %", "%% %Q"} {
		got := evalScalarSQL(t, "SELECT STRFTIME('"+format+"', '2024-01-01') AS v")
		if got != nil {
			t.Errorf("format %q: got %#v, want NULL", format, got)
		}
	}
}

func TestStrftimePreservesZoneAndEpoch(t *testing.T) {
	date := time.Date(2024, 1, 1, 0, 30, 0, 0, time.FixedZone("east", 2*60*60))
	got, ok := strftimeFormat(date, "%F %T|%s")
	want := "2024-01-01 00:30:00|1704061800"
	if !ok || got != want {
		t.Fatalf("got %q/%v, want %q", got, ok, want)
	}
	got, ok = strftimeFormat(time.Unix(-1, 0).UTC(), "%F %T|%s")
	if !ok || got != "1969-12-31 23:59:59|-1" {
		t.Fatalf("negative epoch: got %q/%v", got, ok)
	}
}

func TestStrftimeJulianDay(t *testing.T) {
	for _, date := range []string{"0000-01-01", "1970-01-01", "2024-03-05 06:07:08.123", "9999-12-31 23:59:59"} {
		formatted := evalScalarSQL(t, "SELECT STRFTIME('%J', '"+date+"') AS v").(string)
		got, err := strconv.ParseFloat(formatted, 64)
		if err != nil {
			t.Fatalf("%s: invalid Julian day %q: %v", date, formatted, err)
		}
		want := evalScalarSQL(t, "SELECT JULIANDAY('"+date+"') AS v").(float64)
		if math.Abs(got-want) > 1e-9 {
			t.Errorf("%s: formatted %v, numeric %v", date, got, want)
		}
	}
	if got := evalScalarSQL(t, "SELECT STRFTIME('%J', '1970-01-01') AS v"); got != "2440587.5" {
		t.Errorf("epoch Julian day = %#v", got)
	}
}

func TestStrftimeLongFormat(t *testing.T) {
	format := strings.Repeat("%F %% 2006 %T ", 32)
	want := strings.Repeat("2024-03-05 % 2006 06:07:08 ", 32)
	got, ok := strftimeFormat(time.Date(2024, 3, 5, 6, 7, 8, 0, time.UTC), format)
	if !ok || got != want {
		t.Fatalf("long format: got %q/%v, want %q", got, ok, want)
	}
}

func TestStrftimeNativeYearWidth(t *testing.T) {
	// Native temporal values can extend beyond SQLite's four-digit year range.
	for _, year := range []int{math.MinInt32, -1, 0, 7, 9999, 10000} {
		date := time.Date(year, 1, 1, 0, 0, 0, 0, time.UTC)
		got, ok := strftimeFormat(date, "%Y-%m-%d")
		if want := date.Format("2006-01-02"); !ok || got != want {
			t.Errorf("year %d: got %q/%v, want %q", year, got, ok, want)
		}
	}
}
