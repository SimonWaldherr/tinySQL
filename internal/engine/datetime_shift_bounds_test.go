package engine

import (
	"context"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestDateClockShiftBoundsAndTruncation(t *testing.T) {
	base := time.Date(2024, 1, 15, 10, 30, 0, 123456789, time.FixedZone("test", 90*60))
	for _, tc := range []struct {
		unit string
		span time.Duration
	}{{"HOUR", time.Hour}, {"MINUTE", time.Minute}, {"SECOND", time.Second}} {
		bound := int64(math.MaxInt64) / int64(tc.span)
		for _, count := range []float64{0.9, -0.9, 1.9, -1.9, float64(bound) + 0.75, -float64(bound) - 0.75} {
			got, err := applyDateUnitShift(base, count, tc.unit)
			want := base.Add(time.Duration(math.Trunc(count)) * tc.span)
			if err != nil || !got.Equal(want) || got.Location() != base.Location() {
				t.Errorf("%v %s = %v, %v; want %v", count, tc.unit, got, err, want)
			}
		}
		for _, count := range []float64{float64(bound + 1), -float64(bound + 1), math.MaxFloat64} {
			if _, err := applyDateUnitShift(base, count, tc.unit); err == nil {
				t.Errorf("%v %s: expected range error", count, tc.unit)
			}
		}
	}
	got, err := applyDateUnitShift(base, 10000, "YEAR")
	if err != nil || !got.Equal(base.AddDate(10000, 0, 0)) {
		t.Fatalf("large calendar year shift = %v, %v", got, err)
	}
}

func TestISODurationClockBounds(t *testing.T) {
	base := time.Date(2024, 1, 15, 10, 30, 0, 123456789, time.UTC)
	for _, input := range []string{"PT2562047H47M16S", "-PT2562047H47M16S"} {
		duration, err := parseISO8601Duration(input)
		if err != nil {
			t.Fatal(err)
		}
		for _, negate := range []bool{false, true} {
			got, err := duration.shift(base, negate)
			sign := time.Duration(1)
			if strings.HasPrefix(input, "-") != negate {
				sign = -1
			}
			want := base.Add(sign * 9223372036 * time.Second)
			if err != nil || !got.Equal(want) {
				t.Errorf("%s negate=%v = %v, %v; want %v", input, negate, got, err, want)
			}
		}
	}
}

func TestDateShiftInvalidIntervalsSQL(t *testing.T) {
	db := storage.NewDB()
	weekOverflow := "P" + strconv.Itoa(math.MaxInt) + "W"
	daySumOverflow := "P" + strconv.Itoa(math.MaxInt/7) + "W" + strconv.Itoa(math.MaxInt%7+1) + "D"
	negativeDaySumOverflow := "-P" + strconv.Itoa(math.MaxInt/7) + "W" + strconv.Itoa(math.MaxInt%7+2) + "D"
	for _, name := range []string{"DATE_ADD", "DATE_SUB"} {
		for _, arguments := range []string{
			`2562048, 'HOUR'`, `-2562048, 'HOUR'`, `153722868, 'MINUTE'`, `9223372037, 'SECOND'`,
			`POWER(10, 400), 'DAY'`, `-POWER(10, 400), 'SECOND'`, `POWER(-1, 0.5), 'YEAR'`,
			`1e30, 'WEEK'`, `'PT2562048H'`, `'-PT2562048H'`, `'PT153722868M'`, `'PT9223372037S'`,
			`1e18, 'YEAR'`, `-1e18, 'MONTH'`, `1e18, 'DAY'`, `'P1000000000000000000D'`,
			`'PT2562047H60M'`, `'-PT2562047H47M17S'`,
			"'" + weekOverflow + "'", "'" + daySumOverflow + "'", "'" + negativeDaySumOverflow + "'",
		} {
			query := "SELECT " + name + "('2024-01-15', " + arguments + ") AS d"
			if _, err := Execute(context.Background(), db, "default", mustParse(query)); err == nil || !strings.Contains(err.Error(), name+":") {
				t.Errorf("%s: expected %s range error, got %v", query, name, err)
			}
		}
	}
	query := "SELECT DATE_SUB('2024-01-15', '-" + daySumOverflow + "') AS d"
	if _, err := Execute(context.Background(), db, "default", mustParse(query)); err == nil || !strings.Contains(err.Error(), "DATE_SUB:") {
		t.Errorf("negating the minimum integer calendar interval must fail, got %v", err)
	}
}

func TestDateCalendarExtremeShiftBounds(t *testing.T) {
	base := time.Date(2024, 1, 31, 12, 34, 56, 123, time.UTC)
	for _, shift := range [][3]int{{1000001, 0, 0}, {-1000001, 0, 0}, {0, 1000001, 0}, {0, 0, -1000001}, {0, 0, 1000001}} {
		got, err := addDateChecked(base, shift[0], shift[1], shift[2])
		want := base.AddDate(shift[0], shift[1], shift[2])
		if err != nil || !got.Equal(want) {
			t.Errorf("shift %v = %v, %v; want %v", shift, got, err, want)
		}
	}
	for _, shift := range [][3]int{{math.MaxInt, 0, 0}, {math.MinInt, 0, 0}, {0, math.MaxInt, 0}, {0, 0, math.MaxInt}, {0, 0, math.MinInt}} {
		if strconv.IntSize == 32 {
			continue // These counts still fit time.Time's range on a 32-bit target.
		}
		if _, err := addDateChecked(base, shift[0], shift[1], shift[2]); err == nil {
			t.Errorf("shift %v: expected range error", shift)
		}
	}
	for _, expression := range []string{`EOMONTH('2024-01-31', 1e18)`, `ADD_MONTHS('2024-01-31', -1e18)`} {
		if strconv.IntSize == 32 {
			continue
		}
		db := storage.NewDB()
		if _, err := Execute(context.Background(), db, "default", mustParse("SELECT "+expression)); err == nil {
			t.Errorf("%s: expected range error", expression)
		}
	}
}

func TestDateCalendarLargeValidShifts(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	for range 2000 {
		base := time.Date(rng.Intn(2000001)-1000000, time.Month(rng.Intn(12)+1), rng.Intn(28)+1, 12, 34, 56, 123, time.FixedZone("test", 345*60))
		years, months, days := rng.Intn(200000001)-100000000, rng.Intn(200000001)-100000000, rng.Intn(200000001)-100000000
		got, err := addDateChecked(base, years, months, days)
		want := base.AddDate(years, months, days)
		if err != nil || !got.Equal(want) || got.Location() != base.Location() {
			t.Fatalf("%v + (%d, %d, %d) = %v, %v; want %v", base, years, months, days, got, err, want)
		}
	}
}
