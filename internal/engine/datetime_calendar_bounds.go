package engine

import (
	"fmt"
	"math"
	"time"
)

// AddDate normalizes fields using machine integers, which can wrap for huge
// offsets even when conversion of the offset itself succeeded. Keep ordinary
// calendar arithmetic unchanged; validate extreme shifts before construction.
func addDateChecked(t time.Time, years, months, days int) (time.Time, error) {
	year, month, day := t.Date()
	if year >= -10000 && year <= 10000 && years >= -1000000 && years <= 1000000 && months >= -1000000 && months <= 1000000 && days >= -1000000 && days <= 1000000 {
		return t.AddDate(years, months, days), nil
	}
	errRange := func() (time.Time, error) { return t, fmt.Errorf("calendar interval out of range") }
	monthIndex := int(month) - 1 + months%12
	carry := int64(months / 12)
	if monthIndex < 0 {
		monthIndex += 12
		carry--
	} else if monthIndex >= 12 {
		monthIndex -= 12
		carry++
	}
	targetYear, ok := addCalendarInt(int64(year), int64(years))
	if !ok {
		return errRange()
	}
	targetYear, ok = addCalendarInt(targetYear, carry)
	// Every representable time.Time year lies well within +/- 2^40. This
	// bound also keeps the Gregorian day calculation below int64 overflow.
	if !ok || targetYear < -1<<40 || targetYear > 1<<40 || targetYear < math.MinInt || targetYear > math.MaxInt {
		return errRange()
	}
	targetDay, ok := addCalendarInt(int64(day), int64(days))
	if !ok || targetDay < math.MinInt || targetDay > math.MaxInt {
		return errRange()
	}
	want, ok := addCalendarInt(gregorianDayNumber(targetYear, monthIndex+1, 1), targetDay-1)
	if !ok {
		return errRange()
	}
	hour, minute, second := t.Clock()
	utc := time.Date(int(targetYear), time.Month(monthIndex+1), int(targetDay), hour, minute, second, t.Nanosecond(), time.UTC)
	actual := gregorianDayNumber(int64(utc.Year()), int(utc.Month()), utc.Day())
	if actual != want || want >= gregorianDayNumber(1, 1, 1) && utc.Before(time.Time{}) {
		return errRange()
	}
	return time.Date(int(targetYear), time.Month(monthIndex+1), int(targetDay), hour, minute, second, t.Nanosecond(), t.Location()), nil
}

func addCalendarInt(a, b int64) (int64, bool) {
	if b > 0 && a > math.MaxInt64-b || b < 0 && a < math.MinInt64-b {
		return 0, false
	}
	return a + b, true
}

// Count days by 400-year Gregorian cycles; the origin is intentionally
// arbitrary because only equality and relative order are needed above.
func gregorianDayNumber(year int64, month, day int) int64 {
	if month <= 2 {
		year--
		month += 12
	}
	era := year / 400
	if year < 0 && year%400 != 0 {
		era--
	}
	yearOfEra := year - era*400
	return era*146097 + yearOfEra*365 + yearOfEra/4 - yearOfEra/100 + int64((153*(month-3)+2)/5+day-1)
}
