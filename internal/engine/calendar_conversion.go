package engine

import (
	"fmt"
	"strings"
	"time"
)

type calendarKind uint8

const (
	calendarGregorian calendarKind = iota
	calendarJulian
	calendarDiscordian
	calendarIslamicCivil
	calendarISOWeek
	calendarOrdinal
)

func parseCalendarKind(value any) (calendarKind, error) {
	name, ok := value.(string)
	if !ok {
		return 0, fmt.Errorf("calendar name must be a string")
	}
	switch strings.ToUpper(strings.TrimSpace(name)) {
	case "GREGORIAN":
		return calendarGregorian, nil
	case "JULIAN":
		return calendarJulian, nil
	case "DISCORDIAN":
		return calendarDiscordian, nil
	case "ISLAMIC_CIVIL", "ISLAMIC-CIVIL":
		return calendarIslamicCivil, nil
	case "ISO_WEEK", "ISO_WEEK_DATE":
		return calendarISOWeek, nil
	case "ORDINAL", "ISO_ORDINAL":
		return calendarOrdinal, nil
	default:
		return 0, fmt.Errorf("unsupported calendar %q", name)
	}
}

func evalCalendarConversion(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	return evalCalendarConversionCall(ex, func(arg Expr) (any, error) {
		return evalExpr(env, arg, row)
	})
}

func evalCalendarConversionCall(ex *FuncCall, eval func(Expr) (any, error)) (any, error) {
	name := strings.ToUpper(ex.Name)
	want := 2
	if name == "CALENDAR_CONVERT" {
		want = 3
	}
	if len(ex.Args) != want {
		return nil, fmt.Errorf("%s expects %d arguments", name, want)
	}
	var values [3]any
	for i, arg := range ex.Args {
		value, err := eval(arg)
		if err != nil || value == nil {
			return nil, err
		}
		values[i] = value
	}
	kind, err := parseCalendarKind(values[1])
	if err != nil {
		return nil, fmt.Errorf("%s: %w", name, err)
	}
	var date time.Time
	if name == "TO_CALENDAR" {
		date, err = parseDateTime(values[0])
	} else {
		text, ok := values[0].(string)
		if !ok {
			return nil, fmt.Errorf("%s: calendar date must be a string", name)
		}
		date, err = parseCalendarDate(strings.TrimSpace(text), kind)
	}
	if err != nil {
		return nil, fmt.Errorf("%s: %w", name, err)
	}
	if date.Year() < 1 || date.Year() > 9999 {
		return nil, fmt.Errorf("%s: Gregorian date must be in years 1 through 9999", name)
	}
	if name == "FROM_CALENDAR" {
		return date, nil
	}
	if name == "CALENDAR_CONVERT" {
		kind, err = parseCalendarKind(values[2])
		if err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
	}
	result, err := formatCalendarDate(date, kind)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", name, err)
	}
	return result, nil
}

const (
	// Integer Julian day numbers label civil days; no floating-point Julian
	// dates, times of day or historical calendar cutovers are involved.
	gregorianFirstJDN = 1721426 // 0001-01-01 Gregorian
	gregorianLastJDN  = 5373484 // 9999-12-31 Gregorian
	islamicCivilEpoch = 1948440 // 0622-07-19 Gregorian, Friday civil epoch
)

func calendarDateFromJDN(jdn int64) (time.Time, error) {
	if jdn < gregorianFirstJDN || jdn > gregorianLastJDN {
		return time.Time{}, fmt.Errorf("converted Gregorian date is outside years 1 through 9999")
	}
	a := jdn + 32044
	b := (4*a + 3) / 146097
	c := a - 146097*b/4
	d := (4*c + 3) / 1461
	e := c - 1461*d/4
	m := (5*e + 2) / 153
	day := e - (153*m+2)/5 + 1
	month := m + 3 - 12*(m/10)
	year := 100*b + d - 4800 + m/10
	return time.Date(int(year), time.Month(month), int(day), 0, 0, 0, 0, time.UTC), nil
}

func calendarDateJDN(date time.Time) int64 {
	year, month, day := date.Date()
	// gregorianDayNumber's origin is March 1 of astronomical year zero.
	return gregorianDayNumber(int64(year), int(month), day) + 1721120
}

func julianDateJDN(year, month, day int) int64 {
	a := int64((14 - month) / 12)
	y := int64(year) + 4800 - a
	m := int64(month) + 12*a - 3
	return int64(day) + (153*m+2)/5 + 365*y + y/4 - 32083
}

func julianDateFromJDN(jdn int64) (year, month, day int) {
	c := jdn + 32082
	d := (4*c + 3) / 1461
	e := c - 1461*d/4
	m := (5*e + 2) / 153
	return int(d - 4800 + m/10), int(m + 3 - 12*(m/10)), int(e - (153*m+2)/5 + 1)
}

func islamicCivilLeap(year int) bool { return (11*year+14)%30 < 11 }

func islamicCivilJDN(year, month, day int) int64 {
	return islamicCivilEpoch + 354*int64(year-1) + int64((3+11*year)/30) + int64((59*(month-1)+1)/2+day-1)
}

func islamicCivilFromJDN(jdn int64) (year, month, day int) {
	year = int((30*(jdn-islamicCivilEpoch) + 10646) / 10631)
	// The estimate can land beside the year at a leap-cycle boundary.
	for islamicCivilJDN(year+1, 1, 1) <= jdn {
		year++
	}
	for islamicCivilJDN(year, 1, 1) > jdn {
		year--
	}
	month = 1
	for month < 12 && islamicCivilJDN(year, month+1, 1) <= jdn {
		month++
	}
	day = int(jdn-islamicCivilJDN(year, month, 1)) + 1
	return
}

func gregorianLeap(year int) bool { return year%4 == 0 && (year%100 != 0 || year%400 == 0) }

func calendarDigits(text string) (int, bool) {
	if text == "" {
		return 0, false
	}
	value := 0
	for i := 0; i < len(text); i++ {
		if text[i] < '0' || text[i] > '9' {
			return 0, false
		}
		value = value*10 + int(text[i]-'0')
	}
	return value, true
}

func parseCalendarYMD(text string) (year, month, day int, ok bool) {
	if len(text) != 10 || text[4] != '-' || text[7] != '-' {
		return
	}
	var yOK, mOK, dOK bool
	year, yOK = calendarDigits(text[:4])
	month, mOK = calendarDigits(text[5:7])
	day, dOK = calendarDigits(text[8:])
	ok = yOK && mOK && dOK && year >= 1 && month >= 1 && month <= 12 && day >= 1
	return
}

func calendarMonthDays(year, month int, kind calendarKind) int {
	if kind == calendarIslamicCivil {
		if month%2 == 1 || month == 12 && islamicCivilLeap(year) {
			return 30
		}
		return 29
	}
	if month == 2 {
		if kind == calendarJulian && year%4 == 0 || kind == calendarGregorian && gregorianLeap(year) {
			return 29
		}
		return 28
	}
	return [...]int{31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}[month-1]
}

func parseCalendarDate(text string, kind calendarKind) (time.Time, error) {
	invalid := func() (time.Time, error) { return time.Time{}, fmt.Errorf("invalid calendar date %q", text) }
	switch kind {
	case calendarGregorian, calendarJulian, calendarIslamicCivil:
		year, month, day, ok := parseCalendarYMD(text)
		if !ok || day > calendarMonthDays(year, month, kind) {
			return invalid()
		}
		switch kind {
		case calendarJulian:
			return calendarDateFromJDN(julianDateJDN(year, month, day))
		case calendarIslamicCivil:
			return calendarDateFromJDN(islamicCivilJDN(year, month, day))
		default:
			return time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC), nil
		}
	case calendarOrdinal:
		if len(text) != 8 || text[4] != '-' {
			return invalid()
		}
		year, yOK := calendarDigits(text[:4])
		day, dOK := calendarDigits(text[5:])
		if !yOK || !dOK || year < 1 || day < 1 || day > 366 || day == 366 && !gregorianLeap(year) {
			return invalid()
		}
		return time.Date(year, 1, day, 0, 0, 0, 0, time.UTC), nil
	case calendarISOWeek:
		if len(text) != 10 || text[4] != '-' || text[5] != 'W' && text[5] != 'w' || text[8] != '-' {
			return invalid()
		}
		year, yOK := calendarDigits(text[:4])
		week, wOK := calendarDigits(text[6:8])
		day, dOK := calendarDigits(text[9:])
		if !yOK || !wOK || !dOK || year < 1 || week < 1 || week > 53 || day < 1 || day > 7 {
			return invalid()
		}
		jan4 := time.Date(year, 1, 4, 0, 0, 0, 0, time.UTC)
		date := jan4.AddDate(0, 0, -(int(jan4.Weekday())+6)%7+(week-1)*7+day-1)
		isoYear, isoWeek := date.ISOWeek()
		if isoYear != year || isoWeek != week || date.Year() < 1 || date.Year() > 9999 {
			return invalid()
		}
		return date, nil
	case calendarDiscordian:
		return parseDiscordianDate(text)
	default:
		return invalid()
	}
}

func formatCalendarDate(date time.Time, kind calendarKind) (string, error) {
	year, month, day := date.Date()
	switch kind {
	case calendarGregorian:
		return date.Format("2006-01-02"), nil
	case calendarJulian:
		y, m, d := julianDateFromJDN(calendarDateJDN(date))
		return fmt.Sprintf("%04d-%02d-%02d", y, m, d), nil
	case calendarIslamicCivil:
		jdn := calendarDateJDN(date)
		if jdn < islamicCivilEpoch {
			return "", fmt.Errorf("date precedes the Islamic civil epoch (0622-07-19 Gregorian)")
		}
		y, m, d := islamicCivilFromJDN(jdn)
		return fmt.Sprintf("%04d-%02d-%02d", y, m, d), nil
	case calendarDiscordian:
		if month == time.February && day == 29 {
			return fmt.Sprintf("%04d-StTibs", year+1166), nil
		}
		ordinal := date.YearDay() - 1
		if gregorianLeap(year) && ordinal >= 60 {
			ordinal--
		}
		return fmt.Sprintf("%04d-%s-%02d", year+1166, discordianSeasons[ordinal/73], ordinal%73+1), nil
	case calendarISOWeek:
		isoYear, week := date.ISOWeek()
		return fmt.Sprintf("%04d-W%02d-%d", isoYear, week, (int(date.Weekday())+6)%7+1), nil
	case calendarOrdinal:
		return fmt.Sprintf("%04d-%03d", year, date.YearDay()), nil
	default:
		return "", fmt.Errorf("unsupported calendar")
	}
}

var discordianSeasons = [...]string{"Chaos", "Discord", "Confusion", "Bureaucracy", "Aftermath"}

func parseDiscordianDate(text string) (time.Time, error) {
	invalid := func() (time.Time, error) { return time.Time{}, fmt.Errorf("invalid Discordian date %q", text) }
	yearText, rest, found := strings.Cut(text, "-")
	if !found || len(yearText) < 4 || len(yearText) > 5 {
		return invalid()
	}
	yold, ok := calendarDigits(yearText)
	if !ok || yold < 1167 || yold > 11165 {
		return invalid()
	}
	year := yold - 1166
	if strings.EqualFold(rest, "StTibs") {
		if !gregorianLeap(year) {
			return invalid()
		}
		return time.Date(year, 2, 29, 0, 0, 0, 0, time.UTC), nil
	}
	season, dayText, found := strings.Cut(rest, "-")
	if !found || len(dayText) != 2 {
		return invalid()
	}
	day, ok := calendarDigits(dayText)
	if !ok || day < 1 || day > 73 {
		return invalid()
	}
	for i, name := range discordianSeasons {
		if strings.EqualFold(season, name) || i == 4 && strings.EqualFold(season, "The Aftermath") {
			ordinal := i*73 + day
			if gregorianLeap(year) && ordinal >= 60 {
				ordinal++
			}
			return time.Date(year, 1, ordinal, 0, 0, 0, 0, time.UTC), nil
		}
	}
	return invalid()
}
