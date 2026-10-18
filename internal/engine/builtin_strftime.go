package engine

import (
	"strconv"
	"strings"
	"time"
)

// strftimeFormat scans SQLite substitutions directly, so literal text cannot
// accidentally become a Go time layout. Unsupported substitutions yield NULL.
func strftimeFormat(t time.Time, format string) (string, bool) {
	first := strings.IndexByte(format, '%')
	if first < 0 {
		return format, true
	}

	var small [128]byte
	out := append(small[:0], format[:first]...)
	year, month, day := t.Date()
	hour, minute, second := t.Clock()
	for i := first; i < len(format); i++ {
		if format[i] != '%' {
			out = append(out, format[i])
			continue
		}
		i++
		if i == len(format) {
			return "", false
		}
		switch format[i] {
		case '%':
			out = append(out, '%')
		case 'Y':
			out = appendStrftimeInt(out, year, 4, '0')
		case 'm':
			out = appendStrftimeInt(out, int(month), 2, '0')
		case 'd':
			out = appendStrftimeInt(out, day, 2, '0')
		case 'e':
			out = appendStrftimeInt(out, day, 2, ' ')
		case 'F':
			out = appendStrftimeInt(out, year, 4, '0')
			out = append(out, '-')
			out = appendStrftimeInt(out, int(month), 2, '0')
			out = append(out, '-')
			out = appendStrftimeInt(out, day, 2, '0')
		case 'H', 'k':
			pad := byte('0')
			if format[i] == 'k' {
				pad = ' '
			}
			out = appendStrftimeInt(out, hour, 2, pad)
		case 'I', 'l':
			h := hour % 12
			if h == 0 {
				h = 12
			}
			pad := byte('0')
			if format[i] == 'l' {
				pad = ' '
			}
			out = appendStrftimeInt(out, h, 2, pad)
		case 'M':
			out = appendStrftimeInt(out, minute, 2, '0')
		case 'S':
			out = appendStrftimeInt(out, second, 2, '0')
		case 'R', 'T':
			out = appendStrftimeInt(out, hour, 2, '0')
			out = append(out, ':')
			out = appendStrftimeInt(out, minute, 2, '0')
			if format[i] == 'T' {
				out = append(out, ':')
				out = appendStrftimeInt(out, second, 2, '0')
			}
		case 'p', 'P':
			ampm := "AM"
			if hour >= 12 {
				ampm = "PM"
			}
			if format[i] == 'P' {
				out = append(out, ampm[0]+('a'-'A'), 'm')
			} else {
				out = append(out, ampm...)
			}
		case 'f':
			millis := second*1000 + (t.Nanosecond()+500_000)/1_000_000
			if millis > 59999 {
				millis = 59999
			}
			out = appendStrftimeInt(out, millis/1000, 2, '0')
			out = append(out, '.')
			out = appendStrftimeInt(out, millis%1000, 3, '0')
		case 'j':
			out = appendStrftimeInt(out, t.YearDay(), 3, '0')
		case 'w':
			out = append(out, byte(t.Weekday())+'0')
		case 'u':
			out = append(out, byte((int(t.Weekday())+6)%7)+'1')
		case 'U':
			week := (t.YearDay() + 6 - int(t.Weekday())) / 7
			out = appendStrftimeInt(out, week, 2, '0')
		case 'W':
			week := (t.YearDay() + 6 - (int(t.Weekday())+6)%7) / 7
			out = appendStrftimeInt(out, week, 2, '0')
		case 'G', 'g', 'V':
			isoYear, isoWeek := t.ISOWeek()
			switch format[i] {
			case 'G':
				width := 4
				if isoYear < 0 {
					width-- // SQLite's ISO year width includes the sign.
				}
				out = appendStrftimeInt(out, isoYear, width, '0')
			case 'g':
				width := 2
				if isoYear%100 < 0 {
					width--
				}
				out = appendStrftimeInt(out, isoYear%100, width, '0')
			case 'V':
				out = appendStrftimeInt(out, isoWeek, 2, '0')
			}
		case 's':
			out = strconv.AppendInt(out, t.Unix(), 10)
		case 'J':
			const secondsPerDay = 86400.0
			jd := float64(t.Unix())/secondsPerDay +
				float64(t.Nanosecond())/(secondsPerDay*1e9) + unixEpochJulianDay
			out = strconv.AppendFloat(out, jd, 'g', 16, 64)
		default:
			return "", false
		}
	}
	return string(out), true
}

func appendStrftimeInt(dst []byte, value, width int, pad byte) []byte {
	magnitude := uint64(value)
	if value < 0 {
		dst = append(dst, '-')
		magnitude = uint64(-(value + 1)) + 1
	}
	for n := magnitude; n >= 10; n /= 10 {
		width--
	}
	for ; width > 1; width-- {
		dst = append(dst, pad)
	}
	return strconv.AppendUint(dst, magnitude, 10)
}
