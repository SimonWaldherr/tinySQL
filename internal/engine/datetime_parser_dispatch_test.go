package engine

import (
	"fmt"
	"testing"
	"time"
)

func BenchmarkDateTimeParserLayouts(b *testing.B) {
	for _, input := range []string{
		"2026-09-08", "2026-09-08 12:34:56", "2026-09-08T12:34:56.123456789Z",
		"2026-09-08T12:34:56.123456789+02:30", "09/08/2026", "08-Sep-2026",
	} {
		b.Run(input, func(b *testing.B) {
			value := any(input)
			b.ReportAllocs()
			for b.Loop() {
				if _, err := parseDateTime(value); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkTimeValueParserLayouts(b *testing.B) {
	for _, input := range []string{"12:34", "12:34:56", "12:34:56.123456789", "2026-09-08 12:34", "2026-09-08 12:34:56"} {
		b.Run(input, func(b *testing.B) {
			value := any(input)
			b.ReportAllocs()
			for b.Loop() {
				if _, err := parseTimeValue(value); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// These reference parsers keep the accepted layouts and their ordering explicit.
// time.Parse intentionally accepts some variable-width hours and fractions not
// spelled out in its layouts, so shape dispatch must retain that fallback.
func referenceDateTimeLayouts(input string) (time.Time, error) {
	for _, layout := range []string{time.RFC3339, "2006-01-02 15:04:05", "2006-01-02T15:04:05", "2006-01-02", "01/02/2006", "02-Jan-2006"} {
		if parsed, err := time.Parse(layout, input); err == nil {
			return parsed, nil
		}
	}
	return time.Time{}, fmt.Errorf("cannot parse '%s' as datetime", input)
}

func referenceTimeValueLayouts(input string) (time.Time, error) {
	var layouts []string
	switch len(input) {
	case 5:
		layouts = []string{"15:04"}
	case 8:
		layouts = []string{"15:04:05"}
	case 10:
		layouts = []string{"2006-01-02"}
	case 16:
		layouts = []string{"2006-01-02 15:04"}
	case 19:
		layouts = []string{"2006-01-02 15:04:05", "2006-01-02T15:04:05"}
	default:
		layouts = []string{time.RFC3339, "2006-01-02 15:04:05", "2006-01-02T15:04:05", "2006-01-02 15:04", "2006-01-02", "15:04:05", "15:04"}
	}
	for _, layout := range layouts {
		if parsed, err := time.Parse(layout, input); err == nil {
			return parsed, nil
		}
	}
	return time.Time{}, fmt.Errorf("cannot parse '%s' as time", input)
}

func TestDateTimeParserLayoutCompatibility(t *testing.T) {
	inputs := []string{
		"", "now", "2026", "2026-9-08", "2026-09-8", "09/08/2026", "9/08/2026", "09/8/2026", "8-Sep-2026", "08-SEP-2026", "08-sep-2026",
		"02/29/2024", "02/29/2023", "31-Apr-2026", "00-Jan-2026", "01-Jan-0000", "31-Dec-9999",
		"12:34", "12:34:56", "1:34", "1:34:56", "24:00:00", "12:60:00", "12:34:60",
		"12:34:56.1", "12:34:56,123456789", "12:34:56.123456789123", "1:34:56.1", "12:34:56.", "12:34:56.1Z",
		"12:34:56.12", "12:34:56.1234567", "12:34:56.1234567890", "12:34:56.12345678901",
		"2026-09-08 12:34", "2026-09-08 1:34", "2026-09-08T12:34", "2026-09-08 12:34:56", "2026-09-08T12:34:56",
		"2026-09-08 1:34:56", "2026-09-08T1:34:56Z", "2026-09-08 12:34:56.123456789123",
		"2026-09-08T12:34:56,1Z", "2026-09-08T12:34:56.1+02:30", "2026-09-08T12:34:56-07:00",
		"2026-09-08T12:34:56+24:00", "2026-09-08T12:34:56+02:60", "2026-09-08T12:34:56+25:00", "2026-09-08 12:34:56Z",
	}
	for _, input := range inputs {
		t.Run(input, func(t *testing.T) {
			for _, parser := range []struct {
				name string
				got  func(any) (time.Time, error)
				want func(string) (time.Time, error)
			}{{"datetime", parseDateTime, referenceDateTimeLayouts}, {"time", parseTimeValue, referenceTimeValueLayouts}} {
				got, gotErr := parser.got(input)
				want, wantErr := parser.want(input)
				if (gotErr == nil) != (wantErr == nil) || gotErr != nil && gotErr.Error() != wantErr.Error() || gotErr == nil && !got.Equal(want) {
					t.Fatalf("%s(%q) = %v, %v; want %v, %v", parser.name, input, got, gotErr, want, wantErr)
				}
				if gotErr == nil {
					_, gotOffset := got.Zone()
					_, wantOffset := want.Zone()
					if gotOffset != wantOffset {
						t.Fatalf("%s zone offset = %d; want %d", parser.name, gotOffset, wantOffset)
					}
				}
			}
		})
	}
}

func TestDateTimeParserInputValues(t *testing.T) {
	instant := time.Date(2026, 9, 8, 12, 34, 56, 789, time.FixedZone("test", 90*60))
	for _, parser := range []func(any) (time.Time, error){parseDateTime, parseTimeValue} {
		got, err := parser(instant)
		if err != nil || got != instant {
			t.Fatalf("time.Time input = %v, %v; want unchanged %v", got, err, instant)
		}
		if _, err := parser(nil); err == nil {
			t.Fatal("nil input must return an error")
		}
	}
	if _, err := parseTimeValue(20260908); err == nil || err.Error() != "cannot convert int to time" {
		t.Fatalf("integer time input error = %v", err)
	}
	if _, err := parseDateTime(20260908); err == nil || err.Error() != "cannot parse '20260908' as datetime" {
		t.Fatalf("integer datetime input error = %v", err)
	}
}

func FuzzDateTimeParserLayouts(f *testing.F) {
	for _, input := range []string{"09/08/2026", "08-Sep-2026", "12:34", "12:34:56.123456789", "2026-09-08 12:34", "2026-09-08T12:34:56+02:30", "2026-09-08T1:34:56Z"} {
		f.Add(input)
	}
	f.Fuzz(func(t *testing.T, input string) {
		for _, parser := range []struct {
			got  func(any) (time.Time, error)
			want func(string) (time.Time, error)
		}{{parseDateTime, referenceDateTimeLayouts}, {parseTimeValue, referenceTimeValueLayouts}} {
			got, gotErr := parser.got(input)
			want, wantErr := parser.want(input)
			if (gotErr == nil) != (wantErr == nil) || gotErr == nil && !got.Equal(want) {
				t.Fatalf("parse %q = %v, %v; want %v, %v", input, got, gotErr, want, wantErr)
			}
		}
	})
}
