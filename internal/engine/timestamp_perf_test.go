package engine

import (
	"testing"
	"time"
)

func BenchmarkTimestampParse(b *testing.B) {
	for _, input := range []string{"2026-09-08 12:34:56", "2026-09-08 12:34:56.123456789", "2026-09-08T12:34:56.123456789", "2026-09-08T12:34:56.123456789Z", "2026-09-08T12:34:56.123456789+02:00"} {
		b.Run(input, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if _, err := parseDateTime(input); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestTimestampParsingMatchesLayouts(t *testing.T) {
	inputs := []string{
		"2026-09-08 12:34:56", "2026-09-08T12:34:56", "2026-09-08",
		"2024-02-29 23:59:59", "2023-02-29 12:00:00", "2026-13-01 00:00:00",
		"2026-09-08 24:00:00", "2026-09-08T12:34:60Z", "2026-09-08T12:34:56z",
		"2026-09-08 12:34:56Z", "2026-09-08Z", "2026-09-08T12:34:56.Z",
		"0000-01-01 00:00:00", "9999-12-31 23:59:59", "2026-09-08 12:34:56.x",
		"2026-09-08T12:34:56.123456789+02:30", "2026-09-08T12:34:56-07:00",
	}
	for _, sep := range []string{" ", "T"} {
		for _, fraction := range []string{".1", ".123456", ".123456789", ".12345678912345", ",123", ".", ".12x"} {
			for _, zone := range []string{"", "Z"} {
				inputs = append(inputs, "2026-09-08"+sep+"12:34:56"+fraction+zone)
			}
		}
	}
	for _, input := range inputs {
		t.Run(input, func(t *testing.T) {
			var want time.Time
			var valid bool
			for _, layout := range parseDateTimeFallbackFormats {
				if parsed, err := time.Parse(layout, input); err == nil {
					want, valid = parsed, true
					break
				}
			}
			got, err := parseDateTime(input)
			if (err == nil) != valid || valid && !got.Equal(want) {
				t.Fatalf("got %v, %v; want %v (valid=%v)", got, err, want, valid)
			}
			// These ISO inputs have the same accepted layouts in both consumers.
			got, err = parseTimeValue(input)
			if (err == nil) != valid || valid && !got.Equal(want) {
				t.Fatalf("parseTimeValue: got %v, %v; want %v (valid=%v)", got, err, want, valid)
			}
		})
	}
}

func FuzzTimestampFastPath(f *testing.F) {
	for _, s := range []string{"2026-09-08T12:34:56.123456789Z", "2026-09-08 12:34:56,123456789123", "2023-02-29", "2026-09-08T12:34:56+02:00"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		got, ok := parseTimeFixedDigits(s)
		if !ok {
			return
		}
		for _, layout := range parseDateTimeFallbackFormats {
			if want, err := time.Parse(layout, s); err == nil {
				if !got.Equal(want) {
					t.Fatalf("fast parse %q: %v, want %v", s, got, want)
				}
				return
			}
		}
		t.Fatalf("fast path accepted invalid timestamp %q", s)
	})
}
