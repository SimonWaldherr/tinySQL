package engine

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"
)

var strftimeBenchmarkSink string

// Keep the previous formatter as a same-process benchmark control. Its layout
// translation has known literal and week-numbering defects; only timings matter.
var legacyStrftimeReplacer = strings.NewReplacer(
	"%Y", "2006", "%m", "01", "%d", "02", "%H", "15",
	"%M", "04", "%S", "05", "%%", "%",
)

func legacyStrftimeFormat(t time.Time, format string) string {
	result := format
	if strings.Contains(result, "%j") {
		result = strings.ReplaceAll(result, "%j", fmt.Sprintf("%03d", t.YearDay()))
	}
	if strings.Contains(result, "%W") {
		result = strings.ReplaceAll(result, "%W", fmt.Sprintf("%02d", (t.YearDay()-int(t.Weekday())+7)/7))
	}
	if strings.Contains(result, "%w") {
		result = strings.ReplaceAll(result, "%w", strconv.Itoa(int(t.Weekday())))
	}
	if strings.Contains(result, "%s") {
		result = strings.ReplaceAll(result, "%s", strconv.FormatInt(t.Unix(), 10))
	}
	return t.Format(legacyStrftimeReplacer.Replace(result))
}

func BenchmarkStrftimeFormat(b *testing.B) {
	date := time.Date(2024, 3, 5, 6, 7, 8, 123_000_000, time.UTC)
	for _, tc := range []struct{ name, format string }{
		{"Date", "%Y-%m-%d"},
		{"DateTime", "%Y-%m-%d %H:%M:%S"},
		{"Calendar", "%Y-%j-%W-%w"},
		{"Epoch", "%s"},
		{"Literal", "unchanged literal text"},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.Run("Scanner", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					strftimeBenchmarkSink, _ = strftimeFormat(date, tc.format)
				}
			})
			b.Run("Previous", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					strftimeBenchmarkSink = legacyStrftimeFormat(date, tc.format)
				}
			})
		})
	}
}
