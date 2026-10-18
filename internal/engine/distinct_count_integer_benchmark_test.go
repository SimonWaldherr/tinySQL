package engine

import (
	"strconv"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func BenchmarkDistinctIntegerSet(b *testing.B) {
	for _, tc := range []struct {
		name        string
		cardinality int
		mixed       bool
	}{
		{"repeated", 1000, false},
		{"unique", 50000, false},
		{"mixed", 1000, true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			values := make([]any, 50000)
			for i := range values {
				values[i] = int64(i % tc.cardinality)
			}
			if tc.mixed {
				values[1] = "1"
			}
			for _, version := range []struct {
				name string
				run  func([]any) int
			}{
				{"before", countDistinctTextBenchmark},
				{"after", countDistinctIntegerBenchmark},
			} {
				b.Run(version.name, func(b *testing.B) {
					b.ReportAllocs()
					for b.Loop() {
						if count := version.run(values); count != tc.cardinality {
							b.Fatalf("got %d distinct values, want %d", count, tc.cardinality)
						}
					}
				})
			}
		})
	}
}

func countDistinctIntegerBenchmark(values []any) int {
	var seen distinctCountSet
	count := 0
	for _, value := range values {
		if seen.add(value) {
			count++
		}
	}
	return count
}

// Preserve the previous text-only implementation as an A/B reference in the
// same process, so allocation and throughput comparisons use identical input.
func countDistinctTextBenchmark(values []any) int {
	var seen map[string]struct{}
	var buf []byte
	for _, value := range values {
		if value == nil {
			continue
		}
		if seen == nil {
			seen = make(map[string]struct{})
		}
		if text, ok := value.(string); ok {
			if _, exists := seen[text]; !exists {
				seen[text] = struct{}{}
			}
			continue
		}
		buf = buf[:0]
		switch value := value.(type) {
		case int:
			buf = strconv.AppendInt(buf, int64(value), 10)
		case int64:
			buf = strconv.AppendInt(buf, value, 10)
		case float64:
			buf = strconv.AppendFloat(buf, value, 'g', -1, 64)
		case bool:
			buf = strconv.AppendBool(buf, value)
		default:
			buf = append(buf, valueText(value)...)
		}
		if _, exists := seen[string(buf)]; !exists {
			seen[string(buf)] = struct{}{}
		}
	}
	return len(seen)
}

func BenchmarkDistinctIntegerSQL(b *testing.B) {
	db := storage.NewDB()
	b.Cleanup(func() { _ = db.Close() })
	table := storage.NewTable("integer_counts", []storage.Column{{Name: "g", Type: storage.IntType}, {Name: "v", Type: storage.IntType}}, false)
	for i := 0; i < 50000; i++ {
		table.Rows = append(table.Rows, []any{i % 100, i % 1000})
	}
	if err := db.Put("default", table); err != nil {
		b.Fatal(err)
	}
	for _, tc := range []struct{ name, query string }{
		{"count", `SELECT COUNT(DISTINCT v) AS n FROM integer_counts`},
		{"grouped", `SELECT g, COUNT(DISTINCT v) AS n FROM integer_counts GROUP BY g`},
	} {
		b.Run(tc.name, func(b *testing.B) {
			stmt := mustParse(tc.query)
			b.ReportAllocs()
			for b.Loop() {
				if _, err := Execute(b.Context(), db, "default", stmt); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
