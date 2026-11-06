package tinystore

import (
	"encoding/gob"
	"fmt"
	tsql "github.com/SimonWaldherr/tinySQL"
	"os"
	"path/filepath"
	"testing"
)

// Same input and fsync policy for both codecs. Gob is a codec baseline,
// not an end-to-end ModeDisk database benchmark.
func BenchmarkSnapshot(b *testing.B) {
	cols := []tsql.Column{{Name: "region", Type: tsql.TextType}, {Name: "month", Type: tsql.TextType}, {Name: "units", Type: tsql.IntType}, {Name: "revenue", Type: tsql.FloatType}}
	tbl := tsql.NewTable("facts", cols, false)
	regions := []string{"north", "south", "east", "west"}
	for i := 0; i < 100000; i++ {
		tbl.Rows = append(tbl.Rows, []any{regions[i%4], "2026-11-01", i, float64(i) / 10})
	}
	for _, codec := range []string{"columnar", "gob"} {
		b.Run(codec, func(b *testing.B) {
			dir := b.TempDir()
			path := filepath.Join(dir, FileName(tbl.Name))
			write := func() error {
				if codec == "columnar" {
					_, err := Write(dir, tbl)
					return err
				}
				f, err := os.Create(path)
				if err != nil {
					return err
				}
				defer f.Close()
				if err = gob.NewEncoder(f).Encode(tbl); err != nil {
					return err
				}
				return f.Sync()
			}
			if err := write(); err != nil {
				b.Fatal(err)
			}
			info, err := os.Stat(path)
			if err != nil {
				b.Fatal(err)
			}
			b.Run("write", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					if err := write(); err != nil {
						b.Fatal(err)
					}
				}
				b.ReportMetric(float64(info.Size()), "file-bytes")
			})
			b.Run("read", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					if codec == "columnar" {
						if _, err := Read(path); err != nil {
							b.Fatal(err)
						}
					} else {
						f, err := os.Open(path)
						if err != nil {
							b.Fatal(err)
						}
						var got tsql.Table
						err = gob.NewDecoder(f).Decode(&got)
						f.Close()
						if err != nil {
							b.Fatal(err)
						}
					}
				}
			})
		})
	}
}

// BenchmarkEncodeColumn isolates CPU and allocation costs from filesystem sync.
func BenchmarkEncodeColumn(b *testing.B) {
	const count = 100000
	rows := make([][]any, count)
	for i := range rows {
		var nullable any = i
		if i%10 == 0 {
			nullable = nil
		}
		rows[i] = []any{"2026-11-01", []string{"north", "south", "east", "west"}[i%4], i, float64(i) / 10, nullable, fmt.Sprintf("unique-%d", i), fmt.Sprintf("skew-%d", i%1024)}
	}
	for _, tc := range []struct {
		name string
		col  int
		typ  byte
	}{
		{"text_constant", 0, typeText}, {"text_cycling", 1, typeText},
		{"int", 2, typeInt}, {"float", 3, typeFloat}, {"int_nullable", 4, typeInt}, {"text_distinct", 5, typeText}, {"text_skew", 6, typeText},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_, _, err := encodeColumn(rows, tc.col, tc.typ)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkWriteTextCardinality includes durable file writing for distinct text values.
func BenchmarkWriteTextCardinality(b *testing.B) {
	for _, count := range []int{100000, 120000} {
		b.Run(fmt.Sprintf("rows=%d", count), func(b *testing.B) {
			table := tsql.NewTable("distinct", []tsql.Column{{Name: "value", Type: tsql.TextType}}, false)
			for i := 0; i < count; i++ {
				table.Rows = append(table.Rows, []any{fmt.Sprintf("value-%d", i)})
			}
			dir := b.TempDir()
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if _, err := Write(dir, table); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
