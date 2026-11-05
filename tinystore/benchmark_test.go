package tinystore

import (
	"encoding/gob"
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
