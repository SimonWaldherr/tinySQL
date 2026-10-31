package importer

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func benchmarkJSONLinesData(n int) []byte {
	var data bytes.Buffer
	for _, row := range benchmarkRows(n) {
		fmt.Fprintf(&data, "{\"id\":%d,\"name\":%q,\"active\":%t,\"score\":%.1f}\n", row.ID, row.Name, row.Active, row.Score)
	}
	return data.Bytes()
}

func BenchmarkJSONRecordParsing(b *testing.B) {
	data := benchmarkJSONLinesData(2000)
	b.Run("StandardJSONL", func(b *testing.B) {
		b.SetBytes(int64(len(data)))
		b.ReportAllocs()
		for b.Loop() {
			result := &ImportResult{}
			next, err := streamJSONRecords(bytes.NewReader(data), result, 0)
			if err != nil {
				b.Fatal(err)
			}
			count := 0
			for {
				_, err := next()
				if err == io.EOF {
					break
				}
				if err != nil {
					b.Fatal(err)
				}
				count++
			}
			if count != 2000 || len(result.Errors) != 0 {
				b.Fatalf("records=%d, errors=%v", count, result.Errors)
			}
		}
	})
	b.Run("FuzzyJSONL", func(b *testing.B) {
		text := string(data)
		opts := &FuzzyImportOptions{FuzzyJSON: true}
		b.SetBytes(int64(len(data)))
		b.ReportAllocs()
		for b.Loop() {
			records, err := fuzzyParseJSONInput(text, opts)
			if err != nil || len(records) != 2000 {
				b.Fatalf("records=%d error=%v", len(records), err)
			}
		}
	})
}

func BenchmarkJSONImport(b *testing.B) {
	for _, fixture := range []struct {
		name string
		data []byte
	}{{"Array", benchmarkJSONData(2000)}, {"Lines", benchmarkJSONLinesData(2000)}} {
		b.Run("Standard/"+fixture.name, func(b *testing.B) {
			b.SetBytes(int64(len(fixture.data)))
			b.ReportAllocs()
			for b.Loop() {
				db := storage.NewDB()
				result, err := ImportJSON(b.Context(), db, "default", "items", bytes.NewReader(fixture.data), &ImportOptions{CreateTable: true, TypeInference: true})
				_ = db.Close()
				if err != nil || result.RowsInserted != 2000 {
					b.Fatalf("result=%+v error=%v", result, err)
				}
			}
		})
		for _, inference := range []bool{false, true} {
			b.Run(fmt.Sprintf("Fuzzy/%s/Inference=%v", fixture.name, inference), func(b *testing.B) {
				b.SetBytes(int64(len(fixture.data)))
				b.ReportAllocs()
				for b.Loop() {
					db := storage.NewDB()
					opts := &FuzzyImportOptions{ImportOptions: &ImportOptions{CreateTable: true, TypeInference: inference}}
					result, err := FuzzyImportJSON(b.Context(), db, "default", "items", bytes.NewReader(fixture.data), opts)
					_ = db.Close()
					if err != nil || result.RowsInserted != 2000 {
						b.Fatalf("result=%+v error=%v", result, err)
					}
				}
			})
		}
	}
}
