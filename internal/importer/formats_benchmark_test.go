package importer

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func benchmarkRows(n int) []struct {
	ID     int
	Name   string
	Active bool
	Score  float64
} {
	rows := make([]struct {
		ID     int
		Name   string
		Active bool
		Score  float64
	}, n)
	for i := range rows {
		rows[i] = struct {
			ID     int
			Name   string
			Active bool
			Score  float64
		}{
			ID:     i + 1,
			Name:   fmt.Sprintf("user_%04d", i+1),
			Active: i%2 == 0,
			Score:  float64(i%100) + 0.5,
		}
	}
	return rows
}

func benchmarkCSVData(n int) []byte {
	var b strings.Builder
	b.WriteString("id,name,active,score\n")
	for _, row := range benchmarkRows(n) {
		fmt.Fprintf(&b, "%d,%s,%t,%.1f\n", row.ID, row.Name, row.Active, row.Score)
	}
	return []byte(b.String())
}

func benchmarkJSONData(n int) []byte {
	var b strings.Builder
	b.WriteByte('[')
	for i, row := range benchmarkRows(n) {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, `{"id":%d,"name":%q,"active":%t,"score":%.1f}`,
			row.ID, row.Name, row.Active, row.Score)
	}
	b.WriteByte(']')
	return []byte(b.String())
}

func benchmarkYAMLData(n int) []byte {
	var b strings.Builder
	for _, row := range benchmarkRows(n) {
		fmt.Fprintf(&b, "- id: %d\n  name: %s\n  active: %t\n  score: %.1f\n",
			row.ID, row.Name, row.Active, row.Score)
	}
	return []byte(b.String())
}

func benchmarkXMLData(n int) []byte {
	var b strings.Builder
	b.WriteString("<root>")
	for _, row := range benchmarkRows(n) {
		fmt.Fprintf(&b, `<record id="%d" name="%s" active="%t" score="%.1f"/>`,
			row.ID, row.Name, row.Active, row.Score)
	}
	b.WriteString("</root>")
	return []byte(b.String())
}

func BenchmarkImportStructuredFormats(b *testing.B) {
	ctx := context.Background()
	benchmarks := []struct {
		name       string
		data       []byte
		importFunc func(context.Context, *storage.DB, string, string, *bytes.Reader, *ImportOptions) (*ImportResult, error)
	}{
		{
			name: "csv",
			data: benchmarkCSVData(500),
			importFunc: func(ctx context.Context, db *storage.DB, tenant, table string, src *bytes.Reader, opts *ImportOptions) (*ImportResult, error) {
				return ImportCSV(ctx, db, tenant, table, src, opts)
			},
		},
		{
			name: "json",
			data: benchmarkJSONData(500),
			importFunc: func(ctx context.Context, db *storage.DB, tenant, table string, src *bytes.Reader, opts *ImportOptions) (*ImportResult, error) {
				return ImportJSON(ctx, db, tenant, table, src, opts)
			},
		},
		{
			name: "yaml",
			data: benchmarkYAMLData(500),
			importFunc: func(ctx context.Context, db *storage.DB, tenant, table string, src *bytes.Reader, opts *ImportOptions) (*ImportResult, error) {
				return ImportYAML(ctx, db, tenant, table, src, opts)
			},
		},
		{
			name: "xml",
			data: benchmarkXMLData(500),
			importFunc: func(ctx context.Context, db *storage.DB, tenant, table string, src *bytes.Reader, opts *ImportOptions) (*ImportResult, error) {
				return ImportXML(ctx, db, tenant, table, src, opts)
			},
		},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			b.SetBytes(int64(len(bm.data)))
			for i := 0; i < b.N; i++ {
				db := storage.NewDB()
				reader := bytes.NewReader(bm.data)
				_, err := bm.importFunc(ctx, db, "default", "bench_data", reader,
					&ImportOptions{CreateTable: true, HeaderMode: "present", TypeInference: true})
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
