package exporter

import (
	"fmt"
	"io"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/engine"
)

func benchmarkResultSet(rows int) *engine.ResultSet {
	rs := &engine.ResultSet{
		Cols: []string{"id", "name", "active", "score"},
		Rows: make([]engine.Row, rows),
	}
	for i := range rs.Rows {
		rs.Rows[i] = engine.Row{
			"id":     i,
			"name":   fmt.Sprintf("row-%d", i),
			"active": i%2 == 0,
			"score":  float64(i) / 10,
		}
	}
	return rs
}

func BenchmarkStreamingExports(b *testing.B) {
	rs := benchmarkResultSet(1000)
	benchmarks := []struct {
		name string
		fn   func(io.Writer, *engine.ResultSet, Options) error
	}{
		{name: "csv", fn: ExportCSV},
		{name: "json", fn: ExportJSON},
		{name: "ndjson", fn: ExportNDJSON},
		{name: "xml", fn: func(w io.Writer, rs *engine.ResultSet, _ Options) error { return ExportXML(w, rs) }},
		{name: "gob", fn: func(w io.Writer, rs *engine.ResultSet, _ Options) error { return ExportGOB(w, rs) }},
	}
	for _, benchmark := range benchmarks {
		b.Run(benchmark.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if err := benchmark.fn(io.Discard, rs, Options{}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
