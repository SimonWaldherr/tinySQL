package engine

import (
	"fmt"
	"runtime"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func BenchmarkSmallInProbe(b *testing.B) {
	for _, count := range []int{2, 4, 8} {
		for _, kind := range []string{"integer", "real", "text"} {
			values := make([]any, count)
			var miss any
			for i := range values {
				switch kind {
				case "integer":
					values[i], miss = i, -1
				case "real":
					values[i], miss = float64(i), -1.0
				case "text":
					values[i], miss = fmt.Sprint(i), "missing"
				}
			}
			for _, probe := range []struct {
				name  string
				value any
			}{{"first", values[0]}, {"last", values[count-1]}, {"miss", miss}} {
				b.Run(fmt.Sprintf("%s/n%d/%s", kind, count, probe.name), func(b *testing.B) {
					filter := buildInFilter(0, values, false)
					row := []any{probe.value}
					b.ReportAllocs()
					for b.Loop() {
						got, err := filter(row)
						if err != nil {
							b.Fatal(err)
						}
						runtime.KeepAlive(got)
					}
				})
			}
		}
	}
}

func BenchmarkInSelectPaths(b *testing.B) {
	db := storage.NewDB()
	b.Cleanup(func() { _ = db.Close() })
	table := storage.NewTable("in_paths", []storage.Column{{Name: "id", Type: storage.IntType, Constraint: storage.PrimaryKey}, {Name: "tag", Type: storage.TextType}, {Name: "embedding", Type: storage.VectorType}}, false)
	for i := range 10000 {
		table.Rows = append(table.Rows, []any{i, fmt.Sprint(i % 8), []float64{float64(i % 17), float64(i % 11)}})
	}
	if err := db.Put("default", table); err != nil {
		b.Fatal(err)
	}
	keys := make([]string, 64)
	for i := range keys {
		keys[i] = fmt.Sprint(i * 100)
	}
	for _, tc := range []struct{ name, sql string }{
		{"count", "SELECT COUNT(*) AS n FROM in_paths WHERE tag IN ('1','3','5','7')"},
		{"indexed64", "SELECT COUNT(*) AS n FROM in_paths WHERE id IN (" + strings.Join(keys, ",") + ")"},
		{"vector", "SELECT id,VEC_L2_DISTANCE(embedding, '[1,2]') AS distance FROM in_paths WHERE tag IN ('1','3') ORDER BY distance LIMIT 20"},
	} {
		b.Run(tc.name, func(b *testing.B) {
			stmt := mustParse(tc.sql)
			if _, err := Execute(b.Context(), db, "default", stmt); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			for b.Loop() {
				if _, err := Execute(b.Context(), db, "default", stmt); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
