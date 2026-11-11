package engine

import (
	"fmt"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func BenchmarkDistinctNumericSQL(b *testing.B) {
	for _, kind := range []string{"Float", "Integer"} {
		for _, cardinality := range []int{1000, 50000} {
			b.Run(fmt.Sprintf("%s/Distinct%d", kind, cardinality), func(b *testing.B) {
				db := storage.NewDB()
				b.Cleanup(func() { _ = db.Close() })
				table := storage.NewTable("numeric_values", []storage.Column{
					{Name: "g", Type: storage.IntType}, {Name: "v", Type: storage.InterfaceType},
				}, false)
				for i := 0; i < 50000; i++ {
					var value any = i % cardinality
					if kind == "Float" {
						value = float64(i%cardinality) + 0.5
					}
					table.Rows = append(table.Rows, []any{i % 100, value})
				}
				if err := db.Put("default", table); err != nil {
					b.Fatal(err)
				}
				for _, query := range []struct{ name, sql string }{
					{"Count", "SELECT COUNT(DISTINCT v) AS n FROM numeric_values"},
					{"Select", "SELECT DISTINCT v FROM numeric_values"},
					{"Grouped", "SELECT g, COUNT(DISTINCT v) AS n FROM numeric_values GROUP BY g"},
				} {
					b.Run(query.name, func(b *testing.B) {
						statement := mustParse(query.sql)
						b.ReportAllocs()
						for b.Loop() {
							result, err := Execute(b.Context(), db, "default", statement)
							if err != nil {
								b.Fatal(err)
							}
							switch query.name {
							case "Count":
								if len(result.Rows) != 1 || result.Rows[0]["n"] != cardinality {
									b.Fatal("unexpected distinct count", result.Rows)
								}
							case "Select":
								if len(result.Rows) != cardinality {
									b.Fatal("unexpected distinct rows", len(result.Rows))
								}
							case "Grouped":
								if len(result.Rows) != 100 || result.Rows[0]["n"] != cardinality/100 {
									b.Fatal("unexpected grouped count", result.Rows)
								}
							}
						}
					})
				}
			})
		}
	}
}

func BenchmarkDistinctFloatMixedSQL(b *testing.B) {
	for _, cardinality := range []int{1000, 50000} {
		for _, mixedAt := range []int{1, 49999} {
			b.Run(fmt.Sprintf("Distinct%d/MixedAt%d", cardinality, mixedAt), func(b *testing.B) {
				db := storage.NewDB()
				b.Cleanup(func() { _ = db.Close() })
				table := storage.NewTable("mixed_floats", []storage.Column{{Name: "v", Type: storage.InterfaceType}}, false)
				for i := range 50000 {
					var value any = float64(i%cardinality) + 0.5
					if i == mixedAt {
						value = "0.5"
					}
					table.Rows = append(table.Rows, []any{value})
				}
				if err := db.Put("default", table); err != nil {
					b.Fatal(err)
				}
				want := cardinality
				if cardinality == 50000 {
					want--
				}
				statement := mustParse("SELECT COUNT(DISTINCT v) AS n FROM mixed_floats")
				b.ReportAllocs()
				for b.Loop() {
					result, err := Execute(b.Context(), db, "default", statement)
					if err != nil || len(result.Rows) != 1 || result.Rows[0]["n"] != want {
						b.Fatalf("got %v / %v, want %d", result, err, want)
					}
				}
			})
		}
	}
}
