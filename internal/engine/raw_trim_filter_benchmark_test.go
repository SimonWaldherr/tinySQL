package engine

import (
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func BenchmarkWhereTrimComparisons(b *testing.B) {
	db := storage.NewDB()
	b.Cleanup(func() { _ = db.Close() })
	table := storage.NewTable("where_trim", []storage.Column{{Name: "body", Type: storage.TextType}, {Name: "other", Type: storage.TextType}, {Name: "cut", Type: storage.TextType}}, false)
	for range 50000 {
		table.Rows = append(table.Rows, []any{"xxα🙂xx", "yyα🙂yy", "xy"})
	}
	if err := db.Put("default", table); err != nil {
		b.Fatal(err)
	}
	for _, tc := range []struct{ name, where string }{
		{"Literal", "TRIM(body,cut)='α🙂'"},
		{"Reversed", "'α🙂'=TRIM(body,cut)"},
		{"TwoTrims", "TRIM(body,cut)=TRIM(other,cut)"},
		{"NoMatch", "TRIM(body,cut)='missing'"},
		{"PlainControl", "body='missing'"},
	} {
		b.Run(tc.name, func(b *testing.B) { runBench(b, db, "SELECT COUNT(*) FROM where_trim WHERE "+tc.where) })
	}
}
