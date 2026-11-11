package engine

import (
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func BenchmarkScalarPipeline(b *testing.B) {
	db := storage.NewDB()
	b.Cleanup(func() { _ = db.Close() })
	table := storage.NewTable("scalar_rows", []storage.Column{
		{Name: "v", Type: storage.IntType}, {Name: "body", Type: storage.TextType}, {Name: "cut", Type: storage.TextType},
	}, false)
	for i := range 50000 {
		var value any = i % 100
		if i%10 == 0 {
			value = nil
		}
		table.Rows = append(table.Rows, []any{value, " xx Payload α🙂 xx ", " x"})
	}
	if err := db.Put("default", table); err != nil {
		b.Fatal(err)
	}
	for _, tc := range []struct{ name, sql string }{
		{"isnull_argument", "SELECT COUNT(ISNULL(v)) FROM scalar_rows"},
		{"nullif", "SELECT COUNT(NULLIF(v,1)) FROM scalar_rows"},
		{"coalesce_nested", "SELECT SUM(COALESCE(v,LENGTH(LOWER(body)))) FROM scalar_rows"},
		{"if_fallback", "SELECT SUM(IF(ISNULL(v),LENGTH(LOWER(body)),v)) FROM scalar_rows"},
		{"computed_null", "SELECT COUNT(*) FROM scalar_rows WHERE NULLIF(v,1) IS NULL"},
		{"computed_between", "SELECT COUNT(*) FROM scalar_rows WHERE COALESCE(v,0) BETWEEN 40 AND 60"},
		{"computed_not_between", "SELECT COUNT(*) FROM scalar_rows WHERE COALESCE(v,0) NOT BETWEEN 40 AND 60"},
		{"trim_default", "SELECT COUNT(*) FROM scalar_rows WHERE TRIM(body) = 'missing'"},
		{"trim_literal", "SELECT COUNT(*) FROM scalar_rows WHERE TRIM(body,' x') = 'missing'"},
		{"trim_dynamic", "SELECT COUNT(*) FROM scalar_rows WHERE TRIM(body,cut) = 'missing'"},
		{"trim_length", "SELECT SUM(LENGTH(TRIM(body,cut))) FROM scalar_rows"},
		{"case_conversion", "SELECT COUNT(*) FROM scalar_rows WHERE LOWER(UPPER(body)) = 'missing'"},
		{"conversion_input", "SELECT SUM(TRY_CONVERT(INT,COALESCE(NULLIF(v,1),0))) FROM scalar_rows"},
	} {
		b.Run(tc.name, func(b *testing.B) { runBench(b, db, tc.sql) })
	}
}
