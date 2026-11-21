package engine

import (
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func BenchmarkJSONXML(b *testing.B) {
	doc := map[string]any{"order": map[string]any{"items": []any{map[string]any{"price": 12.5}}}}
	jsonRows := "[" + strings.TrimSuffix(strings.Repeat(`{"id":1,"name":"sample","value":12.5},`, 2000), ",") + "]"
	jsonLines := strings.Repeat("{\"id\":1,\"name\":\"sample\",\"value\":12.5}\n", 2000)
	xmlRows := "<root>" + strings.Repeat(`<record id="1"><name>sample</name></record><record id="2"><value>12.5</value></record>`, 1000) + "</root>"
	b.Run("json_path", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if jsonGet(doc, "order.items[0].price") != 12.5 {
				b.Fatal("wrong value")
			}
		}
	})
	for _, tc := range []struct {
		name string
		run  func() (*ResultSet, error)
	}{
		{"json_table", func() (*ResultSet, error) { return parseJSONToTable(jsonRows, "") }},
		{"json_lines", func() (*ResultSet, error) { return parseJSONLinesToTable(jsonLines) }},
		{"xml_explicit", func() (*ResultSet, error) { return parseXMLToTable(xmlRows, "record") }},
		{"xml_auto", func() (*ResultSet, error) { return parseXMLToTable(xmlRows, "") }},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				rs, err := tc.run()
				if err != nil || len(rs.Rows) != 2000 {
					b.Fatalf("invalid result: %v", err)
				}
			}
		})
	}
}

func BenchmarkJSONPathSQL(b *testing.B) {
	db := storage.NewDB()
	b.Cleanup(func() { _ = db.Close() })
	table := storage.NewTable("json_rows", []storage.Column{
		{Name: "doc", Type: storage.JsonType}, {Name: "path", Type: storage.TextType},
	}, false)
	const path = "order.items[0].price"
	doc := map[string]any{"order": map[string]any{"items": []any{map[string]any{"price": 12.5}}}}
	for i := range 50_000 {
		var value any = doc
		if i%10 == 0 {
			value = nil
		}
		table.Rows = append(table.Rows, []any{value, path})
	}
	if err := db.Put("default", table); err != nil {
		b.Fatal(err)
	}
	for _, tc := range []struct{ name, sql string }{
		{"literal", `SELECT COUNT(JSON_GET(doc, 'order.items[0].price')) AS n FROM json_rows`},
		{"filter", `SELECT COUNT(*) AS n FROM json_rows WHERE JSON_EXTRACT(doc, 'order.items[0].price') >= 10`},
		{"dynamic", `SELECT COUNT(JSON_GET(doc, path)) AS n FROM json_rows`},
	} {
		b.Run(tc.name, func(b *testing.B) {
			stmt := mustParse(tc.sql)
			b.ReportAllocs()
			for b.Loop() {
				result, err := Execute(b.Context(), db, "default", stmt)
				if err != nil {
					b.Fatal(err)
				}
				if len(result.Rows) != 1 || result.Rows[0]["n"] != 45_000 {
					b.Fatalf("unexpected result: %#v", result.Rows)
				}
			}
		})
	}
}
