package engine

import (
	"strings"
	"testing"
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
