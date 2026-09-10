package engine

import (
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestJSONPathCacheCompatibility(t *testing.T) {
	for _, path := range []string{"", "a", "a.b[0].c", "a.0.c", "[0]", "a..b", "a[bad]", "a[-1]", "a[", ".[]", "a[1].b", strings.Repeat("long.", 1000)} {
		if got, want := cachedJSONPath(path), parseJSONPath(path); !reflect.DeepEqual(got, want) {
			t.Fatalf("%q: %v want %v", path, got, want)
		}
	}
	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wg.Go(func() {
			for i := 0; i < 400; i++ {
				path := fmt.Sprintf("items[%d].value", i%4)
				doc := map[string]any{"items": []any{map[string]any{"value": 0}, map[string]any{"value": 1}, map[string]any{"value": 2}, map[string]any{"value": 3}}}
				if got := jsonGet(doc, path); got != i%4 {
					t.Errorf("stale path %s: %v", path, got)
				}
				jsonSet(doc, path, 9)
				if got := jsonGet(doc, path); got != 9 {
					t.Errorf("setter: %v", got)
				}
				cachedJSONPath(fmt.Sprintf("eviction.%d", i))
			}
		})
	}
	wg.Wait()
	jsonPathCache.RLock()
	defer jsonPathCache.RUnlock()
	if len(jsonPathCache.paths) > jsonPathCacheEntries {
		t.Fatal("unbounded cache")
	}
	for path := range jsonPathCache.paths {
		if len(path) > jsonPathCacheMaxBytes {
			t.Fatal("oversized cached path")
		}
	}
}

func TestJSONTableOwnedRows(t *testing.T) {
	for _, tc := range []struct {
		input string
		cols  []string
		rows  []Row
	}{
		{`[{"id":1,"name":"a"},{"ID":2,"Nested":{"UP":true}},null,[1,2]]`, []string{"id", "name", "nested", "value"}, []Row{{"id": float64(1), "name": "a"}, {"id": float64(2), "nested": map[string]any{"UP": true}}, {"value": nil}, {"value": []any{float64(1), float64(2)}}}},
		{`{"NAME":"a","id":1}`, []string{"id", "name"}, []Row{{"name": "a", "id": float64(1)}}},
		{`[]`, []string{}, []Row{}},
		{`null`, []string{"value"}, []Row{{"value": nil}}},
	} {
		got, err := parseJSONToTable(tc.input, "")
		if err != nil || !reflect.DeepEqual(got.Cols, tc.cols) || !reflect.DeepEqual(got.Rows, tc.rows) {
			t.Fatalf("%s: %v %v", tc.input, got, err)
		}
	}
	lines := "\n {\"id\":1} \r\n {\"ID\":2}\n true\n"
	rs, err := parseJSONLinesToTable(lines)
	if err != nil || !reflect.DeepEqual(rs.Rows, []Row{{"id": float64(1)}, {"id": float64(2)}, {"value": true}}) {
		t.Fatalf("JSONL: %v %v", rs, err)
	}
	a, _ := parseJSONToTable(`[{"x":{"v":1}},{"x":{"v":2}}]`, "")
	b, _ := parseJSONToTable(`[{"x":{"v":1}},{"x":{"v":2}}]`, "")
	a.Rows[0]["x"].(map[string]any)["v"] = 9
	if b.Rows[0]["x"].(map[string]any)["v"] != float64(1) || a.Rows[1]["x"].(map[string]any)["v"] != float64(2) {
		t.Fatal("aliased JSON results")
	}
	for _, input := range []string{`{"x":`, `{} garbage`, `[1,]`} {
		if _, err := parseJSONToTable(input, ""); err == nil {
			t.Fatal("invalid JSON accepted")
		}
	}
	if _, err := parseJSONLinesToTable("{}\ninvalid"); err == nil {
		t.Fatal("invalid JSONL accepted")
	}
}

func TestXMLTablePathsAndErrors(t *testing.T) {
	doc := `<root xmlns:p="urn:test"><records><p:record ID="1"><name lang="en"> A &amp; B </name><value><![CDATA[<ok>]]></value></p:record><p:record ID="2"><name>C</name></p:record></records></root>`
	want := []Row{{"attr_id": "1", "attr_name_lang": "en", "name": "A & B", "value": "<ok>"}, {"attr_id": "2", "name": "C"}}
	for _, path := range []string{"record", "/root/records/record", "records/record", ""} {
		rs, err := parseXMLToTable(doc, path)
		if err != nil || !reflect.DeepEqual(rs.Rows, want) {
			t.Fatalf("path %q: %v %v", path, rs, err)
		}
	}
	for _, doc := range []string{`<root><record>`, `<root><record></root>`, `<root><record><x>&unknown;</x></record></root>`, `<root><record/></root><broken`} {
		for _, path := range []string{"record", "", "absent"} {
			if _, err := parseXMLToTable(doc, path); err == nil {
				t.Fatalf("malformed XML accepted: %s path %s", doc, path)
			}
		}
	}
	if rs, err := parseXMLToTable("", ""); err != nil || len(rs.Rows) != 0 {
		t.Fatalf("empty XML: %v %v", rs, err)
	}
	if rs, err := parseXMLToTable(doc, "absent"); err != nil || len(rs.Rows) != 0 {
		t.Fatalf("missing path: %v %v", rs, err)
	}
}

func TestJSONXMLTableSQL(t *testing.T) {
	db := storage.NewDB()
	defer db.Close()
	for _, sql := range []string{
		`SELECT name FROM table_from_json('[{"name":"a"},{"name":"b"}]') ORDER BY name`,
		`SELECT name FROM table_from_xml('<root><record><name>a</name></record><record><name>b</name></record></root>', 'record') ORDER BY name`,
	} {
		rs := execSQL(t, db, sql)
		if !reflect.DeepEqual(rs.Rows, []Row{{"name": "a"}, {"name": "b"}}) {
			t.Fatalf("%s: %v", sql, rs)
		}
	}
	if _, err := Execute(t.Context(), db, "default", mustParse(`SELECT * FROM table_from_xml('<root><record>', 'record')`)); err == nil {
		t.Fatal("XML error lost in SQL execution")
	}
}
