package engine

import (
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// TestJSONSetDotNumericArrayIndex guards jsonGet/jsonSet's array-index
// resolution (json_path.go): parseJSONPath only ever produces an explicit
// pathPart.idx >= 0 for bracket syntax ("items[0]"); a bare numeric
// dot-segment ("items.0") always parses to {key: "0", idx: -1}, matching the
// path convention already used elsewhere in this repo (see
// cmd/wasm_browser/web/app.js's json_get_nested demo query). Before
// arrayIndex existed, landing on a []any with an unresolved idx == -1 made
// both jsonGet and jsonSet's array branches bail out (return nil / return v
// unchanged) without ever applying the read or write.
func TestJSONSetDotNumericArrayIndex(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE orders (id INT PRIMARY KEY, meta JSON)`)
	execSQL(t, db, `INSERT INTO orders VALUES (1, '{"device":"web","items":[{"sku":"A"}]}')`)

	rsBefore := execSQL(t, db, `SELECT JSON_GET(meta, 'items.0.sku') AS sku FROM orders WHERE id = 1`)
	if got := rsBefore.Rows[0]["sku"]; got != "A" {
		t.Fatalf("JSON_GET with a dot-numeric array index: got %v, want %q", got, "A")
	}

	execSQL(t, db, `UPDATE orders SET meta = JSON_SET(meta, 'items.0.sku', 'Z')`)

	rsAfter := execSQL(t, db, `SELECT JSON_GET(meta, 'items.0.sku') AS sku, meta FROM orders WHERE id = 1`)
	if got := rsAfter.Rows[0]["sku"]; got != "Z" {
		t.Fatalf("JSON_SET with a dot-numeric array index did not persist: sku = %v, want %q (meta: %#v)",
			got, "Z", rsAfter.Rows[0]["meta"])
	}
}

// TestJSONSetDotNumericArrayIndexAppend covers setting a not-yet-existing
// array element via a dot-numeric path, and a top-level (not nested) array
// element set (path with exactly two parts).
func TestJSONSetDotNumericArrayIndexAppend(t *testing.T) {
	v := jsonSet(map[string]any{"items": []any{"a"}}, "items.1", "b")
	m, ok := v.(map[string]any)
	if !ok {
		t.Fatalf("expected map[string]any, got %T", v)
	}
	arr, ok := m["items"].([]any)
	if !ok || len(arr) != 2 || arr[0] != "a" || arr[1] != "b" {
		t.Fatalf("expected items == [a b], got %#v", m["items"])
	}
}
