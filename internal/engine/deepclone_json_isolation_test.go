package engine

import (
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// TestDeepCloneJSONColumnIsolation guards cloneCell/cloneJSONValue
// (internal/storage/snapshot.go): a JSON column's cell is a live
// map[string]any/[]any (see coerceToJson), and JSON_SET (json_path.go)
// mutates that structure in place. Before cloneJSONValue existed, cloneCell
// only deep-copied []byte, so a clone's row and the row it was cloned from
// shared the same map -- an UPDATE ... SET col = JSON_SET(col, ...) against
// a DeepClone/SnapshotForTx snapshot silently mutated the live,
// pre-transaction database, in a way ROLLBACK could not undo. This exact
// path is reachable both through the SQL driver's BeginTx and through the
// WASM browser/Node APIs' BEGIN. It covers both a top-level scalar key and a
// dot-numeric array index (see json_set_array_index_test.go for that path
// resolution's own dedicated coverage).
func TestDeepCloneJSONColumnIsolation(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE orders (id INT PRIMARY KEY, meta JSON)`)
	execSQL(t, db, `INSERT INTO orders VALUES (1, '{"device":"web","items":[{"sku":"A"}]}')`)

	clone := db.DeepClone()
	execSQL(t, clone, `UPDATE orders SET meta = JSON_SET(meta, 'device', 'mobile')`)
	execSQL(t, clone, `UPDATE orders SET meta = JSON_SET(meta, 'items.0.sku', 'Z')`)

	rs := execSQL(t, db, `SELECT JSON_GET(meta, 'device') AS device, JSON_GET(meta, 'items.0.sku') AS sku FROM orders WHERE id = 1`)
	if got := rs.Rows[0]["device"]; got != "web" {
		t.Fatalf("DeepClone leaked a scalar JSON_SET into the live DB: device = %v, want %q", got, "web")
	}
	if got := rs.Rows[0]["sku"]; got != "A" {
		t.Fatalf("DeepClone leaked an array-element JSON_SET into the live DB: sku = %v, want %q", got, "A")
	}

	rs2 := execSQL(t, clone, `SELECT JSON_GET(meta, 'device') AS device, JSON_GET(meta, 'items.0.sku') AS sku FROM orders WHERE id = 1`)
	if got := rs2.Rows[0]["device"]; got != "mobile" {
		t.Fatalf("clone did not observe its own scalar JSON_SET: device = %v, want %q", got, "mobile")
	}
	if got := rs2.Rows[0]["sku"]; got != "Z" {
		t.Fatalf("clone did not observe its own array-element JSON_SET: sku = %v, want %q", got, "Z")
	}
}
