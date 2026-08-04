//go:build sqliteimport && !js && !wasm && !baremetal

package storage

import (
	"fmt"
	"math/big"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
)

func TestSQLiteBackend_BasicCRUD(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.sqlite")
	b, err := NewSQLiteBackend(path)
	if err != nil {
		t.Fatal(err)
	}
	if b.Mode() != ModeSQLite {
		t.Fatalf("mode: got %v, want %v", b.Mode(), ModeSQLite)
	}

	tbl := makeTestTable("products", 50)
	if err := b.SaveTable("default", tbl); err != nil {
		t.Fatal(err)
	}

	if !b.TableExists("default", "products") {
		t.Fatal("TableExists should be true")
	}
	if b.TableExists("default", "nonexistent") {
		t.Fatal("TableExists should be false for nonexistent")
	}

	names, err := b.ListTableNames("default")
	if err != nil {
		t.Fatal(err)
	}
	if len(names) != 1 || names[0] != "products" {
		t.Fatalf("ListTableNames: got %v, want [products]", names)
	}

	loaded, err := b.LoadTable("default", "products")
	if err != nil {
		t.Fatal(err)
	}
	assertTableEqualJSON(t, loaded, tbl)

	missing, err := b.LoadTable("default", "nonexistent")
	if err != nil || missing != nil {
		t.Fatalf("LoadTable nonexistent: got %v, %v", missing, err)
	}

	if err := b.DeleteTable("default", "products"); err != nil {
		t.Fatal(err)
	}
	if b.TableExists("default", "products") {
		t.Fatal("TableExists should be false after delete")
	}

	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestSQLiteBackend_NativeColumnTypes verifies the resulting .sqlite file
// really is a plain SQLite table with native columns for the common types —
// the whole point of ModeSQLite over a JSON/GOB blob — by reopening the raw
// file with a second connection and querying it directly.
func TestSQLiteBackend_NativeColumnTypes(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "native.sqlite")
	b, err := NewSQLiteBackend(path)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	cols := []Column{
		{Name: "id", Type: IntType},
		{Name: "price", Type: Float64Type},
		{Name: "label", Type: StringType},
		{Name: "active", Type: BoolType},
		{Name: "payload", Type: BlobType},
	}
	tbl := NewTable("items", cols, false)
	tbl.Rows = append(tbl.Rows, []any{int64(1), 9.99, "widget", true, []byte("blob-data")})
	tbl.Rows = append(tbl.Rows, []any{int64(2), 19.5, "gadget", false, nil})
	if err := b.SaveTable("default", tbl); err != nil {
		t.Fatal(err)
	}

	loaded, err := b.LoadTable("default", "items")
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded.Rows) != 2 {
		t.Fatalf("row count: got %d, want 2", len(loaded.Rows))
	}
	if got, ok := loaded.Rows[0][0].(int64); !ok || got != 1 {
		t.Errorf("id: got %v (%T), want int64(1)", loaded.Rows[0][0], loaded.Rows[0][0])
	}
	if got, ok := loaded.Rows[0][1].(float64); !ok || got != 9.99 {
		t.Errorf("price: got %v (%T), want float64(9.99)", loaded.Rows[0][1], loaded.Rows[0][1])
	}
	if got, ok := loaded.Rows[0][3].(bool); !ok || got != true {
		t.Errorf("active: got %v (%T), want true", loaded.Rows[0][3], loaded.Rows[0][3])
	}
	if got, ok := loaded.Rows[1][3].(bool); !ok || got != false {
		t.Errorf("active row2: got %v (%T), want false", loaded.Rows[1][3], loaded.Rows[1][3])
	}
	if got, ok := loaded.Rows[0][4].([]byte); !ok || string(got) != "blob-data" {
		t.Errorf("payload: got %v (%T), want []byte(blob-data)", loaded.Rows[0][4], loaded.Rows[0][4])
	}
	if loaded.Rows[1][4] != nil {
		t.Errorf("payload row2: got %v, want nil", loaded.Rows[1][4])
	}

	// Query the raw SQLite file directly with a second, independent
	// connection to confirm it is genuinely a native table (INTEGER/REAL/
	// TEXT columns queryable with plain SQL), not an opaque blob.
	raw, err := NewSQLiteBackend(path)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	var label string
	var price float64
	if err := raw.db.QueryRow(`SELECT "label", "price" FROM "items" WHERE "id" = 1`).Scan(&label, &price); err != nil {
		t.Fatalf("direct SQL query against native table failed: %v", err)
	}
	if label != "widget" || price != 9.99 {
		t.Errorf("direct query: got (%q, %v), want (widget, 9.99)", label, price)
	}
}

// TestSQLiteBackend_ExoticTypes verifies types with no native SQLite
// equivalent (Decimal, UUID, JSON, Vector) round-trip through the
// JSON-encoded-text fallback path.
func TestSQLiteBackend_ExoticTypes(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "exotic.sqlite")
	b, err := NewSQLiteBackend(path)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	cols := []Column{
		{Name: "id", Type: IntType},
		{Name: "amount", Type: DecimalType},
		{Name: "token", Type: UUIDType},
		{Name: "meta", Type: JsonType},
		{Name: "embedding", Type: VectorType},
	}
	tbl := NewTable("exotic", cols, false)
	id := uuid.New()
	rat := big.NewRat(355, 113)
	tbl.Rows = append(tbl.Rows, []any{
		int64(1),
		rat,
		id,
		map[string]any{"k": "v", "n": float64(3)},
		[]float64{1.5, 2.5, 3.5},
	})
	if err := b.SaveTable("default", tbl); err != nil {
		t.Fatal(err)
	}

	loaded, err := b.LoadTable("default", "exotic")
	if err != nil {
		t.Fatal(err)
	}
	row := loaded.Rows[0]
	if got := fmt.Sprint(row[1]); got != rat.String() {
		t.Errorf("amount: got %v, want %v", got, rat.String())
	}
	if got := fmt.Sprint(row[2]); got != id.String() {
		t.Errorf("token: got %v, want %v", got, id.String())
	}
	metaMap, ok := row[3].(map[string]any)
	if !ok || metaMap["k"] != "v" {
		t.Errorf("meta: got %#v, want map with k=v", row[3])
	}
	vec, ok := row[4].([]float64)
	if !ok || len(vec) != 3 || vec[0] != 1.5 {
		t.Errorf("embedding: got %#v (%T), want []float64{1.5,2.5,3.5}", row[4], row[4])
	}
}

// TestSQLiteBackend_MultiTenant verifies two tenants can each have a
// same-named table without colliding in the underlying SQL table namespace.
func TestSQLiteBackend_MultiTenant(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "multi.sqlite")
	b, err := NewSQLiteBackend(path)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	t1 := makeTestTable("users", 3)
	t2 := makeTestTable("users", 5)
	if err := b.SaveTable("tenant_a", t1); err != nil {
		t.Fatal(err)
	}
	if err := b.SaveTable("tenant_b", t2); err != nil {
		t.Fatal(err)
	}

	loaded1, err := b.LoadTable("tenant_a", "users")
	if err != nil {
		t.Fatal(err)
	}
	loaded2, err := b.LoadTable("tenant_b", "users")
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded1.Rows) != 3 {
		t.Errorf("tenant_a rows: got %d, want 3", len(loaded1.Rows))
	}
	if len(loaded2.Rows) != 5 {
		t.Errorf("tenant_b rows: got %d, want 5", len(loaded2.Rows))
	}
}

// TestSQLiteBackend_ReadOnly verifies SetReadOnly rejects mutations.
func TestSQLiteBackend_ReadOnly(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ro.sqlite")
	b, err := NewSQLiteBackend(path)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	tbl := makeTestTable("locked", 2)
	if err := b.SaveTable("default", tbl); err != nil {
		t.Fatal(err)
	}
	b.SetReadOnly(true)
	if err := b.SaveTable("default", tbl); err != ErrReadOnlyStorage {
		t.Fatalf("SaveTable while read-only: got %v, want ErrReadOnlyStorage", err)
	}
	if err := b.DeleteTable("default", "locked"); err != ErrReadOnlyStorage {
		t.Fatalf("DeleteTable while read-only: got %v, want ErrReadOnlyStorage", err)
	}
}

// TestSQLiteBackend_OpenDBRoundTrip exercises ModeSQLite through the public
// OpenDB entry point, including a close+reopen cycle.
func TestSQLiteBackend_OpenDBRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "opendb.sqlite")

	db, err := OpenDB(StorageConfig{Mode: ModeSQLite, Path: path})
	if err != nil {
		t.Fatal(err)
	}
	tbl := makeTestTable("accounts", 10)
	if err := db.Put("default", tbl); err != nil {
		t.Fatal(err)
	}
	if err := db.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db2, err := OpenDB(StorageConfig{Mode: ModeSQLite, Path: path})
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	got, err := db2.Get("default", "accounts")
	if err != nil {
		t.Fatal(err)
	}
	assertTableEqualJSON(t, got, tbl)
}
