package main

import (
	"context"
	"fmt"
	"testing"

	tinysql "github.com/SimonWaldherr/tinySQL"
)

// execTestSQL parses and executes sql against db, failing the test on any
// error. It mirrors the ParseSQL+Execute pattern used throughout main.go.
func execTestSQL(t *testing.T, ctx context.Context, db *tinysql.DB, tenant, sql string) *tinysql.ResultSet {
	t.Helper()
	stmt, err := tinysql.ParseSQL(sql)
	if err != nil {
		t.Fatalf("ParseSQL(%q) failed: %v", sql, err)
	}
	result, err := tinysql.Execute(ctx, db, tenant, stmt)
	if err != nil {
		t.Fatalf("Execute(%q) failed: %v", sql, err)
	}
	return result
}

// asInt64 normalizes the numeric types tinySQL might hand back for an
// integer column (int, int64, float64) into an int64 for easy comparison.
func asInt64(t *testing.T, v any) int64 {
	t.Helper()
	switch n := v.(type) {
	case int:
		return int64(n)
	case int64:
		return n
	case float64:
		return int64(n)
	}
	t.Fatalf("value %v (%T) is not a recognized integer type", v, v)
	return 0
}

func TestUpsertRowsIntoTinySQL_MixNewAndExisting(t *testing.T) {
	ctx := context.Background()
	db := tinysql.NewDB()
	tenant := "default"

	execTestSQL(t, ctx, db, tenant, "CREATE TABLE items (id INT, name TEXT, qty INT)")
	execTestSQL(t, ctx, db, tenant, "INSERT INTO items (id, name, qty) VALUES (1, 'a', 10)")
	execTestSQL(t, ctx, db, tenant, "INSERT INTO items (id, name, qty) VALUES (2, 'b', 20)")

	keyCols := []string{"id"}
	colNames := []string{"id", "name", "qty"}
	rows := []IncrementalRow{
		{
			Key:       computeRowKey([]any{int64(1)}),
			KeyValues: []any{int64(1)},
			Columns:   []any{int64(1), "a-updated", int64(99)},
		},
		{
			Key:       computeRowKey([]any{int64(3)}),
			KeyValues: []any{int64(3)},
			Columns:   []any{int64(3), "c", int64(30)},
		},
	}

	upserted, err := upsertRowsIntoTinySQL(ctx, db, tenant, "items", keyCols, colNames, rows)
	if err != nil {
		t.Fatalf("upsertRowsIntoTinySQL failed: %v", err)
	}
	if upserted != 2 {
		t.Fatalf("upserted = %d, want 2", upserted)
	}

	result := execTestSQL(t, ctx, db, tenant, "SELECT id, name, qty FROM items")
	if len(result.Rows) != 3 {
		t.Fatalf("final row count = %d, want 3", len(result.Rows))
	}

	byID := map[int64]tinysql.Row{}
	for _, r := range result.Rows {
		byID[asInt64(t, r["id"])] = r
	}

	if r, ok := byID[1]; !ok {
		t.Fatalf("id=1 missing from final rows")
	} else {
		if r["name"] != "a-updated" {
			t.Errorf("id=1 name = %v, want %q", r["name"], "a-updated")
		}
		if asInt64(t, r["qty"]) != 99 {
			t.Errorf("id=1 qty = %v, want 99", r["qty"])
		}
	}

	if r, ok := byID[2]; !ok {
		t.Fatalf("id=2 missing from final rows (should be untouched)")
	} else {
		if r["name"] != "b" {
			t.Errorf("id=2 name = %v, want %q (should be untouched)", r["name"], "b")
		}
		if asInt64(t, r["qty"]) != 20 {
			t.Errorf("id=2 qty = %v, want 20 (should be untouched)", r["qty"])
		}
	}

	if r, ok := byID[3]; !ok {
		t.Fatalf("id=3 missing from final rows (should have been inserted)")
	} else {
		if r["name"] != "c" {
			t.Errorf("id=3 name = %v, want %q", r["name"], "c")
		}
		if asInt64(t, r["qty"]) != 30 {
			t.Errorf("id=3 qty = %v, want 30", r["qty"])
		}
	}
}

func TestUpsertRowsIntoTinySQL_CompositeKey(t *testing.T) {
	ctx := context.Background()
	db := tinysql.NewDB()
	tenant := "default"

	execTestSQL(t, ctx, db, tenant, "CREATE TABLE stock (wh INT, sku TEXT, qty INT)")
	execTestSQL(t, ctx, db, tenant, "INSERT INTO stock (wh, sku, qty) VALUES (1, 'A', 5)")

	keyCols := []string{"wh", "sku"}
	colNames := []string{"wh", "sku", "qty"}
	rows := []IncrementalRow{
		{ // update existing (1, "A")
			KeyValues: []any{int64(1), "A"},
			Columns:   []any{int64(1), "A", int64(50)},
		},
		{ // insert new (1, "B")
			KeyValues: []any{int64(1), "B"},
			Columns:   []any{int64(1), "B", int64(7)},
		},
		{ // insert new (2, "A")
			KeyValues: []any{int64(2), "A"},
			Columns:   []any{int64(2), "A", int64(3)},
		},
	}
	for i := range rows {
		rows[i].Key = computeRowKey(rows[i].KeyValues)
	}

	upserted, err := upsertRowsIntoTinySQL(ctx, db, tenant, "stock", keyCols, colNames, rows)
	if err != nil {
		t.Fatalf("upsertRowsIntoTinySQL failed: %v", err)
	}
	if upserted != 3 {
		t.Fatalf("upserted = %d, want 3", upserted)
	}

	result := execTestSQL(t, ctx, db, tenant, "SELECT wh, sku, qty FROM stock")
	if len(result.Rows) != 3 {
		t.Fatalf("final row count = %d, want 3", len(result.Rows))
	}

	type key struct {
		wh  int64
		sku string
	}
	byKey := map[key]int64{}
	for _, r := range result.Rows {
		byKey[key{asInt64(t, r["wh"]), r["sku"].(string)}] = asInt64(t, r["qty"])
	}

	want := map[key]int64{
		{1, "A"}: 50,
		{1, "B"}: 7,
		{2, "A"}: 3,
	}
	for k, wantQty := range want {
		gotQty, ok := byKey[k]
		if !ok {
			t.Errorf("key %+v missing from final rows", k)
			continue
		}
		if gotQty != wantQty {
			t.Errorf("key %+v qty = %d, want %d", k, gotQty, wantQty)
		}
	}
}

func TestUpsertRowsIntoTinySQL_Empty(t *testing.T) {
	ctx := context.Background()
	db := tinysql.NewDB()
	tenant := "default"
	execTestSQL(t, ctx, db, tenant, "CREATE TABLE items (id INT, name TEXT)")

	upserted, err := upsertRowsIntoTinySQL(ctx, db, tenant, "items", []string{"id"}, []string{"id", "name"}, nil)
	if err != nil {
		t.Fatalf("upsertRowsIntoTinySQL with no rows failed: %v", err)
	}
	if upserted != 0 {
		t.Fatalf("upserted = %d, want 0", upserted)
	}
}

func TestDeleteRowsFromTinySQL_CompositeKeyChunking(t *testing.T) {
	ctx := context.Background()
	db := tinysql.NewDB()
	tenant := "default"

	execTestSQL(t, ctx, db, tenant, "CREATE TABLE wide (a INT, b INT, val TEXT)")

	const totalRows = 500
	const deleteCount = 450 // > deleteChunkSize (400), forces 2 chunks

	// Composite key (a, b): a = i/100 (0..4), b = i%100 (0..99) -> 500
	// unique pairs.
	for i := 0; i < totalRows; i++ {
		a := i / 100
		b := i % 100
		sql := fmt.Sprintf("INSERT INTO wide (a, b, val) VALUES (%d, %d, 'row-%d')", a, b, i)
		execTestSQL(t, ctx, db, tenant, sql)
	}

	keyCols := []string{"a", "b"}
	keysToDelete := make([]IncrementalRow, 0, deleteCount+5)
	for i := 0; i < deleteCount; i++ {
		a := i / 100
		b := i % 100
		keysToDelete = append(keysToDelete, IncrementalRow{KeyValues: []any{int64(a), int64(b)}})
	}
	// Add a few keys that don't exist, to confirm they don't inflate the
	// affected-row count.
	for i := 0; i < 5; i++ {
		keysToDelete = append(keysToDelete, IncrementalRow{KeyValues: []any{int64(999), int64(900 + i)}})
	}

	deleted, err := deleteRowsFromTinySQL(ctx, db, tenant, "wide", keyCols, keysToDelete)
	if err != nil {
		t.Fatalf("deleteRowsFromTinySQL failed: %v", err)
	}
	if deleted != int64(deleteCount) {
		t.Fatalf("deleted = %d, want %d", deleted, deleteCount)
	}

	result := execTestSQL(t, ctx, db, tenant, "SELECT a, b, val FROM wide")
	if len(result.Rows) != totalRows-deleteCount {
		t.Fatalf("remaining row count = %d, want %d", len(result.Rows), totalRows-deleteCount)
	}

	type key struct{ a, b int64 }
	remaining := map[key]bool{}
	for _, r := range result.Rows {
		remaining[key{asInt64(t, r["a"]), asInt64(t, r["b"])}] = true
	}

	// Everything from i=0..449 must be gone.
	for i := 0; i < deleteCount; i++ {
		a := int64(i / 100)
		b := int64(i % 100)
		if remaining[key{a, b}] {
			t.Errorf("row (a=%d, b=%d) [i=%d] should have been deleted but is still present", a, b, i)
		}
	}
	// Everything from i=450..499 must still be there.
	for i := deleteCount; i < totalRows; i++ {
		a := int64(i / 100)
		b := int64(i % 100)
		if !remaining[key{a, b}] {
			t.Errorf("row (a=%d, b=%d) [i=%d] should NOT have been deleted but is missing", a, b, i)
		}
	}
}

func TestDeleteRowsFromTinySQL_Empty(t *testing.T) {
	ctx := context.Background()
	db := tinysql.NewDB()
	tenant := "default"
	execTestSQL(t, ctx, db, tenant, "CREATE TABLE items (id INT)")

	deleted, err := deleteRowsFromTinySQL(ctx, db, tenant, "items", []string{"id"}, nil)
	if err != nil {
		t.Fatalf("deleteRowsFromTinySQL with no keys failed: %v", err)
	}
	if deleted != 0 {
		t.Fatalf("deleted = %d, want 0", deleted)
	}
}
