package main

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
)

// openExternalTestDB opens a fresh temp-file SQLite database for a test via
// database/sql (driver "sqlite", modernc.org/sqlite -- already vendored, no
// server required), matching makeSourceSQLiteDB's pattern elsewhere in this
// package.
func openExternalTestDB(t *testing.T) *sql.DB {
	t.Helper()
	path := filepath.Join(t.TempDir(), "external_target.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestUpsertRowsIntoExternal_MixNewAndExisting(t *testing.T) {
	ctx := context.Background()
	extDB := openExternalTestDB(t)

	if _, err := extDB.ExecContext(ctx, `CREATE TABLE "items" ("id" INTEGER, "name" TEXT, "qty" INTEGER)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := extDB.ExecContext(ctx, `INSERT INTO "items" ("id", "name", "qty") VALUES (1, 'a', 10)`); err != nil {
		t.Fatalf("seed row 1: %v", err)
	}
	if _, err := extDB.ExecContext(ctx, `INSERT INTO "items" ("id", "name", "qty") VALUES (2, 'b', 20)`); err != nil {
		t.Fatalf("seed row 2: %v", err)
	}

	tx, err := extDB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}

	keyCols := []string{"id"}
	colNames := []string{"id", "name", "qty"}
	rows := []IncrementalRow{
		{ // update existing id=1
			KeyValues: []any{int64(1)},
			Columns:   []any{int64(1), "a-updated", int64(99)},
		},
		{ // insert new id=3
			KeyValues: []any{int64(3)},
			Columns:   []any{int64(3), "c", int64(30)},
		},
	}

	upserted, err := upsertRowsIntoExternal(ctx, tx, "sqlite", "items", keyCols, colNames, rows)
	if err != nil {
		tx.Rollback()
		t.Fatalf("upsertRowsIntoExternal failed: %v", err)
	}
	if upserted != 2 {
		tx.Rollback()
		t.Fatalf("upserted = %d, want 2", upserted)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	sqlRows, err := extDB.QueryContext(ctx, `SELECT "id", "name", "qty" FROM "items" ORDER BY "id"`)
	if err != nil {
		t.Fatalf("select final rows: %v", err)
	}
	defer sqlRows.Close()

	type got struct {
		id   int64
		name string
		qty  int64
	}
	var final []got
	for sqlRows.Next() {
		var g got
		if err := sqlRows.Scan(&g.id, &g.name, &g.qty); err != nil {
			t.Fatalf("scan: %v", err)
		}
		final = append(final, g)
	}
	if err := sqlRows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}

	want := []got{
		{1, "a-updated", 99}, // updated
		{2, "b", 20},         // untouched
		{3, "c", 30},         // inserted
	}
	if len(final) != len(want) {
		t.Fatalf("final row count = %d, want %d: %+v", len(final), len(want), final)
	}
	for i := range want {
		if final[i] != want[i] {
			t.Errorf("row %d = %+v, want %+v", i, final[i], want[i])
		}
	}
}

func TestUpsertRowsIntoExternal_CompositeKey(t *testing.T) {
	ctx := context.Background()
	extDB := openExternalTestDB(t)

	if _, err := extDB.ExecContext(ctx, `CREATE TABLE "stock" ("wh" INTEGER, "sku" TEXT, "qty" INTEGER)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := extDB.ExecContext(ctx, `INSERT INTO "stock" ("wh", "sku", "qty") VALUES (1, 'A', 5)`); err != nil {
		t.Fatalf("seed row: %v", err)
	}

	tx, err := extDB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}

	keyCols := []string{"wh", "sku"}
	colNames := []string{"wh", "sku", "qty"}
	rows := []IncrementalRow{
		{KeyValues: []any{int64(1), "A"}, Columns: []any{int64(1), "A", int64(50)}}, // update
		{KeyValues: []any{int64(1), "B"}, Columns: []any{int64(1), "B", int64(7)}},  // insert
		{KeyValues: []any{int64(2), "A"}, Columns: []any{int64(2), "A", int64(3)}},  // insert
	}

	upserted, err := upsertRowsIntoExternal(ctx, tx, "sqlite", "stock", keyCols, colNames, rows)
	if err != nil {
		tx.Rollback()
		t.Fatalf("upsertRowsIntoExternal failed: %v", err)
	}
	if upserted != 3 {
		tx.Rollback()
		t.Fatalf("upserted = %d, want 3", upserted)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	sqlRows, err := extDB.QueryContext(ctx, `SELECT "wh", "sku", "qty" FROM "stock" ORDER BY "wh", "sku"`)
	if err != nil {
		t.Fatalf("select final rows: %v", err)
	}
	defer sqlRows.Close()

	type key struct {
		wh  int64
		sku string
	}
	byKey := map[key]int64{}
	for sqlRows.Next() {
		var wh, qty int64
		var sku string
		if err := sqlRows.Scan(&wh, &sku, &qty); err != nil {
			t.Fatalf("scan: %v", err)
		}
		byKey[key{wh, sku}] = qty
	}
	if err := sqlRows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}

	want := map[key]int64{
		{1, "A"}: 50,
		{1, "B"}: 7,
		{2, "A"}: 3,
	}
	if len(byKey) != len(want) {
		t.Fatalf("final row count = %d, want %d: %+v", len(byKey), len(want), byKey)
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

func TestUpsertRowsIntoExternal_Empty(t *testing.T) {
	ctx := context.Background()
	extDB := openExternalTestDB(t)
	if _, err := extDB.ExecContext(ctx, `CREATE TABLE "items" ("id" INTEGER, "name" TEXT)`); err != nil {
		t.Fatalf("create table: %v", err)
	}

	tx, err := extDB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback()

	upserted, err := upsertRowsIntoExternal(ctx, tx, "sqlite", "items", []string{"id"}, []string{"id", "name"}, nil)
	if err != nil {
		t.Fatalf("upsertRowsIntoExternal with no rows failed: %v", err)
	}
	if upserted != 0 {
		t.Fatalf("upserted = %d, want 0", upserted)
	}
}

func TestDeleteRowsFromExternal_CompositeKeyChunking(t *testing.T) {
	ctx := context.Background()
	extDB := openExternalTestDB(t)

	if _, err := extDB.ExecContext(ctx, `CREATE TABLE "wide" ("a" INTEGER, "b" INTEGER, "val" TEXT)`); err != nil {
		t.Fatalf("create table: %v", err)
	}

	const totalRows = 500
	const deleteCount = 450 // > deleteChunkSize (400), forces 2 chunks

	// Composite key (a, b): a = i/100 (0..4), b = i%100 (0..99) -> 500
	// unique pairs.
	{
		tx, err := extDB.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("begin seed tx: %v", err)
		}
		for i := 0; i < totalRows; i++ {
			a := i / 100
			b := i % 100
			if _, err := tx.ExecContext(ctx, `INSERT INTO "wide" ("a", "b", "val") VALUES (?, ?, ?)`, a, b, fmt.Sprintf("row-%d", i)); err != nil {
				tx.Rollback()
				t.Fatalf("seed row %d: %v", i, err)
			}
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("commit seed: %v", err)
		}
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

	tx, err := extDB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin delete tx: %v", err)
	}

	deleted, err := deleteRowsFromExternal(ctx, tx, "sqlite", "wide", keyCols, keysToDelete)
	if err != nil {
		tx.Rollback()
		t.Fatalf("deleteRowsFromExternal failed: %v", err)
	}
	if deleted != int64(deleteCount) {
		tx.Rollback()
		t.Fatalf("deleted = %d, want %d", deleted, deleteCount)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit delete: %v", err)
	}

	sqlRows, err := extDB.QueryContext(ctx, `SELECT "a", "b" FROM "wide"`)
	if err != nil {
		t.Fatalf("select remaining rows: %v", err)
	}
	defer sqlRows.Close()

	type key struct{ a, b int64 }
	remaining := map[key]bool{}
	for sqlRows.Next() {
		var a, b int64
		if err := sqlRows.Scan(&a, &b); err != nil {
			t.Fatalf("scan: %v", err)
		}
		remaining[key{a, b}] = true
	}
	if err := sqlRows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}

	if len(remaining) != totalRows-deleteCount {
		t.Fatalf("remaining row count = %d, want %d", len(remaining), totalRows-deleteCount)
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

func TestDeleteRowsFromExternal_SingleColumnKeyUsesIN(t *testing.T) {
	ctx := context.Background()
	extDB := openExternalTestDB(t)

	if _, err := extDB.ExecContext(ctx, `CREATE TABLE "items" ("id" INTEGER, "name" TEXT)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	for i := 1; i <= 5; i++ {
		if _, err := extDB.ExecContext(ctx, `INSERT INTO "items" ("id", "name") VALUES (?, ?)`, i, fmt.Sprintf("item-%d", i)); err != nil {
			t.Fatalf("seed row %d: %v", i, err)
		}
	}

	tx, err := extDB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}

	keys := []IncrementalRow{
		{KeyValues: []any{int64(2)}},
		{KeyValues: []any{int64(4)}},
	}
	deleted, err := deleteRowsFromExternal(ctx, tx, "sqlite", "items", []string{"id"}, keys)
	if err != nil {
		tx.Rollback()
		t.Fatalf("deleteRowsFromExternal failed: %v", err)
	}
	if deleted != 2 {
		tx.Rollback()
		t.Fatalf("deleted = %d, want 2", deleted)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	sqlRows, err := extDB.QueryContext(ctx, `SELECT "id" FROM "items" ORDER BY "id"`)
	if err != nil {
		t.Fatalf("select remaining rows: %v", err)
	}
	defer sqlRows.Close()

	var remaining []int64
	for sqlRows.Next() {
		var id int64
		if err := sqlRows.Scan(&id); err != nil {
			t.Fatalf("scan: %v", err)
		}
		remaining = append(remaining, id)
	}
	if err := sqlRows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}

	want := []int64{1, 3, 5}
	if len(remaining) != len(want) {
		t.Fatalf("remaining = %v, want %v", remaining, want)
	}
	for i := range want {
		if remaining[i] != want[i] {
			t.Errorf("remaining[%d] = %d, want %d", i, remaining[i], want[i])
		}
	}
}

func TestDeleteRowsFromExternal_Empty(t *testing.T) {
	ctx := context.Background()
	extDB := openExternalTestDB(t)
	if _, err := extDB.ExecContext(ctx, `CREATE TABLE "items" ("id" INTEGER)`); err != nil {
		t.Fatalf("create table: %v", err)
	}

	tx, err := extDB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback()

	deleted, err := deleteRowsFromExternal(ctx, tx, "sqlite", "items", []string{"id"}, nil)
	if err != nil {
		t.Fatalf("deleteRowsFromExternal with no keys failed: %v", err)
	}
	if deleted != 0 {
		t.Fatalf("deleted = %d, want 0", deleted)
	}
}
