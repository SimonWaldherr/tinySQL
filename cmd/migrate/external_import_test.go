package main

import (
	"bytes"
	"context"
	"database/sql"
	"math/big"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	tinysql "github.com/SimonWaldherr/tinySQL"
)

func migrationSource(t *testing.T) *sql.DB {
	t.Helper()
	src, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "source.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { src.Close() })
	return src
}
func migrationExec(t *testing.T, db *tinysql.DB, query string) *tinysql.ResultSet {
	t.Helper()
	stmt, err := tinysql.ParseSQL(query)
	if err != nil {
		t.Fatal(err)
	}
	rs, err := tinysql.Execute(t.Context(), db, "default", stmt)
	if err != nil {
		t.Fatal(err)
	}
	return rs
}

func TestExternalImportBatchFallback(t *testing.T) {
	src := migrationSource(t)
	for _, q := range []string{`CREATE TABLE source (id INTEGER, value INTEGER)`, `WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM seq WHERE n<600) INSERT INTO source SELECT n,CASE WHEN n%200=0 THEN 'invalid' ELSE n END FROM seq`} {
		if _, err := src.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	db := tinysql.NewDB()
	defer db.Close()
	stats, err := importFromExternal(db, t.Context(), "default", src, `SELECT * FROM source ORDER BY id`, "target")
	if err != nil || stats.Imported != 597 || stats.Skipped != 3 {
		t.Fatalf("stats=%+v err=%v", stats, err)
	}
	for i, n := range []string{"200", "400", "600"} {
		if !strings.Contains(stats.Errors[i], "source row "+n) {
			t.Fatal(stats.Errors)
		}
	}
	rs := migrationExec(t, db, `SELECT COUNT(*) AS n, COUNT(DISTINCT id) AS unique_count FROM target`)
	if rs.Rows[0]["n"] != 597 || rs.Rows[0]["unique_count"] != 597 {
		t.Fatalf("rollback/retry duplicated rows: %v", rs)
	}
}

func TestExternalImportExactTypesAndNames(t *testing.T) {
	src := migrationSource(t)
	if _, err := src.Exec(`CREATE TABLE source ("order" INTEGER, price "TEXT DECIMAL", payload BLOB, label TEXT)`); err != nil {
		t.Fatal(err)
	}
	amount := "12345678901234567890.123456789"
	first := bytes.Repeat([]byte{0, 255, 128}, 220000)
	second := bytes.Repeat([]byte{1, 254, 129}, 220000)
	for i, payload := range [][]byte{first, second} {
		if _, err := src.Exec(`INSERT INTO source VALUES (?,?,?,?)`, i+1, amount, payload, "it’s text"); err != nil {
			t.Fatal(err)
		}
	}
	db := tinysql.NewDB()
	defer db.Close()
	stats, err := importFromExternal(db, t.Context(), "default", src, `SELECT * FROM source ORDER BY "order"`, "target")
	if err != nil || stats.Imported != 2 || stats.Skipped != 0 {
		t.Fatalf("%+v %v", stats, err)
	}
	table, err := db.Get("default", "target")
	if err != nil {
		t.Fatal(err)
	}
	if table.Cols[1].Type != tinysql.DecimalType || table.Cols[2].Type != tinysql.BlobType {
		t.Fatal(table.Cols)
	}
	want, _ := new(big.Rat).SetString(amount)
	for i, row := range table.Rows {
		got, ok := row[1].(*big.Rat)
		if !ok || got.Cmp(want) != 0 {
			t.Fatalf("decimal: %T %v", row[1], row[1])
		}
		if !bytes.Equal(row[2].([]byte), [][]byte{first, second}[i]) {
			t.Fatal("corrupt blob")
		}
	}
	// Incremental reads use the same owned, typed values.
	_, _, rows, err := fetchExternalRows(t.Context(), src, `SELECT * FROM source ORDER BY "order"`)
	if err != nil || len(rows) != 2 || rows[0][1].(*big.Rat).Cmp(want) != 0 || !bytes.Equal(rows[0][2].([]byte), first) {
		t.Fatalf("incremental scan: %v", err)
	}
}

func TestNormalizeExternalDriverBytes(t *testing.T) {
	input := []any{[]byte("hello"), []byte("123.456"), []byte{0, 255}, []byte("42"), nil}
	got, _, err := normalizeExternalRow(input, []string{"TEXT", "DECIMAL", "BLOB", "INT", "TEXT"})
	want := []any{"hello", big.NewRat(123456, 1000), []byte{0, 255}, "42", nil}
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("%v %v", got, err)
	}
	input[0] = "next row"
	if got[0] != "hello" {
		t.Fatal("aliased scan destinations")
	}
	if _, _, err := normalizeExternalRow([]any{[]byte("invalid")}, []string{"DECIMAL"}); err == nil {
		t.Fatal("invalid decimal accepted")
	}
}

func TestExternalImportDuplicateColumnsAndSkipLimit(t *testing.T) {
	src := migrationSource(t)
	db := tinysql.NewDB()
	defer db.Close()
	if _, err := importFromExternal(db, t.Context(), "default", src, `SELECT 1 AS "a-b", 2 AS "a_b"`, "conflict"); err == nil {
		t.Fatal("ambiguous columns accepted")
	}
	if _, err := db.Get("default", "conflict"); err == nil {
		t.Fatal("created ambiguous table")
	}
	if _, err := tinysql.Execute(t.Context(), db, "default", tinysql.NewTableBuilder("target").Int("id").Build()); err != nil {
		t.Fatal(err)
	}
	batch := make([]externalImportRow, 110)
	for i := range batch {
		batch[i] = externalImportRow{values: []any{"invalid"}, number: i + 1}
	}
	var stats importStats
	if err := insertExternalBatch(t.Context(), db, "default", "target", []string{"id"}, batch, &stats); err == nil || stats.Skipped != 101 || len(stats.Errors) != importErrorSample {
		t.Fatalf("%+v %v", stats, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := insertExternalBatch(ctx, db, "default", "target", []string{"id"}, []externalImportRow{{values: []any{1}, number: 1}}, &stats); err != context.Canceled {
		t.Fatal(err)
	}
	if rs := migrationExec(t, db, `SELECT * FROM target`); len(rs.Rows) != 0 {
		t.Fatal(rs)
	}
}
