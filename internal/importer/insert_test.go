package importer

import (
	"context"
	"encoding/csv"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// csvReaderFromString is a small helper to avoid importing encoding/csv in tests repeatedly.
func csvReaderFromString(s string) *csv.Reader {
	return csv.NewReader(strings.NewReader(s))
}

func TestTruncateAndInsertAllRecords(t *testing.T) {
	ctx := context.Background()
	db := storage.NewDB()

	// Create table
	colNames := []string{"id", "name"}
	colTypes := []storage.ColType{storage.IntType, storage.TextType}
	if err := createTable(ctx, db, "default", "t1", colNames, colTypes); err != nil {
		t.Fatalf("createTable failed: %v", err)
	}

	opts := &ImportOptions{BatchSize: 1, TypeInference: false, CreateTable: true}
	recs := [][]string{{"1", "A"}, {"2", "B"}}

	rows, skipped, errs := insertAllRecords(ctx, db, "default", "t1", colNames, colTypes, recs, opts)
	if rows != 2 || skipped != 0 || len(errs) != 0 {
		t.Fatalf("insertAllRecords unexpected result: rows=%d skipped=%d errs=%v", rows, skipped, errs)
	}

	// Truncate
	if err := truncateTable(ctx, db, "default", "t1"); err != nil {
		t.Fatalf("truncateTable failed: %v", err)
	}
	tbl, _ := db.Get("default", "t1")
	if len(tbl.Rows) != 0 {
		t.Fatalf("expected 0 rows after truncate, got %d", len(tbl.Rows))
	}
}

func TestConvertRow_StrictFallback(t *testing.T) {
	opts := &ImportOptions{StrictTypes: false, DateTimeFormats: nil, NullLiterals: []string{""}}
	colNames := []string{"a"}
	colTypes := []storage.ColType{storage.IntType}
	row, err := convertRow([]string{"notint"}, colNames, colTypes, opts)
	if err != nil {
		t.Fatalf("convertRow should not error in non-strict mode: %v", err)
	}
	if row[0] != "notint" {
		t.Fatalf("convertRow fallback expected string, got %v", row[0])
	}

	// strict mode should error
	opts.StrictTypes = true
	if _, err := convertRow([]string{"notint"}, colNames, colTypes, opts); err == nil {
		t.Fatalf("convertRow expected error in strict mode")
	}
}

func TestInsertAllRecordsStrictContinuesAfterBadRow(t *testing.T) {
	db := storage.NewDB()
	cols := []string{"num"}
	types := []storage.ColType{storage.IntType}
	if err := createTable(context.Background(), db, "default", "records", cols, types); err != nil {
		t.Fatal(err)
	}
	opts := &ImportOptions{StrictTypes: true, BatchSize: 10}
	inserted, skipped, errs := insertAllRecords(context.Background(), db, "default", "records", cols, types,
		[][]string{{"12"}, {"bad"}, {"34"}}, opts)
	if inserted != 2 || skipped != 1 || len(errs) != 1 {
		t.Fatalf("inserted=%d skipped=%d errors=%v", inserted, skipped, errs)
	}
	tbl, _ := db.Get("default", "records")
	if len(tbl.Rows) != 2 || tbl.Rows[0][0] != int64(12) || tbl.Rows[1][0] != int64(34) {
		t.Fatalf("rows = %#v", tbl.Rows)
	}
}

func TestStreamInsertCSV(t *testing.T) {
	ctx := context.Background()
	db := storage.NewDB()

	colNames := []string{"id", "name"}
	colTypes := []storage.ColType{storage.IntType, storage.TextType}
	if err := createTable(ctx, db, "default", "stream_tbl", colNames, colTypes); err != nil {
		t.Fatalf("createTable failed: %v", err)
	}

	// CSV data: two rows
	csvData := "1,A\n2,B\n"
	r := csvReaderFromString(csvData)

	opts := &ImportOptions{BatchSize: 1, TypeInference: false}
	rows, skipped, errs, err := insertCSVRecords(ctx, db, "default", "stream_tbl", colNames, colTypes, nil, r, opts)
	if rows != 2 || skipped != 0 || len(errs) != 0 || err != nil {
		t.Fatalf("streamInsertCSV unexpected: rows=%d skipped=%d errs=%v error=%v", rows, skipped, errs, err)
	}
}
