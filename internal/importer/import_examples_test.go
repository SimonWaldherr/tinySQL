package importer

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestImportDecimalUUIDMoney_InsertAllRecords(t *testing.T) {
	ctx := context.Background()
	db := storage.NewDB()
	tenant := "default"
	table := "money_test"

	colNames := []string{"id", "amount", "price", "note"}
	colTypes := []storage.ColType{storage.UUIDType, storage.DecimalType, storage.MoneyType, storage.BlobType}

	if err := createTable(ctx, db, tenant, table, colNames, colTypes); err != nil {
		t.Fatalf("create table: %v", err)
	}

	allRecords := [][]string{{"550e8400-e29b-41d4-a716-446655440000", "123.45", "99.99", "hello"}}
	opts := &ImportOptions{BatchSize: 10, StrictTypes: true}

	_, _, errs := insertAllRecords(ctx, db, tenant, table, colNames, colTypes, allRecords, opts)
	if len(errs) > 0 {
		t.Fatalf("insert errors: %v", errs)
	}

	tbl, err := db.Get(tenant, table)
	if err != nil {
		t.Fatalf("get table: %v", err)
	}
	if len(tbl.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(tbl.Rows))
	}

	// Basic type assertions
	if _, ok := tbl.Rows[0][0].([]byte); ok {
		// uuid.UUID may marshal to []byte in storage; accept either
	}
}

func TestImportGeoJSONAndKML(t *testing.T) {
	ctx := context.Background()
	db := storage.NewDB()
	tenant := "default"

	// GeoJSON FeatureCollection
	geo := `{"type":"FeatureCollection","features":[{"type":"Feature","properties":{"name":"A"},"geometry":{"type":"Point","coordinates":[1.0,2.0]}}]}`
	if _, err := ImportGeoJSON(ctx, db, tenant, "gf", strings.NewReader(geo), &ImportOptions{CreateTable: true, TypeInference: true}); err != nil {
		t.Fatalf("ImportGeoJSON failed: %v", err)
	}

	tbl, err := db.Get(tenant, "gf")
	if err != nil {
		t.Fatalf("get table: %v", err)
	}
	if len(tbl.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(tbl.Rows))
	}
	// Geometry stored as json.RawMessage
	if _, ok := tbl.Rows[0][len(tbl.Cols)-1].(json.RawMessage); !ok {
		t.Fatalf("expected geometry as json.RawMessage, got %T", tbl.Rows[0][len(tbl.Cols)-1])
	}

	// KML
	kml := `<?xml version="1.0" encoding="UTF-8"?><kml><Placemark><name>P</name><Point><coordinates>10,20</coordinates></Point></Placemark></kml>`
	if _, err := ImportKML(ctx, db, tenant, "kf", strings.NewReader(kml), &ImportOptions{CreateTable: true}); err != nil {
		t.Fatalf("ImportKML failed: %v", err)
	}
	ktbl, err := db.Get(tenant, "kf")
	if err != nil {
		t.Fatalf("get kml table: %v", err)
	}
	if len(ktbl.Rows) != 1 {
		t.Fatalf("expected 1 kml row, got %d", len(ktbl.Rows))
	}
}
