package exporter_test

import (
	"archive/zip"
	"bytes"
	"context"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/engine"
	"github.com/SimonWaldherr/tinySQL/internal/exporter"
	"github.com/SimonWaldherr/tinySQL/internal/importer"
	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestExportXLSXStructure(t *testing.T) {
	rs := &engine.ResultSet{
		Cols: []string{"id", "name"},
		Rows: []engine.Row{
			{"id": int64(1), "name": "Alice"},
			{"id": int64(2), "name": "Bob & Co <special>"},
		},
	}
	var buf bytes.Buffer
	if err := exporter.ExportXLSX(&buf, rs, exporter.Options{}); err != nil {
		t.Fatalf("ExportXLSX: %v", err)
	}

	zr, err := zip.NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatalf("output is not a valid zip/xlsx: %v", err)
	}
	want := map[string]bool{
		"[Content_Types].xml":        false,
		"_rels/.rels":                false,
		"xl/workbook.xml":            false,
		"xl/_rels/workbook.xml.rels": false,
		"xl/styles.xml":              false,
		"xl/worksheets/sheet1.xml":   false,
	}
	for _, f := range zr.File {
		if _, ok := want[f.Name]; ok {
			want[f.Name] = true
		}
	}
	for name, found := range want {
		if !found {
			t.Errorf("missing expected zip part %q", name)
		}
	}
}

// TestExportImportXLSXRoundTrip exports a ResultSet with a mix of value
// types (int, float, string with XML-special characters, bool, nil) and
// re-imports the resulting workbook, checking the data survives the round
// trip with sensible native typing (numbers stay numeric, not text).
func TestExportImportXLSXRoundTrip(t *testing.T) {
	rs := &engine.ResultSet{
		Cols: []string{"id", "label", "price", "active", "note"},
		Rows: []engine.Row{
			{"id": int64(1), "label": "widget", "price": 9.99, "active": true, "note": nil},
			{"id": int64(2), "label": "Bob & Co <\"quoted\">", "price": 100.0, "active": false, "note": "hello"},
		},
	}
	var buf bytes.Buffer
	if err := exporter.ExportXLSX(&buf, rs, exporter.Options{}); err != nil {
		t.Fatalf("ExportXLSX: %v", err)
	}

	db := storage.NewDB()
	ctx := context.Background()
	opts := &importer.ImportOptions{CreateTable: true, TypeInference: true}
	result, err := importer.ImportXLSX(ctx, db, "default", "items", bytes.NewReader(buf.Bytes()), opts)
	if err != nil {
		t.Fatalf("ImportXLSX: %v", err)
	}
	if result.RowsInserted != 2 {
		t.Fatalf("RowsInserted = %d, want 2", result.RowsInserted)
	}
	if !result.HadHeader {
		t.Fatalf("HadHeader = false, want true")
	}

	tbl, err := db.Get("default", "items")
	if err != nil {
		t.Fatalf("get table: %v", err)
	}
	if len(tbl.Rows) != 2 {
		t.Fatalf("row count = %d, want 2", len(tbl.Rows))
	}

	colIdx := func(name string) int {
		for i, c := range tbl.Cols {
			if c.Name == name {
				return i
			}
		}
		t.Fatalf("column %q not found", name)
		return -1
	}
	idIdx := colIdx("id")
	labelIdx := colIdx("label")
	priceIdx := colIdx("price")
	activeIdx := colIdx("active")
	noteIdx := colIdx("note")

	row0 := tbl.Rows[0]
	if got := row0[labelIdx]; got != "widget" {
		t.Errorf("row0 label = %v, want widget", got)
	}
	if got, ok := row0[idIdx].(int64); !ok || got != 1 {
		t.Errorf("row0 id = %v (%T), want int64(1)", row0[idIdx], row0[idIdx])
	}
	if got, ok := row0[priceIdx].(float64); !ok || got != 9.99 {
		t.Errorf("row0 price = %v (%T), want float64(9.99)", row0[priceIdx], row0[priceIdx])
	}
	if got, ok := row0[activeIdx].(bool); !ok || got != true {
		t.Errorf("row0 active = %v (%T), want true", row0[activeIdx], row0[activeIdx])
	}
	if row0[noteIdx] != nil {
		t.Errorf("row0 note = %v, want nil", row0[noteIdx])
	}

	row1 := tbl.Rows[1]
	if got := row1[labelIdx]; got != `Bob & Co <"quoted">` {
		t.Errorf("row1 label = %q, want %q", got, `Bob & Co <"quoted">`)
	}
	if got, ok := row1[activeIdx].(bool); !ok || got != false {
		t.Errorf("row1 active = %v (%T), want false", row1[activeIdx], row1[activeIdx])
	}
}
