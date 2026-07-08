package importer

import (
	"context"
	"strings"
	"testing"

	tinysql "github.com/SimonWaldherr/tinySQL"
)

func TestImportCSVWrapper(t *testing.T) {
	ctx := context.Background()
	db := tinysql.NewDB()

	res, err := ImportCSV(ctx, db, "default", "people", strings.NewReader("id,name\n1,Ada\n"), &ImportOptions{
		CreateTable: true,
		HeaderMode:  "first",
	})
	if err != nil {
		t.Fatalf("ImportCSV: %v", err)
	}
	if res.RowsInserted != 1 {
		t.Fatalf("RowsInserted = %d", res.RowsInserted)
	}
}

func TestMapImportWrappers(t *testing.T) {
	ctx := context.Background()
	db := tinysql.NewDB()

	if _, err := ImportGeoJSON(ctx, db, "default", "geo", strings.NewReader(`{"type":"Feature","properties":{"name":"x"},"geometry":{"type":"Point","coordinates":[1,2]}}`), &ImportOptions{CreateTable: true}); err != nil {
		t.Fatalf("ImportGeoJSON: %v", err)
	}
	if _, err := ImportOSM(ctx, db, "default", "osm", strings.NewReader(`<osm><node id="1" lat="1" lon="2"/></osm>`), &ImportOptions{CreateTable: true}); err != nil {
		t.Fatalf("ImportOSM: %v", err)
	}
	if _, err := ImportRoutingGraph(ctx, db, "default", "rg", strings.NewReader(`[{"source":"a","target":"b","cost":1}]`), &ImportOptions{CreateTable: true}); err != nil {
		t.Fatalf("ImportRoutingGraph: %v", err)
	}

	var _ = ImportShapefileZip
	var _ = ImportMBTilesReader
}
