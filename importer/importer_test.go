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

func TestImportJSONNestedValuesAndPrimaryKey(t *testing.T) {
	ctx := context.Background()
	db := tinysql.NewDB()
	res, err := ImportJSON(ctx, db, "default", "events", strings.NewReader(`[
		{"id": 1, "meta": {"source":"api"}, "tags": ["new","hot"]}
	]`), &ImportOptions{CreateTable: true, PrimaryKey: "id"})
	if err != nil {
		t.Fatalf("ImportJSON: %v", err)
	}
	if res.RowsInserted != 1 || len(res.ColumnTypes) != 3 {
		t.Fatalf("result = %#v", res)
	}
	for i, column := range res.ColumnNames {
		if column == "meta" || column == "tags" {
			if res.ColumnTypes[i] != tinysql.JsonType {
				t.Fatalf("%s type = %v, want JSON", column, res.ColumnTypes[i])
			}
		}
	}
	stmt, err := tinysql.ParseSQL(`INSERT INTO events (id) VALUES (1)`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tinysql.Execute(ctx, db, "default", stmt); err == nil {
		t.Fatal("duplicate primary key insert succeeded")
	}
}
