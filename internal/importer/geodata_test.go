package importer

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestImportGeoJSONCollectsPropertiesAcrossFeatures(t *testing.T) {
	ctx := context.Background()
	db := storage.NewDB()
	geo := `{"type":"FeatureCollection","features":[
		{"type":"Feature","properties":{"name":"A"},"geometry":{"type":"Point","coordinates":[1,2]}},
		{"type":"Feature","properties":{"population":42},"geometry":{"type":"LineString","coordinates":[[1,2],[3,4]]}}
	]}`

	res, err := ImportGeoJSON(ctx, db, "default", "geo_props", strings.NewReader(geo), &ImportOptions{CreateTable: true, TypeInference: true})
	if err != nil {
		t.Fatalf("ImportGeoJSON: %v", err)
	}

	wantCols := []string{"name", "population", "geometry_type", "geometry"}
	if got := strings.Join(res.ColumnNames, ","); got != strings.Join(wantCols, ",") {
		t.Fatalf("columns = %v, want %v", res.ColumnNames, wantCols)
	}

	tbl, err := db.Get("default", "geo_props")
	if err != nil {
		t.Fatalf("get table: %v", err)
	}
	if len(tbl.Rows) != 2 {
		t.Fatalf("rows = %d, want 2", len(tbl.Rows))
	}
	if tbl.Rows[1][2] != "LineString" {
		t.Fatalf("geometry_type = %v, want LineString", tbl.Rows[1][2])
	}
	if _, ok := tbl.Rows[1][3].(json.RawMessage); !ok {
		t.Fatalf("geometry = %T, want json.RawMessage", tbl.Rows[1][3])
	}
}

func TestImportOSMXML(t *testing.T) {
	ctx := context.Background()
	db := storage.NewDB()
	osm := `<osm version="0.6">
		<node id="1" lat="52.1" lon="13.1"><tag k="name" v="A"/></node>
		<node id="2" lat="52.2" lon="13.2"/>
		<way id="10"><nd ref="1"/><nd ref="2"/><tag k="highway" v="residential"/></way>
		<relation id="20"><member type="way" ref="10" role="outer"/><tag k="type" v="multipolygon"/></relation>
	</osm>`

	res, err := ImportOSM(ctx, db, "default", "osm_data", strings.NewReader(osm), &ImportOptions{CreateTable: true})
	if err != nil {
		t.Fatalf("ImportOSM: %v", err)
	}
	if res.RowsInserted != 4 {
		t.Fatalf("RowsInserted = %d, want 4", res.RowsInserted)
	}

	tbl, err := db.Get("default", "osm_data")
	if err != nil {
		t.Fatalf("get table: %v", err)
	}
	if tbl.Rows[0][0] != "node" || tbl.Rows[2][0] != "way" || tbl.Rows[3][0] != "relation" {
		t.Fatalf("unexpected osm row types: %#v", tbl.Rows)
	}
	if tbl.Rows[2][11] != "LineString" {
		t.Fatalf("way geometry_type = %v, want LineString", tbl.Rows[2][11])
	}
}

func TestImportRoutingGraphJSON(t *testing.T) {
	ctx := context.Background()
	db := storage.NewDB()
	graph := `{
		"nodes":[{"id":"a","lat":52.1,"lon":13.1},{"id":"b","lat":52.2,"lon":13.2}],
		"edges":[{"id":"e1","source":"a","target":"b","cost":7.5,"mode":"car","geometry":{"type":"LineString","coordinates":[[13.1,52.1],[13.2,52.2]]}}]
	}`

	res, err := ImportRoutingGraph(ctx, db, "default", "rg", strings.NewReader(graph), &ImportOptions{CreateTable: true})
	if err != nil {
		t.Fatalf("ImportRoutingGraph: %v", err)
	}
	if res.RowsInserted != 1 {
		t.Fatalf("edge rows = %d, want 1", res.RowsInserted)
	}

	nodes, err := db.Get("default", "rg_nodes")
	if err != nil {
		t.Fatalf("get nodes table: %v", err)
	}
	if len(nodes.Rows) != 2 {
		t.Fatalf("node rows = %d, want 2", len(nodes.Rows))
	}
	edges, err := db.Get("default", "rg")
	if err != nil {
		t.Fatalf("get edges table: %v", err)
	}
	if edges.Rows[0][8] != "LineString" {
		t.Fatalf("edge geometry_type = %v, want LineString", edges.Rows[0][8])
	}
}

func TestImportRoutingGraphCSV(t *testing.T) {
	ctx := context.Background()
	db := storage.NewDB()
	csvData := "source,target,cost,distance,mode\nn1,n2,3.5,12,bike\n"

	res, err := ImportRoutingGraph(ctx, db, "default", "rg_csv", strings.NewReader(csvData), &ImportOptions{CreateTable: true})
	if err != nil {
		t.Fatalf("ImportRoutingGraph CSV: %v", err)
	}
	if res.RowsInserted != 1 {
		t.Fatalf("RowsInserted = %d, want 1", res.RowsInserted)
	}
}

func TestImportRoutingGraphNDJSON(t *testing.T) {
	ctx := context.Background()
	db := storage.NewDB()
	graph := `{"type":"node","id":"a","lat":52.1,"lon":13.1}
{"type":"edge","id":"e1","source":"a","target":"b","cost":4.5,"geometry":{"type":"LineString","coordinates":[[13.1,52.1],[13.2,52.2]]}}
{"node":{"id":"b","lat":52.2,"lon":13.2}}`

	res, err := ImportRoutingGraph(ctx, db, "default", "rg_ndjson", strings.NewReader(graph), &ImportOptions{CreateTable: true})
	if err != nil {
		t.Fatalf("ImportRoutingGraph NDJSON: %v", err)
	}
	if res.RowsInserted != 1 {
		t.Fatalf("edge rows = %d, want 1", res.RowsInserted)
	}
	nodes, err := db.Get("default", "rg_ndjson_nodes")
	if err != nil {
		t.Fatalf("get ndjson nodes: %v", err)
	}
	if len(nodes.Rows) != 2 {
		t.Fatalf("node rows = %d, want 2", len(nodes.Rows))
	}
}

func TestImportKMLExtendedDataAndMultiGeometry(t *testing.T) {
	ctx := context.Background()
	db := storage.NewDB()
	kml := `<kml><Document><Folder><Placemark>
		<name>route</name>
		<styleUrl>#main</styleUrl>
		<ExtendedData>
			<Data name="category"><value>trail</value></Data>
			<SchemaData schemaUrl="#schema"><SimpleData name="speed">12.5</SimpleData></SchemaData>
		</ExtendedData>
		<MultiGeometry>
			<Point><coordinates>11.1,48.1,500</coordinates></Point>
			<LineString><coordinates>11.1,48.1,500 11.2,48.2,510</coordinates></LineString>
		</MultiGeometry>
	</Placemark></Folder></Document></kml>`

	res, err := ImportKML(ctx, db, "default", "kml_ext", strings.NewReader(kml), &ImportOptions{CreateTable: true, TypeInference: true})
	if err != nil {
		t.Fatalf("ImportKML extended: %v", err)
	}
	if res.RowsInserted != 1 {
		t.Fatalf("RowsInserted = %d, want 1", res.RowsInserted)
	}
	tbl, err := db.Get("default", "kml_ext")
	if err != nil {
		t.Fatalf("get kml_ext: %v", err)
	}
	col := make(map[string]int, len(tbl.Cols))
	for i, c := range tbl.Cols {
		col[c.Name] = i
	}
	if tbl.Rows[0][col["category"]] != "trail" || tbl.Rows[0][col["styleUrl"]] != "#main" {
		t.Fatalf("unexpected KML properties: cols=%#v rows=%#v", tbl.Cols, tbl.Rows)
	}
	if tbl.Rows[0][col["geometry_type"]] != "GeometryCollection" {
		t.Fatalf("geometry_type = %v, want GeometryCollection", tbl.Rows[0][col["geometry_type"]])
	}
	var geom map[string]any
	if err := json.Unmarshal(tbl.Rows[0][col["geometry"]].(json.RawMessage), &geom); err != nil {
		t.Fatalf("decode geometry: %v", err)
	}
	geoms := geom["geometries"].([]any)
	point := geoms[0].(map[string]any)
	coords := point["coordinates"].([]any)
	if len(coords) != 3 || coords[2].(float64) != 500 {
		t.Fatalf("point coordinates = %#v, want altitude", coords)
	}
}

func TestImportFileMapExtensions(t *testing.T) {
	ctx := context.Background()
	db := storage.NewDB()
	dir := t.TempDir()

	osmFile := filepath.Join(dir, "sample.osm.xml")
	if err := os.WriteFile(osmFile, []byte(`<osm><node id="1" lat="1" lon="2"/></osm>`), 0o644); err != nil {
		t.Fatalf("write osm: %v", err)
	}
	if _, err := ImportFile(ctx, db, "default", "osm_file", osmFile, &ImportOptions{CreateTable: true}); err != nil {
		t.Fatalf("ImportFile .osm.xml: %v", err)
	}

	rgFile := filepath.Join(dir, "network.rg")
	if err := os.WriteFile(rgFile, []byte(`[{"source":"a","target":"b","cost":1}]`), 0o644); err != nil {
		t.Fatalf("write rg: %v", err)
	}
	if _, err := ImportFile(ctx, db, "default", "rg_file", rgFile, &ImportOptions{CreateTable: true}); err != nil {
		t.Fatalf("ImportFile .rg: %v", err)
	}

	graphJSONFile := filepath.Join(dir, "network.graph.json")
	if err := os.WriteFile(graphJSONFile, []byte(`{"source":"a","target":"b","cost":1}
{"source":"b","target":"c","cost":2}`), 0o644); err != nil {
		t.Fatalf("write graph json: %v", err)
	}
	if _, err := ImportFile(ctx, db, "default", "graph_json_file", graphJSONFile, &ImportOptions{CreateTable: true}); err != nil {
		t.Fatalf("ImportFile .graph.json: %v", err)
	}

	routingAliasFile := filepath.Join(dir, "network.routing_graph")
	if err := os.WriteFile(routingAliasFile, []byte(`[{"source":"a","target":"b","cost":1}]`), 0o644); err != nil {
		t.Fatalf("write routing alias: %v", err)
	}
	if _, err := ImportFile(ctx, db, "default", "routing_alias_file", routingAliasFile, &ImportOptions{CreateTable: true}); err != nil {
		t.Fatalf("ImportFile .routing_graph: %v", err)
	}

	pbfFile := filepath.Join(dir, "planet.osm.pbf")
	if err := os.WriteFile(pbfFile, []byte{0}, 0o644); err != nil {
		t.Fatalf("write pbf: %v", err)
	}
	if _, err := ImportFile(ctx, db, "default", "pbf_file", pbfFile, &ImportOptions{CreateTable: true}); err == nil {
		t.Fatal("expected .osm.pbf to return an unsupported-format error")
	}

}
