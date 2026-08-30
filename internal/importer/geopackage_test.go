//go:build sqliteimport && !js && !wasm && !baremetal

package importer

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	_ "modernc.org/sqlite"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

type gpkgFixtureLayer struct {
	name string
	srid int
	x, y float64
}

func createGeoPackageFixture(t *testing.T, layers ...gpkgFixtureLayer) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "fixture.gpkg")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	statements := []string{
		`PRAGMA application_id = 1196444487`,
		`PRAGMA user_version = 10400`,
		`CREATE TABLE gpkg_spatial_ref_sys (
			srs_name TEXT NOT NULL, srs_id INTEGER NOT NULL PRIMARY KEY,
			organization TEXT NOT NULL, organization_coordsys_id INTEGER NOT NULL,
			definition TEXT NOT NULL, description TEXT)`,
		`CREATE TABLE gpkg_contents (
			table_name TEXT NOT NULL PRIMARY KEY, data_type TEXT NOT NULL,
			identifier TEXT UNIQUE, description TEXT DEFAULT '',
			last_change DATETIME NOT NULL DEFAULT '2026-01-02T03:04:05.000Z',
			min_x DOUBLE, min_y DOUBLE, max_x DOUBLE, max_y DOUBLE, srs_id INTEGER)`,
		`CREATE TABLE gpkg_geometry_columns (
			table_name TEXT NOT NULL, column_name TEXT NOT NULL,
			geometry_type_name TEXT NOT NULL, srs_id INTEGER NOT NULL,
			z TINYINT NOT NULL, m TINYINT NOT NULL,
			PRIMARY KEY (table_name, column_name))`,
		`INSERT INTO gpkg_spatial_ref_sys VALUES
			('WGS 84 geodetic', 4326, 'EPSG', 4326, 'GEOGCRS["WGS 84"]', ''),
			('ETRS89 / UTM zone 32N', 25832, 'EPSG', 25832, 'PROJCRS["ETRS89 / UTM zone 32N"]', '')`,
	}
	for _, statement := range statements {
		if _, err := db.Exec(statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	for _, layer := range layers {
		quoted := quoteSQLiteIdentifier(layer.name)
		if _, err := db.Exec(`CREATE TABLE ` + quoted + ` (fid INTEGER PRIMARY KEY, name TEXT, active BOOLEAN, geom POINT)`); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`INSERT INTO gpkg_contents
			(table_name,data_type,identifier,description,min_x,min_y,max_x,max_y,srs_id)
			VALUES (?,?,?,?,?,?,?,?,?)`, layer.name, "features", strings.ToUpper(layer.name), "fixture layer", layer.x, layer.y, layer.x, layer.y, layer.srid); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`INSERT INTO gpkg_geometry_columns VALUES (?, 'geom', 'POINT', ?, 0, 0)`, layer.name, layer.srid); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`INSERT INTO `+quoted+` (fid,name,active,geom) VALUES (1,'alpha',1,?)`, fixtureGeoPackagePoint(int32(layer.srid), layer.x, layer.y)); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

func fixtureGeoPackagePoint(srid int32, x, y float64) []byte {
	body := make([]byte, 40+21)
	copy(body, []byte{'G', 'P', 0, 0x03})
	binary.LittleEndian.PutUint32(body[4:], uint32(srid))
	for i, value := range []float64{x, x, y, y} {
		binary.LittleEndian.PutUint64(body[8+i*8:], math.Float64bits(value))
	}
	body[40] = 1
	binary.LittleEndian.PutUint32(body[41:], 1)
	binary.LittleEndian.PutUint64(body[45:], math.Float64bits(x))
	binary.LittleEndian.PutUint64(body[53:], math.Float64bits(y))
	return body
}

func TestInspectAndImportGeoPackageWGS84(t *testing.T) {
	ctx := context.Background()
	path := createGeoPackageFixture(t, gpkgFixtureLayer{name: "places", srid: 4326, x: 11.5, y: 48.25})
	info, err := InspectGeoPackage(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if info.ApplicationID != geoPackageApplicationID || info.UserVersion != 10400 || info.Version != "1.4.0" || len(info.Layers) != 1 {
		t.Fatalf("info = %#v", info)
	}
	layer := info.Layers[0]
	if layer.TableName != "places" || layer.GeometryColumn != "geom" || layer.GeometryType != "POINT" ||
		layer.SRID != 4326 || layer.SRSOrganization != "EPSG" || layer.MinX == nil || *layer.MinX != 11.5 {
		t.Fatalf("layer = %#v", layer)
	}

	target := storage.NewDB()
	result, err := ImportGeoPackage(ctx, target, "default", "imported_places", path, &ImportOptions{CreateTable: true})
	if err != nil {
		t.Fatal(err)
	}
	if result.RowsInserted != 1 || result.RowsSkipped != 0 || len(result.ColumnNames) != 4 || result.ColumnNames[3] != "geom" || result.ColumnTypes[3] != storage.GeometryType {
		t.Fatalf("result = %#v", result)
	}
	table, err := target.Get("default", "imported_places")
	if err != nil {
		t.Fatal(err)
	}
	if table.Rows[0][0] != int64(1) || table.Rows[0][1] != "alpha" || table.Rows[0][2] != true {
		t.Fatalf("attributes = %#v", table.Rows[0])
	}
	var geometry map[string]any
	if err := json.Unmarshal(table.Rows[0][3].(json.RawMessage), &geometry); err != nil {
		t.Fatal(err)
	}
	coordinates := geometry["coordinates"].([]any)
	if coordinates[0] != 11.5 || coordinates[1] != 48.25 {
		t.Fatalf("geometry = %#v", geometry)
	}
	metadata, err := target.Get("default", "imported_places_metadata")
	if err != nil {
		t.Fatal(err)
	}
	values := map[string]any{}
	for _, row := range metadata.Rows {
		values[row[0].(string)] = row[1]
	}
	if values["format"] != "OGC GeoPackage" || values["geometry_encoding"] != "geojson" || values["srs_id"] != "4326" {
		t.Fatalf("metadata = %#v", values)
	}
}

func TestImportGeoPackageProjectedGeometryIsNotRelabelled(t *testing.T) {
	ctx := context.Background()
	body := fixtureGeoPackagePoint(25832, 691875, 5335575)
	path := createGeoPackageFixture(t, gpkgFixtureLayer{name: "buildings", srid: 25832, x: 691875, y: 5335575})
	target := storage.NewDB()
	result, err := ImportGeoPackage(ctx, target, "default", "buildings", path, &ImportOptions{CreateTable: true})
	if err != nil {
		t.Fatal(err)
	}
	if result.ColumnTypes[3] != storage.BlobType {
		t.Fatalf("projected geometry type = %v, want BLOB", result.ColumnTypes[3])
	}
	table, _ := target.Get("default", "buildings")
	if got := table.Rows[0][3].([]byte); !bytes.Equal(got, body) {
		t.Fatalf("native GeoPackageBinary changed: %x != %x", got, body)
	}

	if _, err := ImportGeoPackage(ctx, storage.NewDB(), "default", "bad", path, &ImportOptions{CreateTable: true, GeoPackageGeometryMode: "geojson"}); err == nil {
		t.Fatal("projected GeoPackage should require explicit reprojection before GeoJSON")
	}

	wkbTarget := storage.NewDB()
	if _, err := ImportGeoPackage(ctx, wkbTarget, "default", "wkb", path, &ImportOptions{CreateTable: true, GeoPackageGeometryMode: "wkb"}); err != nil {
		t.Fatal(err)
	}
	wkbTable, _ := wkbTarget.Get("default", "wkb")
	if got := wkbTable.Rows[0][3].([]byte); !bytes.Equal(got, body[40:]) {
		t.Fatalf("WKB mode = %x, want %x", got, body[40:])
	}
}

func TestImportGeoPackageRequiresLayerChoice(t *testing.T) {
	ctx := context.Background()
	path := createGeoPackageFixture(t,
		gpkgFixtureLayer{name: "roads", srid: 4326, x: 11, y: 48},
		gpkgFixtureLayer{name: "water", srid: 4326, x: 12, y: 49},
	)
	if _, err := ImportGeoPackage(ctx, storage.NewDB(), "default", "all", path, &ImportOptions{CreateTable: true}); err == nil || !strings.Contains(err.Error(), "GeoPackageLayer") {
		t.Fatalf("multi-layer import error = %v", err)
	}
	target := storage.NewDB()
	if _, err := ImportGeoPackage(ctx, target, "default", "selected", path, &ImportOptions{CreateTable: true, GeoPackageLayer: "water"}); err != nil {
		t.Fatal(err)
	}
	table, _ := target.Get("default", "selected")
	var geometry map[string]any
	if err := json.Unmarshal(table.Rows[0][3].(json.RawMessage), &geometry); err != nil {
		t.Fatal(err)
	}
	if geometry["coordinates"].([]any)[0] != 12.0 {
		t.Fatalf("selected wrong layer: %#v", geometry)
	}
}

func TestImportGeoPackageReaderAndFileDetection(t *testing.T) {
	ctx := context.Background()
	path := createGeoPackageFixture(t, gpkgFixtureLayer{name: "places", srid: 4326, x: 11.5, y: 48.25})
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ImportGeoPackageReader(ctx, storage.NewDB(), "default", "from_reader", bytes.NewReader(body), &ImportOptions{CreateTable: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := ImportFile(ctx, storage.NewDB(), "default", "from_file", path, &ImportOptions{CreateTable: true}); err != nil {
		t.Fatal(err)
	}
}

func TestInspectGeoPackagePrefersCRSWKT2Extension(t *testing.T) {
	ctx := context.Background()
	path := createGeoPackageFixture(t, gpkgFixtureLayer{name: "places", srid: 4326, x: 11.5, y: 48.25})
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`ALTER TABLE gpkg_spatial_ref_sys ADD COLUMN definition_12_063 TEXT`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE gpkg_spatial_ref_sys SET definition_12_063 = 'GEOGCRS["WGS 84",CS[ellipsoidal,2]]' WHERE srs_id = 4326`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	info, err := InspectGeoPackage(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Layers[0].SRSDefinition; got != `GEOGCRS["WGS 84",CS[ellipsoidal,2]]` {
		t.Fatalf("SRS definition = %q, want WKT2 extension value", got)
	}
}

func TestInspectGeoPackageAcceptsLegacyStandardIDs(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		id      int64
		version string
	}{{geoPackage10ID, "1.0"}, {geoPackage11ID, "1.1"}} {
		path := createGeoPackageFixture(t, gpkgFixtureLayer{name: "places", srid: 4326, x: 11.5, y: 48.25})
		db, err := sql.Open("sqlite", path)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`PRAGMA application_id = ` + strconv.FormatInt(tc.id, 10)); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`PRAGMA user_version = 0`); err != nil {
			t.Fatal(err)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		info, err := InspectGeoPackage(ctx, path)
		if err != nil {
			t.Fatal(err)
		}
		if info.Version != tc.version {
			t.Fatalf("legacy id 0x%x version = %q, want %q", tc.id, info.Version, tc.version)
		}
	}
}
