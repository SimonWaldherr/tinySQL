//go:build sqliteimport && !js && !wasm && !baremetal

package main

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	tiles "github.com/SimonWaldherr/tinySQL/tiles"
)

const fakePreprocessEnv = "TINYTILES_FAKE_PREPROCESS"

// The build command starts a separate executable. Re-executing this test
// binary gives the integration test a portable fake Karte preprocessor without
// depending on sqlite3, a shell, or a Karte.Bayern checkout at test time.
func init() {
	if os.Getenv(fakePreprocessEnv) != "1" {
		return
	}
	if err := fakePreprocess(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	os.Exit(0)
}

func fakePreprocess(args []string) error {
	var out string
	for i := 0; i+1 < len(args); i++ {
		if args[i] == "-out" {
			out = args[i+1]
			break
		}
	}
	if out == "" {
		return fmt.Errorf("fake preprocess: missing -out")
	}
	if err := os.MkdirAll(filepath.Dir(out), 0o755); err != nil {
		return err
	}
	db, err := sql.Open("sqlite", out)
	if err != nil {
		return err
	}
	defer db.Close()
	_, err = db.Exec(`
		CREATE TABLE metadata (name TEXT, value TEXT);
		CREATE TABLE map (zoom_level INTEGER, tile_column INTEGER, tile_row INTEGER, tile_id TEXT);
		CREATE TABLE images (tile_id TEXT PRIMARY KEY, tile_data BLOB);
		INSERT INTO metadata(name, value) VALUES ('format', 'pbf'), ('name', 'fake PBF output');
		INSERT INTO images(tile_id, tile_data) VALUES ('tile-1', X'010203');
		INSERT INTO map(zoom_level, tile_column, tile_row, tile_id) VALUES (8, 137, 167, 'tile-1');
	`)
	return err
}

func TestKartePreprocessOptionsArgs(t *testing.T) {
	options := kartePreprocessOptions{
		PBFInputs:         []string{"first.osm.pbf", "second.osm.pbf"},
		MBTiles:           "/tmp/source.mbtiles",
		ShardDir:          "/tmp/shards",
		Districts:         "/tmp/districts.geojson",
		MinZoom:           5,
		MaxZoom:           14,
		BuildingMinZoom:   12,
		Shards:            64,
		Concurrency:       3,
		ReduceConcurrency: 2,
		MinLat:            47.1,
		MinLon:            8.2,
		MaxLat:            49.3,
		MaxLon:            13.4,
		CenterLat:         48.1,
		CenterLon:         11.2,
		RadiusKM:          100,
	}
	want := []string{
		"-pbf", "first.osm.pbf,second.osm.pbf",
		"-out", "/tmp/source.mbtiles",
		"-tmp", "/tmp/shards",
		"-minzoom", "5",
		"-maxzoom", "14",
		"-building-minzoom", "12",
		"-shards", "64",
		"-shard-compression=true",
		"-clean",
		"-districts", "/tmp/districts.geojson",
		"-concurrency", "3",
		"-reduce-concurrency", "2",
		"-minLat", "47.1",
		"-minLon", "8.2",
		"-maxLat", "49.3",
		"-maxLon", "13.4",
		"-centerLat", "48.1",
		"-centerLon", "11.2",
		"-radiusKm", "100",
	}
	if got := options.args(); !reflect.DeepEqual(got, want) {
		t.Fatalf("generator args:\n got: %#v\nwant: %#v", got, want)
	}
}

func TestKartePreprocessOptionsValidate(t *testing.T) {
	valid := kartePreprocessOptions{PBFInputs: []string{"source.osm.pbf"}, MinZoom: 5, MaxZoom: 14, BuildingMinZoom: 12, Shards: 1}
	if err := valid.validate(); err != nil {
		t.Fatalf("valid options rejected: %v", err)
	}
	cases := []kartePreprocessOptions{
		{PBFInputs: []string{"source.osm.pbf"}, MinZoom: 14, MaxZoom: 5, BuildingMinZoom: 12, Shards: 1},
		{PBFInputs: []string{"source.osm.pbf"}, MinZoom: 5, MaxZoom: 14, BuildingMinZoom: 23, Shards: 1},
		{PBFInputs: []string{"source.osm.pbf"}, MinZoom: 5, MaxZoom: 14, BuildingMinZoom: 12, Shards: 0},
		{PBFInputs: []string{"source.osm.pbf"}, MinZoom: 5, MaxZoom: 14, BuildingMinZoom: 12, Shards: 1, RadiusKM: 10, MinLat: 47, MinLon: 8, MaxLat: 49, MaxLon: 13},
	}
	for _, options := range cases {
		if err := options.validate(); err == nil {
			t.Fatalf("invalid options accepted: %+v", options)
		}
	}
}

func TestParsePBFInputs(t *testing.T) {
	temp := t.TempDir()
	pbf := filepath.Join(temp, "region.osm.pbf")
	if err := os.WriteFile(pbf, []byte("not-a-real-pbf-but-nonempty"), 0o644); err != nil {
		t.Fatal(err)
	}
	inputs, err := parsePBFInputs(pbf + ", " + pbf)
	if err != nil {
		t.Fatalf("parsePBFInputs: %v", err)
	}
	if !reflect.DeepEqual(inputs, []string{pbf}) {
		t.Fatalf("inputs=%q, want deduplicated %q", inputs, []string{pbf})
	}
	if _, err := parsePBFInputs(temp); err == nil {
		t.Fatal("directory accepted as PBF input")
	}
	empty := filepath.Join(temp, "empty.osm.pbf")
	if err := os.WriteFile(empty, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := parsePBFInputs(empty); err == nil {
		t.Fatal("empty PBF input accepted")
	}
}

func TestBuildPathSafety(t *testing.T) {
	root := t.TempDir()
	pbf := filepath.Join(root, "region.osm.pbf")
	artifact := filepath.Join(root, "region.ttiles")
	if err := validatePersistentBuildPaths(pbf, artifact, []string{pbf}, ""); err == nil {
		t.Fatal("PBF replacement accepted")
	}
	if err := validatePersistentBuildPaths(filepath.Join(artifact, "source.mbtiles"), artifact, []string{pbf}, ""); err == nil {
		t.Fatal("MBTiles inside artifact accepted")
	}
	if inside, err := pathWithin(filepath.Join(artifact, "work"), artifact); err != nil || !inside {
		t.Fatalf("pathWithin inside=%t err=%v, want true nil", inside, err)
	}
	if inside, err := pathWithin(filepath.Join(root, "elsewhere"), artifact); err != nil || inside {
		t.Fatalf("pathWithin outside=%t err=%v, want false nil", inside, err)
	}
	alias := filepath.Join(root, "source-alias.osm.pbf")
	if err := os.WriteFile(pbf, []byte("pbf"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(pbf, alias); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	if err := validatePersistentBuildPaths(pbf, artifact, []string{alias}, ""); err == nil {
		t.Fatal("symlinked PBF replacement accepted")
	}
}

func TestCommandBuildRejectsMissingGenerator(t *testing.T) {
	temp := t.TempDir()
	pbf := filepath.Join(temp, "region.osm.pbf")
	if err := os.WriteFile(pbf, []byte("pbf"), 0o644); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	code := run([]string{"build", "--generator", filepath.Join(temp, "missing-generator"), pbf, filepath.Join(temp, "region.ttiles")}, &stdout, &stderr)
	if code != 2 || !strings.Contains(stderr.String(), "not found") {
		t.Fatalf("code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
}

func TestCommandBuildEndToEnd(t *testing.T) {
	temp := t.TempDir()
	pbf := filepath.Join(temp, "region.osm.pbf")
	if err := os.WriteFile(pbf, []byte("pbf"), 0o644); err != nil {
		t.Fatal(err)
	}
	mbtiles := filepath.Join(temp, "region.mbtiles")
	artifact := filepath.Join(temp, "region.ttiles")
	t.Setenv(fakePreprocessEnv, "1")
	var stdout, stderr bytes.Buffer
	code := run([]string{
		"build",
		"--generator", os.Args[0],
		"--mbtiles-out", mbtiles,
		"--shards", "1",
		"--concurrency", "1",
		pbf,
		artifact,
	}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("build code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	if !strings.Contains(stdout.String(), "phase=generate") || !strings.Contains(stdout.String(), "phase=import") {
		t.Fatalf("missing build phases in stdout=%q", stdout.String())
	}
	manifest, err := tiles.ValidateArtifact(context.Background(), artifact)
	if err != nil {
		t.Fatalf("validate artifact: %v", err)
	}
	if manifest.Schema != tiles.SchemaNormalized {
		t.Fatalf("schema=%q, want normalized", manifest.Schema)
	}
	if manifest.Provenance["kind"] != "osm-pbf" {
		t.Fatalf("PBF provenance missing from manifest: %#v", manifest.Provenance)
	}
	config, ok := manifest.Provenance["generator_config"].(map[string]any)
	if !ok || config["minzoom"] != float64(5) || config["maxzoom"] != float64(14) {
		t.Fatalf("generator config provenance=%#v", manifest.Provenance["generator_config"])
	}
	reader, err := tiles.OpenArtifact(context.Background(), artifact, tiles.OpenOptions{MaxMemoryBytes: 1 << 20})
	if err != nil {
		t.Fatalf("open artifact reader: %v", err)
	}
	defer reader.Close()
	tile, found, err := reader.Lookup(context.Background(), tiles.Key{Z: 8, X: 137, Y: 167})
	if err != nil || !found || !bytes.Equal(tile.Data, []byte{1, 2, 3}) {
		t.Fatalf("tile found=%t data=%x err=%v", found, tile.Data, err)
	}
}

func TestQuoteArguments(t *testing.T) {
	if got, want := quoteArguments([]string{"-pbf", "a file.osm.pbf", "-districts", ""}), `"-pbf" "a file.osm.pbf" "-districts" ""`; got != want {
		t.Fatalf("quoteArguments=%q, want %q", got, want)
	}
}
