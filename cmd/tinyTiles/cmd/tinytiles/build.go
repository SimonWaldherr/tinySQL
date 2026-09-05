//go:build sqliteimport && !js && !wasm && !baremetal

package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
)

// commandBuild provides a one-command PBF-to-tinyTiles path without baking a
// map style or OSM feature policy into this repository. The configured
// generator owns PBF interpretation; Karte.Bayern's preprocess binary is the
// first supported generator adapter.
func commandBuild(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("build", flag.ContinueOnError)
	fs.SetOutput(stderr)
	generator := fs.String("generator", "karte-preprocess", "Karte.Bayern-compatible PBF→MBTiles generator executable")
	mbtilesOut := fs.String("mbtiles-out", "", "persist the generated MBTiles at this path")
	replaceMBTiles := fs.Bool("replace-mbtiles", false, "allow replacement of an existing --mbtiles-out file")
	workDir := fs.String("work-dir", "", "parent directory for temporary MBTiles and shards")
	keepWork := fs.Bool("keep-work", false, "keep temporary source MBTiles and generator files for inspection")
	minZoom := fs.Int("minzoom", 5, "minimum tile zoom")
	maxZoom := fs.Int("maxzoom", 14, "maximum tile zoom")
	buildingMinZoom := fs.Int("building-minzoom", 12, "minimum building zoom")
	shards := fs.Int("shards", 256, "temporary generator shard count")
	concurrency := fs.Int("concurrency", 0, "generator decode concurrency; 0 uses its default")
	reduceConcurrency := fs.Int("reduce-concurrency", 0, "generator reduce concurrency; 0 uses its default")
	districts := fs.String("districts", "", "optional district-boundary GeoJSON")
	minLat := fs.Float64("min-lat", 0, "optional geographic filter: southern latitude")
	minLon := fs.Float64("min-lon", 0, "optional geographic filter: western longitude")
	maxLat := fs.Float64("max-lat", 0, "optional geographic filter: northern latitude")
	maxLon := fs.Float64("max-lon", 0, "optional geographic filter: eastern longitude")
	centerLat := fs.Float64("center-lat", 0, "optional radius-filter center latitude")
	centerLon := fs.Float64("center-lon", 0, "optional radius-filter center longitude")
	radiusKM := fs.Float64("radius-km", 0, "optional radius filter in kilometres")
	schema := fs.String("schema", "auto", "artifact schema: auto, flat, normalized")
	batch := fs.Int("batch", 1_000, "rows per bounded artifact import batch")
	memory := fs.Int64("max-memory", 256<<20, "maximum tinySQL cache budget in bytes")
	reserve := fs.Int64("min-free", 1<<30, "required free disk reserve in bytes")
	replace := fs.Bool("replace", false, "atomically replace an existing artifact")
	if fs.Parse(args) != nil || fs.NArg() != 2 {
		fmt.Fprintln(stderr, "usage: tinytiles build [flags] source.osm.pbf[,more.osm.pbf] dataset.ttiles/")
		return 2
	}

	pbfInputs, err := parsePBFInputs(fs.Arg(0))
	if err != nil {
		fmt.Fprintf(stderr, "tinytiles build: %v\n", err)
		return 2
	}
	options := kartePreprocessOptions{
		PBFInputs:         pbfInputs,
		MinZoom:           *minZoom,
		MaxZoom:           *maxZoom,
		BuildingMinZoom:   *buildingMinZoom,
		Shards:            *shards,
		Concurrency:       *concurrency,
		ReduceConcurrency: *reduceConcurrency,
		Districts:         *districts,
		MinLat:            *minLat,
		MinLon:            *minLon,
		MaxLat:            *maxLat,
		MaxLon:            *maxLon,
		CenterLat:         *centerLat,
		CenterLon:         *centerLon,
		RadiusKM:          *radiusKM,
	}
	if err := options.validate(); err != nil {
		fmt.Fprintf(stderr, "tinytiles build: %v\n", err)
		return 2
	}
	if strings.TrimSpace(*districts) != "" {
		if err := requireRegularFile("districts", *districts); err != nil {
			fmt.Fprintf(stderr, "tinytiles build: %v\n", err)
			return 2
		}
	}
	artifactSchema, err := parseSchema(*schema)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	if *batch < 1 || *memory <= 0 || *reserve < 0 {
		fmt.Fprintln(stderr, "tinytiles build: batch and max-memory must be positive; min-free must not be negative")
		return 2
	}

	generatorPath, err := exec.LookPath(strings.TrimSpace(*generator))
	if err != nil {
		fmt.Fprintf(stderr, "tinytiles build: generator %q not found: %v\n", *generator, err)
		return 2
	}
	if *mbtilesOut != "" {
		if err := validatePersistentBuildPaths(*mbtilesOut, fs.Arg(1), pbfInputs, *districts); err != nil {
			fmt.Fprintf(stderr, "tinytiles build: %v\n", err)
			return 2
		}
		if err := checkMBTilesDestination(*mbtilesOut, *replaceMBTiles); err != nil {
			fmt.Fprintf(stderr, "tinytiles build: %v\n", err)
			return 2
		}
	}

	parent := strings.TrimSpace(*workDir)
	if parent == "" {
		parent = filepath.Dir(filepath.Clean(fs.Arg(1)))
	} else if contained, err := pathWithin(parent, fs.Arg(1)); err != nil {
		fmt.Fprintf(stderr, "tinytiles build: validate work directory: %v\n", err)
		return 2
	} else if contained {
		fmt.Fprintln(stderr, "tinytiles build: work-dir must not be inside the artifact directory")
		return 2
	}
	if err := os.MkdirAll(parent, 0o755); err != nil {
		fmt.Fprintf(stderr, "tinytiles build: work parent: %v\n", err)
		return 1
	}
	work, err := os.MkdirTemp(parent, ".tinytiles-build-*")
	if err != nil {
		fmt.Fprintf(stderr, "tinytiles build: work directory: %v\n", err)
		return 1
	}
	if *keepWork {
		fmt.Fprintf(stdout, "work-dir=%s\n", work)
	} else {
		defer func() { _ = os.RemoveAll(work) }()
	}

	mbtiles := *mbtilesOut
	if mbtiles == "" {
		mbtiles = filepath.Join(work, "source.mbtiles")
	}
	options.MBTiles = mbtiles
	options.ShardDir = filepath.Join(work, "shards")
	generatorArgs := options.args()
	provenance, err := options.provenance(generatorPath)
	if err != nil {
		fmt.Fprintf(stderr, "tinytiles build: collect PBF provenance: %v\n", err)
		return 1
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	command := exec.CommandContext(ctx, generatorPath, generatorArgs...)
	command.Stdout, command.Stderr = stdout, stderr
	fmt.Fprintln(stdout, "phase=generate")
	fmt.Fprintf(stdout, "generator=%s\n", generatorPath)
	fmt.Fprintf(stdout, "generator-args=%s\n", quoteArguments(generatorArgs))
	if err := command.Run(); err != nil {
		if ctx.Err() != nil {
			fmt.Fprintf(stderr, "tinytiles build: generation cancelled: %v\n", ctx.Err())
		} else {
			fmt.Fprintf(stderr, "tinytiles build: generator failed: %v\n", err)
		}
		return 1
	}
	if err := requireRegularFile("generated MBTiles", mbtiles); err != nil {
		fmt.Fprintf(stderr, "tinytiles build: %v\n", err)
		return 1
	}

	fmt.Fprintln(stdout, "phase=import")
	result, err := importArtifactWithProvenance(ctx, mbtiles, fs.Arg(1), artifactSchema, *batch, *memory, *reserve, *replace, provenance, stdout)
	if err != nil {
		fmt.Fprintf(stderr, "tinytiles build: import generated MBTiles: %v\n", err)
		return 1
	}
	fmt.Fprintf(stdout, "artifact=%s schema=%s\n", result.ArtifactPath, result.Info.Schema)
	if *mbtilesOut != "" {
		fmt.Fprintf(stdout, "mbtiles=%s\n", mbtiles)
	}
	return 0
}

// kartePreprocessOptions mirrors the documented, stable flags of
// Karte.Bayern cmd/preprocess. Keeping the adapter explicit prevents tinyTiles
// from claiming that all PBF renderers share the same map semantics.
type kartePreprocessOptions struct {
	PBFInputs                                 []string
	MBTiles, ShardDir, Districts              string
	MinZoom, MaxZoom, BuildingMinZoom, Shards int
	Concurrency, ReduceConcurrency            int
	MinLat, MinLon, MaxLat, MaxLon            float64
	CenterLat, CenterLon, RadiusKM            float64
}

func (o kartePreprocessOptions) validate() error {
	if len(o.PBFInputs) == 0 {
		return fmt.Errorf("at least one PBF input is required")
	}
	if o.MinZoom < 0 || o.MaxZoom < o.MinZoom || o.MaxZoom > 22 {
		return fmt.Errorf("invalid zoom range: minzoom=%d maxzoom=%d", o.MinZoom, o.MaxZoom)
	}
	if o.BuildingMinZoom < 0 || o.BuildingMinZoom > 22 {
		return fmt.Errorf("invalid building-minzoom=%d", o.BuildingMinZoom)
	}
	if o.Shards < 1 || o.Shards > 4096 {
		return fmt.Errorf("shards must be between 1 and 4096")
	}
	if o.Concurrency < 0 || o.ReduceConcurrency < 0 {
		return fmt.Errorf("concurrency values must not be negative")
	}
	if math.IsNaN(o.RadiusKM) || math.IsInf(o.RadiusKM, 0) || o.RadiusKM < 0 {
		return fmt.Errorf("radius-km must be a finite value >= 0")
	}
	bboxProvided := o.MinLat != 0 || o.MinLon != 0 || o.MaxLat != 0 || o.MaxLon != 0
	if o.RadiusKM > 0 && bboxProvided {
		return fmt.Errorf("cannot combine radius-km with min-lat/min-lon/max-lat/max-lon")
	}
	if bboxProvided {
		if o.MinLat == 0 || o.MinLon == 0 || o.MaxLat == 0 || o.MaxLon == 0 {
			return fmt.Errorf("geographic bounding box requires min-lat, min-lon, max-lat and max-lon")
		}
		if o.MinLat < -90 || o.MinLat > 90 || o.MaxLat < -90 || o.MaxLat > 90 || o.MinLon < -180 || o.MinLon > 180 || o.MaxLon < -180 || o.MaxLon > 180 {
			return fmt.Errorf("geographic bounding box coordinates are out of range")
		}
		if o.MinLat >= o.MaxLat || o.MinLon >= o.MaxLon {
			return fmt.Errorf("geographic bounding box must satisfy min-lat<max-lat and min-lon<max-lon")
		}
	}
	if o.RadiusKM > 0 && (o.CenterLat < -90 || o.CenterLat > 90 || o.CenterLon < -180 || o.CenterLon > 180) {
		return fmt.Errorf("radius center coordinates are out of range")
	}
	return nil
}

func (o kartePreprocessOptions) args() []string {
	args := []string{
		"-pbf", strings.Join(o.PBFInputs, ","),
		"-out", o.MBTiles,
		"-tmp", o.ShardDir,
		"-minzoom", strconv.Itoa(o.MinZoom),
		"-maxzoom", strconv.Itoa(o.MaxZoom),
		"-building-minzoom", strconv.Itoa(o.BuildingMinZoom),
		"-shards", strconv.Itoa(o.Shards),
		"-shard-compression=true",
		"-clean",
		"-districts", o.Districts,
	}
	if o.Concurrency > 0 {
		args = append(args, "-concurrency", strconv.Itoa(o.Concurrency))
	}
	if o.ReduceConcurrency > 0 {
		args = append(args, "-reduce-concurrency", strconv.Itoa(o.ReduceConcurrency))
	}
	if o.MinLat != 0 || o.MinLon != 0 || o.MaxLat != 0 || o.MaxLon != 0 {
		args = append(args,
			"-minLat", strconv.FormatFloat(o.MinLat, 'f', -1, 64),
			"-minLon", strconv.FormatFloat(o.MinLon, 'f', -1, 64),
			"-maxLat", strconv.FormatFloat(o.MaxLat, 'f', -1, 64),
			"-maxLon", strconv.FormatFloat(o.MaxLon, 'f', -1, 64),
		)
	}
	if o.RadiusKM > 0 {
		args = append(args,
			"-centerLat", strconv.FormatFloat(o.CenterLat, 'f', -1, 64),
			"-centerLon", strconv.FormatFloat(o.CenterLon, 'f', -1, 64),
			"-radiusKm", strconv.FormatFloat(o.RadiusKM, 'f', -1, 64),
		)
	}
	return args
}

func (o kartePreprocessOptions) provenance(generatorPath string) (map[string]any, error) {
	inputs := make([]map[string]any, 0, len(o.PBFInputs))
	for _, input := range o.PBFInputs {
		info, err := os.Stat(input)
		if err != nil {
			return nil, fmt.Errorf("stat PBF input %q: %w", input, err)
		}
		inputs = append(inputs, map[string]any{
			"name":  filepath.Base(input),
			"bytes": info.Size(),
		})
	}
	config := map[string]any{
		"minzoom":           o.MinZoom,
		"maxzoom":           o.MaxZoom,
		"building_minzoom":  o.BuildingMinZoom,
		"shards":            o.Shards,
		"shard_compression": true,
	}
	if o.Concurrency > 0 {
		config["concurrency"] = o.Concurrency
	}
	if o.ReduceConcurrency > 0 {
		config["reduce_concurrency"] = o.ReduceConcurrency
	}
	if strings.TrimSpace(o.Districts) != "" {
		config["districts"] = filepath.Base(o.Districts)
	}
	if o.MinLat != 0 || o.MinLon != 0 || o.MaxLat != 0 || o.MaxLon != 0 {
		config["bbox"] = []float64{o.MinLat, o.MinLon, o.MaxLat, o.MaxLon}
	}
	if o.RadiusKM > 0 {
		config["radius"] = map[string]any{"center_lat": o.CenterLat, "center_lon": o.CenterLon, "km": o.RadiusKM}
	}
	return map[string]any{
		"kind":       "osm-pbf",
		"pbf_inputs": inputs,
		"generator": map[string]any{
			"adapter":    "karte-bayern-preprocess",
			"executable": filepath.Base(generatorPath),
		},
		"generator_config": config,
	}, nil
}

func parsePBFInputs(raw string) ([]string, error) {
	var inputs []string
	seen := make(map[string]struct{})
	for _, input := range strings.Split(raw, ",") {
		input = strings.TrimSpace(input)
		if input == "" {
			continue
		}
		if err := requireRegularFile("PBF input", input); err != nil {
			return nil, err
		}
		absolute, err := filepath.Abs(input)
		if err != nil {
			return nil, fmt.Errorf("PBF input %q: %w", input, err)
		}
		absolute = filepath.Clean(absolute)
		if _, duplicate := seen[absolute]; duplicate {
			continue
		}
		seen[absolute] = struct{}{}
		inputs = append(inputs, input)
	}
	if len(inputs) == 0 {
		return nil, fmt.Errorf("at least one PBF input is required")
	}
	return inputs, nil
}

func requireRegularFile(label, path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("%s %q: %w", label, path, err)
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("%s %q: expected a regular file", label, path)
	}
	if info.Size() == 0 {
		return fmt.Errorf("%s %q: file is empty", label, path)
	}
	return nil
}

func checkMBTilesDestination(path string, replace bool) error {
	info, err := os.Stat(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("MBTiles output %q: %w", path, err)
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("MBTiles output %q: expected a regular file or a new path", path)
	}
	if !replace {
		return fmt.Errorf("MBTiles output %q already exists; pass --replace-mbtiles to replace it", path)
	}
	return nil
}

func validatePersistentBuildPaths(mbtiles, artifact string, pbfInputs []string, districts string) error {
	for _, input := range pbfInputs {
		same, err := samePath(mbtiles, input)
		if err != nil {
			return err
		}
		if same {
			return fmt.Errorf("MBTiles output %q must not replace PBF input %q", mbtiles, input)
		}
	}
	if strings.TrimSpace(districts) != "" {
		same, err := samePath(mbtiles, districts)
		if err != nil {
			return err
		}
		if same {
			return fmt.Errorf("MBTiles output %q must not replace district input %q", mbtiles, districts)
		}
	}
	if within, err := pathWithin(mbtiles, artifact); err != nil {
		return err
	} else if within {
		return fmt.Errorf("MBTiles output %q must not be the artifact path or be inside it", mbtiles)
	}
	if within, err := pathWithin(artifact, mbtiles); err != nil {
		return err
	} else if within {
		return fmt.Errorf("artifact path %q must not be inside MBTiles output %q", artifact, mbtiles)
	}
	return nil
}

func samePath(a, b string) (bool, error) {
	aAbs, err := canonicalExistingPath(a)
	if err != nil {
		return false, err
	}
	bAbs, err := canonicalExistingPath(b)
	if err != nil {
		return false, err
	}
	return aAbs == bAbs, nil
}

// canonicalExistingPath resolves symlinks when the path exists. That prevents
// --mbtiles-out from accidentally naming the same on-disk file as a PBF input
// through a different symlink spelling. For a not-yet-created destination the
// cleaned absolute path is the strongest portable comparison available.
func canonicalExistingPath(path string) (string, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	abs = filepath.Clean(abs)
	resolved, err := filepath.EvalSymlinks(abs)
	if err == nil {
		return filepath.Clean(resolved), nil
	}
	if os.IsNotExist(err) {
		return abs, nil
	}
	return "", err
}

// pathWithin reports whether child is parent itself or is nested inside it.
// It deliberately works on cleaned absolute paths: output destinations often
// do not exist yet, so EvalSymlinks cannot provide a reliable check here.
func pathWithin(child, parent string) (bool, error) {
	childAbs, err := filepath.Abs(child)
	if err != nil {
		return false, err
	}
	parentAbs, err := filepath.Abs(parent)
	if err != nil {
		return false, err
	}
	relative, err := filepath.Rel(filepath.Clean(parentAbs), filepath.Clean(childAbs))
	if err != nil {
		return false, err
	}
	return relative == "." || (relative != ".." && !strings.HasPrefix(relative, ".."+string(os.PathSeparator))), nil
}

func quoteArguments(args []string) string {
	quoted := make([]string, len(args))
	for i, arg := range args {
		quoted[i] = strconv.Quote(arg)
	}
	return strings.Join(quoted, " ")
}
