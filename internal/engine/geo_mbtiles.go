// MBTILES_TILE, MBTILES_TILES and MBTILES_METADATA let a SQL query read
// directly from an .mbtiles file's own SQLite storage, without the
// GEO_FROM_WKT-style step of first running a separate Go-API import/open
// call (ImportMBTiles, OpenMBTiles, ImportMBTilesArtifact -- all in
// internal/importer, and all still the right tool when the goal is
// *persisting* a tileset into tinySQL rather than reading it in place from
// one query).
//
// This file adds no SQLite-reading code of its own: it is a thin bridge
// from the SQL layer to internal/importer's existing, already-hardened
// mbtiles readers (mbtiles_open.go's OpenMBTiles for the bulk/metadata
// cases, mbtiles_lookup.go's LookupMBTilesTile for the single-tile case).
// Those are gated behind the sqliteimport build tag with a same-signature
// "not supported in this build" stub for every other target (js/wasm/
// baremetal, or sqliteimport simply not requested) -- calling into them
// unconditionally here, with no build tag on this file itself, means these
// three SQL functions degrade the exact same way on those targets: they
// exist and can be called, and return that same clear error, rather than
// being silently absent from the function registry.
package engine

import (
	"context"
	"fmt"
	"math"

	"github.com/SimonWaldherr/tinySQL/internal/importer"
	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// mbtilesTVFMaxTiles bounds MBTILES_TILES the same way every other
// unbounded-by-default read in this codebase is bounded (see
// geoWKTMaxInputBytes, geoDissolveMaxResultBytes): large enough that a
// realistic overview-zoom query or a modest single-layer extract always
// fits, small enough that an unbounded "read the whole planet at z14"
// mistake fails fast with a clear error instead of exhausting memory.
// Rather than silently truncate (indistinguishable from "that's everything"
// in a SELECT result), a query that hits this bound is asked to narrow its
// zoom range instead.
const mbtilesTVFMaxTiles = 200_000

func getMBTilesFunctions() map[string]funcHandler {
	return map[string]funcHandler{
		"MBTILES_TILE": evalMBTilesTile,
	}
}

func evalMBTilesTile(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if err := requireArgs(ex.Name, ex, 4, 4); err != nil {
		return nil, err
	}
	path, err := evalGeoTextArg(env, ex, row, 0)
	if err != nil {
		return nil, err
	}
	z, err := evalMBTilesIntArg(env, ex, row, 1)
	if err != nil {
		return nil, err
	}
	x, err := evalMBTilesIntArg(env, ex, row, 2)
	if err != nil {
		return nil, err
	}
	y, err := evalMBTilesIntArg(env, ex, row, 3)
	if err != nil {
		return nil, err
	}
	if z < 0 || z > tileMaxZoom {
		return nil, fmt.Errorf("%s: zoom %d out of range 0..%d", ex.Name, z, tileMaxZoom)
	}
	data, found, err := importer.LookupMBTilesTile(mbtilesCtx(env), path, z, x, y)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", ex.Name, err)
	}
	if !found {
		return nil, nil
	}
	return data, nil
}

func evalMBTilesIntArg(env ExecEnv, ex *FuncCall, row Row, idx int) (int, error) {
	f, err := evalGeoFloatArg(env, ex, row, idx)
	if err != nil {
		return 0, err
	}
	if f != math.Trunc(f) {
		return 0, fmt.Errorf("%s arg%d: expected an integer, got %v", ex.Name, idx+1, f)
	}
	return int(f), nil
}

// mbtilesCtx mirrors GeoSearchTableFunc's own ctx-or-env.ctx fallback
// (geo_search.go): a scalar funcHandler has no ctx parameter of its own, so
// env.ctx is the only source; a table function is handed one directly but
// falls back the same way if the caller passed nil.
func mbtilesCtx(env ExecEnv) context.Context {
	if env.ctx != nil {
		return env.ctx
	}
	return context.Background()
}

// mbtilesZoomRangeArgs reads an optional (min_zoom, max_zoom) argument pair
// shared by MBTILES_TILES' one- and three-argument forms, validating both
// against the same 0..tileMaxZoom grid every TILE_* function already uses.
func mbtilesZoomRangeArgs(env ExecEnv, ex *FuncCall, row Row) ([]int, error) {
	if len(ex.Args) == 1 {
		return nil, nil
	}
	minZoom, err := evalMBTilesIntArg(env, ex, row, 1)
	if err != nil {
		return nil, err
	}
	maxZoom, err := evalMBTilesIntArg(env, ex, row, 2)
	if err != nil {
		return nil, err
	}
	if minZoom < 0 || maxZoom > tileMaxZoom || minZoom > maxZoom {
		return nil, fmt.Errorf("%s: invalid zoom range %d..%d (must be within 0..%d)", ex.Name, minZoom, maxZoom, tileMaxZoom)
	}
	zooms := make([]int, 0, maxZoom-minZoom+1)
	for z := minZoom; z <= maxZoom; z++ {
		zooms = append(zooms, z)
	}
	return zooms, nil
}

// ── MBTILES_TILES (table-valued function) ───────────────────────────────

type MBTilesTilesTableFunc struct{}

func (f *MBTilesTilesTableFunc) Name() string { return "MBTILES_TILES" }

func (f *MBTilesTilesTableFunc) ValidateArgs(args []Expr) error {
	if len(args) != 1 && len(args) != 3 {
		return fmt.Errorf("MBTILES_TILES requires (file_path) or (file_path, min_zoom, max_zoom), got %d arguments", len(args))
	}
	return nil
}

func (f *MBTilesTilesTableFunc) Execute(ctx context.Context, args []Expr, env ExecEnv, row Row) (*ResultSet, error) {
	if err := f.ValidateArgs(args); err != nil {
		return nil, err
	}
	fc := &FuncCall{Name: f.Name(), Args: args}
	pathVal, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, fmt.Errorf("MBTILES_TILES path: %w", err)
	}
	path, ok := pathVal.(string)
	if !ok {
		return nil, fmt.Errorf("MBTILES_TILES: file path must be a string, got %T", pathVal)
	}
	zooms, err := mbtilesZoomRangeArgs(env, fc, row)
	if err != nil {
		return nil, err
	}

	if ctx == nil {
		ctx = mbtilesCtx(env)
	}
	tempDB := storage.NewDB()
	opts := &importer.OpenMBTilesOptions{
		TilesTable:    "tiles",
		MetadataTable: "tiles_metadata",
		MaxTiles:      mbtilesTVFMaxTiles,
		Zooms:         zooms,
	}
	result, err := importer.OpenMBTiles(ctx, tempDB, "default", path, opts)
	if err != nil {
		return nil, fmt.Errorf("MBTILES_TILES: %w", err)
	}
	if result.Truncated {
		return nil, fmt.Errorf("MBTILES_TILES: result exceeds %d tiles; narrow the range with MBTILES_TILES(file_path, min_zoom, max_zoom)", mbtilesTVFMaxTiles)
	}

	table, err := tempDB.Get("default", opts.TilesTable)
	if err != nil {
		return nil, fmt.Errorf("MBTILES_TILES: %w", err)
	}
	return tableToResultSet(table), nil
}

func init() {
	RegisterTableFunc(&MBTilesTilesTableFunc{})
}

// ── MBTILES_METADATA (table-valued function) ─────────────────────────────

type MBTilesMetadataTableFunc struct{}

func (f *MBTilesMetadataTableFunc) Name() string { return "MBTILES_METADATA" }

func (f *MBTilesMetadataTableFunc) ValidateArgs(args []Expr) error {
	if len(args) != 1 {
		return fmt.Errorf("MBTILES_METADATA requires (file_path), got %d arguments", len(args))
	}
	return nil
}

func (f *MBTilesMetadataTableFunc) Execute(ctx context.Context, args []Expr, env ExecEnv, row Row) (*ResultSet, error) {
	if err := f.ValidateArgs(args); err != nil {
		return nil, err
	}
	pathVal, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, fmt.Errorf("MBTILES_METADATA path: %w", err)
	}
	path, ok := pathVal.(string)
	if !ok {
		return nil, fmt.Errorf("MBTILES_METADATA: file path must be a string, got %T", pathVal)
	}

	if ctx == nil {
		ctx = mbtilesCtx(env)
	}
	tempDB := storage.NewDB()
	opts := &importer.OpenMBTilesOptions{
		TilesTable:      "tiles",
		MetadataTable:   "tiles_metadata",
		WithoutTileData: true,
		// Metadata is always read in full by OpenMBTiles regardless of tile
		// filtering; MaxTiles just keeps this call from also paying for a
		// tile scan it does not need any part of.
		MaxTiles: 1,
	}
	if _, err := importer.OpenMBTiles(ctx, tempDB, "default", path, opts); err != nil {
		return nil, fmt.Errorf("MBTILES_METADATA: %w", err)
	}

	table, err := tempDB.Get("default", opts.MetadataTable)
	if err != nil {
		// No metadata table is a legitimate, empty-result case (a tiles-only
		// file is still a valid, servable tileset), not a fault.
		return &ResultSet{Cols: []string{"name", "value"}, Rows: nil}, nil
	}
	return tableToResultSet(table), nil
}

func init() {
	RegisterTableFunc(&MBTilesMetadataTableFunc{})
}

// tableToResultSet projects every row of table into a ResultSet, the same
// column-by-name conversion GeoSearchTableFunc (geo_search.go) does for its
// own table scan.
func tableToResultSet(table *storage.Table) *ResultSet {
	cols := make([]string, 0, len(table.Cols))
	for _, c := range table.Cols {
		cols = append(cols, c.Name)
	}
	rows := make([]Row, 0, len(table.Rows))
	for _, r := range table.Rows {
		out := make(Row, len(table.Cols))
		for ci, c := range table.Cols {
			if ci < len(r) {
				out[c.Name] = r[ci]
			}
		}
		rows = append(rows, out)
	}
	return &ResultSet{Cols: cols, Rows: rows}
}
