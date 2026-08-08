//go:build sqliteimport && !js && !wasm && !baremetal

package importer

// Query an existing .mbtiles file without copying the whole tileset.
//
// ImportMBTiles duplicates every tile into tinySQL, which is right for a small
// tileset and impossible for a continental one. The options here make the useful
// middle ground reachable:
//
//   - Zooms restricts the read to selected zoom levels. Tile counts grow 4x per
//     level, so zooms 0-8 together are ~0.4% of a z0-14 tileset.
//   - WithoutTileData reads only the addressing columns plus each tile's size and
//     hash, giving a tile *index*. That is small even for a huge tileset and is
//     what coverage, integrity and duplicate-detection queries actually need.
//   - MaxTiles is a guard rail so a mistaken open fails fast instead of eating
//     memory.
//
// The source file is opened read-only and never modified.

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"net/url"
	"sort"
	"strings"

	_ "modernc.org/sqlite"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func applyOpenDefaults(opts *OpenMBTilesOptions) {
	opts.TilesTable, opts.MetadataTable = mbtilesDefaultTableNames(opts.TilesTable, opts.MetadataTable)
}

// OpenMBTiles exposes an existing .mbtiles file as queryable tinySQL tables
// without importing the whole tileset.
//
// The metadata table is always read in full: it is a handful of rows and is what
// tells a caller the tileset's format, bounds and zoom range.
func OpenMBTiles(
	ctx context.Context,
	db *storage.DB,
	tenant string,
	filePath string,
	opts *OpenMBTilesOptions,
) (*OpenMBTilesResult, error) {
	if opts == nil {
		opts = &OpenMBTilesOptions{}
	}
	applyOpenDefaults(opts)
	if tenant == "" {
		tenant = "default"
	}

	// Read-only, and immutable so the driver does not attempt to create or
	// recover a WAL sidecar next to somebody's tileset.
	dsn := "file:" + url.PathEscape(filePath) + "?mode=ro&immutable=1"
	src, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open mbtiles: %w", err)
	}
	defer src.Close()
	if err := src.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("open mbtiles %s: %w", filePath, err)
	}

	result := &OpenMBTilesResult{MinZoom: -1, MaxZoom: -1}

	importOpts := &ImportOptions{CreateTable: true, Truncate: true}
	applyDefaults(importOpts)
	if err := importMBTilesMetadata(ctx, db, tenant, opts.MetadataTable, src, importOpts); err != nil {
		return nil, err
	}
	if table, err := db.Get(tenant, opts.MetadataTable); err == nil {
		result.MetadataExposed = int64(len(table.Rows))
	}

	query, args := mbtilesTileQuery(opts)
	sqlRows, err := src.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("open mbtiles: query tiles: %w", err)
	}
	defer sqlRows.Close()

	colNames := []string{"zoom_level", "tile_column", "tile_row", "tile_size", "tile_sha256"}
	colTypes := []storage.ColType{storage.IntType, storage.IntType, storage.IntType, storage.IntType, storage.TextType}
	if !opts.WithoutTileData {
		colNames = append(colNames, "tile_data")
		colTypes = append(colTypes, storage.BlobType)
	}

	batchSize := mbtilesDefaultBatchSize
	// Not applyDefaults(appendOpts): its CreateTable heuristic ("enable it
	// when neither CreateTable nor Truncate is explicitly set") cannot tell
	// this struct's explicit false apart from an unset field, and would flip
	// CreateTable back to true here -- which is exactly what appendOpts
	// exists to say "no" to for every batch after the first. See the same
	// reasoning in insertTypedRows (geodata_helpers.go), which no longer
	// calls applyDefaults for this reason. BatchSize needs no default here:
	// insertTypedRows supplies its own when unset.
	appendOpts := &ImportOptions{CreateTable: false, Truncate: false}
	batchOpts := &ImportOptions{CreateTable: true, Truncate: true}

	rows := make([][]any, 0, batchSize)
	current := batchOpts
	flush := func() error {
		if len(rows) == 0 {
			return nil
		}
		res := &ImportResult{Encoding: "binary", ColumnNames: colNames, ColumnTypes: colTypes}
		if err := insertTypedRows(ctx, db, tenant, opts.TilesTable, colNames, colTypes, rows, current, res); err != nil {
			return err
		}
		rows = rows[:0]
		current = appendOpts
		return nil
	}

	for sqlRows.Next() {
		if opts.MaxTiles > 0 && result.TilesExposed >= opts.MaxTiles {
			result.Truncated = true
			break
		}
		var zoom, col, row int
		var tile []byte
		if err := sqlRows.Scan(&zoom, &col, &row, &tile); err != nil {
			return nil, fmt.Errorf("open mbtiles: scan tile: %w", err)
		}
		hash := sha256.Sum256(tile)
		out := []any{zoom, col, row, len(tile), fmt.Sprintf("%x", hash)}
		if !opts.WithoutTileData {
			out = append(out, tile)
		}
		rows = append(rows, out)

		result.TilesExposed++
		if result.MinZoom < 0 || zoom < result.MinZoom {
			result.MinZoom = zoom
		}
		if zoom > result.MaxZoom {
			result.MaxZoom = zoom
		}
		if len(rows) >= batchSize {
			if err := flush(); err != nil {
				return nil, err
			}
		}
	}
	if err := sqlRows.Err(); err != nil && !result.Truncated {
		return nil, fmt.Errorf("open mbtiles: read tiles: %w", err)
	}
	if err := flush(); err != nil {
		return nil, err
	}
	// Create the (empty) table even when no tile matched, so a query against it
	// returns no rows rather than "table not found".
	if result.TilesExposed == 0 {
		res := &ImportResult{Encoding: "binary", ColumnNames: colNames, ColumnTypes: colTypes}
		if err := insertTypedRows(ctx, db, tenant, opts.TilesTable, colNames, colTypes, nil, batchOpts, res); err != nil {
			return nil, err
		}
	}
	return result, nil
}

// mbtilesTileQuery builds the tile SELECT, restricting zoom levels in SQL so the
// source database does the filtering rather than the caller reading and
// discarding.
//
// tile_data is always selected: its length and hash are reported even when the
// payload is not retained, and those are what make an index-only view useful.
func mbtilesTileQuery(opts *OpenMBTilesOptions) (string, []any) {
	const base = "SELECT zoom_level, tile_column, tile_row, tile_data FROM tiles"
	if len(opts.Zooms) == 0 {
		return base, nil
	}
	zooms := append([]int(nil), opts.Zooms...)
	sort.Ints(zooms)
	placeholders := make([]string, 0, len(zooms))
	args := make([]any, 0, len(zooms))
	seen := make(map[int]bool, len(zooms))
	for _, z := range zooms {
		if seen[z] {
			continue
		}
		seen[z] = true
		placeholders = append(placeholders, "?")
		args = append(args, z)
	}
	return base + " WHERE zoom_level IN (" + strings.Join(placeholders, ", ") + ")", args
}
