//go:build sqliteimport && !js && !wasm && !baremetal

package importer

// Export a tinySQL tileset back to a real .mbtiles file.
//
// MBTiles is a SQLite database with a prescribed schema, so the output is written
// through the same driver the importer reads with. That matters more than it
// might seem: the point of exporting is that the result works with tooling that
// knows nothing about tinySQL (tileserver-gl, mbutil, QGIS, Felt, ...), and the
// only way to guarantee that is to produce a genuine SQLite file with the
// specification's schema, indexes and required metadata keys.
//
// Specification: https://github.com/mapbox/mbtiles-spec (1.3)

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"

	_ "modernc.org/sqlite"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func applyExportDefaults(opts *ExportMBTilesOptions) {
	opts.TilesTable, opts.MetadataTable = mbtilesDefaultTableNames(opts.TilesTable, opts.MetadataTable)
	if opts.BatchSize <= 0 {
		opts.BatchSize = mbtilesDefaultBatchSize
	}
}

// ExportMBTiles writes a tinySQL tileset to filePath as an MBTiles database.
//
// An existing file is refused rather than overwritten: a tileset is expensive to
// produce and silently replacing one is the wrong default. Remove it first if
// that is the intent.
func ExportMBTiles(
	ctx context.Context,
	db *storage.DB,
	tenant string,
	filePath string,
	opts *ExportMBTilesOptions,
) (*ExportMBTilesResult, error) {
	if opts == nil {
		opts = &ExportMBTilesOptions{TileRowIsTMS: true}
	}
	applyExportDefaults(opts)
	if tenant == "" {
		tenant = "default"
	}

	if _, err := os.Stat(filePath); err == nil {
		return nil, fmt.Errorf("export mbtiles: %s already exists", filePath)
	} else if !os.IsNotExist(err) {
		return nil, fmt.Errorf("export mbtiles: stat %s: %w", filePath, err)
	}

	// A schema-only lookup is enough to resolve the tile columns, and it is
	// the cheap kind on a ModePagedIndex backend (catalog only, no row scan)
	// -- unlike db.Get, which would materialize the whole tiles table just to
	// read its column list. mbtilesWriteTiles below does the equivalent
	// schema/rows split for the tile data itself.
	var tiles *storage.Table
	if meta, ok, metaErr := db.PagedIndexMetadata(tenant, opts.TilesTable); metaErr != nil {
		return nil, fmt.Errorf("export mbtiles: tiles table %q: %w", opts.TilesTable, metaErr)
	} else if ok {
		tiles = meta
	}
	if tiles == nil {
		var err error
		tiles, err = db.Get(tenant, opts.TilesTable)
		if err != nil {
			return nil, fmt.Errorf("export mbtiles: tiles table %q: %w", opts.TilesTable, err)
		}
	}
	idx, err := mbtilesTileColumns(tiles)
	if err != nil {
		return nil, err
	}

	dst, err := sql.Open("sqlite", filePath)
	if err != nil {
		return nil, fmt.Errorf("export mbtiles: create %s: %w", filePath, err)
	}
	defer dst.Close()

	if err := mbtilesCreateSchema(ctx, dst); err != nil {
		return nil, err
	}

	result := &ExportMBTilesResult{MinZoom: -1, MaxZoom: -1}
	if err := mbtilesWriteTiles(ctx, dst, db, tenant, opts.TilesTable, tiles, idx, opts, result); err != nil {
		return nil, err
	}
	if err := mbtilesWriteMetadata(ctx, dst, db, tenant, opts, result); err != nil {
		return nil, err
	}
	return result, nil
}

// mbtilesTileIdx records where the four required tile columns live.
type mbtilesTileIdx struct {
	zoom, column, row, data int
}

func mbtilesTileColumns(tiles *storage.Table) (mbtilesTileIdx, error) {
	var idx mbtilesTileIdx
	for _, spec := range []struct {
		name string
		dst  *int
	}{
		{"zoom_level", &idx.zoom},
		{"tile_column", &idx.column},
		{"tile_row", &idx.row},
		{"tile_data", &idx.data},
	} {
		i, err := tiles.ColIndex(spec.name)
		if err != nil {
			return idx, fmt.Errorf("export mbtiles: tiles table needs a %q column: %w", spec.name, err)
		}
		*spec.dst = i
	}
	return idx, nil
}

// mbtilesCreateSchema creates the specification's tables and the unique index
// that makes a tile lookup a seek. The index is also what enforces one tile per
// (zoom, column, row).
func mbtilesCreateSchema(ctx context.Context, dst *sql.DB) error {
	stmts := []string{
		`CREATE TABLE metadata (name TEXT, value TEXT)`,
		`CREATE TABLE tiles (zoom_level INTEGER, tile_column INTEGER, tile_row INTEGER, tile_data BLOB)`,
		`CREATE UNIQUE INDEX name ON metadata (name)`,
		`CREATE UNIQUE INDEX tile_index ON tiles (zoom_level, tile_column, tile_row)`,
	}
	for _, stmt := range stmts {
		if _, err := dst.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("export mbtiles: %s: %w", stmt, err)
		}
	}
	return nil
}

// mbtilesWriteTiles writes every row of the tenant's tiles table to dst.
//
// It streams through db.ScanRowsFast when the source is ModePagedIndex,
// reading one B+Tree leaf at a time, so exporting a tileset larger than
// memory does not first need db.Get to load the whole thing (the write side
// of what AppendRowsFast does for import). Every other storage mode falls
// back to ranging over tiles.Rows, unchanged from before this existed.
func mbtilesWriteTiles(
	ctx context.Context,
	dst *sql.DB,
	db *storage.DB,
	tenant, tableName string,
	tiles *storage.Table,
	idx mbtilesTileIdx,
	opts *ExportMBTilesOptions,
	result *ExportMBTilesResult,
) error {
	insert := `INSERT OR REPLACE INTO tiles (zoom_level, tile_column, tile_row, tile_data) VALUES (?, ?, ?, ?)`

	var tx *sql.Tx
	var stmt *sql.Stmt
	pending := 0
	begin := func() error {
		var err error
		if tx, err = dst.BeginTx(ctx, nil); err != nil {
			return fmt.Errorf("export mbtiles: begin: %w", err)
		}
		if stmt, err = tx.PrepareContext(ctx, insert); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("export mbtiles: prepare: %w", err)
		}
		return nil
	}
	commit := func() error {
		if tx == nil {
			return nil
		}
		if err := stmt.Close(); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("export mbtiles: close stmt: %w", err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("export mbtiles: commit: %w", err)
		}
		tx, stmt, pending = nil, nil, 0
		return nil
	}

	rowNum := 0
	writeRow := func(row []any) error {
		zoom, err := mbtilesInt(row, idx.zoom, "zoom_level", rowNum)
		if err != nil {
			return err
		}
		col, err := mbtilesInt(row, idx.column, "tile_column", rowNum)
		if err != nil {
			return err
		}
		tileRow, err := mbtilesInt(row, idx.row, "tile_row", rowNum)
		if err != nil {
			return err
		}
		if zoom < 0 || zoom > 30 {
			return fmt.Errorf("export mbtiles: row %d: zoom_level %d out of range 0..30", rowNum, zoom)
		}
		limit := 1 << uint(zoom)
		if col < 0 || col >= limit || tileRow < 0 || tileRow >= limit {
			return fmt.Errorf("export mbtiles: row %d: tile %d/%d/%d outside the zoom's %d-tile grid",
				rowNum, zoom, col, tileRow, limit)
		}
		if !opts.TileRowIsTMS {
			tileRow = limit - 1 - tileRow
		}

		data, err := mbtilesBlob(row, idx.data, rowNum)
		if err != nil {
			return err
		}

		if tx == nil {
			if err := begin(); err != nil {
				return err
			}
		}
		if _, err := stmt.ExecContext(ctx, zoom, col, tileRow, data); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("export mbtiles: insert tile %d/%d/%d: %w", zoom, col, tileRow, err)
		}
		result.TilesWritten++
		if result.MinZoom < 0 || zoom < result.MinZoom {
			result.MinZoom = zoom
		}
		if zoom > result.MaxZoom {
			result.MaxZoom = zoom
		}

		pending++
		if pending >= opts.BatchSize {
			if err := commit(); err != nil {
				return err
			}
		}
		rowNum++
		return nil
	}

	var streamErr error
	streamed, scanErr := db.ScanRowsFast(tenant, tableName, func(row []any) bool {
		if err := ctx.Err(); err != nil {
			streamErr = err
			return false
		}
		if err := writeRow(row); err != nil {
			streamErr = err
			return false
		}
		return true
	})
	if streamed {
		// A callback returning false to stop the scan early is not itself an
		// error to ScanRange/ScanRowsFast, so streamErr (our own reason for
		// stopping) and scanErr (a pager-level failure) are checked
		// separately -- either one means the export did not fully complete.
		if scanErr != nil {
			_ = commit()
			return scanErr
		}
		if streamErr != nil {
			_ = commit()
			return streamErr
		}
		return commit()
	}

	for _, row := range tiles.Rows {
		if err := ctx.Err(); err != nil {
			_ = commit()
			return err
		}
		if err := writeRow(row); err != nil {
			return err
		}
	}
	return commit()
}

func mbtilesInt(row []any, idx int, name string, rowNum int) (int, error) {
	if idx >= len(row) || row[idx] == nil {
		return 0, fmt.Errorf("export mbtiles: row %d: %s is NULL", rowNum, name)
	}
	switch v := row[idx].(type) {
	case int:
		return v, nil
	case int32:
		return int(v), nil
	case int64:
		return int(v), nil
	case float64:
		if v != float64(int(v)) {
			return 0, fmt.Errorf("export mbtiles: row %d: %s %v is not an integer", rowNum, name, v)
		}
		return int(v), nil
	default:
		return 0, fmt.Errorf("export mbtiles: row %d: %s has type %T, want an integer", rowNum, name, row[idx])
	}
}

// mbtilesBlob reads a tile payload. A NULL tile is written as NULL rather than
// an empty blob, because the two mean different things to a tile server: absent
// versus present-but-blank.
func mbtilesBlob(row []any, idx, rowNum int) (any, error) {
	if idx >= len(row) || row[idx] == nil {
		return nil, nil
	}
	switch v := row[idx].(type) {
	case []byte:
		return v, nil
	case string:
		// A tileset round-tripped through a text-only transport can arrive as a
		// string; pass the bytes through unchanged rather than guessing an
		// encoding.
		return []byte(v), nil
	default:
		return nil, fmt.Errorf("export mbtiles: row %d: tile_data has type %T, want BLOB", rowNum, row[idx])
	}
}

// mbtilesWriteMetadata copies the metadata table, applies overrides, and fills in
// the keys the specification requires so the output is valid even when the source
// tileset carried none.
func mbtilesWriteMetadata(
	ctx context.Context,
	dst *sql.DB,
	db *storage.DB,
	tenant string,
	opts *ExportMBTilesOptions,
	result *ExportMBTilesResult,
) error {
	meta := map[string]string{}
	order := make([]string, 0, 8)
	put := func(k, v string) {
		if _, seen := meta[k]; !seen {
			order = append(order, k)
		}
		meta[k] = v
	}

	if table, err := db.Get(tenant, opts.MetadataTable); err == nil {
		nameIdx, errName := table.ColIndex("name")
		valueIdx, errValue := table.ColIndex("value")
		if errName != nil || errValue != nil {
			return fmt.Errorf("export mbtiles: metadata table %q needs name and value columns", opts.MetadataTable)
		}
		for _, row := range table.Rows {
			if nameIdx >= len(row) || row[nameIdx] == nil {
				continue
			}
			name := fmt.Sprintf("%v", row[nameIdx])
			value := ""
			if valueIdx < len(row) && row[valueIdx] != nil {
				value = fmt.Sprintf("%v", row[valueIdx])
			}
			put(name, value)
		}
	}
	for k, v := range opts.Metadata {
		put(k, v)
	}

	// The specification requires name and format; minzoom/maxzoom are optional
	// but every consumer uses them, and they are known exactly from the tiles
	// just written.
	if _, ok := meta["name"]; !ok {
		put("name", opts.TilesTable)
	}
	if _, ok := meta["format"]; !ok {
		// png is the safest default: it is the format the specification's own
		// examples use, and a wrong guess here only affects the Content-Type a
		// server reports, not the bytes.
		put("format", "png")
	}
	if _, ok := meta["minzoom"]; !ok && result.MinZoom >= 0 {
		put("minzoom", fmt.Sprint(result.MinZoom))
	}
	if _, ok := meta["maxzoom"]; !ok && result.MaxZoom >= 0 {
		put("maxzoom", fmt.Sprint(result.MaxZoom))
	}

	tx, err := dst.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("export mbtiles: begin metadata: %w", err)
	}
	stmt, err := tx.PrepareContext(ctx, `INSERT OR REPLACE INTO metadata (name, value) VALUES (?, ?)`)
	if err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("export mbtiles: prepare metadata: %w", err)
	}
	for _, k := range order {
		if strings.TrimSpace(k) == "" {
			continue
		}
		if _, err := stmt.ExecContext(ctx, k, meta[k]); err != nil {
			_ = stmt.Close()
			_ = tx.Rollback()
			return fmt.Errorf("export mbtiles: write metadata %q: %w", k, err)
		}
		result.MetadataWritten++
	}
	if err := stmt.Close(); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("export mbtiles: close metadata stmt: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("export mbtiles: commit metadata: %w", err)
	}
	return nil
}
