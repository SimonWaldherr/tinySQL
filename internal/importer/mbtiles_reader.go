//go:build sqliteimport && !js && !wasm && !baremetal

package importer

import (
	"context"
	"errors"
	"fmt"
	"math"
)

// OpenMBTilesReader validates the complete artifact before returning a reader.
func OpenMBTilesReader(ctx context.Context, artifactPath string, maxMemoryBytes int64) (*MBTilesReader, error) {
	db, manifest, err := OpenMBTilesArtifact(ctx, artifactPath, maxMemoryBytes)
	if err != nil {
		return nil, err
	}
	return &MBTilesReader{db: db, manifest: manifest}, nil
}

func (r *MBTilesReader) Manifest() *MBTilesArtifactManifest {
	if r == nil || r.manifest == nil {
		return nil
	}
	copy := *r.manifest
	return &copy
}

// LookupTile performs one exact TMS lookup and returns a copy of tile_data.
func (r *MBTilesReader) LookupTile(ctx context.Context, z, x, y int) ([]byte, bool, error) {
	if r == nil || r.db == nil {
		return nil, false, errors.New("MBTiles reader is closed")
	}
	if err := checkContext(ctx); err != nil {
		return nil, false, err
	}
	if err := validateTileCoordinate(z, x, y); err != nil {
		return nil, false, err
	}
	table, index := "tiles", "tiles_zxy"
	if r.manifest.Schema == MBTilesSchemaNormalized {
		table, index = "map", "map_zxy"
	}
	rows, handled, err := r.db.PagedIndexRows("default", table, index, []any{z, x, y})
	if err != nil || !handled || len(rows) == 0 {
		return nil, false, err
	}
	if len(rows) != 1 {
		return nil, false, fmt.Errorf("tile index returned %d rows for z=%d x=%d y=%d", len(rows), z, x, y)
	}
	if r.manifest.Schema == MBTilesSchemaFlat {
		data, ok := rows[0][3].([]byte)
		if !ok {
			return nil, false, errors.New("tile_data has invalid type")
		}
		return append([]byte(nil), data...), true, nil
	}
	id, ok := rows[0][3].(string)
	if !ok {
		return nil, false, errors.New("tile_id has invalid type")
	}
	images, handled, err := r.db.PagedIndexRows("default", "images", "images_tile_id", []any{id})
	if err != nil || !handled || len(images) == 0 {
		return nil, false, err
	}
	if len(images) != 1 {
		return nil, false, fmt.Errorf("image index returned %d rows for tile_id %q", len(images), id)
	}
	data, ok := images[0][1].([]byte)
	if !ok {
		return nil, false, errors.New("tile_data has invalid type")
	}
	return append([]byte(nil), data...), true, nil
}

// ScanTileRange streams a TMS rectangle. The ordered z/x/y index narrows the
// physical scan to the x interval; the y predicate is applied while rows are
// streamed, so a spatially contiguous request never materializes the table.
func (r *MBTilesReader) ScanTileRange(ctx context.Context, z, xMin, xMax, yMin, yMax int, fn func(z, x, y int, data []byte) bool) error {
	if r == nil || r.db == nil {
		return errors.New("MBTiles reader is closed")
	}
	if fn == nil {
		return errors.New("tile range callback is nil")
	}
	if xMin > xMax || yMin > yMax {
		return errors.New("invalid tile range")
	}
	if err := validateTileCoordinate(z, xMin, yMin); err != nil {
		return err
	}
	if err := validateTileCoordinate(z, xMax, yMax); err != nil {
		return err
	}
	table, index := "tiles", "tiles_zxy"
	if r.manifest.Schema == MBTilesSchemaNormalized {
		table, index = "map", "map_zxy"
	}
	// Signed extrema are encoded in canonical integer order and therefore make
	// the interval inclusive of every y at the requested x values.
	start := []any{z, xMin, math.MinInt64}
	end := []any{z, xMax, math.MaxInt64}
	_, err := r.db.ScanPagedIndexRange("default", table, index, start, end, func(row []any) bool {
		if err := checkContext(ctx); err != nil {
			return false
		}
		if len(row) < 4 {
			return false
		}
		zi, ok1 := artifactInt(row[0])
		xi, ok2 := artifactInt(row[1])
		yi, ok3 := artifactInt(row[2])
		if !ok1 || !ok2 || !ok3 || zi != z || xi < xMin || xi > xMax || yi < yMin || yi > yMax {
			return true
		}
		var data []byte
		if r.manifest.Schema == MBTilesSchemaFlat {
			data, _ = row[3].([]byte)
		} else if id, ok := row[3].(string); ok {
			images, handled, lookupErr := r.db.PagedIndexRows("default", "images", "images_tile_id", []any{id})
			if lookupErr != nil || !handled || len(images) != 1 {
				return false
			}
			data, _ = images[0][1].([]byte)
		}
		if data == nil {
			return false
		}
		return fn(zi, xi, yi, append([]byte(nil), data...))
	})
	if err != nil {
		return err
	}
	if err := checkContext(ctx); err != nil {
		return err
	}
	return nil
}

func (r *MBTilesReader) Close() error {
	if r == nil || r.db == nil {
		return nil
	}
	err := r.db.Close()
	r.db = nil
	return err
}
