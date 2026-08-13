//go:build !js && !wasm && !baremetal

package importer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// OpenMBTilesReader validates the complete artifact before returning a reader.
func OpenMBTilesReader(ctx context.Context, artifactPath string, maxMemoryBytes int64) (*MBTilesReader, error) {
	db, manifest, err := OpenMBTilesArtifact(ctx, artifactPath, maxMemoryBytes)
	if err != nil {
		return nil, err
	}
	reader := &MBTilesReader{db: db, manifest: manifest}
	if err := reader.prepareIndexes(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return reader, nil
}

func (r *MBTilesReader) prepareIndexes() error {
	var err error
	r.metadataIndex, err = requirePagedIndex(r.db, "metadata", "metadata_name")
	if err != nil {
		return err
	}
	if r.manifest.Schema == MBTilesSchemaFlat {
		r.tileIndex, err = requirePagedIndex(r.db, "tiles", "tiles_zxy")
		return err
	}
	r.tileIndex, err = requirePagedIndex(r.db, "map", "map_zxy")
	if err != nil {
		return err
	}
	r.imageIndex, err = requirePagedIndex(r.db, "images", "images_tile_id")
	return err
}

func requirePagedIndex(db *storage.DB, table, index string) (*storage.PagedIndexLocator, error) {
	locator, found, err := db.LocatePagedIndex("default", table, index)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("MBTiles reader index %s.%s is missing", table, index)
	}
	return locator, nil
}

func (r *MBTilesReader) Manifest() *MBTilesArtifactManifest {
	if r == nil || r.manifest == nil {
		return nil
	}
	return cloneReaderManifest(r.manifest)
}

// cloneReaderManifest keeps the reader's validated manifest private. In
// particular, Provenance and IndexConfig can contain nested JSON values, so a
// shallow struct copy would allow a caller to mutate a later Manifest result.
func cloneReaderManifest(in *MBTilesArtifactManifest) *MBTilesArtifactManifest {
	if in == nil {
		return nil
	}
	out := *in
	out.Provenance = cloneReaderJSONMap(in.Provenance)
	out.IndexConfig = cloneReaderJSONMap(in.IndexConfig)
	if in.Checksums != nil {
		out.Checksums = make(map[string]string, len(in.Checksums))
		for name, sum := range in.Checksums {
			out.Checksums[name] = sum
		}
	}
	if in.Tables != nil {
		out.Tables = make([]MBTilesArtifactTable, len(in.Tables))
		for i, table := range in.Tables {
			out.Tables[i] = MBTilesArtifactTable{Name: table.Name, Columns: append([]string(nil), table.Columns...), Rows: table.Rows}
			if table.Indexes != nil {
				out.Tables[i].Indexes = make([]MBTilesArtifactIndex, len(table.Indexes))
				for j, index := range table.Indexes {
					out.Tables[i].Indexes[j] = MBTilesArtifactIndex{Name: index.Name, Columns: append([]string(nil), index.Columns...), Unique: index.Unique}
				}
			}
		}
	}
	return &out
}

func cloneReaderJSONMap(in map[string]any) map[string]any {
	if len(in) == 0 {
		return nil
	}
	encoded, err := json.Marshal(in)
	if err != nil {
		return nil
	}
	var out map[string]any
	if err := json.Unmarshal(encoded, &out); err != nil {
		return nil
	}
	return out
}

// LookupMetadata returns one MBTiles metadata value by its standard name.
// Metadata is indexed separately from tiles, so callers do not need to scan
// the tile catalogue for ordinary fields such as format or bounds.
func (r *MBTilesReader) LookupMetadata(ctx context.Context, name string) (string, bool, error) {
	if r == nil || r.db == nil || r.manifest == nil {
		return "", false, errors.New("MBTiles reader is closed")
	}
	if err := checkContext(ctx); err != nil {
		return "", false, err
	}
	if name == "" {
		return "", false, errors.New("metadata name is empty")
	}
	var keyBuffer [128]byte
	key := storage.AppendCanonicalIndexValue(keyBuffer[:0], name)
	var value string
	found, err := r.metadataIndex.LookupUniqueColumn(key, 1, func(raw any) error {
		var ok bool
		value, ok = raw.(string)
		if !ok {
			return errors.New("metadata value has invalid type")
		}
		return nil
	})
	return value, found, err
}

// ScanMetadata streams the complete metadata table in the artifact's stable
// import order. Metadata is intentionally scanned through the paged table
// reader rather than collected from a query result: a serving application may
// copy this small set at open time without ever materializing its tile rows.
func (r *MBTilesReader) ScanMetadata(ctx context.Context, fn func(name, value string) error) error {
	if r == nil || r.db == nil || r.manifest == nil {
		return errors.New("MBTiles reader is closed")
	}
	if err := checkContext(ctx); err != nil {
		return err
	}
	if fn == nil {
		return errors.New("metadata callback is nil")
	}
	var callbackErr error
	handled, err := r.db.ScanRowsFast("default", "metadata", func(row []any) bool {
		if callbackErr != nil {
			return false
		}
		if err := checkContext(ctx); err != nil {
			callbackErr = err
			return false
		}
		if len(row) != 2 {
			callbackErr = fmt.Errorf("metadata row has %d columns", len(row))
			return false
		}
		name, nameOK := row[0].(string)
		value, valueOK := row[1].(string)
		if !nameOK || !valueOK || name == "" {
			callbackErr = errors.New("metadata row has invalid name or value")
			return false
		}
		callbackErr = fn(name, value)
		return callbackErr == nil
	})
	if err != nil {
		return err
	}
	if !handled {
		return errors.New("metadata table is not backed by a paged index")
	}
	return callbackErr
}

// LookupTile performs one exact TMS lookup. The returned BLOB is owned by the
// caller: the pager decoder has already copied it out of its page cache, so no
// second defensive payload copy is necessary here.
func (r *MBTilesReader) LookupTile(ctx context.Context, z, x, y int) ([]byte, bool, error) {
	var data []byte
	found, err := r.LookupTileFunc(ctx, z, x, y, func(tile []byte) error {
		data = tile
		return nil
	})
	return data, found, err
}

// LookupTileFunc is the allocation-conscious exact lookup API. fn receives a
// caller-owned decoded payload and may retain it. Unlike LookupTile, it does
// not need an additional result slice or a second BLOB copy.
func (r *MBTilesReader) LookupTileFunc(ctx context.Context, z, x, y int, fn func(data []byte) error) (bool, error) {
	if r == nil || r.db == nil || r.manifest == nil {
		return false, errors.New("MBTiles reader is closed")
	}
	if err := checkContext(ctx); err != nil {
		return false, err
	}
	if err := validateTileCoordinate(z, x, y); err != nil {
		return false, err
	}
	if fn == nil {
		return false, errors.New("tile callback is nil")
	}
	var keyBuffer [64]byte
	key := storage.AppendCanonicalIndexValue(keyBuffer[:0], z)
	key = storage.AppendCanonicalIndexValue(key, x)
	key = storage.AppendCanonicalIndexValue(key, y)
	if r.manifest.Schema == MBTilesSchemaFlat {
		return r.tileIndex.LookupUniqueBytesColumn(key, 3, fn)
	}
	var id string
	found, err := r.tileIndex.LookupUniqueColumn(key, 3, func(raw any) error {
		var ok bool
		id, ok = raw.(string)
		if !ok {
			return errors.New("tile_id has invalid type")
		}
		return nil
	})
	if err != nil || !found {
		return found, err
	}
	var imageKeyBuffer [128]byte
	imageKey := storage.AppendCanonicalIndexValue(imageKeyBuffer[:0], id)
	return r.imageIndex.LookupUniqueBytesColumn(imageKey, 1, fn)
}

// ScanTileRange streams a TMS rectangle. A z/x/y B+Tree cannot express a
// rectangular y bound across multiple x values as one contiguous key range,
// so scan one tightly bounded y interval per x. This avoids decoding every
// tile in the selected columns for the common small map-window workload.
func (r *MBTilesReader) ScanTileRange(ctx context.Context, z, xMin, xMax, yMin, yMax int, fn func(z, x, y int, data []byte) bool) error {
	if r == nil || r.db == nil || r.manifest == nil {
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
	width, height := xMax-xMin+1, yMax-yMin+1
	if width <= 8 && height <= 8 && width*height <= 64 {
		for x := xMin; x <= xMax; x++ {
			for y := yMin; y <= yMax; y++ {
				keepGoing := true
				_, err := r.LookupTileFunc(ctx, z, x, y, func(data []byte) error {
					keepGoing = fn(z, x, y, data)
					return nil
				})
				if err != nil {
					return err
				}
				if !keepGoing {
					return nil
				}
			}
		}
		return nil
	}
	table, index := "tiles", "tiles_zxy"
	if r.manifest.Schema == MBTilesSchemaNormalized {
		table, index = "map", "map_zxy"
	}
	var callbackErr error
	for x := xMin; x <= xMax; x++ {
		start := []any{z, x, yMin}
		end := []any{z, x, yMax}
		_, err := r.db.ScanPagedIndexRange("default", table, index, start, end, func(row []any) bool {
			if callbackErr != nil {
				return false
			}
			if err := checkContext(ctx); err != nil {
				callbackErr = err
				return false
			}
			if len(row) < 4 {
				callbackErr = errors.New("tile range index row has fewer than four columns")
				return false
			}
			zi, ok1 := artifactInt(row[0])
			xi, ok2 := artifactInt(row[1])
			yi, ok3 := artifactInt(row[2])
			if !ok1 || !ok2 || !ok3 || zi != z || xi != x || yi < yMin || yi > yMax {
				callbackErr = errors.New("tile range index returned an out-of-range key")
				return false
			}
			var data []byte
			if r.manifest.Schema == MBTilesSchemaFlat {
				var ok bool
				data, ok = row[3].([]byte)
				if !ok {
					callbackErr = errors.New("tile_data has invalid type")
					return false
				}
			} else {
				id, ok := row[3].(string)
				if !ok {
					callbackErr = errors.New("tile_id has invalid type")
					return false
				}
				var found bool
				found, callbackErr = r.imageIndex.LookupUniqueColumn(storage.AppendCanonicalIndexValue(nil, id), 1, func(raw any) error {
					var ok bool
					data, ok = raw.([]byte)
					if !ok {
						return fmt.Errorf("tile_data has invalid type for tile_id %q", id)
					}
					return nil
				})
				if callbackErr != nil {
					return false
				}
				if !found {
					callbackErr = fmt.Errorf("image index has no row for tile_id %q", id)
					return false
				}
			}
			// An empty tile is a valid BLOB. The type assertions above distinguish it
			// from malformed stored data without treating []byte{} as a failure.
			return fn(zi, xi, yi, data)
		})
		if err != nil {
			return err
		}
		if callbackErr != nil {
			return callbackErr
		}
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
