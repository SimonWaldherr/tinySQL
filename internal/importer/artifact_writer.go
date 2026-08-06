//go:build !js && !wasm && !baremetal

package importer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func cloneArtifactOptions(in *MBTilesArtifactOptions) (*MBTilesArtifactOptions, error) {
	out := *in
	provenance, err := cloneArtifactProvenance(in.Provenance)
	if err != nil {
		return nil, err
	}
	out.Provenance = provenance
	return &out, nil
}

func cloneArtifactProvenance(in map[string]any) (map[string]any, error) {
	if len(in) == 0 {
		return nil, nil
	}
	encoded, err := json.Marshal(in)
	if err != nil {
		return nil, fmt.Errorf("encode provenance as JSON: %w", err)
	}
	var out map[string]any
	if err := json.Unmarshal(encoded, &out); err != nil {
		return nil, fmt.Errorf("decode provenance as JSON: %w", err)
	}
	return out, nil
}

func createArtifactTables(db *storage.DB, schema MBTilesArtifactSchema) error {
	metadata := storage.NewTable("metadata", []storage.Column{{Name: "name", Type: storage.TextType}, {Name: "value", Type: storage.TextType}}, false)
	if err := metadata.CreateSecondaryIndex("metadata_name", []string{"name"}, true); err != nil {
		return err
	}
	if err := db.Put("default", metadata); err != nil {
		return fmt.Errorf("create metadata table: %w", err)
	}
	if schema == MBTilesSchemaFlat {
		tiles := storage.NewTable("tiles", []storage.Column{{Name: "z", Type: storage.IntType}, {Name: "x", Type: storage.IntType}, {Name: "y", Type: storage.IntType}, {Name: "tile_data", Type: storage.BlobType}}, false)
		if err := tiles.CreateSecondaryIndex("tiles_zxy", []string{"z", "x", "y"}, true); err != nil {
			return err
		}
		return db.Put("default", tiles)
	}
	mapTable := storage.NewTable("map", []storage.Column{{Name: "z", Type: storage.IntType}, {Name: "x", Type: storage.IntType}, {Name: "y", Type: storage.IntType}, {Name: "tile_id", Type: storage.TextType}}, false)
	if err := mapTable.CreateSecondaryIndex("map_zxy", []string{"z", "x", "y"}, true); err != nil {
		return err
	}
	if err := db.Put("default", mapTable); err != nil {
		return fmt.Errorf("create map table: %w", err)
	}
	images := storage.NewTable("images", []storage.Column{{Name: "tile_id", Type: storage.TextType}, {Name: "tile_data", Type: storage.BlobType}}, false)
	if err := images.CreateSecondaryIndex("images_tile_id", []string{"tile_id"}, true); err != nil {
		return err
	}
	if err := db.Put("default", images); err != nil {
		return fmt.Errorf("create images table: %w", err)
	}
	return nil
}

type artifactProgress struct {
	opts                                          *MBTilesArtifactOptions
	total, read, written, bytesRead, bytesWritten int64
	batchSize                                     int
	lastEmit                                      time.Time
}

func (p *artifactProgress) emit(phase string) {
	now := time.Now()
	if phase == "import" && !p.lastEmit.IsZero() && now.Sub(p.lastEmit) < p.opts.ProgressEvery && p.written < p.total {
		return
	}
	if p.opts.Progress != nil {
		p.opts.Progress(MBTilesProgress{Phase: phase, RowsRead: p.read, RowsWritten: p.written, BytesRead: p.bytesRead, BytesWritten: p.bytesWritten, TotalRows: p.total, BatchSize: p.batchSize})
	}
	p.lastEmit = now
}

func appendArtifactRows(ctx context.Context, db *storage.DB, tenant, table string, rows [][]any, batchSize int, progress *artifactProgress, opts *MBTilesArtifactOptions) error {
	// Put stores an empty compatibility snapshot in DB.tenants. This writer
	// bypasses it, so discard it even when there are no rows to append; otherwise
	// Close can overwrite a valid empty table with that stale snapshot.
	defer db.DiscardCachedTable(tenant, table)
	for start := 0; start < len(rows); start += batchSize {
		end := start + batchSize
		if end > len(rows) {
			end = len(rows)
		}
		if err := checkContext(ctx); err != nil {
			return err
		}
		ok, err := db.AppendRowsFast(tenant, table, rows[start:end])
		if err != nil {
			return fmt.Errorf("append %s: %w", table, err)
		}
		if !ok {
			return fmt.Errorf("append %s: paged index fast path unavailable", table)
		}
		if progress != nil {
			progress.written += int64(end - start)
			for _, row := range rows[start:end] {
				if len(row) > 3 {
					if data, ok := row[len(row)-1].([]byte); ok {
						progress.bytesWritten += int64(len(data))
					}
				}
			}
			progress.emit("import")
		}
	}
	_ = opts
	return nil
}

func artifactIndexConfig(schema MBTilesArtifactSchema) map[string]any {
	if schema == MBTilesSchemaFlat {
		return map[string]any{"tiles": "tiles_zxy", "metadata": "metadata_name", "coordinate_system": "TMS"}
	}
	return map[string]any{"map": "map_zxy", "images": "images_tile_id", "metadata": "metadata_name", "coordinate_system": "TMS"}
}

func artifactTableManifest(schema MBTilesArtifactSchema, tiles, images, metadata int64) []MBTilesArtifactTable {
	out := []MBTilesArtifactTable{{Name: "metadata", Columns: []string{"name", "value"}, Rows: metadata, Indexes: []MBTilesArtifactIndex{{Name: "metadata_name", Columns: []string{"name"}, Unique: true}}}}
	if schema == MBTilesSchemaFlat {
		return append(out, MBTilesArtifactTable{Name: "tiles", Columns: []string{"z", "x", "y", "tile_data"}, Rows: tiles, Indexes: []MBTilesArtifactIndex{{Name: "tiles_zxy", Columns: []string{"z", "x", "y"}, Unique: true}}})
	}
	return append(out, MBTilesArtifactTable{Name: "map", Columns: []string{"z", "x", "y", "tile_id"}, Rows: tiles, Indexes: []MBTilesArtifactIndex{{Name: "map_zxy", Columns: []string{"z", "x", "y"}, Unique: true}}}, MBTilesArtifactTable{Name: "images", Columns: []string{"tile_id", "tile_data"}, Rows: images, Indexes: []MBTilesArtifactIndex{{Name: "images_tile_id", Columns: []string{"tile_id"}, Unique: true}}})
}

func writeJSONFile(path string, value any) error {
	b, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	return os.WriteFile(path, b, 0o644)
}

func checksumFiles(root string, names []string) (map[string]string, error) {
	out := make(map[string]string, len(names))
	for _, name := range names {
		sum, err := checksumFile(filepath.Join(root, name))
		if err != nil {
			return nil, fmt.Errorf("checksum %s: %w", name, err)
		}
		out[name] = sum
	}
	return out, nil
}

func writeChecksumsFile(root string) error {
	names := append([]string{"manifest.json"}, artifactDataFileNames(root)...)
	sort.Strings(names)
	f, err := os.OpenFile(filepath.Join(root, "checksums.sha256"), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	for _, name := range names {
		sum, err := checksumFile(filepath.Join(root, name))
		if err != nil {
			return err
		}
		if _, err := fmt.Fprintf(f, "%s  %s\n", sum, name); err != nil {
			return err
		}
	}
	return f.Sync()
}

func syncArtifactTree(root string) error {
	names := append([]string{"manifest.json"}, artifactDataFileNames(root)...)
	names = append(names, "checksums.sha256", "COMPLETE")
	for _, name := range names {
		f, err := os.OpenFile(filepath.Join(root, name), os.O_RDONLY, 0)
		if err != nil {
			return err
		}
		if err = f.Sync(); err != nil {
			_ = f.Close()
			return err
		}
		if err = f.Close(); err != nil {
			return err
		}
	}
	d, err := os.Open(root)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func publishArtifact(tmp, target string, replace bool) error {
	if !replace {
		return os.Rename(tmp, target)
	}
	backup := target + ".rollback"
	_ = os.RemoveAll(backup)
	if _, err := os.Lstat(target); err == nil {
		if err := os.Rename(target, backup); err != nil {
			return err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := os.Rename(tmp, target); err != nil {
		if _, backupErr := os.Lstat(backup); backupErr == nil {
			_ = os.Rename(backup, target)
		}
		return err
	}
	if _, err := os.Lstat(backup); err == nil {
		return os.RemoveAll(backup)
	}
	return nil
}
