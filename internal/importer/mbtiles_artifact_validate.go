//go:build sqliteimport && !js && !wasm && !baremetal

package importer

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	_ "modernc.org/sqlite"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func ValidateMBTilesArtifact(ctx context.Context, artifactPath string) (*MBTilesArtifactManifest, error) {
	return validateArtifact(ctx, artifactPath, true)
}

func OpenMBTilesArtifact(ctx context.Context, artifactPath string, maxMemoryBytes int64) (*storage.DB, *MBTilesArtifactManifest, error) {
	manifest, err := ValidateMBTilesArtifact(ctx, artifactPath)
	if err != nil {
		return nil, nil, err
	}
	if maxMemoryBytes <= 0 {
		maxMemoryBytes = 64 << 20
	}
	db, err := storage.OpenDB(storage.StorageConfig{Mode: storage.ModePagedIndex, Path: filepath.Join(artifactPath, "database"), MaxMemoryBytes: maxMemoryBytes, ReadOnly: true})
	if err != nil {
		return nil, nil, fmt.Errorf("open validated MBTiles artifact: %w", err)
	}
	return db, manifest, nil
}

func validateArtifact(ctx context.Context, root string, requireComplete bool) (*MBTilesArtifactManifest, error) {
	if requireComplete {
		if info, err := os.Stat(filepath.Join(root, "COMPLETE")); err != nil || !info.Mode().IsRegular() || info.Size() != 0 {
			return nil, errors.New("artifact is not complete")
		}
	}
	for _, dir := range []string{"database", "indexes"} {
		info, err := os.Stat(filepath.Join(root, dir))
		if err != nil || !info.IsDir() {
			return nil, fmt.Errorf("artifact directory %s is missing", dir)
		}
	}
	manifestBytes, err := os.ReadFile(filepath.Join(root, "manifest.json"))
	if err != nil {
		return nil, fmt.Errorf("read manifest: %w", err)
	}
	var manifest MBTilesArtifactManifest
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		return nil, fmt.Errorf("decode manifest: %w", err)
	}
	if manifest.FormatVersion != mbtilesArtifactFormatVersion {
		return nil, fmt.Errorf("unsupported artifact format version %d", manifest.FormatVersion)
	}
	if manifest.Schema != MBTilesSchemaFlat && manifest.Schema != MBTilesSchemaNormalized {
		return nil, fmt.Errorf("invalid artifact schema %q", manifest.Schema)
	}
	if len(manifest.Tables) == 0 {
		return nil, errors.New("manifest contains no tables")
	}
	if err := verifyArtifactChecksums(root, manifest, manifestBytes); err != nil {
		return nil, err
	}
	indexConfig, err := os.ReadFile(filepath.Join(root, "indexes", "config.json"))
	if err != nil {
		return nil, fmt.Errorf("read index configuration: %w", err)
	}
	var config map[string]any
	if err := json.Unmarshal(indexConfig, &config); err != nil {
		return nil, fmt.Errorf("decode index configuration: %w", err)
	}
	if coordinate, _ := config["coordinate_system"].(string); coordinate != "TMS" {
		return nil, errors.New("artifact does not declare TMS coordinates")
	}

	db, err := storage.OpenDB(storage.StorageConfig{Mode: storage.ModePagedIndex, Path: filepath.Join(root, "database"), MaxMemoryBytes: 64 << 20, ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("open artifact database for validation: %w", err)
	}
	defer db.Close()
	for _, table := range manifest.Tables {
		if err := validateArtifactTable(ctx, db, table); err != nil {
			return nil, err
		}
	}
	metadataDigest, err := validateArtifactMetadata(ctx, db)
	if err != nil {
		return nil, err
	}
	if expected, _ := manifest.IndexConfig["metadata_digest_sha256"].(string); expected != "" && expected != metadataDigest {
		return nil, fmt.Errorf("metadata digest mismatch: got %s want %s", metadataDigest, expected)
	}
	digest, err := validateArtifactTiles(ctx, db, manifest.Schema)
	if err != nil {
		return nil, err
	}
	if manifest.Schema == MBTilesSchemaNormalized {
		if err := validateArtifactImages(ctx, db); err != nil {
			return nil, err
		}
	}
	if expected, _ := manifest.IndexConfig["tile_digest_sha256"].(string); expected != "" && expected != digest {
		return nil, fmt.Errorf("tile digest mismatch: got %s want %s", digest, expected)
	}
	return &manifest, nil
}

func validateArtifactImages(ctx context.Context, db *storage.DB) error {
	var callbackErr error
	var previous []byte
	_, err := db.ScanRowsFast("default", "images", func(row []any) bool {
		if len(row) != 2 {
			callbackErr = errors.New("image row has wrong arity")
			return false
		}
		id, ok1 := row[0].(string)
		if _, ok := row[1].([]byte); !ok || !ok1 {
			callbackErr = fmt.Errorf("image row has wrong types: %T %T", row[0], row[1])
			return false
		}
		key := storage.CanonicalIndexKey([]any{id})
		if bytes.Equal(previous, key) {
			callbackErr = fmt.Errorf("duplicate image key %q", id)
			return false
		}
		previous = append(previous[:0], key...)
		indexed, handled, lookupErr := db.PagedIndexRows("default", "images", "images_tile_id", []any{id})
		if lookupErr != nil || !handled || len(indexed) != 1 {
			callbackErr = fmt.Errorf("image index incomplete for %q", id)
			return false
		}
		if err := checkContext(ctx); err != nil {
			callbackErr = err
			return false
		}
		return true
	})
	if err != nil {
		return fmt.Errorf("validate images: %w", err)
	}
	if callbackErr != nil {
		return fmt.Errorf("validate images: %w", callbackErr)
	}
	return nil
}

func verifyArtifactChecksums(root string, manifest MBTilesArtifactManifest, manifestBytes []byte) error {
	b, err := os.ReadFile(filepath.Join(root, "checksums.sha256"))
	if err != nil {
		return fmt.Errorf("read checksums: %w", err)
	}
	listed := make(map[string]string)
	scanner := bufio.NewScanner(bytes.NewReader(b))
	for scanner.Scan() {
		var sum, name string
		if _, err := fmt.Sscanf(scanner.Text(), "%64s  %s", &sum, &name); err != nil || len(sum) != 64 {
			return fmt.Errorf("invalid checksum line %q", scanner.Text())
		}
		if filepath.Clean(name) != name || filepath.IsAbs(name) || name == "checksums.sha256" || name == "COMPLETE" {
			return fmt.Errorf("invalid checksum path %q", name)
		}
		listed[name] = sum
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	if got := checksumBytes(manifestBytes); listed["manifest.json"] != got {
		return fmt.Errorf("manifest checksum mismatch")
	}
	for name, expected := range listed {
		data, err := os.ReadFile(filepath.Join(root, name))
		if err != nil {
			return fmt.Errorf("checksum file %s: %w", name, err)
		}
		if checksumBytes(data) != expected {
			return fmt.Errorf("checksum mismatch for %s", name)
		}
		if name != "manifest.json" && manifest.Checksums[name] != expected {
			return fmt.Errorf("manifest does not agree with checksum file for %s", name)
		}
	}
	for name, expected := range manifest.Checksums {
		if listed[name] != expected {
			return fmt.Errorf("manifest checksum %s is missing or different", name)
		}
	}
	for _, name := range artifactDataFileNames(root) {
		if _, ok := listed[name]; !ok {
			return fmt.Errorf("artifact data file %s is not checksummed", name)
		}
	}
	return nil
}

func checksumBytes(data []byte) string { h := sha256.Sum256(data); return hex.EncodeToString(h[:]) }

func validateArtifactTable(ctx context.Context, db *storage.DB, want MBTilesArtifactTable) error {
	meta, ok, err := db.PagedIndexMetadata("default", want.Name)
	if err != nil || !ok || meta == nil {
		return fmt.Errorf("manifest table %s is missing", want.Name)
	}
	if len(meta.Cols) != len(want.Columns) {
		return fmt.Errorf("table %s column count mismatch", want.Name)
	}
	for i, col := range want.Columns {
		if meta.Cols[i].Name != col {
			return fmt.Errorf("table %s column %d mismatch", want.Name, i)
		}
	}
	for _, index := range want.Indexes {
		physical := meta.Indexes[index.Name]
		if physical == nil || physical.Unique != index.Unique || !sameStrings(physical.Columns, index.Columns) {
			return fmt.Errorf("table %s index %s mismatch", want.Name, index.Name)
		}
	}
	var count int64
	_, err = db.ScanRowsFast("default", want.Name, func([]any) bool { count++; return true })
	if err != nil {
		return fmt.Errorf("scan table %s: %w", want.Name, err)
	}
	if count != want.Rows {
		return fmt.Errorf("table %s row count %d want %d", want.Name, count, want.Rows)
	}
	return nil
}

func validateArtifactMetadata(ctx context.Context, db *storage.DB) (string, error) {
	h := sha256.New()
	var callbackErr error
	var previous []byte
	_, err := db.ScanRowsFast("default", "metadata", func(row []any) bool {
		if len(row) != 2 {
			callbackErr = errors.New("metadata row has wrong arity")
			return false
		}
		name, ok1 := row[0].(string)
		value, ok2 := row[1].(string)
		if !ok1 || !ok2 {
			callbackErr = errors.New("metadata row has wrong types")
			return false
		}
		key := storage.CanonicalIndexKey([]any{name})
		if bytes.Equal(previous, key) {
			callbackErr = fmt.Errorf("duplicate metadata key %q", name)
			return false
		}
		previous = append(previous[:0], key...)
		indexed, handled, lookupErr := db.PagedIndexRows("default", "metadata", "metadata_name", []any{name})
		if lookupErr != nil || !handled || len(indexed) != 1 {
			callbackErr = fmt.Errorf("metadata index incomplete for %q", name)
			return false
		}
		hashMetadata(h, name, value)
		return true
	})
	if err != nil {
		return "", fmt.Errorf("validate metadata: %w", err)
	}
	if callbackErr != nil {
		return "", fmt.Errorf("validate metadata: %w", callbackErr)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func validateArtifactTiles(ctx context.Context, db *storage.DB, schema MBTilesArtifactSchema) (string, error) {
	h := sha256.New()
	if schema == MBTilesSchemaFlat {
		var callbackErr error
		var previous []byte
		_, err := db.ScanRowsFast("default", "tiles", func(row []any) bool {
			if len(row) != 4 {
				callbackErr = errors.New("tile row has wrong arity")
				return false
			}
			z, ok1 := artifactInt(row[0])
			x, ok2 := artifactInt(row[1])
			y, ok3 := artifactInt(row[2])
			data, ok4 := row[3].([]byte)
			if !ok1 || !ok2 || !ok3 || !ok4 {
				callbackErr = errors.New("tile row has wrong types")
				return false
			}
			if err := validateTileCoordinate(z, x, y); err != nil {
				callbackErr = err
				return false
			}
			key := storage.CanonicalIndexKey([]any{z, x, y})
			if bytes.Equal(previous, key) {
				callbackErr = fmt.Errorf("duplicate tile key z=%d x=%d y=%d", z, x, y)
				return false
			}
			previous = append(previous[:0], key...)
			indexed, handled, lookupErr := db.PagedIndexRows("default", "tiles", "tiles_zxy", []any{z, x, y})
			if lookupErr != nil || !handled || len(indexed) != 1 {
				callbackErr = fmt.Errorf("tile index incomplete for z=%d x=%d y=%d", z, x, y)
				return false
			}
			hashTile(h, z, x, y, data)
			return true
		})
		if err != nil {
			return "", fmt.Errorf("validate tiles: %w", err)
		}
		if callbackErr != nil {
			return "", fmt.Errorf("validate tiles: %w", callbackErr)
		}
		return hex.EncodeToString(h.Sum(nil)), nil
	}
	var callbackErr error
	var previous []byte
	_, err := db.ScanRowsFast("default", "map", func(row []any) bool {
		if len(row) != 4 {
			callbackErr = errors.New("map row has wrong arity")
			return false
		}
		z, ok1 := artifactInt(row[0])
		x, ok2 := artifactInt(row[1])
		y, ok3 := artifactInt(row[2])
		id, ok4 := row[3].(string)
		if !ok1 || !ok2 || !ok3 || !ok4 {
			callbackErr = fmt.Errorf("map row has wrong types: %T %T %T %T", row[0], row[1], row[2], row[3])
			return false
		}
		if err := validateTileCoordinate(z, x, y); err != nil {
			callbackErr = err
			return false
		}
		key := storage.CanonicalIndexKey([]any{z, x, y})
		if bytes.Equal(previous, key) {
			callbackErr = fmt.Errorf("duplicate tile key z=%d x=%d y=%d", z, x, y)
			return false
		}
		previous = append(previous[:0], key...)
		indexed, handled, lookupErr := db.PagedIndexRows("default", "map", "map_zxy", []any{z, x, y})
		if lookupErr != nil || !handled || len(indexed) != 1 {
			callbackErr = fmt.Errorf("map index incomplete for z=%d x=%d y=%d", z, x, y)
			return false
		}
		images, handled, e := db.PagedIndexRows("default", "images", "images_tile_id", []any{id})
		if e != nil || !handled || len(images) != 1 {
			return false
		}
		data, ok := images[0][1].([]byte)
		if !ok {
			return false
		}
		hashTile(h, z, x, y, data)
		return true
	})
	if err != nil {
		return "", fmt.Errorf("validate normalized tiles: %w", err)
	}
	if callbackErr != nil {
		return "", fmt.Errorf("validate normalized tiles: %w", callbackErr)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func artifactInt(value any) (int, bool) {
	switch v := value.(type) {
	case int:
		return v, true
	case int64:
		return int(v), int64(int(v)) == v
	case int32:
		return int(v), true
	case float64:
		return int(v), v == float64(int(v))
	default:
		return 0, false
	}
}

func sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
