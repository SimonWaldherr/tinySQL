//go:build !js && !wasm && !baremetal

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
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

type artifactValidationFingerprint struct {
	files map[string]artifactValidationFile
}

type artifactValidationFile struct {
	size    int64
	modTime time.Time
}

var artifactValidationCache = struct {
	sync.Mutex
	entries map[string]artifactValidationFingerprint
}{entries: make(map[string]artifactValidationFingerprint)}

// Validation streams tables and indexes and therefore does not benefit from
// the larger interactive-reader cache. Keep first-open RSS bounded even for
// multi-gigabyte artifacts.
const artifactValidationMemoryBytes int64 = 32 << 20

func ValidateMBTilesArtifact(ctx context.Context, artifactPath string) (*MBTilesArtifactManifest, error) {
	return validateArtifact(ctx, artifactPath, true)
}

func OpenMBTilesArtifact(ctx context.Context, artifactPath string, maxMemoryBytes int64) (*storage.DB, *MBTilesArtifactManifest, error) {
	manifest, err := ValidateMBTilesArtifact(ctx, artifactPath)
	if err != nil {
		return nil, nil, err
	}
	if maxMemoryBytes <= 0 {
		maxMemoryBytes = defaultArtifactMaxMemoryBytes
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
	var fingerprint artifactValidationFingerprint
	if requireComplete {
		fingerprint, err = artifactFingerprint(root)
		if err != nil {
			return nil, err
		}
		// A strict validation is expensive by design (it streams every
		// checksummed byte and validates index reachability). Serialize and
		// share the first complete proof before doing either expensive phase.
		// A normal filesystem mutation changes size or mtime and invalidates
		// the entry; a new process always performs a fresh strict validation.
		artifactValidationCache.Lock()
		defer artifactValidationCache.Unlock()
		if cached, ok := artifactValidationCache.entries[root]; ok && sameArtifactFingerprint(cached, fingerprint) {
			return &manifest, nil
		}
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
	db, err := storage.OpenDB(storage.StorageConfig{Mode: storage.ModePagedIndex, Path: filepath.Join(root, "database"), MaxMemoryBytes: artifactValidationMemoryBytes, ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("open artifact database for validation: %w", err)
	}
	defer db.Close()
	tableRows := make(map[string]int64, len(manifest.Tables))
	for _, table := range manifest.Tables {
		if err := validateArtifactTable(ctx, db, table); err != nil {
			return nil, err
		}
		tableRows[table.Name] = table.Rows
	}
	metadataDigest, err := validateArtifactMetadata(ctx, db, tableRows["metadata"])
	if err != nil {
		return nil, err
	}
	if expected, _ := manifest.IndexConfig["metadata_digest_sha256"].(string); expected != "" && expected != metadataDigest {
		return nil, fmt.Errorf("metadata digest mismatch: got %s want %s", metadataDigest, expected)
	}
	digest, err := validateArtifactTiles(ctx, db, manifest.Schema, tableRows)
	if err != nil {
		return nil, err
	}
	if manifest.Schema == MBTilesSchemaNormalized {
		if err := validateArtifactImages(ctx, db, tableRows["images"]); err != nil {
			return nil, err
		}
	}
	if expected, _ := manifest.IndexConfig["tile_digest_sha256"].(string); expected != "" && expected != digest {
		return nil, fmt.Errorf("tile digest mismatch: got %s want %s", digest, expected)
	}
	if requireComplete {
		artifactValidationCache.entries[root] = fingerprint
	}
	return &manifest, nil
}

func artifactFingerprint(root string) (artifactValidationFingerprint, error) {
	names := append([]string{"COMPLETE", "manifest.json", "checksums.sha256"}, artifactDataFileNames(root)...)
	fingerprint := artifactValidationFingerprint{files: make(map[string]artifactValidationFile, len(names))}
	for _, name := range names {
		info, err := os.Stat(filepath.Join(root, name))
		if err != nil {
			return artifactValidationFingerprint{}, fmt.Errorf("stat artifact file %s: %w", name, err)
		}
		if !info.Mode().IsRegular() {
			return artifactValidationFingerprint{}, fmt.Errorf("artifact file %s is not regular", name)
		}
		fingerprint.files[name] = artifactValidationFile{size: info.Size(), modTime: info.ModTime()}
	}
	return fingerprint, nil
}

func sameArtifactFingerprint(a, b artifactValidationFingerprint) bool {
	if len(a.files) != len(b.files) {
		return false
	}
	for name, af := range a.files {
		bf, ok := b.files[name]
		if !ok || af.size != bf.size || !af.modTime.Equal(bf.modTime) {
			return false
		}
	}
	return true
}

// duplicateKeyCursor tracks the most recently seen canonical index key while
// scanning rows in key order, so a caller can detect a duplicate key without
// buffering every key seen so far. Rows must be scanned in strictly
// increasing key order, which every ScanRowsFast caller here already relies
// on for its row-count check.
type duplicateKeyCursor struct {
	previous []byte
}

// advance reports whether key duplicates the previous key, then records key
// as the new previous key when it does not. It performs no I/O.
func (c *duplicateKeyCursor) advance(key []byte) (duplicate bool) {
	if bytes.Equal(c.previous, key) {
		return true
	}
	c.previous = append(c.previous[:0], key...)
	return false
}

func validateArtifactImages(ctx context.Context, db *storage.DB, expectedRows int64) error {
	imageIndex, found, err := db.LocatePagedIndex("default", "images", "images_tile_id")
	if err != nil || !found {
		return fmt.Errorf("validate images: locate image index: found=%v err=%v", found, err)
	}
	var callbackErr error
	cursor := duplicateKeyCursor{}
	var count int64
	_, err = db.ScanRowsFast("default", "images", func(row []any) bool {
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
		if cursor.advance(key) {
			callbackErr = fmt.Errorf("duplicate image key %q", id)
			return false
		}
		count++
		indexed, lookupErr := imageIndex.ContainsUnique(key)
		if lookupErr != nil || !indexed {
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
	if count != expectedRows {
		return fmt.Errorf("validate images: row count %d want %d", count, expectedRows)
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
		if strings.ContainsRune(name, '\\') || path.Clean(name) != name || filepath.IsAbs(name) || name == "checksums.sha256" || name == "COMPLETE" {
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
		got, err := checksumFile(filepath.Join(root, name))
		if err != nil {
			return fmt.Errorf("checksum file %s: %w", name, err)
		}
		if got != expected {
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
	return nil
}

func validateArtifactMetadata(ctx context.Context, db *storage.DB, expectedRows int64) (string, error) {
	metadataIndex, found, err := db.LocatePagedIndex("default", "metadata", "metadata_name")
	if err != nil || !found {
		return "", fmt.Errorf("validate metadata: locate metadata index: found=%v err=%v", found, err)
	}
	h := sha256.New()
	var callbackErr error
	cursor := duplicateKeyCursor{}
	var count int64
	_, err = db.ScanRowsFast("default", "metadata", func(row []any) bool {
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
		if cursor.advance(key) {
			callbackErr = fmt.Errorf("duplicate metadata key %q", name)
			return false
		}
		count++
		indexed, lookupErr := metadataIndex.ContainsUnique(key)
		if lookupErr != nil || !indexed {
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
	if count != expectedRows {
		return "", fmt.Errorf("validate metadata: row count %d want %d", count, expectedRows)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func validateArtifactTiles(ctx context.Context, db *storage.DB, schema MBTilesArtifactSchema, expectedRows map[string]int64) (string, error) {
	h := sha256.New()
	if schema == MBTilesSchemaFlat {
		tileIndex, found, err := db.LocatePagedIndex("default", "tiles", "tiles_zxy")
		if err != nil || !found {
			return "", fmt.Errorf("validate tiles: locate tile index: found=%v err=%v", found, err)
		}
		var callbackErr error
		cursor := duplicateKeyCursor{}
		var count int64
		_, err = db.ScanRowsFast("default", "tiles", func(row []any) bool {
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
			if cursor.advance(key) {
				callbackErr = fmt.Errorf("duplicate tile key z=%d x=%d y=%d", z, x, y)
				return false
			}
			count++
			indexed, lookupErr := tileIndex.ContainsUnique(key)
			if lookupErr != nil || !indexed {
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
		if count != expectedRows["tiles"] {
			return "", fmt.Errorf("validate tiles: row count %d want %d", count, expectedRows["tiles"])
		}
		return hex.EncodeToString(h.Sum(nil)), nil
	}
	mapIndex, found, err := db.LocatePagedIndex("default", "map", "map_zxy")
	if err != nil || !found {
		return "", fmt.Errorf("validate normalized tiles: locate map index: found=%v err=%v", found, err)
	}
	imageIndex, found, err := db.LocatePagedIndex("default", "images", "images_tile_id")
	if err != nil || !found {
		return "", fmt.Errorf("validate normalized tiles: locate image index: found=%v err=%v", found, err)
	}
	var callbackErr error
	cursor := duplicateKeyCursor{}
	var count int64
	_, err = db.ScanRowsFast("default", "map", func(row []any) bool {
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
		if cursor.advance(key) {
			callbackErr = fmt.Errorf("duplicate tile key z=%d x=%d y=%d", z, x, y)
			return false
		}
		count++
		indexed, lookupErr := mapIndex.ContainsUnique(key)
		if lookupErr != nil || !indexed {
			callbackErr = fmt.Errorf("map index incomplete for z=%d x=%d y=%d", z, x, y)
			return false
		}
		var data []byte
		imageKey := storage.CanonicalIndexKey([]any{id})
		imageFound, lookupErr := imageIndex.LookupUniqueColumn(imageKey, 1, func(raw any) error {
			var ok bool
			data, ok = raw.([]byte)
			if !ok {
				return errors.New("image tile_data has wrong type")
			}
			return nil
		})
		if lookupErr != nil || !imageFound {
			callbackErr = fmt.Errorf("image index incomplete for tile_id %q", id)
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
	if count != expectedRows["map"] {
		return "", fmt.Errorf("validate normalized tiles: row count %d want %d", count, expectedRows["map"])
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
