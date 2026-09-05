//go:build !js || !wasm

package offline

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
)

const (
	diskTileMagic      = "TTIL\x01"
	maxTileHeaderBytes = 64 << 10
)

// FileStore is a native, durable offline cache. It uses content-safe hashed
// path components and atomic rename publication, so user-controlled dataset
// and revision names never become filesystem paths.
type FileStore struct {
	root        string
	maxTileSize int64
	mu          sync.RWMutex
}

func NewFileStore(root string) (*FileStore, error) {
	root = filepath.Clean(root)
	if root == "." || root == "" {
		return nil, errors.New("offline cache root must not be empty")
	}
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("resolve offline cache root: %w", err)
	}
	if isFilesystemRoot(absRoot) {
		return nil, errors.New("offline cache root must not be the filesystem root")
	}
	for _, directory := range []string{filepath.Join(absRoot, "manifests"), filepath.Join(absRoot, "tiles")} {
		if err := os.MkdirAll(directory, 0o755); err != nil {
			return nil, fmt.Errorf("create offline cache directory: %w", err)
		}
	}
	return &FileStore{root: absRoot, maxTileSize: defaultMaxTileBytes}, nil
}

func isFilesystemRoot(path string) bool {
	return filepath.Clean(path) == filepath.VolumeName(path)+string(os.PathSeparator)
}

// SetMaxTileSize adjusts the defensive size bound used when reading cache
// files. It must be positive and is intended for deployments with larger
// raster tiles.
func (s *FileStore) SetMaxTileSize(max int64) error {
	if max <= 0 {
		return errors.New("maximum tile size must be positive")
	}
	if _, err := maxTileRecordSize(max); err != nil {
		return err
	}
	s.mu.Lock()
	s.maxTileSize = max
	s.mu.Unlock()
	return nil
}

func (s *FileStore) GetManifest(ctx context.Context, dataset string) (Manifest, bool, error) {
	if err := ctx.Err(); err != nil {
		return Manifest{}, false, err
	}
	if err := validateIdentifier("dataset", dataset, 256); err != nil {
		return Manifest{}, false, err
	}
	s.mu.RLock()
	path := s.manifestPath(dataset)
	s.mu.RUnlock()
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return Manifest{}, false, nil
	}
	if err != nil {
		return Manifest{}, false, fmt.Errorf("read cached manifest: %w", err)
	}
	var manifest Manifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return Manifest{}, false, fmt.Errorf("decode cached manifest: %w", err)
	}
	if err := manifest.Validate(); err != nil {
		return Manifest{}, false, fmt.Errorf("validate cached manifest: %w", err)
	}
	if manifest.Dataset != dataset {
		return Manifest{}, false, errors.New("cached manifest dataset does not match its key")
	}
	return manifest, true, nil
}

func (s *FileStore) PutManifest(ctx context.Context, manifest Manifest) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := manifest.Validate(); err != nil {
		return err
	}
	data, err := json.Marshal(manifest)
	if err != nil {
		return fmt.Errorf("encode cached manifest: %w", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return writeAtomicFile(s.manifestPath(manifest.Dataset), data, 0o644)
}

func (s *FileStore) GetTile(ctx context.Context, dataset, revision string, key TileKey) (Tile, bool, error) {
	if err := ctx.Err(); err != nil {
		return Tile{}, false, err
	}
	if err := validateCacheKey(dataset, revision, key); err != nil {
		return Tile{}, false, err
	}
	s.mu.RLock()
	path := s.tilePath(dataset, revision, key)
	max := s.maxTileSize
	s.mu.RUnlock()
	info, err := os.Stat(path)
	if os.IsNotExist(err) {
		return Tile{}, false, nil
	}
	if err != nil {
		return Tile{}, false, fmt.Errorf("stat cached tile: %w", err)
	}
	recordLimit, err := maxTileRecordSize(max)
	if err != nil {
		return Tile{}, false, err
	}
	if info.Size() > recordLimit {
		return Tile{}, false, fmt.Errorf("cached tile exceeds configured maximum %d bytes", max)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return Tile{}, false, fmt.Errorf("read cached tile: %w", err)
	}
	tile, err := decodeDiskTile(data, max)
	if err != nil {
		return Tile{}, false, fmt.Errorf("decode cached tile: %w", err)
	}
	if err := verifyTile(tile); err != nil {
		return Tile{}, false, fmt.Errorf("verify cached tile: %w", err)
	}
	return tile, true, nil
}

func maxTileRecordSize(maxTileSize int64) (int64, error) {
	overhead := int64(maxTileHeaderBytes) + int64(len(diskTileMagic)) + 4
	const maxInt64 = int64(^uint64(0) >> 1)
	if maxTileSize > maxInt64-overhead {
		return 0, errors.New("maximum tile size is too large")
	}
	return maxTileSize + overhead, nil
}

func (s *FileStore) PutTile(ctx context.Context, dataset, revision string, key TileKey, tile Tile) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateCacheKey(dataset, revision, key); err != nil {
		return err
	}
	if err := verifyTile(tile); err != nil {
		return err
	}
	s.mu.RLock()
	max := s.maxTileSize
	path := s.tilePath(dataset, revision, key)
	s.mu.RUnlock()
	if int64(len(tile.Data)) > max {
		return fmt.Errorf("tile is %d bytes, above configured maximum %d", len(tile.Data), max)
	}
	encoded, err := encodeDiskTile(tile)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return writeAtomicFile(path, encoded, 0o644)
}

func (s *FileStore) DeleteRevision(ctx context.Context, dataset, revision string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateIdentifier("dataset", dataset, 256); err != nil {
		return err
	}
	if err := validateIdentifier("revision", revision, 512); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	path := filepath.Join(s.root, "tiles", cacheComponent(dataset), cacheComponent(revision))
	if err := os.RemoveAll(path); err != nil {
		return fmt.Errorf("remove cached revision: %w", err)
	}
	return nil
}

func (s *FileStore) manifestPath(dataset string) string {
	return filepath.Join(s.root, "manifests", cacheComponent(dataset)+".json")
}

func (s *FileStore) tilePath(dataset, revision string, key TileKey) string {
	return filepath.Join(s.root, "tiles", cacheComponent(dataset), cacheComponent(revision), strconv.Itoa(key.Z), strconv.Itoa(key.X), strconv.Itoa(key.Y)+".tile")
}

type diskTileHeader struct {
	ContentType     string `json:"content_type,omitempty"`
	ContentEncoding string `json:"content_encoding,omitempty"`
	ETag            string `json:"etag,omitempty"`
	Checksum        string `json:"checksum_sha256,omitempty"`
}

func encodeDiskTile(tile Tile) ([]byte, error) {
	header, err := json.Marshal(diskTileHeader{ContentType: tile.ContentType, ContentEncoding: tile.ContentEncoding, ETag: tile.ETag, Checksum: tile.Checksum})
	if err != nil {
		return nil, err
	}
	if len(header) > maxTileHeaderBytes {
		return nil, errors.New("tile header exceeds maximum size")
	}
	encoded := make([]byte, len(diskTileMagic)+4+len(header)+len(tile.Data))
	copy(encoded, diskTileMagic)
	binary.BigEndian.PutUint32(encoded[len(diskTileMagic):], uint32(len(header)))
	copy(encoded[len(diskTileMagic)+4:], header)
	copy(encoded[len(diskTileMagic)+4+len(header):], tile.Data)
	return encoded, nil
}

func decodeDiskTile(encoded []byte, maxTileSize int64) (Tile, error) {
	if len(encoded) < len(diskTileMagic)+4 || !bytes.Equal(encoded[:len(diskTileMagic)], []byte(diskTileMagic)) {
		return Tile{}, errors.New("invalid tile record magic")
	}
	headerLength := int(binary.BigEndian.Uint32(encoded[len(diskTileMagic):]))
	if headerLength < 0 || headerLength > maxTileHeaderBytes || len(encoded) < len(diskTileMagic)+4+headerLength {
		return Tile{}, errors.New("invalid tile record header length")
	}
	payload := encoded[len(diskTileMagic)+4+headerLength:]
	if int64(len(payload)) > maxTileSize {
		return Tile{}, errors.New("tile record payload exceeds maximum size")
	}
	var header diskTileHeader
	if err := json.Unmarshal(encoded[len(diskTileMagic)+4:len(diskTileMagic)+4+headerLength], &header); err != nil {
		return Tile{}, err
	}
	return Tile{Data: append([]byte(nil), payload...), ContentType: header.ContentType, ContentEncoding: header.ContentEncoding, ETag: header.ETag, Checksum: header.Checksum}, nil
}

func writeAtomicFile(path string, data []byte, mode os.FileMode) error {
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return err
	}
	temporary, err := os.CreateTemp(directory, ".tinytiles-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	removeTemporary := true
	defer func() {
		if removeTemporary {
			_ = os.Remove(temporaryPath)
		}
	}()
	if err := temporary.Chmod(mode); err != nil {
		_ = temporary.Close()
		return err
	}
	if _, err := temporary.Write(data); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return err
	}
	removeTemporary = false
	if directoryHandle, err := os.Open(directory); err == nil {
		_ = directoryHandle.Sync()
		_ = directoryHandle.Close()
	}
	return nil
}
