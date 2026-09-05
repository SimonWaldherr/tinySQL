package offline

import (
	"context"
	"sync"
)

// MemoryStore is a race-safe reference store used by tests, demos and short
// lived command-line clients. It copies tile payloads at the public boundary.
type MemoryStore struct {
	mu        sync.RWMutex
	manifests map[string]Manifest
	tiles     map[string]Tile
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{manifests: make(map[string]Manifest), tiles: make(map[string]Tile)}
}

func (s *MemoryStore) GetManifest(ctx context.Context, dataset string) (Manifest, bool, error) {
	if err := ctx.Err(); err != nil {
		return Manifest{}, false, err
	}
	if err := validateIdentifier("dataset", dataset, 256); err != nil {
		return Manifest{}, false, err
	}
	s.mu.RLock()
	manifest, found := s.manifests[dataset]
	s.mu.RUnlock()
	return manifest, found, nil
}

func (s *MemoryStore) PutManifest(ctx context.Context, manifest Manifest) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := manifest.Validate(); err != nil {
		return err
	}
	s.mu.Lock()
	s.manifests[manifest.Dataset] = manifest
	s.mu.Unlock()
	return nil
}

func (s *MemoryStore) GetTile(ctx context.Context, dataset, revision string, key TileKey) (Tile, bool, error) {
	if err := ctx.Err(); err != nil {
		return Tile{}, false, err
	}
	if err := validateCacheKey(dataset, revision, key); err != nil {
		return Tile{}, false, err
	}
	s.mu.RLock()
	tile, found := s.tiles[memoryTileKey(dataset, revision, key)]
	s.mu.RUnlock()
	return tile.Clone(), found, nil
}

func (s *MemoryStore) PutTile(ctx context.Context, dataset, revision string, key TileKey, tile Tile) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateCacheKey(dataset, revision, key); err != nil {
		return err
	}
	if err := tile.Validate(); err != nil {
		return err
	}
	s.mu.Lock()
	s.tiles[memoryTileKey(dataset, revision, key)] = tile.Clone()
	s.mu.Unlock()
	return nil
}

func (s *MemoryStore) DeleteRevision(ctx context.Context, dataset, revision string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateIdentifier("dataset", dataset, 256); err != nil {
		return err
	}
	if err := validateIdentifier("revision", revision, 512); err != nil {
		return err
	}
	prefix := dataset + "\x00" + revision + "\x00"
	s.mu.Lock()
	for key := range s.tiles {
		if len(key) >= len(prefix) && key[:len(prefix)] == prefix {
			delete(s.tiles, key)
		}
	}
	s.mu.Unlock()
	return nil
}

func validateCacheKey(dataset, revision string, key TileKey) error {
	if err := validateIdentifier("dataset", dataset, 256); err != nil {
		return err
	}
	if err := validateIdentifier("revision", revision, 512); err != nil {
		return err
	}
	return key.Validate()
}

func memoryTileKey(dataset, revision string, key TileKey) string {
	return dataset + "\x00" + revision + "\x00" + key.String()
}
