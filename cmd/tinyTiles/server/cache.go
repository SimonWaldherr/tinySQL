package server

import (
	"container/list"
	"context"
	"sync"

	"github.com/Karte-Bayern/tinyTiles/offline"
	tiles "github.com/SimonWaldherr/tinySQL/tiles"
)

// A Server owns one immutable artifact revision. XYZ and sync requests share
// the same TMS key space and immutable payloads; no cache survives replacement
// of that Server. The byte budget covers payloads/checksums, and the entry cap
// also bounds bookkeeping for tiny tiles.
const tileCacheMaxEntries = 4096

type tilePayload struct {
	data     []byte
	checksum string
}
type tileCacheEntry struct {
	key     tiles.Key
	payload tilePayload
}
type tileCache struct {
	mu           sync.Mutex
	budget, used int64
	entries      map[tiles.Key]*list.Element
	order        list.List
}

func newTileCache(budget int64) *tileCache {
	if budget == 0 {
		return nil
	}
	return &tileCache{budget: budget, entries: make(map[tiles.Key]*list.Element)}
}
func (c *tileCache) get(key tiles.Key) (tilePayload, bool) {
	if c == nil {
		return tilePayload{}, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if elem := c.entries[key]; elem != nil {
		c.order.MoveToFront(elem)
		return elem.Value.(tileCacheEntry).payload, true
	}
	return tilePayload{}, false
}
func (c *tileCache) put(key tiles.Key, payload tilePayload) {
	if c == nil {
		return
	}
	size := int64(len(payload.data) + len(payload.checksum))
	if size > c.budget {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if elem := c.entries[key]; elem != nil {
		c.order.MoveToFront(elem)
		return
	}
	for c.used > c.budget-size || len(c.entries) >= tileCacheMaxEntries {
		oldest := c.order.Back()
		entry := oldest.Value.(tileCacheEntry)
		c.used -= int64(len(entry.payload.data) + len(entry.payload.checksum))
		delete(c.entries, entry.key)
		c.order.Remove(oldest)
	}
	c.entries[key] = c.order.PushFront(tileCacheEntry{key: key, payload: payload})
	c.used += size
}

func (s *Server) lookupTile(ctx context.Context, key tiles.Key) (tilePayload, bool, error) {
	if err := ctx.Err(); err != nil {
		return tilePayload{}, false, err
	}
	if payload, ok := s.tileCache.get(key); ok {
		return payload, true, nil
	}
	tile, found, err := s.dataset.LookupTMS(ctx, key)
	if err != nil || !found {
		return tilePayload{}, found, err
	}
	// Reader.Lookup transfers ownership of the payload. The HTTP path only
	// reads these bytes, including while another request evicts the entry.
	payload := tilePayload{data: tile.Data, checksum: offline.Checksum(tile.Data)}
	s.tileCache.put(key, payload)
	return payload, true, nil
}
