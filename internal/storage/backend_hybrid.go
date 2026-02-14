package storage

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
)

// ───────────────────────────────────────────────────────────────────────────
// HybridBackend – LRU memory cache over DiskBackend
// ───────────────────────────────────────────────────────────────────────────

// HybridBackend keeps frequently-accessed tables in an LRU memory cache
// backed by individual table files on disk. When the cache exceeds the
// configured memory limit, cold tables are evicted. This gives near in-
// memory performance for the working set while allowing the full database
// to exceed available RAM.
//
// For ModeIndex, the same structure is used with a much smaller memory
// limit and aggressive eviction – essentially only table schemas (loaded
// as metadata from the manifest) stay resident.
type HybridBackend struct {
	disk *DiskBackend
	pool *BufferPool
	mode StorageMode // ModeHybrid or ModeIndex
	mu   sync.RWMutex

	// Track which tables have been modified in memory so Sync can flush them.
	dirty     map[string]map[string]bool // tenant → lower(name) → dirty
	dirtyLock sync.Mutex

	loadCount     atomic.Int64
	evictionCount atomic.Int64
}

// NewHybridBackend creates a HybridBackend.
//   - dir: directory for table files
//   - maxMemoryBytes: memory limit (0 = 256 MB default)
//   - compress: gzip table files
//   - mode: ModeHybrid or ModeIndex
func NewHybridBackend(dir string, maxMemoryBytes int64, compress bool, mode StorageMode) (*HybridBackend, error) {
	disk, err := NewDiskBackend(dir, compress)
	if err != nil {
		return nil, err
	}

	if maxMemoryBytes <= 0 {
		switch mode {
		case ModeIndex:
			maxMemoryBytes = 64 * 1024 * 1024 // 64 MB
		default:
			maxMemoryBytes = 256 * 1024 * 1024 // 256 MB
		}
	}

	policy := &MemoryPolicy{
		MaxMemoryBytes:      maxMemoryBytes,
		Strategy:            StrategyLRU,
		EvictionThreshold:   0.85,
		EnableEviction:      true,
		EvictionBatchSize:   5,
		TrackAccessPatterns: true,
	}
	pool := NewBufferPool(policy)

	return &HybridBackend{
		disk:  disk,
		pool:  pool,
		mode:  mode,
		dirty: make(map[string]map[string]bool),
	}, nil
}

// ──── StorageBackend interface ────────────────────────────────────────────

// LoadTable tries the in-memory cache first, then loads from disk.
func (h *HybridBackend) LoadTable(tenant, name string) (*Table, error) {
	lc := strings.ToLower(name)
	tn := strings.ToLower(tenant)

	// Check buffer pool cache
	if t, ok := h.pool.Get(tn, lc); ok {
		return t, nil
	}

	// Load from disk
	t, err := h.disk.LoadTable(tenant, name)
	if err != nil {
		return nil, err
	}
	if t == nil {
		return nil, nil
	}

	h.loadCount.Add(1)

	// Cache in buffer pool (may trigger eviction of cold tables)
	if err := h.pool.Put(tn, lc, t); err != nil {
		// If we can't cache (e.g. memory exceeded without eviction), still return
		// the table. It just won't be cached.
		_ = err
	}

	return t, nil
}

// SaveTable writes to disk and updates the cache.
func (h *HybridBackend) SaveTable(tenant string, t *Table) error {
	if err := h.disk.SaveTable(tenant, t); err != nil {
		return err
	}

	lc := strings.ToLower(t.Name)
	tn := strings.ToLower(tenant)

	// Update cache
	_ = h.pool.Put(tn, lc, t)

	// Clear dirty flag
	h.dirtyLock.Lock()
	if dm := h.dirty[tn]; dm != nil {
		delete(dm, lc)
	}
	h.dirtyLock.Unlock()

	return nil
}

// DeleteTable removes from both disk and cache.
func (h *HybridBackend) DeleteTable(tenant, name string) error {
	lc := strings.ToLower(name)
	tn := strings.ToLower(tenant)

	h.pool.Remove(tn, lc)

	h.dirtyLock.Lock()
	if dm := h.dirty[tn]; dm != nil {
		delete(dm, lc)
	}
	h.dirtyLock.Unlock()

	return h.disk.DeleteTable(tenant, name)
}

// ListTableNames returns names from the disk manifest (authoritative source).
func (h *HybridBackend) ListTableNames(tenant string) ([]string, error) {
	return h.disk.ListTableNames(tenant)
}

// TableExists checks the disk manifest.
func (h *HybridBackend) TableExists(tenant, name string) bool {
	return h.disk.TableExists(tenant, name)
}

// MarkDirty records that a table has been modified in memory and needs to be
// flushed on the next Sync. Called by DB after mutations.
func (h *HybridBackend) MarkDirty(tenant, name string) {
	tn := strings.ToLower(tenant)
	lc := strings.ToLower(name)
	h.dirtyLock.Lock()
	if h.dirty[tn] == nil {
		h.dirty[tn] = make(map[string]bool)
	}
	h.dirty[tn][lc] = true
	h.dirtyLock.Unlock()
}

// Sync writes all dirty in-memory tables to disk.
func (h *HybridBackend) Sync() error {
	h.dirtyLock.Lock()
	snapshot := make(map[string]map[string]bool)
	for tn, dm := range h.dirty {
		snapshot[tn] = make(map[string]bool, len(dm))
		for name := range dm {
			snapshot[tn][name] = true
		}
	}
	h.dirtyLock.Unlock()

	for tn, dm := range snapshot {
		for name := range dm {
			t, ok := h.pool.Get(tn, name)
			if !ok {
				continue // evicted, already on disk via earlier save
			}
			if err := h.disk.SaveTable(tn, t); err != nil {
				return fmt.Errorf("hybrid sync %s/%s: %w", tn, name, err)
			}
		}
	}

	// Clear dirty set for flushed tables
	h.dirtyLock.Lock()
	for tn, dm := range snapshot {
		if current := h.dirty[tn]; current != nil {
			for name := range dm {
				delete(current, name)
			}
		}
	}
	h.dirtyLock.Unlock()

	return h.disk.Sync()
}

// Close flushes dirty tables and closes the disk backend.
func (h *HybridBackend) Close() error {
	if err := h.Sync(); err != nil {
		return err
	}
	return h.disk.Close()
}

func (h *HybridBackend) Mode() StorageMode { return h.mode }

func (h *HybridBackend) Stats() BackendStats {
	ds := h.disk.Stats()
	ps := h.pool.GetStats()

	hitRate := float64(0)
	if total := ps.CacheHits + ps.CacheMisses; total > 0 {
		hitRate = float64(ps.CacheHits) / float64(total)
	}

	return BackendStats{
		Mode:             h.mode,
		TablesInMemory:   ps.TablesInMemory,
		TablesOnDisk:     ds.TablesOnDisk,
		MemoryUsedBytes:  ps.MemoryUsed,
		MemoryLimitBytes: ps.MemoryLimit,
		DiskUsedBytes:    ds.DiskUsedBytes,
		CacheHitRate:     hitRate,
		SyncCount:        ds.SyncCount,
		LoadCount:        h.loadCount.Load(),
		EvictionCount:    ps.EvictionCount,
	}
}

// Disk returns the underlying DiskBackend for advanced operations
// (e.g. migration, metadata access).
func (h *HybridBackend) Disk() *DiskBackend { return h.disk }
