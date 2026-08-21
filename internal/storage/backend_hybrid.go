package storage

import (
	"fmt"
	"log"
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

	// Track which tables have been modified in memory so Sync can flush them.
	dirty     map[string]map[string]bool // tenant → lower(name) → dirty
	dirtyLock sync.Mutex

	loadCount atomic.Int64

	// rejectedLogged holds the tenant:name keys already reported by
	// noteAdmissionReject. A table too big for the budget is refused on
	// every access, so logging per call would emit one line per query for
	// as long as the process runs; the operator needs the fact once, and
	// BackendStats.AdmissionRejects carries the ongoing count.
	rejectedLogged sync.Map
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
	// A pool eviction (triggered from within LoadTable/Get on an unrelated
	// table) must not silently drop a dirty table that was mutated in place
	// after being loaded — see the DB.Get doc comment on the query-scoped
	// lease. Save it first, going straight to disk (not through
	// HybridBackend.SaveTable, which would call pool.Put and deadlock: this
	// callback runs with bp.mu already held).
	pool.SetEvictionSaver(func(tenant, name string, t *Table) {
		if disk.IsDirty(tenant, name, t.Version) {
			if err := disk.SaveTable(tenant, t); err != nil {
				log.Printf("warning: flush-before-evict failed for %s/%s: %v", tenant, name, err)
			}
		}
	})

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

	// Cache in buffer pool (may trigger eviction of cold tables). A refusal
	// is not an error for this caller — the table lease is still valid and
	// is returned below — but it does mean this load will repeat on every
	// future access, so it must not vanish silently.
	if err := h.pool.Put(tn, lc, t); err != nil {
		h.noteAdmissionReject(tn, lc, EstimateTableSize(t), err)
	}

	return t, nil
}

// noteAdmissionReject reports the first time a table could not be admitted
// to the buffer pool. Everything after the first report for that table is
// left to BackendStats.AdmissionRejects / LargestRejectedBytes, which count
// every refusal without flooding the log.
func (h *HybridBackend) noteAdmissionReject(tenant, name string, size int64, cause error) {
	if _, loaded := h.rejectedLogged.LoadOrStore(tenant+":"+name, struct{}{}); loaded {
		return
	}
	limit := h.pool.policy.MaxMemoryBytes
	log.Printf("tinysql: table %s/%s (~%d bytes) does not fit the %s buffer pool budget of %d bytes and will be re-read from disk on every access; raise MaxMemoryBytes/max_memory_bytes above the table size to cache it (%v)",
		tenant, name, size, h.mode, limit, cause)
}

// SaveTable writes to disk and updates the cache.
func (h *HybridBackend) SaveTable(tenant string, t *Table) error {
	if err := h.disk.SaveTable(tenant, t); err != nil {
		return err
	}

	lc := strings.ToLower(t.Name)
	tn := strings.ToLower(tenant)

	// Update cache. Routed through the same once-per-table report as the
	// read path: an oversized table is refused on every save too, and a
	// write loop over it would otherwise log a line per statement.
	if err := h.pool.Put(tn, lc, t); err != nil {
		h.noteAdmissionReject(tn, lc, EstimateTableSize(t), err)
	}

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
	if h.disk.IsReadOnly() {
		return nil
	}
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

// Mode reports the configured hybrid storage mode.
func (h *HybridBackend) Mode() StorageMode { return h.mode }

// SetEncryptor forwards table-file encryption to the underlying DiskBackend.
func (h *HybridBackend) SetEncryptor(enc *Encryptor) {
	h.disk.SetEncryptor(enc)
}

// SetReadOnly forwards the serving-only contract to the durable backend.
func (h *HybridBackend) SetReadOnly(readOnly bool) {
	h.disk.SetReadOnly(readOnly)
}

// Stats returns combined disk and in-memory cache statistics.
func (h *HybridBackend) Stats() BackendStats {
	ds := h.disk.Stats()
	ps := h.pool.GetStats()

	hitRate := float64(0)
	if total := ps.CacheHits + ps.CacheMisses; total > 0 {
		hitRate = float64(ps.CacheHits) / float64(total)
	}

	return BackendStats{
		Mode:                 h.mode,
		TablesInMemory:       ps.TablesInMemory,
		TablesOnDisk:         ds.TablesOnDisk,
		MemoryUsedBytes:      ps.MemoryUsed,
		MemoryLimitBytes:     ps.MemoryLimit,
		DiskUsedBytes:        ds.DiskUsedBytes,
		CacheHitRate:         hitRate,
		SyncCount:            ds.SyncCount,
		LoadCount:            h.loadCount.Load(),
		EvictionCount:        ps.EvictionCount,
		AdmissionRejects:     ps.AdmissionRejects,
		LargestRejectedBytes: ps.LargestRejectedBytes,
	}
}

// Disk returns the underlying DiskBackend for advanced operations
// (e.g. migration, metadata access).
func (h *HybridBackend) Disk() *DiskBackend { return h.disk }

// IsDirty delegates to the underlying DiskBackend's version-based dirty
// check, satisfying the dirtyTracker interface DB.Sync uses for both
// db.tenants entries and PooledTables leases.
func (h *HybridBackend) IsDirty(tenant, name string, currentVersion int) bool {
	return h.disk.IsDirty(tenant, name, currentVersion)
}

// PooledTables returns every table currently resident in the buffer pool,
// satisfying the pooledTableSource interface. DB.Sync/DB.Close use this to
// find and flush a table that DB.Get loaded lazily for this evictable
// storage mode (ModeHybrid/ModeIndex) without registering it in DB.tenants.
func (h *HybridBackend) PooledTables() []PooledTableRef {
	entries := h.pool.Snapshot()
	out := make([]PooledTableRef, 0, len(entries))
	for _, e := range entries {
		out = append(out, PooledTableRef{Tenant: e.Tenant, Table: e.Table})
	}
	return out
}
