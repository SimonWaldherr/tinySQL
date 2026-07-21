package engine

import (
	"crypto/sha256"
	"encoding/binary"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// VectorCacheConfig controls the optional process-wide VEC_SEARCH result
// cache. The existing column/ANN caches remain active independently.
type VectorCacheConfig struct {
	// ResultCacheEntries bounds cached result sets. Zero disables the cache.
	ResultCacheEntries int
	// ResultCacheTTL expires cached result sets. A positive entry limit with a
	// non-positive TTL uses the conservative default of 30 seconds.
	ResultCacheTTL time.Duration
	// Analytics enables the bounded in-memory recent-query window.
	Analytics bool
	// AnalyticsWindow limits how long query events remain visible.
	AnalyticsWindow time.Duration
	// AnalyticsMaxEvents bounds retained query events independently of time.
	AnalyticsMaxEvents int
}

const (
	defaultVectorCacheTTL        = 30 * time.Second
	defaultVectorAnalyticsWindow = time.Minute
	defaultVectorAnalyticsEvents = 128
)

// DefaultVectorCacheConfig returns the conservative profile: caching and
// analytics are opt-in, while bounded values are ready for enabled features.
func DefaultVectorCacheConfig() VectorCacheConfig {
	return VectorCacheConfig{ResultCacheTTL: defaultVectorCacheTTL, AnalyticsWindow: defaultVectorAnalyticsWindow, AnalyticsMaxEvents: defaultVectorAnalyticsEvents}
}

// VectorCacheStats is a snapshot suitable for embedding applications and the
// server analytics endpoint. Query names exclude vector contents.
type VectorCacheStats struct {
	Enabled       bool               `json:"enabled"`
	Entries       int                `json:"entries"`
	Hits          uint64             `json:"hits"`
	Misses        uint64             `json:"misses"`
	Evictions     uint64             `json:"evictions"`
	ApproxBytes   int64              `json:"approx_bytes"`
	HeapAlloc     uint64             `json:"heap_alloc_bytes"`
	RecentQueries []VectorQueryEvent `json:"recent_queries,omitempty"`
}

type VectorQueryEvent struct {
	At       time.Time `json:"at"`
	Table    string    `json:"table"`
	Column   string    `json:"column"`
	Metric   string    `json:"metric"`
	Index    string    `json:"index"`
	K        int       `json:"k"`
	CacheHit bool      `json:"cache_hit"`
	// Duration covers the vector-search stage, including a cache lookup.
	Duration time.Duration `json:"duration"`
}

type vecQueryCacheKey struct {
	tenant, table, column, metric, index string
	version, k                           int
	query                                [32]byte
}
type vecQueryCacheEntry struct {
	rows    []vecScoredRow
	expires time.Time
}

var vecQueryCacheState = struct {
	sync.Mutex
	cfg       VectorCacheConfig
	entries   map[vecQueryCacheKey]vecQueryCacheEntry
	hits      uint64
	misses    uint64
	evictions uint64
	events    []VectorQueryEvent
	// cacheEnabled and analyticsEnabled mirror cfg.ResultCacheEntries>0 &&
	// cfg.ResultCacheTTL>0, and cfg.Analytics, respectively. They exist purely
	// as a lock-free fast path for the VEC_SEARCH hot path (every single
	// search call, regardless of table size or index mode, used to pay two
	// full Lock/Unlock round trips on this one global mutex just to learn
	// "disabled" — see vecQueryCacheEnabled and recordVecQuery below).
	// ConfigureVectorCache is the ONLY writer, and it stores both atomics
	// while still holding the mutex, in the same critical section as the cfg
	// write they mirror — so concurrent ConfigureVectorCache calls can never
	// leave the atomics reflecting an older cfg than the one currently
	// stored. Readers never treat these as a second source of truth for a
	// mutation: getVecQueryCache/putVecQueryCache/recordVecQuery's slow path
	// all re-check the authoritative, lock-protected cfg before touching
	// entries/events.
	cacheEnabled     atomic.Bool
	analyticsEnabled atomic.Bool
}{entries: make(map[vecQueryCacheKey]vecQueryCacheEntry)}

func ConfigureVectorCache(cfg VectorCacheConfig) {
	vecQueryCacheState.Lock()
	defer vecQueryCacheState.Unlock()
	if cfg.ResultCacheEntries < 0 {
		cfg.ResultCacheEntries = 0
	}
	if cfg.AnalyticsMaxEvents < 0 {
		cfg.AnalyticsMaxEvents = 0
	}
	if cfg.ResultCacheEntries > 0 && cfg.ResultCacheTTL <= 0 {
		cfg.ResultCacheTTL = defaultVectorCacheTTL
	}
	if cfg.AnalyticsWindow <= 0 {
		cfg.AnalyticsWindow = defaultVectorAnalyticsWindow
	}
	if cfg.Analytics && cfg.AnalyticsMaxEvents == 0 {
		cfg.AnalyticsMaxEvents = defaultVectorAnalyticsEvents
	}
	vecQueryCacheState.cfg = cfg
	vecQueryCacheState.hits = 0
	vecQueryCacheState.misses = 0
	vecQueryCacheState.evictions = 0
	if cfg.ResultCacheEntries == 0 || cfg.ResultCacheTTL <= 0 {
		vecQueryCacheState.entries = make(map[vecQueryCacheKey]vecQueryCacheEntry)
	}
	if !cfg.Analytics {
		vecQueryCacheState.events = nil
	}
	// Publish the fast-path flags last, still inside this critical section,
	// so they always end up consistent with the cfg this same call just set.
	vecQueryCacheState.cacheEnabled.Store(cfg.ResultCacheEntries > 0 && cfg.ResultCacheTTL > 0)
	vecQueryCacheState.analyticsEnabled.Store(cfg.Analytics)
}

func VectorCacheAnalytics() VectorCacheStats {
	vecQueryCacheState.Lock()
	defer vecQueryCacheState.Unlock()
	now := time.Now()
	pruneVecQueryCacheLocked(now)
	stats := VectorCacheStats{Enabled: vecQueryCacheState.cfg.ResultCacheEntries > 0 && vecQueryCacheState.cfg.ResultCacheTTL > 0, Entries: len(vecQueryCacheState.entries), Hits: vecQueryCacheState.hits, Misses: vecQueryCacheState.misses, Evictions: vecQueryCacheState.evictions}
	stats.ApproxBytes = int64(len(vecQueryCacheState.entries) * 128)
	for _, entry := range vecQueryCacheState.entries {
		stats.ApproxBytes += int64(len(entry.rows) * 16)
	}
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	stats.HeapAlloc = mem.HeapAlloc
	if vecQueryCacheState.cfg.Analytics {
		stats.RecentQueries = append([]VectorQueryEvent(nil), vecQueryCacheState.events...)
	}
	return stats
}

// vecQueryCacheEnabled reports whether the opt-in result cache is active. When
// it is off (the default), the search path skips hashing the query vector.
// Lock-free atomic read — called on every VEC_SEARCH invocation, so it must
// not contend with concurrent searches on other goroutines; see the
// cacheEnabled field comment on vecQueryCacheState for why this is safe
// without the mutex.
func vecQueryCacheEnabled() bool {
	return vecQueryCacheState.cacheEnabled.Load()
}

func vecQueryKey(tenant string, tableName, colName string, version int, a vecSearchArgs) vecQueryCacheKey {
	h := sha256.New()
	var bits [8]byte
	for _, v := range a.queryVec {
		binary.LittleEndian.PutUint64(bits[:], mathFloat64bits(v))
		_, _ = h.Write(bits[:])
	}
	var query [32]byte
	copy(query[:], h.Sum(nil))
	return vecQueryCacheKey{tenant: tenant, table: tableName, column: colName, version: version, metric: a.metric, index: a.indexMode, k: a.k, query: query}
}

func mathFloat64bits(v float64) uint64 { return math.Float64bits(v) }

func getVecQueryCache(key vecQueryCacheKey) ([]vecScoredRow, bool) {
	vecQueryCacheState.Lock()
	defer vecQueryCacheState.Unlock()
	if vecQueryCacheState.cfg.ResultCacheEntries <= 0 || vecQueryCacheState.cfg.ResultCacheTTL <= 0 {
		return nil, false
	}
	entry, ok := vecQueryCacheState.entries[key]
	if !ok || !time.Now().Before(entry.expires) {
		if ok {
			delete(vecQueryCacheState.entries, key)
		}
		vecQueryCacheState.misses++
		return nil, false
	}
	vecQueryCacheState.hits++
	return append([]vecScoredRow(nil), entry.rows...), true
}

func putVecQueryCache(key vecQueryCacheKey, rows []vecScoredRow) {
	vecQueryCacheState.Lock()
	defer vecQueryCacheState.Unlock()
	cfg := vecQueryCacheState.cfg
	if cfg.ResultCacheEntries <= 0 || cfg.ResultCacheTTL <= 0 {
		return
	}
	pruneVecQueryCacheLocked(time.Now())
	for len(vecQueryCacheState.entries) >= cfg.ResultCacheEntries {
		for old := range vecQueryCacheState.entries {
			delete(vecQueryCacheState.entries, old)
			vecQueryCacheState.evictions++
			break
		}
	}
	vecQueryCacheState.entries[key] = vecQueryCacheEntry{rows: append([]vecScoredRow(nil), rows...), expires: time.Now().Add(cfg.ResultCacheTTL)}
}

// purgeVecQueryCacheFor removes query-result cache entries made obsolete by a
// rolled-back DML statement. Normally table versions invalidate these entries;
// rollback restores the old version, so the explicit purge is required.
func purgeVecQueryCacheFor(tenant, table string) {
	vecQueryCacheState.Lock()
	for key := range vecQueryCacheState.entries {
		if key.tenant == tenant && key.table == table {
			delete(vecQueryCacheState.entries, key)
		}
	}
	vecQueryCacheState.Unlock()
}

func recordVecQuery(event VectorQueryEvent) {
	// Fast path: analytics is off by default, so the overwhelmingly common
	// call (every VEC_SEARCH invocation makes one) skips the mutex entirely.
	// See the analyticsEnabled field comment on vecQueryCacheState.
	if !vecQueryCacheState.analyticsEnabled.Load() {
		return
	}
	vecQueryCacheState.Lock()
	defer vecQueryCacheState.Unlock()
	// Re-check under the lock: cfg is the sole source of truth. A concurrent
	// ConfigureVectorCache call may have disabled analytics between the
	// atomic load above and acquiring the lock here; without this recheck an
	// in-flight call could resurrect vecQueryCacheState.events right after
	// ConfigureVectorCache had just cleared it to nil.
	if !vecQueryCacheState.cfg.Analytics {
		return
	}
	pruneVecQueryCacheLocked(event.At)
	max := vecQueryCacheState.cfg.AnalyticsMaxEvents
	if max <= 0 {
		max = 128
	}
	vecQueryCacheState.events = append(vecQueryCacheState.events, event)
	if len(vecQueryCacheState.events) > max {
		vecQueryCacheState.events = append([]VectorQueryEvent(nil), vecQueryCacheState.events[len(vecQueryCacheState.events)-max:]...)
	}
}

func pruneVecQueryCacheLocked(now time.Time) {
	for k, v := range vecQueryCacheState.entries {
		if !now.Before(v.expires) {
			delete(vecQueryCacheState.entries, k)
		}
	}
	if vecQueryCacheState.cfg.Analytics {
		cutoff := now.Add(-vecQueryCacheState.cfg.AnalyticsWindow)
		n := 0
		for _, e := range vecQueryCacheState.events {
			if !e.At.Before(cutoff) {
				vecQueryCacheState.events[n] = e
				n++
			}
		}
		vecQueryCacheState.events = vecQueryCacheState.events[:n]
	}
}
