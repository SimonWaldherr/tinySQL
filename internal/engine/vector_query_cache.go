package engine

import (
	"crypto/sha256"
	"encoding/binary"
	"math"
	"runtime"
	"sync"
	"time"
)

// VectorCacheConfig controls the optional process-wide VEC_SEARCH result
// cache. The existing column/ANN caches remain active independently.
type VectorCacheConfig struct {
	ResultCacheEntries int
	ResultCacheTTL     time.Duration
	Analytics          bool
	AnalyticsWindow    time.Duration
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
	At       time.Time     `json:"at"`
	Table    string        `json:"table"`
	Column   string        `json:"column"`
	Metric   string        `json:"metric"`
	Index    string        `json:"index"`
	K        int           `json:"k"`
	CacheHit bool          `json:"cache_hit"`
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

func recordVecQuery(event VectorQueryEvent) {
	vecQueryCacheState.Lock()
	defer vecQueryCacheState.Unlock()
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
