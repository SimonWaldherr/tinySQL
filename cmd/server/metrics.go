package main

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/engine"
	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

type requestKey struct {
	Protocol string
	Route    string
	Method   string
	Status   int
}

type durationKey struct {
	Protocol string
	Route    string
	Method   string
}

type durationHistogram struct {
	bounds []float64
	bins   []uint64
	sum    float64
	count  uint64
}

type metricsRegistry struct {
	mu           sync.Mutex
	requests     map[requestKey]uint64
	durations    map[durationKey]*durationHistogram
	totalByProto map[string]uint64

	// backendStatsSource, when set, supplies storage.BackendStats for the
	// tinysql_memory_used_bytes/etc. gauges emitted by PrometheusText. Nil
	// (the zero value) disables that section, e.g. before a DB is attached.
	backendStatsSource func() storage.BackendStats

	// vectorCacheMetricsEnabled gates the tinysql_vector_cache_* section of
	// PrometheusText. This mirrors cmd/tinysqld's -analytics opt-in: a scrape
	// should not pay for engine.VectorCacheAnalytics() (which reads
	// runtime.MemStats) unless the operator explicitly enabled analytics.
	vectorCacheMetricsEnabled bool
}

func newMetricsRegistry() *metricsRegistry {
	return &metricsRegistry{
		requests:     make(map[requestKey]uint64),
		durations:    make(map[durationKey]*durationHistogram),
		totalByProto: make(map[string]uint64),
	}
}

// SetBackendStatsSource wires a storage backend stats provider into the
// registry so PrometheusText can emit tinysql_memory_used_bytes and related
// gauges/counters. Pass nil to disable that section.
func (m *metricsRegistry) SetBackendStatsSource(fn func() storage.BackendStats) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.backendStatsSource = fn
}

// SetVectorCacheMetricsEnabled gates whether PrometheusText emits
// tinysql_vector_cache_* metrics (see vectorCacheMetricsEnabled).
func (m *metricsRegistry) SetVectorCacheMetricsEnabled(enabled bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.vectorCacheMetricsEnabled = enabled
}

func defaultDurationBounds() []float64 {
	return []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30}
}

func (m *metricsRegistry) getOrCreateDurationHistogram(k durationKey) *durationHistogram {
	h, ok := m.durations[k]
	if ok {
		return h
	}
	bounds := defaultDurationBounds()
	h = &durationHistogram{
		bounds: append([]float64(nil), bounds...),
		bins:   make([]uint64, len(bounds)+1), // +Inf bucket is last
	}
	m.durations[k] = h
	return h
}

func (h *durationHistogram) observe(d time.Duration) {
	v := d.Seconds()
	h.sum += v
	h.count++
	for i, b := range h.bounds {
		if v <= b {
			h.bins[i]++
			return
		}
	}
	h.bins[len(h.bins)-1]++
}

func (m *metricsRegistry) Observe(protocol, route, method string, status int, d time.Duration) {
	if protocol == "" {
		protocol = "unknown"
	}
	if route == "" {
		route = "unknown"
	}
	if method == "" {
		method = "UNKNOWN"
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	rk := requestKey{Protocol: protocol, Route: route, Method: method, Status: status}
	m.requests[rk]++
	m.totalByProto[protocol]++

	dk := durationKey{Protocol: protocol, Route: route, Method: method}
	m.getOrCreateDurationHistogram(dk).observe(d)
}

func (m *metricsRegistry) TotalRequestsByProtocol() map[string]uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()

	out := make(map[string]uint64, len(m.totalByProto))
	for k, v := range m.totalByProto {
		out[k] = v
	}
	return out
}

func prometheusEscapeLabel(v string) string {
	v = strings.ReplaceAll(v, "\\", "\\\\")
	v = strings.ReplaceAll(v, "\n", "\\n")
	v = strings.ReplaceAll(v, "\"", "\\\"")
	return v
}

func (m *metricsRegistry) PrometheusText() string {
	m.mu.Lock()
	defer m.mu.Unlock()

	var b strings.Builder

	b.WriteString("# HELP tinysql_requests_total Total number of requests handled by tinySQL server.\n")
	b.WriteString("# TYPE tinysql_requests_total counter\n")

	requestKeys := make([]requestKey, 0, len(m.requests))
	for k := range m.requests {
		requestKeys = append(requestKeys, k)
	}
	sort.Slice(requestKeys, func(i, j int) bool {
		a := requestKeys[i]
		c := requestKeys[j]
		if a.Protocol != c.Protocol {
			return a.Protocol < c.Protocol
		}
		if a.Route != c.Route {
			return a.Route < c.Route
		}
		if a.Method != c.Method {
			return a.Method < c.Method
		}
		return a.Status < c.Status
	})

	for _, k := range requestKeys {
		fmt.Fprintf(&b,
			"tinysql_requests_total{protocol=\"%s\",route=\"%s\",method=\"%s\",status=\"%d\"} %d\n",
			prometheusEscapeLabel(k.Protocol),
			prometheusEscapeLabel(k.Route),
			prometheusEscapeLabel(k.Method),
			k.Status,
			m.requests[k],
		)
	}

	b.WriteString("# HELP tinysql_request_duration_seconds Request duration in seconds.\n")
	b.WriteString("# TYPE tinysql_request_duration_seconds histogram\n")

	durationKeys := make([]durationKey, 0, len(m.durations))
	for k := range m.durations {
		durationKeys = append(durationKeys, k)
	}
	sort.Slice(durationKeys, func(i, j int) bool {
		a := durationKeys[i]
		c := durationKeys[j]
		if a.Protocol != c.Protocol {
			return a.Protocol < c.Protocol
		}
		if a.Route != c.Route {
			return a.Route < c.Route
		}
		return a.Method < c.Method
	})

	for _, k := range durationKeys {
		h := m.durations[k]
		cum := uint64(0)
		for i, upper := range h.bounds {
			cum += h.bins[i]
			fmt.Fprintf(&b,
				"tinysql_request_duration_seconds_bucket{protocol=\"%s\",route=\"%s\",method=\"%s\",le=\"%s\"} %d\n",
				prometheusEscapeLabel(k.Protocol),
				prometheusEscapeLabel(k.Route),
				prometheusEscapeLabel(k.Method),
				strconv.FormatFloat(upper, 'f', -1, 64),
				cum,
			)
		}
		cum += h.bins[len(h.bins)-1]
		fmt.Fprintf(&b,
			"tinysql_request_duration_seconds_bucket{protocol=\"%s\",route=\"%s\",method=\"%s\",le=\"+Inf\"} %d\n",
			prometheusEscapeLabel(k.Protocol),
			prometheusEscapeLabel(k.Route),
			prometheusEscapeLabel(k.Method),
			cum,
		)
		fmt.Fprintf(&b,
			"tinysql_request_duration_seconds_sum{protocol=\"%s\",route=\"%s\",method=\"%s\"} %.9f\n",
			prometheusEscapeLabel(k.Protocol),
			prometheusEscapeLabel(k.Route),
			prometheusEscapeLabel(k.Method),
			h.sum,
		)
		fmt.Fprintf(&b,
			"tinysql_request_duration_seconds_count{protocol=\"%s\",route=\"%s\",method=\"%s\"} %d\n",
			prometheusEscapeLabel(k.Protocol),
			prometheusEscapeLabel(k.Route),
			prometheusEscapeLabel(k.Method),
			h.count,
		)
	}

	if m.backendStatsSource != nil {
		stats := m.backendStatsSource()

		b.WriteString("# HELP tinysql_memory_used_bytes Storage backend memory usage in bytes.\n")
		b.WriteString("# TYPE tinysql_memory_used_bytes gauge\n")
		fmt.Fprintf(&b, "tinysql_memory_used_bytes %d\n", stats.MemoryUsedBytes)

		b.WriteString("# HELP tinysql_memory_limit_bytes Storage backend memory limit in bytes (0 = unlimited).\n")
		b.WriteString("# TYPE tinysql_memory_limit_bytes gauge\n")
		fmt.Fprintf(&b, "tinysql_memory_limit_bytes %d\n", stats.MemoryLimitBytes)

		b.WriteString("# HELP tinysql_disk_used_bytes Storage backend disk usage in bytes.\n")
		b.WriteString("# TYPE tinysql_disk_used_bytes gauge\n")
		fmt.Fprintf(&b, "tinysql_disk_used_bytes %d\n", stats.DiskUsedBytes)

		b.WriteString("# HELP tinysql_cache_hit_rate Storage backend cache hit rate (0..1).\n")
		b.WriteString("# TYPE tinysql_cache_hit_rate gauge\n")
		fmt.Fprintf(&b, "tinysql_cache_hit_rate %s\n", strconv.FormatFloat(stats.CacheHitRate, 'f', -1, 64))

		b.WriteString("# HELP tinysql_sync_count_total Storage backend sync operations.\n")
		b.WriteString("# TYPE tinysql_sync_count_total counter\n")
		fmt.Fprintf(&b, "tinysql_sync_count_total %d\n", stats.SyncCount)

		b.WriteString("# HELP tinysql_load_count_total Storage backend load operations.\n")
		b.WriteString("# TYPE tinysql_load_count_total counter\n")
		fmt.Fprintf(&b, "tinysql_load_count_total %d\n", stats.LoadCount)

		b.WriteString("# HELP tinysql_cache_evictions_total Storage backend cache evictions.\n")
		b.WriteString("# TYPE tinysql_cache_evictions_total counter\n")
		fmt.Fprintf(&b, "tinysql_cache_evictions_total %d\n", stats.EvictionCount)
	}

	if m.vectorCacheMetricsEnabled {
		vc := engine.VectorCacheAnalytics()

		b.WriteString("# HELP tinysql_vector_cache_hits_total VEC_SEARCH result-cache hits.\n")
		b.WriteString("# TYPE tinysql_vector_cache_hits_total counter\n")
		fmt.Fprintf(&b, "tinysql_vector_cache_hits_total %d\n", vc.Hits)

		b.WriteString("# HELP tinysql_vector_cache_misses_total VEC_SEARCH result-cache misses.\n")
		b.WriteString("# TYPE tinysql_vector_cache_misses_total counter\n")
		fmt.Fprintf(&b, "tinysql_vector_cache_misses_total %d\n", vc.Misses)

		b.WriteString("# HELP tinysql_vector_cache_evictions_total VEC_SEARCH result-cache evictions.\n")
		b.WriteString("# TYPE tinysql_vector_cache_evictions_total counter\n")
		fmt.Fprintf(&b, "tinysql_vector_cache_evictions_total %d\n", vc.Evictions)
	}

	return b.String()
}
