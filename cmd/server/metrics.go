package main

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
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
}

func newMetricsRegistry() *metricsRegistry {
	return &metricsRegistry{
		requests:     make(map[requestKey]uint64),
		durations:    make(map[durationKey]*durationHistogram),
		totalByProto: make(map[string]uint64),
	}
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

	return b.String()
}
