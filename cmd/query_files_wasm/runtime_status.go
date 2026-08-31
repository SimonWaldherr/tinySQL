package main

import (
	"runtime"
	"strings"
	"sync"
	"time"

	tinysql "github.com/SimonWaldherr/tinySQL"
)

type runtimeRequest struct {
	kind      string
	userID    string
	startedAt time.Time
}

type runtimeRequestCounters struct {
	Active   int64
	Total    int64
	Failed   int64
	TimedOut int64
}

type runtimeUserCounters struct {
	Sessions       map[string]struct{}
	ActiveRequests int64
	TotalRequests  int64
	FailedRequests int64
	LastSeen       time.Time
}

type runtimeMonitor struct {
	mu sync.Mutex

	startedAt    time.Time
	userID       string
	sessionID    string
	active       int64
	total        int64
	failed       int64
	timedOut     int64
	peakActive   int64
	busyStarted  time.Time
	busyDuration time.Duration
	totalLatency time.Duration
	lastLatency  time.Duration
	lastKind     string
	lastSuccess  bool
	lastAt       time.Time
	byKind       map[string]*runtimeRequestCounters
	users        map[string]*runtimeUserCounters
	recent       []time.Time
}

func newRuntimeMonitor() *runtimeMonitor {
	now := time.Now()
	return &runtimeMonitor{
		startedAt: now,
		userID:    "local-browser",
		byKind:    make(map[string]*runtimeRequestCounters),
		users:     make(map[string]*runtimeUserCounters),
	}
}

func (monitor *runtimeMonitor) setIdentity(userID, sessionID string) {
	monitor.mu.Lock()
	defer monitor.mu.Unlock()
	if value := strings.TrimSpace(userID); value != "" {
		monitor.userID = value
	}
	if value := strings.TrimSpace(sessionID); value != "" {
		monitor.sessionID = value
	}
	monitor.ensureUserLocked(monitor.userID, monitor.sessionID, time.Now())
}

func (monitor *runtimeMonitor) begin(kind string) runtimeRequest {
	now := time.Now()
	monitor.mu.Lock()
	defer monitor.mu.Unlock()
	if kind == "" {
		kind = "other"
	}
	request := runtimeRequest{kind: kind, userID: monitor.userID, startedAt: now}
	monitor.active++
	monitor.total++
	if monitor.active == 1 {
		monitor.busyStarted = now
	}
	if monitor.active > monitor.peakActive {
		monitor.peakActive = monitor.active
	}
	counters := monitor.kindLocked(kind)
	counters.Active++
	counters.Total++
	user := monitor.ensureUserLocked(request.userID, monitor.sessionID, now)
	user.ActiveRequests++
	user.TotalRequests++
	return request
}

func (monitor *runtimeMonitor) finish(request runtimeRequest, success, timedOut bool) {
	now := time.Now()
	duration := now.Sub(request.startedAt)
	monitor.mu.Lock()
	defer monitor.mu.Unlock()
	if monitor.active > 0 {
		monitor.active--
	}
	if monitor.active == 0 && !monitor.busyStarted.IsZero() {
		monitor.busyDuration += now.Sub(monitor.busyStarted)
		monitor.busyStarted = time.Time{}
	}
	counters := monitor.kindLocked(request.kind)
	if counters.Active > 0 {
		counters.Active--
	}
	if !success {
		monitor.failed++
		counters.Failed++
	}
	if timedOut {
		monitor.timedOut++
		counters.TimedOut++
	}
	if user := monitor.users[request.userID]; user != nil {
		if user.ActiveRequests > 0 {
			user.ActiveRequests--
		}
		if !success {
			user.FailedRequests++
		}
		user.LastSeen = now
	}
	monitor.totalLatency += duration
	monitor.lastLatency = duration
	monitor.lastKind = request.kind
	monitor.lastSuccess = success
	monitor.lastAt = now
	monitor.recent = append(monitor.recent, now)
	if len(monitor.recent) > 512 {
		monitor.recent = append([]time.Time(nil), monitor.recent[len(monitor.recent)-512:]...)
	}
}

func (monitor *runtimeMonitor) kindLocked(kind string) *runtimeRequestCounters {
	counters := monitor.byKind[kind]
	if counters == nil {
		counters = &runtimeRequestCounters{}
		monitor.byKind[kind] = counters
	}
	return counters
}

func (monitor *runtimeMonitor) ensureUserLocked(userID, sessionID string, now time.Time) *runtimeUserCounters {
	user := monitor.users[userID]
	if user == nil {
		user = &runtimeUserCounters{Sessions: make(map[string]struct{})}
		monitor.users[userID] = user
	}
	if sessionID != "" {
		user.Sessions[sessionID] = struct{}{}
	}
	user.LastSeen = now
	return user
}

func (monitor *runtimeMonitor) snapshot(database *tinysql.DB, tenant string, cache *tinysql.QueryCache) map[string]interface{} {
	now := time.Now()
	monitor.mu.Lock()
	uptime := now.Sub(monitor.startedAt)
	busy := monitor.busyDuration
	if monitor.active > 0 && !monitor.busyStarted.IsZero() {
		busy += now.Sub(monitor.busyStarted)
	}
	busyPercent := 0.0
	if uptime > 0 {
		busyPercent = float64(busy) / float64(uptime) * 100
	}
	averageMs := 0.0
	if monitor.total > 0 {
		averageMs = float64(monitor.totalLatency.Microseconds()) / 1000 / float64(monitor.total)
	}
	cutoff := now.Add(-time.Minute)
	recentRequests := 0
	for _, completedAt := range monitor.recent {
		if !completedAt.Before(cutoff) {
			recentRequests++
		}
	}
	byKind := make(map[string]interface{}, len(monitor.byKind))
	for kind, counters := range monitor.byKind {
		byKind[kind] = map[string]interface{}{
			"active": counters.Active, "total": counters.Total,
			"failed": counters.Failed, "timedOut": counters.TimedOut,
		}
	}
	users := make([]interface{}, 0, len(monitor.users))
	for userID, counters := range monitor.users {
		users = append(users, map[string]interface{}{
			"userId": userID, "sessions": len(counters.Sessions),
			"activeRequests": counters.ActiveRequests, "totalRequests": counters.TotalRequests,
			"failedRequests": counters.FailedRequests, "lastSeen": counters.LastSeen.UTC().Format(time.RFC3339),
		})
	}
	requests := map[string]interface{}{
		"active": monitor.active, "total": monitor.total, "failed": monitor.failed,
		"timedOut": monitor.timedOut, "peakActive": monitor.peakActive,
		"completedLastMinute": recentRequests, "byKind": byKind,
	}
	performance := map[string]interface{}{
		"state": "idle", "busyPercentSinceStart": busyPercent,
		"averageDurationMs": averageMs, "lastDurationMs": float64(monitor.lastLatency.Microseconds()) / 1000,
		"lastKind": monitor.lastKind, "lastSuccess": monitor.lastSuccess,
	}
	if monitor.active > 0 {
		performance["state"] = "busy"
	}
	if !monitor.lastAt.IsZero() {
		performance["lastCompletedAt"] = monitor.lastAt.UTC().Format(time.RFC3339)
	}
	identity := map[string]interface{}{
		"mode": "local-single-user", "tenant": tenant, "userId": monitor.userID,
		"sessionId": monitor.sessionID, "distinctUsers": len(monitor.users),
	}
	monitor.mu.Unlock()

	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	tableCount := 0
	rowCount := 0
	if database != nil {
		tables := database.ListTables(tenant)
		tableCount = len(tables)
		for _, table := range tables {
			rowCount += len(table.Rows)
		}
	}
	cacheSize := 0
	cacheMax := 0
	if cache != nil {
		stats := cache.Stats()
		cacheSize, _ = stats["size"].(int)
		cacheMax, _ = stats["maxSize"].(int)
	}
	return map[string]interface{}{
		"success":       true,
		"identity":      identity,
		"uptimeSeconds": int64(uptime.Seconds()),
		"requests":      requests,
		"performance":   performance,
		"database":      map[string]interface{}{"tables": tableCount, "rows": rowCount, "queryCacheEntries": cacheSize, "queryCacheCapacity": cacheMax},
		"memory": map[string]interface{}{
			"heapAllocBytes": mem.HeapAlloc, "heapInUseBytes": mem.HeapInuse,
			"heapSystemBytes": mem.HeapSys, "heapObjects": mem.HeapObjects,
		},
		"runtime": map[string]interface{}{"goVersion": runtime.Version(), "os": runtime.GOOS, "arch": runtime.GOARCH, "goroutines": runtime.NumGoroutine()},
		"users":   users,
	}
}
