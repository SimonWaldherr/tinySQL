package engine

import (
	"fmt"
	"slices"
	"testing"
	"time"
)

func TestVectorAnalyticsRollingWindow(t *testing.T) {
	for _, maxEvents := range []int{1, 8} {
		t.Run(fmt.Sprint(maxEvents), func(t *testing.T) {
			ConfigureVectorCache(VectorCacheConfig{Analytics: true, AnalyticsWindow: time.Minute, AnalyticsMaxEvents: maxEvents})
			t.Cleanup(func() { ConfigureVectorCache(VectorCacheConfig{}) })
			now := time.Now()
			for i := range 3 * maxEvents {
				recordVecQuery(VectorQueryEvent{At: now, K: i})
			}
			stats := VectorCacheAnalytics()
			if len(stats.RecentQueries) != maxEvents {
				t.Fatalf("window size = %d, want %d", len(stats.RecentQueries), maxEvents)
			}
			for i, event := range stats.RecentQueries {
				if event.K != 2*maxEvents+i {
					t.Fatalf("event %d has K=%d, want %d", i, event.K, 2*maxEvents+i)
				}
			}
			// Shrinking a configured window still retains the newest events.
			ConfigureVectorCache(VectorCacheConfig{Analytics: true, AnalyticsWindow: time.Minute, AnalyticsMaxEvents: 1})
			recordVecQuery(VectorQueryEvent{At: now, K: 99})
			stats = VectorCacheAnalytics()
			if len(stats.RecentQueries) != 1 || stats.RecentQueries[0].K != 99 {
				t.Fatalf("reconfigured window = %v", stats.RecentQueries)
			}
		})
	}
}

func BenchmarkVectorAnalyticsFullWindow(b *testing.B) {
	ConfigureVectorCache(VectorCacheConfig{Analytics: true, AnalyticsWindow: time.Minute, AnalyticsMaxEvents: 128})
	b.Cleanup(func() { ConfigureVectorCache(VectorCacheConfig{}) })
	event := VectorQueryEvent{At: time.Now(), Table: "docs", Column: "embedding", Metric: "cosine", Index: "hnsw", K: 20}
	for range 128 {
		recordVecQuery(event)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		recordVecQuery(event)
	}
}

func TestVectorAnalyticsPruneReleasesExpiredEvents(t *testing.T) {
	ConfigureVectorCache(VectorCacheConfig{Analytics: true, AnalyticsWindow: time.Minute})
	t.Cleanup(func() { ConfigureVectorCache(VectorCacheConfig{}) })
	now := time.Date(2026, time.November, 11, 12, 0, 0, 0, time.UTC)
	// Events may arrive out of order from concurrent searches. Keep the cutoff
	// boundary and a recent event, releasing expired references at both ends.
	backing := []VectorQueryEvent{
		{At: now.Add(-2 * time.Minute), Table: "expired-first"},
		{At: now.Add(-time.Minute), Table: "boundary"},
		{At: now, Table: "recent"},
		{At: now.Add(-3 * time.Minute), Table: "expired-last"},
	}
	want := append([]VectorQueryEvent(nil), backing[1:3]...)
	vecQueryCacheState.Lock()
	defer vecQueryCacheState.Unlock()
	vecQueryCacheState.events = backing
	pruneVecQueryCacheForce(now)
	if !slices.Equal(vecQueryCacheState.events, want) {
		t.Fatalf("events = %v, want %v", vecQueryCacheState.events, want)
	}
	for i, event := range backing[len(want):] {
		if event != (VectorQueryEvent{}) {
			t.Errorf("discarded slot %d retains %v", len(want)+i, event)
		}
	}
	pruneVecQueryCacheForce(now.Add(2 * time.Minute))
	if len(vecQueryCacheState.events) != 0 {
		t.Fatal("expired events remain visible")
	}
	for i, event := range backing {
		if event != (VectorQueryEvent{}) {
			t.Errorf("fully expired slot %d retains %v", i, event)
		}
	}
}
