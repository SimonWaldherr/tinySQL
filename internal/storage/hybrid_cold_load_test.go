package storage

import (
	"runtime"
	"testing"
	"time"
)

func TestHybridConcurrentColdLoadsShareOneTable(t *testing.T) {
	backend, err := NewHybridBackend(t.TempDir(), 8<<20, false, ModeHybrid)
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	table := snapshotTestTable(t, "docs", 2048)
	if err := backend.SaveTable("default", table); err != nil {
		t.Fatal(err)
	}
	backend.pool.Remove("default", "docs")

	// Park disk lookups until every caller has missed the pool. Without
	// coalescing, each caller proceeds to decode its own copy of the file.
	const readers = 8
	type loadResult struct {
		table *Table
		err   error
	}
	results := make(chan loadResult, readers)
	misses := backend.pool.GetStats().CacheMisses
	backend.disk.mu.Lock()
	for i := range readers {
		go func() {
			tenant, name := "default", "docs"
			if i%2 != 0 {
				tenant, name = "Default", "Docs"
			}
			loaded, err := backend.LoadTable(tenant, name)
			results <- loadResult{table: loaded, err: err}
		}()
	}
	deadline := time.Now().Add(5 * time.Second)
	for backend.pool.GetStats().CacheMisses-misses < readers && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	missed := backend.pool.GetStats().CacheMisses - misses
	backend.disk.mu.Unlock()
	if missed != readers {
		t.Fatalf("only %d of %d readers reached the cold load", missed, readers)
	}

	var shared *Table
	for range readers {
		select {
		case result := <-results:
			if result.err != nil || result.table == nil || len(result.table.Rows) != len(table.Rows) {
				t.Fatalf("load failed or returned incomplete rows: %v", result.err)
			}
			if shared == nil {
				shared = result.table
			} else if shared != result.table {
				t.Fatal("cold readers received distinct table objects")
			}
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for a cold load")
		}
	}
	if got := backend.disk.loadCount.Load(); got != 1 {
		t.Fatalf("performed %d disk decodes, want 1", got)
	}
	if resident, ok := backend.pool.Get("default", "docs"); !ok || resident != shared {
		t.Fatal("buffer pool did not retain the table shared by the readers")
	}
}
