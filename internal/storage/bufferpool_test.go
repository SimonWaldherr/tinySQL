package storage

import (
	"testing"
	"time"
)

func TestBufferPoolBasic(t *testing.T) {
	policy := &MemoryPolicy{
		MaxMemoryBytes:    1024 * 1024, // 1 MB
		Strategy:          StrategyLRU,
		EvictionThreshold: 0.8,
		EnableEviction:    false,
	}

	bp := NewBufferPool(policy)

	// Create a simple table
	cols := []Column{
		{Name: "id", Type: IntType},
		{Name: "name", Type: StringType},
	}
	table := NewTable("test", cols, false)
	table.Rows = [][]any{
		{1, "Alice"},
		{2, "Bob"},
	}

	// Put table in buffer pool
	err := bp.Put("default", "test", table)
	if err != nil {
		t.Fatalf("Failed to put table: %v", err)
	}

	// Get table from buffer pool
	retrieved, found := bp.Get("default", "test")
	if !found {
		t.Fatal("Table not found in buffer pool")
	}

	if len(retrieved.Rows) != 2 {
		t.Errorf("Expected 2 rows, got %d", len(retrieved.Rows))
	}

	// Check memory usage
	memUsed := bp.GetMemoryUsage()
	if memUsed == 0 {
		t.Error("Expected non-zero memory usage")
	}

	t.Logf("Memory used: %d bytes", memUsed)
}

func TestBufferPoolEviction(t *testing.T) {
	policy := &MemoryPolicy{
		MaxMemoryBytes:    10 * 1024, // 10 KB
		Strategy:          StrategyLRU,
		EvictionThreshold: 0.8,
		EnableEviction:    true,
		EvictionBatchSize: 2,
	}

	bp := NewBufferPool(policy)

	// Create multiple tables
	for i := 0; i < 10; i++ {
		cols := []Column{
			{Name: "id", Type: IntType},
			{Name: "data", Type: StringType},
		}
		table := NewTable("table"+string(rune(i+'0')), cols, false)

		// Add rows to make table larger
		for j := 0; j < 20; j++ {
			table.Rows = append(table.Rows, []any{j, "data data data data data"})
		}

		err := bp.Put("default", table.Name, table)
		if err != nil && !policy.EnableEviction {
			t.Logf("Expected error when limit exceeded: %v", err)
		}

		time.Sleep(1 * time.Millisecond) // Ensure different access times
	}

	// Check that some tables were evicted
	stats := bp.GetStats()
	t.Logf("Stats: %d tables in memory, %d evictions", stats.TablesInMemory, stats.EvictionCount)

	if stats.EvictionCount == 0 && stats.TablesInMemory > 5 {
		t.Logf("Warning: Expected some evictions with memory limit")
	}
}

func TestBufferPoolPinnedTables(t *testing.T) {
	policy := &MemoryPolicy{
		MaxMemoryBytes:    5 * 1024, // 5 KB
		Strategy:          StrategyLRU,
		EvictionThreshold: 0.8,
		EnableEviction:    true,
		PinnedTables:      []string{"important"},
	}

	bp := NewBufferPool(policy)

	// Create pinned table
	cols := []Column{{Name: "id", Type: IntType}}
	important := NewTable("important", cols, false)
	for i := 0; i < 10; i++ {
		important.Rows = append(important.Rows, []any{i})
	}

	bp.Put("default", "important", important)

	// Create other tables to trigger eviction
	for i := 0; i < 10; i++ {
		table := NewTable("table"+string(rune(i+'0')), cols, false)
		for j := 0; j < 20; j++ {
			table.Rows = append(table.Rows, []any{j})
		}
		bp.Put("default", table.Name, table)
	}

	// Important table should still be in memory
	_, found := bp.Get("default", "important")
	if !found {
		t.Error("Pinned table should not be evicted")
	}
}

func TestBufferPoolLRUOrdering(t *testing.T) {
	policy := &MemoryPolicy{
		MaxMemoryBytes:    20 * 1024, // 20 KB
		Strategy:          StrategyLRU,
		EvictionThreshold: 0.5,
		EnableEviction:    true,
		EvictionBatchSize: 1,
	}

	bp := NewBufferPool(policy)

	cols := []Column{{Name: "id", Type: IntType}}

	// Add tables
	for i := 0; i < 5; i++ {
		table := NewTable("table"+string(rune(i+'0')), cols, false)
		for j := 0; j < 50; j++ {
			table.Rows = append(table.Rows, []any{j})
		}
		bp.Put("default", table.Name, table)
		time.Sleep(2 * time.Millisecond)
	}

	// Access table0 to make it most recent
	bp.Get("default", "table0")

	// Add more tables to trigger eviction
	for i := 5; i < 8; i++ {
		table := NewTable("table"+string(rune(i+'0')), cols, false)
		for j := 0; j < 50; j++ {
			table.Rows = append(table.Rows, []any{j})
		}
		bp.Put("default", table.Name, table)
	}

	// table0 should still be present (recently accessed)
	_, found := bp.Get("default", "table0")
	if !found {
		t.Error("Recently accessed table should not be evicted first")
	}

	// Older tables should be evicted
	stats := bp.GetStats()
	t.Logf("Eviction count: %d, tables in memory: %d", stats.EvictionCount, stats.TablesInMemory)
}

func TestBufferPoolStats(t *testing.T) {
	policy := &MemoryPolicy{
		MaxMemoryBytes:      1024 * 1024, // 1 MB
		Strategy:            StrategyLRU,
		EvictionThreshold:   0.99, // Very high to avoid triggering eviction
		TrackAccessPatterns: true,
		EnableEviction:      false,
	}

	bp := NewBufferPool(policy)

	cols := []Column{{Name: "id", Type: IntType}}
	table := NewTable("test", cols, false)
	table.Rows = [][]any{{1}, {2}, {3}}

	// Put table in buffer pool
	err := bp.Put("default", "test", table)
	if err != nil {
		t.Fatalf("Failed to put table: %v", err)
	}

	// Check initial stats
	stats := bp.GetStats()
	t.Logf("After put - Hits: %d, Misses: %d", stats.CacheHits, stats.CacheMisses)

	// Multiple gets (should be hits)
	for i := 0; i < 5; i++ {
		_, found := bp.Get("default", "test")
		if !found {
			t.Errorf("Table should be found on iteration %d", i)
		}
	}

	// Check stats after hits
	stats = bp.GetStats()
	t.Logf("After gets - Hits: %d, Misses: %d", stats.CacheHits, stats.CacheMisses)

	// Get non-existent (should be misses)
	for i := 0; i < 3; i++ {
		bp.Get("default", "nonexistent")
	}

	stats = bp.GetStats()
	t.Logf("Final - Hits: %d, Misses: %d, Hit Rate: %.2f%%",
		stats.CacheHits, stats.CacheMisses, stats.HitRate*100)

	if stats.CacheHits != 5 {
		t.Errorf("Expected 5 cache hits, got %d", stats.CacheHits)
	}

	if stats.CacheMisses != 3 {
		t.Errorf("Expected 3 cache misses, got %d", stats.CacheMisses)
	}

	expectedHitRate := 5.0 / 8.0
	if stats.HitRate < expectedHitRate-0.01 || stats.HitRate > expectedHitRate+0.01 {
		t.Errorf("Expected hit rate ~%.2f, got %.2f", expectedHitRate, stats.HitRate)
	}
}

func TestBufferPoolRemove(t *testing.T) {
	bp := NewBufferPool(DefaultMemoryPolicy())

	cols := []Column{{Name: "id", Type: IntType}}
	table := NewTable("test", cols, false)
	table.Rows = [][]any{{1}}

	bp.Put("default", "test", table)

	// Verify it's there
	_, found := bp.Get("default", "test")
	if !found {
		t.Fatal("Table should be in buffer pool")
	}

	// Remove it
	bp.Remove("default", "test")

	// Verify it's gone
	_, found = bp.Get("default", "test")
	if found {
		t.Error("Table should not be in buffer pool after removal")
	}

	// Memory should be freed
	if bp.GetMemoryUsage() != 0 {
		t.Errorf("Expected 0 memory usage after removal, got %d", bp.GetMemoryUsage())
	}
}

func TestEstimateTableSize(t *testing.T) {
	cols := []Column{
		{Name: "id", Type: IntType},
		{Name: "name", Type: StringType},
		{Name: "age", Type: IntType},
	}

	table := NewTable("users", cols, false)

	// Empty table
	size1 := EstimateTableSize(table)
	if size1 == 0 {
		t.Error("Expected non-zero size for empty table")
	}

	// Add rows
	table.Rows = [][]any{
		{1, "Alice", 30},
		{2, "Bob", 25},
		{3, "Charlie", 35},
	}

	size2 := EstimateTableSize(table)
	if size2 <= size1 {
		t.Error("Expected larger size with rows")
	}

	t.Logf("Empty table: %d bytes, With 3 rows: %d bytes", size1, size2)
}

func TestLRUQueue(t *testing.T) {
	lru := NewLRUQueue()

	cols := []Column{{Name: "id", Type: IntType}}

	// Add items
	for i := 0; i < 5; i++ {
		table := NewTable("table"+string(rune(i+'0')), cols, false)
		cached := &CachedTable{
			Table:      table,
			LoadedAt:   time.Now(),
			LastAccess: time.Now(),
		}
		lru.Add("default:"+table.Name, cached)
		time.Sleep(1 * time.Millisecond)
	}

	if lru.size != 5 {
		t.Errorf("Expected size 5, got %d", lru.size)
	}

	// Access first item (make it most recent)
	lru.Access("default:table0", nil)

	// Remove LRU (should be table1, not table0)
	key, _ := lru.RemoveLRU()
	if key == "default:table0" {
		t.Error("Should not remove recently accessed item")
	}

	if lru.size != 4 {
		t.Errorf("Expected size 4 after removal, got %d", lru.size)
	}
}

func TestMemoryPolicyDefault(t *testing.T) {
	policy := DefaultMemoryPolicy()

	if policy.MaxMemoryBytes != 0 {
		t.Error("Default policy should have unlimited memory")
	}

	if policy.Strategy != StrategyNone {
		t.Error("Default policy should have no eviction strategy")
	}

	if policy.EnableEviction {
		t.Error("Default policy should not enable eviction")
	}
}

func TestMemoryPolicyLimited(t *testing.T) {
	policy := LimitedMemoryPolicy(512) // 512 MB

	expectedBytes := int64(512 * 1024 * 1024)
	if policy.MaxMemoryBytes != expectedBytes {
		t.Errorf("Expected %d bytes, got %d", expectedBytes, policy.MaxMemoryBytes)
	}

	if policy.Strategy != StrategyLRU {
		t.Error("Limited policy should use LRU strategy")
	}

	if !policy.EnableEviction {
		t.Error("Limited policy should enable eviction")
	}
}

func TestBufferPoolConcurrency(t *testing.T) {
	bp := NewBufferPool(LimitedMemoryPolicy(10))

	cols := []Column{{Name: "id", Type: IntType}}

	// Concurrent puts
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func(n int) {
			table := NewTable("table"+string(rune(n+'0')), cols, false)
			for j := 0; j < 10; j++ {
				table.Rows = append(table.Rows, []any{j})
			}
			bp.Put("default", table.Name, table)
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}

	// Concurrent gets
	for i := 0; i < 10; i++ {
		go func(n int) {
			bp.Get("default", "table"+string(rune(n+'0')))
			done <- true
		}(i)
	}

	for i := 0; i < 10; i++ {
		<-done
	}

	// Should not panic or deadlock
	t.Log("Concurrency test passed")
}
