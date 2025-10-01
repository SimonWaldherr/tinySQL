// Package storage - Memory management and buffer pool implementation
//
// What: Configurable memory limits with LRU eviction for hybrid storage
// How: Track memory usage, evict least recently used tables when limit exceeded
// Why: Enable databases larger than available RAM while maintaining performance

package storage

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// CacheStrategy defines the eviction policy for the buffer pool.
type CacheStrategy int

const (
	StrategyNone CacheStrategy = iota // No eviction (full in-memory)
	StrategyLRU                       // Least Recently Used
	StrategyLFU                       // Least Frequently Used (future)
	StrategyARC                       // Adaptive Replacement Cache (future)
)

func (s CacheStrategy) String() string {
	switch s {
	case StrategyNone:
		return "None"
	case StrategyLRU:
		return "LRU"
	case StrategyLFU:
		return "LFU"
	case StrategyARC:
		return "ARC"
	default:
		return "Unknown"
	}
}

// MemoryPolicy defines memory management configuration.
type MemoryPolicy struct {
	// Maximum memory usage in bytes (0 = unlimited)
	MaxMemoryBytes int64

	// Cache eviction strategy
	Strategy CacheStrategy

	// Start evicting when memory usage exceeds this ratio (0.0-1.0)
	EvictionThreshold float64

	// Tables that should always stay in memory
	PinnedTables []string

	// Tables that should never be cached
	IgnoreTables []string

	// Enable eviction (if false, OOM when limit reached)
	EnableEviction bool

	// Number of tables to evict in one batch
	EvictionBatchSize int

	// Track access patterns for better eviction decisions
	TrackAccessPatterns bool

	// Time window for access tracking
	AccessWindow time.Duration
}

// DefaultMemoryPolicy returns a sensible default configuration.
func DefaultMemoryPolicy() *MemoryPolicy {
	return &MemoryPolicy{
		MaxMemoryBytes:      0, // Unlimited (current behavior)
		Strategy:            StrategyNone,
		EvictionThreshold:   0.8,
		EnableEviction:      false,
		EvictionBatchSize:   10,
		TrackAccessPatterns: true,
		AccessWindow:        5 * time.Minute,
	}
}

// LimitedMemoryPolicy returns a policy with memory limits.
func LimitedMemoryPolicy(maxMB int64) *MemoryPolicy {
	return &MemoryPolicy{
		MaxMemoryBytes:      maxMB * 1024 * 1024,
		Strategy:            StrategyLRU,
		EvictionThreshold:   0.8,
		EnableEviction:      true,
		EvictionBatchSize:   5,
		TrackAccessPatterns: true,
		AccessWindow:        5 * time.Minute,
	}
}

// BufferPool manages in-memory tables with configurable eviction.
type BufferPool struct {
	policy *MemoryPolicy

	// Current memory usage (atomic for lock-free reads)
	currentMemory atomic.Int64

	// Table cache: tenant -> table name -> cached table
	cache map[string]map[string]*CachedTable

	// LRU eviction queue
	lru *LRUQueue

	// Access statistics
	stats *CacheStats

	mu sync.RWMutex
}

// CachedTable wraps a table with caching metadata.
type CachedTable struct {
	Table       *Table
	Size        int64
	LoadedAt    time.Time
	LastAccess  time.Time
	AccessCount int64
	Pinned      bool

	// Disk location (for lazy loading - future)
	OnDisk     bool
	DiskOffset int64

	mu sync.RWMutex
}

// LRUQueue implements least-recently-used eviction.
type LRUQueue struct {
	head, tail *LRUNode
	nodes      map[string]*LRUNode
	size       int
	mu         sync.Mutex
}

// LRUNode is a node in the LRU doubly-linked list.
type LRUNode struct {
	key        string // "tenant:table"
	table      *CachedTable
	accessTime time.Time
	prev, next *LRUNode
}

// CacheStats tracks buffer pool performance metrics.
type CacheStats struct {
	MemoryUsed        int64
	MemoryLimit       int64
	MemoryUtilization float64

	CacheHits   int64
	CacheMisses int64
	HitRate     float64

	EvictionCount int64
	EvictionSize  int64

	TablesInMemory int
	TablesOnDisk   int

	mu sync.RWMutex
}

// NewBufferPool creates a buffer pool with the given policy.
func NewBufferPool(policy *MemoryPolicy) *BufferPool {
	if policy == nil {
		policy = DefaultMemoryPolicy()
	}

	bp := &BufferPool{
		policy: policy,
		cache:  make(map[string]map[string]*CachedTable),
		lru:    NewLRUQueue(),
		stats:  &CacheStats{MemoryLimit: policy.MaxMemoryBytes},
	}

	bp.currentMemory.Store(0)
	return bp
}

// NewLRUQueue creates an empty LRU queue.
func NewLRUQueue() *LRUQueue {
	return &LRUQueue{
		nodes: make(map[string]*LRUNode),
	}
}

// Put adds or updates a table in the buffer pool.
func (bp *BufferPool) Put(tenant, name string, table *Table) error {
	tableSize := EstimateTableSize(table)
	key := fmt.Sprintf("%s:%s", tenant, name)

	// Check if table is pinned or should be ignored
	pinned := bp.isPinned(name)
	ignored := bp.isIgnored(name)

	if ignored {
		return nil // Don't cache
	}

	bp.mu.Lock()
	defer bp.mu.Unlock()

	// Check memory limit
	if bp.policy.MaxMemoryBytes > 0 && !pinned {
		currentMem := bp.currentMemory.Load()
		newTotal := currentMem + tableSize
		threshold := int64(float64(bp.policy.MaxMemoryBytes) * bp.policy.EvictionThreshold)

		if newTotal > threshold {
			if bp.policy.EnableEviction {
				// Evict to make space
				bp.evictLRU(tableSize)
			} else {
				return fmt.Errorf("memory limit exceeded: %d/%d bytes",
					currentMem, bp.policy.MaxMemoryBytes)
			}
		}
	}

	// Get or create tenant cache
	if bp.cache[tenant] == nil {
		bp.cache[tenant] = make(map[string]*CachedTable)
	}

	// Check if table already cached
	if cached, exists := bp.cache[tenant][name]; exists {
		// Update existing
		oldSize := cached.Size
		cached.Table = table
		cached.Size = tableSize
		cached.LastAccess = time.Now()
		cached.AccessCount++

		// Update memory usage
		bp.currentMemory.Add(tableSize - oldSize)

		// Update LRU
		bp.lru.Access(key, cached)
	} else {
		// Add new
		cached := &CachedTable{
			Table:       table,
			Size:        tableSize,
			LoadedAt:    time.Now(),
			LastAccess:  time.Now(),
			AccessCount: 1,
			Pinned:      pinned,
		}

		bp.cache[tenant][name] = cached
		bp.currentMemory.Add(tableSize)

		// Add to LRU
		bp.lru.Add(key, cached)

		// Update stats
		bp.stats.mu.Lock()
		bp.stats.TablesInMemory++
		bp.stats.mu.Unlock()
	}

	// Update stats
	bp.updateStats()

	return nil
}

// Get retrieves a table from the buffer pool.
func (bp *BufferPool) Get(tenant, name string) (*Table, bool) {
	key := fmt.Sprintf("%s:%s", tenant, name)

	bp.mu.RLock()
	tenantCache, tenantExists := bp.cache[tenant]
	if !tenantExists {
		bp.mu.RUnlock()
		bp.recordMiss()
		return nil, false
	}

	cached, tableExists := tenantCache[name]
	bp.mu.RUnlock()

	if !tableExists {
		bp.recordMiss()
		return nil, false
	}

	// Update access time
	cached.mu.Lock()
	cached.LastAccess = time.Now()
	cached.AccessCount++
	cached.mu.Unlock()

	// Update LRU
	bp.lru.Access(key, cached)

	// Record hit
	bp.recordHit()

	return cached.Table, true
}

// Remove removes a table from the buffer pool.
func (bp *BufferPool) Remove(tenant, name string) {
	key := fmt.Sprintf("%s:%s", tenant, name)

	bp.mu.Lock()
	defer bp.mu.Unlock()

	if tenantCache, exists := bp.cache[tenant]; exists {
		if cached, exists := tenantCache[name]; exists {
			bp.currentMemory.Add(-cached.Size)
			delete(tenantCache, name)

			bp.lru.Remove(key)

			bp.stats.mu.Lock()
			bp.stats.TablesInMemory--
			bp.stats.mu.Unlock()
		}
	}

	bp.updateStats()
}

// evictLRU evicts least recently used tables to free up space.
func (bp *BufferPool) evictLRU(needed int64) {
	freed := int64(0)
	evicted := 0

	for freed < needed && evicted < bp.policy.EvictionBatchSize {
		key, cached := bp.lru.RemoveLRU()
		if key == "" {
			break // No more tables to evict
		}

		// Don't evict pinned tables
		if cached.Pinned {
			continue
		}

		// Parse key
		tenant, name := bp.parseKey(key)
		if tenantCache, exists := bp.cache[tenant]; exists {
			delete(tenantCache, name)
			freed += cached.Size
			evicted++

			bp.stats.mu.Lock()
			bp.stats.EvictionCount++
			bp.stats.EvictionSize += cached.Size
			bp.stats.TablesInMemory--
			bp.stats.mu.Unlock()
		}
	}

	bp.currentMemory.Add(-freed)
}

// isPinned checks if a table should always stay in memory.
func (bp *BufferPool) isPinned(name string) bool {
	for _, pinned := range bp.policy.PinnedTables {
		if pinned == name {
			return true
		}
	}
	return false
}

// isIgnored checks if a table should not be cached.
func (bp *BufferPool) isIgnored(name string) bool {
	for _, ignored := range bp.policy.IgnoreTables {
		if ignored == name {
			return true
		}
	}
	return false
}

// parseKey splits "tenant:table" into components.
func (bp *BufferPool) parseKey(key string) (tenant, name string) {
	for i, ch := range key {
		if ch == ':' {
			return key[:i], key[i+1:]
		}
	}
	return "", key
}

// recordHit increments cache hit counter.
func (bp *BufferPool) recordHit() {
	bp.stats.mu.Lock()
	bp.stats.CacheHits++
	total := bp.stats.CacheHits + bp.stats.CacheMisses
	if total > 0 {
		bp.stats.HitRate = float64(bp.stats.CacheHits) / float64(total)
	}
	bp.stats.mu.Unlock()
}

// recordMiss increments cache miss counter.
func (bp *BufferPool) recordMiss() {
	bp.stats.mu.Lock()
	bp.stats.CacheMisses++
	total := bp.stats.CacheHits + bp.stats.CacheMisses
	if total > 0 {
		bp.stats.HitRate = float64(bp.stats.CacheHits) / float64(total)
	}
	bp.stats.mu.Unlock()
}

// updateStats updates computed statistics.
func (bp *BufferPool) updateStats() {
	memUsed := bp.currentMemory.Load()

	bp.stats.mu.Lock()
	bp.stats.MemoryUsed = memUsed

	if bp.policy.MaxMemoryBytes > 0 {
		bp.stats.MemoryUtilization = float64(memUsed) / float64(bp.policy.MaxMemoryBytes)
	}

	total := bp.stats.CacheHits + bp.stats.CacheMisses
	if total > 0 {
		bp.stats.HitRate = float64(bp.stats.CacheHits) / float64(total)
	}
	bp.stats.mu.Unlock()
}

// GetStats returns a copy of current statistics.
func (bp *BufferPool) GetStats() CacheStats {
	bp.stats.mu.RLock()
	defer bp.stats.mu.RUnlock()

	// Return copy without mutex
	return CacheStats{
		MemoryUsed:        bp.stats.MemoryUsed,
		MemoryLimit:       bp.stats.MemoryLimit,
		MemoryUtilization: bp.stats.MemoryUtilization,
		CacheHits:         bp.stats.CacheHits,
		CacheMisses:       bp.stats.CacheMisses,
		HitRate:           bp.stats.HitRate,
		EvictionCount:     bp.stats.EvictionCount,
		EvictionSize:      bp.stats.EvictionSize,
		TablesInMemory:    bp.stats.TablesInMemory,
		TablesOnDisk:      bp.stats.TablesOnDisk,
	}
}

// GetMemoryUsage returns current memory usage in bytes.
func (bp *BufferPool) GetMemoryUsage() int64 {
	return bp.currentMemory.Load()
}

// GetMemoryLimit returns the configured memory limit.
func (bp *BufferPool) GetMemoryLimit() int64 {
	return bp.policy.MaxMemoryBytes
}

// Add adds a node to the LRU queue (most recent).
func (lru *LRUQueue) Add(key string, table *CachedTable) {
	lru.mu.Lock()
	defer lru.mu.Unlock()

	// Remove if already exists
	if node, exists := lru.nodes[key]; exists {
		lru.remove(node)
	}

	// Create new node
	node := &LRUNode{
		key:        key,
		table:      table,
		accessTime: time.Now(),
	}

	// Add to front
	lru.addFront(node)
	lru.nodes[key] = node
	lru.size++
}

// Access moves a node to the front (most recent).
func (lru *LRUQueue) Access(key string, table *CachedTable) {
	lru.mu.Lock()
	defer lru.mu.Unlock()

	if node, exists := lru.nodes[key]; exists {
		node.accessTime = time.Now()
		lru.remove(node)
		lru.addFront(node)
	}
}

// Remove removes a node from the queue.
func (lru *LRUQueue) Remove(key string) {
	lru.mu.Lock()
	defer lru.mu.Unlock()

	if node, exists := lru.nodes[key]; exists {
		lru.remove(node)
		delete(lru.nodes, key)
		lru.size--
	}
}

// RemoveLRU removes and returns the least recently used node.
func (lru *LRUQueue) RemoveLRU() (string, *CachedTable) {
	lru.mu.Lock()
	defer lru.mu.Unlock()

	if lru.tail == nil {
		return "", nil
	}

	node := lru.tail
	lru.remove(node)
	delete(lru.nodes, node.key)
	lru.size--

	return node.key, node.table
}

// addFront adds a node to the front of the queue.
func (lru *LRUQueue) addFront(node *LRUNode) {
	node.next = lru.head
	node.prev = nil

	if lru.head != nil {
		lru.head.prev = node
	}
	lru.head = node

	if lru.tail == nil {
		lru.tail = node
	}
}

// remove removes a node from the queue.
func (lru *LRUQueue) remove(node *LRUNode) {
	if node.prev != nil {
		node.prev.next = node.next
	} else {
		lru.head = node.next
	}

	if node.next != nil {
		node.next.prev = node.prev
	} else {
		lru.tail = node.prev
	}
}

// EstimateTableSize estimates memory usage of a table in bytes.
func EstimateTableSize(t *Table) int64 {
	if t == nil {
		return 0
	}

	// Header: table name, column metadata
	headerSize := int64(len(t.Name) + 100)

	// Column metadata
	for _, col := range t.Cols {
		headerSize += int64(len(col.Name) + 50)
	}

	// Row data
	rowSize := int64(0)
	if len(t.Rows) > 0 {
		// Sample first row
		for _, val := range t.Rows[0] {
			rowSize += EstimateValueSize(val)
		}
	} else {
		// Estimate from column types
		for _, col := range t.Cols {
			rowSize += EstimateColumnSize(col.Type)
		}
	}

	totalSize := headerSize + (rowSize * int64(len(t.Rows)))

	// Add overhead (pointers, maps, slices)
	overhead := totalSize / 10 // ~10% overhead

	return totalSize + overhead
}

// EstimateValueSize estimates the memory size of a value.
func EstimateValueSize(val any) int64 {
	if val == nil {
		return 8 // Pointer size
	}

	switch v := val.(type) {
	case int, int64, uint64, float64:
		return 8
	case int32, uint32, float32:
		return 4
	case int16, uint16:
		return 2
	case int8, uint8, bool:
		return 1
	case string:
		return int64(len(v) + 16) // String header + data
	case []byte:
		return int64(len(v) + 24) // Slice header + data
	default:
		return 64 // Default estimate
	}
}

// EstimateColumnSize estimates the average size of a column type.
func EstimateColumnSize(typ ColType) int64 {
	switch typ {
	case Int64Type, IntType, UintType, Uint64Type, Float64Type, FloatType:
		return 8
	case Int32Type, Uint32Type, Float32Type:
		return 4
	case Int16Type, Uint16Type:
		return 2
	case Int8Type, Uint8Type, BoolType, ByteType:
		return 1
	case StringType, TextType:
		return 32 // Average string
	case JsonType, JsonbType:
		return 128 // Average JSON
	case TimeType, DateType, DateTimeType, TimestampType:
		return 24
	default:
		return 16 // Default
	}
}
