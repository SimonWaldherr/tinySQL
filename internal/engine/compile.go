// Package engine provides SQL parsing, planning, and execution for tinySQL.
//
// This file focuses on the query compilation cache:
//   - What: A lightweight in-memory LRU cache that stores parsed/compiled
//     representations of SQL statements (CompiledQuery).
//   - How: Queries are keyed by their exact SQL string. The cache holds a
//     Statement AST plus metadata (ParsedAt) and returns it to callers to
//     avoid re-parsing. LRU eviction using container/list keeps the cache
//     within a fixed size with O(1) eviction.
//   - Why: Parsing is comparatively expensive and often repeated in loops or
//     hot paths. Caching reduces parse overhead, improves latency, and keeps
//     the execution path predictable while remaining simple and thread-safe.
package engine

import (
	"container/list"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// CompiledQuery represents a pre-parsed and cached SQL query.
type CompiledQuery struct {
	SQL       string
	Statement Statement
	ParsedAt  time.Time
}

// cacheEntry pairs a cache key with its compiled query for LRU tracking.
type cacheEntry struct {
	key string
	cq  *CompiledQuery
}

// QueryCache manages compiled queries with LRU eviction.
type QueryCache struct {
	mu      sync.RWMutex
	entries map[string]*list.Element
	order   *list.List // front = most recently used
	maxSize int
}

// NewQueryCache creates a new query cache with the specified maximum size.
func NewQueryCache(maxSize int) *QueryCache {
	if maxSize <= 0 {
		maxSize = 1000 // default cache size
	}
	return &QueryCache{
		entries: make(map[string]*list.Element, maxSize),
		order:   list.New(),
		maxSize: maxSize,
	}
}

// Compile parses and caches a SQL query for reuse.
func (qc *QueryCache) Compile(sql string) (*CompiledQuery, error) {
	qc.mu.RLock()
	if elem, exists := qc.entries[sql]; exists {
		qc.mu.RUnlock()
		// Promote to front (most recently used) under write lock.
		qc.mu.Lock()
		qc.order.MoveToFront(elem)
		qc.mu.Unlock()
		return elem.Value.(*cacheEntry).cq, nil
	}
	qc.mu.RUnlock()

	// Parse the query
	parser := NewParser(sql)
	stmt, err := parser.ParseStatement()
	if err != nil {
		return nil, fmt.Errorf("compile error: %w", err)
	}

	compiled := &CompiledQuery{
		SQL:       sql,
		Statement: stmt,
		ParsedAt:  time.Now(),
	}

	// Cache the compiled query
	qc.mu.Lock()
	defer qc.mu.Unlock()

	// Double-check after acquiring write lock (another goroutine may have inserted).
	if elem, exists := qc.entries[sql]; exists {
		qc.order.MoveToFront(elem)
		return elem.Value.(*cacheEntry).cq, nil
	}

	// Evict LRU entry if at capacity — O(1).
	if qc.order.Len() >= qc.maxSize {
		tail := qc.order.Back()
		if tail != nil {
			qc.order.Remove(tail)
			delete(qc.entries, tail.Value.(*cacheEntry).key)
		}
	}

	entry := &cacheEntry{key: sql, cq: compiled}
	elem := qc.order.PushFront(entry)
	qc.entries[sql] = elem
	return compiled, nil
}

// Execute runs a compiled query against the database.
func (cq *CompiledQuery) Execute(ctx context.Context, db *storage.DB, tenant string) (*ResultSet, error) {
	return Execute(ctx, db, tenant, cq.Statement)
}

// MustCompile is like Compile but panics on error (similar to regexp.MustCompile).
func (qc *QueryCache) MustCompile(sql string) *CompiledQuery {
	cq, err := qc.Compile(sql)
	if err != nil {
		panic(fmt.Sprintf("MustCompile(%q): %v", sql, err))
	}
	return cq
}

// Clear removes all cached queries.
func (qc *QueryCache) Clear() {
	qc.mu.Lock()
	defer qc.mu.Unlock()
	qc.entries = make(map[string]*list.Element, qc.maxSize)
	qc.order.Init()
}

// Size returns the number of cached queries.
func (qc *QueryCache) Size() int {
	qc.mu.RLock()
	defer qc.mu.RUnlock()
	return len(qc.entries)
}

// Stats returns cache statistics.
func (qc *QueryCache) Stats() map[string]interface{} {
	qc.mu.RLock()
	defer qc.mu.RUnlock()

	return map[string]interface{}{
		"size":    len(qc.entries),
		"maxSize": qc.maxSize,
		"queries": len(qc.entries),
	}
}
