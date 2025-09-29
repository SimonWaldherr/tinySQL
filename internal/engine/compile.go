// Package engine provides SQL parsing, planning, and execution for tinySQL.
//
// This file focuses on the query compilation cache:
//   - What: A lightweight in-memory cache that stores parsed/compiled
//     representations of SQL statements (CompiledQuery).
//   - How: Queries are keyed by their exact SQL string. The cache holds a
//     Statement AST plus metadata (ParsedAt) and returns it to callers to
//     avoid re-parsing. A simple FIFO eviction based on oldest ParsedAt keeps
//     the cache within a fixed size.
//   - Why: Parsing is comparatively expensive and often repeated in loops or
//     hot paths. Caching reduces parse overhead, improves latency, and keeps
//     the execution path predictable while remaining simple and thread-safe.
package engine

import (
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

// QueryCache manages compiled queries.
type QueryCache struct {
	mu      sync.RWMutex
	queries map[string]*CompiledQuery
	maxSize int
}

// NewQueryCache creates a new query cache with the specified maximum size.
func NewQueryCache(maxSize int) *QueryCache {
	if maxSize <= 0 {
		maxSize = 1000 // default cache size
	}
	return &QueryCache{
		queries: make(map[string]*CompiledQuery),
		maxSize: maxSize,
	}
}

// Compile parses and caches a SQL query for reuse.
func (qc *QueryCache) Compile(sql string) (*CompiledQuery, error) {
	qc.mu.RLock()
	if cached, exists := qc.queries[sql]; exists {
		qc.mu.RUnlock()
		return cached, nil
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

	// If cache is full, remove oldest entry (simple FIFO)
	if len(qc.queries) >= qc.maxSize {
		var oldestSQL string
		var oldestTime time.Time
		first := true
		for sql, cq := range qc.queries {
			if first || cq.ParsedAt.Before(oldestTime) {
				oldestSQL = sql
				oldestTime = cq.ParsedAt
				first = false
			}
		}
		delete(qc.queries, oldestSQL)
	}

	qc.queries[sql] = compiled
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
	qc.queries = make(map[string]*CompiledQuery)
}

// Size returns the number of cached queries.
func (qc *QueryCache) Size() int {
	qc.mu.RLock()
	defer qc.mu.RUnlock()
	return len(qc.queries)
}

// Stats returns cache statistics.
func (qc *QueryCache) Stats() map[string]interface{} {
	qc.mu.RLock()
	defer qc.mu.RUnlock()

	return map[string]interface{}{
		"size":    len(qc.queries),
		"maxSize": qc.maxSize,
		"queries": len(qc.queries),
	}
}
