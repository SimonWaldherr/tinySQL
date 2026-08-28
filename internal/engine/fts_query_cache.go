package engine

import (
	"sync"
	"sync/atomic"
)

// FTS_MATCH/FTS_RANK/FTS_SNIPPET (and FTS_SEARCH's own initial parse) all
// call ftsParseQuery on their query argument, which is virtually always the
// same constant string across every row of a scan (e.g. WHERE FTS_MATCH(col,
// 'vector OR search') ORDER BY BM25(col, 'vector OR search')) — reparsing it
// per row means rune-slicing, tokenizing, and allocating a fresh
// *ftsQueryNode tree N times for one logical query. ftsBindIDF's doc comment
// (fts_index.go) already establishes the invariant this cache relies on:
// "a caller's parsed tree may be reused across queries" — ftsBindIDF copies
// rather than annotating in place specifically so a shared cached tree is
// never mutated, and the scalar-function scoring path (ftsScoreNode with a
// nil idf func) never binds IDF at all, so those callers only ever read the
// tree. Mirrors compileCachedRegexp (regex_cache.go) for the same reason
// REGEXP/RLIKE/SIMILAR TO needed one.
//
// Backed by atomic.Pointer[sync.Map] rather than a map+RWMutex, mirroring
// compileCachedLikeMatcher's cache (like_cache.go): this is read on every row
// of a WHERE FTS_MATCH(...)/ORDER BY FTS_RANK(...) scan, almost always for
// the same handful of query strings, so the dominant case is a repeat lookup
// of an already-cached key — exactly sync.Map's target workload, and one
// that should never pay an RLock/RUnlock pair (real cache-line-bouncing
// contention under concurrent scans on a many-core machine) just to read a
// value that hasn't changed.
const ftsQueryCacheMaxEntries = 256

var (
	ftsQueryCache      atomic.Pointer[sync.Map]
	ftsQueryCacheCount atomic.Int64
)

func init() {
	ftsQueryCache.Store(&sync.Map{})
}

// parseCachedFTSQuery returns a parsed query tree from a global, bounded,
// concurrency-safe cache. The returned *ftsQueryNode must never be mutated
// by the caller — see the package comment above and ftsBindIDF's doc
// comment for why that invariant already has to hold everywhere else this
// tree is used.
func parseCachedFTSQuery(query string) *ftsQueryNode {
	m := ftsQueryCache.Load()
	if v, ok := m.Load(query); ok {
		return v.(*ftsQueryNode)
	}
	node := ftsParseQuery(query)
	if ftsQueryCacheCount.Add(1) > ftsQueryCacheMaxEntries {
		// Simple full reset, same tradeoff compileCachedLikeMatcher/
		// compileCachedRegexp make: no LRU bookkeeping, and a reset is rare
		// with a reasonable working set of distinct queries. A concurrent
		// reset racing this one just means whichever store lands last wins;
		// the loser's entry is simply reparsed on its next lookup, which is
		// harmless since ftsParseQuery is a pure function of its argument.
		fresh := &sync.Map{}
		fresh.Store(query, node)
		ftsQueryCache.Store(fresh)
		ftsQueryCacheCount.Store(1)
	} else {
		m.Store(query, node)
	}
	return node
}
