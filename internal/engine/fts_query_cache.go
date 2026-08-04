package engine

import "sync"

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
const ftsQueryCacheMaxEntries = 256

var (
	ftsQueryCacheMu sync.RWMutex
	ftsQueryCache   = make(map[string]*ftsQueryNode, 64)
)

// parseCachedFTSQuery returns a parsed query tree from a global, bounded,
// concurrency-safe cache. The returned *ftsQueryNode must never be mutated
// by the caller — see the package comment above and ftsBindIDF's doc
// comment for why that invariant already has to hold everywhere else this
// tree is used.
func parseCachedFTSQuery(query string) *ftsQueryNode {
	ftsQueryCacheMu.RLock()
	node, ok := ftsQueryCache[query]
	ftsQueryCacheMu.RUnlock()
	if ok {
		return node
	}
	node = ftsParseQuery(query)
	ftsQueryCacheMu.Lock()
	if len(ftsQueryCache) >= ftsQueryCacheMaxEntries {
		// Simple full reset, same tradeoff compileCachedRegexp makes: no LRU
		// bookkeeping, and a reset is rare with a reasonable working set of
		// distinct queries.
		ftsQueryCache = make(map[string]*ftsQueryNode, 64)
	}
	ftsQueryCache[query] = node
	ftsQueryCacheMu.Unlock()
	return node
}
