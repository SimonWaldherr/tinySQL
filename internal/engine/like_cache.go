package engine

import (
	"sync"
	"sync/atomic"
)

// evalLike previously re-lowercased the pattern (for ILIKE) and always ran
// the general backtracking matcher on every single row, even for
// exact/prefix/suffix/substring patterns that compileLikeStringMatcher
// (exec_raw_filter.go) already fast-paths for the raw-filter scan. This
// bounded cache reuses that same compiled matcher across rows whenever the
// pattern repeats — which it does for essentially every WHERE/JOIN/CASE
// clause, since the pattern is almost always the same literal (or the same
// recurring computed value) across a query's row loop — mirroring
// compileCachedRegexp's cache for REGEXP/RLIKE/SIMILAR TO (regex_cache.go).
//
// Keyed by the runtime pattern value rather than requiring a literal AST
// node, so it also helps a LIKE whose pattern is a non-literal expression
// that happens to evaluate to a repeated value across rows — a case the
// literal-only raw-filter fast path can't cover at all.
//
// Backed by a sync.Map rather than a map+RWMutex: this is a read-mostly,
// small-keyspace cache (the same handful of patterns get looked up on every
// row of a scan), which is exactly sync.Map's target workload — repeat
// lookups of an already-cached key hit its lock-free read map instead of
// paying an RLock/RUnlock pair per row.
const likeMatcherCacheMaxEntries = 256

type likeMatcherCacheKey struct {
	pattern         string
	caseInsensitive bool
}

var (
	likeMatcherCache      atomic.Pointer[sync.Map]
	likeMatcherCacheCount atomic.Int64
)

func init() {
	likeMatcherCache.Store(&sync.Map{})
}

// compileCachedLikeMatcher returns a func(string) bool for a LIKE/ILIKE
// pattern (default '\' escape, no GLOB — see evalLike) from a global,
// bounded, concurrency-safe cache.
func compileCachedLikeMatcher(pattern string, caseInsensitive bool) func(string) bool {
	key := likeMatcherCacheKey{pattern: pattern, caseInsensitive: caseInsensitive}
	m := likeMatcherCache.Load()
	if v, ok := m.Load(key); ok {
		return v.(func(string) bool)
	}
	matcher := compileLikeStringMatcher(pattern, caseInsensitive)
	if likeMatcherCacheCount.Add(1) > likeMatcherCacheMaxEntries {
		// Simple full reset, same tradeoff compileCachedRegexp makes: no LRU
		// bookkeeping, and a reset is rare with a reasonable working set of
		// distinct patterns. A concurrent reset racing this one just means
		// whichever store lands last wins; the loser's entry is simply
		// recomputed on its next lookup, which is harmless since
		// compileLikeStringMatcher is a pure function of its arguments.
		fresh := &sync.Map{}
		fresh.Store(key, matcher)
		likeMatcherCache.Store(fresh)
		likeMatcherCacheCount.Store(1)
	} else {
		m.Store(key, matcher)
	}
	return matcher
}
