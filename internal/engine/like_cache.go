package engine

import "sync"

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
const likeMatcherCacheMaxEntries = 256

type likeMatcherCacheKey struct {
	pattern         string
	caseInsensitive bool
}

var (
	likeMatcherCacheMu sync.RWMutex
	likeMatcherCache   = make(map[likeMatcherCacheKey]func(string) bool, 64)
)

// compileCachedLikeMatcher returns a func(string) bool for a LIKE/ILIKE
// pattern (default '\' escape, no GLOB — see evalLike) from a global,
// bounded, concurrency-safe cache.
func compileCachedLikeMatcher(pattern string, caseInsensitive bool) func(string) bool {
	key := likeMatcherCacheKey{pattern: pattern, caseInsensitive: caseInsensitive}
	likeMatcherCacheMu.RLock()
	m := likeMatcherCache[key]
	likeMatcherCacheMu.RUnlock()
	if m != nil {
		return m
	}
	m = compileLikeStringMatcher(pattern, caseInsensitive)
	likeMatcherCacheMu.Lock()
	if len(likeMatcherCache) >= likeMatcherCacheMaxEntries {
		// Simple full reset, same tradeoff compileCachedRegexp makes: no LRU
		// bookkeeping, and a reset is rare with a reasonable working set of
		// distinct patterns.
		likeMatcherCache = make(map[likeMatcherCacheKey]func(string) bool, 64)
	}
	likeMatcherCache[key] = m
	likeMatcherCacheMu.Unlock()
	return m
}
