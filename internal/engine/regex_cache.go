package engine

import (
	"regexp"
	"sync"
)

// The REGEXP / RLIKE / SIMILAR TO predicates and the REGEXP_* functions are
// evaluated per row; compiling the pattern each time made them O(compile)
// per row. This bounded cache compiles each distinct pattern once.
const regexCacheMaxEntries = 256

var (
	regexCacheMu sync.RWMutex
	regexCache   = make(map[string]*cachedRegexp, 64)
)

type cachedRegexp struct {
	re        *regexp.Regexp
	matchOnce sync.Once
	match     func(string) bool
}

// compileCachedRegexp returns a compiled regular expression from a global,
// bounded, concurrency-safe cache. Compiled *regexp.Regexp values are safe
// for concurrent use.
func compileCachedRegexp(pattern string) (*regexp.Regexp, error) {
	compiled, err := loadCachedRegexp(pattern)
	if err != nil {
		return nil, err
	}
	return compiled.re, nil
}

// Boolean predicates can use a literal matcher while extraction/replacement
// retain the original regexp, including its captures and match boundaries.
func compileCachedRegexpMatcher(pattern string) (func(string) bool, error) {
	compiled, err := loadCachedRegexp(pattern)
	if err != nil {
		return nil, err
	}
	compiled.matchOnce.Do(func() { compiled.match = compileRegexpStringMatcher(compiled.re) })
	return compiled.match, nil
}

func loadCachedRegexp(pattern string) (*cachedRegexp, error) {
	regexCacheMu.RLock()
	compiled := regexCache[pattern]
	regexCacheMu.RUnlock()
	if compiled != nil {
		return compiled, nil
	}
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, err
	}
	compiled = &cachedRegexp{re: re}
	regexCacheMu.Lock()
	defer regexCacheMu.Unlock()
	if existing := regexCache[pattern]; existing != nil {
		return existing, nil
	}
	if len(regexCache) >= regexCacheMaxEntries {
		// Simple full reset: bounded memory without LRU bookkeeping. With
		// 256 distinct live patterns a reset is rare and re-compilation cheap.
		regexCache = make(map[string]*cachedRegexp, 64)
	}
	regexCache[pattern] = compiled
	return compiled, nil
}
