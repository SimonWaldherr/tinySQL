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
	regexCache   = make(map[string]*regexp.Regexp, 64)
)

// compileCachedRegexp returns a compiled regular expression from a global,
// bounded, concurrency-safe cache. Compiled *regexp.Regexp values are safe
// for concurrent use.
func compileCachedRegexp(pattern string) (*regexp.Regexp, error) {
	regexCacheMu.RLock()
	re := regexCache[pattern]
	regexCacheMu.RUnlock()
	if re != nil {
		return re, nil
	}
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, err
	}
	regexCacheMu.Lock()
	if len(regexCache) >= regexCacheMaxEntries {
		// Simple full reset: bounded memory without LRU bookkeeping. With
		// 256 distinct live patterns a reset is rare and re-compilation cheap.
		regexCache = make(map[string]*regexp.Regexp, 64)
	}
	regexCache[pattern] = re
	regexCacheMu.Unlock()
	return re, nil
}
