package engine

import (
	"strings"
	"testing"
	"unsafe"
)

func TestPatternCachesOwnSource(t *testing.T) {
	t.Run("LIKE", func(t *testing.T) {
		for _, insensitive := range []bool{false, true} {
			const pattern = "cache_ownership_like_20261027%"
			source := pattern + strings.Repeat("query padding ", 1024)
			borrowed := source[:len(pattern)]
			match := compileCachedLikeMatcher(borrowed, insensitive)
			if !match("cache_ownership_like_20261027_value") || match("unrelated") {
				t.Fatal("cached matcher changed pattern semantics")
			}
			found := false
			likeMatcherCache.Load().Range(func(key, _ any) bool {
				k := key.(likeMatcherCacheKey)
				if k.pattern == pattern && k.caseInsensitive == insensitive {
					found = true
					if unsafe.StringData(k.pattern) == unsafe.StringData(borrowed) {
						t.Error("cached LIKE key retains the query buffer")
					}
				}
				return true
			})
			if !found {
				t.Fatal("compiled LIKE pattern was not cached")
			}
		}
	})
	t.Run("REGEXP", func(t *testing.T) {
		const pattern = "^cache_ownership_regexp_20261027.*$"
		source := pattern + strings.Repeat("query padding ", 1024)
		borrowed := source[:len(pattern)]
		compiled, err := loadCachedRegexp(borrowed)
		if err != nil {
			t.Fatal(err)
		}
		if !compiled.re.MatchString("cache_ownership_regexp_20261027_value") || compiled.re.MatchString("unrelated") {
			t.Fatal("cached regexp changed pattern semantics")
		}
		if unsafe.StringData(compiled.re.String()) == unsafe.StringData(borrowed) {
			t.Error("cached regexp source retains the query buffer")
		}
		regexCacheMu.RLock()
		defer regexCacheMu.RUnlock()
		for key := range regexCache {
			if key == pattern && unsafe.StringData(key) == unsafe.StringData(borrowed) {
				t.Error("cached regexp key retains the query buffer")
			}
		}
	})
}

func patternCacheHitCases() []struct {
	name   string
	lookup func() bool
} {
	return []struct {
		name   string
		lookup func() bool
	}{
		{"LIKE", func() bool {
			return compileCachedLikeMatcher("cache_alloc_like_20261027%", false)("cache_alloc_like_20261027_value")
		}},
		{"REGEXP", func() bool {
			re, err := compileCachedRegexp("^cache_alloc_regexp_20261027.*$")
			return err == nil && re.MatchString("cache_alloc_regexp_20261027_value")
		}},
		{"REGEXP matcher", func() bool {
			match, err := compileCachedRegexpMatcher("^cache_alloc_regexp_matcher_20261027.*$")
			return err == nil && match("cache_alloc_regexp_matcher_20261027_value")
		}},
	}
}

func TestPatternCacheHitsDoNotAllocate(t *testing.T) {
	for _, tc := range patternCacheHitCases() {
		t.Run(tc.name, func(t *testing.T) {
			if !tc.lookup() {
				t.Fatal("cached pattern did not match")
			}
			allocs := testing.AllocsPerRun(100, func() {
				if !tc.lookup() {
					t.Fatal("cached pattern did not match")
				}
			})
			if allocs != 0 {
				t.Fatalf("cache hit allocated %v times", allocs)
			}
		})
	}
}

func BenchmarkPatternCacheHit(b *testing.B) {
	for _, tc := range patternCacheHitCases() {
		b.Run(tc.name, func(b *testing.B) {
			if !tc.lookup() {
				b.Fatal("cached pattern did not match")
			}
			b.ReportAllocs()
			for b.Loop() {
				if !tc.lookup() {
					b.Fatal("cached pattern did not match")
				}
			}
		})
	}
}
