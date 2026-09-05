package engine

import (
	"strings"
	"sync"
	"sync/atomic"
)

// FTS_SNIPPET/FTS_HIGHLIGHT run per row of a scan, and their query argument
// is virtually always the same constant literal across the whole result set
// (e.g. FTS_SNIPPET(col, 'vector OR search')). parseCachedFTSQuery
// (fts_query_cache.go) already avoids reparsing that literal into a tree on
// every row, but the snippet/highlight path additionally rebuilds the
// highlight-matching term set (ftsQueryTerms + ftsTokenize, both fresh map
// allocations) and re-derives its wildcard-prefix list from scratch each
// call. ftsQueryTerms(node) and ftsTokenize(query) are both pure functions
// of the query string alone (ftsParseQuery is pure too, so a cache miss that
// reparses still yields an equivalent tree), so this cache -- keyed the same
// way, bounded the same way, backed by the same atomic.Pointer[sync.Map] for
// the same never-a-writer-under-readers reasoning -- mirrors
// ftsQueryCache exactly.
type ftsSnippetTermSet struct {
	terms            map[string]bool
	wildcardPrefixes []string
}

const ftsSnippetSetCacheMaxEntries = 256

var (
	ftsSnippetSetCache      atomic.Pointer[sync.Map]
	ftsSnippetSetCacheCount atomic.Int64
)

func init() {
	ftsSnippetSetCache.Store(&sync.Map{})
}

// ftsCachedSnippetTermSet returns the highlight term set for queryStr from a
// global, bounded, concurrency-safe cache. The returned *ftsSnippetTermSet
// (and its terms map) must never be mutated by the caller: it is shared
// across every row of every query using the same literal, and potentially
// across concurrent scans too.
func ftsCachedSnippetTermSet(queryStr string, node *ftsQueryNode) *ftsSnippetTermSet {
	m := ftsSnippetSetCache.Load()
	if v, ok := m.Load(queryStr); ok {
		return v.(*ftsSnippetTermSet)
	}

	terms := ftsQueryTerms(node)
	// Also add simple tokenized terms for backward compatibility.
	for _, q := range ftsTokenize(queryStr) {
		terms[q] = true
	}
	var wildcardPrefixes []string
	for tok := range terms {
		if strings.HasSuffix(tok, "*") {
			wildcardPrefixes = append(wildcardPrefixes, strings.TrimSuffix(tok, "*"))
		}
	}
	set := &ftsSnippetTermSet{terms: terms, wildcardPrefixes: wildcardPrefixes}

	if ftsSnippetSetCacheCount.Add(1) > ftsSnippetSetCacheMaxEntries {
		// Same full-reset tradeoff as ftsQueryCache: no LRU bookkeeping, a
		// reset is rare with a reasonable working set of distinct queries,
		// and a losing concurrent reset just means its entry gets rebuilt
		// (harmlessly, since this is a pure function of queryStr) next call.
		fresh := &sync.Map{}
		fresh.Store(queryStr, set)
		ftsSnippetSetCache.Store(fresh)
		ftsSnippetSetCacheCount.Store(1)
	} else {
		m.Store(queryStr, set)
	}
	return set
}
