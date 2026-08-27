package engine

import (
	"sync"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// ftsPreparedQueryCache keeps the corpus-dependent portion of an FTS query
// plan: wildcard expansion, the postings-derived candidate set, and the tree
// with dense term IDs/BM25 weights bound. The parsed syntax tree is already
// cached by parseCachedFTSQuery, but rebuilding these three pieces on every
// request still made repeated RAG questions pay map walks, allocations, and
// logarithms after the document index itself was warm.
//
// Entries are immutable. They are keyed by the same table/column-set identity
// as the document cache and validated against both table pointer and Version,
// so no prepared plan can survive a source-row mutation or a DROP+CREATE that
// happens to reuse a name/version pair.
const ftsPreparedQueryCacheMaxEntries = 256

type ftsPreparedQueryCacheKey struct {
	doc   ftsDocCacheKey
	query string
}

type ftsPreparedQueryCacheEntry struct {
	table      *storage.Table
	version    int
	node       *ftsQueryNode
	candidates ftsCandidates
}

var (
	ftsPreparedQueryCacheMu sync.RWMutex
	ftsPreparedQueryCache   = make(map[ftsPreparedQueryCacheKey]ftsPreparedQueryCacheEntry)
)

// prepareFTSQuery returns a corpus-bound, immutable query tree and the rows
// it may match. cache must be the current document cache for table/cols.
func prepareFTSQuery(tenant string, table *storage.Table, cols []int, query string, cache ftsDocCacheEntry) (*ftsQueryNode, ftsCandidates) {
	key := ftsPreparedQueryCacheKey{
		doc:   ftsDocCacheKey{tenant: tenant, table: table.Name, cols: ftsColsCacheKey(cols)},
		query: query,
	}

	ftsPreparedQueryCacheMu.RLock()
	entry, ok := ftsPreparedQueryCache[key]
	ftsPreparedQueryCacheMu.RUnlock()
	if ok && entry.table == table && entry.version == table.Version {
		return entry.node, entry.candidates
	}

	node := parseCachedFTSQuery(query)
	if node == nil {
		return nil, ftsCandidates{}
	}
	node = ftsExpandQuery(node, cache.postings)
	candidates := ftsQueryCandidates(node, cache.postings, len(cache.docs))
	node = ftsBindIDF(node, ftsIDFLookup(cache), cache.termIDs)

	computed := ftsPreparedQueryCacheEntry{
		table:      table,
		version:    table.Version,
		node:       node,
		candidates: candidates,
	}
	ftsPreparedQueryCacheMu.Lock()
	// A concurrent reader may have completed the same immutable plan while we
	// were computing ours. Prefer the published copy so all callers share its
	// backing candidate slice, but never use it if the table changed meanwhile.
	if entry, ok := ftsPreparedQueryCache[key]; ok && entry.table == table && entry.version == table.Version {
		ftsPreparedQueryCacheMu.Unlock()
		return entry.node, entry.candidates
	}
	if _, exists := ftsPreparedQueryCache[key]; !exists {
		evictOverCap(ftsPreparedQueryCache, ftsPreparedQueryCacheMaxEntries)
	}
	ftsPreparedQueryCache[key] = computed
	ftsPreparedQueryCacheMu.Unlock()
	return node, candidates
}

// purgeFTSPreparedQueryCachesFor releases corpus-bound query plans alongside
// the document cache when a table is dropped. Version checks are sufficient
// for correctness, but eager purging avoids retaining an otherwise unreachable
// table and its postings through a long-lived server process.
func purgeFTSPreparedQueryCachesFor(tenant, table string) {
	if tenant == "" {
		tenant = "default"
	}
	ftsPreparedQueryCacheMu.Lock()
	for key := range ftsPreparedQueryCache {
		if key.doc.tenant == tenant && key.doc.table == table {
			delete(ftsPreparedQueryCache, key)
		}
	}
	ftsPreparedQueryCacheMu.Unlock()
}
