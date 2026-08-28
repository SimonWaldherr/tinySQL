package engine

import (
	"sync"
	"sync/atomic"

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
	// ftsPreparedQueryCacheSnapshot mirrors the map above for lock-free reads,
	// the same way vecSearchColumnCacheSnapshot does for the vector column cache
	// (vector_search.go).
	//
	// Every FTS_SEARCH — and so every hybrid RAG_SEARCH, which runs an FTS pass
	// on each question — reads this cache once, and the warm hit is by far the
	// common case. Taking an RWMutex read lock for it means all concurrent
	// retrievals contend on one shared reader counter's cache line, which is
	// exactly the cost that shows up on a many-core server and not on a
	// single-threaded benchmark.
	//
	// Freshness is unaffected: it was never the mutex that made a cached plan
	// valid, but the table-pointer and Version comparison against the entry
	// itself, which the lock-free path performs identically. Entries are
	// immutable once published (see the type's doc comment).
	ftsPreparedQueryCacheSnapshot atomic.Pointer[map[ftsPreparedQueryCacheKey]ftsPreparedQueryCacheEntry]
)

func init() {
	empty := make(map[ftsPreparedQueryCacheKey]ftsPreparedQueryCacheEntry)
	ftsPreparedQueryCacheSnapshot.Store(&empty)
}

// publishFTSPreparedQueryCacheSnapshotLocked refreshes the lock-free snapshot
// from the authoritative map. Callers must hold ftsPreparedQueryCacheMu and
// must call this after every mutation — insert and delete alike, since a purge
// that skipped it would leave lock-free readers serving plans for a dropped
// table. Bounded by ftsPreparedQueryCacheMaxEntries, so the copy is cheap next
// to the wildcard expansion and IDF binding that produced the new entry.
func publishFTSPreparedQueryCacheSnapshotLocked() {
	snap := make(map[ftsPreparedQueryCacheKey]ftsPreparedQueryCacheEntry, len(ftsPreparedQueryCache))
	for k, v := range ftsPreparedQueryCache {
		snap[k] = v
	}
	ftsPreparedQueryCacheSnapshot.Store(&snap)
}

// prepareFTSQuery returns a corpus-bound, immutable query tree and the rows
// it may match. cache must be the current document cache for table/cols.
func prepareFTSQuery(tenant string, table *storage.Table, cols []int, query string, cache ftsDocCacheEntry) (*ftsQueryNode, ftsCandidates) {
	key := ftsPreparedQueryCacheKey{
		doc:   ftsDocCacheKey{tenant: tenant, table: table.Name, cols: ftsColsCacheKey(cols)},
		query: query,
	}

	// Lock-free fast path — see publishFTSPreparedQueryCacheSnapshotLocked.
	if snap := ftsPreparedQueryCacheSnapshot.Load(); snap != nil {
		if entry, ok := (*snap)[key]; ok && entry.table == table && entry.version == table.Version {
			return entry.node, entry.candidates
		}
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
	publishFTSPreparedQueryCacheSnapshotLocked()
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
	publishFTSPreparedQueryCacheSnapshotLocked()
	ftsPreparedQueryCacheMu.Unlock()
}
