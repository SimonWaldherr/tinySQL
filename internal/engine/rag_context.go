package engine

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// ─────────────────────── neighbor-chunk index cache ──────────────────────────
//
// The per-document chunk index is a pure function of the source table's contents
// and the two column names, but it used to be rebuilt on every expansion — a
// full scan of the corpus per query, which became the dominant cost of a RAG
// SELECT once retrieval itself was made fast. It is cached here the same way the
// vector column cache and the tokenized-document cache are: keyed by
// (tenant, table, columns), validated against the table pointer and
// table.Version, and rebuilt lazily whenever either changes.
//
// Entries are immutable once published, so concurrent queries share one index.

type ragContextIndexCacheKey struct {
	tenant   string
	table    string
	docCol   string // lower-cased
	chunkCol string // lower-cased
}

type ragContextIndexCacheEntry struct {
	table   *storage.Table
	version int
	index   ragContextIndex
}

// ragContextIndexCacheMaxEntries bounds the cache for the same reason the vector
// and FTS caches are bounded: each entry pins its *storage.Table, so without a
// cap a process that cycles through many tables or column pairs would retain one
// table per orphaned key.
const ragContextIndexCacheMaxEntries = 256

var (
	ragContextIndexCacheMu sync.RWMutex
	ragContextIndexCache   = make(map[ragContextIndexCacheKey]ragContextIndexCacheEntry)
)

// purgeRAGContextCachesFor drops cached neighbor indexes for one table, called
// from DROP TABLE. Purging is always safe: the index rebuilds lazily.
func purgeRAGContextCachesFor(tenant, table string) {
	if tenant == "" {
		tenant = "default"
	}
	ragContextIndexCacheMu.Lock()
	for k := range ragContextIndexCache {
		if k.tenant == tenant && k.table == table {
			delete(ragContextIndexCache, k)
		}
	}
	ragContextIndexCacheMu.Unlock()
}

// getRAGContextIndex returns the neighbor-chunk index for source, building it
// only when no valid cached copy exists. Sources without a stable identity (CTE
// results, in-memory fused hit sets) are always built fresh.
func getRAGContextIndex(source ragSource, docCol, chunkCol string) ragContextIndex {
	if !source.tableSource || source.table == nil {
		return ragBuildContextIndex(source, docCol, chunkCol)
	}
	key := ragContextIndexCacheKey{
		tenant:   source.tenant,
		table:    source.table.Name,
		docCol:   strings.ToLower(docCol),
		chunkCol: strings.ToLower(chunkCol),
	}

	ragContextIndexCacheMu.RLock()
	entry, ok := ragContextIndexCache[key]
	ragContextIndexCacheMu.RUnlock()
	if ok && entry.table == source.table && entry.version == source.table.Version {
		return entry.index
	}

	index := ragBuildContextIndex(source, docCol, chunkCol)

	ragContextIndexCacheMu.Lock()
	if _, exists := ragContextIndexCache[key]; !exists {
		evictOverCap(ragContextIndexCache, ragContextIndexCacheMaxEntries)
	}
	ragContextIndexCache[key] = ragContextIndexCacheEntry{
		table:   source.table,
		version: source.table.Version,
		index:   index,
	}
	ragContextIndexCacheMu.Unlock()
	return index
}

// RAG_CONTEXT loads neighboring chunks for a single retrieved chunk.
//
// Usage:
//
//	SELECT * FROM RAG_CONTEXT('chunks', 'doc_id', 'chunk_index', 'doc-1', 7, 2 [, 1])
//
// Arguments:
//
//	source table/CTE, document id column, chunk index column,
//	document id, center chunk index, chunks before, optional chunks after.
type RAGContextTableFunc struct{}

func (f *RAGContextTableFunc) Name() string { return "RAG_CONTEXT" }

func (f *RAGContextTableFunc) ValidateArgs(args []Expr) error {
	if len(args) < 6 || len(args) > 7 {
		return fmt.Errorf("RAG_CONTEXT requires 6-7 arguments: (source, doc_id_col, chunk_index_col, doc_id, chunk_index, before [, after])")
	}
	return nil
}

func (f *RAGContextTableFunc) Execute(ctx context.Context, args []Expr, env ExecEnv, row Row) (*ResultSet, error) {
	if err := f.ValidateArgs(args); err != nil {
		return nil, err
	}

	sourceName, err := ragStringArg(env, args, row, 0, "source")
	if err != nil {
		return nil, err
	}
	docCol, err := ragStringArg(env, args, row, 1, "doc_id_col")
	if err != nil {
		return nil, err
	}
	chunkCol, err := ragStringArg(env, args, row, 2, "chunk_index_col")
	if err != nil {
		return nil, err
	}
	docID, err := evalExpr(env, args[3], row)
	if err != nil {
		return nil, fmt.Errorf("RAG_CONTEXT doc_id: %w", err)
	}
	centerChunk, err := ragIntArg(env, args, row, 4, "chunk_index")
	if err != nil {
		return nil, err
	}
	before, err := ragIntArg(env, args, row, 5, "before")
	if err != nil {
		return nil, err
	}
	after := 0
	if len(args) == 7 {
		after, err = ragIntArg(env, args, row, 6, "after")
		if err != nil {
			return nil, err
		}
	}
	if before < 0 || after < 0 {
		return nil, fmt.Errorf("RAG_CONTEXT: before/after must be non-negative")
	}

	source, err := ragLoadSource(env, sourceName)
	if err != nil {
		return nil, err
	}

	// A single known chunk is cheaper to resolve with one direct scan. The
	// document index is reserved for RAG_CONTEXT_FROM, where it is reused by
	// many retrieval hits.
	matches := ragFindContextRows(source, docCol, chunkCol, docID, centerChunk, before, after)
	cols := append(append([]string{}, source.cols...), "_context_offset", "_context_rank")
	out := make([]Row, 0, len(matches))
	for i, m := range matches {
		r := source.outputRow(m.sourceRow)
		r["_context_offset"] = m.chunkIndex - centerChunk
		r["_context_rank"] = i + 1
		out = append(out, r)
	}
	return &ResultSet{Cols: cols, Rows: out}, nil
}

// RAG_CONTEXT_FROM expands multiple retrieval hits into neighboring chunks.
//
// Usage:
//
//	WITH topk AS (...)
//	SELECT * FROM RAG_CONTEXT_FROM('chunks', 'doc_id', 'chunk_index',
//	                               'topk', 'doc_id', 'chunk_index', 1 [, 1])
type RAGContextFromTableFunc struct{}

func (f *RAGContextFromTableFunc) Name() string { return "RAG_CONTEXT_FROM" }

func (f *RAGContextFromTableFunc) ValidateArgs(args []Expr) error {
	if len(args) < 7 || len(args) > 8 {
		return fmt.Errorf("RAG_CONTEXT_FROM requires 7-8 arguments: (source, doc_id_col, chunk_index_col, hits, hit_doc_id_col, hit_chunk_index_col, before [, after])")
	}
	return nil
}

func (f *RAGContextFromTableFunc) Execute(ctx context.Context, args []Expr, env ExecEnv, row Row) (*ResultSet, error) {
	if err := f.ValidateArgs(args); err != nil {
		return nil, err
	}

	sourceName, err := ragStringArg(env, args, row, 0, "source")
	if err != nil {
		return nil, err
	}
	docCol, err := ragStringArg(env, args, row, 1, "doc_id_col")
	if err != nil {
		return nil, err
	}
	chunkCol, err := ragStringArg(env, args, row, 2, "chunk_index_col")
	if err != nil {
		return nil, err
	}
	hitsName, err := ragStringArg(env, args, row, 3, "hits")
	if err != nil {
		return nil, err
	}
	hitDocCol, err := ragStringArg(env, args, row, 4, "hit_doc_id_col")
	if err != nil {
		return nil, err
	}
	hitChunkCol, err := ragStringArg(env, args, row, 5, "hit_chunk_index_col")
	if err != nil {
		return nil, err
	}
	before, err := ragIntArg(env, args, row, 6, "before")
	if err != nil {
		return nil, err
	}
	after := 0
	if len(args) == 8 {
		after, err = ragIntArg(env, args, row, 7, "after")
		if err != nil {
			return nil, err
		}
	}
	if before < 0 || after < 0 {
		return nil, fmt.Errorf("RAG_CONTEXT_FROM: before/after must be non-negative")
	}

	source, err := ragLoadSource(env, sourceName)
	if err != nil {
		return nil, err
	}
	hits, err := ragLoadSource(env, hitsName)
	if err != nil {
		return nil, err
	}
	// Validate every hit row's doc/chunk columns up front, so a malformed or
	// mistyped hit set still produces the original hard error instead of
	// ragExpandContextFrom's own lenient per-row skip (that leniency exists
	// so ragExpandContextFrom can also serve RAG_SEARCH's in-memory, not
	// necessarily uniform, fused result set — see its doc comment — but
	// RAG_CONTEXT_FROM's own contract with real callers is to fail loudly on
	// a bad column name or a non-numeric chunk index, exactly as it did
	// before the extract-method refactor).
	for hitIdx := 0; hitIdx < hits.len(); hitIdx++ {
		hit := hits.outputRow(hitIdx)
		if _, ok := ragValue(hit, hitDocCol); !ok {
			return nil, fmt.Errorf("RAG_CONTEXT_FROM: hit column %q not found", hitDocCol)
		}
		chunkVal, ok := ragValue(hit, hitChunkCol)
		if !ok {
			return nil, fmt.Errorf("RAG_CONTEXT_FROM: hit column %q not found", hitChunkCol)
		}
		if _, err := toInt(chunkVal); err != nil {
			return nil, fmt.Errorf("RAG_CONTEXT_FROM %s: %w", hitChunkCol, err)
		}
	}

	return ragExpandContextFrom(source, hits, docCol, chunkCol, hitDocCol, hitChunkCol, before, after), nil
}

// ragExpandContextFrom implements the neighbor-expansion core of
// RAG_CONTEXT_FROM: build a per-document chunk index over source, then for
// each row in hits find neighboring chunks (within before/after of the hit's
// chunk position), merging overlapping provenance across hits and ranking by
// (hit rank, hit order, |offset|, chunk identity). Extracted verbatim from
// RAGContextFromTableFunc.Execute so RAG_SEARCH (rag_search.go) can reuse the
// exact same expansion logic against an in-memory fused result set instead of
// a named table/CTE.
func ragExpandContextFrom(source, hits ragSource, docCol, chunkCol, hitDocCol, hitChunkCol string, before, after int) *ResultSet {
	contexts := getRAGContextIndex(source, docCol, chunkCol)
	cols := append(append([]string{}, source.cols...), "_hit_rank", "_context_offset", "_context_rank", "_context_hits")
	candidates := make(map[ragContextKey]*ragContextCandidate)
	for hitIdx := 0; hitIdx < hits.len(); hitIdx++ {
		hit := hits.outputRow(hitIdx)
		docID, ok := ragValue(hit, hitDocCol)
		if !ok {
			// Column presence is validated up front by the caller (see
			// RAGContextFromTableFunc.Execute) against every current caller's
			// hit set; a row that still lacks it (e.g. a sparse in-memory
			// fused result set built directly by RAG_SEARCH) is skipped rather
			// than aborting the whole expansion, since this helper has no
			// error return.
			continue
		}
		chunkVal, ok := ragValue(hit, hitChunkCol)
		if !ok {
			continue
		}
		centerChunk, err := toInt(chunkVal)
		if err != nil {
			continue
		}

		matches := contexts.find(docID, centerChunk, before, after)
		hitRank := ragHitRank(hit, hitIdx+1)
		for _, m := range matches {
			key := ragContextIdentity(m.docID, m.chunkIndex)
			offset := m.chunkIndex - centerChunk
			if existing, ok := candidates[key]; ok {
				existing.hitCount++
				if ragBetterContextProvenance(hitRank, offset, hitIdx, existing) {
					existing.hitRank = hitRank
					existing.offset = offset
					existing.hitIndex = hitIdx
				}
				continue
			}
			candidates[key] = &ragContextCandidate{
				context:  m,
				hitRank:  hitRank,
				offset:   offset,
				hitIndex: hitIdx,
				hitCount: 1,
			}
		}
	}

	ordered := make([]*ragContextCandidate, 0, len(candidates))
	for _, candidate := range candidates {
		ordered = append(ordered, candidate)
	}
	// sort.Slice already runs the same unstable pdqsort as sort.Sort; the win
	// here is only in the Swap. sort.Slice builds its Swap via reflect.Swapper,
	// which falls back to a generic, pointer-aware swap for any element type
	// containing a pointer — *ragContextCandidate is one word but still a
	// pointer (same rationale as orderedRawRowsAsc in
	// exec_fastpath_select.go). A concrete sort.Interface's Swap is a direct
	// two-word assignment instead.
	sort.Sort(ragContextCandidatesAsc(ordered))

	out := make([]Row, 0, len(ordered))
	for rank, candidate := range ordered {
		r := source.outputRow(candidate.context.sourceRow)
		r["_hit_rank"] = candidate.hitRank
		r["_context_offset"] = candidate.offset
		r["_context_rank"] = rank + 1
		r["_context_hits"] = candidate.hitCount
		out = append(out, r)
	}
	return &ResultSet{Cols: cols, Rows: out}
}

type ragSource struct {
	cols        []string
	rows        []Row // CTE result rows
	rawRows     [][]any
	columnIdx   map[string]int
	tableSource bool
	// tenant and table identify a named-table source so its neighbor-chunk
	// index can be cached and invalidated by table.Version. Both are zero for
	// CTE and in-memory sources, which are per-query values with no stable
	// identity to cache under.
	tenant string
	table  *storage.Table
}

type ragContextRow struct {
	sourceRow  int
	docID      any
	chunkIndex int
}

// ragContextIndex groups chunks by document and keeps each group in chunk
// order. RAG_CONTEXT_FROM builds it once per source instead of scanning the
// entire source for every retrieval hit.
type ragContextIndex struct {
	byDocument map[ragDocumentKey][]ragContextRow
}

// ragDocumentKey is comparable without allocating for the text document IDs
// that dominate RAG datasets. Numeric values share one key kind to preserve
// rawEqual's int/int64/float64 comparison behavior.
type ragDocumentKey struct {
	kind   uint8
	text   string
	number float64
	bool   bool
}

type ragContextKey struct {
	document   ragDocumentKey
	chunkIndex int
}

type ragContextCandidate struct {
	context  ragContextRow
	hitRank  int
	offset   int
	hitIndex int
	hitCount int
}

// ragContextCandidatesAsc gives []*ragContextCandidate a concrete Swap; see
// its use in ragExpandContextFrom for why reflect.Swapper needs sidestepping
// here.
type ragContextCandidatesAsc []*ragContextCandidate

func (s ragContextCandidatesAsc) Len() int { return len(s) }
func (s ragContextCandidatesAsc) Less(i, j int) bool {
	left, right := s[i], s[j]
	if left.hitRank != right.hitRank {
		return left.hitRank < right.hitRank
	}
	if left.hitIndex != right.hitIndex {
		return left.hitIndex < right.hitIndex
	}
	if left.offset != right.offset {
		return left.offset < right.offset
	}
	return ragContextKeyLess(ragContextIdentity(left.context.docID, left.context.chunkIndex), ragContextIdentity(right.context.docID, right.context.chunkIndex))
}
func (s ragContextCandidatesAsc) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

// ragContextRowsAsc orders []ragContextRow by chunkIndex, falling back to
// original position on ties (duplicate chunk indexes for one document are
// possible with imperfect input data). The idx tie-break is what lets
// sort.Sort (pdqsort) replace sort.SliceStable's O(n log² n) merge while
// reproducing the exact stable order — same pattern as orderedRawRowsAsc's
// idx field (exec_fastpath_select.go), and a concrete Swap sidesteps
// reflect.Swapper the same way: ragContextRow embeds an `any` (docID), so it
// contains a pointer and misses reflect.Swapper's non-pointer fast paths
// regardless of the struct's size.
type ragContextRowsAsc struct {
	rows []ragContextRow
	idx  []int32
}

func newRagContextRowsAsc(rows []ragContextRow) ragContextRowsAsc {
	idx := make([]int32, len(rows))
	for i := range idx {
		idx[i] = int32(i)
	}
	return ragContextRowsAsc{rows: rows, idx: idx}
}

func (s ragContextRowsAsc) Len() int { return len(s.idx) }
func (s ragContextRowsAsc) Less(i, j int) bool {
	pi, pj := s.idx[i], s.idx[j]
	if s.rows[pi].chunkIndex != s.rows[pj].chunkIndex {
		return s.rows[pi].chunkIndex < s.rows[pj].chunkIndex
	}
	return pi < pj
}
func (s ragContextRowsAsc) Swap(i, j int) {
	s.rows[i], s.rows[j] = s.rows[j], s.rows[i]
	s.idx[i], s.idx[j] = s.idx[j], s.idx[i]
}

// sortRagContextRows sorts rows by chunkIndex in place, preserving relative
// order among duplicate chunk indexes exactly like the sort.SliceStable call
// it replaces.
func sortRagContextRows(rows []ragContextRow) {
	sort.Sort(newRagContextRowsAsc(rows))
}

func ragStringArg(env ExecEnv, args []Expr, row Row, idx int, name string) (string, error) {
	val, err := evalExpr(env, args[idx], row)
	if err != nil {
		return "", fmt.Errorf("RAG %s: %w", name, err)
	}
	s, ok := val.(string)
	if !ok {
		return "", fmt.Errorf("RAG %s must be a string, got %T", name, val)
	}
	return s, nil
}

func ragIntArg(env ExecEnv, args []Expr, row Row, idx int, name string) (int, error) {
	val, err := evalExpr(env, args[idx], row)
	if err != nil {
		return 0, fmt.Errorf("RAG %s: %w", name, err)
	}
	n, err := toInt(val)
	if err != nil {
		return 0, fmt.Errorf("RAG %s: %w", name, err)
	}
	return n, nil
}

func ragLoadSource(env ExecEnv, name string) (ragSource, error) {
	if env.ctes != nil {
		for cteName, rs := range env.ctes {
			if strings.EqualFold(cteName, name) {
				return ragSource{cols: rs.Cols, rows: rs.Rows}, nil
			}
		}
	}

	tenant := env.tenant
	if tenant == "" {
		tenant = "default"
	}
	table, err := env.db.Get(tenant, name)
	if err != nil {
		return ragSource{}, fmt.Errorf("RAG source %q not found: %w", name, err)
	}

	cols := colNames(table.Cols)
	columnIdx := make(map[string]int, len(table.Cols))
	for i, col := range table.Cols {
		columnIdx[strings.ToLower(col.Name)] = i
	}
	return ragSource{cols: cols, rawRows: table.Rows, columnIdx: columnIdx, tableSource: true,
		tenant: tenant, table: table}, nil
}

func (source ragSource) len() int {
	if source.tableSource {
		return len(source.rawRows)
	}
	return len(source.rows)
}

func (source ragSource) value(rowIndex int, col string) (any, bool) {
	if !source.tableSource {
		return ragValue(source.rows[rowIndex], col)
	}
	columnIndex, ok := source.columnIdx[strings.ToLower(col)]
	if !ok || columnIndex >= len(source.rawRows[rowIndex]) {
		return nil, false
	}
	return source.rawRows[rowIndex][columnIndex], true
}

func (source ragSource) outputRow(rowIndex int) Row {
	if !source.tableSource {
		return ragCopyOutputRow(source.cols, source.rows[rowIndex])
	}
	out := make(Row, len(source.cols)+3)
	raw := source.rawRows[rowIndex]
	for i, col := range source.cols {
		key := strings.ToLower(col)
		if i < len(raw) {
			out[key] = raw[i]
		} else {
			out[key] = nil
		}
	}
	return out
}

func ragFindContextRows(source ragSource, docCol, chunkCol string, docID any, centerChunk, before, after int) []ragContextRow {
	minChunk := centerChunk - before
	maxChunk := centerChunk + after
	matches := make([]ragContextRow, 0, before+after+1)
	for rowIndex := 0; rowIndex < source.len(); rowIndex++ {
		docVal, ok := source.value(rowIndex, docCol)
		if !ok || !rawEqual(docVal, docID) {
			continue
		}
		chunkVal, ok := source.value(rowIndex, chunkCol)
		if !ok {
			continue
		}
		chunkIndex, err := toInt(chunkVal)
		if err != nil || chunkIndex < minChunk || chunkIndex > maxChunk {
			continue
		}
		matches = append(matches, ragContextRow{sourceRow: rowIndex, docID: docVal, chunkIndex: chunkIndex})
	}
	sortRagContextRows(matches)
	return matches
}

func ragBuildContextIndex(source ragSource, docCol, chunkCol string) ragContextIndex {
	contexts := ragContextIndex{byDocument: make(map[ragDocumentKey][]ragContextRow)}

	if source.tableSource {
		// Resolve the two column positions once rather than per cell.
		// source.value lower-cases the column name and hashes it on every call,
		// so scanning a 20,000-row corpus for two columns cost 40,000
		// strings.ToLower calls and 40,000 map lookups before reading any data.
		docIdx, docOK := source.columnIdx[strings.ToLower(docCol)]
		chunkIdx, chunkOK := source.columnIdx[strings.ToLower(chunkCol)]
		if !docOK || !chunkOK {
			// Same outcome as the generic loop below, which would skip every row.
			return contexts
		}
		for rowIndex, raw := range source.rawRows {
			if docIdx >= len(raw) || chunkIdx >= len(raw) {
				continue
			}
			chunkIndex, err := toInt(raw[chunkIdx])
			if err != nil {
				continue
			}
			docVal := raw[docIdx]
			key := ragContextDocumentKey(docVal)
			contexts.byDocument[key] = append(contexts.byDocument[key], ragContextRow{
				sourceRow:  rowIndex,
				docID:      docVal,
				chunkIndex: chunkIndex,
			})
		}
		ragSortContextIndex(contexts)
		return contexts
	}

	for rowIndex := 0; rowIndex < source.len(); rowIndex++ {
		docVal, ok := source.value(rowIndex, docCol)
		if !ok {
			continue
		}
		chunkVal, ok := source.value(rowIndex, chunkCol)
		if !ok {
			continue
		}
		chunkIndex, err := toInt(chunkVal)
		if err != nil {
			continue
		}
		key := ragContextDocumentKey(docVal)
		contexts.byDocument[key] = append(contexts.byDocument[key], ragContextRow{
			sourceRow:  rowIndex,
			docID:      docVal,
			chunkIndex: chunkIndex,
		})
	}

	ragSortContextIndex(contexts)
	return contexts
}

// ragSortContextIndex orders each document's chunks by chunk index, which is
// what lets find() binary-search a neighbor window.
func ragSortContextIndex(contexts ragContextIndex) {
	for _, chunks := range contexts.byDocument {
		sortRagContextRows(chunks)
	}
}

func (idx ragContextIndex) find(docID any, centerChunk, before, after int) []ragContextRow {
	chunks := idx.byDocument[ragContextDocumentKey(docID)]
	if len(chunks) == 0 {
		return nil
	}
	minChunk := centerChunk - before
	maxChunk := centerChunk + after
	start := sort.Search(len(chunks), func(i int) bool { return chunks[i].chunkIndex >= minChunk })
	end := sort.Search(len(chunks), func(i int) bool { return chunks[i].chunkIndex > maxChunk })
	matches := chunks[start:end]

	// The key preserves rawEqual's cross-numeric-type behavior; retain the
	// final equality check for edge values such as NaN.
	if len(matches) == 0 {
		return nil
	}
	for i, match := range matches {
		if rawEqual(match.docID, docID) {
			continue
		}
		filtered := make([]ragContextRow, 0, len(matches)-1)
		filtered = append(filtered, matches[:i]...)
		for _, remaining := range matches[i+1:] {
			if rawEqual(remaining.docID, docID) {
				filtered = append(filtered, remaining)
			}
		}
		return filtered
	}
	return matches
}

func ragContextDocumentKey(docID any) ragDocumentKey {
	switch v := docID.(type) {
	case nil:
		return ragDocumentKey{kind: 1}
	case int:
		return ragDocumentKey{kind: 2, number: float64(v)}
	case int64:
		return ragDocumentKey{kind: 2, number: float64(v)}
	case float64:
		return ragDocumentKey{kind: 2, number: v}
	case string:
		return ragDocumentKey{kind: 3, text: v}
	case bool:
		return ragDocumentKey{kind: 4, bool: v}
	default:
		// rawEqual cannot match this type, so all unsupported values may share
		// a key. The final rawEqual check in find filters them back out.
		return ragDocumentKey{}
	}
}

func ragContextIdentity(docID any, chunkIndex int) ragContextKey {
	return ragContextKey{document: ragContextDocumentKey(docID), chunkIndex: chunkIndex}
}

func ragContextKeyLess(left, right ragContextKey) bool {
	if left.document.kind != right.document.kind {
		return left.document.kind < right.document.kind
	}
	if left.document.text != right.document.text {
		return left.document.text < right.document.text
	}
	if left.document.number != right.document.number {
		return left.document.number < right.document.number
	}
	if left.document.bool != right.document.bool {
		return !left.document.bool && right.document.bool
	}
	return left.chunkIndex < right.chunkIndex
}

func ragBetterContextProvenance(hitRank, offset, hitIndex int, current *ragContextCandidate) bool {
	if hitRank != current.hitRank {
		return hitRank < current.hitRank
	}
	if absInt(offset) != absInt(current.offset) {
		return absInt(offset) < absInt(current.offset)
	}
	return hitIndex < current.hitIndex
}

func absInt(v int) int {
	if v < 0 {
		return -v
	}
	return v
}

func ragValue(row Row, col string) (any, bool) {
	lower := strings.ToLower(col)
	if v, ok := row[lower]; ok {
		return v, true
	}
	for k, v := range row {
		if strings.EqualFold(k, col) {
			return v, true
		}
	}
	return nil, false
}

func ragCopyOutputRow(cols []string, src Row) Row {
	out := make(Row, len(cols)+3)
	for _, col := range cols {
		key := strings.ToLower(col)
		if v, ok := ragValue(src, col); ok {
			out[key] = v
		} else {
			out[key] = nil
		}
	}
	return out
}

// ragHitRank resolves a retrieval hit's rank for context-expansion provenance
// ordering. _rrf_rank is checked before _vec_rank: a RAG_SEARCH hybrid-mode
// hit set can carry both (a row matched by the vector pass keeps its partial
// _vec_rank alongside the final fused _rrf_rank), and _rrf_rank — reflecting
// the combined vector+text ranking — is the more authoritative signal for a
// hybrid hit set. Callers whose rows never carry "_rrf_rank" (every existing
// RAG_CONTEXT_FROM caller before RAG_SEARCH) are unaffected: the lookup for
// that column simply never matches, falling through exactly as before.
func ragHitRank(hit Row, fallback int) int {
	for _, col := range []string{"_rrf_rank", "_vec_rank", "_hit_rank", "rank"} {
		if v, ok := ragValue(hit, col); ok {
			if n, err := toInt(v); err == nil {
				return n
			}
		}
	}
	return fallback
}

func init() {
	RegisterTableFunc(&RAGContextTableFunc{})
	RegisterTableFunc(&RAGContextFromTableFunc{})
}
