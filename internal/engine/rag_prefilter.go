package engine

// Retrieval pre-filters for the RAG table-valued functions.
//
// A SQL WHERE following a table-valued retrieval function is necessarily a
// post-filter: the function has already selected its candidate slots. That is
// fine for presentation, but it is the wrong place for tenant/ACL boundaries
// and loses recall whenever disallowed rows consume a top-k slot. The public
// `pre_filter` options below are deliberately separate from WHERE and are
// applied before vector, FTS, and RRF ranking.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/big"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// ragPreFilterOptions is the explicit, before-ranking restriction accepted by
// RAG_SEARCH/HYBRID_SEARCH and the *_FILTERED table functions.
//
// allowed_row_ids contains stable values from id_column (or the source table's
// single-column PRIMARY KEY when id_column is omitted). It intentionally does
// not expose physical row offsets: offsets can move after DELETE and must never
// become an application authorization token. equals is an AND of equality
// metadata predicates. Both restrictions may be combined and are intersected.
//
// Values are JSON values and are coerced to their target column's type before
// matching/index lookup, so {"tenant_id": 42} correctly reaches an INT
// tenant_id column rather than looking up float64(42) in a typed index.
type ragPreFilterOptions struct {
	IDColumn      string         `json:"id_column"`
	AllowedRowIDs []any          `json:"allowed_row_ids"`
	Equals        map[string]any `json:"equals"`
}

// ragRowFilter is an immutable, sorted physical-row set shared by the vector
// and FTS branches of one retrieval. Keeping the set as row IDs avoids copying
// source rows before final materialization and lets FTS intersect it directly
// with postings lists.
type ragRowFilter struct {
	rows []int
}

func (f *ragRowFilter) empty() bool { return f != nil && len(f.rows) == 0 }

// The resolved authorization subset is commonly stable across many requests
// from one principal. Cache the immutable row-ID set by table version and a
// canonical options representation, just as FTS caches a prepared query plan.
// This keeps an indexed tenant filter from allocating/scanning its matching
// rows on every natural-language query.
const ragRowFilterCacheMaxEntries = 256

type ragRowFilterCacheKey struct {
	table   *storage.Table
	version int
	spec    string
}

var (
	ragRowFilterCacheMu sync.RWMutex
	ragRowFilterCache   = make(map[ragRowFilterCacheKey]*ragRowFilter)
)

type ragEqualityPredicate struct {
	column string
	pos    int
	value  any
}

// ragParseSearchOptions decodes the JSON option object shared by RAG_SEARCH,
// HYBRID_SEARCH, and the standalone filtered retrievers. Keeping parsing in
// one place makes `pre_filter` identical across package and SQL-tool callers.
func ragParseSearchOptions(raw string) (ragSearchOptions, error) {
	var opts ragSearchOptions
	if strings.TrimSpace(raw) == "" {
		return opts, nil
	}
	decoder := json.NewDecoder(strings.NewReader(raw))
	// Authorization IDs are often 64-bit database IDs. json.Unmarshal turns
	// numbers inside []any/map[string]any into float64 and would silently round
	// values above 2^53; preserve them until the target column's type is known.
	decoder.UseNumber()
	if err := decoder.Decode(&opts); err != nil {
		return opts, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return opts, fmt.Errorf("multiple JSON values")
		}
		return opts, err
	}
	// A missing pre_filter keeps RAG_SEARCH's legacy unrestricted behavior,
	// but an explicitly supplied null must never silently turn an intended ACL
	// boundary into that unrestricted search. The struct decoder represents both
	// cases as a nil pointer, so inspect the original object for field presence.
	var fields map[string]json.RawMessage
	if err := json.Unmarshal([]byte(raw), &fields); err != nil {
		return opts, err
	}
	for name, value := range fields {
		if strings.EqualFold(name, "pre_filter") && strings.TrimSpace(string(value)) == "null" {
			return opts, fmt.Errorf("pre_filter must not be null")
		}
	}
	return opts, nil
}

func ragGetSourceTable(env ExecEnv, tableName string) (*storage.Table, string, error) {
	tenant := env.tenant
	if tenant == "" {
		tenant = "default"
	}
	table, err := env.db.Get(tenant, tableName)
	if err != nil {
		return nil, tenant, err
	}
	return table, tenant, nil
}

// ragBuildRowFilter resolves a public pre_filter to physical source-row IDs.
// nil means no restriction; an allocated filter with zero rows means the
// caller explicitly authorized no rows and must return no hits.
func ragBuildRowFilter(table *storage.Table, opts *ragPreFilterOptions) (*ragRowFilter, error) {
	if opts == nil {
		return nil, nil
	}
	if opts.AllowedRowIDs == nil && len(opts.Equals) == 0 {
		// Treating an empty object as an unrestricted search would be an
		// especially dangerous foot-gun for the dedicated *_FILTERED APIs:
		// callers could believe they installed an ACL boundary while returning
		// every row. An explicit empty allowed_row_ids array is still valid and
		// deliberately means deny all.
		return nil, fmt.Errorf("pre_filter requires allowed_row_ids or at least one equals predicate")
	}
	if opts.AllowedRowIDs == nil && strings.TrimSpace(opts.IDColumn) != "" {
		return nil, fmt.Errorf("pre_filter.id_column requires allowed_row_ids")
	}
	key, cacheable := ragRowFilterCacheKeyFor(table, opts)
	if cacheable {
		ragRowFilterCacheMu.RLock()
		cached := ragRowFilterCache[key]
		ragRowFilterCacheMu.RUnlock()
		if cached != nil {
			return cached, nil
		}
	}

	var selected []int
	if opts.AllowedRowIDs != nil {
		column, pos, err := ragAllowedIDColumn(table, opts.IDColumn)
		if err != nil {
			return nil, err
		}
		values := make([]any, 0, len(opts.AllowedRowIDs))
		for _, raw := range opts.AllowedRowIDs {
			if raw == nil {
				return nil, fmt.Errorf("pre_filter.allowed_row_ids must not contain null")
			}
			value, err := ragCoercePreFilterValue(raw, table.Cols[pos])
			if err != nil {
				return nil, fmt.Errorf("pre_filter.allowed_row_ids for %q: %w", column, err)
			}
			values = append(values, value)
		}
		rows, err := ragRowsForAllowedIDs(table, column, pos, values)
		if err != nil {
			return nil, err
		}
		selected = ragNormalizeRowIDs(len(table.Rows), rows)
	}

	predicates, err := ragNormalizeEqualityPredicates(table, opts.Equals)
	if err != nil {
		return nil, err
	}
	if len(predicates) > 0 {
		rows, err := ragRowsForEqualities(table, predicates)
		if err != nil {
			return nil, err
		}
		if opts.AllowedRowIDs == nil {
			selected = rows
		} else {
			selected = ragIntersectRowIDs(selected, rows)
		}
	}

	filter := &ragRowFilter{rows: selected}
	if cacheable {
		ragRowFilterCacheMu.Lock()
		if cached := ragRowFilterCache[key]; cached != nil {
			ragRowFilterCacheMu.Unlock()
			return cached, nil
		}
		if len(ragRowFilterCache) >= ragRowFilterCacheMaxEntries {
			evictOverCap(ragRowFilterCache, ragRowFilterCacheMaxEntries)
		}
		ragRowFilterCache[key] = filter
		ragRowFilterCacheMu.Unlock()
	}
	return filter, nil
}

func ragRowFilterCacheKeyFor(table *storage.Table, opts *ragPreFilterOptions) (ragRowFilterCacheKey, bool) {
	// encoding/json sorts map keys, so semantically identical equality objects
	// share one cache entry regardless of their input-key order. If an internal
	// caller supplied a non-JSON-marshallable value, keep the normal exact path
	// and let validation return its usual error rather than inventing a key.
	raw, err := json.Marshal(opts)
	if err != nil {
		return ragRowFilterCacheKey{}, false
	}
	return ragRowFilterCacheKey{table: table, version: table.Version, spec: string(raw)}, true
}

func ragAllowedIDColumn(table *storage.Table, requested string) (string, int, error) {
	if strings.TrimSpace(requested) != "" {
		pos, err := table.ColIndex(requested)
		if err != nil {
			return "", 0, fmt.Errorf("pre_filter.id_column %q: %w", requested, err)
		}
		return table.Cols[pos].Name, pos, nil
	}

	pos := -1
	for i, column := range table.Cols {
		if column.Constraint != storage.PrimaryKey {
			continue
		}
		if pos >= 0 {
			return "", 0, fmt.Errorf("pre_filter.allowed_row_ids needs id_column for a composite PRIMARY KEY")
		}
		pos = i
	}
	if pos < 0 {
		return "", 0, fmt.Errorf("pre_filter.allowed_row_ids needs id_column or a single-column PRIMARY KEY")
	}
	return table.Cols[pos].Name, pos, nil
}

func ragNormalizeEqualityPredicates(table *storage.Table, values map[string]any) ([]ragEqualityPredicate, error) {
	if len(values) == 0 {
		return nil, nil
	}
	predicates := make([]ragEqualityPredicate, 0, len(values))
	seen := make(map[int]struct{}, len(values))
	for column, raw := range values {
		if strings.TrimSpace(column) == "" {
			return nil, fmt.Errorf("pre_filter.equals contains an empty column name")
		}
		if raw == nil {
			return nil, fmt.Errorf("pre_filter.equals.%s must not be null", column)
		}
		pos, err := table.ColIndex(column)
		if err != nil {
			return nil, fmt.Errorf("pre_filter.equals.%s: %w", column, err)
		}
		if _, duplicate := seen[pos]; duplicate {
			return nil, fmt.Errorf("pre_filter.equals specifies column %q more than once", column)
		}
		value, err := ragCoercePreFilterValue(raw, table.Cols[pos])
		if err != nil {
			return nil, fmt.Errorf("pre_filter.equals.%s: %w", column, err)
		}
		predicates = append(predicates, ragEqualityPredicate{column: table.Cols[pos].Name, pos: pos, value: value})
		seen[pos] = struct{}{}
	}
	// The public JSON object is unordered. A deterministic predicate order keeps
	// index selection and errors stable regardless of Go map iteration order.
	sort.Slice(predicates, func(i, j int) bool { return predicates[i].pos < predicates[j].pos })
	return predicates, nil
}

// ragCoercePreFilterValue is coerceColumnValue plus lossless json.Number
// handling for options_json's untyped pre_filter values. The normal INSERT
// path never sees json.Number, but this public JSON boundary must not turn a
// large numeric ACL ID into a different floating-point number.
func ragCoercePreFilterValue(raw any, column storage.Column) (any, error) {
	number, ok := raw.(json.Number)
	if !ok {
		return coerceColumnValue(raw, column)
	}
	integer := func() (any, error) {
		// json.Number.Int64 deliberately rejects JSON spellings such as 1.0
		// and 1e0 even though they denote the same exact SQL integer. Parse the
		// decimal rational instead, then require an exact integer and a native
		// int range before converting. This accepts lossless representations
		// without rounding a large ACL ID through float64.
		rational, ok := new(big.Rat).SetString(number.String())
		if !ok || !rational.IsInt() || !rational.Num().IsInt64() {
			return nil, fmt.Errorf("cannot convert JSON number %q to integer without loss", number)
		}
		value := rational.Num().Int64()
		if int64(int(value)) != value {
			return nil, fmt.Errorf("cannot convert JSON number %q to integer: out of range", number)
		}
		return int(value), nil
	}
	real := func() (any, error) {
		value, err := number.Float64()
		if err != nil {
			return nil, fmt.Errorf("cannot convert JSON number %q to float: %w", number, err)
		}
		return value, nil
	}

	switch column.Affinity {
	case storage.AffinityInteger:
		return integer()
	case storage.AffinityReal:
		return real()
	case storage.AffinityText:
		return number.String(), nil
	case storage.AffinityNumeric:
		if value, err := integer(); err == nil {
			return value, nil
		}
		return real()
	}
	switch column.Type {
	case storage.IntType, storage.Int8Type, storage.Int16Type, storage.Int32Type, storage.Int64Type,
		storage.UintType, storage.Uint8Type, storage.Uint16Type, storage.Uint32Type, storage.Uint64Type:
		return integer()
	case storage.Float32Type, storage.Float64Type, storage.FloatType:
		return real()
	case storage.StringType, storage.TextType:
		return number.String(), nil
	case storage.JsonType, storage.JsonbType:
		// JSON cells produced by the standard decoder use float64 for number
		// leaves, so use the same representation for equality here.
		return real()
	default:
		return nil, fmt.Errorf("cannot use JSON number %q with %s column in pre_filter", number, column.Type)
	}
}

func ragRowsForAllowedIDs(table *storage.Table, column string, pos int, values []any) ([]int, error) {
	if len(values) == 0 {
		return nil, nil
	}
	idx := ragLeadingIndex(table, column)
	if idx != nil {
		for _, value := range values {
			if isNumericSQLValue(value) && !numericSecondaryIndexSeekSafe(table, pos, value) {
				idx = nil
				break
			}
		}
	}
	if idx != nil {
		rows := make([]int, 0, len(values))
		for _, value := range values {
			// Prefix lookup intentionally supports an index such as
			// (chunk_id, version), not only an index consisting of id alone.
			ids, err := table.LookupSecondaryIndexPrefix(idx, []any{value})
			if err != nil {
				return nil, fmt.Errorf("pre_filter.allowed_row_ids index %q: %w", idx.Name, err)
			}
			rows = append(rows, ids...)
		}
		return rows, nil
	}

	// A primary/unique constraint does not need to be duplicated as a secondary
	// index for correctness. The scan fallback preserves SQL equality semantics
	// for mixed SQLite-affinity numeric columns where a type-tagged seek could
	// otherwise miss a numerically equal value.
	rows := make([]int, 0, len(values))
	for rowID, row := range table.Rows {
		if pos >= len(row) {
			continue
		}
		for _, value := range values {
			if ragFilterValuesEqual(row[pos], value) {
				rows = append(rows, rowID)
				break
			}
		}
	}
	return rows, nil
}

func ragRowsForEqualities(table *storage.Table, predicates []ragEqualityPredicate) ([]int, error) {
	var candidates []int
	if idx, values := ragBestEqualityIndex(table, predicates); idx != nil {
		var err error
		if len(values) == len(idx.Columns) {
			candidates, err = table.LookupSecondaryIndexPoint(idx, values)
		} else {
			candidates, err = table.LookupSecondaryIndexPrefix(idx, values)
		}
		if err != nil {
			return nil, fmt.Errorf("pre_filter.equals index %q: %w", idx.Name, err)
		}
	} else {
		candidates = make([]int, len(table.Rows))
		for i := range candidates {
			candidates[i] = i
		}
	}

	// Even an exact index prefix only covers its leading columns. Checking every
	// predicate here is cheap over the narrowed row set and is the correctness
	// backstop for trailing predicates and SQLite-compatible comparisons. Both
	// index lookup APIs and the scan fallback enumerate physical row IDs in
	// ascending order, and this loop keeps each at most once, so the result is
	// already the sorted unique form ragRowFilter needs (no defensive copy).
	rows := make([]int, 0, len(candidates))
	for _, rowID := range candidates {
		if rowID < 0 || rowID >= len(table.Rows) || ragRowMatchesEqualities(table.Rows[rowID], predicates) == false {
			continue
		}
		rows = append(rows, rowID)
	}
	return rows, nil
}

// ragBestEqualityIndex chooses the longest safe equality prefix. It mirrors
// the normal SELECT planner's numeric-tag safety rule so a pre-filter cannot
// turn a compatible scan into a false-negative typed index lookup.
func ragBestEqualityIndex(table *storage.Table, predicates []ragEqualityPredicate) (*storage.SecondaryIndex, []any) {
	if len(table.Indexes) == 0 {
		return nil, nil
	}
	byColumn := make(map[string]ragEqualityPredicate, len(predicates))
	for _, predicate := range predicates {
		byColumn[strings.ToLower(predicate.column)] = predicate
	}

	var best *storage.SecondaryIndex
	var bestValues []any
	for _, name := range sortedIndexNames(table) {
		idx := table.Indexes[name]
		if idx == nil {
			continue
		}
		values := make([]any, 0, len(idx.Columns))
		for _, column := range idx.Columns {
			predicate, ok := byColumn[strings.ToLower(column)]
			if !ok {
				break
			}
			if isNumericSQLValue(predicate.value) && !numericSecondaryIndexSeekSafe(table, predicate.pos, predicate.value) {
				break
			}
			values = append(values, predicate.value)
		}
		if len(values) > len(bestValues) {
			best, bestValues = idx, values
		}
	}
	if len(bestValues) == 0 {
		return nil, nil
	}
	return best, bestValues
}

func ragLeadingIndex(table *storage.Table, column string) *storage.SecondaryIndex {
	for _, name := range sortedIndexNames(table) {
		idx := table.Indexes[name]
		if idx != nil && len(idx.Columns) > 0 && strings.EqualFold(idx.Columns[0], column) {
			return idx
		}
	}
	return nil
}

func ragRowMatchesEqualities(row []any, predicates []ragEqualityPredicate) bool {
	for _, predicate := range predicates {
		if predicate.pos >= len(row) || !ragFilterValuesEqual(row[predicate.pos], predicate.value) {
			return false
		}
	}
	return true
}

func ragFilterValuesEqual(left, right any) bool {
	if left == nil || right == nil {
		return false
	}
	cmp, err := compare(left, right)
	return err == nil && cmp == 0
}

func ragNormalizeRowIDs(total int, rows []int) []int {
	if len(rows) == 0 {
		return nil
	}
	out := make([]int, 0, len(rows))
	for _, rowID := range rows {
		if rowID >= 0 && rowID < total {
			out = append(out, rowID)
		}
	}
	sort.Ints(out)
	write := 0
	for _, rowID := range out {
		if write == 0 || out[write-1] != rowID {
			out[write] = rowID
			write++
		}
	}
	return out[:write]
}

func ragIntersectRowIDs(left, right []int) []int {
	out := make([]int, 0, min(len(left), len(right)))
	i, j := 0, 0
	for i < len(left) && j < len(right) {
		switch {
		case left[i] < right[j]:
			i++
		case left[i] > right[j]:
			j++
		default:
			out = append(out, left[i])
			i++
			j++
		}
	}
	return out
}

// ragVecSearchCandidatesFiltered ranks only rows selected by pre_filter. Flat
// remains exact; an explicit HNSW mode builds an isolated graph for the filter
// instead of filtering a global ANN frontier afterwards.
func ragVecSearchCandidatesFiltered(ctx context.Context, env ExecEnv, a vecSearchArgs, table *storage.Table, filter *ragRowFilter) ([]vecScoredRow, error) {
	if filter == nil {
		_, rows, err := vecSearchCandidates(ctx, env, a)
		return rows, err
	}
	if filter.empty() {
		return nil, nil
	}
	vecColIdx, err := table.ColIndex(a.colName)
	if err != nil {
		return nil, fmt.Errorf("VEC_SEARCH: %w", err)
	}
	queryNorm := 0.0
	if a.metric == "cosine" {
		queryNorm = vectorL2Norm(a.queryVec)
	}
	searchCtx := ctx
	if searchCtx == nil {
		searchCtx = env.ctx
	}
	started := time.Now()
	tenant := env.tenant
	if tenant == "" {
		tenant = "default"
	}
	cache := getVecColumnCache(tenant, table, vecColIdx, a.metric == "cosine")
	distFn := buildVecDistanceFunc(a.metric, a.queryVec, queryNorm, cache)
	if a.indexMode == vecIndexHNSW && len(filter.rows) >= vecSearchParallelMinRows {
		idx, err := getRAGFilteredANNIndex(searchCtx, tenant, table, vecColIdx, a.metric, len(a.queryVec), filter, cache)
		if err != nil {
			return nil, err
		}
		rows, err := idx.searchFiltered(searchCtx, a.queryVec, queryNorm, a.k, cache, filter)
		if err != nil {
			return nil, err
		}
		finalizeVecScoredRows(a.metric, rows)
		recordVecQuery(VectorQueryEvent{At: time.Now(), Table: table.Name, Column: a.colName, Metric: a.metric, Index: "filter-hnsw", K: a.k, Duration: time.Since(started)})
		return rows, nil
	}
	rows, err := ragVecTopKAllowed(searchCtx, filter.rows, len(a.queryVec), a.k, cache, distFn)
	if err != nil {
		return nil, err
	}
	finalizeVecScoredRows(a.metric, rows)
	recordVecQuery(VectorQueryEvent{At: time.Now(), Table: table.Name, Column: a.colName, Metric: a.metric, Index: "flat", K: a.k, Duration: time.Since(started)})
	return rows, nil
}

func ragVecTopKAllowed(ctx context.Context, allowed []int, queryLen, k int, cache vecSearchColumnCacheEntry, distFn vecDistanceFunc) ([]vecScoredRow, error) {
	workers := vecSearchWorkerCount(len(allowed), queryLen)
	if workers == 1 {
		h, err := ragVecTopKAllowedRange(ctx, allowed, 0, len(allowed), queryLen, k, cache, distFn)
		if err != nil {
			return nil, err
		}
		return topKFromHeap(&h, k), nil
	}

	type workerResult struct {
		heapRows vecScoredHeap
		err      error
	}
	results := make([]workerResult, workers)
	chunk := (len(allowed) + workers - 1) / workers
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		start := worker * chunk
		end := min(start+chunk, len(allowed))
		if start >= end {
			continue
		}
		wg.Add(1)
		go func(worker, start, end int) {
			defer wg.Done()
			h, err := ragVecTopKAllowedRange(ctx, allowed, start, end, queryLen, k, cache, distFn)
			results[worker] = workerResult{heapRows: h, err: err}
		}(worker, start, end)
	}
	wg.Wait()

	merged := &vecScoredHeap{}
	for i := range results {
		if results[i].err != nil {
			return nil, results[i].err
		}
		for _, row := range topKFromHeap(&results[i].heapRows, k) {
			pushTopK(merged, row.rowIdx, row.distance, k)
		}
	}
	return topKFromHeap(merged, k), nil
}

func ragVecTopKAllowedRange(ctx context.Context, allowed []int, start, end, queryLen, k int, cache vecSearchColumnCacheEntry, distFn vecDistanceFunc) (vecScoredHeap, error) {
	heapRows := make(vecScoredHeap, 0, k)
	for i := start; i < end; i++ {
		if i&1023 == 0 {
			if err := checkCtx(ctx); err != nil {
				return nil, err
			}
		}
		rowID := allowed[i]
		if rowID < 0 || rowID >= cache.rowCount() || !cache.validAt(rowID) {
			continue
		}
		vec := cache.vector(rowID)
		if len(vec) != queryLen {
			continue
		}
		distance, ok := distFn(vec, rowID)
		if ok {
			pushTopK(&heapRows, rowID, distance, k)
		}
	}
	return heapRows, nil
}

// ragFTSSearchCandidatesFiltered intersects a postings-derived candidate list
// with the pre-filter before BM25 scoring. It can still reuse the corpus-wide
// token/postings cache, but derives BM25 N/DF/length statistics from the
// authorized rows so neither scores nor ranks depend on forbidden documents.
func ragFTSSearchCandidatesFiltered(ctx context.Context, tenant string, table *storage.Table, query string, k int, searchCols []int, filter *ragRowFilter) ([]ftsScored, error) {
	if filter == nil {
		return ftsSearchCandidates(ctx, tenant, table, query, k, searchCols)
	}
	if filter.empty() {
		return nil, nil
	}
	cache := getFTSDocCache(tenant, table, searchCols)
	node, candidates := prepareFTSQuery(tenant, table, searchCols, query, cache)
	if node == nil {
		return nil, nil
	}
	rows := ragCachedFTSFilterCandidates(table, searchCols, query, candidates, filter)
	if len(rows) == 0 {
		return nil, nil
	}
	// BM25's document frequency and average document length are ranking inputs.
	// Derive them from the authorized set too: using corpus-wide IDF after a
	// row filter would both make ranks depend on forbidden rows and expose a
	// small aggregate side channel through the public _fts_score values.
	filteredCache, idf := ragFilteredFTSStatistics(table, searchCols, cache, filter)
	node = ftsBindIDF(node, idf, filteredCache.termIDs)
	results, err := ftsScanTopK(ctx, filteredCache, node, nil, rows, true, k)
	if err != nil {
		return nil, fmt.Errorf("FTS_SEARCH: %w", err)
	}
	return results, nil
}

// A prepared FTS query already caches its wildcard expansion and postings
// candidate list. Cache the final intersection with a stable authorization set
// as well; otherwise a selective tenant filter still allocates an int32 list on
// every repeated question even though neither the query nor permissions moved.
const ragFilteredFTSCandidateCacheMaxEntries = 256

type ragFilteredFTSCandidateCacheKey struct {
	table   *storage.Table
	version int
	cols    string
	query   string
	filter  *ragRowFilter
}

var (
	ragFilteredFTSCandidateCacheMu sync.RWMutex
	ragFilteredFTSCandidateCache   = make(map[ragFilteredFTSCandidateCacheKey][]int32)
)

// BM25 statistics are independent of the textual query, so cache the
// authorized corpus size/average length separately from the query-specific
// postings intersection. Entries are immutable and version-scoped just like
// the prepared FTS query cache.
const ragFilteredFTSStatsCacheMaxEntries = 256

type ragFilteredFTSStatsCacheKey struct {
	table   *storage.Table
	version int
	cols    string
	filter  *ragRowFilter
}

type ragFilteredFTSStats struct {
	numDocs   int
	avgDocLen float64
}

var (
	ragFilteredFTSStatsCacheMu sync.RWMutex
	ragFilteredFTSStatsCache   = make(map[ragFilteredFTSStatsCacheKey]ragFilteredFTSStats)
)

func ragCachedFTSFilterCandidates(table *storage.Table, searchCols []int, query string, candidates ftsCandidates, filter *ragRowFilter) []int32 {
	key := ragFilteredFTSCandidateCacheKey{
		table: table, version: table.Version, cols: ftsColsCacheKey(searchCols), query: query, filter: filter,
	}
	ragFilteredFTSCandidateCacheMu.RLock()
	cached, ok := ragFilteredFTSCandidateCache[key]
	ragFilteredFTSCandidateCacheMu.RUnlock()
	if ok {
		return cached
	}

	rows := ragIntersectFTSCandidates(candidates, filter.rows)
	ragFilteredFTSCandidateCacheMu.Lock()
	if cached, ok := ragFilteredFTSCandidateCache[key]; ok {
		ragFilteredFTSCandidateCacheMu.Unlock()
		return cached
	}
	if len(ragFilteredFTSCandidateCache) >= ragFilteredFTSCandidateCacheMaxEntries {
		evictOverCap(ragFilteredFTSCandidateCache, ragFilteredFTSCandidateCacheMaxEntries)
	}
	ragFilteredFTSCandidateCache[key] = rows
	ragFilteredFTSCandidateCacheMu.Unlock()
	return rows
}

func ragIntersectFTSCandidates(candidates ftsCandidates, allowed []int) []int32 {
	if candidates.unrestricted {
		out := make([]int32, 0, len(allowed))
		for _, rowID := range allowed {
			out = append(out, int32(rowID))
		}
		return out
	}
	rows := make([]int32, 0, min(len(candidates.rows), len(allowed)))
	i, j := 0, 0
	for i < len(candidates.rows) && j < len(allowed) {
		candidate := int(candidates.rows[i])
		switch {
		case candidate < allowed[j]:
			i++
		case candidate > allowed[j]:
			j++
		default:
			rows = append(rows, candidates.rows[i])
			i++
			j++
		}
	}
	return rows
}

// ragFilteredFTSStatistics returns a cache view whose BM25 length
// normalization and IDF function are confined to filter. This intentionally
// keeps the shared term dictionary/postings arena corpus-wide for fast query
// parsing, but no unauthorized document contributes to an observable score.
func ragFilteredFTSStatistics(table *storage.Table, searchCols []int, cache ftsDocCacheEntry, filter *ragRowFilter) (ftsDocCacheEntry, ftsIDFFunc) {
	key := ragFilteredFTSStatsCacheKey{
		table: table, version: table.Version, cols: ftsColsCacheKey(searchCols), filter: filter,
	}
	ragFilteredFTSStatsCacheMu.RLock()
	stats, ok := ragFilteredFTSStatsCache[key]
	ragFilteredFTSStatsCacheMu.RUnlock()
	if !ok {
		var totalDocLen float64
		for _, rowID := range filter.rows {
			if rowID < 0 || rowID >= len(cache.docs) || !cache.docs[rowID].Valid {
				continue
			}
			stats.numDocs++
			totalDocLen += cache.docs[rowID].DocLen
		}
		if stats.numDocs > 0 {
			stats.avgDocLen = totalDocLen / float64(stats.numDocs)
		}
		ragFilteredFTSStatsCacheMu.Lock()
		if cached, exists := ragFilteredFTSStatsCache[key]; exists {
			stats = cached
		} else {
			if len(ragFilteredFTSStatsCache) >= ragFilteredFTSStatsCacheMaxEntries {
				evictOverCap(ragFilteredFTSStatsCache, ragFilteredFTSStatsCacheMaxEntries)
			}
			ragFilteredFTSStatsCache[key] = stats
		}
		ragFilteredFTSStatsCacheMu.Unlock()
	}

	filteredCache := cache
	filteredCache.numDocs = stats.numDocs
	filteredCache.avgDocLen = stats.avgDocLen
	if stats.numDocs == 0 {
		return filteredCache, nil
	}

	// ftsBindIDF asks once per term in the prepared tree. Memoize the
	// two-pointer postings/filter intersection in this per-query closure so
	// repeated terms in compound queries cost no additional work.
	weights := make(map[string]float64)
	return filteredCache, func(term string) float64 {
		if weight, ok := weights[term]; ok {
			return weight
		}
		df := ragFilteredFTSDocFrequency(cache.postings[term], filter.rows)
		weight := 0.0
		if df > 0 {
			n := float64(stats.numDocs)
			weight = math.Log(1 + (n-float64(df)+0.5)/(float64(df)+0.5))
		}
		weights[term] = weight
		return weight
	}
}

func ragFilteredFTSDocFrequency(postings []int32, allowed []int) int {
	count := 0
	i, j := 0, 0
	for i < len(postings) && j < len(allowed) {
		posting := int(postings[i])
		switch {
		case posting < allowed[j]:
			i++
		case posting > allowed[j]:
			j++
		default:
			count++
			i++
			j++
		}
	}
	return count
}

// purgeRAGPreFilterCachesFor eagerly releases table pointers when a table is
// dropped or restored after a failed statement. The row-filter keys are based
// on table pointers rather than tenant names, so a same-named table in another
// tenant may be evicted too; that only causes a harmless lazy rebuild.
func purgeRAGPreFilterCachesFor(table string) {
	ragRowFilterCacheMu.Lock()
	for key := range ragRowFilterCache {
		if key.table != nil && strings.EqualFold(key.table.Name, table) {
			delete(ragRowFilterCache, key)
		}
	}
	ragRowFilterCacheMu.Unlock()

	ragFilteredFTSCandidateCacheMu.Lock()
	for key := range ragFilteredFTSCandidateCache {
		if key.table != nil && strings.EqualFold(key.table.Name, table) {
			delete(ragFilteredFTSCandidateCache, key)
		}
	}
	ragFilteredFTSCandidateCacheMu.Unlock()

	ragFilteredFTSStatsCacheMu.Lock()
	for key := range ragFilteredFTSStatsCache {
		if key.table != nil && strings.EqualFold(key.table.Name, table) {
			delete(ragFilteredFTSStatsCache, key)
		}
	}
	ragFilteredFTSStatsCacheMu.Unlock()
}

// ragFilterSource keeps neighbor expansion inside the same authorization set.
// Without this, a permitted hit could expand into an adjacent, unauthorized
// chunk after retrieval had correctly excluded that chunk.
func ragFilterSource(source ragSource, filter *ragRowFilter) ragSource {
	if filter == nil || !source.tableSource {
		return source
	}
	rawRows := make([][]any, 0, len(filter.rows))
	for _, rowID := range filter.rows {
		if rowID >= 0 && rowID < len(source.rawRows) {
			rawRows = append(rawRows, source.rawRows[rowID])
		}
	}
	// A filtered source has a filter-specific neighborhood topology, so do not
	// reuse the full-table context-index cache. table=nil intentionally selects
	// ragBuildContextIndex's uncached path while retaining direct raw-cell reads.
	source.rawRows = rawRows
	source.table = nil
	return source
}

// VecSearchFilteredTableFunc exposes exact, pre-filtered vector retrieval
// without overloading VEC_SEARCH's legacy positional metric/index arguments.
//
// VEC_SEARCH_FILTERED(table, column, query_vector, k, options_json)
// options_json accepts metric, index (documented but filter-safe searches use
// exact flat ranking), and a required pre_filter object.
type VecSearchFilteredTableFunc struct{}

func (f *VecSearchFilteredTableFunc) Name() string { return "VEC_SEARCH_FILTERED" }

func (f *VecSearchFilteredTableFunc) ValidateArgs(args []Expr) error {
	if len(args) != 5 {
		return fmt.Errorf("VEC_SEARCH_FILTERED requires 5 arguments: (table, column, query_vector, k, options_json)")
	}
	return nil
}

func (f *VecSearchFilteredTableFunc) Execute(ctx context.Context, args []Expr, env ExecEnv, row Row) (*ResultSet, error) {
	if err := f.ValidateArgs(args); err != nil {
		return nil, err
	}
	a, err := vecParseArgs(env, args[:4], row)
	if err != nil {
		return nil, err
	}
	opts, err := ragOptionsArg(env, args[4], row, f.Name())
	if err != nil {
		return nil, err
	}
	if opts.PreFilter == nil {
		return nil, fmt.Errorf("%s: options_json requires pre_filter", f.Name())
	}
	if opts.Metric != "" {
		a.metric = normalizeVecMetric(opts.Metric)
		if a.metric == "" {
			return nil, fmt.Errorf("%s: unknown metric %q", f.Name(), opts.Metric)
		}
	}
	if opts.Index != "" {
		a.indexMode = normalizeVecIndexMode(opts.Index)
		if a.indexMode == "" {
			return nil, fmt.Errorf("%s: unknown index %q", f.Name(), opts.Index)
		}
	}
	table, _, err := ragGetSourceTable(env, a.tableName)
	if err != nil {
		return nil, fmt.Errorf("%s: table %q not found: %w", f.Name(), a.tableName, err)
	}
	filter, err := ragBuildRowFilter(table, opts.PreFilter)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", f.Name(), err)
	}
	rows, err := ragVecSearchCandidatesFiltered(ctx, env, a, table, filter)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", f.Name(), err)
	}
	return materializeVecCandidates(table, rows, a.metric), nil
}

// FTSSearchFilteredTableFunc exposes FTS pre-filtering without making the
// existing FTS_SEARCH(column...) suffix ambiguous.
//
// FTS_SEARCH_FILTERED(table, query, k, options_json [, column1, column2...])
type FTSSearchFilteredTableFunc struct{}

func (f *FTSSearchFilteredTableFunc) Name() string { return "FTS_SEARCH_FILTERED" }

func (f *FTSSearchFilteredTableFunc) ValidateArgs(args []Expr) error {
	if len(args) < 4 {
		return fmt.Errorf("FTS_SEARCH_FILTERED requires at least 4 arguments: (table, query, k, options_json [, columns...])")
	}
	return nil
}

func (f *FTSSearchFilteredTableFunc) Execute(ctx context.Context, args []Expr, env ExecEnv, row Row) (*ResultSet, error) {
	if err := f.ValidateArgs(args); err != nil {
		return nil, err
	}
	tableName, err := ragRequiredStringArg(env, args[0], row, f.Name(), "table")
	if err != nil {
		return nil, err
	}
	query, err := ragRequiredStringArg(env, args[1], row, f.Name(), "query")
	if err != nil {
		return nil, err
	}
	kValue, err := evalExpr(env, args[2], row)
	if err != nil {
		return nil, fmt.Errorf("%s k: %w", f.Name(), err)
	}
	k, err := toInt(kValue)
	if err != nil || k <= 0 {
		if err == nil {
			err = fmt.Errorf("must be > 0")
		}
		return nil, fmt.Errorf("%s k: %w", f.Name(), err)
	}
	opts, err := ragOptionsArg(env, args[3], row, f.Name())
	if err != nil {
		return nil, err
	}
	if opts.PreFilter == nil {
		return nil, fmt.Errorf("%s: options_json requires pre_filter", f.Name())
	}
	table, tenant, err := ragGetSourceTable(env, tableName)
	if err != nil {
		return nil, fmt.Errorf("%s: table %q not found: %w", f.Name(), tableName, err)
	}
	searchCols := ragFTSColumns(table, env, args[4:], row)
	filter, err := ragBuildRowFilter(table, opts.PreFilter)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", f.Name(), err)
	}
	results, err := ragFTSSearchCandidatesFiltered(ctx, tenant, table, query, k, searchCols, filter)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", f.Name(), err)
	}
	return materializeFTSCandidates(table, results), nil
}

func ragOptionsArg(env ExecEnv, expr Expr, row Row, function string) (ragSearchOptions, error) {
	value, err := evalExpr(env, expr, row)
	if err != nil {
		return ragSearchOptions{}, fmt.Errorf("%s options: %w", function, err)
	}
	raw, ok := value.(string)
	if !ok {
		return ragSearchOptions{}, fmt.Errorf("%s: options_json must be a JSON string, got %T", function, value)
	}
	opts, err := ragParseSearchOptions(raw)
	if err != nil {
		return ragSearchOptions{}, fmt.Errorf("%s: invalid options JSON: %w", function, err)
	}
	return opts, nil
}

func ragRequiredStringArg(env ExecEnv, expr Expr, row Row, function, argument string) (string, error) {
	value, err := evalExpr(env, expr, row)
	if err != nil {
		return "", fmt.Errorf("%s %s: %w", function, argument, err)
	}
	text, ok := value.(string)
	if !ok || strings.TrimSpace(text) == "" {
		return "", fmt.Errorf("%s: %s must be a non-empty string", function, argument)
	}
	return text, nil
}

func ragFTSColumns(table *storage.Table, env ExecEnv, args []Expr, row Row) []int {
	searchCols := make([]int, 0, len(args))
	for _, arg := range args {
		value, err := evalExpr(env, arg, row)
		if err != nil {
			continue
		}
		column, ok := value.(string)
		if !ok {
			continue
		}
		if pos, err := table.ColIndex(column); err == nil {
			searchCols = append(searchCols, pos)
		}
	}
	if len(searchCols) == 0 {
		for i := range table.Cols {
			searchCols = append(searchCols, i)
		}
	}
	return searchCols
}

func init() {
	RegisterTableFunc(&VecSearchFilteredTableFunc{})
	RegisterTableFunc(&FTSSearchFilteredTableFunc{})
}
