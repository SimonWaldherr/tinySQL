// RAG_SEARCH composes VEC_SEARCH, FTS_SEARCH, and RAG_CONTEXT_FROM's
// neighbor-expansion logic into a single table-valued function, so a typical
// RAG retrieval workflow — vector search, optional hybrid text fusion,
// optional neighboring-chunk expansion — no longer requires callers to
// hand-write the multi-primitive pipeline documented in docs/rag-guide.md.
//
// Usage:
//
//	SELECT * FROM RAG_SEARCH('chunks', 'embedding', query_vector, 5)
//
//	SELECT * FROM RAG_SEARCH('chunks', 'embedding', query_vector, 5, '{
//	  "text_columns": ["heading", "chunk_text"],
//	  "text_query": "what is the capital of France",
//	  "key_columns": ["doc_id", "chunk_index"],
//	  "expand_before": 1,
//	  "expand_after": 1,
//	  "doc_id_column": "doc_id",
//	  "chunk_index_column": "chunk_index"
//	}')
package engine

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// ragSearchOptions is the optional 5th-argument JSON payload for RAG_SEARCH.
type ragSearchOptions struct {
	Metric           string   `json:"metric"`
	Index            string   `json:"index"`
	TextColumn       string   `json:"text_column"`
	TextColumns      []string `json:"text_columns"`
	TextQuery        string   `json:"text_query"`
	AutoOrExpand     *bool    `json:"auto_or_expand"`
	CandidateK       int      `json:"candidate_k"`
	RRFK             float64  `json:"rrf_k"`
	KeyColumns       []string `json:"key_columns"`
	ExpandBefore     int      `json:"expand_before"`
	ExpandAfter      int      `json:"expand_after"`
	DocIDColumn      string   `json:"doc_id_column"`
	ChunkIndexColumn string   `json:"chunk_index_column"`
	// PreFilter is deliberately distinct from an outer SQL WHERE: it limits
	// the source row set before vector/FTS candidate selection and RRF fusion.
	// See rag_prefilter.go for its stable-ID/equality contract.
	PreFilter *ragPreFilterOptions `json:"pre_filter"`
}

const ragSearchDefaultRRFK = 60.0

// RAGSearchTableFunc implements the RAG_SEARCH table-valued function.
type RAGSearchTableFunc struct{}

func (f *RAGSearchTableFunc) Name() string { return "RAG_SEARCH" }

func (f *RAGSearchTableFunc) ValidateArgs(args []Expr) error {
	if len(args) < 4 || len(args) > 5 {
		return fmt.Errorf("RAG_SEARCH requires 4-5 arguments: (table, vector_column, query_vector, k [, options_json])")
	}
	return nil
}

func (f *RAGSearchTableFunc) Execute(ctx context.Context, args []Expr, env ExecEnv, row Row) (*ResultSet, error) {
	if err := f.ValidateArgs(args); err != nil {
		return nil, err
	}

	// Reuse VEC_SEARCH's own arg parser for the shared (table, column,
	// query_vector, k) prefix so RAG_SEARCH accepts exactly the same value
	// types/coercions VEC_SEARCH does (string or []float64 query vector,
	// int/int64/float64 k, ...). Called with only the first 4 args, so its
	// metric/index defaults ("cosine"/"flat") apply; ragSearchOptions.Metric/
	// Index below can still override them.
	vecArgsParsed, err := vecParseArgs(env, args[:4], row)
	if err != nil {
		return nil, fmt.Errorf("RAG_SEARCH: %w", err)
	}

	var opts ragSearchOptions
	if len(args) == 5 {
		optVal, err := evalExpr(env, args[4], row)
		if err != nil {
			return nil, fmt.Errorf("RAG_SEARCH options: %w", err)
		}
		if optVal != nil {
			optStr, ok := optVal.(string)
			if !ok {
				return nil, fmt.Errorf("RAG_SEARCH: options must be a JSON string, got %T", optVal)
			}
			if strings.TrimSpace(optStr) != "" {
				opts, err = ragParseSearchOptions(optStr)
				if err != nil {
					return nil, fmt.Errorf("RAG_SEARCH: invalid options JSON: %w", err)
				}
			}
		}
	}

	return ragSearchExecute(ctx, env, row, vecArgsParsed, opts)
}

// ragSearchExecute is RAG_SEARCH's body once the shared (table, column,
// query_vector, k) prefix and the options struct are already resolved.
// RAGSearchTableFunc.Execute reaches it after parsing an options JSON
// string argument; HybridSearchTableFunc.Execute calls it directly with an
// already-populated ragSearchOptions, skipping a marshal-to-JSON-just-to-
// immediately-unmarshal-it-back round trip through this same function.
func ragSearchExecute(ctx context.Context, env ExecEnv, row Row, vecArgsParsed vecSearchArgs, opts ragSearchOptions) (*ResultSet, error) {
	metric := vecArgsParsed.metric
	if opts.Metric != "" {
		metric = normalizeVecMetric(opts.Metric)
		if metric == "" {
			return nil, fmt.Errorf("RAG_SEARCH: unknown metric %q", opts.Metric)
		}
	}
	index := vecArgsParsed.indexMode
	if opts.Index != "" {
		index = normalizeVecIndexMode(opts.Index)
		if index == "" {
			return nil, fmt.Errorf("RAG_SEARCH: unknown index %q", opts.Index)
		}
	}

	k := vecArgsParsed.k
	textColumns := ragSearchTextColumns(&opts)
	hybrid := len(textColumns) > 0 && opts.TextQuery != ""
	candidateK := k
	if hybrid {
		candidateK = opts.CandidateK
		if candidateK <= 0 {
			candidateK = k * 4
		}
		if candidateK < k {
			candidateK = k
		}
	}

	searchArgs := vecArgsParsed
	searchArgs.k = candidateK
	searchArgs.metric = metric
	searchArgs.indexMode = index

	tenant := env.tenant
	if tenant == "" {
		tenant = "default"
	}
	var (
		preFilterTable *storage.Table
		rowFilter      *ragRowFilter
	)
	if opts.PreFilter != nil {
		var err error
		preFilterTable, _, err = ragGetSourceTable(env, vecArgsParsed.tableName)
		if err != nil {
			return nil, fmt.Errorf("RAG_SEARCH: table %q not found: %w", vecArgsParsed.tableName, err)
		}
		rowFilter, err = ragBuildRowFilterContext(ctx, tenant, preFilterTable, opts.PreFilter)
		if err != nil {
			return nil, fmt.Errorf("RAG_SEARCH: %w", err)
		}
	}

	var (
		result      *ResultSet
		sourceTable *storage.Table
	)
	if !hybrid {
		var (
			table      *storage.Table
			candidates []vecScoredRow
			err        error
		)
		if rowFilter == nil {
			table, candidates, err = vecSearchCandidates(ctx, env, searchArgs)
		} else {
			table = preFilterTable
			candidates, err = ragVecSearchCandidatesFiltered(ctx, env, searchArgs, table, rowFilter)
		}
		if err != nil {
			return nil, fmt.Errorf("RAG_SEARCH: vector pass: %w", err)
		}
		if len(candidates) > k {
			candidates = candidates[:k]
		}
		result = materializeVecCandidates(table, candidates, metric)
		sourceTable = table
	} else {
		if len(opts.KeyColumns) == 0 {
			return nil, fmt.Errorf("RAG_SEARCH: key_columns is required for hybrid text+vector search")
		}

		autoOr := true
		if opts.AutoOrExpand != nil {
			autoOr = *opts.AutoOrExpand
		}
		ftsQuery := opts.TextQuery
		if autoOr {
			ftsQuery = ftsAutoOrExpand(ftsQuery)
		}

		table := preFilterTable
		if table == nil {
			var err error
			table, _, err = ragGetSourceTable(env, vecArgsParsed.tableName)
			if err != nil {
				return nil, fmt.Errorf("RAG_SEARCH: table %q not found: %w", vecArgsParsed.tableName, err)
			}
		}
		searchCols := make([]int, 0, len(textColumns))
		for _, col := range textColumns {
			idx, err := table.ColIndex(col)
			if err != nil {
				return nil, fmt.Errorf("RAG_SEARCH: text column %q: %w", col, err)
			}
			searchCols = append(searchCols, idx)
		}

		// The vector pass and the FTS pass are independent reads with no
		// data dependency between them: ftsArgs above does not use
		// vecResult, and each pass hits its own cache (the vector column/
		// IVF/HNSW/query-result caches vs. the FTS tokenized-document/
		// query-parse caches). Running them concurrently instead of
		// sequentially bounds this stage's latency by
		// max(vecTime, ftsTime) instead of vecTime+ftsTime.
		//
		// Safety: each goroutine below writes only to its own pre-declared
		// result/error variable, never a variable the other writes too.
		// env and row are shared but read-only from both call sites here
		// (verified by reading VecSearchTableFunc.Execute and
		// FTSSearchTableFunc.Execute in full): neither writes into the row
		// map, nor through any pointer/map field reachable from env
		// (env.db, env.ctes, env.subqueryCache, env.statementWAL, etc. are
		// all untouched by a pure VEC_SEARCH/FTS_SEARCH read). Every cache
		// either side can lazily build — vecSearchColumnCache/vecIVFCache/
		// vecHNSWCache/the opt-in vector query-result cache (vector_search.go,
		// vector_index.go, vector_query_cache.go) and the FTS document/
		// query-parse caches (fts.go, fts_query_cache.go) — already
		// synchronizes itself with a mutex, RWMutex, or atomic, because
		// concurrent callers (e.g. two simultaneous VEC_SEARCH queries from
		// different connections) were already possible before this change.
		// Both passes only read *storage.Table (Rows/Cols/Version); the only
		// writers of those fields are DML statements, which take DB's
		// exclusive content lock (see Execute's doc comment in exec.go) and
		// therefore cannot run concurrently with the read-locked SELECT that
		// reaches this function.
		var (
			wg             sync.WaitGroup
			vecTable       *storage.Table
			vecCandidates  []vecScoredRow
			ftsCandidates  []ftsScored
			vecErr, ftsErr error
		)
		wg.Add(2)
		go func() {
			defer wg.Done()
			if rowFilter == nil {
				vecTable, vecCandidates, vecErr = vecSearchCandidates(ctx, env, searchArgs)
				return
			}
			vecTable = table
			vecCandidates, vecErr = ragVecSearchCandidatesFiltered(ctx, env, searchArgs, table, rowFilter)
		}()
		go func() {
			defer wg.Done()
			ftsCandidates, ftsErr = ragFTSSearchCandidatesFiltered(ctx, tenant, table, ftsQuery, candidateK, searchCols, rowFilter)
		}()
		wg.Wait()

		if vecErr != nil {
			if ftsErr != nil {
				return nil, fmt.Errorf("RAG_SEARCH: vector pass: %w (text pass also failed: %v)", vecErr, ftsErr)
			}
			return nil, fmt.Errorf("RAG_SEARCH: vector pass: %w", vecErr)
		}
		if ftsErr != nil {
			return nil, fmt.Errorf("RAG_SEARCH: text pass: %w", ftsErr)
		}
		if vecTable != table {
			return nil, fmt.Errorf("RAG_SEARCH: source table changed during retrieval")
		}

		rrfK := opts.RRFK
		if rrfK <= 0 {
			rrfK = ragSearchDefaultRRFK
		}
		result = ragFuseCandidates(table, vecCandidates, ftsCandidates, metric, rrfK, k)
		sourceTable = table
	}

	// ---- Optional neighbor-context expansion --------------------------
	if opts.ExpandBefore > 0 || opts.ExpandAfter > 0 {
		if opts.DocIDColumn == "" || opts.ChunkIndexColumn == "" {
			return nil, fmt.Errorf("RAG_SEARCH: doc_id_column and chunk_index_column are required when expand_before/expand_after is set")
		}
		if sourceTable == nil {
			return nil, fmt.Errorf("RAG_SEARCH: source table was not resolved")
		}
		// This must be the exact physical table that supplied vector/FTS
		// candidates. In particular, do not route through ragLoadSource here:
		// a same-named CTE may shadow that name, but its row IDs do not belong to
		// the resolved pre-filter and could otherwise bypass the ACL boundary.
		source := ragSourceFromTable(tenant, sourceTable)
		// Context expansion is part of retrieval output, not an exempt display
		// phase. Reapply the pre-filter to the source so an authorized hit can
		// never pull an adjacent unauthorized chunk into the final context.
		source = ragFilterSource(source, rowFilter)
		// hits is built directly from the fused/vector-only ResultSet already
		// computed above rather than via ragLoadSource: RAG_SEARCH's hits are
		// an in-memory result the caller never registered as a table or CTE.
		// A directly-constructed, non-table ragSource only needs cols/rows
		// populated (see ragSource.value/outputRow/len) — exactly what
		// ragLoadSource itself builds for a CTE hit set.
		hits := ragSource{cols: result.Cols, rows: result.Rows}
		result = ragExpandContextFrom(source, hits, opts.DocIDColumn, opts.ChunkIndexColumn, opts.DocIDColumn, opts.ChunkIndexColumn, opts.ExpandBefore, opts.ExpandAfter)
	}

	return result, nil
}

// ragSearchTextColumns resolves the BM25 columns from the singular
// text_column and the plural text_columns options, in that order, dropping
// case-insensitive duplicates. Duplicates matter beyond tidiness: FTS_SEARCH
// scores each column it is handed, so naming one twice would double-count its
// term frequencies against the rest.
func ragSearchTextColumns(opts *ragSearchOptions) []string {
	out := make([]string, 0, len(opts.TextColumns)+1)
	appendColumn := func(col string) {
		col = strings.TrimSpace(col)
		if col == "" {
			return
		}
		for _, existing := range out {
			if strings.EqualFold(existing, col) {
				return
			}
		}
		out = append(out, col)
	}
	appendColumn(opts.TextColumn)
	for _, col := range opts.TextColumns {
		appendColumn(col)
	}
	return out
}

// ragNativeCandidate is the compact common currency of hybrid retrieval.
// Both search branches address the same immutable table snapshot, so the
// physical row index is a lossless identity and avoids materializing rows or
// formatting composite keys before reciprocal-rank fusion.
type ragNativeCandidate struct {
	rowIdx   int
	vecRank  int
	ftsRank  int
	distance float64
	ftsScore float64
	order    int
	rrfScore float64
}

// Keep the sort order as pointers, but allocate all candidates in one compact
// backing array per retrieval. This retains cheap pointer swaps in sort.Sort
// while avoiding one heap allocation per vector/FTS candidate.
type ragNativeCandidates []*ragNativeCandidate

func (s ragNativeCandidates) Len() int { return len(s) }
func (s ragNativeCandidates) Less(i, j int) bool {
	if s[i].rrfScore != s[j].rrfScore {
		return s[i].rrfScore > s[j].rrfScore
	}
	return s[i].order < s[j].order
}
func (s ragNativeCandidates) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

// ragFuseCandidates reciprocal-rank-fuses the vector and text candidate sets
// into one ResultSet. Both branches address the same immutable table snapshot,
// so candidates are matched on physical row index rather than a formatted
// composite key.
//
// Absent-column convention: a row present in only one of the two candidate
// sets carries only that pass's rank/score columns in its output Row — an
// FTS-only hit has no "_vec_rank"/"_vec_distance"/"_vec_similarity" keys at
// all, rather than a zero or NULL sentinel for them. Callers should test key
// presence (as ragValue/ragHitRank already do) rather than compare against a
// placeholder to tell whether a row was retrieved by a given pass.
// _rrf_score/_rrf_rank are always present on every output row.
func ragFuseCandidates(table *storage.Table, vecRows []vecScoredRow, ftsRows []ftsScored, metric string, rrfK float64, k int) *ResultSet {
	// The two retrieval branches operate on the same immutable table snapshot,
	// so a physical row index is a complete identity. Candidate count can never
	// exceed the total input length, including any duplicate or invalid input
	// row IDs, so this capacity also guarantees that pointers into candidates
	// remain stable through all appends below.
	candidateCap := len(vecRows) + len(ftsRows)
	byRow := make(map[int]*ragNativeCandidate, candidateCap)
	candidates := make([]ragNativeCandidate, 0, candidateCap)
	ordered := make(ragNativeCandidates, 0, candidateCap)
	for rank, candidate := range vecRows {
		if candidate.rowIdx < 0 || candidate.rowIdx >= len(table.Rows) {
			continue
		}
		candidates = append(candidates, ragNativeCandidate{
			rowIdx: candidate.rowIdx, vecRank: rank + 1,
			distance: candidate.distance, order: len(ordered),
		})
		entry := &candidates[len(candidates)-1]
		byRow[candidate.rowIdx] = entry
		ordered = append(ordered, entry)
	}
	for rank, candidate := range ftsRows {
		if candidate.rowIdx < 0 || candidate.rowIdx >= len(table.Rows) {
			continue
		}
		entry := byRow[candidate.rowIdx]
		if entry == nil {
			candidates = append(candidates, ragNativeCandidate{rowIdx: candidate.rowIdx, order: len(ordered)})
			entry = &candidates[len(candidates)-1]
			byRow[candidate.rowIdx] = entry
			ordered = append(ordered, entry)
		}
		entry.ftsRank = rank + 1
		entry.ftsScore = candidate.score
	}
	for _, candidate := range ordered {
		if candidate.vecRank > 0 {
			candidate.rrfScore += 1.0 / (rrfK + float64(candidate.vecRank))
		}
		if candidate.ftsRank > 0 {
			candidate.rrfScore += 1.0 / (rrfK + float64(candidate.ftsRank))
		}
	}
	sort.Sort(ordered)
	if k < len(ordered) {
		ordered = ordered[:k]
	}

	cols := make([]string, 0, len(table.Cols)+7)
	for _, column := range table.Cols {
		cols = append(cols, column.Name)
	}
	cols = append(cols, "_vec_rank", "_vec_distance", "_vec_similarity", "_fts_rank", "_fts_score", "_rrf_score", "_rrf_rank")
	rows := make([]Row, 0, len(ordered))
	for rank, candidate := range ordered {
		source := table.Rows[candidate.rowIdx]
		row := make(Row, len(cols))
		for columnIndex, column := range table.Cols {
			if columnIndex < len(source) {
				row[strings.ToLower(column.Name)] = source[columnIndex]
			}
		}
		if candidate.vecRank > 0 {
			row["_vec_rank"] = candidate.vecRank
			row["_vec_distance"] = candidate.distance
			row["_vec_similarity"] = vecSimilarityFromDistance(metric, candidate.distance)
		}
		if candidate.ftsRank > 0 {
			row["_fts_rank"] = candidate.ftsRank
			row["_fts_score"] = candidate.ftsScore
		}
		row["_rrf_score"] = candidate.rrfScore
		row["_rrf_rank"] = rank + 1
		rows = append(rows, row)
	}
	return &ResultSet{Cols: cols, Rows: rows}
}

func init() {
	RegisterTableFunc(&RAGSearchTableFunc{})
}
