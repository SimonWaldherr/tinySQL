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
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
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
				if err := json.Unmarshal([]byte(optStr), &opts); err != nil {
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
	candidateK := opts.CandidateK
	if candidateK <= 0 {
		candidateK = k * 4
	}
	if candidateK < k {
		candidateK = k
	}

	textColumns := ragSearchTextColumns(&opts)
	hybrid := len(textColumns) > 0 && opts.TextQuery != ""

	// ---- Vector pass arguments (built up front: no dependency on either
	// pass's result) ----------------------------------------------------
	vecArgs := []Expr{
		&Literal{Val: vecArgsParsed.tableName},
		&Literal{Val: vecArgsParsed.colName},
		&Literal{Val: vecArgsParsed.queryVec},
		&Literal{Val: candidateK},
		&Literal{Val: metric},
		&Literal{Val: index},
	}

	var result *ResultSet
	if !hybrid {
		vecResult, err := (&VecSearchTableFunc{}).Execute(ctx, vecArgs, env, row)
		if err != nil {
			return nil, fmt.Errorf("RAG_SEARCH: vector pass: %w", err)
		}
		result = ragSearchTruncate(vecResult, k)
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

		ftsArgs := []Expr{
			&Literal{Val: vecArgsParsed.tableName},
			&Literal{Val: ftsQuery},
			&Literal{Val: candidateK},
		}
		for _, col := range textColumns {
			ftsArgs = append(ftsArgs, &Literal{Val: col})
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
			wg                   sync.WaitGroup
			vecResult, ftsResult *ResultSet
			vecErr, ftsErr       error
		)
		wg.Add(2)
		go func() {
			defer wg.Done()
			vecResult, vecErr = (&VecSearchTableFunc{}).Execute(ctx, vecArgs, env, row)
		}()
		go func() {
			defer wg.Done()
			ftsResult, ftsErr = (&FTSSearchTableFunc{}).Execute(ctx, ftsArgs, env, row)
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

		rrfK := opts.RRFK
		if rrfK <= 0 {
			rrfK = ragSearchDefaultRRFK
		}
		fused := ragSearchFuse(vecResult, ftsResult, opts.KeyColumns, rrfK)
		result = ragSearchTruncate(fused, k)
	}

	// ---- Optional neighbor-context expansion --------------------------
	if opts.ExpandBefore > 0 || opts.ExpandAfter > 0 {
		if opts.DocIDColumn == "" || opts.ChunkIndexColumn == "" {
			return nil, fmt.Errorf("RAG_SEARCH: doc_id_column and chunk_index_column are required when expand_before/expand_after is set")
		}
		source, err := ragLoadSource(env, vecArgsParsed.tableName)
		if err != nil {
			return nil, fmt.Errorf("RAG_SEARCH: %w", err)
		}
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

// ragSearchTruncate returns rs re-sliced to at most k rows, preserving
// existing order (both VEC_SEARCH's output and the RRF-fused output are
// already sorted best-first before this is called). Never mutates rs.Rows'
// backing array in a way visible to the caller — a fresh ResultSet is
// returned so the original candidateK-sized result is left untouched.
func ragSearchTruncate(rs *ResultSet, k int) *ResultSet {
	if rs == nil {
		return &ResultSet{Cols: nil, Rows: nil}
	}
	if k >= len(rs.Rows) {
		return rs
	}
	return &ResultSet{Cols: rs.Cols, Rows: rs.Rows[:k]}
}

// ragSearchBaseCols strips a table-valued function's trailing score/rank
// columns off the end of cols, given how many trailing columns it appended
// (VEC_SEARCH appends 3: _vec_distance/_vec_similarity/_vec_rank; FTS_SEARCH
// appends 2: _fts_score/_fts_rank — see their Execute implementations).
func ragSearchBaseCols(cols []string, trailing int) []string {
	if len(cols) < trailing {
		return cols
	}
	return cols[:len(cols)-trailing]
}

// ragSearchKey builds a composite identity key for a row by concatenating
// its keyCols values (via fmt.Sprintf("%v", ...), joined by a separator that
// cannot appear in a normal column value's default formatting). Rows from
// VEC_SEARCH and FTS_SEARCH on the same source table copy their key column
// values straight out of storage.Table.Rows with no type coercion, so the
// same underlying row produces an identical key from either pass.
func ragSearchKey(r Row, keyCols []string) string {
	// A single scalar primary key is overwhelmingly common. Returning its
	// formatted value directly skips the Builder backing allocation and copy
	// for every vector and FTS candidate entering fusion.
	if len(keyCols) == 1 {
		v, _ := ragValue(r, keyCols[0])
		switch t := v.(type) {
		case string:
			return t
		case int:
			return strconv.Itoa(t)
		case int64:
			return strconv.FormatInt(t, 10)
		case bool:
			if t {
				return "true"
			}
			return "false"
		default:
			return fmt.Sprint(v)
		}
	}
	var b strings.Builder
	for i, col := range keyCols {
		if i > 0 {
			b.WriteByte('\x1f')
		}
		v, _ := ragValue(r, col)
		ragWriteKeyValue(&b, v)
	}
	return b.String()
}

// ragWriteKeyValue mirrors ftsWriteValue's fast-path/fallback split (see
// fts.go): a type switch over the scalar kinds that make up real key-column
// values (row IDs, chunk indexes, doc IDs) avoids fmt.Fprintf's reflection
// overhead per key column per candidate row during fusion. float64
// deliberately stays on the %v fallback, same as ftsWriteValue, since its
// shortest-round-trip formatting needs care to reproduce exactly via
// strconv.
func ragWriteKeyValue(b *strings.Builder, v any) {
	switch t := v.(type) {
	case string:
		b.WriteString(t)
	case int:
		b.WriteString(strconv.Itoa(t))
	case int64:
		b.WriteString(strconv.FormatInt(t, 10))
	case bool:
		if t {
			b.WriteString("true")
		} else {
			b.WriteString("false")
		}
	default:
		fmt.Fprintf(b, "%v", v)
	}
}

// ragFusedRow tracks one candidate row's merged data plus its RRF inputs
// while fusing the vector and text candidate sets.
type ragFusedRow struct {
	row      Row
	vecRank  int // 0 = not present in the vector candidate set
	ftsRank  int // 0 = not present in the FTS candidate set
	order    int // first-seen order, used as a deterministic sort tie-break
	rrfScore float64
}

// ragOrderByScoreAsc sorts a []string of fused-row keys by descending RRF
// score, falling back to each row's unique insertion order. A concrete Swap
// (a single string-header assignment) replaces reflect.Swapper's generic,
// pointer-aware swap.
type ragOrderByScoreAsc struct {
	fused map[string]*ragFusedRow
	order []string
}

func (s ragOrderByScoreAsc) Len() int { return len(s.order) }
func (s ragOrderByScoreAsc) Less(i, j int) bool {
	a, b := s.fused[s.order[i]], s.fused[s.order[j]]
	if a.rrfScore != b.rrfScore {
		return a.rrfScore > b.rrfScore
	}
	return a.order < b.order
}
func (s ragOrderByScoreAsc) Swap(i, j int) { s.order[i], s.order[j] = s.order[j], s.order[i] }

// ragSearchFuse reciprocal-rank-fuses vecResult and ftsResult into one
// ResultSet, matching rows across the two sets by concatenating their
// keyCols values into a composite key.
//
// Absent-column convention: a row present in only one of the two input sets
// carries only that pass's rank/score columns in its output Row — e.g. an
// FTS-only hit has no "_vec_rank"/"_vec_distance"/"_vec_similarity" keys at
// all (not a zero or NULL sentinel value for them). Callers should check key
// presence (as ragValue/ragHitRank already do), not compare against a
// placeholder value, to test whether a row was retrieved by a given pass.
// _rrf_score/_rrf_rank are always present on every output row.
func ragSearchFuse(vecResult, ftsResult *ResultSet, keyCols []string, rrfK float64) *ResultSet {
	candidateCap := len(vecResult.Rows) + len(ftsResult.Rows)
	fused := make(map[string]*ragFusedRow, candidateCap)
	order := make([]string, 0, candidateCap)

	// r[c.Name] (VEC_SEARCH/FTS_SEARCH's own row-building convention) is not
	// guaranteed lower-cased, so every lookup below goes through ragValue
	// (case-insensitive by column name) rather than direct map indexing;
	// every value copied into fr.row is written back under a canonical
	// lower-cased key, matching ragCopyOutputRow's convention elsewhere in
	// the RAG code so downstream ragValue/ragExpandContextFrom calls hit the
	// fast lower-cased-key path instead of always falling back to a scan.
	vecBase := ragSearchBaseCols(vecResult.Cols, 3)
	for _, r := range vecResult.Rows {
		vecRankVal, _ := ragValue(r, "_vec_rank")
		vecRank, _ := toInt(vecRankVal)
		key := ragSearchKey(r, keyCols)
		fr, ok := fused[key]
		if !ok {
			fr = &ragFusedRow{row: make(Row, len(r)+7), order: len(order)}
			for _, c := range vecBase {
				if v, ok := ragValue(r, c); ok {
					fr.row[strings.ToLower(c)] = v
				}
			}
			fused[key] = fr
			order = append(order, key)
		}
		fr.vecRank = vecRank
		if v, ok := ragValue(r, "_vec_rank"); ok {
			fr.row["_vec_rank"] = v
		}
		if v, ok := ragValue(r, "_vec_distance"); ok {
			fr.row["_vec_distance"] = v
		}
		if v, ok := ragValue(r, "_vec_similarity"); ok {
			fr.row["_vec_similarity"] = v
		}
	}

	ftsBase := ragSearchBaseCols(ftsResult.Cols, 2)
	for _, r := range ftsResult.Rows {
		ftsRankVal, _ := ragValue(r, "_fts_rank")
		ftsRank, _ := toInt(ftsRankVal)
		key := ragSearchKey(r, keyCols)
		fr, ok := fused[key]
		if !ok {
			fr = &ragFusedRow{row: make(Row, len(r)+7), order: len(order)}
			for _, c := range ftsBase {
				if v, ok := ragValue(r, c); ok {
					fr.row[strings.ToLower(c)] = v
				}
			}
			fused[key] = fr
			order = append(order, key)
		}
		fr.ftsRank = ftsRank
		if v, ok := ragValue(r, "_fts_score"); ok {
			fr.row["_fts_score"] = v
		}
		if v, ok := ragValue(r, "_fts_rank"); ok {
			fr.row["_fts_rank"] = v
		}
	}

	for _, key := range order {
		fr := fused[key]
		var score float64
		if fr.vecRank > 0 {
			score += 1.0 / (rrfK + float64(fr.vecRank))
		}
		if fr.ftsRank > 0 {
			score += 1.0 / (rrfK + float64(fr.ftsRank))
		}
		fr.rrfScore = score
	}

	// A concrete sort.Interface with a direct Swap avoids reflect.Swapper's
	// generic path, which sort.Slice/SliceStable fall back to for any element
	// type containing a pointer — a plain string included, per the same
	// rationale documented on orderedRawRowsAsc (exec_fastpath_select.go).
	// fr.order is a unique insertion sequence number (see ragFusedRow), so the
	// comparator is already a strict total order and sort.Sort (pdqsort)
	// reproduces the exact order sort.SliceStable did, without symMerge.
	sort.Sort(ragOrderByScoreAsc{fused: fused, order: order})

	baseColSet := make(map[string]bool, len(vecBase)+len(ftsBase))
	baseCols := make([]string, 0, len(vecBase)+len(ftsBase))
	for _, c := range vecBase {
		lc := strings.ToLower(c)
		if !baseColSet[lc] {
			baseColSet[lc] = true
			baseCols = append(baseCols, c)
		}
	}
	for _, c := range ftsBase {
		lc := strings.ToLower(c)
		if !baseColSet[lc] {
			baseColSet[lc] = true
			baseCols = append(baseCols, c)
		}
	}
	cols := append(append([]string{}, baseCols...), "_vec_rank", "_vec_distance", "_vec_similarity", "_fts_rank", "_fts_score", "_rrf_score", "_rrf_rank")

	rows := make([]Row, 0, len(order))
	for rank, key := range order {
		fr := fused[key]
		fr.row["_rrf_score"] = fr.rrfScore
		fr.row["_rrf_rank"] = rank + 1
		rows = append(rows, fr.row)
	}

	return &ResultSet{Cols: cols, Rows: rows}
}

func init() {
	RegisterTableFunc(&RAGSearchTableFunc{})
}
