package engine

import (
	"context"
	"fmt"
	"sort"
	"strings"
)

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

	matches := ragFindContextRows(source, docCol, chunkCol, docID, centerChunk, before, after)
	cols := append(append([]string{}, source.cols...), "_context_offset", "_context_rank")
	out := make([]Row, 0, len(matches))
	for i, m := range matches {
		r := ragCopyOutputRow(source.cols, m.row)
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

	cols := append(append([]string{}, source.cols...), "_hit_rank", "_context_offset", "_context_rank")
	out := make([]Row, 0)
	seen := make(map[string]struct{})
	for hitIdx, hit := range hits.rows {
		docID, ok := ragValue(hit, hitDocCol)
		if !ok {
			return nil, fmt.Errorf("RAG_CONTEXT_FROM: hit column %q not found", hitDocCol)
		}
		chunkVal, ok := ragValue(hit, hitChunkCol)
		if !ok {
			return nil, fmt.Errorf("RAG_CONTEXT_FROM: hit column %q not found", hitChunkCol)
		}
		centerChunk, err := toInt(chunkVal)
		if err != nil {
			return nil, fmt.Errorf("RAG_CONTEXT_FROM %s: %w", hitChunkCol, err)
		}

		matches := ragFindContextRows(source, docCol, chunkCol, docID, centerChunk, before, after)
		hitRank := ragHitRank(hit, hitIdx+1)
		for _, m := range matches {
			key := fmtKeyPart(docID) + "|" + fmtKeyPart(m.chunkIndex)
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}

			r := ragCopyOutputRow(source.cols, m.row)
			r["_hit_rank"] = hitRank
			r["_context_offset"] = m.chunkIndex - centerChunk
			r["_context_rank"] = len(out) + 1
			out = append(out, r)
		}
	}
	return &ResultSet{Cols: cols, Rows: out}, nil
}

type ragSource struct {
	cols []string
	rows []Row
}

type ragContextRow struct {
	row        Row
	chunkIndex int
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
	rows := make([]Row, 0, len(table.Rows))
	for _, raw := range table.Rows {
		r := make(Row, len(table.Cols))
		for i, col := range table.Cols {
			if i < len(raw) {
				r[strings.ToLower(col.Name)] = raw[i]
			}
		}
		rows = append(rows, r)
	}
	return ragSource{cols: cols, rows: rows}, nil
}

func ragFindContextRows(source ragSource, docCol, chunkCol string, docID any, centerChunk, before, after int) []ragContextRow {
	minChunk := centerChunk - before
	maxChunk := centerChunk + after
	matches := make([]ragContextRow, 0, before+after+1)

	for _, r := range source.rows {
		docVal, ok := ragValue(r, docCol)
		if !ok || !rawEqual(docVal, docID) {
			continue
		}
		chunkVal, ok := ragValue(r, chunkCol)
		if !ok {
			continue
		}
		chunkIndex, err := toInt(chunkVal)
		if err != nil || chunkIndex < minChunk || chunkIndex > maxChunk {
			continue
		}
		matches = append(matches, ragContextRow{row: r, chunkIndex: chunkIndex})
	}

	sort.SliceStable(matches, func(i, j int) bool {
		return matches[i].chunkIndex < matches[j].chunkIndex
	})
	return matches
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

func ragHitRank(hit Row, fallback int) int {
	for _, col := range []string{"_vec_rank", "_hit_rank", "rank"} {
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
