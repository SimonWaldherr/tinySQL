package engine

import (
	"context"
	"fmt"
	"time"
)

// RAG_WARM builds the independent vector and lexical retrieval structures in
// parallel, shifting corpus-dependent startup work out of the first request.
// It intentionally reuses the exact caches populated by VEC_WARM/FTS_WARM.
type RAGWarmTableFunc struct{}

func (f *RAGWarmTableFunc) Name() string { return "RAG_WARM" }

func (f *RAGWarmTableFunc) ValidateArgs(args []Expr) error {
	if len(args) < 3 || len(args) > 5 {
		return fmt.Errorf("RAG_WARM requires (table, text_column, vector_column [, metric [, vector_index]])")
	}
	return nil
}

type ragVectorWarmResult struct {
	rowCount, vectorCount, dims, distinctDims, excludedRows int
	err                                                     error
}

func (f *RAGWarmTableFunc) Execute(ctx context.Context, args []Expr, env ExecEnv, row Row) (*ResultSet, error) {
	if err := f.ValidateArgs(args); err != nil {
		return nil, err
	}
	if ctx == nil {
		ctx = env.ctx
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("%s: %w", f.Name(), err)
	}
	stringArg := func(index int) (string, error) {
		value, err := evalExpr(env, args[index], row)
		if err != nil {
			return "", err
		}
		text, ok := value.(string)
		if !ok {
			return "", fmt.Errorf("%s arg%d must be a string, got %T", f.Name(), index+1, value)
		}
		return text, nil
	}
	tableName, err := stringArg(0)
	if err != nil {
		return nil, err
	}
	textColumn, err := stringArg(1)
	if err != nil {
		return nil, err
	}
	vectorColumn, err := stringArg(2)
	if err != nil {
		return nil, err
	}
	metric := "cosine"
	if len(args) >= 4 {
		raw, metricErr := stringArg(3)
		if metricErr != nil {
			return nil, metricErr
		}
		metric = normalizeVecMetric(raw)
		if metric == "" {
			return nil, fmt.Errorf("%s: unknown metric %q", f.Name(), raw)
		}
	}
	indexMode := vecIndexFlat
	if len(args) == 5 {
		raw, indexErr := stringArg(4)
		if indexErr != nil {
			return nil, indexErr
		}
		indexMode = normalizeVecIndexMode(raw)
		if indexMode == "" {
			return nil, fmt.Errorf("%s: unknown vector index %q", f.Name(), raw)
		}
	}
	tenant := env.tenant
	if tenant == "" {
		tenant = "default"
	}
	table, err := env.db.Get(tenant, tableName)
	if err != nil {
		return nil, fmt.Errorf("%s: table %q not found: %w", f.Name(), tableName, err)
	}
	textIndex, err := table.ColIndex(textColumn)
	if err != nil {
		return nil, fmt.Errorf("%s text column: %w", f.Name(), err)
	}
	vectorIndex, err := table.ColIndex(vectorColumn)
	if err != nil {
		return nil, fmt.Errorf("%s vector column: %w", f.Name(), err)
	}

	started := time.Now()
	vectorDone := make(chan ragVectorWarmResult, 1)
	ftsDone := make(chan ftsDocCacheEntry, 1)
	go func() {
		rowCount, vectorCount, dims, distinctDims, excludedRows, warmErr := warmVectorStructures(ctx, tenant, table, vectorIndex, metric, indexMode)
		vectorDone <- ragVectorWarmResult{rowCount, vectorCount, dims, distinctDims, excludedRows, warmErr}
	}()
	go func() {
		ftsDone <- getFTSDocCache(tenant, table, []int{textIndex})
	}()
	vector := <-vectorDone
	fts := <-ftsDone
	if vector.err != nil {
		return nil, fmt.Errorf("%s vector warm: %w", f.Name(), vector.err)
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("%s: %w", f.Name(), err)
	}
	postingCount := 0
	for _, postings := range fts.postings {
		postingCount += len(postings)
	}
	columns := []string{
		"table_name", "text_column", "vector_column", "metric", "index_mode", "row_count",
		"vector_count", "dims", "distinct_dims", "excluded_vectors", "fts_valid_docs", "fts_terms", "fts_postings", "elapsed_ms",
	}
	result := Row{
		"table_name": tableName, "text_column": table.Cols[textIndex].Name, "vector_column": table.Cols[vectorIndex].Name,
		"metric": metric, "index_mode": indexMode, "row_count": vector.rowCount,
		"vector_count": vector.vectorCount, "dims": vector.dims, "distinct_dims": vector.distinctDims, "excluded_vectors": vector.excludedRows,
		"fts_valid_docs": fts.numDocs, "fts_terms": len(fts.termIDs), "fts_postings": postingCount,
		"elapsed_ms": float64(time.Since(started).Nanoseconds()) / 1e6,
	}
	return &ResultSet{Cols: columns, Rows: []Row{result}}, nil
}

func init() {
	RegisterTableFunc(&RAGWarmTableFunc{})
}
