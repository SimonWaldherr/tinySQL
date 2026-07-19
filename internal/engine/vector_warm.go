// VEC_WARM — eager warm-up for vector search structures.
//
// Usage:
//
//	SELECT * FROM VEC_WARM('table_name', 'vector_column' [, 'metric' [, 'index']])
//
// Prebuilds the vector column cache (and, for cosine, the L2 norms) plus the
// requested ANN index (ivf or hnsw). This shifts the one-time O(n log n)
// index-build cost from the first query to an explicit warm-up step — e.g.
// right after a nightly bulk load — so serving queries never pay it.
//
// Returns a single row describing what was warmed:
//
//	table_name, column_name, metric, index_mode, row_count, vector_count, dims,
//	distinct_dims, excluded_rows
//
// distinct_dims counts how many different vector lengths were seen among the
// valid rows (e.g. during an embedding-model migration where old and new
// dimensionalities briefly coexist in the same column). The ANN index is
// only ever built for the first-seen dims; excluded_rows reports how many
// otherwise-valid rows were left out of that index because their vector's
// length did not match dims, so callers can detect a mixed-dimensionality
// column instead of silently getting a partial index.
package engine

import (
	"context"
	"fmt"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// VecWarmTableFunc implements the VEC_WARM table-valued function.
type VecWarmTableFunc struct{}

func (f *VecWarmTableFunc) Name() string { return "VEC_WARM" }

func (f *VecWarmTableFunc) ValidateArgs(args []Expr) error {
	if len(args) < 2 || len(args) > 4 {
		return fmt.Errorf("VEC_WARM requires 2-4 arguments: (table, column [, metric [, index]])")
	}
	return nil
}

func (f *VecWarmTableFunc) Execute(ctx context.Context, args []Expr, env ExecEnv, row Row) (*ResultSet, error) {
	if err := f.ValidateArgs(args); err != nil {
		return nil, err
	}

	evalString := func(arg Expr, what string) (string, error) {
		v, err := evalExpr(env, arg, row)
		if err != nil {
			return "", fmt.Errorf("VEC_WARM %s: %w", what, err)
		}
		s, ok := v.(string)
		if !ok {
			return "", fmt.Errorf("VEC_WARM: %s must be a string, got %T", what, v)
		}
		return s, nil
	}

	tableName, err := evalString(args[0], "table")
	if err != nil {
		return nil, err
	}
	colName, err := evalString(args[1], "column")
	if err != nil {
		return nil, err
	}
	metric := "cosine"
	if len(args) >= 3 {
		ms, err := evalString(args[2], "metric")
		if err != nil {
			return nil, err
		}
		metric = normalizeVecMetric(ms)
		if metric == "" {
			return nil, fmt.Errorf("VEC_WARM: unknown metric %q (supported: cosine, l2, euclidean, manhattan, l1, dot, inner_product)", ms)
		}
	}
	indexMode := vecIndexFlat
	if len(args) == 4 {
		is, err := evalString(args[3], "index")
		if err != nil {
			return nil, err
		}
		indexMode = normalizeVecIndexMode(is)
		if indexMode == "" {
			return nil, fmt.Errorf("VEC_WARM: unknown index %q (supported: flat, exact, ivf, hnsw)", is)
		}
	}

	tenant := env.tenant
	if tenant == "" {
		tenant = "default"
	}
	table, err := env.db.Get(tenant, tableName)
	if err != nil {
		return nil, fmt.Errorf("VEC_WARM: table %q not found: %w", tableName, err)
	}
	vecColIdx, err := table.ColIndex(colName)
	if err != nil {
		return nil, fmt.Errorf("VEC_WARM: %w", err)
	}

	warmCtx := ctx
	if warmCtx == nil {
		warmCtx = env.ctx
	}

	rowCount, vectorCount, dims, distinctDims, excludedRows, err := warmVectorStructures(warmCtx, tenant, table, vecColIdx, metric, indexMode)
	if err != nil {
		return nil, err
	}

	out := Row{
		"table_name":    tableName,
		"column_name":   colName,
		"metric":        metric,
		"index_mode":    indexMode,
		"row_count":     rowCount,
		"vector_count":  vectorCount,
		"dims":          dims,
		"distinct_dims": distinctDims,
		"excluded_rows": excludedRows,
	}
	return &ResultSet{
		Cols: []string{"table_name", "column_name", "metric", "index_mode", "row_count", "vector_count", "dims", "distinct_dims", "excluded_rows"},
		Rows: []Row{out},
	}, nil
}

// warmVectorStructures builds the column cache, norms (cosine), and the
// requested ANN index for the given column. It returns basic statistics,
// including how many distinct vector lengths were seen among valid rows
// (distinctDims) and how many otherwise-valid rows were excluded from the
// built index because their length didn't match the chosen dims
// (excludedRows) — e.g. when old and new embedding dimensionalities briefly
// coexist in the same column during a migration.
func warmVectorStructures(ctx context.Context, tenant string, table *storage.Table, colIdx int, metric, indexMode string) (rowCount, vectorCount, dims, distinctDims, excludedRows int, err error) {
	cache := getVecColumnCache(tenant, table, colIdx, metricNeedsNorms(metric))
	rowCount = len(cache.vectors)
	lenCounts := make(map[int]int)
	for i := range cache.vectors {
		if cache.valid[i] {
			vectorCount++
			l := len(cache.vectors[i])
			lenCounts[l]++
			if dims == 0 {
				dims = l
			}
		}
	}
	distinctDims = len(lenCounts)
	if dims != 0 {
		excludedRows = vectorCount - lenCounts[dims]
	}
	if dims == 0 {
		return rowCount, vectorCount, dims, distinctDims, excludedRows, nil
	}
	switch indexMode {
	case vecIndexIVF:
		_, err = getVecIVFIndex(ctx, tenant, table, colIdx, metric, dims, cache)
	case vecIndexHNSW:
		_, err = getVecHNSWIndex(ctx, tenant, table, colIdx, metric, dims, cache)
	}
	return rowCount, vectorCount, dims, distinctDims, excludedRows, err
}

func init() {
	RegisterTableFunc(&VecWarmTableFunc{})
}
