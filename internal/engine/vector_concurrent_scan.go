package engine

import (
	"context"
	"slices"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

const (
	vecConcurrentMinCoordinates = 1 << 20
	vecConcurrentSpareHits      = 32
	vecConcurrentMaxChangedRows = 1024
	vecConcurrentMaxCoordinates = 1 << 20
)

func largeVecScan(rows, dims int) bool {
	return rows >= vecSearchParallelMinRows && dims > 0 && rows > (vecConcurrentMinCoordinates-1)/dims
}

// rankVecSearchAlongsideWrites is used only by a top-level SELECT that owns
// its read lock. Scoring reads immutable packed vectors (and the captured
// row count). Materialization and any fallback still run under the lock.
func rankVecSearchAlongsideWrites(ctx context.Context, env ExecEnv, tenant string, a vecSearchArgs, queryNorm float64, table *storage.Table, col int, cache vecSearchColumnCacheEntry) (*storage.Table, []vecScoredRow, error) {
	rows := table.Rows
	limit := a.k + min(vecConcurrentSpareHits, max(0, cache.rows-a.k))
	hits, err := func() ([]vecScoredRow, error) {
		env.db.UnlockContentForRead()
		defer env.db.LockContentForRead()
		if guard := env.vectorReadGuard; guard != nil {
			guard.RUnlock()
			defer guard.RLock()
		}
		return vecSearchFlat(ctx, rows, a.metric, a.queryVec, queryNorm, limit, cache)
	}()
	if err != nil {
		return nil, nil, err
	}
	if err := checkPermission(env.ctx, env.db, env.vectorReadRoot); err != nil {
		return nil, nil, err
	}
	if storage.ResidentTable(env.db, tenant, a.tableName) == table {
		hits, reusable, err := repairFlatVectorRanking(ctx, table, col, cache, hits, limit, a, queryNorm)
		if err != nil {
			return nil, nil, err
		}
		if reusable {
			finalizeVecScoredRows(a.metric, hits)
			return table, hits, nil
		}
	}

	// Take one ordinary locked scan when the ranking cannot be repaired.
	current, currentCol, err := vecSearchSource(env.db, tenant, a)
	if err != nil {
		return nil, nil, err
	}
	cache = getVecColumnCache(tenant, current, currentCol, a.metric == "cosine")
	hits, err = vecSearchTopKWithIndex(ctx, tenant, current, currentCol, a, queryNorm, cache)
	return current, hits, err
}

func repairFlatVectorRanking(ctx context.Context, table *storage.Table, col int, cache vecSearchColumnCacheEntry, hits []vecScoredRow, limit int, a vecSearchArgs, queryNorm float64) ([]vecScoredRow, bool, error) {
	rowDelta := len(table.Rows) - cache.rows
	if table.Version < cache.version || rowDelta < -vecConcurrentMaxChangedRows || rowDelta > vecConcurrentMaxChangedRows {
		return nil, false, nil
	}
	changes, ok := table.ChangesSince(cache.structVersion, cache.rows, storage.ColumnBit(col))
	if !ok {
		return nil, false, nil
	}
	rescore := len(changes.Updated) + len(table.Rows) - changes.Survivors
	if rescore+len(changes.Dead) > vecConcurrentMaxChangedRows || rescore > vecConcurrentMaxCoordinates/max(1, len(a.queryVec)) {
		return nil, false, nil
	}
	if rescore == 0 && len(changes.Dead) == 0 {
		return hits[:min(a.k, len(hits))], true, nil
	}
	complete := len(hits) < limit || limit >= cache.rows
	kept := hits[:min(a.k, len(hits))]
	if len(changes.Dead) != 0 || len(changes.Updated) != 0 {
		kept = hits[:0]
		for _, hit := range hits {
			if len(kept) == a.k {
				break
			}
			shift, removed := slices.BinarySearch(changes.Dead, hit.rowIdx)
			if _, updated := slices.BinarySearch(changes.Updated, hit.rowIdx); removed || updated {
				continue
			}
			hit.rowIdx -= shift
			kept = append(kept, hit)
		}
	}
	// With k unchanged candidates, no untouched row beyond the old ranking
	// can enter the top k. If the ranking was exhaustive, there are no others.
	if !complete && len(kept) < a.k {
		return nil, false, nil
	}
	if rescore == 0 {
		return kept, true, nil
	}
	if err := checkCtx(ctx); err != nil {
		return nil, false, err
	}

	heap := vecScoredHeap(kept)
	heapifyVecScoredRows(heap)
	distance := buildVecDistanceFunc(a.metric, a.queryVec, queryNorm)
	scoreRow := func(i int) {
		row := table.Rows[i]
		if col >= len(row) {
			return
		}
		vector, valid := vecRowValue(row[col])
		if !valid || len(vector) != len(a.queryVec) {
			return
		}
		var norm float64
		if a.metric == "cosine" {
			norm = vectorL2Norm(vector)
		}
		if score, ok := distance(vector, norm); ok {
			pushTopK(&heap, i, score, a.k)
		}
	}
	for _, row := range changes.Updated {
		scoreRow(changes.NewPosition(row))
	}
	for row := changes.Survivors; row < len(table.Rows); row++ {
		scoreRow(row)
	}
	return topKFromHeap(&heap, a.k), true, nil
}
