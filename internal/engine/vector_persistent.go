package engine

import (
	"fmt"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// vecPersistentFormat versions the representation stored in
// storage.Table.VectorIndexes.  It is deliberately independent from the
// runtime graph type: old or corrupt topology is rejected and rebuilt rather
// than ever being interpreted optimistically.
const vecPersistentFormat = 1

// vecPersistentHNSWHydrateHook is test-only instrumentation.  It stays nil
// in production and lets the reopen regression test prove it used persisted
// topology rather than merely rebuilding an identical deterministic graph.
var vecPersistentHNSWHydrateHook func()

func vecPersistentHNSWKey(colIdx int, metric string) string {
	return fmt.Sprintf("hnsw:%d:%s", colIdx, metric)
}

// loadPersistentVecHNSWIndex returns an independent runtime graph when a
// persisted graph is compatible with the table's present schema/content.  The
// deep copy is important: append extension mutates the runtime graph while the
// persisted structure must stay immutable until the new version is fully built
// and published.
func loadPersistentVecHNSWIndex(table *storage.Table, colIdx int, metric string, dims int, cache vecSearchColumnCacheEntry) *vecHNSWIndex {
	if table == nil {
		return nil
	}
	key := vecPersistentHNSWKey(colIdx, metric)
	table.DerivedLock()
	stored := table.VectorIndexes[key]
	if !usablePersistentVecHNSW(stored, table, colIdx, metric, dims, cache) {
		table.DerivedUnlock()
		return nil
	}
	clone := clonePersistentVecIndex(stored)
	table.DerivedUnlock()
	if vecPersistentHNSWHydrateHook != nil {
		vecPersistentHNSWHydrateHook()
	}
	return &vecHNSWIndex{
		table:         table,
		version:       clone.Version,
		structVersion: clone.StructVersion,
		metric:        metric,
		dims:          dims,
		entry:         clone.Entry,
		maxLevel:      clone.MaxLevel,
		levels:        clone.Levels,
		neighbors:     clone.Neighbors,
	}
}

func usablePersistentVecHNSW(index *storage.VectorIndex, table *storage.Table, colIdx int, metric string, dims int, cache vecSearchColumnCacheEntry) bool {
	if index == nil || index.Format != vecPersistentFormat || index.Kind != vecIndexHNSW ||
		index.Column != colIdx || index.Metric != metric || index.Dims != dims ||
		index.Version > table.Version || index.StructVersion != table.StructVersion() || index.BuiltRows < 0 ||
		index.BuiltRows > len(table.Rows) || len(index.Levels) != index.BuiltRows ||
		len(index.Neighbors) != index.BuiltRows {
		return false
	}
	// A graph at the current table version must cover every physical row.  A
	// partial topology with an apparently current version would otherwise pass
	// through the runtime-cache fast path and silently omit tail vectors.  A
	// deliberately older graph is valid only for an append-only extension.
	if index.Version == table.Version && index.BuiltRows != len(table.Rows) {
		return false
	}
	if index.BuiltRows == 0 {
		return index.Entry == -1 && index.MaxLevel == -1
	}
	// A table can have physical rows but no valid vectors (nil or mixed
	// malformed cells). buildVecHNSWIndex represents that correctly as an
	// empty graph over a non-empty row range; retain it across reopen too.
	if index.Entry == -1 || index.MaxLevel == -1 {
		if index.Entry != -1 || index.MaxLevel != -1 {
			return false
		}
		for row := 0; row < cache.rowCount(); row++ {
			if validCacheRow(cache, row, dims) {
				return false
			}
		}
		return true
	}
	if index.Entry < 0 || index.Entry >= index.BuiltRows || index.MaxLevel < 0 {
		return false
	}
	for row, layers := range index.Neighbors {
		valid := row < cache.rowCount() && validCacheRow(cache, row, dims)
		if valid {
			// Every valid vector is inserted by buildVecHNSWIndex and therefore
			// owns exactly one adjacency slice per level.  Treat a missing layer
			// as corruption rather than accepting an index that merely happens
			// to return fewer ANN candidates after reopen.
			if index.Levels[row] < 0 || len(layers) != index.Levels[row]+1 {
				return false
			}
		} else if len(layers) != 0 {
			return false
		}
		if len(layers) > index.Levels[row]+1 {
			return false
		}
		for _, neighbors := range layers {
			if len(neighbors) > vecHNSWM {
				return false
			}
			for _, neighbor := range neighbors {
				if neighbor < 0 || neighbor >= index.BuiltRows {
					return false
				}
			}
		}
	}
	return true
}

func clonePersistentVecIndex(index *storage.VectorIndex) *storage.VectorIndex {
	clone := *index
	clone.Levels = append([]int(nil), index.Levels...)
	clone.Neighbors = make([][][]int, len(index.Neighbors))
	for row, layers := range index.Neighbors {
		clone.Neighbors[row] = make([][]int, len(layers))
		for layer, neighbors := range layers {
			clone.Neighbors[row][layer] = append([]int(nil), neighbors...)
		}
	}
	return &clone
}

// persistVecHNSWIndex publishes a deep, validated snapshot of idx to the
// table.  A storage Sync/Close will subsequently flush the table even if its
// SQL rows did not change, via Table.VectorIndexesPersistenceState.
func persistVecHNSWIndex(table *storage.Table, colIdx int, metric string, idx *vecHNSWIndex) {
	if table == nil || idx == nil {
		return
	}
	idx.mu.RLock()
	persisted := &storage.VectorIndex{
		Format:        vecPersistentFormat,
		Kind:          vecIndexHNSW,
		Column:        colIdx,
		Metric:        metric,
		Version:       idx.version,
		StructVersion: idx.structVersion,
		BuiltRows:     len(idx.levels),
		Dims:          idx.dims,
		Entry:         idx.entry,
		MaxLevel:      idx.maxLevel,
		Levels:        append([]int(nil), idx.levels...),
		Neighbors:     make([][][]int, len(idx.neighbors)),
	}
	for row, layers := range idx.neighbors {
		persisted.Neighbors[row] = make([][]int, len(layers))
		for layer, neighbors := range layers {
			persisted.Neighbors[row][layer] = append([]int(nil), neighbors...)
		}
	}
	idx.mu.RUnlock()

	table.DerivedLock()
	if table.VectorIndexes == nil {
		table.VectorIndexes = make(map[string]*storage.VectorIndex)
	}
	table.VectorIndexes[vecPersistentHNSWKey(colIdx, metric)] = persisted
	table.MarkVectorIndexesChanged()
	table.DerivedUnlock()
}
