// Copies of a database: the deep clone and the transaction snapshot pair the
// SQL driver runs a transaction against, and the row-less metadata snapshot the
// engine diffs for write-ahead logging.
//
// Every clone must carry the runtime state that is not tenant data — see
// DB.copyRuntimeState in db.go. Hand-copying individual fields here is what
// once left a promoted clone without its write-ahead log.
package storage

// DeepClone creates a full copy of the database (MVCC-light snapshot).
// Note: This is not copy-on-write; it creates a full copy (simple but O(n)).
//
// The result is a shadow: it carries the full runtime state and schema of the
// original, but statements executed against it do not write to the WAL. Call
// PromoteShadow if it is going to replace the live database.
func (db *DB) DeepClone() *DB {
	out := NewDB()
	db.copyRuntimeState(out, true)
	markShadow(out)
	for tn, tdb := range db.tenants {
		for _, t := range tdb.tables {
			out.upsertTable(tn, cloneTable(t))
		}
	}
	return out
}

// SnapshotForTx creates the pair of snapshots a SQL transaction needs while
// copying row data only once instead of twice.
//
// shadow is a full deep clone that receives the transaction's writes. base is
// a lightweight snapshot that records each table's identity and Version but no
// rows: the only consumers of the base — CollectWALChanges and the driver's
// conflict detection — read Table.Version and table existence exclusively and
// never inspect rows. Copying rows into the base (as DeepClonePair does) would
// therefore waste memory proportional to the entire database on every Begin.
func (db *DB) SnapshotForTx() (base *DB, shadow *DB) {
	base = NewDB()
	shadow = NewDB()
	// The shadow needs the full schema — views, triggers, materialized views,
	// jobs, RBAC — or a statement inside the transaction cannot resolve a view
	// and its triggers silently never fire. It gets its own deep copy so
	// uncommitted DDL stays invisible to the live database until COMMIT.
	db.copyRuntimeState(shadow, true)
	markShadow(shadow)
	// The base is only ever read for table identity and Version (by
	// CollectWALChanges and the driver's conflict detection), so it needs no
	// runtime state at all. It is still flagged as a shadow so an accidental
	// write against it can never reach a WAL.
	markShadow(base)
	for tn, tdb := range db.tenants {
		for _, t := range tdb.tables {
			base.upsertTable(tn, cloneTableMeta(t))
			shadow.upsertTable(tn, cloneTable(t))
		}
	}
	return base, shadow
}

// cloneTableMeta copies a table's identity, schema and Version but not its
// rows. It backs the row-less snapshots — SnapshotForTx's conflict-detection
// base and MetaSnapshot's WAL diff pre-image — where only Version and
// existence are ever read. Cols are shared by reference: neither snapshot is
// mutated, and any schema change bumps Version, so a stale shared header
// cannot hide a change.
func cloneTableMeta(t *Table) *Table {
	nt := NewTable(t.Name, t.Cols, t.IsTemp)
	nt.Version = t.Version
	nt.structVersion = t.structVersion
	nt.rowUpdateBase = t.rowUpdateBase
	nt.rowUpdateLog = append([]rowUpdateDelta(nil), t.rowUpdateLog...)
	return nt
}

func cloneTable(t *Table) *Table {
	return cloneTableWithRows(t, cloneRows(t.Rows), true)
}

// cloneTableForStreamDML makes a private writer view for a table currently
// being read by a ResultStream. DML replaces row slices, removes them from a
// new outer slice, or appends new ones; it never writes through an existing
// row's cells. Copying only the row headers is therefore sufficient to keep
// the stream immutable and avoids cloning every JSON/vector/blob cell on a
// same-table write. Schema changes still use cloneTable above.
func cloneTableForStreamDML(t *Table) *Table {
	rows := make([][]any, len(t.Rows))
	copy(rows, t.Rows)
	return cloneTableWithRows(t, rows, false)
}

func cloneTableWithRows(t *Table, rows [][]any, copySearchIndexes bool) *Table {
	cols := make([]Column, len(t.Cols))
	copy(cols, t.Cols)
	nt := NewTable(t.Name, cols, t.IsTemp)
	nt.Version = t.Version
	nt.structVersion = t.structVersion
	nt.rowUpdateBase = t.rowUpdateBase
	nt.rowUpdateLog = append([]rowUpdateDelta(nil), t.rowUpdateLog...)
	nt.Indexes = cloneSecondaryIndexes(t.Indexes)
	nt.Stats = cloneTableStats(t.Stats)
	nt.dirtyFrom = t.dirtyFrom
	nt.dirtyRows = append([]int(nil), t.dirtyRows...)
	nt.dirtyRowsState = t.dirtyRowsState
	nt.Rows = rows
	// See DerivedCloner's doc comment: the row sequence is byte-identical at
	// clone time, even for a header-only stream-DML clone, so cloneable derived
	// state (the constraint-index cache, today) remains valid for the clone.
	t.DerivedLock()
	if copySearchIndexes {
		nt.FTSIndexes = cloneFTSIndexes(t.FTSIndexes)
		nt.ftsGeneration = t.ftsGeneration
		nt.ftsPersistedGeneration = t.ftsPersistedGeneration
		nt.VectorIndexes = cloneVectorIndexes(t.VectorIndexes)
		nt.vectorGeneration = t.vectorGeneration
		nt.vectorPersistedGeneration = t.vectorPersistedGeneration
	}
	if cloner, ok := t.derived.(DerivedCloner); ok {
		nt.derived = cloner.CloneDerived()
	}
	t.DerivedUnlock()
	return nt
}

func cloneFTSIndexes(src map[string]*FTSIndex) map[string]*FTSIndex {
	if len(src) == 0 {
		return make(map[string]*FTSIndex)
	}
	out := make(map[string]*FTSIndex, len(src))
	for key, index := range src {
		if index == nil {
			continue
		}
		clone := *index
		clone.Docs = append([]FTSDocument(nil), index.Docs...)
		clone.DocTermIDs = append([]int32(nil), index.DocTermIDs...)
		clone.DocTermCounts = append([]int32(nil), index.DocTermCounts...)
		clone.DocTokenIDs = append([]int32(nil), index.DocTokenIDs...)
		clone.PostingBlocks = make(map[string][]FTSPostingBlock, len(index.PostingBlocks))
		for term, blocks := range index.PostingBlocks {
			clone.PostingBlocks[term] = append([]FTSPostingBlock(nil), blocks...)
		}
		clone.PostingCounts = make(map[string][]int32, len(index.PostingCounts))
		for term, counts := range index.PostingCounts {
			clone.PostingCounts[term] = append([]int32(nil), counts...)
		}
		clone.Postings = make(map[string][]int32, len(index.Postings))
		for term, rows := range index.Postings {
			clone.Postings[term] = append([]int32(nil), rows...)
		}
		clone.TermIDs = make(map[string]int32, len(index.TermIDs))
		for term, id := range index.TermIDs {
			clone.TermIDs[term] = id
		}
		out[key] = &clone
	}
	return out
}

// cloneVectorIndexes returns a deep copy suitable for a transaction snapshot
// or a persistence boundary.  ANN neighbor lists are mutable while an
// append-only graph is being extended, so sharing even an inner []int would
// let the live table and a snapshot corrupt one another.
func cloneVectorIndexes(src map[string]*VectorIndex) map[string]*VectorIndex {
	if len(src) == 0 {
		return make(map[string]*VectorIndex)
	}
	out := make(map[string]*VectorIndex, len(src))
	for key, index := range src {
		if index == nil {
			continue
		}
		clone := *index
		clone.Levels = append([]int(nil), index.Levels...)
		clone.Neighbors = make([][][]int, len(index.Neighbors))
		for row, layers := range index.Neighbors {
			clone.Neighbors[row] = make([][]int, len(layers))
			for layer, neighbors := range layers {
				clone.Neighbors[row][layer] = append([]int(nil), neighbors...)
			}
		}
		out[key] = &clone
	}
	return out
}

// cloneRows copies all row headers into a single backing array. A statement
// snapshot commonly clones tens of thousands of rows; keeping the cells
// contiguous avoids one allocation per row while preserving the original
// per-row append semantics through a full slice expression.
func cloneRows(rows [][]any) [][]any {
	cloned := make([][]any, len(rows))
	maxInt := int(^uint(0) >> 1)
	totalCells := 0
	for _, row := range rows {
		if len(row) > maxInt-totalCells {
			// The contiguous allocation cannot be represented. This is only
			// reachable for an impossibly large in-memory table on supported
			// platforms, but retain the safe per-row behavior rather than
			// overflowing the allocation size.
			return cloneRowsIndividually(rows)
		}
		totalCells += len(row)
	}

	cells := make([]any, totalCells)
	offset := 0
	for i, row := range rows {
		end := offset + len(row)
		// Restrict capacity to the row length. Before this optimization each
		// row was independently allocated with cap == len, so append must not
		// be able to overwrite the next row in the shared backing array.
		copyRow := cells[offset:end:end]
		for j, value := range row {
			copyRow[j] = cloneCell(value)
		}
		cloned[i] = copyRow
		offset = end
	}
	return cloned
}

func cloneRowsIndividually(rows [][]any) [][]any {
	cloned := make([][]any, len(rows))
	for i, row := range rows {
		copyRow := make([]any, len(row))
		for j, value := range row {
			copyRow[j] = cloneCell(value)
		}
		cloned[i] = copyRow
	}
	return cloned
}

// cloneCell preserves snapshot isolation for mutable reference-typed values.
// Plain scalars (int, string, float64, bool, time.Time, *big.Rat, ...) are
// immutable/value types at the storage boundary and pass through unchanged.
//
// A JSON column's cell is not a string: coerceToJson (internal/engine/
// coerce.go) parses it once at write time into map[string]any/[]any, and
// json_path.go's JSON_SET mutates that structure in place rather than
// copying it. Without cloneJSONValue below, a clone's row and the live row it
// was cloned from shared the same map/slice object, so an UPDATE ... SET
// col = JSON_SET(col, ...) against a transaction snapshot (SnapshotForTx/
// DeepClone, used by both the SQL driver's BeginTx and the WASM browser/Node
// APIs) mutated the live, pre-transaction database immediately -- a mutation
// ROLLBACK could not undo, since there was never an independent copy to roll
// back to. A VECTOR column's []float64 is defensively copied too, even
// though no current function mutates one in place, since a shared slice
// silently stops being an isolation bug only for as long as that stays true.
func cloneCell(v any) any {
	switch x := v.(type) {
	case []byte:
		return append([]byte(nil), x...)
	case []float64:
		return append([]float64(nil), x...)
	case map[string]any, []any:
		return cloneJSONValue(x)
	default:
		return v
	}
}

// cloneJSONValue deep-copies a value tree of the shape json.Unmarshal
// produces into `any` (nested map[string]any/[]any with scalar leaves), so
// cloneCell can hand a JSON/array column's clone its own independent
// structure. Leaves (strings, numbers, bools, nil) are immutable and pass
// through unchanged.
func cloneJSONValue(v any) any {
	switch x := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, val := range x {
			out[k] = cloneJSONValue(val)
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i, val := range x {
			out[i] = cloneJSONValue(val)
		}
		return out
	default:
		return v
	}
}

// MetaSnapshot captures every table's identity and Version but none of its
// rows. It is the "before" image the engine diffs against to decide what one
// statement changed for WALManager logging.
//
// It replaces the previous approach of reusing the statement's full rollback
// snapshot for that diff, which forced a deep copy of the entire database on
// every INSERT into a WAL-backed database and still could not see a CREATE or
// DROP TABLE, because DDL takes no rollback snapshot at all. A metadata
// snapshot costs O(number of tables) and detects created, dropped and mutated
// tables alike.
func (db *DB) MetaSnapshot() *DB {
	if db == nil {
		return nil
	}
	out := NewDB()
	db.mu.RLock()
	defer db.mu.RUnlock()
	for tn, tdb := range db.tenants {
		for key, t := range tdb.tables {
			td := out.getTenant(tn)
			td.tables[key] = cloneTableMeta(t)
		}
	}
	return out
}
