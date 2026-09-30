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

// SnapshotForReadTx is DeepClone for a snapshot that is only read, such as a
// read-only transaction: it shares immutable rows with the source (see
// cloneRowsShared) instead of copying them, so taking it does not cost a full
// copy of the database. Callers that write to the snapshot should use
// SnapshotForWriteTx or SnapshotForTx.
func (db *DB) SnapshotForReadTx() *DB {
	out := NewDB()
	db.copyRuntimeState(out, true)
	markShadow(out)
	for tn, tdb := range db.tenants {
		for _, t := range tdb.tables {
			out.upsertTable(tn, cloneTableForRead(t))
		}
	}
	return out
}

// SnapshotForWriteTx creates a private writable shadow, sharing immutable
// scalar rows and copying mutable cells. Embeddings with one connection can
// promote it directly without allocating a conflict-detection base. Callers
// must hold LockContentForRead while taking the snapshot, as for SnapshotForTx.
func (db *DB) SnapshotForWriteTx() *DB {
	out := NewDB()
	db.copyRuntimeState(out, true)
	markShadow(out)
	for tn, tdb := range db.tenants {
		for _, t := range tdb.tables {
			out.upsertTable(tn, cloneTableForTx(t))
		}
	}
	return out
}

// SnapshotForTx creates the pair of snapshots a SQL transaction needs while
// sharing immutable rows and copying mutable cells only once.
//
// shadow is a private writable snapshot receiving the transaction's writes. base is
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
			shadow.upsertTable(tn, cloneTableForTx(t))
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

// cloneTableForTx is cloneTable for a transaction's private shadow. The shadow
// exists to be written, and the first INSERT appends to its row slice; sizing
// that slice exactly would make the first append copy every row header a
// second time (and allocate 25% more on top), so the clone reserves a little
// room up front.
//
// The shadow shares every row that holds only scalar cells with the live table
// (see cloneRowsShared) instead of copying it: copying made BEGIN cost a full
// copy of every table.
func cloneTableForTx(t *Table) *Table {
	return cloneTableWithRows(t, cloneRowsShared(t.Rows, len(t.Rows)/16+16), true)
}

// cloneTableForRead is cloneTable for a snapshot that is only ever read, such
// as a read-only transaction: rows are shared like cloneTableForTx does, with
// no spare capacity.
func cloneTableForRead(t *Table) *Table {
	return cloneTableWithRows(t, cloneRowsShared(t.Rows, 0), true)
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
		clone.Levels, clone.Neighbors = CloneVectorTopology(index.Levels, index.Neighbors)
		out[key] = &clone
	}
	return out
}

// cloneRows copies all row headers into a single backing array. A statement
// snapshot commonly clones tens of thousands of rows; keeping the cells
// contiguous avoids one allocation per row while preserving the original
// per-row append semantics through a full slice expression.
func cloneRows(rows [][]any) [][]any {
	return cloneRowsHeadroom(rows, 0)
}

// cloneRowsHeadroom is cloneRows with spare capacity in the returned row slice.
func cloneRowsHeadroom(rows [][]any, headroom int) [][]any {
	cloned := make([][]any, len(rows), len(rows)+headroom)
	maxInt := int(^uint(0) >> 1)
	totalCells := 0
	for _, row := range rows {
		if len(row) > maxInt-totalCells {
			// The contiguous allocation cannot be represented. This is only
			// reachable for an impossibly large in-memory table on supported
			// platforms, but retain the safe per-row behavior rather than
			// overflowing the allocation size.
			return cloneRowsIndividually(rows, headroom)
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
		copy(copyRow, row)
		cloneMutableCells(copyRow)
		cloned[i] = copyRow
		offset = end
	}
	return cloned
}

func cloneRowsIndividually(rows [][]any, headroom int) [][]any {
	cloned := make([][]any, len(rows), len(rows)+headroom)
	for i, row := range rows {
		copyRow := make([]any, len(row))
		copy(copyRow, row)
		cloneMutableCells(copyRow)
		cloned[i] = copyRow
	}
	return cloned
}

// cloneRowsShared copies the row-header slice but shares each row whose cells
// are all immutable scalars with the source, relying on Table.Rows' rule that a
// stored row slice is never written through. A row holding a mutable reference
// cell (BLOB, VECTOR, JSON: see cloneCell) gets a private copy, exactly as
// cloneRows would give it, so JSON_SET and friends still cannot reach the
// source through a transaction.
//
// Sharing turns the per-row cost from copying every cell into one type test per
// cell, and the allocation from the whole table into its row headers.
func cloneRowsShared(rows [][]any, headroom int) [][]any {
	cloned := make([][]any, len(rows), len(rows)+headroom)
	var chunk []any // backing store for the private copies, carved per row
	for i, row := range rows {
		if !hasMutableCell(row) {
			cloned[i] = row
			continue
		}
		if len(chunk) < len(row) {
			chunk = make([]any, max(len(row), 1024))
		}
		private := chunk[:len(row):len(row)]
		chunk = chunk[len(row):]
		copy(private, row)
		cloneMutableCells(private)
		cloned[i] = private
	}
	return cloned
}

func hasMutableCell(row []any) bool {
	for _, value := range row {
		switch value.(type) {
		case []byte, []float64, map[string]any, []any:
			return true
		}
	}
	return false
}

// cloneMutableCells replaces, in a row that was just copied, every cell whose
// value is a mutable reference type with a private copy (see cloneCell). The
// scalar cells that make up nearly every row were already copied by the caller
// with one memmove, so this is only a type test per cell: calling cloneCell for
// each of them was a third of the cost of cloning a large table.
func cloneMutableCells(row []any) {
	for j, value := range row {
		switch value.(type) {
		case []byte, []float64, map[string]any, []any:
			row[j] = cloneCell(value)
		}
	}
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

// CloneVectorTopology deep-copies an ANN graph's per-row levels and neighbor
// lists. Every statement snapshot of a table clones its persisted graphs, and
// the executor copies a graph each time it persists or reloads one, so the
// copy packs all neighbor lists into one backing array and all per-row layer
// headers into another instead of allocating each list separately: three
// allocations instead of one per row and layer. Each list is capped at its own
// length, so an append to one copy reallocates rather than writing into its
// neighbor's storage, and nothing is shared with the source. Empty lists stay
// nil, as the per-list copies produced them.
func CloneVectorTopology(levels []int, neighbors [][][]int) ([]int, [][][]int) {
	outLevels := append([]int(nil), levels...)
	layerCount, linkCount := 0, 0
	for _, layers := range neighbors {
		layerCount += len(layers)
		for _, links := range layers {
			linkCount += len(links)
		}
	}
	outNeighbors := make([][][]int, len(neighbors))
	layerArena := make([][]int, layerCount)
	linkArena := make([]int, linkCount)
	for row, layers := range neighbors {
		rowLayers := layerArena[:len(layers):len(layers)]
		layerArena = layerArena[len(layers):]
		for layer, links := range layers {
			if len(links) == 0 {
				continue
			}
			dst := linkArena[:len(links):len(links)]
			linkArena = linkArena[len(links):]
			copy(dst, links)
			rowLayers[layer] = dst
		}
		outNeighbors[row] = rowLayers
	}
	return outLevels, outNeighbors
}
