// Table: the rows of one table plus the schema and statistics describing them,
// and the dirty tracking that lets the write-ahead log write a delta instead of
// a whole table.
package storage

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Table stores rows along with column metadata and indexes.
type Table struct {
	Name string
	Cols []Column
	// Rows holds the table's rows. A stored row slice is immutable: a writer
	// that changes a row builds a new slice and replaces the slot
	// (Rows[i] = next), it never assigns into an existing row's cells, and it
	// never appends to one without first clipping its capacity. The rows of a
	// transaction's shadow rely on this: SnapshotForTx shares each row that
	// holds only scalar cells with the live table instead of copying it, which
	// is what keeps BEGIN from costing a full copy of every table. Row
	// contents that are themselves mutable (BLOB, VECTOR and JSON cells) are
	// still copied per row; see cloneRowsShared.
	Rows [][]any
	// Indexes contains materialized secondary and composite indexes keyed by
	// lower-case SQL index name. Unlike catalog metadata these entries are
	// used by the executor and persisted with table snapshots.
	Indexes map[string]*SecondaryIndex
	// FTSIndexes contains persisted, executor-built full-text indexes keyed by
	// the searched column positions (for example "1,2"). They are derived
	// from Rows, but unlike the process-local hot cache they survive database
	// reopen. Version and StructVersion let the executor either reuse an index,
	// extend it for append-only INSERTs, or rebuild it after UPDATE/DELETE/DDL.
	FTSIndexes map[string]*FTSIndex
	// VectorIndexes contains persisted ANN structures keyed by the executor's
	// stable index key (kind, column position and metric).  Like FTSIndexes,
	// these are derived from Rows but deliberately live with the table so a
	// reopened database can use a validated graph rather than rebuilding it on
	// its first vector query.
	VectorIndexes map[string]*VectorIndex
	// ftsGeneration changes when a lazy FTS build changes persisted metadata;
	// ftsPersistedGeneration records the generation most recently flushed.
	// This is deliberately separate from Version: a SELECT may build an index
	// and must become durable without pretending the table's SQL data changed.
	ftsGeneration             int
	ftsPersistedGeneration    int
	vectorGeneration          int
	vectorPersistedGeneration int
	IsTemp                    bool
	CheckConstraints          []CheckConstraint
	colPos                    map[string]int
	Version                   int
	// Stats is populated by ANALYZE and persisted with the table. DML marks it
	// stale rather than trying to estimate distinct values incrementally.
	Stats *TableStats
	// dirtyFrom tracks the first row index modified since the last
	// WAL checkpoint. -1 means no dirty rows (full table must be logged).
	// For append-only workloads (INSERT without UPDATE/DELETE), this
	// enables the WAL to log only new rows instead of the entire table.
	dirtyFrom int
	// dirtyRows lists the rows an UPDATE replaced in place since the last
	// ResetDirty, letting the WAL log those rows instead of the whole table.
	// It is only trusted while dirtyRowsState is dirtyRowsExact; anything that
	// adds, removes or reorders rows takes the state to dirtyRowsUnknown
	// through MarkDirtyFrom, and the WAL falls back to a full-table record.
	// The fallback is the safe direction: a missed MarkRowUpdated costs one
	// oversized record, never a lost change.
	dirtyRows      []int
	dirtyRowsState dirtyRowsState
	// structVersion increments on every mutation that changes or removes an
	// existing row's content (UPDATE, DELETE, a cascaded foreign-key action,
	// or a schema change) but never on a pure append of new rows. Unlike
	// dirtyFrom/dirtyRows it is never reset by ResetDirty, so it stays valid
	// as a signal across an arbitrarily long window, not just since the last
	// WAL checkpoint. See noteStructuralChange and StructVersion: this exists
	// so a derived structure that only knows how to grow by appending (e.g.
	// the HNSW vector index in engine/vector_index.go) can tell "only inserts
	// happened since I was built" from "something else happened" without
	// paying for a full rebuild on every append.
	structVersion int
	// rowUpdateLog records the physical rows changed by UPDATE-only structural
	// versions. It is deliberately separate from dirtyRows: WAL checkpoints
	// reset dirtyRows, whereas derived search indexes may be queried much later.
	// A DELETE, reorder or schema mutation clears the log and raises its base,
	// because physical row positions can no longer be translated safely.
	rowUpdateLog  rowUpdateHistory
	rowUpdateBase int
	// rowUpdateFolds summarises the oldest deltas of a long log whose per-row
	// detail was dropped to bound memory: each fold names the highest structVersion
	// it covers and the union of the columns changed up to there. The versions
	// between rowUpdateBase and the first retained delta are exactly the folds'
	// ranges. A reader whose stamp lies inside a fold can still be told that no
	// column it depends on changed (UpdatedRowsSinceCols) but never which rows.
	// A removal is not summarised: it stays in the list as a fold of its own that
	// carries its positions (see rowUpdateFold), so the DELETEs of a long window
	// are still composed by ChangesSince however many UPDATEs followed them.
	rowUpdateFolds []rowUpdateFold
	// derived is a slot for executor-owned state that is a pure function of
	// this table's contents and can always be rebuilt from them: today, the
	// constraint-value index that turns PRIMARY KEY and UNIQUE enforcement
	// into a hash lookup instead of a full scan (see internal/engine).
	// storage neither builds nor interprets it, which is why the type is any.
	//
	// It lives on the table rather than in a process-global map keyed by
	// *Table, which is where the engine used to keep it, because that map
	//   - put every constrained INSERT/UPDATE in the process behind a single
	//     mutex, so writers to unrelated tables contended with each other and
	//     with readers;
	//   - made each maintenance operation cost O(cached columns across every
	//     table in the process) rather than O(this table's columns); and
	//   - never dropped an entry, so every table that was ever written — every
	//     transaction shadow, every evicted backend page, every dropped table —
	//     stayed reachable, with all of its rows, for the process's lifetime.
	//
	// A clone starts with an empty slot unless the stored value implements
	// DerivedCloner, in which case cloneTable copies it: cloneTable's row
	// copy is byte-identical to the source at the moment of cloning, so
	// derived state describing "these rows have these values" remains
	// exactly as true of the clone as of the table it was built from, until
	// the two independently diverge afterwards. Skipping that copy when
	// possible was never a correctness requirement, only the simplest
	// choice; see DerivedCloner for why it stopped being simplest once a
	// per-transaction table clone (SnapshotForTx, called on every db.Begin())
	// made "rebuild this from scratch" a per-transaction cost, not a
	// per-table one.
	derivedMu sync.Mutex
	derived   any
	// twin is a spare row-header array kept equal to Rows so the next
	// transaction that writes this table need not copy Rows; twinOrigin is set
	// on a transaction's private copy and names the table it replaces. See
	// row_twin.go.
	twin       atomic.Pointer[rowTwin]
	twinOrigin *rowTwinOrigin
}

// rowUpdateDelta is one event of the table's ordered change log: either an
// UPDATE of one row (removed == nil) or a DELETE that removed several rows at
// once (removed != nil). Both advance structVersion by exactly one.
type rowUpdateDelta struct {
	structVersion int
	// row is the position of the updated row in the table as it was at that
	// moment. It is unused (-1) for a removal.
	row int
	// cols is the set of columns the UPDATE changed (see ColumnMask). Zero means
	// "unknown", which every consumer must treat as "every column".
	cols uint64
	// removed is non-nil for a DELETE that reported the rows it removed.
	removed *rowRemoval
}

// rowRemoval lists the positions a DELETE removed, in the coordinates of the
// table immediately before that statement, strictly ascending. It is immutable
// once logged, so clones of the log share it.
type rowRemoval struct {
	positions []int32
}

// Bounds on the removal events of the change log. A log holding more removed
// positions than this sheds the OLDEST removals (see shedRemovedPositions): a
// consumer stamped before them rebuilds, one that is newer keeps following. The
// other bound (the number of events) is maxRowUpdateDeltas, shared with UPDATE
// events.
const (
	maxRowRemovedPositions = 1 << 20
	// maxChangeComposeMoves bounds the work ChangesSince may spend merging removal
	// events (element moves); a log that would cost more reports ok=false, which
	// makes the consumer rebuild, exactly as an overflowing log does.
	maxChangeComposeMoves = 1 << 28
)

// rowUpdateFold is either a summary of consecutive dropped deltas (removed ==
// nil): every structVersion up to upTo (and after the previous fold, or
// rowUpdateBase) was an UPDATE, and cols is the union of the columns they
// changed (all bits when one was unknown); or one retained removal (removed !=
// nil), which covers exactly the structVersion upTo and keeps its positions.
type rowUpdateFold struct {
	upTo    int
	cols    uint64
	removed *rowRemoval
}

// rowUpdateChunk is the number of deltas per sealed block of a rowUpdateHistory.
const rowUpdateChunk = 64

// rowUpdateHistory is an append-only list stored in blocks. Existing deltas
// and sealed block headers are never overwritten, so clones can share them.
// Clipped capacities make each clone copy a buffer before extending it.
type rowUpdateHistory struct {
	sealed [][]rowUpdateDelta // full blocks of rowUpdateChunk deltas, immutable
	open   []rowUpdateDelta   // the block being filled, owned by this history
	// removedTotal is the number of positions held by the removal events of the
	// log and of the folds that carry removals (kept by add; foldRowUpdateLog and
	// shedRemovedPositions subtract what they forget).
	removedTotal int
}

func (h *rowUpdateHistory) len() int { return len(h.sealed)*rowUpdateChunk + len(h.open) }

func (h *rowUpdateHistory) at(i int) rowUpdateDelta {
	if block := i / rowUpdateChunk; block < len(h.sealed) {
		return h.sealed[block][i%rowUpdateChunk]
	}
	return h.open[i-len(h.sealed)*rowUpdateChunk]
}

func (h *rowUpdateHistory) add(delta rowUpdateDelta) {
	if delta.removed != nil {
		h.removedTotal += len(delta.removed.positions)
	}
	if len(h.open) == rowUpdateChunk {
		h.sealed = append(h.sealed, h.open[:rowUpdateChunk:rowUpdateChunk])
		h.open = nil
	}
	if len(h.open) == cap(h.open) {
		// A cloned prefix has no spare capacity. Copy before extending it;
		// the lender may independently append to its own open block.
		capacity := rowUpdateChunk
		if len(h.open) > 0 {
			// Reserve less for small cloned tails. Geometric growth bounds
			// repeated prefix copies when a statement updates many rows.
			capacity = min(rowUpdateChunk, 2*len(h.open))
		}
		open := make([]rowUpdateDelta, len(h.open), capacity)
		copy(open, h.open)
		h.open = open
	}
	h.open = append(h.open, delta)
}

func (h *rowUpdateHistory) reset() { *h = rowUpdateHistory{} }

// dropBlocks forgets the oldest n sealed blocks. The remaining blocks are
// untouched, so clones that still share the dropped ones keep them.
func (h *rowUpdateHistory) dropBlocks(n int) {
	h.sealed = h.sealed[n:len(h.sealed):len(h.sealed)]
}

// clone shares immutable prefixes. Appends only write beyond those prefixes;
// clipped capacities ensure a borrower cannot overwrite a lender's next slot.
func (h *rowUpdateHistory) clone() rowUpdateHistory {
	return rowUpdateHistory{
		sealed:       h.sealed[:len(h.sealed):len(h.sealed)],
		open:         h.open[:len(h.open):len(h.open)],
		removedTotal: h.removedTotal,
	}
}

// search returns the position of the first delta whose structVersion is at
// least version.
func (h *rowUpdateHistory) search(version int) int {
	return sort.Search(h.len(), func(i int) bool { return h.at(i).structVersion >= version })
}

// FTSDocument is the compact per-row directory for a persisted FTS index.
// The offsets address the index's contiguous term/frequency/token arenas.
type FTSDocument struct {
	TermStart  int32
	TermCount  int32
	TokenStart int32
	TokenCount int32
	DocLen     float64
	Valid      bool
}

// FTSPostingBlockSize is the interval used by the format-2 score bounds.
const FTSPostingBlockSize = 128

// FTSPostingBlock stores score-independent bounds for a posting interval.
type FTSPostingBlock struct {
	MaxFrequency int32
	MinDocLen    float64
}

// FTSIndex is the backend-neutral persisted representation of the BM25
// document index used by FTS_SEARCH. Format allows future encoding changes.
type FTSIndex struct {
	Format        int
	Version       int
	StructVersion int
	BuiltRows     int
	Docs          []FTSDocument
	AvgDocLen     float64
	TotalDocLen   float64
	Postings      map[string][]int32
	// PostingCounts is position-aligned with Postings for each term (format 2).
	PostingCounts map[string][]int32
	PostingBlocks map[string][]FTSPostingBlock
	NumDocs       int
	TermIDs       map[string]int32
	DocTermIDs    []int32
	DocTermCounts []int32
	DocTokenIDs   []int32
	// Stale* count arena entries superseded by UPDATE-only maintenance. They
	// let the executor compact before a long update-heavy workload retains an
	// unbounded trail of obsolete document terms/tokens.
	StaleTerms  int
	StaleTokens int

	// verified and termNames are engine-owned caches, never persisted (gob and
	// JSON skip unexported fields). A decoded index starts unverified; Clone carries
	// both over, because a deep copy of a validated structure is valid. They are
	// read and written under the owning table's DerivedLock, like the rest of the
	// index.
	verified  bool
	termNames []string
}

// Verified reports whether the engine has validated the whole structure
// (posting/count/block alignment, arena ranges, term ids) since the last time
// it could have been damaged: false for an index that was decoded, cloned or
// touched by a mutation that did not finish.
func (i *FTSIndex) Verified() bool { return i.verified }

// SetVerified records the result of a full validation, or false when the
// structure may no longer be consistent.
func (i *FTSIndex) SetVerified(v bool) { i.verified = v }

// TermNames returns the dense inverse of TermIDs: TermNames()[id] is the term
// with that id. It is built on first use and kept by SetTermNames afterwards; a
// cached slice whose length no longer matches TermIDs is rebuilt.
func (i *FTSIndex) TermNames() []string {
	if len(i.termNames) != len(i.TermIDs) {
		names := make([]string, len(i.TermIDs))
		for term, id := range i.TermIDs {
			if id >= 0 && int(id) < len(names) {
				names[id] = term
			}
		}
		i.termNames = names
	}
	return i.termNames
}

// SetTermNames stores the dense inverse of TermIDs after terms were appended;
// names must have exactly one entry per term id.
func (i *FTSIndex) SetTermNames(names []string) { i.termNames = names }

// VectorIndex is the backend-neutral persisted representation of an ANN
// index.  The current format stores HNSW topology; the Kind field makes the
// envelope forward-compatible with additional vector index families without
// coupling storage to the execution engine's runtime types.
//
// Row positions are physical table row positions.  StructVersion and
// BuiltRows distinguish append-only growth (which can extend a graph) from a
// mutation of an existing row (which must rebuild it).
type VectorIndex struct {
	Format        int
	Kind          string
	Column        int
	Metric        string
	Version       int
	StructVersion int
	BuiltRows     int
	Dims          int
	Entry         int
	MaxLevel      int
	Levels        []int
	Neighbors     [][][]int
	// Removed is how many graph nodes followed removals dropped since the graph
	// was last built from scratch (0 for a fresh build and for graphs stored by
	// older versions); it bounds how far the graph may drift from a fresh build.
	Removed int
}

// DerivedCloner is implemented by derived state (see the field above) that
// knows how to produce an independent copy of itself. cloneTable checks for
// it via a type assertion so storage never has to know the concrete type
// stored there — today, only internal/engine's constraint-index cache
// implements it. Derived state that does not implement DerivedCloner is
// simply dropped on clone, exactly as before this interface existed.
//
// CloneDerived must return a value that shares no mutable state with the
// receiver: the clone and the table it was cloned from immediately start
// diverging independently (new rows appended, rows rolled back, etc.), so
// anything shared between them (e.g. a map, or a []int slice that either
// side might later append to) would let one side's later mutation corrupt
// the other's cached state.
type DerivedCloner interface {
	CloneDerived() any
}

// DerivedBorrower is implemented by derived state that can lend itself to the
// copy of a table without duplicating its contents. The lender and the borrower
// share the underlying structure read-only until either one first changes it,
// at which point that side copies what it needs; neither can observe the
// other's later changes.
//
// BorrowDerived reports false when the state cannot be lent (the caller then
// falls back to DerivedCloner). Like CloneDerived it is called with the
// source's DerivedLock held.
type DerivedBorrower interface {
	BorrowDerived() (any, bool)
}

// DerivedLock locks this table's derived-state slot. Derived and SetDerived
// may only be called while it is held; pair it with DerivedUnlock.
//
// The lock is per-table by design: two statements writing different tables
// never meet on it, which is the entire reason the state moved here. Two
// touching the *same* table are already serialized by DB's content lock — this
// exists for the paths that hold only its read side, where two concurrent
// SELECTs can both find a cold cache and try to build it.
func (t *Table) DerivedLock() { t.derivedMu.Lock() }

// DerivedUnlock releases the lock taken by DerivedLock.
func (t *Table) DerivedUnlock() { t.derivedMu.Unlock() }

// Derived returns the executor state stored on this table, or nil. The caller
// must hold DerivedLock.
func (t *Table) Derived() any { return t.derived }

// SetDerived stores executor state on this table; nil discards it. The caller
// must hold DerivedLock.
func (t *Table) SetDerived(v any) { t.derived = v }

// HasDerivedState reports whether the derived-state slot holds anything, without
// taking DerivedLock. Only a caller that holds the database's content lock for
// WRITE may use it: every writer of the slot (the constraint indexes a search or
// an INSERT builds) holds at least the read side, so none can run, and the
// answer is exact. A statement that finds the slot empty then skips the lock,
// which a Sync holds for the whole of its search-image copy.
func (t *Table) HasDerivedState() bool { return t.derived != nil }

// MarkFTSIndexesChanged records a persist-relevant FTS metadata change. The
// caller must hold DerivedLock, matching direct access to FTSIndexes.
func (t *Table) MarkFTSIndexesChanged() { t.ftsGeneration++ }

// MarkVectorIndexesChanged records a persist-relevant ANN metadata change.
// The caller must hold DerivedLock, matching direct access to VectorIndexes.
func (t *Table) MarkVectorIndexesChanged() { t.vectorGeneration++ }

// FTSIndexesPersistenceState atomically returns the current FTS generation
// and whether it has been flushed to the storage backend.
func (t *Table) FTSIndexesPersistenceState() (generation int, dirty bool) {
	t.DerivedLock()
	generation = t.ftsGeneration
	dirty = generation != t.ftsPersistedGeneration
	t.DerivedUnlock()
	return generation, dirty
}

// MarkFTSIndexesPersisted marks generation durable unless a newer FTS build
// completed while the backend was saving the older snapshot.
func (t *Table) MarkFTSIndexesPersisted(generation int) {
	t.DerivedLock()
	if generation > t.ftsPersistedGeneration && generation <= t.ftsGeneration {
		t.ftsPersistedGeneration = generation
	}
	t.DerivedUnlock()
}

// VectorIndexesPersistenceState atomically returns the current ANN generation
// and whether it has crossed a persistence boundary.
func (t *Table) VectorIndexesPersistenceState() (generation int, dirty bool) {
	t.DerivedLock()
	generation = t.vectorGeneration
	dirty = generation != t.vectorPersistedGeneration
	t.DerivedUnlock()
	return generation, dirty
}

// MarkVectorIndexesPersisted marks ANN metadata durable unless a newer build
// completed while the backend was saving an older snapshot.
func (t *Table) MarkVectorIndexesPersisted(generation int) {
	t.DerivedLock()
	if generation > t.vectorPersistedGeneration && generation <= t.vectorGeneration {
		t.vectorPersistedGeneration = generation
	}
	t.DerivedUnlock()
}

// snapshotFTSIndexes takes a backend-safe deep copy. FTS indexes may be built
// by concurrent SELECTs, so persistence cannot read their maps and arenas
// without the per-table derived lock.
func (t *Table) snapshotFTSIndexes() map[string]*FTSIndex {
	t.DerivedLock()
	indexes := cloneFTSIndexes(t.FTSIndexes)
	t.DerivedUnlock()
	return indexes
}

// snapshotVectorIndexes takes a backend-safe deep copy.  ANN graphs are built
// or append-extended by concurrent reads, so persistence must not retain their
// mutable slices after it releases the table's derived-state lock.
func (t *Table) snapshotVectorIndexes() map[string]*VectorIndex {
	t.DerivedLock()
	indexes := cloneVectorIndexes(t.VectorIndexes)
	t.DerivedUnlock()
	return indexes
}

// dropDerived discards this table's executor state without the caller having
// to take the lock itself. Every storage-side path that replaces rows behind
// the executor's back — a rollback restore, a WAL replay — must call it: the
// state describes rows that are no longer there.
func (t *Table) dropDerived() {
	if t == nil {
		return
	}
	t.derivedMu.Lock()
	t.derived = nil
	t.derivedMu.Unlock()
}

// dropDerivedExclusive is dropDerived for a caller that holds the database's
// content lock for write (a statement restoring its rollback state): it skips the
// derived-state lock when the slot is empty, which a Sync holds for the whole of
// its search-image copy. See HasDerivedState.
func (t *Table) dropDerivedExclusive() {
	if t == nil || t.derived == nil {
		return
	}
	t.dropDerived()
}

// dirtyRowsState distinguishes "no in-place updates recorded yet" from "the
// recorded list is complete" and from "row positions moved, so no list can
// describe this change".
type dirtyRowsState uint8

const (
	dirtyRowsNone dirtyRowsState = iota
	dirtyRowsExact
	dirtyRowsUnknown
)

// ColumnStats summarizes one column as of TableStats.AnalyzedAt. Min and Max
// are display values for introspection; the planner currently uses row and
// distinct counts, which remain meaningful across all supported column types.
type ColumnStats struct {
	NullCount     int
	DistinctCount int
	Min           string
	Max           string
	HasMinMax     bool
}

// TableStats is the persisted result of ANALYZE for one table.
type TableStats struct {
	RowCount   int
	Columns    map[string]ColumnStats // lower-cased column name → statistics
	AnalyzedAt time.Time
	Stale      bool
}

// CloneCheckConstraints returns a copy of src (or nil if empty).
func CloneCheckConstraints(src []CheckConstraint) []CheckConstraint {
	if len(src) == 0 {
		return nil
	}
	out := make([]CheckConstraint, len(src))
	copy(out, src)
	return out
}

// NewTable creates a new Table with case-insensitive column lookup indices.
func NewTable(name string, cols []Column, isTemp bool) *Table {
	pos := make(map[string]int, len(cols))
	for i, c := range cols {
		pos[strings.ToLower(c.Name)] = i
	}
	return &Table{Name: name, Cols: cols, colPos: pos, IsTemp: isTemp, dirtyFrom: -1, Indexes: make(map[string]*SecondaryIndex), FTSIndexes: make(map[string]*FTSIndex), VectorIndexes: make(map[string]*VectorIndex)}
}

// Analyze computes exact cardinality, null and simple range summaries for the
// current table contents. The first statistics implementation scans all rows
// deliberately: transparent and correct inputs are more useful than a sampled
// model whose accuracy would need separate policy and tuning.
func (t *Table) Analyze() *TableStats {
	return cloneTableStats(t.analyze())
}

// AnalyzeSummary refreshes the persisted statistics and returns just the
// values needed by callers that report ANALYZE progress. It avoids cloning
// every per-column statistic when the detailed result will not be inspected.
// The caller must already provide the same table-content synchronization that
// is required for Analyze.
func (t *Table) AnalyzeSummary() (rowCount, columnCount int, analyzedAt time.Time) {
	stats := t.analyze()
	return stats.RowCount, len(stats.Columns), stats.AnalyzedAt
}

func (t *Table) analyze() *TableStats {
	stats := &TableStats{
		RowCount:   len(t.Rows),
		Columns:    make(map[string]ColumnStats, len(t.Cols)),
		AnalyzedAt: time.Now().UTC(),
	}
	for colIdx, column := range t.Cols {
		columnStats := ColumnStats{}
		distinct := make(map[string]struct{})
		var minValue, maxValue any
		for _, row := range t.Rows {
			if colIdx >= len(row) || row[colIdx] == nil {
				columnStats.NullCount++
				continue
			}
			value := row[colIdx]
			distinct[analyzeDistinctKey(value)] = struct{}{}
			if !columnStats.HasMinMax || statsLess(value, minValue) {
				minValue = value
			}
			if !columnStats.HasMinMax || statsLess(maxValue, value) {
				maxValue = value
			}
			columnStats.HasMinMax = true
		}
		columnStats.DistinctCount = len(distinct)
		if columnStats.HasMinMax {
			columnStats.Min = fmt.Sprint(minValue)
			columnStats.Max = fmt.Sprint(maxValue)
		}
		stats.Columns[strings.ToLower(column.Name)] = columnStats
	}
	t.Stats = stats
	return stats
}

// analyzeDistinctKey uses the same durable, type-tagged representation as
// secondary indexes. The stack-backed buffer avoids allocating a temporary
// []byte for the common scalar case; only the map-owned string is retained.
func analyzeDistinctKey(value any) string {
	var buffer [64]byte
	return string(AppendCanonicalIndexValue(buffer[:0], value))
}

// InvalidateStats marks the previous ANALYZE result stale after a mutation.
// RowCount remains useful for observability while distinct/range values are
// excluded from planner decisions until ANALYZE is run again.
func (t *Table) InvalidateStats() {
	if t.Stats == nil {
		return
	}
	t.Stats.RowCount = len(t.Rows)
	t.Stats.Stale = true
}

// Statistics returns a defensive copy of the latest ANALYZE result.
func (t *Table) Statistics() *TableStats { return cloneTableStats(t.Stats) }

func cloneTableStats(stats *TableStats) *TableStats {
	if stats == nil {
		return nil
	}
	copy := *stats
	copy.Columns = make(map[string]ColumnStats, len(stats.Columns))
	for name, column := range stats.Columns {
		copy.Columns[name] = column
	}
	return &copy
}

func statsLess(left, right any) bool {
	if right == nil {
		return true
	}
	leftNumber, leftIsNumber := statsNumber(left)
	rightNumber, rightIsNumber := statsNumber(right)
	if leftIsNumber && rightIsNumber {
		return leftNumber < rightNumber
	}
	if leftTime, ok := left.(time.Time); ok {
		if rightTime, ok := right.(time.Time); ok {
			return leftTime.Before(rightTime)
		}
	}
	if leftDuration, ok := left.(time.Duration); ok {
		if rightDuration, ok := right.(time.Duration); ok {
			return leftDuration < rightDuration
		}
	}
	return fmt.Sprint(left) < fmt.Sprint(right)
}

func statsNumber(value any) (float64, bool) {
	switch value := value.(type) {
	case int:
		return float64(value), true
	case int8:
		return float64(value), true
	case int16:
		return float64(value), true
	case int32:
		return float64(value), true
	case int64:
		return float64(value), true
	case uint:
		return float64(value), true
	case uint8:
		return float64(value), true
	case uint16:
		return float64(value), true
	case uint32:
		return float64(value), true
	case uint64:
		return float64(value), true
	case float32:
		return float64(value), true
	case float64:
		return value, true
	default:
		return 0, false
	}
}

// MarkDirtyFrom records the first row index that was modified. If an earlier
// index is already set, it is kept. Use -1 for non-append mutations (UPDATE,
// DELETE) to force a full-table WAL entry.
//
// The -1 (full-table) sentinel is sticky: once a mutation within a transaction
// forces a full-table entry, a later append-only INSERT must not downgrade it
// to a delta, or the earlier UPDATE/DELETE would be lost on WAL recovery.
// Any call also gives up on the in-place row list: an append shifts nothing but
// adds rows the list cannot describe, and an explicit -1 from a caller that is
// not reporting individual rows means the shape of the change is unknown.
func (t *Table) MarkDirtyFrom(idx int) {
	t.dirtyRows = nil
	t.dirtyRowsState = dirtyRowsUnknown
	if idx < 0 {
		t.dirtyFrom = -1
		t.noteStructuralInvalidation()
		return
	}
	if t.dirtyFrom < 0 {
		return // full-table WAL entry already forced; keep the sentinel
	}
	if t.dirtyFrom <= idx {
		return // already tracking earlier rows
	}
	t.dirtyFrom = idx
}

// MarkDirtyKeepingRows records that the table must be persisted and logged as a
// whole (a full-table WAL record, no row delta) although no row's content,
// position or count changed: an index or other table-level definition was
// created or dropped. It is MarkDirtyFrom(-1) without the structural
// invalidation, because structVersion is the signal that existing rows changed
// and the derived full-text and vector structures key on it; advancing it for a
// change that left every row alone would make them rebuild for nothing.
func (t *Table) MarkDirtyKeepingRows() {
	t.dirtyRows = nil
	t.dirtyRowsState = dirtyRowsUnknown
	t.dirtyFrom = -1
}

// MarkRowUpdated records that row idx was replaced in place, so the WAL can log
// that row instead of the entire table. UPDATE is the only shape this fits: it
// changes row contents without changing how many rows there are or where they
// sit.
//
// Callers must report every row they change. Reporting none is safe — the WAL
// writes the whole table, as it always did. Reporting some but not all is not,
// so a mutation path that cannot enumerate its rows must call MarkDirtyFrom(-1)
// instead, which permanently gives up the list for this dirty window.
//
// The list is bounded in two ways, because ResetDirty is only reached at a WAL
// checkpoint: a database with no WAL attached never resets it at all, and one
// with a WAL still accumulates between checkpoints. Every rollback snapshot
// carries this list, so an unbounded one makes repeated UPDATEs quadratic.
func (t *Table) MarkRowUpdated(idx int) {
	t.MarkRowUpdatedCols(idx, 0)
}

// MarkRowUpdatedCols is MarkRowUpdated for a caller that knows exactly which
// columns of row idx the UPDATE changed. cols is a ColumnMask; zero means the
// set is unknown and is treated as every column, so passing the mask of an
// empty change list is always safe. A derived structure over a column that is
// not in cols may keep serving its data (see UpdatedRowsSinceCols), which is
// only correct when cols names EVERY column whose cell differs afterwards:
// callers whose statement can alter other cells (trigger bodies, cascades,
// generated columns) must pass zero or invalidate instead.
func (t *Table) MarkRowUpdatedCols(idx int, cols uint64) {
	t.dirtyFrom = -1
	t.noteRowUpdatedCols(idx, cols)
	if t.dirtyRowsState == dirtyRowsUnknown || idx < 0 {
		return
	}
	// Bound 1: repeatedly rewriting the same row — a counter, a status
	// column, a claimed queue slot — is a common shape, and re-appending idx
	// would grow the list (and the WAL record built from it, which writes
	// each listed row separately) without describing anything new.
	if n := len(t.dirtyRows); n > 0 && t.dirtyRows[n-1] == idx {
		t.dirtyRowsState = dirtyRowsExact
		return
	}
	// Bound 2: once the list is no shorter than the table, LogTransaction
	// writes the whole table anyway (see its `len(updated) < len(Rows)`
	// guard), so continuing to extend it buys nothing and costs memory plus
	// a longer copy in every rollback snapshot. Give it up at exactly that
	// point; dirtyRowsUnknown is the documented safe direction.
	if len(t.dirtyRows)+1 >= len(t.Rows) {
		t.dirtyRows = nil
		t.dirtyRowsState = dirtyRowsUnknown
		return
	}
	t.dirtyRowsState = dirtyRowsExact
	t.dirtyRows = append(t.dirtyRows, idx)
}

// noteStructuralChange records that an existing row's content changed,
// moved, or was removed, or that the schema changed. Every caller that
// currently forces a full-table WAL entry (MarkDirtyFrom(-1)) or records an
// in-place row replacement (MarkRowUpdated) is exactly such a case, so both
// call this instead of requiring every one of their own call sites (DELETE,
// UPDATE, DDL, foreign-key cascades) to remember to report it separately.
// WAL replay and replication apply rows directly without going through
// either method, so those paths call this directly too — see
// applyOperation in wal_advanced.go and the update-rows delta branch in
// wal_manager.go.
func (t *Table) noteStructuralChange() {
	t.noteStructuralInvalidation()
}

func (t *Table) noteStructuralInvalidation() {
	t.structVersion++
	t.resetRowLog()
}

// resetRowLog forgets the whole change log: a consumer stamped with any version
// before the current one can no longer be told what happened and rebuilds.
func (t *Table) resetRowLog() {
	t.rowUpdateLog.reset()
	t.rowUpdateFolds = nil
	t.rowUpdateBase = t.structVersion
}

// maxRowUpdateDeltas bounds the retained per-row UPDATE history. It is sized so
// one touch batch of a few thousand rows (and several of them in a row) still
// fits: a derived structure that skips columns it does not index
// (UpdatedRowsSinceCols) needs the whole chain back to its own stamp to prove
// that nothing relevant happened. 65536 deltas are 1.5 MB per table at most.
// When the log is full its older half is folded into a column summary
// (rowUpdateFold) instead of being forgotten, so even a bookkeeping UPDATE of
// every row of a million-row table, or many of them between two searches, still
// proves that no indexed column changed. At most maxRowUpdateFolds summaries are
// kept (a few million updates in all); a stamp older than that rebuilds. A
// removal is never summarised: it is kept as a fold of its own (at most
// maxRowUpdateFoldEntries folds in all, and the position bound above), so a
// consumer that missed a DELETE and then a long run of bookkeeping UPDATEs still
// follows the DELETE.
const maxRowUpdateDeltas = 65536

const (
	rowUpdateFoldBlocks     = maxRowUpdateDeltas / rowUpdateChunk / 2
	maxRowUpdateFolds       = 64
	maxRowUpdateFoldEntries = 4096
)

// maxLegacyUpdateSpan is how many structural versions UpdatedRowsSince may span.
// The log once held 4096 deltas at most, so a caller older than that always got
// ok=false; its consumers (subscriptions) keep exactly that contract now that
// the log is longer.
const maxLegacyUpdateSpan = 4096

func (t *Table) noteRowUpdated(row int) { t.noteRowUpdatedCols(row, 0) }

func (t *Table) noteRowUpdatedCols(row int, cols uint64) {
	t.logRowEvent(rowUpdateDelta{row: row, cols: cols})
}

// logRowEvent advances structVersion by one and appends delta to the change log
// under that version.
func (t *Table) logRowEvent(delta rowUpdateDelta) {
	t.structVersion++
	delta.structVersion = t.structVersion
	// Keep the history bounded even for a long-lived in-memory database. A
	// cache older than the retained tail simply performs one safe rebuild.
	if t.rowUpdateLog.len() >= maxRowUpdateDeltas {
		t.foldRowUpdateLog()
	}
	t.rowUpdateLog.add(delta)
}

// MarkRowsRemoved records that a DELETE removed exactly the rows at positions,
// listed strictly ascending in the coordinates of the table as it was before the
// statement (the survivors then closed up, keeping their order). It is the
// reporting alternative to MarkDirtyFrom(-1): the table is marked for a full
// WAL record all the same, but the change log keeps the statement as one event,
// so a consumer that indexed the table earlier can ask ChangesSince which of its
// rows are gone and renumber the rest instead of rebuilding.
//
// Call it after the rows have been removed from t.Rows. The caller must name
// EVERY removed row; a path that cannot (DELETE without WHERE, swap-and-pop, a
// cascade, a trigger-observed delete, anything that reorders) calls
// MarkDirtyFrom(-1) instead, which drops the log. A call with no positions
// records nothing, because nothing was removed. A list that does not describe a
// plausible removal (unsorted, negative, beyond the former length) is treated as
// an unreportable change.
func (t *Table) MarkRowsRemoved(positions []int) {
	t.MarkDirtyKeepingRows()
	if len(positions) == 0 {
		return
	}
	last := -1
	for _, p := range positions {
		if p <= last || p > 1<<31-2 {
			t.noteStructuralInvalidation()
			return
		}
		last = p
	}
	if last >= len(t.Rows)+len(positions) || len(positions) > maxRowRemovedPositions || !t.shedRemovedPositions(len(positions)) {
		t.noteStructuralInvalidation()
		return
	}
	removal := &rowRemoval{positions: make([]int32, len(positions))}
	for i, p := range positions {
		removal.positions[i] = int32(p)
	}
	t.logRowEvent(rowUpdateDelta{row: -1, removed: removal})
}

// foldRowUpdateLog replaces the oldest half of the log by folds. Called with a
// full log, before the next delta is added. A run of consecutive UPDATEs becomes
// one column summary; a removal cannot be summarised and becomes a fold of its
// own that keeps its positions (the removal itself is shared, not copied). So a
// stamp older than a removal is still served after any number of UPDATEs, as
// long as none of the summaries between touches a column the reader depends on.
// The oldest folds are dropped (rowUpdateBase moves up, older stamps rebuild)
// when there are more than maxRowUpdateFolds summaries, more than
// maxRowUpdateFoldEntries folds, or the positions bound is exceeded.
func (t *Table) foldRowUpdateLog() {
	log := &t.rowUpdateLog
	n := min(rowUpdateFoldBlocks, len(log.sealed))
	if n == 0 {
		return
	}
	dropped := n * rowUpdateChunk
	first := log.at(0)
	var made []rowUpdateFold
	var run rowUpdateFold
	inRun := false
	for i := 0; i < dropped; i++ {
		delta := log.at(i)
		if delta.removed != nil {
			if inRun {
				made = append(made, run)
				inRun = false
			}
			made = append(made, rowUpdateFold{upTo: delta.structVersion, removed: delta.removed})
			continue
		}
		if !inRun {
			run, inRun = rowUpdateFold{}, true
		}
		if delta.cols == 0 {
			run.cols = ^uint64(0)
		} else {
			run.cols |= delta.cols
		}
		run.upTo = delta.structVersion
	}
	if inRun {
		made = append(made, run)
	}
	old := t.rowUpdateFolds
	switch last := len(old) - 1; {
	case last < 0:
		// The oldest stamp that can be served is the version just before the
		// first retained delta (rowUpdateBase itself for a log that grew from a
		// reset; later for a table loaded with a non-zero structVersion).
		t.rowUpdateBase = first.structVersion - 1
	case old[last].upTo != first.structVersion-1:
		// Not contiguous with the previous fold: forget it and everything
		// before it rather than bridge a gap.
		log.removedTotal -= foldRemovedPositions(old)
		old = nil
		t.rowUpdateBase = first.structVersion - 1
	}
	folds := make([]rowUpdateFold, 0, len(old)+len(made))
	folds = append(append(folds, old...), made...)
	summaries := 0
	for _, fold := range folds {
		if fold.removed == nil {
			summaries++
		}
	}
	drop := 0
	for drop < len(folds) && (len(folds)-drop > maxRowUpdateFoldEntries || summaries > maxRowUpdateFolds) {
		if folds[drop].removed == nil {
			summaries--
		} else {
			log.removedTotal -= len(folds[drop].removed.positions)
		}
		t.rowUpdateBase = folds[drop].upTo
		drop++
	}
	t.rowUpdateFolds = folds[drop:len(folds):len(folds)]
	log.dropBlocks(n)
}

// foldRemovedPositions counts the removed positions the removal folds hold.
func foldRemovedPositions(folds []rowUpdateFold) (n int) {
	for _, fold := range folds {
		if fold.removed != nil {
			n += len(fold.removed.positions)
		}
	}
	return n
}

// shedRemovedPositions makes room for a removal of need positions under
// maxRowRemovedPositions by forgetting the oldest history: the folds first, then
// the sealed blocks of the log, oldest first, each time moving rowUpdateBase up
// to the last forgotten version. A consumer stamped before that rebuilds; every
// other one keeps following. It reports false when forgetting everything that can
// be forgotten does not make room (the caller then invalidates the log).
func (t *Table) shedRemovedPositions(need int) bool {
	log := &t.rowUpdateLog
	for log.removedTotal+need > maxRowRemovedPositions && len(t.rowUpdateFolds) > 0 {
		fold := t.rowUpdateFolds[0]
		if fold.removed != nil {
			log.removedTotal -= len(fold.removed.positions)
		}
		t.rowUpdateBase = fold.upTo
		t.rowUpdateFolds = t.rowUpdateFolds[1:len(t.rowUpdateFolds):len(t.rowUpdateFolds)]
	}
	for log.removedTotal+need > maxRowRemovedPositions && len(log.sealed) > 0 {
		block := log.sealed[0]
		for _, delta := range block {
			if delta.removed != nil {
				log.removedTotal -= len(delta.removed.positions)
			}
		}
		t.rowUpdateBase = block[len(block)-1].structVersion
		t.rowUpdateFolds = nil // empty already: folds precede the log
		log.dropBlocks(1)
	}
	return log.removedTotal+need <= maxRowRemovedPositions
}

// rowUpdateRowsFrom is the oldest structVersion for which the log still names
// the changed rows; older stamps (down to rowUpdateBase) are covered by folds
// only.
func (t *Table) rowUpdateRowsFrom() int {
	if n := len(t.rowUpdateFolds); n > 0 {
		return t.rowUpdateFolds[n-1].upTo
	}
	return t.rowUpdateBase
}

// ColumnBit returns the ColumnMask bit of column idx. Columns 0..62 own one bit
// each; every column from 63 on shares bit 63, so a wide table stays safe (two
// such columns look like one, which only ever widens a set). A negative index
// is not a column and yields every bit.
func ColumnBit(idx int) uint64 {
	switch {
	case idx < 0:
		return ^uint64(0)
	case idx >= 63:
		return 1 << 63
	default:
		return 1 << uint(idx)
	}
}

// ColumnMask returns the union of ColumnBit over idxs. An empty list yields 0,
// which MarkRowUpdatedCols reads as "unknown column set".
func ColumnMask(idxs ...int) uint64 {
	var mask uint64
	for _, idx := range idxs {
		mask |= ColumnBit(idx)
	}
	return mask
}

// StructVersion returns the current structural-change counter. See
// noteStructuralChange's doc comment. Equality across two observations
// proves no row indexed at the earlier observation has since changed
// content or been removed — the table may only have grown by appending.
func (t *Table) StructVersion() int { return t.structVersion }

// UpdatedRowsSince returns the rows changed exclusively by UPDATEs since a
// prior structural version. ok is false when a DELETE, reorder, schema change,
// or an evicted delta history occurred, in which case index users must rebuild.
// It reports every changed row whatever column changed, and refuses a span of
// more than maxLegacyUpdateSpan versions.
func (t *Table) UpdatedRowsSince(version int) (rows []int, ok bool) {
	if version == t.structVersion {
		return nil, true
	}
	if t.structVersion-version > maxLegacyUpdateSpan {
		return nil, false
	}
	return t.updatedRowsSince(version, 0)
}

// UpdatedRowsSinceCols is UpdatedRowsSince for a structure that only depends on
// the columns in interest (a ColumnMask; zero means every column): rows whose
// recorded change set does not intersect interest are left out. A delta without
// a column set always counts. ok has the same meaning as for UpdatedRowsSince
// and the version chain is validated over ALL deltas, relevant or not, so a
// gap, an evicted history, a DELETE or a schema change still yields ok=false.
// ok with no rows means the structure is still exact and only needs its
// version stamps moved forward.
func (t *Table) UpdatedRowsSinceCols(version int, interest uint64) (rows []int, ok bool) {
	if version == t.structVersion {
		return nil, true
	}
	return t.updatedRowsSince(version, interest)
}

func (t *Table) updatedRowsSince(version int, interest uint64) (rows []int, ok bool) {
	if version < t.rowUpdateBase {
		return nil, false
	}
	// Deltas dropped into a fold can only vouch that nothing relevant changed,
	// and a removal shifts positions, which this reader cannot follow.
	if len(t.rowUpdateFolds) > 0 {
		for _, fold := range t.rowUpdateFolds {
			if fold.upTo > version && (fold.removed != nil || interest == 0 || fold.cols&interest != 0) {
				return nil, false
			}
		}
		version = max(version, t.rowUpdateRowsFrom())
	}
	log := &t.rowUpdateLog
	total := log.len()
	want := version + 1
	start := log.search(want)
	var seen map[int]struct{}
	for i := start; i < total; i++ {
		delta := log.at(i)
		if delta.structVersion < want {
			continue
		}
		if delta.structVersion != want || delta.removed != nil {
			// A removal shifts positions, which this reader (row twins, the
			// flat vector cache, HNSW, subscriptions) cannot follow.
			return nil, false
		}
		want++
		if interest != 0 && delta.cols != 0 && delta.cols&interest == 0 {
			continue
		}
		if seen == nil {
			seen = make(map[int]struct{})
		}
		if _, duplicate := seen[delta.row]; !duplicate {
			seen[delta.row] = struct{}{}
			rows = append(rows, delta.row)
		}
	}
	if want != t.structVersion+1 {
		return nil, false
	}
	return rows, true
}

// DirtyFrom returns the first dirty row index, or -1 if non-append-only.
func (t *Table) DirtyFrom() int { return t.dirtyFrom }

// DirtyRows returns the in-place updated row indices and whether that list is
// known to be complete. A false second result means the caller must treat the
// whole table as changed.
func (t *Table) DirtyRows() ([]int, bool) {
	if t.dirtyRowsState != dirtyRowsExact || len(t.dirtyRows) == 0 {
		return nil, false
	}
	return t.dirtyRows, true
}

// ResetDirty marks the table as clean (called after WAL checkpoint).
func (t *Table) ResetDirty() {
	t.dirtyFrom = len(t.Rows)
	t.dirtyRows = nil
	t.dirtyRowsState = dirtyRowsNone
}

// AddColumn appends a column to the table's schema and backfills every
// existing row with a NULL for it.
//
// Cols and the lower-cased name index ColIndex answers from are two
// representations of the same thing, and only this package can keep them in
// step — colPos is unexported and is built once, by NewTable. A caller that
// appends to Cols directly (which is what ALTER TABLE ADD COLUMN did) ends up
// with a column that is present in the schema and in every row but that no
// lookup can resolve.
//
// The duplicate check is case-insensitive, matching ColIndex: two columns
// whose names differ only in case would be indistinguishable to every lookup,
// and the second would shadow the first in colPos.
func (t *Table) AddColumn(col Column) error {
	key := strings.ToLower(col.Name)
	if key == "" {
		return fmt.Errorf("column name cannot be empty")
	}
	if t.colPos == nil {
		t.colPos = make(map[string]int, len(t.Cols)+1)
		for i, c := range t.Cols {
			t.colPos[strings.ToLower(c.Name)] = i
		}
	}
	if _, exists := t.colPos[key]; exists {
		return fmt.Errorf("column %q already exists on table %q", col.Name, t.Name)
	}
	// Everything above this line only reads, so a rejected AddColumn leaves
	// the table exactly as it found it. ALTER TABLE takes no rollback snapshot
	// unless a write-ahead log is attached, so being all-or-nothing by
	// construction is what makes it safe.
	t.Cols = append(t.Cols, col)
	t.colPos[key] = len(t.Cols) - 1
	for i, row := range t.Rows {
		// Clip the capacity so the widened row is always a new slice, even
		// when the stored one has spare room: a stored row may be shared with
		// a transaction shadow (see Rows), and appending into shared spare
		// capacity would write through to it.
		t.Rows[i] = append(row[:len(row):len(row)], nil)
	}
	// Every existing row's contents changed, so a derived structure that only
	// knows how to grow by appending rows must rebuild rather than assume it
	// is still current. See noteStructuralChange.
	t.noteStructuralChange()
	return nil
}

// RenameColumn changes one column name while preserving its physical position
// and every stored value. Secondary-index keys are positional, so their live
// entries remain valid; their column metadata and the lookup index are updated
// to make future writes and query planning use the new name.
func (t *Table) RenameColumn(from, to string) error {
	fromKey := strings.ToLower(from)
	toKey := strings.ToLower(to)
	if toKey == "" {
		return fmt.Errorf("column name cannot be empty")
	}
	if t.colPos == nil {
		t.colPos = make(map[string]int, len(t.Cols))
		for i, col := range t.Cols {
			t.colPos[strings.ToLower(col.Name)] = i
		}
	}
	columnIdx, exists := t.colPos[fromKey]
	if !exists {
		return fmt.Errorf("unknown column %q on table %q", from, t.Name)
	}
	if existing, exists := t.colPos[toKey]; exists && existing != columnIdx {
		return fmt.Errorf("column %q already exists on table %q", to, t.Name)
	}
	t.Cols[columnIdx].Name = to
	delete(t.colPos, fromKey)
	t.colPos[toKey] = columnIdx
	for _, index := range t.Indexes {
		for i, column := range index.Columns {
			if strings.EqualFold(column, from) {
				index.Columns[i] = to
			}
		}
	}
	if t.Stats != nil {
		if columnStats, ok := t.Stats.Columns[fromKey]; ok {
			delete(t.Stats.Columns, fromKey)
			t.Stats.Columns[toKey] = columnStats
		}
	}
	// Derived executor state may retain column names, unlike the positional
	// secondary-index entries above.
	t.noteStructuralChange()
	return nil
}

// ColIndex returns the zero-based index of the named column.
func (t *Table) ColIndex(name string) (int, error) {
	i, ok := t.colPos[strings.ToLower(name)]
	if !ok {
		return -1, fmt.Errorf("unknown column %q on table %q", name, t.Name)
	}
	return i, nil
}
