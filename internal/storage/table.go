// Table: the rows of one table plus the schema and statistics describing them,
// and the dirty tracking that lets the write-ahead log write a delta instead of
// a whole table.
package storage

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

// Table stores rows along with column metadata and indexes.
type Table struct {
	Name string
	Cols []Column
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
	// ftsGeneration changes when a lazy FTS build changes persisted metadata;
	// ftsPersistedGeneration records the generation most recently flushed.
	// This is deliberately separate from Version: a SELECT may build an index
	// and must become durable without pretending the table's SQL data changed.
	ftsGeneration          int
	ftsPersistedGeneration int
	IsTemp                 bool
	colPos                 map[string]int
	Version                int
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
	NumDocs       int
	TermIDs       map[string]int32
	DocTermIDs    []int32
	DocTermCounts []int32
	DocTokenIDs   []int32
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

// MarkFTSIndexesChanged records a persist-relevant FTS metadata change. The
// caller must hold DerivedLock, matching direct access to FTSIndexes.
func (t *Table) MarkFTSIndexesChanged() { t.ftsGeneration++ }

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

// snapshotFTSIndexes takes a backend-safe deep copy. FTS indexes may be built
// by concurrent SELECTs, so persistence cannot read their maps and arenas
// without the per-table derived lock.
func (t *Table) snapshotFTSIndexes() map[string]*FTSIndex {
	t.DerivedLock()
	indexes := cloneFTSIndexes(t.FTSIndexes)
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

// NewTable creates a new Table with case-insensitive column lookup indices.
func NewTable(name string, cols []Column, isTemp bool) *Table {
	pos := make(map[string]int, len(cols))
	for i, c := range cols {
		pos[strings.ToLower(c.Name)] = i
	}
	return &Table{Name: name, Cols: cols, colPos: pos, IsTemp: isTemp, dirtyFrom: -1, Indexes: make(map[string]*SecondaryIndex), FTSIndexes: make(map[string]*FTSIndex)}
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
		t.noteStructuralChange()
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
	t.dirtyFrom = -1
	t.noteStructuralChange()
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
	t.structVersion++
}

// StructVersion returns the current structural-change counter. See
// noteStructuralChange's doc comment. Equality across two observations
// proves no row indexed at the earlier observation has since changed
// content or been removed — the table may only have grown by appending.
func (t *Table) StructVersion() int { return t.structVersion }

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
	for i := range t.Rows {
		t.Rows[i] = append(t.Rows[i], nil)
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
