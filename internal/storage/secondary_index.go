package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
)

// IndexEntry is one canonical composite key and the table row positions that
// match it. Entries are sorted by Key, making equality and prefix seeks a
// binary-search operation rather than a table scan.
type IndexEntry struct {
	Key    []byte
	RowIDs []int
}

// SecondaryIndex is a materialized, persistent secondary index. It is kept
// alongside table rows so GOB snapshots, disk and hybrid backends preserve the
// index itself, not merely CREATE INDEX catalog metadata.
//
// Entries is the GOB/JSON wire format and nothing else: it is the byte-
// identical, backward-compatible on-disk shape older saved databases already
// use. fast is the live, runtime-only backing structure -- a skip list (see
// skiplist.go) that gives every insert/lookup/delete/range-scan O(log n)
// expected-case cost instead of Entries' O(n) sorted-slice insert. Being
// unexported, fast is automatically skipped by gob, so this costs zero wire
// format changes.
//
// Entries is therefore no longer kept in sync on every mutation -- doing so
// would defeat the whole point of adding fast. It is only ever refreshed by
// materialize, called immediately before this index crosses a persistence
// boundary (GOB/JSON encode, or the paged-index backend's B+Tree writer).
// Between one materialize and the next, Entries can be arbitrarily stale;
// nothing except a persistence boundary is allowed to read it directly.
//
// mu guards exactly two operations, hydrate and materialize, which are the
// only things introduced by fast that mutate a SecondaryIndex from what used
// to be a read-only path: DB.Get and ordinary query execution take only
// DB.mu's read lock (see db.go), so multiple SELECTs -- and multiple
// concurrent checkpoints -- against the same index can run genuinely in
// parallel. Every other operation (Insert/Remove/Get/Range on an
// already-hydrated fast) takes no lock at all, matching this package's
// existing convention that table mutations are already serialized by the
// caller holding DB's write lock for the whole statement.
type SecondaryIndex struct {
	Name    string
	Columns []string
	Unique  bool
	Entries []IndexEntry

	mu   sync.Mutex
	fast *SkipList
}

// hydrate lazily builds fast from Entries the first time this index is
// touched (read or written) since being loaded, freshly rebuilt, or
// constructed. Every insert/lookup/delete/range-scan entry point calls this
// first instead of requiring every construction/load call site to remember
// to populate fast explicitly -- a nil check here is more robust against a
// missed call site than mandatory explicit hydration everywhere a
// *SecondaryIndex might come from.
func (idx *SecondaryIndex) hydrate() *SkipList {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	if idx.fast == nil {
		fast := NewSkipList()
		for _, entry := range idx.Entries {
			for _, rowID := range entry.RowIDs {
				fast.Insert(entry.Key, rowID)
			}
		}
		idx.fast = fast
	}
	return idx.fast
}

// materialize rebuilds Entries from the live skip list in ascending key
// order -- a single O(n) traversal via SkipList.All. It must be called only
// at a persistence boundary, immediately before this index is GOB- or
// JSON-encoded or before its Entries feed the paged-index backend's B+Tree
// writer -- never on every mutation, or it reintroduces the O(n)-per-insert
// cost this package exists to remove.
//
// A nil fast means nothing has mutated (or even read) this index since it
// was last loaded or rebuilt, so Entries is already exact and this is a
// cheap no-op.
func (idx *SecondaryIndex) materialize() {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	if idx.fast != nil {
		idx.Entries = idx.fast.All()
	}
}

// clone returns a deep, independent copy of idx for an MVCC/rollback
// snapshot: a structural clone of the live skip list when one exists (cheap
// relative to a full materialize, and exactly the O(n) cost cloning this
// index already had before fast existed), or a deep copy of Entries when
// nothing has touched this index since it was loaded. It deliberately never
// touches Entries' live value or forces a skip-list walk into Entries, so
// calling this from the per-statement rollback-snapshot hot path cannot
// reintroduce the O(n)-materialize-per-mutation cost fast exists to avoid.
func (idx *SecondaryIndex) clone() *SecondaryIndex {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	out := &SecondaryIndex{Name: idx.Name, Columns: append([]string(nil), idx.Columns...), Unique: idx.Unique}
	if idx.fast != nil {
		out.fast = idx.fast.Clone()
		return out
	}
	out.Entries = make([]IndexEntry, len(idx.Entries))
	for i, entry := range idx.Entries {
		out.Entries[i] = IndexEntry{Key: append([]byte(nil), entry.Key...), RowIDs: append([]int(nil), entry.RowIDs...)}
	}
	return out
}

// Len reports the number of distinct composite keys this index currently
// holds, hydrating from Entries first if nothing has touched it yet since it
// was loaded. It exists for introspection and tests that want the
// materialized key count without reaching into Entries directly -- which,
// unlike before, is not kept in sync on every mutation and so cannot be
// trusted outside a persistence boundary.
func (idx *SecondaryIndex) Len() int {
	if idx == nil {
		return 0
	}
	return idx.hydrate().Len()
}

// CreateSecondaryIndex builds an index over both existing and future table
// rows. Names are case-insensitive, matching SQL identifiers.
func (t *Table) CreateSecondaryIndex(name string, columns []string, unique bool) error {
	if len(columns) == 0 {
		return fmt.Errorf("index %q has no columns", name)
	}
	if t.Indexes == nil {
		t.Indexes = make(map[string]*SecondaryIndex)
	}
	key := strings.ToLower(name)
	if _, exists := t.Indexes[key]; exists {
		return fmt.Errorf("index %q already exists", name)
	}
	for _, col := range columns {
		if _, err := t.ColIndex(col); err != nil {
			return err
		}
	}
	idx := &SecondaryIndex{Name: name, Columns: append([]string(nil), columns...), Unique: unique}
	fast, err := t.buildSecondaryIndex(idx)
	if err != nil {
		return err
	}
	idx.fast = fast
	// Publish only the successfully built index; existing indexes are untouched.
	t.Indexes[key] = idx
	return nil
}

// DropSecondaryIndex removes the materialized structure from the table.
func (t *Table) DropSecondaryIndex(name string) bool {
	if t.Indexes == nil {
		return false
	}
	key := strings.ToLower(name)
	if _, ok := t.Indexes[key]; !ok {
		return false
	}
	delete(t.Indexes, key)
	return true
}

// CheckSecondaryIndexConstraints rejects a duplicate before a new row is
// appended. skipRow is used by UPDATE to ignore a row's current key.
func (t *Table) CheckSecondaryIndexConstraints(row []any, skipRow int) error {
	for _, idx := range t.Indexes {
		if !idx.Unique {
			continue
		}
		var scratch [128]byte
		key, err := t.indexKeyInto(scratch[:0], idx.Columns, row)
		if err != nil {
			return fmt.Errorf("index %q: %w", idx.Name, err)
		}
		for _, existing := range idx.lookup(key) {
			if existing != skipRow {
				return fmt.Errorf("unique index %q: duplicate key", idx.Name)
			}
		}
	}
	return nil
}

// RebuildSecondaryIndexes rebuilds every materialized index from table rows.
// It is called after DML, during recovery and before persistence boundaries so
// index/table versions cannot diverge across snapshots or WAL replay.
//
// The rebuild targets fast directly via bulk insert rather than going through
// the old build-a-map-then-sort-into-Entries path: it produces an equivalent
// end state (same keys, same RowIDs, same uniqueness enforcement) while
// leaving Entries to be derived from fast by materialize only when this
// index is actually persisted.
func (t *Table) RebuildSecondaryIndexes() error {
	for _, idx := range t.Indexes {
		fast, err := t.buildSecondaryIndex(idx)
		if err != nil {
			return err
		}
		idx.mu.Lock()
		idx.fast = fast
		idx.Entries = nil
		idx.mu.Unlock()
	}
	return nil
}

// InsertSecondaryIndexRow adds one appended table row to every materialized
// secondary index. It avoids rebuilding unaffected keys after every INSERT.
// Call it only after the row has been appended to t.Rows and constraints have
// been checked.
//
// names is the table's index names in sorted order (see SortedIndexNames).
// The caller computes it once per statement, since the index set never
// changes mid-statement, instead of paying a rebuild-from-map-plus-sort on
// every single row of a multi-row INSERT.
func (t *Table) InsertSecondaryIndexRow(rowID int, row []any, names []string) error {
	for _, name := range names {
		index := t.Indexes[name]
		var scratch [128]byte
		key, err := t.indexKeyInto(scratch[:0], index.Columns, row)
		if err != nil {
			return fmt.Errorf("index %q: %w", index.Name, err)
		}
		index.hydrate().Insert(key, rowID)
	}
	return nil
}

// UpdateSecondaryIndexRow moves one stable row position between composite
// keys. Row positions do not change during UPDATE, so this is O(indexes ·
// log(keys)) instead of rescanning the table.
//
// names is the table's index names in sorted order (see SortedIndexNames),
// computed once per statement by the caller. Without it this rebuilt and
// sorted the same name list from t.Indexes twice per row -- once for the
// before key, once for the after key -- even though the index set is
// identical for both and does not change mid-statement.
func (t *Table) UpdateSecondaryIndexRow(rowID int, before, after []any, names []string) error {
	for _, name := range names {
		index := t.Indexes[name]
		var beforeScratch, afterScratch [128]byte
		beforeKey, err := t.indexKeyInto(beforeScratch[:0], index.Columns, before)
		if err != nil {
			return fmt.Errorf("index %q: %w", index.Name, err)
		}
		afterKey, err := t.indexKeyInto(afterScratch[:0], index.Columns, after)
		if err != nil {
			return fmt.Errorf("index %q: %w", index.Name, err)
		}
		if bytes.Equal(beforeKey, afterKey) {
			continue
		}
		fast := index.hydrate()
		fast.Remove(beforeKey, rowID)
		fast.Insert(afterKey, rowID)
	}
	return nil
}

// ReindexSecondaryIndexRows applies the old-to-new row-position mapping made
// by DELETE. Keys stay the same and are not recomputed; only RowIDs belonging
// to removed rows disappear, and surviving RowIDs are renumbered. This is
// deliberately named "reindex" rather than "rebuild": it preserves the
// materialized key structures instead of recomputing keys from row data.
//
// Every key is visited regardless of backing structure (there is no reverse
// rowID-to-key index to consult instead), so this rebuilds fast wholesale via
// one ascending walk of the old list -- the same O(total entries + total
// RowIDs) cost ReindexSecondaryIndexRows already had before fast existed.
func (t *Table) ReindexSecondaryIndexRows(oldToNew map[int]int) {
	for _, index := range t.Indexes {
		old := index.hydrate().All()
		rebuilt := NewSkipList()
		for _, entry := range old {
			for _, oldID := range entry.RowIDs {
				if newID, ok := oldToNew[oldID]; ok {
					rebuilt.Insert(entry.Key, newID)
				}
			}
		}
		index.fast = rebuilt
	}
}

// DeleteSecondaryIndexRow removes one stable row position and shifts every
// later RowID down by one. Point DELETE uses this to avoid allocating an
// old-to-new map proportional to the whole table.
//
// Like ReindexSecondaryIndexRows, every key must be visited, so this rebuilds
// fast wholesale via one ascending walk rather than trying to patch it in
// place.
func (t *Table) DeleteSecondaryIndexRow(rowID int) {
	for _, index := range t.Indexes {
		old := index.hydrate().All()
		rebuilt := NewSkipList()
		for _, entry := range old {
			for _, oldID := range entry.RowIDs {
				switch {
				case oldID < rowID:
					rebuilt.Insert(entry.Key, oldID)
				case oldID > rowID:
					rebuilt.Insert(entry.Key, oldID-1)
				}
			}
		}
		index.fast = rebuilt
	}
}

// SwapRemoveSecondaryIndexRow updates every materialized secondary index
// after a point DELETE removes deleteRowID via swap-and-pop: the caller
// moves its current last row (lastRowID, with values lastRow) into
// deleteRowID's slot and truncates, instead of shifting every subsequent
// row down by one. Only two row positions are affected, so this patches
// exactly those entries instead of rescanning the whole index the way
// ReindexSecondaryIndexRows (bulk shift) or DeleteSecondaryIndexRow (single
// shift-compact) do.
//
// deletedRow is the row being removed (its values before the swap).
// lastRow is the row that swap-and-pop relocates from lastRowID to
// deleteRowID; when deleteRowID == lastRowID the delete removed the table's
// last row and nothing moves. names is the table's index names in sorted
// order (see SortedIndexNames), computed once per statement by the caller.
func (t *Table) SwapRemoveSecondaryIndexRow(deleteRowID int, deletedRow []any, lastRowID int, lastRow []any, names []string) error {
	delUpdates, err := t.indexRowKeys(deletedRow, names)
	if err != nil {
		return err
	}
	if deleteRowID == lastRowID {
		for _, u := range delUpdates {
			u.index.hydrate().Remove(u.key, deleteRowID)
		}
		return nil
	}
	lastUpdates, err := t.indexRowKeys(lastRow, names)
	if err != nil {
		return err
	}
	for i, u := range delUpdates {
		u.index.hydrate().Remove(u.key, deleteRowID)
		lu := lastUpdates[i]
		lu.index.hydrate().Remove(lu.key, lastRowID)
		lu.index.hydrate().Insert(lu.key, deleteRowID)
	}
	return nil
}

// ClearSecondaryIndexes removes all RowIDs while retaining CREATE INDEX
// metadata, as required after DELETE without a WHERE clause.
func (t *Table) ClearSecondaryIndexes() {
	for _, index := range t.Indexes {
		index.fast = NewSkipList()
		index.Entries = nil
	}
}

type secondaryIndexRowKey struct {
	index *SecondaryIndex
	key   []byte
}

// SortedIndexNames returns this table's secondary index names (the same
// lower-cased keys used by t.Indexes) in sorted order, for callers that just
// want the list (e.g. tests). engine's INSERT/UPDATE hot paths deliberately
// do not call this: sort.Strings inside the function body keeps the
// compiler from inlining it, which forces the returned slice to escape to
// heap on every call. They instead inline the equivalent of this function
// (see rawIndexNames in internal/engine) and call sort.Strings themselves at
// the call site, which the compiler can prove non-escaping and keep on the
// stack -- one heap allocation saved per statement instead of per row.
func (t *Table) SortedIndexNames() []string {
	names := make([]string, 0, len(t.Indexes))
	for name := range t.Indexes {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (t *Table) indexRowKeys(row []any, names []string) ([]secondaryIndexRowKey, error) {
	updates := make([]secondaryIndexRowKey, 0, len(names))
	for _, name := range names {
		index := t.Indexes[name]
		key, err := t.indexKey(index.Columns, row)
		if err != nil {
			return nil, fmt.Errorf("index %q: %w", index.Name, err)
		}
		updates = append(updates, secondaryIndexRowKey{index: index, key: key})
	}
	return updates, nil
}

// FindSecondaryIndex returns an index whose leading columns exactly match the
// requested equality predicates. The caller may provide a prefix shorter than
// the full composite index, enabling prefix seeks.
func (t *Table) FindSecondaryIndex(columns []string) *SecondaryIndex {
	if len(columns) == 0 {
		return nil
	}
	for _, idx := range t.Indexes {
		if len(idx.Columns) < len(columns) {
			continue
		}
		match := true
		for i, col := range columns {
			if !strings.EqualFold(idx.Columns[i], col) {
				match = false
				break
			}
		}
		if match {
			return idx
		}
	}
	return nil
}

// LookupSecondaryIndexPrefix performs a skip-list seek to the first key >=
// the prefix, followed by a compact prefix walk. Returned row IDs are sorted
// in table order to preserve the observable order of a scan when a query has
// no ORDER BY clause.
func (t *Table) LookupSecondaryIndexPrefix(idx *SecondaryIndex, values []any) ([]int, error) {
	if idx == nil || len(values) == 0 || len(values) > len(idx.Columns) {
		return nil, nil
	}
	var scratch [128]byte
	key := canonicalIndexKeyInto(scratch[:0], values)
	var out []int
	idx.hydrate().Range(key, func(entryKey []byte, rowIDs []int) bool {
		if !bytes.HasPrefix(entryKey, key) {
			return false
		}
		out = append(out, rowIDs...)
		return true
	})
	sort.Ints(out)
	return out, nil
}

// LookupSecondaryIndexPoint returns the immutable RowID run for a complete
// composite key. Unlike a prefix seek it neither merges nor sorts entries:
// RebuildSecondaryIndexes appends RowIDs in table order, so the entry is
// already in the observable order of a table scan. The returned slice aliases
// the index and is read-only; it remains valid until the table is mutated.
//
// This is intentionally a separate API from LookupSecondaryIndexPrefix. A
// general caller may need an owned prefix result, while the engine's locked
// read path can safely avoid an allocation on every point lookup.
func (t *Table) LookupSecondaryIndexPoint(idx *SecondaryIndex, values []any) ([]int, error) {
	if idx == nil || len(values) != len(idx.Columns) {
		return nil, nil
	}
	// Numeric/geocell/category point keys comfortably fit in this stack buffer.
	// append grows to a private heap slice only for genuinely large text/BLOB
	// components, preserving correctness without charging normal seeks an alloc.
	var scratch [128]byte
	return idx.lookup(canonicalIndexKeyInto(scratch[:0], values)), nil
}

func (idx *SecondaryIndex) lookup(key []byte) []int {
	rowIDs, _ := idx.hydrate().Get(key)
	return rowIDs
}

func (t *Table) indexKey(columns []string, row []any) ([]byte, error) {
	return t.indexKeyInto(nil, columns, row)
}

func (t *Table) indexKeyInto(key []byte, columns []string, row []any) ([]byte, error) {
	if len(row) < len(columns) {
		return nil, fmt.Errorf("row has %d values for %d index columns", len(row), len(columns))
	}
	for _, column := range columns {
		pos, err := t.ColIndex(column)
		if err != nil {
			return nil, err
		}
		if pos >= len(row) {
			return nil, fmt.Errorf("row lacks indexed column %q", column)
		}
		key = appendCanonicalIndexValue(key, row[pos])
	}
	return key, nil
}

func canonicalIndexKeyInto(key []byte, values []any) []byte {
	for _, value := range values {
		key = appendCanonicalIndexValue(key, value)
	}
	return key
}

// CanonicalIndexKey returns the durable, type-tagged composite encoding used
// by materialized secondary indexes. Paged storage uses exactly the same
// encoding so a SQL-bound key and an on-disk B+Tree key compare identically.
func CanonicalIndexKey(values []any) []byte {
	return canonicalIndexKeyInto(nil, values)
}

// AppendCanonicalIndexValue appends one value using the durable index-key
// encoding. Typed hot paths can provide stack-backed scratch space and avoid
// the growing allocations incurred by CanonicalIndexKey's general []any API.
func AppendCanonicalIndexValue(dst []byte, value any) []byte {
	return appendCanonicalIndexValue(dst, value)
}

// CanonicalIndexValueEqual reports whether two individual values have the
// same durable index encoding. The hot primitive forms avoid constructing
// temporary []any and []byte values, while the fallback deliberately uses the
// canonical encoder so newly supported value types retain identical behavior.
func CanonicalIndexValueEqual(left, right any) bool {
	switch value := left.(type) {
	case nil:
		return right == nil
	case bool:
		other, ok := right.(bool)
		return ok && value == other
	case int:
		switch other := right.(type) {
		case int:
			return value == other
		case int64:
			return int64(value) == other
		default:
			return false
		}
	case int64:
		switch other := right.(type) {
		case int:
			return value == int64(other)
		case int64:
			return value == other
		default:
			return false
		}
	case float64:
		other, ok := right.(float64)
		return ok && math.Float64bits(value) == math.Float64bits(other)
	case string:
		other, ok := right.(string)
		return ok && value == other
	case []byte:
		other, ok := right.([]byte)
		return ok && bytes.Equal(value, other)
	default:
		return bytes.Equal(CanonicalIndexKey([]any{left}), CanonicalIndexKey([]any{right}))
	}
}

// appendCanonicalIndexValue produces a type-tagged, length-framed encoding.
// It distinguishes NULL, empty BLOB, non-empty BLOB and text, while keeping a
// complete leading component usable as a byte prefix for composite seeks.
func appendCanonicalIndexValue(dst []byte, value any) []byte {
	var payload []byte
	switch v := value.(type) {
	case nil:
		return append(dst, 0x00)
	case bool:
		if v {
			return append(dst, 0x01, 1)
		}
		return append(dst, 0x01, 0)
	case int:
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(int64(v))^(1<<63))
		return appendIndexPayload(dst, 0x02, buf[:])
	case int64:
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(v)^(1<<63))
		return appendIndexPayload(dst, 0x02, buf[:])
	case float64:
		bits := math.Float64bits(v)
		if bits&(1<<63) != 0 {
			bits = ^bits
		} else {
			bits ^= 1 << 63
		}
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], bits)
		return appendIndexPayload(dst, 0x03, buf[:])
	case string:
		return appendIndexPayload(dst, 0x04, []byte(v))
	case []byte:
		return appendIndexPayload(dst, 0x05, v)
	default:
		payload = []byte(fmt.Sprintf("%T:%v", value, value))
		return appendIndexPayload(dst, 0x7f, payload)
	}
}

func appendIndexPayload(dst []byte, tag byte, payload []byte) []byte {
	dst = append(dst, tag)
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(payload)))
	dst = append(dst, lenBuf[:]...)
	return append(dst, payload...)
}

// cloneSecondaryIndexes deep-copies a table's indexes for an MVCC/rollback
// snapshot (see cloneTable in snapshot.go) or for the decode side of a
// GOB/JSON round trip (see diskToTable in disk_table.go). Each index's own
// clone (see SecondaryIndex.clone) prefers cloning the live skip list when
// one exists, so this never forces the O(n) Entries rebuild materialize
// performs -- calling that unconditionally here would hit every mutating
// statement that takes a rollback snapshot, reintroducing exactly the
// per-mutation O(n) cost the skip list exists to remove.
func cloneSecondaryIndexes(in map[string]*SecondaryIndex) map[string]*SecondaryIndex {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]*SecondaryIndex, len(in))
	for key, idx := range in {
		if idx == nil {
			continue
		}
		out[key] = idx.clone()
	}
	return out
}

// materializeSecondaryIndexesForEncode returns a fresh map[string]*SecondaryIndex
// snapshot suitable for crossing a persistence boundary (GOB/JSON encode, or
// the paged-index backend's B+Tree writer): every entry's Entries reflects
// the live skip-list state as of right now, via materialize's O(n) ascending
// walk. Unlike cloneSecondaryIndexes, this is meant to be called only
// immediately before encoding -- never on every mutation.
func materializeSecondaryIndexesForEncode(in map[string]*SecondaryIndex) map[string]*SecondaryIndex {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]*SecondaryIndex, len(in))
	for key, idx := range in {
		if idx == nil {
			continue
		}
		idx.materialize()
		entries := make([]IndexEntry, len(idx.Entries))
		for i, entry := range idx.Entries {
			entries[i] = IndexEntry{Key: append([]byte(nil), entry.Key...), RowIDs: append([]int(nil), entry.RowIDs...)}
		}
		out[key] = &SecondaryIndex{Name: idx.Name, Columns: append([]string(nil), idx.Columns...), Unique: idx.Unique, Entries: entries}
	}
	return out
}

// Resolve column positions once for a bulk build rather than once per row.
func (t *Table) buildSecondaryIndex(idx *SecondaryIndex) (*SkipList, error) {
	positions := make([]int, len(idx.Columns))
	for i, column := range idx.Columns {
		pos, err := t.ColIndex(column)
		if err != nil {
			return nil, fmt.Errorf("index %q: %w", idx.Name, err)
		}
		positions[i] = pos
	}
	fast := NewSkipList()
	for rowID, row := range t.Rows {
		if len(row) < len(positions) {
			return nil, fmt.Errorf("index %q row %d: row has %d values for %d index columns", idx.Name, rowID, len(row), len(positions))
		}
		var scratch [128]byte
		key := scratch[:0]
		for i, pos := range positions {
			if pos >= len(row) {
				return nil, fmt.Errorf("index %q row %d: row lacks indexed column %q", idx.Name, rowID, idx.Columns[i])
			}
			key = appendCanonicalIndexValue(key, row[pos])
		}
		if idx.Unique {
			if _, found := fast.Get(key); found {
				return nil, fmt.Errorf("unique index %q: duplicate key", idx.Name)
			}
		}
		fast.Insert(key, rowID)
	}
	return fast, nil
}
