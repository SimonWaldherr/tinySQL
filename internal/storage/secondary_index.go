package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"sort"
	"strings"
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
type SecondaryIndex struct {
	Name    string
	Columns []string
	Unique  bool
	Entries []IndexEntry
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
	t.Indexes[key] = &SecondaryIndex{Name: name, Columns: append([]string(nil), columns...), Unique: unique}
	if err := t.RebuildSecondaryIndexes(); err != nil {
		delete(t.Indexes, key)
		return err
	}
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
		key, err := t.indexKey(idx.Columns, row)
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
func (t *Table) RebuildSecondaryIndexes() error {
	for _, idx := range t.Indexes {
		entries := make(map[string]*IndexEntry)
		for rowID, row := range t.Rows {
			key, err := t.indexKey(idx.Columns, row)
			if err != nil {
				return fmt.Errorf("index %q row %d: %w", idx.Name, rowID, err)
			}
			mapKey := string(key)
			entry := entries[mapKey]
			if entry == nil {
				entry = &IndexEntry{Key: append([]byte(nil), key...)}
				entries[mapKey] = entry
			}
			entry.RowIDs = append(entry.RowIDs, rowID)
			if idx.Unique && len(entry.RowIDs) > 1 {
				return fmt.Errorf("unique index %q: duplicate key", idx.Name)
			}
		}
		idx.Entries = make([]IndexEntry, 0, len(entries))
		for _, entry := range entries {
			idx.Entries = append(idx.Entries, *entry)
		}
		sort.Slice(idx.Entries, func(i, j int) bool {
			return bytes.Compare(idx.Entries[i].Key, idx.Entries[j].Key) < 0
		})
	}
	return nil
}

// InsertSecondaryIndexRow adds one appended table row to every materialized
// secondary index. It avoids rebuilding unaffected keys after every INSERT.
// Call it only after the row has been appended to t.Rows and constraints have
// been checked.
func (t *Table) InsertSecondaryIndexRow(rowID int, row []any) error {
	updates, err := t.indexRowKeys(row)
	if err != nil {
		return err
	}
	for _, update := range updates {
		insertSecondaryIndexRowID(update.index, update.key, rowID)
	}
	return nil
}

// UpdateSecondaryIndexRow moves one stable row position between composite
// keys. Row positions do not change during UPDATE, so this is O(indexes ·
// log(keys)) instead of rescanning the table.
func (t *Table) UpdateSecondaryIndexRow(rowID int, before, after []any) error {
	beforeKeys, err := t.indexRowKeys(before)
	if err != nil {
		return err
	}
	afterKeys, err := t.indexRowKeys(after)
	if err != nil {
		return err
	}
	for i, before := range beforeKeys {
		after := afterKeys[i]
		if bytes.Equal(before.key, after.key) {
			continue
		}
		removeSecondaryIndexRowID(before.index, before.key, rowID)
		insertSecondaryIndexRowID(after.index, after.key, rowID)
	}
	return nil
}

// ReindexSecondaryIndexRows applies the old-to-new row-position mapping made
// by DELETE. Keys stay sorted and are not recomputed; only RowIDs belonging
// to removed rows disappear. This is deliberately named "reindex" rather
// than "rebuild": it preserves the materialized key structures.
func (t *Table) ReindexSecondaryIndexRows(oldToNew map[int]int) {
	for _, index := range t.Indexes {
		entries := make([]IndexEntry, 0, len(index.Entries))
		for _, entry := range index.Entries {
			rowIDs := entry.RowIDs[:0]
			for _, oldID := range entry.RowIDs {
				if newID, ok := oldToNew[oldID]; ok {
					rowIDs = append(rowIDs, newID)
				}
			}
			if len(rowIDs) == 0 {
				continue
			}
			entry.RowIDs = rowIDs
			entries = append(entries, entry)
		}
		index.Entries = entries
	}
}

// ClearSecondaryIndexes removes all RowIDs while retaining CREATE INDEX
// metadata, as required after DELETE without a WHERE clause.
func (t *Table) ClearSecondaryIndexes() {
	for _, index := range t.Indexes {
		index.Entries = nil
	}
}

type secondaryIndexRowKey struct {
	index *SecondaryIndex
	key   []byte
}

func (t *Table) indexRowKeys(row []any) ([]secondaryIndexRowKey, error) {
	updates := make([]secondaryIndexRowKey, 0, len(t.Indexes))
	names := make([]string, 0, len(t.Indexes))
	for name := range t.Indexes {
		names = append(names, name)
	}
	sort.Strings(names)
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

func insertSecondaryIndexRowID(index *SecondaryIndex, key []byte, rowID int) {
	pos := sort.Search(len(index.Entries), func(i int) bool {
		return bytes.Compare(index.Entries[i].Key, key) >= 0
	})
	if pos == len(index.Entries) || !bytes.Equal(index.Entries[pos].Key, key) {
		index.Entries = append(index.Entries, IndexEntry{})
		copy(index.Entries[pos+1:], index.Entries[pos:])
		index.Entries[pos] = IndexEntry{Key: append([]byte(nil), key...)}
	}
	rowIDs := index.Entries[pos].RowIDs
	rowPos := sort.SearchInts(rowIDs, rowID)
	if rowPos < len(rowIDs) && rowIDs[rowPos] == rowID {
		return
	}
	rowIDs = append(rowIDs, 0)
	copy(rowIDs[rowPos+1:], rowIDs[rowPos:])
	rowIDs[rowPos] = rowID
	index.Entries[pos].RowIDs = rowIDs
}

func removeSecondaryIndexRowID(index *SecondaryIndex, key []byte, rowID int) {
	pos := sort.Search(len(index.Entries), func(i int) bool {
		return bytes.Compare(index.Entries[i].Key, key) >= 0
	})
	if pos == len(index.Entries) || !bytes.Equal(index.Entries[pos].Key, key) {
		return
	}
	rowIDs := index.Entries[pos].RowIDs
	rowPos := sort.SearchInts(rowIDs, rowID)
	if rowPos == len(rowIDs) || rowIDs[rowPos] != rowID {
		return
	}
	rowIDs = append(rowIDs[:rowPos], rowIDs[rowPos+1:]...)
	if len(rowIDs) > 0 {
		index.Entries[pos].RowIDs = rowIDs
		return
	}
	copy(index.Entries[pos:], index.Entries[pos+1:])
	index.Entries = index.Entries[:len(index.Entries)-1]
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

// LookupSecondaryIndexPrefix performs a binary search followed by a compact
// prefix walk. Returned row IDs are sorted in table order to preserve the
// observable order of a scan when a query has no ORDER BY clause.
func (t *Table) LookupSecondaryIndexPrefix(idx *SecondaryIndex, values []any) ([]int, error) {
	if idx == nil || len(values) == 0 || len(values) > len(idx.Columns) {
		return nil, nil
	}
	var scratch [128]byte
	key := canonicalIndexKeyInto(scratch[:0], values)
	start := sort.Search(len(idx.Entries), func(i int) bool {
		return bytes.Compare(idx.Entries[i].Key, key) >= 0
	})
	var out []int
	for i := start; i < len(idx.Entries) && bytes.HasPrefix(idx.Entries[i].Key, key); i++ {
		out = append(out, idx.Entries[i].RowIDs...)
	}
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
	pos := sort.Search(len(idx.Entries), func(i int) bool {
		return bytes.Compare(idx.Entries[i].Key, key) >= 0
	})
	if pos < len(idx.Entries) && bytes.Equal(idx.Entries[pos].Key, key) {
		return idx.Entries[pos].RowIDs
	}
	return nil
}

func (t *Table) indexKey(columns []string, row []any) ([]byte, error) {
	if len(row) < len(columns) {
		return nil, fmt.Errorf("row has %d values for %d index columns", len(row), len(columns))
	}
	key := make([]byte, 0, len(columns)*12)
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

func cloneSecondaryIndexes(in map[string]*SecondaryIndex) map[string]*SecondaryIndex {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]*SecondaryIndex, len(in))
	for key, idx := range in {
		if idx == nil {
			continue
		}
		copyIdx := &SecondaryIndex{Name: idx.Name, Columns: append([]string(nil), idx.Columns...), Unique: idx.Unique, Entries: make([]IndexEntry, len(idx.Entries))}
		for i, entry := range idx.Entries {
			copyIdx.Entries[i] = IndexEntry{Key: append([]byte(nil), entry.Key...), RowIDs: append([]int(nil), entry.RowIDs...)}
		}
		out[key] = copyIdx
	}
	return out
}
