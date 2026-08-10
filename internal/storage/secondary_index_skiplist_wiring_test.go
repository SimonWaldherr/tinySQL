package storage

// Stage 2 correctness tests: wiring the skip list (skiplist.go) into
// SecondaryIndex as its live insert/lookup/delete/range-scan backing
// structure while keeping the exported, GOB-persisted Entries field
// byte-for-byte compatible with the format used before this stage.

import (
	"bytes"
	"encoding/gob"
	"reflect"
	"sort"
	"testing"
)

// TestSecondaryIndexGobSkipsUnexportedFastField confirms the assumption this
// whole stage depends on: gob only encodes SecondaryIndex's exported fields,
// so a populated runtime-only fast field never reaches the wire, and a
// decoded copy always comes back with fast == nil (ready to be lazily
// rehydrated from Entries) regardless of what the encoder's copy held.
func TestSecondaryIndexGobSkipsUnexportedFastField(t *testing.T) {
	src := &SecondaryIndex{Name: "idx_gob", Columns: []string{"a"}, Unique: true}
	// Touch it through the public insert path so fast gets populated, then
	// materialize so Entries reflects that live state -- exactly the shape a
	// real checkpoint would encode.
	src.hydrate().Insert([]byte("k1"), 1)
	src.hydrate().Insert([]byte("k2"), 2)
	src.hydrate().Insert([]byte("k2"), 3)
	if src.fast == nil {
		t.Fatal("expected fast to be populated before encode")
	}
	src.materialize()

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(src); err != nil {
		t.Fatalf("encode: %v", err)
	}

	var decoded SecondaryIndex
	if err := gob.NewDecoder(&buf).Decode(&decoded); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if decoded.fast != nil {
		t.Fatal("decoded SecondaryIndex.fast should be nil -- gob must not have encoded/decoded it")
	}
	if decoded.Name != src.Name || decoded.Unique != src.Unique || !reflect.DeepEqual(decoded.Columns, src.Columns) {
		t.Fatalf("decoded metadata mismatch: got Name=%q Columns=%#v Unique=%v, want Name=%q Columns=%#v Unique=%v",
			decoded.Name, decoded.Columns, decoded.Unique, src.Name, src.Columns, src.Unique)
	}
	if !reflect.DeepEqual(decoded.Entries, src.Entries) {
		t.Fatalf("decoded Entries mismatch: got %#v, want %#v", decoded.Entries, src.Entries)
	}
	if len(decoded.Entries) != 2 {
		t.Fatalf("decoded Entries len = %d, want 2", len(decoded.Entries))
	}

	// The decoded copy must still work correctly purely from Entries: the
	// first read/write should lazily hydrate fast from it.
	rowIDs, found := decoded.hydrate().Get([]byte("k2"))
	if !found || !reflect.DeepEqual(rowIDs, []int{2, 3}) {
		t.Fatalf("decoded lookup for k2 = %v, %v; want [2 3], true", rowIDs, found)
	}
}

// TestSecondaryIndexMaterializeMatchesLegacySliceAlgorithm builds the same
// sequence of insert/update/delete operations two ways: through the public
// Table API (which now drives the skip list under the hood) and through
// legacySliceInsertSecondaryIndexRowID (skiplist_benchmark_test.go's
// verbatim copy of the pre-stage-2 O(n) sorted-slice algorithm). After
// materializing the skip-list-backed index, its Entries must be identical
// -- same keys, same RowIDs, same order -- to what the old algorithm would
// have produced directly in Entries.
func TestSecondaryIndexMaterializeMatchesLegacySliceAlgorithm(t *testing.T) {
	type op struct {
		key   string
		rowID int
		kind  string // "insert" or "remove"
	}
	ops := []op{
		{"b", 1, "insert"},
		{"a", 2, "insert"},
		{"c", 3, "insert"},
		{"a", 4, "insert"},
		{"b", 1, "remove"}, // drop b's only RowID -- whole entry should disappear
		{"c", 5, "insert"},
		{"a", 2, "remove"},
		{"d", 6, "insert"},
	}

	fresh := &SecondaryIndex{Name: "idx_cmp", Columns: []string{"k"}}
	legacy := &SecondaryIndex{Name: "idx_cmp", Columns: []string{"k"}}

	for _, o := range ops {
		switch o.kind {
		case "insert":
			fresh.hydrate().Insert([]byte(o.key), o.rowID)
			legacySliceInsertSecondaryIndexRowID(legacy, []byte(o.key), o.rowID)
		case "remove":
			fresh.hydrate().Remove([]byte(o.key), o.rowID)
			legacyRemoveSecondaryIndexRowID(legacy, []byte(o.key), o.rowID)
		}
	}
	fresh.materialize()

	if !reflect.DeepEqual(fresh.Entries, legacy.Entries) {
		t.Fatalf("materialized Entries mismatch:\n  skip-list-backed: %#v\n  legacy slice:      %#v", fresh.Entries, legacy.Entries)
	}
}

// legacyRemoveSecondaryIndexRowID reproduces, verbatim, the pre-stage-2
// removeSecondaryIndexRowID algorithm. Kept test-only (like
// legacySliceInsertSecondaryIndexRowID) purely as a fixed comparison
// baseline; production code no longer has any code path shaped like this.
func legacyRemoveSecondaryIndexRowID(index *SecondaryIndex, key []byte, rowID int) {
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

// TestSecondaryIndexHydratesPreexistingEntriesWithoutFast simulates loading
// an index from a database file saved before this stage existed: only
// Entries is populated (as a real gob.Decode would leave it), fast is left
// at its zero value. Every read path must lazily bootstrap correctly from
// that shape alone.
func TestSecondaryIndexHydratesPreexistingEntriesWithoutFast(t *testing.T) {
	table := NewTable("legacy", []Column{{Name: "id", Type: IntType}, {Name: "kind", Type: TextType}}, false)
	table.Rows = [][]any{{1, "a"}, {2, "b"}, {3, "a"}, {4, "c"}}

	idx := &SecondaryIndex{Name: "idx_kind", Columns: []string{"kind"}}
	// Populate Entries the way a pre-stage-2 RebuildSecondaryIndexes (or a
	// gob.Decode of an old file) would -- directly, with fast left nil.
	// CanonicalIndexKey encodes by value order alone (matching idx.Columns'
	// order), the same encoding a real lookup's canonicalIndexKeyInto(values)
	// produces -- unlike table.indexKey, which additionally maps column
	// names to positions in a *full table row*, so it is not usable here
	// with a single-column values slice.
	keyFor := func(kind string) []byte {
		return CanonicalIndexKey([]any{kind})
	}
	idx.Entries = []IndexEntry{
		{Key: keyFor("a"), RowIDs: []int{0, 2}},
		{Key: keyFor("b"), RowIDs: []int{1}},
		{Key: keyFor("c"), RowIDs: []int{3}},
	}
	sort.Slice(idx.Entries, func(i, j int) bool { return bytes.Compare(idx.Entries[i].Key, idx.Entries[j].Key) < 0 })
	table.Indexes = map[string]*SecondaryIndex{"idx_kind": idx}

	if idx.fast != nil {
		t.Fatal("fast must start nil for this test to exercise lazy hydration")
	}

	rows, err := table.LookupSecondaryIndexPoint(idx, []any{"a"})
	if err != nil || !reflect.DeepEqual(rows, []int{0, 2}) {
		t.Fatalf("point lookup for a = %v, %v; want [0 2], nil", rows, err)
	}
	rows, err = table.LookupSecondaryIndexPrefix(idx, []any{"c"})
	if err != nil || !reflect.DeepEqual(rows, []int{3}) {
		t.Fatalf("prefix lookup for c = %v, %v; want [3], nil", rows, err)
	}

	// A write against the hydrated index must behave exactly like it would
	// against a freshly built one.
	table.Rows = append(table.Rows, []any{5, "b"})
	if err := table.InsertSecondaryIndexRow(4, table.Rows[4], table.SortedIndexNames()); err != nil {
		t.Fatalf("insert: %v", err)
	}
	rows, err = table.LookupSecondaryIndexPoint(idx, []any{"b"})
	if err != nil || !reflect.DeepEqual(rows, []int{1, 4}) {
		t.Fatalf("point lookup for b after insert = %v, %v; want [1 4], nil", rows, err)
	}
}

// TestSkipListCloneIsIndependent confirms Clone produces a structurally
// separate copy: same content immediately after cloning, but later mutating
// either list leaves the other one completely unaffected. cloneSecondaryIndexes
// (used by every MVCC/rollback snapshot) depends on this for correctness.
func TestSkipListCloneIsIndependent(t *testing.T) {
	original := NewSkipList()
	for i := 0; i < 500; i++ {
		original.Insert(intKey(i), i)
	}
	clone := original.Clone()

	if clone.Len() != original.Len() {
		t.Fatalf("clone.Len() = %d, want %d", clone.Len(), original.Len())
	}
	for i := 0; i < 500; i++ {
		rowIDs, found := clone.Get(intKey(i))
		if !found || !reflect.DeepEqual(rowIDs, []int{i}) {
			t.Fatalf("clone.Get(%d) = %v, %v; want [%d], true", i, rowIDs, found, i)
		}
	}

	// Mutate the original after cloning: the clone must not see it.
	original.Insert(intKey(9000), 9000)
	original.Remove(intKey(0), 0)
	if _, found := clone.Get(intKey(9000)); found {
		t.Fatal("clone observed an insert made to the original after Clone")
	}
	if _, found := clone.Get(intKey(0)); !found {
		t.Fatal("clone lost an entry the original removed after Clone")
	}

	// Mutate the clone: the original must not see it.
	clone.Insert(intKey(9001), 9001)
	if _, found := original.Get(intKey(9001)); found {
		t.Fatal("original observed an insert made to the clone")
	}
}

// TestSecondaryIndexCloneUsesSkipListWhenHydrated confirms
// SecondaryIndex.clone (cloneSecondaryIndexes' per-index helper, backing
// every MVCC/rollback snapshot) produces a live, independent copy without
// forcing a materialize, and that mutating the clone's table never leaks
// into the source index.
func TestSecondaryIndexCloneUsesSkipListWhenHydrated(t *testing.T) {
	idx := &SecondaryIndex{Name: "idx", Columns: []string{"k"}}
	idx.hydrate().Insert([]byte("x"), 1)
	idx.hydrate().Insert([]byte("y"), 2)

	before := idx.Entries // stale/nil -- never materialized
	clone := idx.clone()
	if clone.fast == nil {
		t.Fatal("clone should have cloned the live skip list, not fallen back to Entries")
	}
	if !reflect.DeepEqual(idx.Entries, before) {
		t.Fatal("clone must not mutate the source index's Entries")
	}

	rowIDs, found := clone.fast.Get([]byte("x"))
	if !found || !reflect.DeepEqual(rowIDs, []int{1}) {
		t.Fatalf("cloned lookup for x = %v, %v; want [1], true", rowIDs, found)
	}

	// Mutate the clone; the source must be unaffected.
	clone.fast.Insert([]byte("z"), 3)
	if _, found := idx.fast.Get([]byte("z")); found {
		t.Fatal("mutating the clone leaked into the source index's skip list")
	}
}

// TestSecondaryIndexConcurrentFirstReadersHydrateSafely exercises the one
// genuinely new concurrency hazard the skip list introduces: DB.Get and
// read-only query execution take only DB's RWMutex read lock (see db.go),
// so multiple SELECTs against the same never-yet-hydrated index can call
// hydrate concurrently. idx.mu must make that safe. Run with -race to be
// meaningful.
func TestSecondaryIndexConcurrentFirstReadersHydrateSafely(t *testing.T) {
	idx := &SecondaryIndex{
		Name:    "idx_concurrent",
		Columns: []string{"k"},
		Entries: []IndexEntry{
			{Key: []byte("a"), RowIDs: []int{1, 2}},
			{Key: []byte("b"), RowIDs: []int{3}},
		},
	}

	const goroutines = 32
	done := make(chan []int, goroutines)
	for g := 0; g < goroutines; g++ {
		go func() {
			rowIDs, _ := idx.hydrate().Get([]byte("a"))
			done <- append([]int(nil), rowIDs...)
		}()
	}
	for g := 0; g < goroutines; g++ {
		rowIDs := <-done
		if !reflect.DeepEqual(rowIDs, []int{1, 2}) {
			t.Fatalf("concurrent hydrate produced %v, want [1 2]", rowIDs)
		}
	}
}
