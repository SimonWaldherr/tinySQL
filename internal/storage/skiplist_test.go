package storage

import (
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"testing"
)

// intKey encodes an int the same way the real secondary-index code does,
// via the package's canonical order-preserving encoding, so ascending-key
// iteration in these tests means the same thing it means in production.
func intKey(n int) []byte {
	return CanonicalIndexKey([]any{n})
}

func TestSkipListInsertThenLookupRoundTrip(t *testing.T) {
	s := NewSkipList()
	s.Insert(intKey(5), 100)
	rowIDs, found := s.Get(intKey(5))
	if !found || len(rowIDs) != 1 || rowIDs[0] != 100 {
		t.Fatalf("Get(5) = %v, %v, want [100], true", rowIDs, found)
	}
	if _, found := s.Get(intKey(6)); found {
		t.Fatalf("Get(6) found = true, want false (key never inserted)")
	}
	if got := s.Len(); got != 1 {
		t.Fatalf("Len() = %d, want 1", got)
	}
}

func TestSkipListInsertAppendsRowIDsSortedAndDeduped(t *testing.T) {
	s := NewSkipList()
	s.Insert(intKey(1), 10)
	s.Insert(intKey(1), 5)
	s.Insert(intKey(1), 20)
	s.Insert(intKey(1), 5) // duplicate: must be a no-op, not a second copy

	rowIDs, found := s.Get(intKey(1))
	if !found {
		t.Fatalf("Get(1) found = false")
	}
	want := []int{5, 10, 20}
	if !equalInts(rowIDs, want) {
		t.Fatalf("RowIDs = %v, want %v (sorted, deduped)", rowIDs, want)
	}
}

func TestSkipListAscendingIterationAfterRandomOrderInserts(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	const n = 5000
	keys := rng.Perm(n) // random permutation of 0..n-1, inserted in that order

	s := NewSkipList()
	for _, k := range keys {
		s.Insert(intKey(k), k)
	}
	if got := s.Len(); got != n {
		t.Fatalf("Len() = %d, want %d", got, n)
	}

	entries := s.All()
	if len(entries) != n {
		t.Fatalf("All() returned %d entries, want %d", len(entries), n)
	}
	for i := 1; i < len(entries); i++ {
		if bytes.Compare(entries[i-1].Key, entries[i].Key) >= 0 {
			t.Fatalf("All() not strictly ascending at index %d: %v then %v", i, entries[i-1].Key, entries[i].Key)
		}
	}
	// Every key 0..n-1 must appear exactly once, in order, with itself as
	// the sole row ID (that's what was inserted).
	for i, entry := range entries {
		if len(entry.RowIDs) != 1 || entry.RowIDs[0] != i {
			t.Fatalf("entries[%d].RowIDs = %v, want [%d]", i, entry.RowIDs, i)
		}
	}
}

func TestSkipListRemoveFromMultiValueEntry(t *testing.T) {
	s := NewSkipList()
	s.Insert(intKey(1), 10)
	s.Insert(intKey(1), 20)
	s.Insert(intKey(1), 30)

	if removed := s.Remove(intKey(1), 20); !removed {
		t.Fatalf("Remove(1, 20) = false, want true")
	}
	rowIDs, found := s.Get(intKey(1))
	if !found {
		t.Fatalf("Get(1) found = false after partial removal")
	}
	if !equalInts(rowIDs, []int{10, 30}) {
		t.Fatalf("RowIDs after removal = %v, want [10 30]", rowIDs)
	}
}

func TestSkipListRemoveLastRowIDDropsEntry(t *testing.T) {
	s := NewSkipList()
	s.Insert(intKey(1), 10)

	if removed := s.Remove(intKey(1), 10); !removed {
		t.Fatalf("Remove(1, 10) = false, want true")
	}
	if _, found := s.Get(intKey(1)); found {
		t.Fatalf("Get(1) found = true after removing its only RowID, want the entry gone")
	}
	if got := s.Len(); got != 0 {
		t.Fatalf("Len() = %d, want 0 after entry removal", got)
	}
}

func TestSkipListRemoveNonexistentIsSafeNoOp(t *testing.T) {
	s := NewSkipList()
	s.Insert(intKey(1), 10)

	if removed := s.Remove(intKey(99), 1); removed {
		t.Fatalf("Remove of nonexistent key reported removed=true")
	}
	if removed := s.Remove(intKey(1), 999); removed {
		t.Fatalf("Remove of nonexistent rowID under an existing key reported removed=true")
	}
	// State must be untouched.
	rowIDs, found := s.Get(intKey(1))
	if !found || !equalInts(rowIDs, []int{10}) {
		t.Fatalf("Get(1) = %v, %v after no-op removals, want [10], true", rowIDs, found)
	}
	if got := s.Len(); got != 1 {
		t.Fatalf("Len() = %d, want 1", got)
	}
}

func TestSkipListRangeEqualityPrefixAndBetween(t *testing.T) {
	s := NewSkipList()
	for i := 0; i < 100; i += 2 { // even numbers 0..98
		s.Insert(intKey(i), i)
	}

	// Equality: seek to a key, stop after the first non-matching key.
	var eqHits []int
	s.Range(intKey(50), func(key []byte, rowIDs []int) bool {
		if !bytes.Equal(key, intKey(50)) {
			return false
		}
		eqHits = append(eqHits, rowIDs...)
		return false // exactly one match expected; stop regardless
	})
	if !equalInts(eqHits, []int{50}) {
		t.Fatalf("equality Range(50) = %v, want [50]", eqHits)
	}

	// "Prefix": walk from the start and collect while a predicate holds
	// (here: key value < 10), the same shape LookupSecondaryIndexPrefix
	// uses over the slice (seek, then walk while HasPrefix holds).
	var below10 []int
	s.Range(nil, func(key []byte, rowIDs []int) bool {
		if bytes.Compare(key, intKey(10)) >= 0 {
			return false
		}
		below10 = append(below10, rowIDs...)
		return true
	})
	sort.Ints(below10)
	if !equalInts(below10, []int{0, 2, 4, 6, 8}) {
		t.Fatalf("prefix-shaped Range(<10) = %v, want [0 2 4 6 8]", below10)
	}

	// BETWEEN 20 and 40 inclusive: seek to the lower bound, walk while <=
	// the upper bound, matching LookupSecondaryIndexRange's shape.
	var between []int
	s.Range(intKey(20), func(key []byte, rowIDs []int) bool {
		if bytes.Compare(key, intKey(40)) > 0 {
			return false
		}
		between = append(between, rowIDs...)
		return true
	})
	sort.Ints(between)
	if !equalInts(between, []int{20, 22, 24, 26, 28, 30, 32, 34, 36, 38, 40}) {
		t.Fatalf("BETWEEN Range(20,40) = %v", between)
	}
}

func TestSkipListRangeStartingBeforeFirstAndAfterLast(t *testing.T) {
	s := NewSkipList()
	s.Insert(intKey(10), 1)
	s.Insert(intKey(20), 2)

	var all []int
	s.Range(intKey(0), func(key []byte, rowIDs []int) bool {
		all = append(all, rowIDs...)
		return true
	})
	if !equalInts(all, []int{1, 2}) {
		t.Fatalf("Range before first key = %v, want [1 2]", all)
	}

	var none []int
	s.Range(intKey(1000), func(key []byte, rowIDs []int) bool {
		none = append(none, rowIDs...)
		return true
	})
	if len(none) != 0 {
		t.Fatalf("Range after last key = %v, want empty", none)
	}
}

func TestSkipListEmptyListIsWellBehaved(t *testing.T) {
	s := NewSkipList()
	if got := s.Len(); got != 0 {
		t.Fatalf("Len() = %d, want 0", got)
	}
	if _, found := s.Get(intKey(1)); found {
		t.Fatalf("Get on empty list found = true")
	}
	if removed := s.Remove(intKey(1), 1); removed {
		t.Fatalf("Remove on empty list removed = true")
	}
	if entries := s.All(); len(entries) != 0 {
		t.Fatalf("All() on empty list = %v, want empty", entries)
	}
	visited := false
	s.Range(nil, func(key []byte, rowIDs []int) bool {
		visited = true
		return true
	})
	if visited {
		t.Fatalf("Range on empty list invoked fn, want zero calls")
	}
}

// TestSkipListStressAgainstReferenceMap performs thousands of randomly
// ordered inserts and deletes, checkpointing the skip list's full state
// against a plain map[string][]int (sorted manually at each checkpoint) as
// ground truth. Any divergence between the two, under any sequence of
// operations, is a skip-list bug.
func TestSkipListStressAgainstReferenceMap(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	s := NewSkipList()
	ref := make(map[string][]int)

	const ops = 20000
	const keySpace = 800 // small key space forces heavy collisions/removals/re-inserts
	const checkpointEvery = 137

	refInsert := func(k string, rowID int) {
		rowIDs := ref[k]
		pos := sort.SearchInts(rowIDs, rowID)
		if pos < len(rowIDs) && rowIDs[pos] == rowID {
			return
		}
		rowIDs = append(rowIDs, 0)
		copy(rowIDs[pos+1:], rowIDs[pos:])
		rowIDs[pos] = rowID
		ref[k] = rowIDs
	}
	refRemove := func(k string, rowID int) {
		rowIDs, ok := ref[k]
		if !ok {
			return
		}
		pos := sort.SearchInts(rowIDs, rowID)
		if pos == len(rowIDs) || rowIDs[pos] != rowID {
			return
		}
		rowIDs = append(rowIDs[:pos], rowIDs[pos+1:]...)
		if len(rowIDs) == 0 {
			delete(ref, k)
		} else {
			ref[k] = rowIDs
		}
	}

	checkAgainstReference := func(step int) {
		entries := s.All()
		if len(entries) != len(ref) {
			t.Fatalf("step %d: skip list has %d keys, reference has %d", step, len(entries), len(ref))
		}
		// entries must be strictly ascending by key.
		for i := 1; i < len(entries); i++ {
			if bytes.Compare(entries[i-1].Key, entries[i].Key) >= 0 {
				t.Fatalf("step %d: All() not strictly ascending at %d", step, i)
			}
		}
		for _, entry := range entries {
			want, ok := ref[string(entry.Key)]
			if !ok {
				t.Fatalf("step %d: skip list has key %v that reference does not", step, entry.Key)
			}
			if !equalInts(entry.RowIDs, want) {
				t.Fatalf("step %d: key %v RowIDs = %v, want %v", step, entry.Key, entry.RowIDs, want)
			}
		}
		if s.Len() != len(ref) {
			t.Fatalf("step %d: Len() = %d, want %d", step, s.Len(), len(ref))
		}
	}

	for i := 0; i < ops; i++ {
		k := rng.Intn(keySpace)
		key := intKey(k)
		rowID := rng.Intn(keySpace)

		if rng.Intn(3) == 0 {
			s.Remove(key, rowID)
			refRemove(string(key), rowID)
		} else {
			s.Insert(key, rowID)
			refInsert(string(key), rowID)
		}

		if i%checkpointEvery == 0 {
			checkAgainstReference(i)
		}
	}
	checkAgainstReference(ops)

	// Drain everything and confirm both end up empty together.
	for k := 0; k < keySpace; k++ {
		key := intKey(k)
		if rowIDs, found := s.Get(key); found {
			for _, rowID := range append([]int(nil), rowIDs...) {
				s.Remove(key, rowID)
			}
		}
	}
	if got := s.Len(); got != 0 {
		t.Fatalf("Len() after draining everything = %d, want 0", got)
	}
	if entries := s.All(); len(entries) != 0 {
		t.Fatalf("All() after draining everything = %v, want empty", entries)
	}
}

func equalInts(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestSkipListManyKeysSameRowIDNoOpDuplicateInsert(t *testing.T) {
	// A unique index that already validated no duplicate exists will still
	// call Insert with the row's key; make sure re-inserting the exact same
	// (key, rowID) pair twice (e.g. a retried statement) never produces two
	// copies of the row ID under one key.
	s := NewSkipList()
	key := intKey(7)
	for i := 0; i < 5; i++ {
		s.Insert(key, 42)
	}
	rowIDs, found := s.Get(key)
	if !found || !equalInts(rowIDs, []int{42}) {
		t.Fatalf("RowIDs after repeated identical inserts = %v, want [42]", rowIDs)
	}
}

func TestSkipListStringKeysOrderMatchesBytesCompare(t *testing.T) {
	// Sanity check against a non-numeric key type: text-encoded keys must
	// iterate in whatever order bytes.Compare gives their *encoded* form --
	// which, per secondary_index_range.go's documented caveat, is NOT
	// lexicographic string order. Text components are framed as tag +
	// 4-byte big-endian length + payload, so shorter strings always sort
	// before longer ones regardless of content ("fig" before "apple"), and
	// only same-length strings compare by payload bytes. The skip list must
	// reproduce this exactly, since it is the same ordering the persisted
	// []IndexEntry slice already relies on.
	words := []string{"banana", "apple", "cherry", "date", "apple", "fig", "banana"}
	s := NewSkipList()
	for i, w := range words {
		s.Insert(CanonicalIndexKey([]any{w}), i)
	}
	entries := s.All()
	var gotKeys [][]byte
	for _, e := range entries {
		gotKeys = append(gotKeys, e.Key)
	}
	// Ascending by encoded-key byte order: length first ("fig"=3, "date"=4,
	// "apple"=5, then the two length-6 words "banana"/"cherry" ordered by
	// payload, b < c).
	wantOrder := []string{"fig", "date", "apple", "banana", "cherry"}
	if len(entries) != len(wantOrder) {
		t.Fatalf("All() has %d entries, want %d", len(entries), len(wantOrder))
	}
	for i, w := range wantOrder {
		if !bytes.Equal(gotKeys[i], CanonicalIndexKey([]any{w})) {
			t.Fatalf("entries[%d] key = %v, want encoding of %q", i, gotKeys[i], w)
		}
	}
	// "apple" was inserted at rows 1 and 4; "banana" at rows 0 and 6.
	appleRowIDs, _ := s.Get(CanonicalIndexKey([]any{"apple"}))
	if !equalInts(appleRowIDs, []int{1, 4}) {
		t.Fatalf("apple RowIDs = %v, want [1 4]", appleRowIDs)
	}
	bananaRowIDs, _ := s.Get(CanonicalIndexKey([]any{"banana"}))
	if !equalInts(bananaRowIDs, []int{0, 6}) {
		t.Fatalf("banana RowIDs = %v, want [0 6]", bananaRowIDs)
	}
}

func TestSkipListLevelNeverExceedsMax(t *testing.T) {
	// Not a correctness property visible from the public API in the
	// ordinary sense, but a basic guard against a level-growth bug turning
	// into an out-of-bounds forward-slice index under heavy insert volume.
	s := NewSkipList()
	for i := 0; i < 50000; i++ {
		s.Insert(intKey(i), i)
	}
	if s.level < 1 || s.level > skipListMaxLevel {
		t.Fatalf("level = %d, want in [1, %d]", s.level, skipListMaxLevel)
	}
}

func ExampleSkipList_usage() {
	s := NewSkipList()
	s.Insert(intKey(3), 30)
	s.Insert(intKey(1), 10)
	s.Insert(intKey(2), 20)
	for _, e := range s.All() {
		fmt.Println(e.RowIDs)
	}
	// Output:
	// [10]
	// [20]
	// [30]
}
