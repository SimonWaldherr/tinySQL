package storage

import (
	"bytes"
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"testing"
)

// checkSkipListShape verifies the invariants every operation must keep: keys
// ascend strictly along every level, the chain of level i holds exactly the nodes
// taller than i, no run is empty or unsorted, length counts the keys and level is
// the number of levels in use.
func checkSkipListShape(t *testing.T, s *SkipList) {
	t.Helper()
	var level0 []*skipListNode
	for x := s.head.forward[0]; x != nil; x = x.forward[0] {
		if len(x.rowIDs) == 0 {
			t.Fatalf("empty run under key %x", x.key)
		}
		if !sort.IntsAreSorted(x.rowIDs) {
			t.Fatalf("run %v of key %x is not sorted", x.rowIDs, x.key)
		}
		for i := 1; i < len(x.rowIDs); i++ {
			if x.rowIDs[i] == x.rowIDs[i-1] {
				t.Fatalf("run %v of key %x repeats a position", x.rowIDs, x.key)
			}
		}
		if n := len(level0); n > 0 && bytes.Compare(level0[n-1].key, x.key) >= 0 {
			t.Fatalf("keys not ascending: %x then %x", level0[n-1].key, x.key)
		}
		level0 = append(level0, x)
	}
	if len(level0) != s.length {
		t.Fatalf("length = %d, want %d", s.length, len(level0))
	}
	top := 1
	for i := 0; i < skipListMaxLevel; i++ {
		var want []*skipListNode
		for _, x := range level0 {
			if len(x.forward) > i {
				want = append(want, x)
			}
		}
		if len(want) > 0 {
			top = i + 1
		}
		var got []*skipListNode
		for x := s.head.forward[i]; x != nil; x = x.forward[i] {
			got = append(got, x)
		}
		if len(got) != len(want) {
			t.Fatalf("level %d chain has %d nodes, want %d", i, len(got), len(want))
		}
		for k := range got {
			if got[k] != want[k] {
				t.Fatalf("level %d chain differs at %d", i, k)
			}
		}
	}
	if s.level != top {
		t.Fatalf("level = %d, want %d", s.level, top)
	}
}

// renumberModel applies a removal the obvious way: a map from key to the
// surviving, renumbered positions.
func renumberModel(model map[string][]int, removed []int) map[string][]int {
	gone := make(map[int]bool, len(removed))
	for _, p := range removed {
		gone[p] = true
	}
	out := make(map[string][]int, len(model))
	for key, ids := range model {
		var kept []int
		for _, id := range ids {
			if gone[id] {
				continue
			}
			n := sort.SearchInts(removed, id) // removed positions below id
			kept = append(kept, id-n)
		}
		if len(kept) > 0 {
			out[key] = kept
		}
	}
	return out
}

func skipListModelEntries(model map[string][]int) []IndexEntry {
	keys := make([]string, 0, len(model))
	for key := range model {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]IndexEntry, 0, len(keys))
	for _, key := range keys {
		out = append(out, IndexEntry{Key: []byte(key), RowIDs: append([]int(nil), model[key]...)})
	}
	return out
}

func randomRemoved(rng *rand.Rand, rows int) []int {
	switch rng.Intn(6) {
	case 0:
		return nil
	case 1: // everything
		out := make([]int, rows)
		for i := range out {
			out[i] = i
		}
		return out
	case 2: // an adjacent run
		if rows == 0 {
			return nil
		}
		from := rng.Intn(rows)
		to := from + rng.Intn(rows-from) + 1
		out := make([]int, 0, to-from)
		for i := from; i < to; i++ {
			out = append(out, i)
		}
		return out
	case 3: // the first or the last row
		if rows == 0 {
			return nil
		}
		return []int{[]int{0, rows - 1}[rng.Intn(2)]}
	default:
		var out []int
		p := rng.Float64()
		for i := 0; i < rows; i++ {
			if rng.Float64() < p {
				out = append(out, i)
			}
		}
		return out
	}
}

func TestSkipListRenumberAfterRemovalMatchesAModel(t *testing.T) {
	rng := rand.New(rand.NewSource(11))
	for round := 0; round < 400; round++ {
		rows := rng.Intn(120)
		keys := 1 + rng.Intn(40)
		s := NewSkipList()
		model := map[string][]int{}
		for row := 0; row < rows; row++ {
			// Several rows per key, and some keys with runs that straddle the
			// removed positions.
			key := string(intKey(rng.Intn(keys)))
			s.Insert([]byte(key), row)
			model[key] = append(model[key], row)
		}
		removed := randomRemoved(rng, rows)
		beforeMutations := s.mutations
		s.startJournal()
		s.renumberAfterRemoval(removed)
		model = renumberModel(model, removed)
		checkSkipListShape(t, s)
		want := skipListModelEntries(model)
		if got := s.All(); !(len(got) == 0 && len(want) == 0) && !reflect.DeepEqual(got, want) {
			t.Fatalf("round %d removed %v:\n got %v\nwant %v", round, removed, got, want)
		}
		for key, ids := range model {
			got, ok := s.Get([]byte(key))
			if !ok || !reflect.DeepEqual(got, ids) {
				t.Fatalf("round %d: Get(%x) = %v, %v, want %v", round, key, got, ok, ids)
			}
		}
		if len(removed) > 0 {
			if s.journaling {
				t.Fatalf("round %d: the journal survived a renumbering", round)
			}
			if s.mutations == beforeMutations {
				t.Fatalf("round %d: the mutation counter did not move", round)
			}
		} else if !s.journaling || s.mutations != beforeMutations {
			t.Fatalf("round %d: an empty removal changed the list", round)
		}
		// The list stays a working skip list afterwards.
		s.Insert(intKey(9999), 1<<20)
		checkSkipListShape(t, s)
	}
}

func TestSkipListRenumberAfterRemovalEdgeCases(t *testing.T) {
	build := func() *SkipList {
		s := NewSkipList()
		for row, key := range []int{5, 3, 5, 1, 3, 5} {
			s.Insert(intKey(key), row)
		}
		return s
	}
	t.Run("nothing removed", func(t *testing.T) {
		s := build()
		before := s.All()
		s.renumberAfterRemoval(nil)
		if !reflect.DeepEqual(before, s.All()) {
			t.Fatal("list changed")
		}
		checkSkipListShape(t, s)
	})
	t.Run("everything removed", func(t *testing.T) {
		s := build()
		s.renumberAfterRemoval([]int{0, 1, 2, 3, 4, 5})
		if s.Len() != 0 || len(s.All()) != 0 || s.level != 1 || s.head.forward[0] != nil {
			t.Fatalf("list not empty: len %d level %d", s.Len(), s.level)
		}
		checkSkipListShape(t, s)
		s.Insert(intKey(2), 0)
		checkSkipListShape(t, s)
	})
	t.Run("a whole key disappears", func(t *testing.T) {
		s := build() // key 1 -> {3}; key 3 -> {1,4}; key 5 -> {0,2,5}
		s.renumberAfterRemoval([]int{3})
		if _, ok := s.Get(intKey(1)); ok {
			t.Fatal("key 1 survived")
		}
		if got, _ := s.Get(intKey(5)); !reflect.DeepEqual(got, []int{0, 2, 4}) {
			t.Fatalf("key 5 = %v", got)
		}
		checkSkipListShape(t, s)
	})
	t.Run("positions beyond the list are harmless", func(t *testing.T) {
		s := build()
		s.renumberAfterRemoval([]int{100, 200})
		if got, _ := s.Get(intKey(5)); !reflect.DeepEqual(got, []int{0, 2, 5}) {
			t.Fatalf("key 5 = %v", got)
		}
		checkSkipListShape(t, s)
	})
}

func TestRenumberRowIDsAfterRemoval(t *testing.T) {
	for _, tc := range []struct {
		ids, removed, want []int
	}{
		{[]int{0, 1, 2}, []int{1}, []int{0, 1}},
		{[]int{5, 9, 40}, []int{0, 1, 2, 3, 4}, []int{0, 4, 35}},
		{[]int{5, 9, 40}, []int{5, 9, 40}, []int{}},
		{[]int{2, 3}, []int{50, 60}, []int{2, 3}},
		{[]int{0, 100000}, []int{1, 2, 3, 99999}, []int{0, 99996}},
		{nil, []int{1}, nil},
	} {
		ids := append([]int(nil), tc.ids...)
		got := renumberRowIDsAfterRemoval(ids, tc.removed)
		if len(got) != len(tc.want) || (len(got) > 0 && !reflect.DeepEqual(got, tc.want)) {
			t.Fatalf("renumber(%v, %v) = %v, want %v", tc.ids, tc.removed, got, tc.want)
		}
	}
}

// The Table method renumbers every index, refuses a list that is not a removal and
// leaves a skip list shared with another index untouched.
func TestRenumberSecondaryIndexRowsAfterRemoval(t *testing.T) {
	newTable := func() *Table {
		tbl := NewTable("t", []Column{{Name: "id", Type: IntType}, {Name: "k", Type: TextType}}, false)
		for i := 0; i < 8; i++ {
			tbl.Rows = append(tbl.Rows, []any{i, fmt.Sprintf("k%d", i%3)})
		}
		if err := tbl.CreateSecondaryIndex("by_k", []string{"k"}, false); err != nil {
			t.Fatal(err)
		}
		if err := tbl.CreateSecondaryIndex("by_id", []string{"id"}, true); err != nil {
			t.Fatal(err)
		}
		return tbl
	}
	compact := func(tbl *Table, removed []int) {
		gone := map[int]bool{}
		for _, p := range removed {
			gone[p] = true
		}
		var kept [][]any
		for i, r := range tbl.Rows {
			if !gone[i] {
				kept = append(kept, r)
			}
		}
		tbl.Rows = kept
	}
	sameAsRebuilt := func(t *testing.T, tbl *Table) {
		t.Helper()
		for name, idx := range tbl.Indexes {
			fast, err := tbl.buildSecondaryIndex(idx)
			if err != nil {
				t.Fatal(err)
			}
			if got, want := idx.hydrate().All(), fast.All(); !reflect.DeepEqual(got, want) {
				t.Fatalf("index %s:\n got %v\nwant %v", name, got, want)
			}
			checkSkipListShape(t, idx.hydrate())
		}
	}

	t.Run("renumbers every index", func(t *testing.T) {
		tbl := newTable()
		removed := []int{1, 2, 6}
		compact(tbl, removed)
		if !tbl.RenumberSecondaryIndexRowsAfterRemoval(removed) {
			t.Fatal("refused a valid list")
		}
		sameAsRebuilt(t, tbl)
	})
	t.Run("refuses a list that is not ascending", func(t *testing.T) {
		for _, bad := range [][]int{{3, 1}, {2, 2}, {-1, 4}} {
			tbl := newTable()
			before := tbl.Indexes["by_k"].hydrate().All()
			if tbl.RenumberSecondaryIndexRowsAfterRemoval(bad) {
				t.Fatalf("accepted %v", bad)
			}
			if !reflect.DeepEqual(before, tbl.Indexes["by_k"].hydrate().All()) {
				t.Fatalf("%v changed the index", bad)
			}
		}
	})
	t.Run("refuses a position that was never a row", func(t *testing.T) {
		// Each case compacts the 8-row table by its "gone" rows and then reports a
		// list whose last position is outside the old table (positions 0..7).
		for _, tc := range []struct{ gone, report []int }{
			{[]int{1, 2}, []int{1, 8}},
			{[]int{1}, []int{9}},
			{[]int{1, 2}, []int{0, 100}},
		} {
			tbl := newTable()
			compact(tbl, tc.gone)
			before := tbl.Indexes["by_k"].hydrate().All()
			if tbl.RenumberSecondaryIndexRowsAfterRemoval(tc.report) {
				t.Fatalf("accepted %v", tc.report)
			}
			if !reflect.DeepEqual(before, tbl.Indexes["by_k"].hydrate().All()) {
				t.Fatalf("%v changed the index", tc.report)
			}
		}
		// The last valid position is accepted.
		tbl := newTable()
		compact(tbl, []int{1, 7})
		if !tbl.RenumberSecondaryIndexRowsAfterRemoval([]int{1, 7}) {
			t.Fatal("refused the last row of the old table")
		}
		sameAsRebuilt(t, tbl)
	})
	t.Run("copies a shared list first", func(t *testing.T) {
		tbl := newTable()
		borrowed := borrowSecondaryIndexes(tbl.Indexes)
		before := borrowed["by_k"].hydrate().All()
		removed := []int{0, 4}
		compact(tbl, removed)
		if !tbl.RenumberSecondaryIndexRowsAfterRemoval(removed) {
			t.Fatal("refused a valid list")
		}
		sameAsRebuilt(t, tbl)
		if !reflect.DeepEqual(before, borrowed["by_k"].hydrate().All()) {
			t.Fatal("the borrowing index saw the renumbering")
		}
	})
	t.Run("a cold index is hydrated and renumbered", func(t *testing.T) {
		tbl := newTable()
		for _, idx := range tbl.Indexes {
			idx.materialize()
			idx.fast = nil
		}
		removed := []int{7}
		compact(tbl, removed)
		if !tbl.RenumberSecondaryIndexRowsAfterRemoval(removed) {
			t.Fatal("refused a valid list")
		}
		sameAsRebuilt(t, tbl)
	})
	t.Run("forgets a parked twin", func(t *testing.T) {
		tbl := newTable()
		idx := tbl.Indexes["by_k"]
		idx.twin = &skipListTwin{list: idx.fast.Clone(), of: idx.fast}
		idx.returned.Store(&returnedList{list: idx.fast.Clone(), base: idx.fast, mutations: idx.fast.mutations})
		removed := []int{0}
		compact(tbl, removed)
		tbl.RenumberSecondaryIndexRowsAfterRemoval(removed)
		if idx.twin != nil || idx.returned.Load() != nil {
			t.Fatal("a twin of the pre-delete list is still parked")
		}
	})
}

// A statement that renumbered the indexes in place and is then rolled back gets
// its rows and an index that agrees with them.
func TestRowsOnlySnapshotRestoresAnInPlaceRenumbering(t *testing.T) {
	db := NewDB()
	t.Cleanup(func() { _ = db.Close() })
	table := rowsOnlyTestTable(t, db)
	before := [][]any{}
	for _, r := range table.Rows {
		before = append(before, append([]any(nil), r...))
	}
	db.LockContentForWrite()
	snapshot, err := db.SnapshotForRowsOnlyStatement("default", "chunks")
	if err != nil {
		db.UnlockContentForWrite()
		t.Fatal(err)
	}
	table.Rows = [][]any{table.Rows[1], table.Rows[3]}
	if !table.RenumberSecondaryIndexRowsAfterRemoval([]int{0, 2}) {
		t.Fatal("refused a valid list")
	}
	table.Version++
	db.RestoreStatementSnapshot(snapshot)
	db.UnlockContentForWrite()
	if !reflect.DeepEqual(table.Rows, before) {
		t.Fatalf("rows after restore:\n got %v\nwant %v", table.Rows, before)
	}
	for src, want := range map[string][]int{"a": {0, 1}, "b": {2}, "c": {3}} {
		if got := indexRows(t, table, src); !reflect.DeepEqual(got, want) {
			t.Errorf("index entries for %s = %v, want %v", src, got, want)
		}
	}
}
