package storage

// A self-implemented skip list keyed by the same canonical, type-tagged
// []byte encoding secondary indexes already use (see appendCanonicalIndexValue
// in secondary_index.go), compared with bytes.Compare -- identical ordering
// semantics to the sorted []IndexEntry slice this is meant to replace as the
// live insert/lookup/delete structure behind SecondaryIndex.
//
// Why a skip list and not a plain binary search tree: table row IDs are
// commonly inserted in ascending order (auto-incrementing primary keys,
// append-only ingestion), which degrades an unbalanced BST to a linked list --
// O(n) per insert, exactly the problem this is meant to fix. A randomized
// skip list's level assignment does not depend on insertion order, so it stays
// O(log n) expected-case regardless of whether keys arrive sorted, reverse
// sorted or random.
//
// Threading: like the []IndexEntry slice it replaces, SkipList has no internal
// locking. SecondaryIndex/Table mutations are already serialized by the
// caller (the engine holds storage.DB's mu for the duration of a write
// statement; see internal/storage/db.go), and nothing in this package's
// existing secondary-index code (insertSecondaryIndexRowID,
// removeSecondaryIndexRowID, etc.) takes its own lock either. Adding one here
// would be redundant and would not match the established convention.
//
// This file intentionally has zero wiring into SecondaryIndex -- it is a
// standalone, independently testable data structure. A later stage swaps
// SecondaryIndex's live operations over to it.

import (
	"bytes"
	"math/rand"
	"sort"
)

const (
	// skipListMaxLevel caps tower height. log2(N) for N=100,000,000 is under
	// 27, so 32 levels leaves headroom far beyond any realistic table size
	// while keeping the head node's forward slice small.
	skipListMaxLevel = 32
	// skipListP is the level-growth probability. p=0.5 is the standard
	// choice from Pugh's original skip list paper, balancing expected search
	// cost against expected pointer overhead per node.
	skipListP = 0.5
)

// skipListNode is one key's entry. RowIDs is kept sorted and deduplicated,
// matching insertSecondaryIndexRowID/removeSecondaryIndexRowID's existing
// exact semantics (see Insert/Remove below).
type skipListNode struct {
	key     []byte
	rowIDs  []int
	forward []*skipListNode
}

// SkipList maps a canonically-encoded []byte key to a sorted []int of row
// IDs, ordered by bytes.Compare. It is the runtime-only counterpart to the
// GOB-persisted []IndexEntry slice: same ordering, same key/value shape, but
// O(log n) expected-case insert/lookup/delete instead of the slice's O(n)
// insert.
type SkipList struct {
	head   *skipListNode
	level  int // number of levels currently in use, always >= 1
	length int // number of distinct keys
	rng    *rand.Rand
}

// NewSkipList returns an empty skip list ready for use.
func NewSkipList() *SkipList {
	return &SkipList{
		head:  &skipListNode{forward: make([]*skipListNode, skipListMaxLevel)},
		level: 1,
		// Seeded from the package-level source once at construction (cheap,
		// and safe for concurrent callers per math/rand's documented
		// top-level guarantees), then used unlocked for every later level
		// draw on this instance -- avoiding global-source contention on
		// every insert. Mirrors the rand.New(rand.NewSource(rand.Int63()))
		// pattern already used in internal/engine/vector_functions.go.
		rng: rand.New(rand.NewSource(rand.Int63())),
	}
}

// randomLevel draws a tower height via repeated coin flips at p=0.5, capped
// at skipListMaxLevel. This is the classic Pugh skip-list level draw.
func (s *SkipList) randomLevel() int {
	level := 1
	for level < skipListMaxLevel && s.rng.Float64() < skipListP {
		level++
	}
	return level
}

// Len returns the number of distinct keys currently stored.
func (s *SkipList) Len() int {
	return s.length
}

// insertRowIDSorted inserts rowID into a sorted, deduplicated []int slice in
// place, matching insertSecondaryIndexRowID's existing exact row-ID
// insertion semantics (secondary_index.go): a rowID already present is a
// no-op, so re-inserting a duplicate (as an already-checked unique index
// might via a caller retry) does not create a second copy.
func insertRowIDSorted(rowIDs []int, rowID int) []int {
	pos := sort.SearchInts(rowIDs, rowID)
	if pos < len(rowIDs) && rowIDs[pos] == rowID {
		return rowIDs
	}
	rowIDs = append(rowIDs, 0)
	copy(rowIDs[pos+1:], rowIDs[pos:])
	rowIDs[pos] = rowID
	return rowIDs
}

// removeRowIDSorted removes rowID from a sorted []int slice in place if
// present. It reports whether a removal happened, matching
// removeSecondaryIndexRowID's existing exact semantics: removing a rowID
// that is not present is a safe no-op.
func removeRowIDSorted(rowIDs []int, rowID int) ([]int, bool) {
	pos := sort.SearchInts(rowIDs, rowID)
	if pos == len(rowIDs) || rowIDs[pos] != rowID {
		return rowIDs, false
	}
	rowIDs = append(rowIDs[:pos], rowIDs[pos+1:]...)
	return rowIDs, true
}

// Insert adds rowID under key. If key does not exist yet, a new entry is
// created holding just rowID. If key already exists, rowID is merged into
// its existing RowIDs via insertRowIDSorted -- the same sorted-insert-with-
// dedup behavior insertSecondaryIndexRowID already applies for both unique
// and non-unique indexes (a unique index's uniqueness is enforced earlier,
// by CheckSecondaryIndexConstraints; by the time Insert is called the value
// being appended is already known-valid, exactly as today).
func (s *SkipList) Insert(key []byte, rowID int) {
	var update [skipListMaxLevel]*skipListNode
	x := s.head
	for i := s.level - 1; i >= 0; i-- {
		for x.forward[i] != nil && bytes.Compare(x.forward[i].key, key) < 0 {
			x = x.forward[i]
		}
		update[i] = x
	}
	next := x.forward[0]
	if next != nil && bytes.Equal(next.key, key) {
		next.rowIDs = insertRowIDSorted(next.rowIDs, rowID)
		return
	}

	newLevel := s.randomLevel()
	if newLevel > s.level {
		for i := s.level; i < newLevel; i++ {
			update[i] = s.head
		}
		s.level = newLevel
	}

	node := &skipListNode{
		key:     append([]byte(nil), key...),
		rowIDs:  []int{rowID},
		forward: make([]*skipListNode, newLevel),
	}
	for i := 0; i < newLevel; i++ {
		node.forward[i] = update[i].forward[i]
		update[i].forward[i] = node
	}
	s.length++
}

// Remove removes rowID from key's RowIDs. If RowIDs becomes empty the whole
// key entry is removed. Removing a rowID from a key that does not exist, or
// a rowID that is not present under an existing key, is a safe no-op that
// reports removed=false -- matching removeSecondaryIndexRowID's existing
// exact semantics.
func (s *SkipList) Remove(key []byte, rowID int) (removed bool) {
	var update [skipListMaxLevel]*skipListNode
	x := s.head
	for i := s.level - 1; i >= 0; i-- {
		for x.forward[i] != nil && bytes.Compare(x.forward[i].key, key) < 0 {
			x = x.forward[i]
		}
		update[i] = x
	}
	target := x.forward[0]
	if target == nil || !bytes.Equal(target.key, key) {
		return false
	}

	rowIDs, ok := removeRowIDSorted(target.rowIDs, rowID)
	if !ok {
		return false
	}
	if len(rowIDs) > 0 {
		target.rowIDs = rowIDs
		return true
	}

	// RowIDs is now empty: unlink the node at every level it participates in.
	for i := 0; i < s.level; i++ {
		if update[i].forward[i] != target {
			break
		}
		update[i].forward[i] = target.forward[i]
	}
	for s.level > 1 && s.head.forward[s.level-1] == nil {
		s.level--
	}
	s.length--
	return true
}

// Get performs a point lookup, returning the RowIDs slice for key. The
// returned slice aliases the skip list's internal storage and must be
// treated as read-only by the caller -- it remains valid only until the
// next mutation, mirroring the existing aliasing contract documented on
// SecondaryIndex.lookup/LookupSecondaryIndexPoint in secondary_index.go.
func (s *SkipList) Get(key []byte) (rowIDs []int, found bool) {
	x := s.head
	for i := s.level - 1; i >= 0; i-- {
		for x.forward[i] != nil && bytes.Compare(x.forward[i].key, key) < 0 {
			x = x.forward[i]
		}
	}
	next := x.forward[0]
	if next != nil && bytes.Equal(next.key, key) {
		return next.rowIDs, true
	}
	return nil, false
}

// seekGE returns the first node whose key is >= key, or nil if none exists.
// An empty/nil key seeks to the very first node, i.e. the start of the list.
func (s *SkipList) seekGE(key []byte) *skipListNode {
	x := s.head
	for i := s.level - 1; i >= 0; i-- {
		for x.forward[i] != nil && bytes.Compare(x.forward[i].key, key) < 0 {
			x = x.forward[i]
		}
	}
	return x.forward[0]
}

// All returns every entry in ascending key order, as freshly allocated
// IndexEntry values -- independent copies, safe to hand to a GOB encoder or
// otherwise retain past the next skip-list mutation. This is what
// materializes the persisted Entries []IndexEntry slice from the live skip
// list at a persistence boundary (checkpoint/SaveTable).
func (s *SkipList) All() []IndexEntry {
	out := make([]IndexEntry, 0, s.length)
	for x := s.head.forward[0]; x != nil; x = x.forward[0] {
		out = append(out, IndexEntry{
			Key:    append([]byte(nil), x.key...),
			RowIDs: append([]int(nil), x.rowIDs...),
		})
	}
	return out
}

// Clone returns a deep, independent copy of the skip list: same keys, same
// RowIDs, same tower heights, but entirely separate nodes -- mutating the
// clone never affects the original, or vice versa.
//
// This is O(n), not O(n log n): rather than re-inserting every key (which
// would re-run randomLevel and a fresh search per key), it walks the
// original once in ascending order while keeping a "last node seen at each
// level" cursor, exactly as Insert's own update[] array would look after
// seeking to the end of the list. Reusing each source node's existing
// height (rather than drawing a new random one) means no search is needed
// at all: the next node at level i is simply linked after whatever node
// last occupied level i.
func (s *SkipList) Clone() *SkipList {
	out := &SkipList{
		head:   &skipListNode{forward: make([]*skipListNode, skipListMaxLevel)},
		level:  s.level,
		length: s.length,
		rng:    rand.New(rand.NewSource(rand.Int63())),
	}
	last := make([]*skipListNode, skipListMaxLevel)
	for i := range last {
		last[i] = out.head
	}
	for x := s.head.forward[0]; x != nil; x = x.forward[0] {
		height := len(x.forward)
		node := &skipListNode{
			key:     append([]byte(nil), x.key...),
			rowIDs:  append([]int(nil), x.rowIDs...),
			forward: make([]*skipListNode, height),
		}
		for i := 0; i < height; i++ {
			last[i].forward[i] = node
			last[i] = node
		}
	}
	return out
}

// Range walks entries in ascending key order starting at the first key >=
// startKey (an empty/nil startKey starts at the very first entry), calling
// fn for each one. fn's key and rowIDs alias internal storage exactly like
// Get's return value -- read-only, valid only until the next mutation.
// Range stops as soon as fn returns false, letting a caller implement
// equality (stop after the first non-matching key), prefix (stop once
// bytes.HasPrefix fails) and bounded-range (stop once past the upper bound)
// seeks the same way secondary_index_range.go's slice-based walk already
// does, without Range needing to know which shape is in play.
func (s *SkipList) Range(startKey []byte, fn func(key []byte, rowIDs []int) bool) {
	var x *skipListNode
	if len(startKey) == 0 {
		x = s.head.forward[0]
	} else {
		x = s.seekGE(startKey)
	}
	for x != nil {
		if !fn(x.key, x.rowIDs) {
			return
		}
		x = x.forward[0]
	}
}
