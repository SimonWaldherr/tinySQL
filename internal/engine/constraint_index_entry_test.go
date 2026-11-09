package engine

import (
	"math/rand"
	"reflect"
	"slices"
	"testing"
)

// modelIndex is the representation constraintIndexEntry replaced: one []int per
// key, mutated with the same append / order-preserving-remove rules. The
// differential test below drives both with the same random operations.
type modelIndex map[any][]int

func (m modelIndex) add(k any, row int) { m[k] = append(m[k], row) }

func (m modelIndex) remove(k any, row int) {
	bucket := m[k]
	for i, ri := range bucket {
		if ri == row {
			if len(bucket) == 1 {
				delete(m, k)
			} else {
				m[k] = append(bucket[:i], bucket[i+1:]...)
			}
			return
		}
	}
}

func (m modelIndex) clone() modelIndex {
	out := make(modelIndex, len(m))
	for k, v := range m {
		out[k] = slices.Clone(v)
	}
	return out
}

func (m modelIndex) exists(k any, exclude int) bool {
	for _, ri := range m[k] {
		if ri != exclude {
			return true
		}
	}
	return false
}

func TestConstraintIndexEntryMatchesModel(t *testing.T) {
	for seed := int64(1); seed <= 40; seed++ {
		rng := rand.New(rand.NewSource(seed))
		// Few keys relative to operations forces multi-row buckets, bucket
		// collapse back to one row, and recycling of freed bucket slots.
		keys := 3 + rng.Intn(20)
		entry := newConstraintIndexEntry(0)
		model := modelIndex{}
		type snapshot struct {
			entry *constraintIndexEntry
			model modelIndex
		}
		var snaps []snapshot

		check := func(step int) {
			t.Helper()
			if got := entry.buckets(); !reflect.DeepEqual(got, map[any][]int(model)) {
				t.Fatalf("seed %d step %d: index=%v model=%v", seed, step, got, model)
			}
			for k := 0; k < keys; k++ {
				for _, ex := range []int{-1, 0, 7} {
					if entry.exists(k, ex) != model.exists(k, ex) {
						t.Fatalf("seed %d step %d: exists(%d,%d) mismatch", seed, step, k, ex)
					}
				}
			}
		}

		for step := 0; step < 600; step++ {
			k := rng.Intn(keys)
			row := rng.Intn(40)
			switch rng.Intn(10) {
			case 0, 1, 2, 3, 4:
				entry.add(k, row)
				model.add(k, row)
			case 5, 6, 7:
				entry.remove(k, row)
				model.remove(k, row)
			case 8:
				snaps = append(snaps, snapshot{entry.clone(), model.clone()})
			default:
				// Mutate the original heavily after a clone: the clone must not move.
				if len(snaps) > 0 {
					s := snaps[rng.Intn(len(snaps))]
					s.entry.add(k, row)
					s.model.add(k, row)
				}
			}
			check(step)
		}
		for i, s := range snaps {
			if got := s.entry.buckets(); !reflect.DeepEqual(got, map[any][]int(s.model)) {
				t.Fatalf("seed %d: clone %d diverged: %v vs %v", seed, i, got, s.model)
			}
		}
	}
}

// Buckets carved from one backing array must not write into each other when one
// of them grows.
func TestConstraintIndexEntryCloneBucketsAreIndependent(t *testing.T) {
	e := newConstraintIndexEntry(0)
	for row := 0; row < 4; row++ {
		e.add("a", row)
		e.add("b", row+10)
	}
	c := e.clone()
	c.add("a", 99)
	c.add("a", 100)
	if got, _ := c.appendRows(nil, "b"); !slices.Equal(got, []int{10, 11, 12, 13}) {
		t.Fatalf("growing bucket a corrupted bucket b: %v", got)
	}
	if got, _ := e.appendRows(nil, "a"); !slices.Equal(got, []int{0, 1, 2, 3}) {
		t.Fatalf("clone mutation leaked into original: %v", got)
	}
}

// uniqueModel is the model for entries holding one row per key, which is the
// only kind constraintIndexEntry.borrow lends.
type uniqueModel map[int]int

func (m uniqueModel) clone() uniqueModel {
	out := make(uniqueModel, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func (m uniqueModel) buckets() map[any][]int {
	out := make(map[any][]int, len(m))
	for k, v := range m {
		out[k] = []int{v}
	}
	return out
}

// Entries borrowed from one another must behave exactly like independent
// copies: a write to any of them, the lender included, is invisible to the
// others. The test keeps a family of entries (originals, borrowers, borrowers of
// borrowers) next to a plain-map model of each and checks all of them after
// every step.
func TestConstraintIndexEntryBorrowMatchesModel(t *testing.T) {
	for seed := int64(1); seed <= 60; seed++ {
		rng := rand.New(rand.NewSource(seed))
		keys := 20 + rng.Intn(200)
		type member struct {
			entry *constraintIndexEntry
			model uniqueModel
		}
		first := newConstraintIndexEntry(0)
		family := []member{{first, uniqueModel{}}}

		for step := 0; step < 800; step++ {
			i := rng.Intn(len(family))
			m := family[i]
			k := rng.Intn(keys)
			switch op := rng.Intn(12); {
			case op < 5: // insert a key that is not held yet
				if _, held := m.model[k]; !held {
					row := rng.Intn(1000)
					m.entry.add(k, row)
					m.model[k] = row
				}
			case op < 8: // delete a held key
				if row, held := m.model[k]; held {
					m.entry.remove(k, row)
					delete(m.model, k)
				}
			case op < 10: // move a key to another row, as an UPDATE of the key does
				if row, held := m.model[k]; held {
					m.entry.remove(k, row)
					next := rng.Intn(1000)
					m.entry.add(k, next)
					m.model[k] = next
				}
			case op == 10 && len(family) < 12:
				b, ok := m.entry.borrow()
				if !ok {
					t.Fatalf("seed %d step %d: borrow refused an entry without multi buckets", seed, step)
				}
				family = append(family, member{b, m.model.clone()})
			default:
				// A flat copy must also stay independent of its source.
				family = append(family, member{m.entry.clone(), m.model.clone()})
			}
			for j, f := range family {
				if got := f.entry.buckets(); !reflect.DeepEqual(got, f.model.buckets()) {
					t.Fatalf("seed %d step %d: member %d diverged from its model", seed, step, j)
				}
				for probe := 0; probe < keys; probe += 7 {
					_, held := f.model[probe]
					if f.entry.exists(probe, -1) != held {
						t.Fatalf("seed %d step %d: member %d exists(%d)=%v want %v", seed, step, j, probe, !held, held)
					}
				}
			}
		}
	}
}

// An entry with multi-row buckets cannot be lent: its bucket slices grow in
// place. borrow must say so instead of sharing them.
func TestConstraintIndexEntryBorrowRefusesMultiBuckets(t *testing.T) {
	e := newConstraintIndexEntry(0)
	e.add("a", 1)
	e.add("a", 2)
	if _, ok := e.borrow(); ok {
		t.Fatal("borrow shared an entry that has a multi-row bucket")
	}
	set := &constraintIndexSet{cols: map[int]*constraintIndexEntry{0: e}}
	if _, ok := set.BorrowDerived(); ok {
		t.Fatal("BorrowDerived lent a set that contains a multi-row bucket")
	}
}

// Once an overlay grows past the point where carrying it forward is cheaper
// than starting over, the next borrower gets a flat map and the chain of
// overlays does not deepen.
func TestConstraintIndexEntryBorrowFlattensLargeOverlay(t *testing.T) {
	e := newConstraintIndexEntry(0)
	for k := 0; k < 100; k++ {
		e.add(k, k)
	}
	b, _ := e.borrow()
	for k := 100; k < 100+constraintOverlayMinDelta*2; k++ {
		b.add(k, k)
	}
	b.remove(5, 5)
	c, ok := b.borrow()
	if !ok {
		t.Fatal("borrow refused")
	}
	if c.base != nil {
		t.Fatal("oversized overlay was carried forward instead of flattened")
	}
	want := 100 + constraintOverlayMinDelta*2 - 1
	if got := len(c.buckets()); got != want {
		t.Fatalf("flattened entry holds %d keys, want %d", got, want)
	}
	if c.exists(5, -1) {
		t.Fatal("key removed in the overlay reappeared after flattening")
	}
	// The lender's own view is untouched.
	if !e.exists(5, -1) || e.exists(100, -1) {
		t.Fatal("lender changed")
	}
}
