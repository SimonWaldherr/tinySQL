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
