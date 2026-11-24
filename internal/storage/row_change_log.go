// ChangesSince: the composed view of the table's ordered change log (UPDATE and
// DELETE events) that lets a derived index follow a table instead of rebuilding.
package storage

import (
	"slices"
	"sort"
)

// TableChanges is what happened to the rows a derived structure indexed, as told
// by Table.ChangesSince. Every position in Dead and Updated is in the ORIGINAL
// coordinates of the index, i.e. the row's position when the index was built.
type TableChanges struct {
	// Dead lists, ascending, the indexed rows that no longer exist.
	Dead []int
	// Updated lists, ascending, the surviving indexed rows whose columns of
	// interest changed. Rows that only changed in other columns are left out.
	Updated []int
	// Survivors is the number of indexed rows that still exist. The first
	// Survivors rows of the table are exactly those rows, in their old order; every
	// row after them was appended since.
	Survivors int
}

// NewPosition maps the original position of a surviving indexed row to its
// position in the table now. It must not be called for a dead row.
func (c *TableChanges) NewPosition(orig int) int {
	return orig - sort.SearchInts(c.Dead, orig)
}

// ChangesSince tells a consumer that indexed the first indexRows rows of this
// table when its StructVersion was version, what happened to those rows since.
// interest is a ColumnMask of the columns the consumer depends on (zero: all); an
// UPDATE that touched none of them is not reported. ok is false whenever the log
// cannot vouch for the answer (an evicted or dropped history, a change that does
// not report its rows, an overflow, a version from the future, rows missing from
// the table): the consumer must then rebuild.
//
// The caller must hold whatever synchronisation it needs against writers; the
// result is a snapshot that owns its slices.
func (t *Table) ChangesSince(version, indexRows int, interest uint64) (changes TableChanges, ok bool) {
	if indexRows < 0 || version > t.structVersion {
		return TableChanges{}, false
	}
	if version == t.structVersion {
		if indexRows > len(t.Rows) {
			return TableChanges{}, false
		}
		return TableChanges{Survivors: indexRows}, true
	}
	if version < t.rowUpdateBase {
		return TableChanges{}, false
	}

	var (
		dead    []int // original positions of removed indexed rows, ascending
		updated []int // original positions of updated indexed rows (unsorted, may repeat)
		origs   []int // scratch: originals of one removal
		moves   int
	)
	cur := indexRows // length of the still-indexed prefix of the table
	// compose folds one removal into the dead list. Its positions are in the
	// coordinates before the statement, so all of them are pulled back against the
	// dead list as it was before it.
	compose := func(removal *rowRemoval) bool {
		origs = origs[:0]
		for _, q := range removal.positions {
			if int(q) >= cur {
				break // ascending: the rest are unindexed rows
			}
			origs = append(origs, pullBack(dead, int(q)))
		}
		if len(origs) == 0 {
			return true
		}
		oldLen := len(dead)
		dead = slices.Grow(dead, len(origs))[:oldLen+len(origs)]
		i1, j, k := oldLen-1, len(origs)-1, len(dead)-1
		for j >= 0 {
			if i1 >= 0 && dead[i1] > origs[j] {
				dead[k] = dead[i1]
				i1--
			} else {
				dead[k] = origs[j]
				j--
			}
			k--
		}
		moves += oldLen - (i1 + 1) + len(origs)
		cur -= len(origs)
		return moves <= maxChangeComposeMoves
	}

	// The oldest history is kept as folds: a summary of consecutive UPDATEs can only
	// vouch that no column of interest changed in them, a removal is kept whole.
	// Folds precede the log without a gap (see foldRowUpdateLog).
	want := version + 1
	for _, fold := range t.rowUpdateFolds {
		if fold.upTo <= version {
			continue
		}
		if fold.removed != nil {
			if fold.upTo != want || !compose(fold.removed) {
				return TableChanges{}, false
			}
		} else if interest == 0 || fold.cols&interest != 0 {
			return TableChanges{}, false
		}
		want = fold.upTo + 1
	}

	log := &t.rowUpdateLog
	total := log.len()
	for i := log.search(want); i < total; i++ {
		delta := log.at(i)
		if delta.structVersion != want {
			return TableChanges{}, false
		}
		want++
		if delta.removed == nil {
			p := delta.row
			if p < 0 {
				return TableChanges{}, false
			}
			if p >= cur {
				continue // an unindexed row appended since
			}
			if interest != 0 && delta.cols != 0 && delta.cols&interest == 0 {
				continue
			}
			updated = append(updated, pullBack(dead, p))
			continue
		}
		if !compose(delta.removed) {
			return TableChanges{}, false
		}
	}
	if want != t.structVersion+1 {
		return TableChanges{}, false
	}
	survivors := indexRows - len(dead)
	if survivors > len(t.Rows) {
		return TableChanges{}, false
	}
	slices.Sort(updated)
	updated = slices.Compact(updated)
	if len(dead) > 0 {
		// A row that was updated and later removed is simply dead.
		kept := updated[:0]
		for _, orig := range updated {
			if _, gone := slices.BinarySearch(dead, orig); !gone {
				kept = append(kept, orig)
			}
		}
		updated = kept
	}
	return TableChanges{Dead: dead, Updated: updated, Survivors: survivors}, true
}

// pullBack returns the original position of the surviving row that sits at
// position p now, given the ascending original positions of the rows removed so
// far. Of the removed positions, dead[i]-i is non-decreasing, and the rows before
// the survivor are exactly those i with dead[i]-i <= p.
func pullBack(dead []int, p int) int {
	return p + sort.Search(len(dead), func(i int) bool { return dead[i]-i > p })
}

// hasRemovalSince reports whether the change log holds a removal event stamped
// after version. A rollback uses it to find out whether the statement it undoes
// logged a removal, which a restored table must not keep.
func (t *Table) hasRemovalSince(version int) bool {
	log := &t.rowUpdateLog
	for i := log.search(version + 1); i < log.len(); i++ {
		if log.at(i).removed != nil {
			return true
		}
	}
	return false
}
