package storage

import "reflect"

// Secondary-index twins: the skip-list counterpart of row twins (row_twin.go).
//
// A transaction's copy of a table borrows the table's secondary indexes and
// copies a skip list the first time it changes one (hydrateForWrite). For a
// transaction that inserts one row into an indexed 100000-row table that copy
// was the whole cost of the transaction.
//
// The copy now journals its mutations. At COMMIT the list of the table being
// replaced is no longer needed by anyone, differs from the committed list by
// exactly that journal, and is replayed and parked on the committed index as
// its twin. The next transaction that copies the table is handed the twin as
// its private list (borrow), again after a journal replay, so no list is copied
// while transactions follow one another.
//
// A twin is dropped, never trusted, when its journal overflowed
// (skipListJournalMax), when the index's list was replaced wholesale (rebuilds,
// DELETE compaction, statement rollback), or when another index may still read
// the list that would be recycled (SkipList.sharers).

// skipListTwin is a spare skip list: list equals of with of's journal undone.
type skipListTwin struct {
	list *SkipList
	of   *SkipList
}

// indexOrigin records, on a transaction's copy of an index, the list the copy
// started from (base) and the private list that has journalled every change
// since (own; nil while the copy still shares base).
type indexOrigin struct {
	base *SkipList
	own  *SkipList
	// lender and baseMutations are set when own was handed over as a twin:
	// the index it came from and how often base had been mutated by then.
	lender        *SecondaryIndex
	baseMutations uint64
}

// returnedList is a twin a transaction handed back unused: list equals base as
// long as base has still been mutated exactly mutations times.
type returnedList struct {
	list      *SkipList
	base      *SkipList
	mutations uint64
}

// returnTwin gives the skip list idx took as a twin back to the index it came
// from, provided idx never changed it. See DB.returnTwins.
func (idx *SecondaryIndex) returnTwin() {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	o := idx.origin
	idx.origin = nil
	if o == nil || o.lender == nil || o.own == nil || idx.fast != o.own ||
		!o.own.journaling || len(o.own.journal) != 0 {
		return
	}
	o.lender.returned.Store(&returnedList{list: o.own, base: o.base, mutations: o.baseMutations})
}

// takeTwin returns a private skip list equal to idx.fast, or nil when idx has
// no usable twin. The caller holds idx.mu, and idx's table cannot change during
// the call.
func (idx *SecondaryIndex) takeTwin() *SkipList {
	returned := idx.returned.Swap(nil)
	twin := idx.twin
	if twin == nil {
		// A list a transaction took and handed back untouched is as good as
		// a twin while the index has not changed since.
		if returned == nil || returned.base != idx.fast || returned.mutations != idx.fast.mutations {
			return nil
		}
		if verifyRowTwins {
			mustMirrorList(returned.list, idx.fast, "return")
		}
		twinIndexTakes.Add(1)
		return returned.list
	}
	idx.twin = nil
	if twin.of != idx.fast || !idx.fast.replayJournalOnto(twin.list) {
		return nil
	}
	idx.fast.stopJournal()
	if verifyRowTwins {
		mustMirrorList(twin.list, idx.fast, "take")
	}
	twinIndexTakes.Add(1)
	return twin.list
}

// adoptIndexTwins is the secondary-index half of adoptTwins: old is unpinned,
// unchanged since next was copied from it, and has just been replaced by next.
func adoptIndexTwins(old, next *Table) {
	for name, nidx := range next.Indexes {
		if nidx == nil {
			continue
		}
		nidx.mu.Lock()
		origin := nidx.origin
		nidx.origin = nil
		oidx := old.Indexes[name]
		switch {
		case origin == nil || oidx == nil || oidx.fast != origin.base:
			// Not a copy of this index, or the old index moved on.
		case nidx.fast == origin.base:
			// The transaction never changed the index and still shares the
			// list with the table that is going away.
			if origin.base.sharers.Add(-1) <= 0 {
				nidx.shared = false
			}
		case origin.own == nil || nidx.fast != origin.own:
			// The transaction replaced its list wholesale.
		case origin.base.sharers.Load() > 0 || len(next.Rows) < rowTwinMinRows:
			// Someone else may still read the old list, or the index is too
			// small for a twin to pay off.
			origin.own.stopJournal()
		case origin.own.replayJournalOnto(origin.base):
			if verifyRowTwins {
				mustMirrorList(origin.base, origin.own, "adopt")
			}
			origin.base.stopJournal()
			origin.own.startJournal()
			nidx.twin = &skipListTwin{list: origin.base, of: origin.own}
		}
		nidx.mu.Unlock()
	}
}

// forgetIndexOrigins clears the copy bookkeeping of a table whose predecessor
// cannot be recycled.
func forgetIndexOrigins(t *Table) {
	for _, idx := range t.Indexes {
		if idx == nil {
			continue
		}
		idx.mu.Lock()
		if o := idx.origin; o != nil && o.own != nil && idx.twin == nil {
			o.own.stopJournal()
		}
		idx.origin = nil
		idx.mu.Unlock()
	}
}

// mustMirrorList panics unless got and want hold the same entries.
func mustMirrorList(got, want *SkipList, stage string) {
	if !reflect.DeepEqual(got.All(), want.All()) {
		panic("tinysql: index twin (" + stage + ") differs from its index")
	}
}
