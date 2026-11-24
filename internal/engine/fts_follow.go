// Following a table through DELETEs: how a persistent full-text index applies
// the removals and UPDATEs the table's change log reports instead of being
// rebuilt from scratch (see Table.ChangesSince).
package engine

import (
	"runtime"
	"slices"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// ftsFollowChanges brings index, which was current at changes' base version, up
// to the table's current rows and returns the index to keep. ok is the second
// result of Table.ChangesSince. It returns a fresh empty index (to be built by
// ftsExtendPersistent) whenever following the log is not possible or not
// worthwhile, so the caller never has to care which way it went.
//
// The caller holds the table's DerivedLock; nothing may read the index's
// postings meanwhile. DML holds the database's exclusive content lock, so no
// search runs during a write, and searches that follow it are serialised here
// by that lock and by the build coalescing of getFTSDocCacheKeyed.
func ftsFollowChanges(table *storage.Table, cols []int, index *storage.FTSIndex, changes storage.TableChanges, ok bool) *storage.FTSIndex {
	if !ok {
		return &storage.FTSIndex{}
	}
	limit := ftsRefreshRowLimit(len(table.Rows))
	if len(changes.Dead) == 0 {
		switch {
		case len(changes.Updated) == 0:
			// Nothing indexed changed: keep postings and arenas as they are.
			// ftsExtendPersistent moves the version stamps forward (and covers
			// rows appended in between).
			index.StructVersion = table.StructVersion()
		case len(changes.Updated) <= limit:
			ftsRefreshUpdatedRows(table, cols, index, changes.Updated)
			if ftsPersistentNeedsCompaction(index) {
				return &storage.FTSIndex{}
			}
		default:
			// Measured at 1M documents: the incremental refresh costs 2-4 ms
			// per row (re-tokenizing plus moving every touched posting list)
			// and loses against a rebuild beyond a few thousand rows.
			return &storage.FTSIndex{}
		}
		return index
	}

	if len(changes.Updated) > limit || ftsRemovalNeedsCompaction(index, changes.Dead) {
		return &storage.FTSIndex{}
	}
	// From here the index is edited piece by piece. It must not claim to be
	// current or valid until it is whole again, so that a panic in the middle can
	// only ever lead to a rebuild.
	wasVerified := index.Verified()
	index.SetVerified(false)
	index.Version, index.StructVersion = -1, -1
	applied, emptyLists := ftsApplyRemovalsCounting(index, changes.Dead)
	if !applied {
		return &storage.FTSIndex{}
	}
	// A term whose documents are all gone keeps an empty list (its id is dense and
	// shared with the term names). They cost memory, sidecar size, every later
	// renumbering pass and every prefix expansion, so once a quarter of the
	// vocabulary is dead weight, rebuild once to drop it. Searches are identical
	// either way: a term in no document has no idf.
	if emptyLists >= ftsEmptyListsMin && emptyLists*4 > len(index.TermIDs) {
		return &storage.FTSIndex{}
	}
	index.BuiltRows = changes.Survivors
	if index.NumDocs > 0 {
		index.AvgDocLen = index.TotalDocLen / float64(index.NumDocs)
	} else {
		index.AvgDocLen = 0
	}
	if len(changes.Updated) > 0 {
		moved := make([]int, len(changes.Updated))
		for i, orig := range changes.Updated {
			moved[i] = changes.NewPosition(orig)
		}
		ftsRefreshUpdatedRows(table, cols, index, moved)
	}
	index.Version = table.Version
	index.StructVersion = table.StructVersion()
	index.SetVerified(wasVerified)
	if ftsPersistentNeedsCompaction(index) {
		return &storage.FTSIndex{}
	}
	return index
}

// ftsRemovalNeedsCompaction reports whether removing the documents at dead would
// leave so many obsolete arena entries that the index is better rebuilt, the
// same 25 percent rule ftsPersistentNeedsCompaction applies after an UPDATE.
// Deciding before the work saves doing it for nothing.
func ftsRemovalNeedsCompaction(index *storage.FTSIndex, dead []int) bool {
	probe := storage.FTSIndex{StaleTerms: index.StaleTerms, StaleTokens: index.StaleTokens,
		DocTermIDs: index.DocTermIDs, DocTokenIDs: index.DocTokenIDs}
	for _, d := range dead {
		if d < 0 || d >= len(index.Docs) {
			return true // inconsistent: rebuild
		}
		if doc := index.Docs[d]; doc.Valid {
			probe.StaleTerms += int(doc.TermCount)
			probe.StaleTokens += int(doc.TokenCount)
		}
	}
	return ftsPersistentNeedsCompaction(&probe)
}

// ftsApplyRemovals deletes the documents at the ascending positions dead from
// index and renumbers the rest, leaving every posting list, count, block bound,
// statistic and the document directory exactly as a build over the surviving
// rows would have them (term ids and arena offsets aside). It returns false when
// the index turned out to be inconsistent; the caller discards it then.
//
// BuiltRows, AvgDocLen and the stamps are the caller's to set.
func ftsApplyRemovals(index *storage.FTSIndex, dead []int) bool {
	ok, _ := ftsApplyRemovalsCounting(index, dead)
	return ok
}

// ftsEmptyListsMin is how many empty posting lists (terms no document has any
// more) an index tolerates before ftsFollowChanges prefers a rebuild.
const ftsEmptyListsMin = 1024

// ftsApplyRemovalsHook, when set by a test, runs after each step of
// ftsApplyRemovalsCounting (1 to 5) so a failure can be injected in the middle.
var ftsApplyRemovalsHook func(step int)

// ftsApplyRemovalsCounting is ftsApplyRemovals that also reports how many
// posting lists are empty afterwards, counted for free by the renumbering pass.
func ftsApplyRemovalsCounting(index *storage.FTSIndex, dead []int) (ok bool, emptyLists int) {
	if len(dead) == 0 {
		return true, 0
	}
	if dead[0] < 0 || dead[len(dead)-1] >= len(index.Docs) {
		return false, 0
	}
	step := func(n int) {
		if ftsApplyRemovalsHook != nil {
			ftsApplyRemovalsHook(n)
		}
	}
	termNames := index.TermNames()

	// 1. Statistics, and which documents leave which posting lists. The runs of
	// the dead documents are still in the arenas; they become obsolete entries.
	pairCount := 0
	for _, d := range dead {
		if doc := index.Docs[d]; doc.Valid && doc.TermCount > 0 {
			pairCount += int(doc.TermCount)
		}
	}
	if pairCount > len(index.DocTermIDs) {
		return false, 0 // the dead documents own more term entries than exist
	}
	pairs := make([]uint64, 0, pairCount) // term id << 32 | original position
	for _, d := range dead {
		doc := index.Docs[d]
		if !doc.Valid {
			continue
		}
		index.NumDocs--
		index.TotalDocLen -= doc.DocLen
		index.StaleTerms += int(doc.TermCount)
		index.StaleTokens += int(doc.TokenCount)
		end := int64(doc.TermStart) + int64(doc.TermCount)
		if doc.TermStart < 0 || doc.TermCount < 0 || end > int64(len(index.DocTermIDs)) {
			return false, 0
		}
		for _, id := range index.DocTermIDs[doc.TermStart:end] {
			if id < 0 || int(id) >= len(termNames) {
				return false, 0
			}
			pairs = append(pairs, uint64(id)<<32|uint64(d))
		}
	}
	slices.Sort(pairs)
	step(1)

	// 2. Drop the dead entries from their lists (original coordinates still), and
	// remember where each list lost its first entry for the block rebuild.
	firstRemoved := make(map[string]int)
	for i := 0; i < len(pairs); {
		id := pairs[i] >> 32
		j := i
		for j < len(pairs) && pairs[j]>>32 == id {
			j++
		}
		term := termNames[id]
		rows, counts := index.Postings[term], index.PostingCounts[term]
		if len(counts) != len(rows) {
			return false, 0
		}
		want := int32(uint32(pairs[i]))
		first := sort.Search(len(rows), func(k int) bool { return rows[k] >= want })
		w, next := first, i
		for k := first; k < len(rows); k++ {
			if next < j && rows[k] == int32(uint32(pairs[next])) {
				next++
				continue
			}
			rows[w], counts[w] = rows[k], counts[k]
			w++
		}
		if next != j {
			return false, 0 // a dead document was not in its list
		}
		index.Postings[term], index.PostingCounts[term] = rows[:w], counts[:w]
		firstRemoved[term] = first
		i = j
	}
	step(2)

	// 3. Close the gaps in the document directory.
	w := dead[0]
	for k, d := range dead {
		end := len(index.Docs)
		if k+1 < len(dead) {
			end = dead[k+1]
		}
		w += copy(index.Docs[w:], index.Docs[d+1:end])
	}
	clear(index.Docs[w:])
	index.Docs = index.Docs[:w]
	step(3)

	// 4. Renumber every posting entry behind the first dead document.
	emptyLists = ftsShiftPostings(index.Postings, dead)
	step(4)

	// 5. Block bounds: lists that only shifted keep theirs (membership, counts
	// and document lengths are unchanged); a list that lost entries rebuilds from
	// the block that held its first removal.
	ftsRebuildBlocksOf(index, firstRemoved)
	step(5)
	return true, emptyLists
}

// ftsRunWorkers runs fn on workers goroutines and waits for all of them. A panic
// in one is re-raised on the caller once every worker has stopped, where the
// executor's recovery turns it into an error; left alone it would end the process.
func ftsRunWorkers(workers int, fn func()) {
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		panicked any
		didPanic bool
	)
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					mu.Lock()
					if !didPanic {
						panicked, didPanic = r, true
					}
					mu.Unlock()
				}
			}()
			fn()
		}()
	}
	wg.Wait()
	if didPanic {
		panic(panicked)
	}
}

// ftsRebuildBlocksOf rebuilds the block bounds of the lists named in first (term
// to the entry from which to rebuild), spreading a large set over the cores. The
// lists are independent and only read the document directory; the map is written
// once the workers are done.
func ftsRebuildBlocksOf(index *storage.FTSIndex, first map[string]int) {
	const parallelMinEntries = 1 << 16
	entries := 0
	for term, from := range first {
		entries += len(index.Postings[term]) - from
	}
	workers := min(runtime.GOMAXPROCS(0), len(first))
	if workers < 2 || entries < parallelMinEntries {
		for term, from := range first {
			ftsRebuildPostingBlocks(index, term, from)
		}
		return
	}
	type job struct {
		term   string
		from   int
		blocks []storage.FTSPostingBlock
	}
	jobs := make([]job, 0, len(first))
	for term, from := range first {
		jobs = append(jobs, job{term: term, from: from})
	}
	var next atomic.Int64
	ftsRunWorkers(workers, func() {
		for {
			i := int(next.Add(1)) - 1
			if i >= len(jobs) {
				return
			}
			j := &jobs[i]
			j.blocks = ftsPostingBlocksFrom(index.Docs, index.Postings[j.term], index.PostingCounts[j.term], index.PostingBlocks[j.term], j.from)
		}
	})
	for _, j := range jobs {
		index.PostingBlocks[j.term] = j.blocks
	}
}

// ftsShiftPostings subtracts from every entry of every list the number of dead
// documents before it. dead is ascending and none of the entries is dead. It
// returns how many of the lists are empty.
func ftsShiftPostings(postings map[string][]int32, dead []int) (empty int) {
	const parallelMinLists = 8192
	workers := runtime.GOMAXPROCS(0)
	if workers < 2 || len(postings) < parallelMinLists {
		for _, list := range postings {
			if len(list) == 0 {
				empty++
				continue
			}
			ftsShiftList(list, dead)
		}
		return empty
	}
	lists := make([][]int32, 0, len(postings))
	for _, list := range postings {
		lists = append(lists, list)
	}
	const chunk = 2048
	var next, emptied atomic.Int64
	ftsRunWorkers(workers, func() {
		for {
			lo := int(next.Add(chunk)) - chunk
			if lo >= len(lists) {
				return
			}
			n := 0
			for _, list := range lists[lo:min(lo+chunk, len(lists))] {
				if len(list) == 0 {
					n++
					continue
				}
				ftsShiftList(list, dead)
			}
			emptied.Add(int64(n))
		}
	})
	return int(emptied.Load())
}

// ftsShiftList renumbers one ascending posting list: entries before dead[0] are
// untouched (found by binary search), the others lose the count of dead
// positions below them.
func ftsShiftList(list []int32, dead []int) {
	first := int32(dead[0])
	lo, hi := 0, len(list)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if list[mid] < first {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	j := 0 // number of dead positions below the current entry
	for i := lo; i < len(list); i++ {
		e := int(list[i])
		if j < len(dead) && dead[j] < e {
			j += sort.SearchInts(dead[j:], e)
		}
		if j == len(dead) {
			for ; i < len(list); i++ {
				list[i] -= int32(j)
			}
			return
		}
		list[i] = int32(e - j)
	}
}
