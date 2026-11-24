// Following a table through DELETEs: how an HNSW graph drops the nodes of removed
// rows and renumbers the rest instead of being rebuilt (0.3 ms per row, which is
// minutes at a million rows), see Table.ChangesSince.
package engine

import (
	"context"
	"runtime"
	"sync/atomic"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

const (
	// vecHNSWFollowTotalPercent is how many nodes may have been removed from a graph
	// since it was last built from scratch, as a share of its size, before the next
	// removal rebuilds it. Dropping a node drops the edges that led to it; the
	// neighbours that lost their edges are not repaired, so the graph slowly loses
	// recall and, in the worst case, nodes whose only edges led to dead nodes.
	vecHNSWFollowTotalPercent = 10
	// vecHNSWFollowSinglePercent is the same bound for a single removal.
	vecHNSWFollowSinglePercent = 5

	// vecDeadRankShift is the block size (1<<shift ids) of the dead-rank table.
	vecDeadRankShift = 8
)

// vecHNSWFollowChunk is the number of nodes a worker takes at a time. A variable so
// that a test can make a small graph use several workers.
var vecHNSWFollowChunk = 4096

// vecHNSWFollowCount counts the graphs brought up to date by followVecHNSWIndex,
// for tests and diagnostics.
var vecHNSWFollowCount atomic.Int64

// vecHNSWFollowHook, when set by a test, runs after each step of the removal pass
// (1 to 5; 5 is after the graph was stamped current, before the appended rows are
// inserted) so a failure can be injected in the middle.
var vecHNSWFollowHook func(step int)

// vecDeadRank answers, for the id of a node, how many removed nodes have a smaller
// id and whether the node itself is gone, in O(1) plus a scan of at most the dead
// ids inside one block.
type vecDeadRank struct {
	dead   []int   // ascending
	prefix []int32 // prefix[b] is the number of dead ids below b<<vecDeadRankShift
}

func newVecDeadRank(dead []int, nodes int) vecDeadRank {
	blocks := nodes>>vecDeadRankShift + 2
	prefix := make([]int32, blocks)
	j := 0
	for b := range prefix {
		start := b << vecDeadRankShift
		for j < len(dead) && dead[j] < start {
			j++
		}
		prefix[b] = int32(j)
	}
	return vecDeadRank{dead: dead, prefix: prefix}
}

// lookup returns the number of dead ids below id and whether id itself is dead.
func (r *vecDeadRank) lookup(id int) (below int, dead bool) {
	j := int(r.prefix[id>>vecDeadRankShift])
	for j < len(r.dead) && r.dead[j] < id {
		j++
	}
	return j, j < len(r.dead) && r.dead[j] == id
}

// vecHNSWRemovalChanges asks the change log what happened to the rows idx indexes.
// It returns the removals when idx can follow them: the log vouches for them, the
// removal is small enough that the graph keeps its quality, and the UPDATEs of the
// vector column since are few enough for the delta set. A window that removed
// only rows the graph never covered (appended since, deleted again) comes back
// with no dead rows: the caller then extends the graph instead of following.
func vecHNSWRemovalChanges(idx *vecHNSWIndex, table *storage.Table, colIdx, dims int) (storage.TableChanges, bool) {
	if idx == nil || idx.table != table || idx.dims != dims {
		return storage.TableChanges{}, false
	}
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	built := len(idx.levels)
	if idx.filteredCache != nil || idx.structVersion < 0 || len(idx.neighbors) != built {
		return storage.TableChanges{}, false
	}
	changes, ok := table.ChangesSince(idx.structVersion, built, storage.ColumnBit(colIdx))
	if !ok || changes.Survivors+len(changes.Dead) != built || changes.Survivors > len(table.Rows) {
		return storage.TableChanges{}, false
	}
	deadNodes := 0
	for _, d := range changes.Dead {
		if d < 0 || d >= built {
			return storage.TableChanges{}, false
		}
		if len(idx.neighbors[d]) > 0 {
			deadNodes++
		}
	}
	if deadNodes*100 > built*vecHNSWFollowSinglePercent || (idx.removed+deadNodes)*100 > built*vecHNSWFollowTotalPercent {
		return storage.TableChanges{}, false
	}
	if len(idx.deltaRows)+len(changes.Updated) > vecHNSWDeltaLimit(len(table.Rows)) {
		return storage.TableChanges{}, false
	}
	return changes, true
}

// vecHNSWDeltaLimit bounds the rows whose vectors were updated since the graph was
// built; search scores them exactly, so the set must stay small.
func vecHNSWDeltaLimit(tableRows int) int {
	return min(1024, max(32, tableRows/8))
}

// followVecHNSWIndex removes the nodes of the rows changes lists as dead from idx,
// renumbers the others to their positions in the table now, registers the UPDATEd
// vectors as delta rows and inserts the rows appended since, so that idx covers the
// table as of table's current versions. cache must be the vector column cache of
// the table's current rows (positions equal table positions).
//
// followed is false when the graph could not be brought up to date (it turned out
// inconsistent, or a panic interrupted the pass): idx is then stamped as not
// current, which makes the caller rebuild it, and must not be used otherwise. A
// non-nil err (the extension of the graph by appended rows was cancelled) leaves a
// graph that is consistent with the table's present structure and only misses the
// last rows, exactly like a cancelled extendVecHNSWIndex.
func followVecHNSWIndex(ctx context.Context, idx *vecHNSWIndex, table *storage.Table, cache vecSearchColumnCacheEntry, changes storage.TableChanges) (followed bool, err error) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	built := len(idx.levels)
	consistent := idx.filteredCache == nil && idx.structVersion >= 0 && len(idx.neighbors) == built &&
		changes.Survivors+len(changes.Dead) == built && cache.rowCount() == len(table.Rows) && changes.Survivors <= cache.rowCount()
	// From here the graph is edited piece by piece. It must not claim to be current
	// until it is whole again, so that a panic or a failure in the middle can only
	// ever lead to a rebuild. A graph that cannot be followed is stamped the same
	// way: the caller then rebuilds instead of asking again.
	idx.version, idx.structVersion = -1, -1
	if !consistent {
		return false, nil
	}
	defer func() {
		if r := recover(); r != nil {
			// The graph may be half edited, or (a panic while the appended rows
			// were inserted) stamped current with rows that were never linked.
			// Either way it must not be served: stamp it not current again.
			idx.version, idx.structVersion = -1, -1
			followed, err = false, nil
		}
	}()
	removed, ok := vecHNSWApplyRemovals(idx, cache, changes.Dead)
	if !ok {
		return false, nil
	}
	if idx.entry < 0 && (len(changes.Updated) != 0 || len(idx.deltaRows) != 0) {
		// No node is left that can start a search, yet rows hold vectors the graph does
		// not link: UPDATEd rows are only registered as delta rows below, which a search
		// scores but never links into the graph. A rebuild makes them nodes, as it did
		// before graphs followed removals; the graph stays stamped as not current, so
		// the caller rebuilds.
		return false, nil
	}
	idx.removed += removed
	if len(changes.Updated) != 0 {
		moved := make([]int, len(changes.Updated))
		for i, orig := range changes.Updated {
			moved[i] = changes.NewPosition(orig)
		}
		idx.addDeltaRows(moved)
	}
	idx.structVersion = table.StructVersion()
	vecHNSWFollowCount.Add(1)
	if vecHNSWFollowHook != nil {
		vecHNSWFollowHook(5)
	}
	if err := extendVecHNSWIndex(ctx, idx, cache); err != nil {
		return true, err
	}
	idx.version = table.Version
	return true, nil
}

// vecHNSWApplyRemovals deletes the nodes at the ascending positions dead and
// renumbers every other node: levels, neighbors, the entry point and the delta
// rows. Edges that led to a dead node are dropped. It returns how many graph nodes
// (rows that were inserted) went away, and false when the graph turned out to be
// inconsistent.
func vecHNSWApplyRemovals(idx *vecHNSWIndex, cache vecSearchColumnCacheEntry, dead []int) (removedNodes int, ok bool) {
	built := len(idx.levels)
	step := func(n int) {
		if vecHNSWFollowHook != nil {
			vecHNSWFollowHook(n)
		}
	}
	rank := newVecDeadRank(dead, built)
	for _, d := range dead {
		if len(idx.neighbors[d]) > 0 {
			removedNodes++
		}
	}
	step(1)

	// 1. Remap every edge of every surviving node, in place; node numbers do not
	// move yet. The nodes are independent, so the pass runs on several cores.
	var broken atomic.Bool
	var next atomic.Int64
	remap := func() {
		for {
			lo := int(next.Add(int64(vecHNSWFollowChunk))) - vecHNSWFollowChunk
			if lo >= built {
				return
			}
			for node := lo; node < min(lo+vecHNSWFollowChunk, built); node++ {
				layers := idx.neighbors[node]
				if len(layers) == 0 {
					continue
				}
				if _, gone := rank.lookup(node); gone {
					continue
				}
				for layer, links := range layers {
					w := 0
					for _, neighbor := range links {
						if neighbor < 0 || neighbor >= built {
							broken.Store(true)
							return
						}
						below, gone := rank.lookup(neighbor)
						if gone {
							continue
						}
						links[w] = neighbor - below
						w++
					}
					layers[layer] = links[:w]
				}
			}
		}
	}
	if workers := min(runtime.GOMAXPROCS(0), max(1, built/vecHNSWFollowChunk)); workers > 1 {
		ftsRunWorkers(workers, remap)
	} else {
		remap()
	}
	if broken.Load() {
		return 0, false
	}
	step(2)

	// 2. Close the gaps in the per-node arrays, in runs between the dead nodes.
	w, from := 0, 0
	run := func(to int) {
		n := copy(idx.levels[w:], idx.levels[from:to])
		copy(idx.neighbors[w:], idx.neighbors[from:to])
		w += n
	}
	for _, d := range dead {
		run(d)
		from = d + 1
	}
	run(built)
	clear(idx.neighbors[w:built])
	idx.levels = idx.levels[:w]
	idx.neighbors = idx.neighbors[:w]
	step(3)

	// 3. The entry point and the delta rows.
	if idx.entry >= 0 {
		if below, gone := rank.lookup(idx.entry); gone {
			idx.entry, idx.maxLevel = vecHNSWPickEntry(idx, cache)
		} else {
			idx.entry -= below
		}
	}
	if len(idx.deltaRows) != 0 {
		kept := idx.deltaRows[:0]
		set := make(map[int]struct{}, len(idx.deltaRows))
		for _, row := range idx.deltaRows {
			if below, gone := rank.lookup(row); !gone {
				kept = append(kept, row-below)
				set[row-below] = struct{}{}
			}
		}
		idx.deltaRows = kept
		idx.deltaSet = set
		if len(kept) == 0 {
			idx.deltaRows, idx.deltaSet = nil, nil
		}
	}
	step(4)
	return removedNodes, true
}

// vecHNSWPickEntry chooses the entry point of a graph whose entry node was removed:
// the first node with the highest level that can be scored (a node whose vector is
// zero under cosine, or no longer valid, cannot start a search). It returns -1, -1
// for a graph without such a node.
func vecHNSWPickEntry(idx *vecHNSWIndex, cache vecSearchColumnCacheEntry) (entry, maxLevel int) {
	rows := newVecRowResolver(&cache, metricNeedsNorms(idx.metric))
	entry, maxLevel = -1, -1
	for node, layers := range idx.neighbors {
		level := len(layers) - 1
		if level <= maxLevel {
			continue
		}
		vector, norm, valid := rows.resolve(node)
		if !valid || len(vector) != idx.dims || (rows.needNorm && norm == 0) {
			continue
		}
		entry, maxLevel = node, level
	}
	return entry, maxLevel
}
