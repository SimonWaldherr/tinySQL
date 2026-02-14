package pager

import (
	"encoding/json"
	"fmt"
)

// ───────────────────────────────────────────────────────────────────────────
// Garbage Collector (VACUUM)
// ───────────────────────────────────────────────────────────────────────────
//
// The GC performs a reachability scan over all pages in the database. It
// starts from the known roots (superblock → catalog B+Tree → table B+Trees)
// and marks every reachable page. Any allocated page that was not visited
// is an orphan and gets added to the free-list.
//
// This reclaims pages lost to:
//   • DeleteTable that forgot to free the B+Tree (historical bug, now fixed)
//   • Crash during SaveTable (partially-built tree pages)
//   • Overflow chains orphaned by key updates (historical bug, now fixed)
//   • Aborted transactions that allocated pages before rolling back
//   • Free-list chain pages leaked at checkpoint (historical bug, now fixed)

// GCResult holds statistics about a garbage collection run.
type GCResult struct {
	TotalPages     int      // total allocated pages in the file
	ReachablePages int      // pages reachable from roots
	FreeBefore     int      // free pages before GC
	FreeAfter      int      // free pages after GC
	Reclaimed      int      // newly freed orphan pages
	Errors         []string // non-fatal issues found during the scan
}

// GC performs a full reachability-based garbage collection on the database.
// It must be called when no other writers are active (exclusive access).
// The GC does NOT shrink the file — it only adds orphans to the free-list
// so they can be reused by future writes.
func (pb *PageBackend) GC() (*GCResult, error) {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	sb := pb.pager.Superblock()
	totalPages := int(sb.NextPageID) // NextPageID = high-water mark
	if totalPages < 1 {
		return &GCResult{}, nil
	}

	result := &GCResult{
		TotalPages: totalPages,
		FreeBefore: pb.pager.freeMgr.Count(),
	}

	// Build the set of reachable pages.
	reachable := make(map[PageID]struct{}, totalPages)

	// 1. Superblock is always page 0.
	reachable[0] = struct{}{}

	// 2. Walk the catalog B+Tree.
	catalogRoot := sb.CatalogRoot
	if catalogRoot != InvalidPageID {
		pb.walkBTree(catalogRoot, reachable, result)
	}

	// 3. For each table in the catalog, walk its B+Tree.
	tableRoots, err := pb.collectTableRoots()
	if err != nil {
		result.Errors = append(result.Errors, fmt.Sprintf("catalog scan: %v", err))
	}
	for _, rootID := range tableRoots {
		pb.walkBTree(rootID, reachable, result)
	}

	// 4. Walk the free-list chain (those pages are "in-use" by the free-list
	//    structure itself, even though they track free pages).
	pb.walkFreeListChain(sb.FreeListRoot, reachable)

	result.ReachablePages = len(reachable)

	// 5. Find orphans: allocated pages that are not reachable and not
	//    already on the free-list.
	freeSet := make(map[PageID]struct{})
	for _, pid := range pb.pager.freeMgr.AllFree() {
		freeSet[pid] = struct{}{}
	}

	var reclaimed int
	for pid := PageID(0); pid < PageID(totalPages); pid++ {
		if _, isReachable := reachable[pid]; isReachable {
			continue
		}
		if _, isFree := freeSet[pid]; isFree {
			continue
		}
		// Orphan found — add to free-list.
		pb.pager.freeMgr.Free(pid)
		reclaimed++
	}

	result.Reclaimed = reclaimed
	result.FreeAfter = pb.pager.freeMgr.Count()

	// If we reclaimed pages, checkpoint to persist the updated free-list.
	if reclaimed > 0 {
		if err := pb.pager.Checkpoint(); err != nil {
			result.Errors = append(result.Errors, fmt.Sprintf("checkpoint: %v", err))
		}
	}

	return result, nil
}

// walkBTree recursively marks all pages of a B+Tree as reachable.
func (pb *PageBackend) walkBTree(rootID PageID, reachable map[PageID]struct{}, result *GCResult) {
	pb.walkBTreePage(rootID, reachable, result)
}

func (pb *PageBackend) walkBTreePage(pid PageID, reachable map[PageID]struct{}, result *GCResult) {
	if pid == InvalidPageID {
		return
	}
	if _, seen := reachable[pid]; seen {
		return // already visited (cycle protection)
	}
	reachable[pid] = struct{}{}

	buf, err := pb.pager.ReadPage(pid)
	if err != nil {
		result.Errors = append(result.Errors, fmt.Sprintf("read page %d: %v", pid, err))
		return
	}
	defer pb.pager.UnpinPage(pid)

	bp := WrapBTreePage(buf)
	if bp.IsLeaf() {
		// Walk all entries — mark overflow chains as reachable.
		sc := bp.slotCount()
		for i := 0; i < sc; i++ {
			entry := bp.GetLeafEntry(i)
			if entry.Overflow {
				pb.walkOverflowChain(entry.OverflowPageID, reachable, result)
			}
		}
		// Mark next/prev leaf siblings — they'll be visited in their own
		// internal-node subtree walk, so we don't recurse into them here.
		return
	}

	// Internal node — recurse into all children.
	sc := bp.slotCount()
	for i := 0; i < sc; i++ {
		ie := bp.GetInternalEntry(i)
		pb.walkBTreePage(ie.ChildID, reachable, result)
	}
	pb.walkBTreePage(bp.RightChild(), reachable, result)
}

func (pb *PageBackend) walkOverflowChain(headID PageID, reachable map[PageID]struct{}, result *GCResult) {
	pid := headID
	for pid != InvalidPageID {
		if _, seen := reachable[pid]; seen {
			break
		}
		reachable[pid] = struct{}{}

		buf, err := pb.pager.ReadPage(pid)
		if err != nil {
			result.Errors = append(result.Errors, fmt.Sprintf("read overflow %d: %v", pid, err))
			return
		}
		op := WrapOverflowPage(buf)
		next := op.NextOverflow()
		pb.pager.UnpinPage(pid)
		pid = next
	}
}

func (pb *PageBackend) walkFreeListChain(headID PageID, reachable map[PageID]struct{}) {
	pid := headID
	for pid != InvalidPageID {
		if _, seen := reachable[pid]; seen {
			break
		}
		reachable[pid] = struct{}{}

		buf, err := pb.pager.ReadPage(pid)
		if err != nil {
			break
		}
		fl := WrapFreeListPage(buf)
		next := fl.NextFreeList()
		pb.pager.UnpinPage(pid)
		pid = next
	}
}

// collectTableRoots reads all catalog entries and returns their root page IDs.
func (pb *PageBackend) collectTableRoots() ([]PageID, error) {
	var roots []PageID
	// Scan the entire catalog tree.
	err := pb.catalog.tree.ScanRange([]byte{0}, nil, func(key, val []byte) bool {
		var entry CatalogEntry
		if err := json.Unmarshal(val, &entry); err != nil {
			return true // skip broken entries
		}
		roots = append(roots, entry.RootPageID)
		return true
	})
	return roots, err
}
