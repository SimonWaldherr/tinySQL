package pager

import (
	"bytes"
	"fmt"
)

// ───────────────────────────────────────────────────────────────────────────
// BTree — transactional B+Tree built on top of the Pager
// ───────────────────────────────────────────────────────────────────────────
//
// This is the main key-value API. Each B+Tree is identified by its root
// page ID (persisted in the system catalog or superblock). All mutations
// happen within a transaction (txID) and are WAL-logged automatically.

// BTree represents a B+Tree stored in the pager.
type BTree struct {
	pager          *Pager
	root           PageID
	overflowThresh int // preferred maximum encoded inline leaf record footprint
}

// NewBTree creates a handle to an existing B+Tree with the given root.
// For a new tree, call CreateBTree first.
func NewBTree(p *Pager, root PageID) *BTree {
	return &BTree{
		pager:          p,
		root:           root,
		overflowThresh: overflowThresholdFor(p.pageSize),
	}
}

// overflowThresholdFor computes the preferred maximum encoded inline record
// footprint. It deliberately reserves most of a leaf page for neighbouring
// records, while Insert still makes the final decision from the complete wire
// representation (key, headers, value/reference and slot entry).
func overflowThresholdFor(pageSize int) int {
	t := btreeRecordCapacity(pageSize) / 4
	if t < 256 {
		t = 256
	}
	return t
}

// CreateBTree allocates a new B+Tree with an empty leaf root page.
// Must be called within a transaction.
func CreateBTree(p *Pager, txID TxID) (*BTree, error) {
	rootID, rootBuf := p.AllocPage()
	InitBTreePage(rootBuf, rootID, true)
	SetPageCRC(rootBuf)
	if err := p.WritePage(txID, rootID, rootBuf); err != nil {
		return nil, err
	}
	p.UnpinPage(rootID)
	return &BTree{pager: p, root: rootID, overflowThresh: overflowThresholdFor(p.pageSize)}, nil
}

// Root returns the root page ID.
func (bt *BTree) Root() PageID { return bt.root }

// ── Search ────────────────────────────────────────────────────────────────

// Get looks up a key. Returns (value, true) or (nil, false).
// Handles overflow pages transparently.
func (bt *BTree) Get(key []byte) ([]byte, bool, error) {
	leafID, buf, err := bt.findLeafPinned(key)
	if err != nil {
		return nil, false, err
	}
	defer bt.pager.UnpinPage(leafID)

	bp := WrapBTreePage(buf)
	pos, found := bp.FindLeafEntry(key)
	if !found {
		return nil, false, nil
	}
	value, overflow, overflowPageID, totalSize := bp.leafValueAt(pos)
	if overflow {
		// readOverflow already returns query-owned memory. Returning it directly
		// avoids the second full-value copy formerly made through GetValue.
		value, err = bt.readOverflow(overflowPageID, totalSize)
		return value, true, err
	}
	// Inline values point into the pinned page cache and therefore still need
	// one defensive copy before the leaf is unpinned.
	return append([]byte(nil), value...), true, nil
}

// Contains reports whether key exists without loading or assembling its value.
// This matters for large overflow-backed rows when an index validator only
// needs to prove that a row locator resolves to a physical table entry.
func (bt *BTree) Contains(key []byte) (bool, error) {
	leafID, buf, err := bt.findLeafPinned(key)
	if err != nil {
		return false, err
	}
	defer bt.pager.UnpinPage(leafID)
	_, found := WrapBTreePage(buf).FindLeafEntry(key)
	return found, nil
}

// GetValue resolves key and invokes visit while the containing page remains
// pinned. Inline values are views into that page and must not be retained by
// visit. Overflow values are assembled into a temporary slice. This allows
// index locators and row codecs to decode directly from page memory without
// allocating a throw-away copy for every B+Tree comparison.
func (bt *BTree) GetValue(key []byte, visit func(value []byte) error) (bool, error) {
	return bt.getValue(key, func(value []byte, _ bool) error {
		return visit(value)
	})
}

// getValue is the ownership-aware variant used by typed row decoders. owned
// is true only for an assembled overflow value; inline values still point into
// the pinned leaf page for the duration of visit.
func (bt *BTree) getValue(key []byte, visit func(value []byte, owned bool) error) (bool, error) {
	leafID, buf, err := bt.findLeafPinned(key)
	if err != nil {
		return false, err
	}
	defer bt.pager.UnpinPage(leafID)

	bp := WrapBTreePage(buf)
	pos, found := bp.FindLeafEntry(key)
	if !found {
		return false, nil
	}
	value, overflow, overflowPageID, totalSize := bp.leafValueAt(pos)
	if overflow {
		value, err = bt.readOverflow(overflowPageID, totalSize)
		if err != nil {
			return false, err
		}
	}
	if err := visit(value, overflow); err != nil {
		return false, err
	}
	return true, nil
}

// findLeaf traverses from root to the leaf page containing key.
func (bt *BTree) findLeaf(key []byte) (PageID, error) {
	pageID, _, err := bt.findLeafPinned(key)
	if err != nil {
		return InvalidPageID, err
	}
	bt.pager.UnpinPage(pageID)
	return pageID, nil
}

// findLeafPinned traverses to the leaf containing key and leaves that leaf
// pinned for the caller. Point lookups use it to avoid re-reading the leaf
// merely to inspect the record after the traversal has already reached it.
func (bt *BTree) findLeafPinned(key []byte) (PageID, []byte, error) {
	pageID := bt.root
	for {
		buf, err := bt.pager.ReadPage(pageID)
		if err != nil {
			return InvalidPageID, nil, err
		}
		bp := WrapBTreePage(buf)
		if bp.IsLeaf() {
			return pageID, buf, nil
		}
		child := bp.SearchInternal(key)
		bt.pager.UnpinPage(pageID)
		pageID = child
	}
}

// ── Insert ────────────────────────────────────────────────────────────────

// Insert adds or updates a key-value pair within the given transaction.
func (bt *BTree) Insert(txID TxID, key, value []byte) error {
	entry, err := bt.makeLeafEntry(txID, key, value)
	if err != nil {
		return err
	}

	return bt.insertIntoTree(txID, key, entry)
}

// leafEntryNeedsOverflow reports whether key/value should be stored as an
// overflow reference rather than inline in a leaf record, using the complete
// encoded record size for both representations. Value length alone is
// insufficient: a long key plus headers and the record's own slot-directory
// entry can make an otherwise modest BLOB impossible to place on a leaf page.
//
// The decision depends only on pageSize, key and value -- never on how full
// any particular destination page happens to be right now. A record that
// would only overflow because the current page is already busy needs a
// split, not an overflow: basing the choice on live free space would make
// overflow placement depend on insertion order and page-fill history, so the
// same row could round-trip through overflow on one import and stay inline
// on another. Every accepted inline record must therefore be sized against
// an *empty* page (btreeRecordCapacity), which is exactly what
// leafRecordFootprint against that capacity guarantees.
func leafEntryNeedsOverflow(pageSize int, key, value []byte) bool {
	capacity := btreeRecordCapacity(pageSize)
	inlineSize := leafRecordFootprint(LeafEntry{Key: key, Value: value})
	if inlineSize > capacity {
		// Cannot be placed inline under any circumstance, even alone on an
		// otherwise empty page. This is not a preference: it is the only way
		// the record could ever be stored, short of erroring out entirely.
		return true
	}
	if inlineSize <= overflowThresholdFor(pageSize) {
		return false
	}
	// Above the preferred threshold but still inline-able: only actually
	// switch to overflow if the reference form is both storable and smaller,
	// so a modest value with a very long key is not needlessly pushed out.
	overflowSize := leafRecordFootprint(LeafEntry{Key: key, Overflow: true, TotalSize: uint32(len(value))})
	return overflowSize <= capacity && overflowSize < inlineSize
}

// makeLeafEntry builds the inline or overflow leaf representation for
// key/value, writing the overflow chain when leafEntryNeedsOverflow says to.
func (bt *BTree) makeLeafEntry(txID TxID, key, value []byte) (LeafEntry, error) {
	if len(key) > int(^uint16(0)) {
		return LeafEntry{}, fmt.Errorf("btree key too large: %d bytes", len(key))
	}
	capacity := btreeRecordCapacity(bt.pager.pageSize)

	if leafEntryNeedsOverflow(bt.pager.pageSize, key, value) {
		overflow := LeafEntry{Key: key, Overflow: true, TotalSize: uint32(len(value))}
		if overflowSize := leafRecordFootprint(overflow); overflowSize <= capacity {
			head, err := bt.writeOverflow(txID, value)
			if err != nil {
				return LeafEntry{}, err
			}
			overflow.OverflowPageID = head
			return overflow, nil
		}
		// The overflow reference itself does not fit -- only possible with an
		// enormous key -- so fall through to the inline check below, which
		// reports a precise error if inline does not fit either.
	}

	inline := LeafEntry{Key: key, Value: value}
	inlineSize := leafRecordFootprint(inline)
	if inlineSize <= capacity {
		return inline, nil
	}
	return LeafEntry{}, fmt.Errorf("btree leaf record too large: encoded %d bytes exceeds page capacity %d", inlineSize, capacity)
}

func (bt *BTree) insertIntoTree(txID TxID, key []byte, entry LeafEntry) error {
	// Walk down to find the leaf.
	path, err := bt.pathToLeaf(key)
	if err != nil {
		return err
	}

	leafID := path[len(path)-1]
	buf, err := bt.pager.ReadPage(leafID)
	if err != nil {
		return err
	}
	bp := WrapBTreePage(buf)

	// Check for existing key — update in place.
	pos, found := bp.FindLeafEntry(key)
	if found {
		// Free old overflow chain if the existing entry was overflow-stored.
		oldEntry := bp.GetLeafEntry(pos)
		if oldEntry.Overflow {
			bt.freeOverflowChain(oldEntry.OverflowPageID)
		}
		if err := bp.UpdateLeafEntry(pos, entry); err != nil {
			// Page full on update — need to split.
			bt.pager.UnpinPage(leafID)
			return bt.insertWithSplit(txID, path, entry)
		}
		SetPageCRC(buf)
		bt.pager.UnpinPage(leafID)
		return bt.pager.WritePage(txID, leafID, buf)
	}

	// New key — try inserting.
	if _, err := bp.InsertLeafEntry(entry); err != nil {
		// Leaf full — need split.
		bt.pager.UnpinPage(leafID)
		return bt.insertWithSplit(txID, path, entry)
	}

	SetPageCRC(buf)
	bt.pager.UnpinPage(leafID)
	return bt.pager.WritePage(txID, leafID, buf)
}

func (bt *BTree) insertWithSplit(txID TxID, path []PageID, entry LeafEntry) error {
	// Read the full leaf.
	leafID := path[len(path)-1]
	buf, err := bt.pager.ReadPage(leafID)
	if err != nil {
		return err
	}
	bp := WrapBTreePage(buf)

	// Collect all entries + new entry, sorted.
	entries := bp.GetAllLeafEntries()
	inserted := false
	merged := make([]LeafEntry, 0, len(entries)+1)
	for _, e := range entries {
		if !inserted && bytes.Compare(entry.Key, e.Key) <= 0 {
			merged = append(merged, entry)
			inserted = true
		}
		if bytes.Equal(e.Key, entry.Key) {
			// Replace existing — free old overflow chain if any.
			if e.Overflow {
				bt.freeOverflowChain(e.OverflowPageID)
			}
			continue
		}
		merged = append(merged, e)
	}
	if !inserted {
		merged = append(merged, entry)
	}

	// The page may have failed to hold merged's entries only because
	// UpdateLeafEntry/InsertLeafEntry never reclaim a record's old slot when
	// it moves or is replaced -- dead space accumulates until the page is
	// rewritten from scratch, which is exactly what a split's "left" half
	// does below. Before paying for an actual split (a new sibling page and a
	// parent-separator update), check whether the *live* entries, freshly
	// packed with no dead space, still fit in one page; if so, rewrite this
	// leaf in place and stop. This is not an optimization only: a leaf with a
	// single entry has nowhere to send a "right" half at all, so
	// leafSplitIndex below would fail outright on the very case a bare
	// replace-of-the-only-key produces (merged has exactly one entry).
	capacity := btreeRecordCapacity(bt.pager.pageSize)
	mergedFootprint := 0
	for _, e := range merged {
		mergedFootprint += leafRecordFootprint(e)
	}
	if mergedFootprint <= capacity {
		freshBuf := make([]byte, bt.pager.pageSize)
		freshBP := InitBTreePage(freshBuf, leafID, true)
		for _, e := range merged {
			if _, err := freshBP.InsertLeafEntry(e); err != nil {
				bt.pager.UnpinPage(leafID)
				return fmt.Errorf("leaf compaction insert: %w", err)
			}
		}
		freshBP.SetNextLeaf(bp.NextLeaf())
		freshBP.SetPrevLeaf(bp.PrevLeaf())
		SetPageCRC(freshBuf)
		bt.pager.UnpinPage(leafID)
		return bt.pager.WritePage(txID, leafID, freshBuf)
	}

	// Split by encoded bytes, not entry count. A count midpoint can put several
	// large records on the right and fail after the old page has already been
	// proved to fit.
	mid, err := bt.leafSplitIndex(merged)
	if err != nil {
		bt.pager.UnpinPage(leafID)
		return err
	}
	leftEntries := merged[:mid]
	rightEntries := merged[mid:]
	splitKey := rightEntries[0].Key

	// Rewrite left leaf (reuse old page).
	leftBuf := make([]byte, bt.pager.pageSize)
	leftBP := InitBTreePage(leftBuf, leafID, true)
	for _, e := range leftEntries {
		if _, err := leftBP.InsertLeafEntry(e); err != nil {
			return fmt.Errorf("split left insert: %w", err)
		}
	}

	// Allocate right leaf.
	rightID, rightBuf := bt.pager.AllocPage()
	rightBP := InitBTreePage(rightBuf, rightID, true)
	for _, e := range rightEntries {
		if _, err := rightBP.InsertLeafEntry(e); err != nil {
			return fmt.Errorf("split right insert: %w", err)
		}
	}

	// Link siblings.
	oldNext := bp.NextLeaf()
	leftBP.SetNextLeaf(rightID)
	leftBP.SetPrevLeaf(bp.PrevLeaf())
	rightBP.SetPrevLeaf(leafID)
	rightBP.SetNextLeaf(oldNext)

	// Write both leaves.
	SetPageCRC(leftBuf)
	if err := bt.pager.WritePage(txID, leafID, leftBuf); err != nil {
		return err
	}
	SetPageCRC(rightBuf)
	if err := bt.pager.WritePage(txID, rightID, rightBuf); err != nil {
		return err
	}
	bt.pager.UnpinPage(leafID)
	bt.pager.UnpinPage(rightID)

	// Update next leaf's prevLeaf pointer if it exists.
	if oldNext != InvalidPageID {
		nextBuf, err := bt.pager.ReadPage(oldNext)
		if err == nil {
			nextBP := WrapBTreePage(nextBuf)
			nextBP.SetPrevLeaf(rightID)
			SetPageCRC(nextBuf)
			if err := bt.pager.WritePage(txID, oldNext, nextBuf); err != nil {
				bt.pager.UnpinPage(oldNext)
				return err
			}
			bt.pager.UnpinPage(oldNext)
		}
	}

	// Propagate split upward.
	return bt.insertIntoParent(txID, path[:len(path)-1], leafID, splitKey, rightID)
}

func (bt *BTree) leafSplitIndex(entries []LeafEntry) (int, error) {
	if len(entries) < 2 {
		return 0, fmt.Errorf("cannot split leaf with %d entries", len(entries))
	}
	capacity := btreeRecordCapacity(bt.pager.pageSize)
	total := 0
	for _, entry := range entries {
		size := leafRecordFootprint(entry)
		if size > capacity {
			return 0, fmt.Errorf("leaf record footprint %d exceeds page capacity %d", size, capacity)
		}
		total += size
	}

	leftBytes := 0
	best, bestSkew := -1, 0
	for split := 1; split < len(entries); split++ {
		leftBytes += leafRecordFootprint(entries[split-1])
		rightBytes := total - leftBytes
		if leftBytes > capacity || rightBytes > capacity {
			continue
		}
		skew := leftBytes - rightBytes
		if skew < 0 {
			skew = -skew
		}
		if best == -1 || skew < bestSkew {
			best, bestSkew = split, skew
		}
	}
	if best == -1 {
		return 0, fmt.Errorf("cannot byte-balance %d leaf records into %d-byte pages", len(entries), capacity)
	}
	return best, nil
}

func (bt *BTree) insertIntoParent(txID TxID, path []PageID, leftID PageID, key []byte, rightID PageID) error {
	if len(path) == 0 {
		// Need a new root.
		return bt.createNewRoot(txID, leftID, key, rightID)
	}

	parentID := path[len(path)-1]
	buf, err := bt.pager.ReadPage(parentID)
	if err != nil {
		return err
	}
	bp := WrapBTreePage(buf)
	entries, rightChild, err := internalEntriesAfterChildSplit(bp, leftID, key, rightID)
	bt.pager.UnpinPage(parentID)
	if err != nil {
		return err
	}

	// Rebuild from logical entries. This avoids fragmented slot space and keeps
	// the child pointer immediately right of the new separator correct.
	newBuf := make([]byte, bt.pager.pageSize)
	newBP := InitBTreePage(newBuf, parentID, false)
	if err := buildInternalPage(newBP, entries, rightChild); err == nil {
		SetPageCRC(newBuf)
		return bt.pager.WritePage(txID, parentID, newBuf)
	}
	return bt.splitInternal(txID, path, entries, rightChild)
}

func internalEntriesAfterChildSplit(bp *BTreePage, leftID PageID, key []byte, rightID PageID) ([]InternalEntry, PageID, error) {
	n := bp.slotCount()
	keys := make([][]byte, n)
	children := make([]PageID, n+1)
	for i := 0; i < n; i++ {
		entry := bp.GetInternalEntry(i)
		keys[i] = entry.Key
		children[i] = entry.ChildID
	}
	children[n] = bp.RightChild()

	pos := -1
	for i, child := range children {
		if child == leftID {
			pos = i
			break
		}
	}
	if pos == -1 {
		return nil, InvalidPageID, fmt.Errorf("parent does not reference split child %d", leftID)
	}
	if (pos > 0 && bytes.Compare(keys[pos-1], key) >= 0) || (pos < len(keys) && bytes.Compare(key, keys[pos]) >= 0) {
		return nil, InvalidPageID, fmt.Errorf("split separator %q is out of parent order", key)
	}

	keys = append(keys, nil)
	copy(keys[pos+1:], keys[pos:])
	keys[pos] = key
	children = append(children, InvalidPageID)
	copy(children[pos+2:], children[pos+1:])
	children[pos+1] = rightID

	entries := make([]InternalEntry, len(keys))
	for i := range keys {
		entries[i] = InternalEntry{ChildID: children[i], Key: keys[i]}
	}
	return entries, children[len(children)-1], nil
}

func buildInternalPage(bp *BTreePage, entries []InternalEntry, rightChild PageID) error {
	for _, entry := range entries {
		if err := bp.InsertInternalEntry(entry); err != nil {
			return err
		}
	}
	bp.SetRightChild(rightChild)
	return nil
}

// splitInternal handles inserting a new separator into an internal page
// that has no room for it (insertIntoParent's InsertInternalEntry call
// already failed once by the time this runs). It merges the existing
// entries with the new one in sorted order, splits the merged list at its
// midpoint into a reused left page (parentID) and a freshly allocated right
// page, and pushes the middle key one level up via a recursive
// insertIntoParent call — the standard B+tree internal-split algorithm.
func (bt *BTree) splitInternal(txID TxID, path []PageID, entries []InternalEntry, rightChild PageID) error {
	parentID := path[len(path)-1]
	mid, err := bt.internalSplitIndex(entries)
	if err != nil {
		return err
	}
	pushUpKey := entries[mid].Key
	leftEntries := entries[:mid]
	rightEntries := entries[mid+1:]
	leftRightChild := entries[mid].ChildID

	// Rewrite left internal (reuse parentID).
	leftBuf := make([]byte, bt.pager.pageSize)
	leftBP := InitBTreePage(leftBuf, parentID, false)
	if err := buildInternalPage(leftBP, leftEntries, leftRightChild); err != nil {
		return fmt.Errorf("split internal left: %w", err)
	}

	// Allocate right internal.
	newRightID, rightBuf := bt.pager.AllocPage()
	rightInternalBP := InitBTreePage(rightBuf, newRightID, false)
	if err := buildInternalPage(rightInternalBP, rightEntries, rightChild); err != nil {
		return fmt.Errorf("split internal right: %w", err)
	}

	// Write both pages.
	SetPageCRC(leftBuf)
	if err := bt.pager.WritePage(txID, parentID, leftBuf); err != nil {
		return err
	}
	SetPageCRC(rightBuf)
	if err := bt.pager.WritePage(txID, newRightID, rightBuf); err != nil {
		return err
	}
	bt.pager.UnpinPage(newRightID)

	// Push separator up.
	return bt.insertIntoParent(txID, path[:len(path)-1], parentID, pushUpKey, newRightID)
}

func (bt *BTree) internalSplitIndex(entries []InternalEntry) (int, error) {
	if len(entries) < 3 {
		return 0, fmt.Errorf("cannot split internal page with %d entries", len(entries))
	}
	capacity := btreeRecordCapacity(bt.pager.pageSize)
	total := 0
	for _, entry := range entries {
		size := internalRecordFootprint(entry)
		if size > capacity {
			return 0, fmt.Errorf("internal record footprint %d exceeds page capacity %d", size, capacity)
		}
		total += size
	}

	leftBytes := 0
	best, bestSkew := -1, 0
	for mid := 1; mid < len(entries)-1; mid++ {
		leftBytes += internalRecordFootprint(entries[mid-1])
		rightBytes := total - leftBytes - internalRecordFootprint(entries[mid])
		if leftBytes > capacity || rightBytes > capacity {
			continue
		}
		skew := leftBytes - rightBytes
		if skew < 0 {
			skew = -skew
		}
		if best == -1 || skew < bestSkew {
			best, bestSkew = mid, skew
		}
	}
	if best == -1 {
		return 0, fmt.Errorf("cannot byte-balance %d internal records into %d-byte pages", len(entries), capacity)
	}
	return best, nil
}

func (bt *BTree) createNewRoot(txID TxID, leftID PageID, key []byte, rightID PageID) error {
	rootID, rootBuf := bt.pager.AllocPage()
	rootBP := InitBTreePage(rootBuf, rootID, false)
	if err := rootBP.InsertInternalEntry(InternalEntry{ChildID: leftID, Key: key}); err != nil {
		return err
	}
	rootBP.SetRightChild(rightID)
	SetPageCRC(rootBuf)
	if err := bt.pager.WritePage(txID, rootID, rootBuf); err != nil {
		return err
	}
	bt.pager.UnpinPage(rootID)
	bt.root = rootID
	return nil
}

// ── Delete ────────────────────────────────────────────────────────────────

// Delete removes a key from the B+Tree.
func (bt *BTree) Delete(txID TxID, key []byte) (bool, error) {
	leafID, err := bt.findLeaf(key)
	if err != nil {
		return false, err
	}
	buf, err := bt.pager.ReadPage(leafID)
	if err != nil {
		return false, err
	}
	bp := WrapBTreePage(buf)

	pos, found := bp.FindLeafEntry(key)
	if !found {
		bt.pager.UnpinPage(leafID)
		return false, nil
	}

	// If overflow, free overflow pages.
	entry := bp.GetLeafEntry(pos)
	if entry.Overflow {
		bt.freeOverflowChain(entry.OverflowPageID)
	}

	if err := bp.DeleteLeafEntry(pos); err != nil {
		bt.pager.UnpinPage(leafID)
		return false, err
	}

	SetPageCRC(buf)
	bt.pager.UnpinPage(leafID)
	if err := bt.pager.WritePage(txID, leafID, buf); err != nil {
		return false, err
	}
	return true, nil
}

// ── Range scan ────────────────────────────────────────────────────────────

// ScanRange calls fn for each key-value pair where startKey <= key <= endKey.
// If endKey is nil, scans to the end. If fn returns false, the scan stops.
func (bt *BTree) ScanRange(startKey, endKey []byte, fn func(key, value []byte) bool) error {
	return bt.scanRange(startKey, endKey, func(key, value []byte, _ bool) bool {
		return fn(key, value)
	})
}

// scanRange is the ownership-aware scan used by row decoders. owned is true
// for an assembled overflow value and false for an inline leaf-page view.
func (bt *BTree) scanRange(startKey, endKey []byte, fn func(key, value []byte, owned bool) bool) error {
	leafID, err := bt.findLeaf(startKey)
	if err != nil {
		return err
	}

	for leafID != InvalidPageID {
		buf, err := bt.pager.ReadPage(leafID)
		if err != nil {
			return err
		}
		bp := WrapBTreePage(buf)
		sc := bp.slotCount()

		for i := 0; i < sc; i++ {
			// leafKeyAt/leafValueAt are the same zero-copy page views Get/
			// getValue already use for point lookups: GetLeafEntry
			// unconditionally deep-copies the whole record (see
			// unmarshalLeafRecord), which every scanRange caller here either
			// discards after this call or explicitly copies itself
			// (unmarshalRow's copyBytes path, or an explicit append/string
			// conversion) -- so that copy was pure waste on top of the one a
			// caller that actually needs to retain the data already makes.
			// Safe because the page stays pinned for this whole per-leaf
			// loop, matching the "owned" contract already documented above.
			key := bp.leafKeyAt(i)
			if bytes.Compare(key, startKey) < 0 {
				continue
			}
			if endKey != nil && bytes.Compare(key, endKey) > 0 {
				bt.pager.UnpinPage(leafID)
				return nil
			}
			val, overflow, overflowPageID, totalSize := bp.leafValueAt(i)
			if overflow {
				val, err = bt.readOverflow(overflowPageID, totalSize)
				if err != nil {
					bt.pager.UnpinPage(leafID)
					return err
				}
			}
			if !fn(key, val, overflow) {
				bt.pager.UnpinPage(leafID)
				return nil
			}
		}

		nextLeaf := bp.NextLeaf()
		bt.pager.UnpinPage(leafID)
		leafID = nextLeaf
	}
	return nil
}

// ── Overflow chain I/O ───────────────────────────────────────────────────

// writeOverflow splits data into page-sized chunks and chains them into a
// singly-linked list of overflow pages (for values too large to fit inline
// in a leaf record), returning the head page ID. Each page's NextOverflow
// pointer must reference the *next* page's ID, which isn't known until that
// next page is allocated — so the loop lags one page behind: it allocates
// page N, then (on the following iteration, once page N+1 exists) sets page
// N's NextOverflow to N+1's ID and writes page N to disk. prevBuf/prevID
// hold that one-page-behind state. The final page has no successor, so it's
// written once after the loop exits instead of inside it.
func (bt *BTree) writeOverflow(txID TxID, data []byte) (PageID, error) {
	cap := OverflowCapacity(bt.pager.pageSize)
	var headID PageID
	var prevBuf []byte
	var prevID PageID

	for off := 0; off < len(data); off += cap {
		end := off + cap
		if end > len(data) {
			end = len(data)
		}
		chunk := data[off:end]

		pid, buf := bt.pager.AllocPage()
		op := InitOverflowPage(buf, pid)
		if err := op.SetData(chunk); err != nil {
			return 0, err
		}

		if prevBuf != nil {
			prevOP := WrapOverflowPage(prevBuf)
			prevOP.SetNextOverflow(pid)
			SetPageCRC(prevBuf)
			if err := bt.pager.WritePage(txID, prevID, prevBuf); err != nil {
				return 0, err
			}
			bt.pager.UnpinPage(prevID)
		} else {
			headID = pid
		}

		prevBuf = buf
		prevID = pid
	}

	// The last page in the chain was never written above (its NextOverflow
	// is left at the zero value, InvalidPageID, correctly marking chain end).
	if prevBuf != nil {
		SetPageCRC(prevBuf)
		if err := bt.pager.WritePage(txID, prevID, prevBuf); err != nil {
			return 0, err
		}
		bt.pager.UnpinPage(prevID)
	}

	return headID, nil
}

const overflowReadBatchPages = 32

func (bt *BTree) readOverflow(headID PageID, totalSize uint32) ([]byte, error) {
	maxInt := int(^uint(0) >> 1)
	if uint64(totalSize) > uint64(maxInt) {
		return nil, fmt.Errorf("overflow value size %d does not fit in memory", totalSize)
	}
	targetSize := int(totalSize)
	result := make([]byte, 0, targetSize)
	pid := headID
	// Only the first, cold overflow page needs the cache-affinity check. Once
	// a chain is warm, retain the normal pager path without taking an extra
	// buffer-pool lock for every following overflow page.
	coalesceColdTail := bt.pager.readOnly && !bt.pager.pageCached(headID)
	for pid != InvalidPageID {
		// Do the first page through the normal pager so it is available to a
		// following warm lookup. A physically contiguous cold tail can then be
		// consumed in a few ReadAt calls without sacrificing the normal cache
		// path on later requests.
		buf, err := bt.pager.ReadPage(pid)
		if err != nil {
			return nil, err
		}
		data, next, err := overflowPageContents(pid, buf)
		bt.pager.UnpinPage(pid)
		if err != nil {
			return nil, err
		}
		if len(data) > targetSize-len(result) {
			return nil, fmt.Errorf("overflow chain %d exceeds declared size %d", headID, totalSize)
		}
		result = append(result, data...)
		if len(result) == targetSize {
			if next != InvalidPageID {
				return nil, fmt.Errorf("overflow chain %d exceeds declared size %d", headID, totalSize)
			}
			return result, nil
		}

		// Immutable artifacts commonly allocate BLOB overflow pages in one
		// contiguous run. Coalesce only a cold tail; mutable pagers must retain
		// transaction/cache semantics, and warm tails stay on the existing LRU
		// path. A non-contiguous chain cleanly falls back to one-page reads.
		if coalesceColdTail && next == pid+1 {
			pid, err = bt.readContiguousOverflowTail(next, targetSize, &result)
			if err != nil {
				return nil, err
			}
			coalesceColdTail = false
			continue
		}
		pid = next
	}
	if len(result) != targetSize {
		return nil, fmt.Errorf("overflow chain %d has %d bytes, want %d", headID, len(result), totalSize)
	}
	return result, nil
}

// readContiguousOverflowTail advances through a physically adjacent overflow
// run. It only validates pages actually reached by the logical next pointer;
// a non-adjacent successor simply returns to the ordinary pager path.
func (bt *BTree) readContiguousOverflowTail(pid PageID, targetSize int, result *[]byte) (PageID, error) {
	capacity := OverflowCapacity(bt.pager.pageSize)
	for pid != InvalidPageID && len(*result) < targetSize {
		remainingPages := (targetSize - len(*result) + capacity - 1) / capacity
		batchPages := min(remainingPages, overflowReadBatchPages)
		buf, err := bt.pager.readContiguousPagesRaw(pid, batchPages)
		if err != nil {
			// The physical read can reach EOF when a logically valid chain
			// jumps elsewhere. Revert to the established one-page path, which
			// remains the correctness fallback for every non-contiguous case.
			return pid, nil
		}
		batchStart := pid
		for index := 0; index < batchPages; index++ {
			pageID := PageID(uint64(batchStart) + uint64(index))
			page := buf[index*bt.pager.pageSize : (index+1)*bt.pager.pageSize]
			if err := VerifyPageCRC(page); err != nil {
				return InvalidPageID, err
			}
			data, next, err := overflowPageContents(pageID, page)
			if err != nil {
				return InvalidPageID, err
			}
			if len(data) > targetSize-len(*result) {
				return InvalidPageID, fmt.Errorf("overflow chain exceeds declared size %d", targetSize)
			}
			*result = append(*result, data...)
			if len(*result) == targetSize {
				if next != InvalidPageID {
					return InvalidPageID, fmt.Errorf("overflow chain exceeds declared size %d", targetSize)
				}
				return InvalidPageID, nil
			}
			if next != pageID+1 {
				return next, nil
			}
			pid = next
		}
	}
	return pid, nil
}

func overflowPageContents(id PageID, buf []byte) ([]byte, PageID, error) {
	if len(buf) < overflowDataOff {
		return nil, InvalidPageID, fmt.Errorf("overflow page %d is shorter than its header", id)
	}
	header := UnmarshalHeader(buf)
	if header.Type != PageTypeOverflow {
		return nil, InvalidPageID, fmt.Errorf("page %d has type %s, want overflow", id, header.Type)
	}
	if header.ID != id {
		return nil, InvalidPageID, fmt.Errorf("overflow page id %d, want %d", header.ID, id)
	}
	op := WrapOverflowPage(buf)
	if dataLen := op.DataLen(); dataLen > OverflowCapacity(len(buf)) {
		return nil, InvalidPageID, fmt.Errorf("overflow page %d data length %d exceeds capacity", id, dataLen)
	}
	return op.Data(), op.NextOverflow(), nil
}

func (bt *BTree) freeOverflowChain(headID PageID) {
	pid := headID
	for pid != InvalidPageID {
		buf, err := bt.pager.ReadPage(pid)
		if err != nil {
			break
		}
		op := WrapOverflowPage(buf)
		next := op.NextOverflow()
		bt.pager.UnpinPage(pid)
		bt.pager.FreePage(pid)
		pid = next
	}
}

// FreeAllPages recursively frees every page owned by this B+Tree
// (internal nodes, leaf nodes, and overflow chains). After this call
// the tree is invalid and must not be used.
func (bt *BTree) FreeAllPages() {
	bt.freeSubtree(bt.root)
}

func (bt *BTree) freeSubtree(pid PageID) {
	if pid == InvalidPageID {
		return
	}
	buf, err := bt.pager.ReadPage(pid)
	if err != nil {
		return
	}
	bp := WrapBTreePage(buf)

	if bp.IsLeaf() {
		// Free any overflow chains in this leaf.
		sc := bp.slotCount()
		for i := 0; i < sc; i++ {
			entry := bp.GetLeafEntry(i)
			if entry.Overflow {
				bt.freeOverflowChain(entry.OverflowPageID)
			}
		}
		bt.pager.UnpinPage(pid)
		bt.pager.FreePage(pid)
		return
	}

	// Internal node — recurse into children.
	sc := bp.slotCount()
	children := make([]PageID, 0, sc+1)
	for i := 0; i < sc; i++ {
		ie := bp.GetInternalEntry(i)
		children = append(children, ie.ChildID)
	}
	children = append(children, bp.RightChild())
	bt.pager.UnpinPage(pid)

	for _, child := range children {
		bt.freeSubtree(child)
	}
	bt.pager.FreePage(pid)
}

// pathToLeaf returns the page IDs from root to the leaf containing key.
func (bt *BTree) pathToLeaf(key []byte) ([]PageID, error) {
	var path []PageID
	pageID := bt.root
	for {
		path = append(path, pageID)
		buf, err := bt.pager.ReadPage(pageID)
		if err != nil {
			return nil, err
		}
		bp := WrapBTreePage(buf)
		if bp.IsLeaf() {
			bt.pager.UnpinPage(pageID)
			return path, nil
		}
		child := bp.SearchInternal(key)
		bt.pager.UnpinPage(pageID)
		pageID = child
	}
}

// ── Count ─────────────────────────────────────────────────────────────────

// Count returns the total number of key-value pairs in the tree.
func (bt *BTree) Count() (int, error) {
	// Find leftmost leaf.
	pageID := bt.root
	for {
		buf, err := bt.pager.ReadPage(pageID)
		if err != nil {
			return 0, err
		}
		bp := WrapBTreePage(buf)
		if bp.IsLeaf() {
			bt.pager.UnpinPage(pageID)
			break
		}
		// Go to leftmost child.
		if bp.slotCount() > 0 {
			child := bp.GetInternalEntry(0).ChildID
			bt.pager.UnpinPage(pageID)
			pageID = child
		} else {
			child := bp.RightChild()
			bt.pager.UnpinPage(pageID)
			pageID = child
		}
	}

	// Walk the leaf chain.
	count := 0
	for pageID != InvalidPageID {
		buf, err := bt.pager.ReadPage(pageID)
		if err != nil {
			return 0, err
		}
		bp := WrapBTreePage(buf)
		count += bp.KeyCount()
		next := bp.NextLeaf()
		bt.pager.UnpinPage(pageID)
		pageID = next
	}
	return count, nil
}
