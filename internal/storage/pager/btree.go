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
	overflowThresh int // max inline value bytes
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

// overflowThresholdFor computes the max inline value size given page size.
func overflowThresholdFor(pageSize int) int {
	usable := pageSize - btreeSlotDirOff - 64 // rough overhead
	t := usable / 4
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
	leafID, err := bt.findLeaf(key)
	if err != nil {
		return nil, false, err
	}
	buf, err := bt.pager.ReadPage(leafID)
	if err != nil {
		return nil, false, err
	}
	defer bt.pager.UnpinPage(leafID)

	bp := WrapBTreePage(buf)
	pos, found := bp.FindLeafEntry(key)
	if !found {
		return nil, false, nil
	}
	entry := bp.GetLeafEntry(pos)
	if entry.Overflow {
		val, err := bt.readOverflow(entry.OverflowPageID, entry.TotalSize)
		if err != nil {
			return nil, false, err
		}
		return val, true, nil
	}
	return entry.Value, true, nil
}

// findLeaf traverses from root to the leaf page containing key.
func (bt *BTree) findLeaf(key []byte) (PageID, error) {
	pageID := bt.root
	for {
		buf, err := bt.pager.ReadPage(pageID)
		if err != nil {
			return 0, err
		}
		bp := WrapBTreePage(buf)
		if bp.IsLeaf() {
			bt.pager.UnpinPage(pageID)
			return pageID, nil
		}
		child := bp.SearchInternal(key)
		bt.pager.UnpinPage(pageID)
		pageID = child
	}
}

// ── Insert ────────────────────────────────────────────────────────────────

// Insert adds or updates a key-value pair within the given transaction.
func (bt *BTree) Insert(txID TxID, key, value []byte) error {
	entry := LeafEntry{Key: key}

	if len(value) > bt.overflowThresh {
		// Store as overflow.
		overflowHead, err := bt.writeOverflow(txID, value)
		if err != nil {
			return err
		}
		entry.Overflow = true
		entry.OverflowPageID = overflowHead
		entry.TotalSize = uint32(len(value))
	} else {
		entry.Value = value
	}

	return bt.insertIntoTree(txID, key, entry)
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
	var merged []LeafEntry
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

	// Split into two halves.
	mid := len(merged) / 2
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
			_ = bt.pager.WritePage(txID, oldNext, nextBuf)
			bt.pager.UnpinPage(oldNext)
		}
	}

	// Propagate split upward.
	return bt.insertIntoParent(txID, path[:len(path)-1], leafID, splitKey, rightID)
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

	newEntry := InternalEntry{ChildID: leftID, Key: key}
	if err := bp.InsertInternalEntry(newEntry); err != nil {
		// Parent full — split internal.
		bt.pager.UnpinPage(parentID)
		return bt.splitInternal(txID, path, leftID, key, rightID)
	}
	// The right child becomes the new child for the next separator (or RightChild).
	// We need to update pointers: after inserting, the right child pointer
	// for the newly inserted key should point to rightID.
	// Internal page layout: entry[i].childID points left of entry[i].key.
	// We set RightChild or the next entry's child based on position.
	// Actually, the entry's ChildID is the left pointer. The right pointer
	// is the ChildID of the next entry, or RightChild if it's the last.
	// Since our key goes between leftID and rightID, and we store leftID
	// in the entry, rightID needs to be stored either as the next entry's
	// child or as RightChild. Let's update RightChild if this new entry is last.
	sc := bp.slotCount()
	// Find where our key ended up.
	for i := 0; i < sc; i++ {
		e := bp.GetInternalEntry(i)
		if bytes.Equal(e.Key, key) {
			// entry[i].childID = leftID ✓
			// The pointer after key is:
			if i+1 < sc {
				// entry[i+1].childID should already be correct
				// unless it was the old "rightChild" that got pushed.
				// We need to ensure rightID is reachable.
				// For internal pages, between key[i] and key[i+1], the child
				// is entry[i+1].childID. So we need entry[i+1].childID = rightID?
				// No: entry[i+1].childID = pointer left of key[i+1].
				// If the old layout was: ... | leftID | oldKey | ... | RightChild
				// And we inserted key between leftID and the next, rightID should be
				// the next entry's child. But the old next entry still points to old child.
				// Actually, for a simple B+Tree internal split propagation:
				// We just need to make sure that after key, the traversal goes to rightID.
				// The simplest correct approach: set entry at position i to have child=leftID,
				// and update the next entry's child or RightChild to rightID.
				next := bp.GetInternalEntry(i + 1)
				// Save old child of next entry — it's still needed.
				oldChild := next.ChildID
				_ = oldChild
				// Actually — in a standard B+Tree, when we push up a separator:
				// old internal page: ... child0 | sep0 | child1 | sep1 | ... | RightChild
				// After split of child_x into leftID/rightID:
				// ... | leftID | key | rightID | ...
				// So we just replace: the child pointer that used to point to child_x
				// is now leftID, and rightID goes right of key.
				//
				// Since InsertInternalEntry put leftID as the key's left child and the
				// existing entries' children remain unchanged, we need to find the old
				// reference to the unsplit child and ensure rightID replaces it properly.
				//
				// Simplification: we put the entry with childID=leftID at pos i.
				// The right pointer of key is the childID of entry[i+1] or RightChild.
				// Before the insert, entry[i+1].childID was some other child — it should
				// now become rightID, and the old child should shift forward.
				// This is getting complex. Let's use a cleaner approach: rebuild entries.

				// For correctness in V1: just set the child pointer after key to rightID.
				// We do this by setting entry[i+1]'s child to rightID if i+1 < sc.
				// But we need to preserve the old entry[i+1].key. The old child at [i+1]
				// was correct for between key[i+1-1] and key[i+1] — but now key[i] is our
				// new separator. So entry[i+1].childID = rightID is correct.
				next.ChildID = rightID
				rec := marshalInternalRecord(next)
				bp.setSlotEntry(i+1, SlotEntry{})
				// Rewrite slot i+1.
				newEnd := bp.freeSpaceEnd() - len(rec)
				copy(bp.buf[newEnd:], rec)
				bp.setFreeSpaceEnd(newEnd)
				bp.setSlotEntry(i+1, SlotEntry{Offset: uint16(newEnd), Length: uint16(len(rec))})
			} else {
				// Last separator — set RightChild = rightID.
				bp.SetRightChild(rightID)
			}
			break
		}
	}

	SetPageCRC(buf)
	bt.pager.UnpinPage(parentID)
	return bt.pager.WritePage(txID, parentID, buf)
}

func (bt *BTree) splitInternal(txID TxID, path []PageID, leftChildID PageID, key []byte, rightChildID PageID) error {
	parentID := path[len(path)-1]
	buf, err := bt.pager.ReadPage(parentID)
	if err != nil {
		return err
	}
	bp := WrapBTreePage(buf)

	// Collect all entries + new entry.
	entries := bp.GetAllInternalEntries()
	oldRight := bp.RightChild()

	// Insert new entry in sorted order.
	newEntry := InternalEntry{ChildID: leftChildID, Key: key}
	var merged []InternalEntry
	inserted := false
	for _, e := range entries {
		if !inserted && bytes.Compare(key, e.Key) < 0 {
			merged = append(merged, newEntry)
			inserted = true
		}
		merged = append(merged, e)
	}
	if !inserted {
		merged = append(merged, newEntry)
	}

	// Split: left gets [0..mid-1], pushUpKey = merged[mid].Key, right gets [mid+1..].
	mid := len(merged) / 2
	pushUpKey := merged[mid].Key
	leftEntries := merged[:mid]
	rightEntries := merged[mid+1:]
	midChildRight := merged[mid].ChildID // this becomes the left part of push-up

	// Rewrite left internal (reuse parentID).
	leftBuf := make([]byte, bt.pager.pageSize)
	leftBP := InitBTreePage(leftBuf, parentID, false)
	for _, e := range leftEntries {
		if err := leftBP.InsertInternalEntry(e); err != nil {
			return fmt.Errorf("split internal left: %w", err)
		}
	}
	// The left internal's right child is the midChild's child.
	// Depending on where the insert happened, we need to handle rightChildID.
	// When the new entry's position was before mid:
	//   merged[mid].ChildID might be rightChildID or some old child.
	// For correctness: after inserting the new separator, the pointer right of key
	// should be rightChildID. Let's find where key landed:
	foundInLeft := false
	for _, e := range leftEntries {
		if bytes.Equal(e.Key, key) {
			foundInLeft = true
			break
		}
	}
	if bytes.Equal(pushUpKey, key) {
		// The new key IS the push-up key.
		leftBP.SetRightChild(leftChildID)
		// Right side starts with rightChildID.
		if len(rightEntries) > 0 {
			rightEntries[0] = InternalEntry{ChildID: rightChildID, Key: rightEntries[0].Key}
		}
	} else if foundInLeft {
		leftBP.SetRightChild(rightChildID)
	} else {
		leftBP.SetRightChild(midChildRight)
	}

	// Allocate right internal.
	newRightID, rightBuf := bt.pager.AllocPage()
	rightInternalBP := InitBTreePage(rightBuf, newRightID, false)
	for _, e := range rightEntries {
		if err := rightInternalBP.InsertInternalEntry(e); err != nil {
			return fmt.Errorf("split internal right: %w", err)
		}
	}
	rightInternalBP.SetRightChild(oldRight)

	// If the new key ended up in the right side, fix child pointers.
	if !foundInLeft && !bytes.Equal(pushUpKey, key) {
		// The new entry is in rightEntries; fix rightChildID placement.
		for i := 0; i < rightInternalBP.slotCount(); i++ {
			e := rightInternalBP.GetInternalEntry(i)
			if bytes.Equal(e.Key, key) {
				if i+1 < rightInternalBP.slotCount() {
					next := rightInternalBP.GetInternalEntry(i + 1)
					next.ChildID = rightChildID
					rec := marshalInternalRecord(next)
					newEnd := rightInternalBP.freeSpaceEnd() - len(rec)
					copy(rightInternalBP.buf[newEnd:], rec)
					rightInternalBP.setFreeSpaceEnd(newEnd)
					rightInternalBP.setSlotEntry(i+1, SlotEntry{Offset: uint16(newEnd), Length: uint16(len(rec))})
				} else {
					rightInternalBP.SetRightChild(rightChildID)
				}
				break
			}
		}
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
	bt.pager.UnpinPage(parentID)
	bt.pager.UnpinPage(newRightID)

	// Push separator up.
	return bt.insertIntoParent(txID, path[:len(path)-1], parentID, pushUpKey, newRightID)
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
			entry := bp.GetLeafEntry(i)
			if bytes.Compare(entry.Key, startKey) < 0 {
				continue
			}
			if endKey != nil && bytes.Compare(entry.Key, endKey) > 0 {
				bt.pager.UnpinPage(leafID)
				return nil
			}
			var val []byte
			if entry.Overflow {
				val, err = bt.readOverflow(entry.OverflowPageID, entry.TotalSize)
				if err != nil {
					bt.pager.UnpinPage(leafID)
					return err
				}
			} else {
				val = entry.Value
			}
			if !fn(entry.Key, val) {
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

	// Write last page.
	if prevBuf != nil {
		SetPageCRC(prevBuf)
		if err := bt.pager.WritePage(txID, prevID, prevBuf); err != nil {
			return 0, err
		}
		bt.pager.UnpinPage(prevID)
	}

	return headID, nil
}

func (bt *BTree) readOverflow(headID PageID, totalSize uint32) ([]byte, error) {
	result := make([]byte, 0, totalSize)
	pid := headID
	for pid != InvalidPageID {
		buf, err := bt.pager.ReadPage(pid)
		if err != nil {
			return nil, err
		}
		op := WrapOverflowPage(buf)
		result = append(result, op.Data()...)
		next := op.NextOverflow()
		bt.pager.UnpinPage(pid)
		pid = next
	}
	return result, nil
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
