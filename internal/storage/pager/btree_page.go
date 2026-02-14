package pager

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// ───────────────────────────────────────────────────────────────────────────
// B+Tree on-disk format
// ───────────────────────────────────────────────────────────────────────────
//
// Internal pages store sorted separator keys and child page pointers.
// Leaf pages store sorted key-value pairs with an optional overflow pointer.
// Both types use slotted-page records for variable-length data.
//
// Internal record layout (per slot):
//   [0:4]   ChildPageID  (uint32 LE) — left child for this key
//   [4:6]   KeyLen       (uint16 LE)
//   [6:6+K] Key          (K bytes)
//   The rightmost child pointer is stored in the page trailer (last 4 bytes
//   before FreeSpaceEnd, managed separately).
//
// Leaf record layout (per slot):
//   [0:2]   KeyLen       (uint16 LE)
//   [2:2+K] Key          (K bytes)
//   [K+2:K+4] Flags      (uint16 LE) — bit 0: overflow flag
//   If overflow:
//     [K+4:K+8] OverflowPageID (uint32 LE)
//     [K+8:K+12] TotalSize    (uint32 LE) — full value size
//   Else:
//     [K+4:K+6] ValLen   (uint16 LE)
//     [K+6:K+6+V] Value  (V bytes)
//
// Page-level metadata stored right after PageHeader:
//   [32:33]  IsLeaf       (uint8 — 1=leaf, 0=internal)
//   [33:35]  KeyCount     (uint16 LE) — managed by slotted page SlotCount
//   [35:39]  RightChild   (uint32 LE) — only for internal pages
//   [39:43]  NextLeaf     (uint32 LE) — only for leaf pages (sibling pointer)
//   [43:47]  PrevLeaf     (uint32 LE) — only for leaf pages (sibling pointer)
//
// The slotted-page header starts at offset 48 (overriding the standard 32).
// We therefore customise the slotted-page layout for B+Tree pages.

const (
	btreeMetaOff       = PageHeaderSize    // 32
	btreeIsLeafOff     = btreeMetaOff      // 32, 1 byte
	btreeKeyCountOff   = btreeMetaOff + 1  // 33, 2 bytes
	btreeRightChildOff = btreeMetaOff + 3  // 35, 4 bytes (internal)
	btreeNextLeafOff   = btreeMetaOff + 3  // 35, 4 bytes (leaf)
	btreePrevLeafOff   = btreeMetaOff + 7  // 39, 4 bytes (leaf)
	btreeSlotHdrOff    = btreeMetaOff + 11 // 43
	// Overriding slotted page: SlotCount at 43, FreeSpaceEnd at 45,
	// slot directory starts at 47.
	btreeSlotDirOff = btreeSlotHdrOff + 4 // 47
)

// Leaf record flags.
const (
	leafFlagOverflow uint16 = 1 << 0
)

// ───────────────────────────────────────────────────────────────────────────
// BTreePage wraps a page buffer as a B+Tree node.
// ───────────────────────────────────────────────────────────────────────────

type BTreePage struct {
	buf      []byte
	pageSize int
}

// WrapBTreePage wraps an existing buffer.
func WrapBTreePage(buf []byte) *BTreePage {
	return &BTreePage{buf: buf, pageSize: len(buf)}
}

// InitBTreePage initialises a page as a B+Tree node.
func InitBTreePage(buf []byte, id PageID, leaf bool) *BTreePage {
	pt := PageTypeBTreeInternal
	if leaf {
		pt = PageTypeBTreeLeaf
	}
	h := &PageHeader{Type: pt, ID: id}
	MarshalHeader(h, buf)
	if leaf {
		buf[btreeIsLeafOff] = 1
	} else {
		buf[btreeIsLeafOff] = 0
	}
	binary.LittleEndian.PutUint16(buf[btreeKeyCountOff:], 0)
	binary.LittleEndian.PutUint32(buf[btreeRightChildOff:], uint32(InvalidPageID))
	binary.LittleEndian.PutUint32(buf[btreePrevLeafOff:], uint32(InvalidPageID))
	// Slotted page header: SlotCount=0, FreeSpaceEnd=pageSize
	binary.LittleEndian.PutUint16(buf[btreeSlotHdrOff:], 0)
	binary.LittleEndian.PutUint16(buf[btreeSlotHdrOff+2:], uint16(len(buf)))
	return &BTreePage{buf: buf, pageSize: len(buf)}
}

// ── Accessors ──────────────────────────────────────────────────────────────

func (bp *BTreePage) IsLeaf() bool {
	return bp.buf[btreeIsLeafOff] == 1
}

func (bp *BTreePage) KeyCount() int {
	return int(binary.LittleEndian.Uint16(bp.buf[btreeKeyCountOff:]))
}

func (bp *BTreePage) setKeyCount(n int) {
	binary.LittleEndian.PutUint16(bp.buf[btreeKeyCountOff:], uint16(n))
}

func (bp *BTreePage) PageID() PageID {
	return PageID(binary.LittleEndian.Uint32(bp.buf[4:8]))
}

func (bp *BTreePage) RightChild() PageID {
	return PageID(binary.LittleEndian.Uint32(bp.buf[btreeRightChildOff:]))
}

func (bp *BTreePage) SetRightChild(pid PageID) {
	binary.LittleEndian.PutUint32(bp.buf[btreeRightChildOff:], uint32(pid))
}

func (bp *BTreePage) NextLeaf() PageID {
	return PageID(binary.LittleEndian.Uint32(bp.buf[btreeNextLeafOff:]))
}

func (bp *BTreePage) SetNextLeaf(pid PageID) {
	binary.LittleEndian.PutUint32(bp.buf[btreeNextLeafOff:], uint32(pid))
}

func (bp *BTreePage) PrevLeaf() PageID {
	return PageID(binary.LittleEndian.Uint32(bp.buf[btreePrevLeafOff:]))
}

func (bp *BTreePage) SetPrevLeaf(pid PageID) {
	binary.LittleEndian.PutUint32(bp.buf[btreePrevLeafOff:], uint32(pid))
}

func (bp *BTreePage) Bytes() []byte { return bp.buf }

// ── Slotted-page helpers (custom offsets) ─────────────────────────────────

func (bp *BTreePage) slotCount() int {
	return int(binary.LittleEndian.Uint16(bp.buf[btreeSlotHdrOff:]))
}
func (bp *BTreePage) setSlotCount(n int) {
	binary.LittleEndian.PutUint16(bp.buf[btreeSlotHdrOff:], uint16(n))
}
func (bp *BTreePage) freeSpaceEnd() int {
	return int(binary.LittleEndian.Uint16(bp.buf[btreeSlotHdrOff+2:]))
}
func (bp *BTreePage) setFreeSpaceEnd(off int) {
	binary.LittleEndian.PutUint16(bp.buf[btreeSlotHdrOff+2:], uint16(off))
}
func (bp *BTreePage) slotDirEnd() int {
	return btreeSlotDirOff + bp.slotCount()*slotEntrySize
}
func (bp *BTreePage) freeSpace() int {
	return bp.freeSpaceEnd() - bp.slotDirEnd() - slotEntrySize
}
func (bp *BTreePage) getSlotEntry(i int) SlotEntry {
	off := btreeSlotDirOff + i*slotEntrySize
	return SlotEntry{
		Offset: binary.LittleEndian.Uint16(bp.buf[off:]),
		Length: binary.LittleEndian.Uint16(bp.buf[off+2:]),
	}
}
func (bp *BTreePage) setSlotEntry(i int, e SlotEntry) {
	off := btreeSlotDirOff + i*slotEntrySize
	binary.LittleEndian.PutUint16(bp.buf[off:], e.Offset)
	binary.LittleEndian.PutUint16(bp.buf[off+2:], e.Length)
}
func (bp *BTreePage) getRecord(i int) []byte {
	e := bp.getSlotEntry(i)
	if e.Offset == 0 && e.Length == 0 {
		return nil
	}
	return bp.buf[e.Offset : e.Offset+e.Length]
}

// appendRecord adds a record to the page and returns its slot index.
func (bp *BTreePage) appendRecord(data []byte) (int, error) {
	needed := len(data)
	if bp.freeSpace() < needed {
		return -1, fmt.Errorf("btree page full: need %d, have %d free", needed, bp.freeSpace())
	}
	newEnd := bp.freeSpaceEnd() - needed
	copy(bp.buf[newEnd:], data)
	bp.setFreeSpaceEnd(newEnd)
	idx := bp.slotCount()
	bp.setSlotEntry(idx, SlotEntry{Offset: uint16(newEnd), Length: uint16(needed)})
	bp.setSlotCount(idx + 1)
	return idx, nil
}

// insertRecordAt inserts a record at position pos, shifting later slots.
func (bp *BTreePage) insertRecordAt(pos int, data []byte) error {
	needed := len(data)
	if bp.freeSpace() < needed {
		return fmt.Errorf("btree page full: need %d, have %d free", needed, bp.freeSpace())
	}
	newEnd := bp.freeSpaceEnd() - needed
	copy(bp.buf[newEnd:], data)
	bp.setFreeSpaceEnd(newEnd)

	sc := bp.slotCount()
	bp.setSlotCount(sc + 1)
	// Shift slots [pos..sc) right by one.
	for i := sc; i > pos; i-- {
		bp.setSlotEntry(i, bp.getSlotEntry(i-1))
	}
	bp.setSlotEntry(pos, SlotEntry{Offset: uint16(newEnd), Length: uint16(needed)})
	return nil
}

// ───────────────────────────────────────────────────────────────────────────
// Internal page operations
// ───────────────────────────────────────────────────────────────────────────

// InternalEntry represents a key + left-child pointer for internal pages.
type InternalEntry struct {
	ChildID PageID
	Key     []byte
}

// marshalInternalRecord creates the wire format for an internal record.
func marshalInternalRecord(entry InternalEntry) []byte {
	rec := make([]byte, 4+2+len(entry.Key))
	binary.LittleEndian.PutUint32(rec[0:4], uint32(entry.ChildID))
	binary.LittleEndian.PutUint16(rec[4:6], uint16(len(entry.Key)))
	copy(rec[6:], entry.Key)
	return rec
}

// unmarshalInternalRecord parses an internal record.
func unmarshalInternalRecord(rec []byte) InternalEntry {
	child := PageID(binary.LittleEndian.Uint32(rec[0:4]))
	kl := int(binary.LittleEndian.Uint16(rec[4:6]))
	key := make([]byte, kl)
	copy(key, rec[6:6+kl])
	return InternalEntry{ChildID: child, Key: key}
}

// GetInternalEntry returns the i-th separator key and its left child.
func (bp *BTreePage) GetInternalEntry(i int) InternalEntry {
	return unmarshalInternalRecord(bp.getRecord(i))
}

// InsertInternalEntry inserts a separator key at the correct sorted position.
func (bp *BTreePage) InsertInternalEntry(entry InternalEntry) error {
	rec := marshalInternalRecord(entry)
	pos := bp.searchInternal(entry.Key)
	if err := bp.insertRecordAt(pos, rec); err != nil {
		return err
	}
	bp.setKeyCount(bp.KeyCount() + 1)
	return nil
}

// searchInternal returns the sorted insertion position for key.
func (bp *BTreePage) searchInternal(key []byte) int {
	sc := bp.slotCount()
	lo, hi := 0, sc
	for lo < hi {
		mid := (lo + hi) / 2
		e := bp.GetInternalEntry(mid)
		if bytes.Compare(e.Key, key) < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

// FindChild returns the child PageID for the given search key.
// For internal-page navigation: find the largest key <= searchKey.
func (bp *BTreePage) FindChild(key []byte) PageID {
	sc := bp.slotCount()
	for i := sc - 1; i >= 0; i-- {
		e := bp.GetInternalEntry(i)
		if bytes.Compare(key, e.Key) >= 0 {
			return e.ChildID
		}
	}
	// Key is smaller than all separators — follow leftmost child.
	if sc > 0 {
		return bp.GetInternalEntry(0).ChildID
	}
	return bp.RightChild()
}

// GetAllInternalEntries returns all separator entries in order.
func (bp *BTreePage) GetAllInternalEntries() []InternalEntry {
	sc := bp.slotCount()
	entries := make([]InternalEntry, sc)
	for i := 0; i < sc; i++ {
		entries[i] = bp.GetInternalEntry(i)
	}
	return entries
}

// ───────────────────────────────────────────────────────────────────────────
// Leaf page operations
// ───────────────────────────────────────────────────────────────────────────

// LeafEntry represents a key-value pair stored in a leaf page.
type LeafEntry struct {
	Key            []byte
	Value          []byte // inline value (empty when overflow)
	Overflow       bool
	OverflowPageID PageID
	TotalSize      uint32
}

// marshalLeafRecord creates the wire format for a leaf record.
func marshalLeafRecord(entry LeafEntry) []byte {
	kl := len(entry.Key)
	if entry.Overflow {
		rec := make([]byte, 2+kl+2+4+4)
		binary.LittleEndian.PutUint16(rec[0:2], uint16(kl))
		copy(rec[2:2+kl], entry.Key)
		off := 2 + kl
		binary.LittleEndian.PutUint16(rec[off:off+2], leafFlagOverflow)
		binary.LittleEndian.PutUint32(rec[off+2:off+6], uint32(entry.OverflowPageID))
		binary.LittleEndian.PutUint32(rec[off+6:off+10], entry.TotalSize)
		return rec
	}
	vl := len(entry.Value)
	rec := make([]byte, 2+kl+2+2+vl)
	binary.LittleEndian.PutUint16(rec[0:2], uint16(kl))
	copy(rec[2:2+kl], entry.Key)
	off := 2 + kl
	binary.LittleEndian.PutUint16(rec[off:off+2], 0) // no flags
	binary.LittleEndian.PutUint16(rec[off+2:off+4], uint16(vl))
	copy(rec[off+4:], entry.Value)
	return rec
}

// unmarshalLeafRecord parses a leaf record.
func unmarshalLeafRecord(rec []byte) LeafEntry {
	kl := int(binary.LittleEndian.Uint16(rec[0:2]))
	key := make([]byte, kl)
	copy(key, rec[2:2+kl])
	off := 2 + kl
	flags := binary.LittleEndian.Uint16(rec[off : off+2])
	if flags&leafFlagOverflow != 0 {
		opid := PageID(binary.LittleEndian.Uint32(rec[off+2 : off+6]))
		ts := binary.LittleEndian.Uint32(rec[off+6 : off+10])
		return LeafEntry{Key: key, Overflow: true, OverflowPageID: opid, TotalSize: ts}
	}
	vl := int(binary.LittleEndian.Uint16(rec[off+2 : off+4]))
	val := make([]byte, vl)
	copy(val, rec[off+4:off+4+vl])
	return LeafEntry{Key: key, Value: val}
}

// GetLeafEntry returns the i-th key-value pair.
func (bp *BTreePage) GetLeafEntry(i int) LeafEntry {
	return unmarshalLeafRecord(bp.getRecord(i))
}

// InsertLeafEntry inserts a key-value pair at the correct sorted position.
// Returns the slot index.
func (bp *BTreePage) InsertLeafEntry(entry LeafEntry) (int, error) {
	rec := marshalLeafRecord(entry)
	pos := bp.searchLeaf(entry.Key)
	if err := bp.insertRecordAt(pos, rec); err != nil {
		return -1, err
	}
	bp.setKeyCount(bp.KeyCount() + 1)
	return pos, nil
}

// UpdateLeafEntry replaces the value at the given sorted position.
func (bp *BTreePage) UpdateLeafEntry(pos int, entry LeafEntry) error {
	rec := marshalLeafRecord(entry)
	old := bp.getSlotEntry(pos)
	if int(old.Length) >= len(rec) {
		// In-place.
		copy(bp.buf[old.Offset:], rec)
		for j := int(old.Offset) + len(rec); j < int(old.Offset+old.Length); j++ {
			bp.buf[j] = 0
		}
		bp.setSlotEntry(pos, SlotEntry{Offset: old.Offset, Length: uint16(len(rec))})
		return nil
	}
	// Re-append.
	if bp.freeSpace()+slotEntrySize < len(rec) {
		return fmt.Errorf("leaf page full on update: need %d", len(rec))
	}
	newEnd := bp.freeSpaceEnd() - len(rec)
	copy(bp.buf[newEnd:], rec)
	bp.setFreeSpaceEnd(newEnd)
	bp.setSlotEntry(pos, SlotEntry{Offset: uint16(newEnd), Length: uint16(len(rec))})
	return nil
}

// DeleteLeafEntry removes the entry at position pos.
func (bp *BTreePage) DeleteLeafEntry(pos int) error {
	if pos < 0 || pos >= bp.slotCount() {
		return fmt.Errorf("delete: slot %d out of range", pos)
	}
	// Shift slots left.
	sc := bp.slotCount()
	for i := pos; i < sc-1; i++ {
		bp.setSlotEntry(i, bp.getSlotEntry(i+1))
	}
	bp.setSlotEntry(sc-1, SlotEntry{})
	bp.setSlotCount(sc - 1)
	bp.setKeyCount(bp.KeyCount() - 1)
	return nil
}

// searchLeaf returns the sorted insertion position for key in a leaf.
func (bp *BTreePage) searchLeaf(key []byte) int {
	sc := bp.slotCount()
	lo, hi := 0, sc
	for lo < hi {
		mid := (lo + hi) / 2
		e := bp.GetLeafEntry(mid)
		if bytes.Compare(e.Key, key) < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

// FindLeafEntry searches for an exact key match. Returns (index, true) or (-1, false).
func (bp *BTreePage) FindLeafEntry(key []byte) (int, bool) {
	pos := bp.searchLeaf(key)
	if pos < bp.slotCount() {
		e := bp.GetLeafEntry(pos)
		if bytes.Equal(e.Key, key) {
			return pos, true
		}
	}
	return -1, false
}

// GetAllLeafEntries returns all leaf entries in order.
func (bp *BTreePage) GetAllLeafEntries() []LeafEntry {
	sc := bp.slotCount()
	entries := make([]LeafEntry, sc)
	for i := 0; i < sc; i++ {
		entries[i] = bp.GetLeafEntry(i)
	}
	return entries
}

// ───────────────────────────────────────────────────────────────────────────
// Internal FindChild corrected — walks separators properly
// ───────────────────────────────────────────────────────────────────────────

// SearchInternal finds the child page for a given key in an internal node.
// Internal page layout: entries[0].child, entries[0].key, entries[1].child, ...
// Keys divide the key space: key < entry[0].key → entry[0].child,
// entry[i-1].key <= key < entry[i].key → entry[i].child,
// key >= entry[last].key → RightChild.
func (bp *BTreePage) SearchInternal(key []byte) PageID {
	sc := bp.slotCount()
	for i := 0; i < sc; i++ {
		e := bp.GetInternalEntry(i)
		if bytes.Compare(key, e.Key) < 0 {
			return e.ChildID
		}
	}
	return bp.RightChild()
}
