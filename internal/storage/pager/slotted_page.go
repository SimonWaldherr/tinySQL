package pager

import (
	"encoding/binary"
	"fmt"
)

// ───────────────────────────────────────────────────────────────────────────
// Slotted Page
// ───────────────────────────────────────────────────────────────────────────
//
// A slotted page stores variable-length records. The layout is:
//
//   [0..31]             Common PageHeader
//   [32..35]            SlotCount  (uint16) + FreeSpaceEnd (uint16)
//   [36..36+4*SlotCount] Slot directory (4 bytes per slot)
//   ... free space ...
//   [FreeSpaceEnd..PageSize]  Record data grows downward
//
// Each slot entry is 4 bytes:
//   [0:2]  Offset  (uint16) — offset of record from page start
//   [2:4]  Length  (uint16) — record length in bytes
//
// A slot with Offset==0 and Length==0 is a tombstone (deleted record).
//
// Invariants:
//   - Records grow downward from the end of the page.
//   - Slots grow forward from after the slotted-page header.
//   - FreeSpaceEnd tracks where the next record can be placed.

const (
	// slottedHeaderOff is the offset of SlotCount within the page.
	slottedHeaderOff = PageHeaderSize // 32

	// slottedSlotCountSize is bytes for SlotCount + FreeSpaceEnd.
	slottedSlotCountSize = 4 // uint16 + uint16

	// slottedSlotDirOff is where slot entries start.
	slottedSlotDirOff = slottedHeaderOff + slottedSlotCountSize // 36

	// slotEntrySize is bytes per slot entry (offset + length).
	slotEntrySize = 4
)

// SlottedPage wraps a raw page buffer and provides record-level operations.
type SlottedPage struct {
	buf      []byte
	pageSize int
}

// SlotEntry describes one slot in the directory.
type SlotEntry struct {
	Offset uint16
	Length uint16
}

// WrapSlottedPage wraps an existing page buffer.
func WrapSlottedPage(buf []byte) *SlottedPage {
	return &SlottedPage{buf: buf, pageSize: len(buf)}
}

// InitSlottedPage initialises a page buffer as an empty slotted page.
func InitSlottedPage(buf []byte, pt PageType, id PageID) *SlottedPage {
	h := &PageHeader{Type: pt, ID: id}
	MarshalHeader(h, buf)
	// SlotCount = 0
	binary.LittleEndian.PutUint16(buf[slottedHeaderOff:], 0)
	// FreeSpaceEnd = pageSize (nothing stored yet)
	binary.LittleEndian.PutUint16(buf[slottedHeaderOff+2:], uint16(len(buf)))
	return WrapSlottedPage(buf)
}

// SlotCount returns the number of slots (including tombstones).
func (sp *SlottedPage) SlotCount() int {
	return int(binary.LittleEndian.Uint16(sp.buf[slottedHeaderOff:]))
}

func (sp *SlottedPage) setSlotCount(n int) {
	binary.LittleEndian.PutUint16(sp.buf[slottedHeaderOff:], uint16(n))
}

// FreeSpaceEnd is the byte offset where the next record will be written.
func (sp *SlottedPage) FreeSpaceEnd() int {
	return int(binary.LittleEndian.Uint16(sp.buf[slottedHeaderOff+2:]))
}

func (sp *SlottedPage) setFreeSpaceEnd(off int) {
	binary.LittleEndian.PutUint16(sp.buf[slottedHeaderOff+2:], uint16(off))
}

// slotDirEnd returns the byte offset just past the last slot entry.
func (sp *SlottedPage) slotDirEnd() int {
	return slottedSlotDirOff + sp.SlotCount()*slotEntrySize
}

// FreeSpace returns the number of bytes available for new records+slots.
func (sp *SlottedPage) FreeSpace() int {
	return sp.FreeSpaceEnd() - sp.slotDirEnd() - slotEntrySize // account for new slot
}

// GetSlot returns the slot entry at index i.
func (sp *SlottedPage) GetSlot(i int) SlotEntry {
	off := slottedSlotDirOff + i*slotEntrySize
	return SlotEntry{
		Offset: binary.LittleEndian.Uint16(sp.buf[off:]),
		Length: binary.LittleEndian.Uint16(sp.buf[off+2:]),
	}
}

func (sp *SlottedPage) setSlot(i int, e SlotEntry) {
	off := slottedSlotDirOff + i*slotEntrySize
	binary.LittleEndian.PutUint16(sp.buf[off:], e.Offset)
	binary.LittleEndian.PutUint16(sp.buf[off+2:], e.Length)
}

// IsDeleted returns true if slot i is a tombstone.
func (sp *SlottedPage) IsDeleted(i int) bool {
	e := sp.GetSlot(i)
	return e.Offset == 0 && e.Length == 0
}

// GetRecord returns the raw bytes of the record at slot i.
// Returns nil if the slot is a tombstone.
func (sp *SlottedPage) GetRecord(i int) []byte {
	e := sp.GetSlot(i)
	if e.Offset == 0 && e.Length == 0 {
		return nil
	}
	return sp.buf[e.Offset : e.Offset+e.Length]
}

// InsertRecord adds a new record to the page.
// Returns the slot index, or an error if there is insufficient space.
func (sp *SlottedPage) InsertRecord(data []byte) (int, error) {
	needed := len(data)
	if sp.FreeSpace() < needed {
		return -1, fmt.Errorf("page full: need %d bytes, have %d", needed, sp.FreeSpace())
	}

	// Place record at FreeSpaceEnd - len(data).
	newEnd := sp.FreeSpaceEnd() - needed
	copy(sp.buf[newEnd:], data)
	sp.setFreeSpaceEnd(newEnd)

	// Try to reuse a tombstone slot first.
	sc := sp.SlotCount()
	for i := 0; i < sc; i++ {
		if sp.IsDeleted(i) {
			sp.setSlot(i, SlotEntry{Offset: uint16(newEnd), Length: uint16(needed)})
			return i, nil
		}
	}

	// Add new slot.
	sp.setSlot(sc, SlotEntry{Offset: uint16(newEnd), Length: uint16(needed)})
	sp.setSlotCount(sc + 1)
	return sc, nil
}

// DeleteRecord marks slot i as deleted (tombstone).
func (sp *SlottedPage) DeleteRecord(i int) error {
	if i < 0 || i >= sp.SlotCount() {
		return fmt.Errorf("slot %d out of range [0..%d)", i, sp.SlotCount())
	}
	sp.setSlot(i, SlotEntry{Offset: 0, Length: 0})
	return nil
}

// UpdateRecord replaces the record at slot i. If the new data fits in the
// old slot's space, it is written in-place; otherwise the old slot is
// tombstoned and a new record is appended.
func (sp *SlottedPage) UpdateRecord(i int, data []byte) error {
	if i < 0 || i >= sp.SlotCount() {
		return fmt.Errorf("slot %d out of range [0..%d)", i, sp.SlotCount())
	}
	old := sp.GetSlot(i)
	if int(old.Length) >= len(data) {
		// In-place update — pad with zeros if shorter.
		copy(sp.buf[old.Offset:], data)
		if len(data) < int(old.Length) {
			for j := int(old.Offset) + len(data); j < int(old.Offset+old.Length); j++ {
				sp.buf[j] = 0
			}
		}
		sp.setSlot(i, SlotEntry{Offset: old.Offset, Length: uint16(len(data))})
		return nil
	}
	// Does not fit — tombstone + new record at FreeSpaceEnd.
	sp.setSlot(i, SlotEntry{Offset: 0, Length: 0})
	needed := len(data)
	if sp.FreeSpace()+slotEntrySize < needed { // FreeSpace deducted a slot
		return fmt.Errorf("page full on update: need %d bytes", needed)
	}
	newEnd := sp.FreeSpaceEnd() - needed
	copy(sp.buf[newEnd:], data)
	sp.setFreeSpaceEnd(newEnd)
	sp.setSlot(i, SlotEntry{Offset: uint16(newEnd), Length: uint16(needed)})
	return nil
}

// Compact reorganises records to remove gaps left by deletions.
// Preserves slot order. This is needed before splitting pages.
func (sp *SlottedPage) Compact() {
	sc := sp.SlotCount()
	type rec struct {
		slot int
		data []byte
	}
	var live []rec
	for i := 0; i < sc; i++ {
		if !sp.IsDeleted(i) {
			live = append(live, rec{slot: i, data: append([]byte{}, sp.GetRecord(i)...)})
		}
	}
	// Reset free space to end of page.
	sp.setFreeSpaceEnd(sp.pageSize)
	// Rewrite records.
	for _, r := range live {
		newEnd := sp.FreeSpaceEnd() - len(r.data)
		copy(sp.buf[newEnd:], r.data)
		sp.setFreeSpaceEnd(newEnd)
		sp.setSlot(r.slot, SlotEntry{Offset: uint16(newEnd), Length: uint16(len(r.data))})
	}
}

// LiveRecords returns the count of non-deleted records.
func (sp *SlottedPage) LiveRecords() int {
	n := 0
	sc := sp.SlotCount()
	for i := 0; i < sc; i++ {
		if !sp.IsDeleted(i) {
			n++
		}
	}
	return n
}

// Bytes returns the underlying page buffer.
func (sp *SlottedPage) Bytes() []byte { return sp.buf }
