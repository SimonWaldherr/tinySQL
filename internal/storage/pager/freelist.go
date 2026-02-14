package pager

import (
	"encoding/binary"
)

// ───────────────────────────────────────────────────────────────────────────
// Free-list pages
// ───────────────────────────────────────────────────────────────────────────
//
// The free-list is a singly-linked chain of pages. Each page stores an array
// of page IDs that are currently free and available for reuse.
//
// Layout:
//   [0:32]   Common PageHeader (Type=FreeList)
//   [32:36]  NextFreeList  (uint32 LE) — next free-list page, 0 = end
//   [36:40]  EntryCount    (uint32 LE) — number of PageID entries
//   [40:40+4*EntryCount]   PageID entries (uint32 LE each)
//
// Capacity per page: (PageSize - 40) / 4 entries.

const (
	freeListNextOff  = PageHeaderSize       // 32
	freeListCountOff = freeListNextOff + 4  // 36
	freeListDataOff  = freeListCountOff + 4 // 40
	freeListEntryLen = 4                    // uint32
)

// FreeListCapacity returns how many page IDs fit in one free-list page.
func FreeListCapacity(pageSize int) int {
	return (pageSize - freeListDataOff) / freeListEntryLen
}

// FreeListPage wraps a page buffer as a free-list page.
type FreeListPage struct {
	buf      []byte
	pageSize int
}

// WrapFreeListPage wraps an existing free-list buffer.
func WrapFreeListPage(buf []byte) *FreeListPage {
	return &FreeListPage{buf: buf, pageSize: len(buf)}
}

// InitFreeListPage creates a new empty free-list page.
func InitFreeListPage(buf []byte, id PageID) *FreeListPage {
	h := &PageHeader{Type: PageTypeFreeList, ID: id}
	MarshalHeader(h, buf)
	binary.LittleEndian.PutUint32(buf[freeListNextOff:], uint32(InvalidPageID))
	binary.LittleEndian.PutUint32(buf[freeListCountOff:], 0)
	return &FreeListPage{buf: buf, pageSize: len(buf)}
}

// NextFreeList returns the next free-list page in the chain.
func (fl *FreeListPage) NextFreeList() PageID {
	return PageID(binary.LittleEndian.Uint32(fl.buf[freeListNextOff:]))
}

// SetNextFreeList sets the next page pointer.
func (fl *FreeListPage) SetNextFreeList(pid PageID) {
	binary.LittleEndian.PutUint32(fl.buf[freeListNextOff:], uint32(pid))
}

// EntryCount returns the number of free page IDs stored.
func (fl *FreeListPage) EntryCount() int {
	return int(binary.LittleEndian.Uint32(fl.buf[freeListCountOff:]))
}

// GetEntry returns the i-th free page ID.
func (fl *FreeListPage) GetEntry(i int) PageID {
	off := freeListDataOff + i*freeListEntryLen
	return PageID(binary.LittleEndian.Uint32(fl.buf[off:]))
}

// AddEntry appends a free page ID. Returns false if the page is full.
func (fl *FreeListPage) AddEntry(pid PageID) bool {
	ec := fl.EntryCount()
	if ec >= FreeListCapacity(fl.pageSize) {
		return false
	}
	off := freeListDataOff + ec*freeListEntryLen
	binary.LittleEndian.PutUint32(fl.buf[off:], uint32(pid))
	binary.LittleEndian.PutUint32(fl.buf[freeListCountOff:], uint32(ec+1))
	return true
}

// PopEntry removes and returns the last entry. Returns InvalidPageID if empty.
func (fl *FreeListPage) PopEntry() PageID {
	ec := fl.EntryCount()
	if ec == 0 {
		return InvalidPageID
	}
	pid := fl.GetEntry(ec - 1)
	binary.LittleEndian.PutUint32(fl.buf[freeListCountOff:], uint32(ec-1))
	return pid
}

// AllEntries returns all stored free page IDs.
func (fl *FreeListPage) AllEntries() []PageID {
	ec := fl.EntryCount()
	ids := make([]PageID, ec)
	for i := 0; i < ec; i++ {
		ids[i] = fl.GetEntry(i)
	}
	return ids
}

// Bytes returns the underlying page buffer.
func (fl *FreeListPage) Bytes() []byte { return fl.buf }

// ───────────────────────────────────────────────────────────────────────────
// FreeSpace manager — coordinates free-list pages via the pager
// ───────────────────────────────────────────────────────────────────────────

// FreeManager tracks free pages using an in-memory set backed by free-list
// pages on disk. The pager calls its methods during allocation and deallocation.
type FreeManager struct {
	free map[PageID]struct{} // set of all free page IDs
	head PageID              // head of the free-list chain on disk (superblock)
}

// NewFreeManager creates a FreeManager. Call LoadFromDisk to populate.
func NewFreeManager() *FreeManager {
	return &FreeManager{free: map[PageID]struct{}{}}
}

// LoadFromDisk walks the free-list chain starting at head and populates
// the in-memory set. readPage is a callback that reads a page by ID.
func (fm *FreeManager) LoadFromDisk(head PageID, readPage func(PageID) ([]byte, error)) error {
	fm.head = head
	pid := head
	for pid != InvalidPageID {
		buf, err := readPage(pid)
		if err != nil {
			return err
		}
		fl := WrapFreeListPage(buf)
		for _, freeID := range fl.AllEntries() {
			fm.free[freeID] = struct{}{}
		}
		pid = fl.NextFreeList()
	}
	return nil
}

// Alloc returns a free page ID (popped from the set) or InvalidPageID if empty.
func (fm *FreeManager) Alloc() PageID {
	for pid := range fm.free {
		delete(fm.free, pid)
		return pid
	}
	return InvalidPageID
}

// Free marks a page ID as available for reuse.
func (fm *FreeManager) Free(pid PageID) {
	fm.free[pid] = struct{}{}
}

// Count returns the number of free pages.
func (fm *FreeManager) Count() int { return len(fm.free) }

// AllFree returns all free page IDs (unsorted).
func (fm *FreeManager) AllFree() []PageID {
	ids := make([]PageID, 0, len(fm.free))
	for pid := range fm.free {
		ids = append(ids, pid)
	}
	return ids
}

// FlushToDisk writes the in-memory free set into free-list pages. It returns
// the head PageID of the new chain and the list of page buffers to write.
// allocPage is a callback that returns a new, zeroed page buffer with a fresh ID.
func (fm *FreeManager) FlushToDisk(pageSize int, allocPage func() (PageID, []byte)) (PageID, [][]byte) {
	ids := fm.AllFree()
	if len(ids) == 0 {
		return InvalidPageID, nil
	}

	cap := FreeListCapacity(pageSize)
	var pages [][]byte
	var head PageID
	var prev *FreeListPage

	for i := 0; i < len(ids); i += cap {
		end := i + cap
		if end > len(ids) {
			end = len(ids)
		}
		chunk := ids[i:end]

		pid, buf := allocPage()
		fl := InitFreeListPage(buf, pid)
		for _, fid := range chunk {
			fl.AddEntry(fid)
		}
		SetPageCRC(buf)
		pages = append(pages, buf)

		if prev != nil {
			prev.SetNextFreeList(pid)
			SetPageCRC(prev.Bytes()) // update CRC after linking
		} else {
			head = pid
		}
		prev = fl
	}

	fm.head = head
	return head, pages
}
