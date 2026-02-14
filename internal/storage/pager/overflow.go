package pager

import (
	"encoding/binary"
	"fmt"
)

// ───────────────────────────────────────────────────────────────────────────
// Overflow pages
// ───────────────────────────────────────────────────────────────────────────
//
// Overflow pages store large values that do not fit inline in B+Tree leaf
// records. They form a singly-linked chain.
//
// Layout:
//   [0:32]   Common PageHeader (Type=Overflow)
//   [32:36]  NextOverflow  (uint32 LE) — next page in chain, 0 = end
//   [36:40]  DataLen       (uint32 LE) — bytes of payload in this page
//   [40:40+DataLen]  Payload data
//
// The usable capacity per overflow page is PageSize - 40.

const (
	overflowNextOff    = PageHeaderSize         // 32
	overflowDataLenOff = overflowNextOff + 4    // 36
	overflowDataOff    = overflowDataLenOff + 4 // 40
)

// OverflowCapacity returns the payload capacity of a single overflow page.
func OverflowCapacity(pageSize int) int {
	return pageSize - overflowDataOff
}

// OverflowPage wraps a page buffer as an overflow page.
type OverflowPage struct {
	buf      []byte
	pageSize int
}

// WrapOverflowPage wraps an existing overflow page buffer.
func WrapOverflowPage(buf []byte) *OverflowPage {
	return &OverflowPage{buf: buf, pageSize: len(buf)}
}

// InitOverflowPage creates a new overflow page.
func InitOverflowPage(buf []byte, id PageID) *OverflowPage {
	h := &PageHeader{Type: PageTypeOverflow, ID: id}
	MarshalHeader(h, buf)
	binary.LittleEndian.PutUint32(buf[overflowNextOff:], uint32(InvalidPageID))
	binary.LittleEndian.PutUint32(buf[overflowDataLenOff:], 0)
	return &OverflowPage{buf: buf, pageSize: len(buf)}
}

// NextOverflow returns the next overflow page in the chain.
func (op *OverflowPage) NextOverflow() PageID {
	return PageID(binary.LittleEndian.Uint32(op.buf[overflowNextOff:]))
}

// SetNextOverflow sets the next-page pointer.
func (op *OverflowPage) SetNextOverflow(pid PageID) {
	binary.LittleEndian.PutUint32(op.buf[overflowNextOff:], uint32(pid))
}

// DataLen returns the number of payload bytes stored.
func (op *OverflowPage) DataLen() int {
	return int(binary.LittleEndian.Uint32(op.buf[overflowDataLenOff:]))
}

// SetData writes payload into the overflow page. Returns an error if the
// data exceeds the capacity.
func (op *OverflowPage) SetData(data []byte) error {
	cap := OverflowCapacity(op.pageSize)
	if len(data) > cap {
		return fmt.Errorf("overflow data %d bytes exceeds capacity %d", len(data), cap)
	}
	binary.LittleEndian.PutUint32(op.buf[overflowDataLenOff:], uint32(len(data)))
	copy(op.buf[overflowDataOff:], data)
	return nil
}

// Data returns the payload bytes.
func (op *OverflowPage) Data() []byte {
	dl := op.DataLen()
	return op.buf[overflowDataOff : overflowDataOff+dl]
}

// Bytes returns the underlying page buffer.
func (op *OverflowPage) Bytes() []byte { return op.buf }
