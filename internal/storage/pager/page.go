// Package pager implements a page-based, transactional storage engine with
// Write-Ahead Logging (WAL) for tinySQL.
//
// The storage format consists of a main database file (tinysql.db) with
// fixed-size pages (default 8 KiB) and a sequential WAL file (tinysql.wal).
// The first page is a superblock; subsequent pages are typed (B+Tree internal,
// B+Tree leaf, overflow, freelist). Every page carries a header with type,
// page-ID, LSN, and CRC32 checksum. Crash recovery replays committed WAL
// transactions from the last checkpoint_lsn.
package pager

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

// ───────────────────────────────────────────────────────────────────────────
// Constants
// ───────────────────────────────────────────────────────────────────────────

const (
	// DefaultPageSize is the default page size in bytes (8 KiB).
	DefaultPageSize = 8192

	// MinPageSize is the minimum allowed page size (4 KiB).
	MinPageSize = 4096

	// MaxPageSize is the maximum allowed page size (64 KiB).
	MaxPageSize = 65536

	// PageHeaderSize is the size of the common page header in bytes.
	// Layout:
	//   [0]    PageType   (1 byte)
	//   [1]    Flags      (1 byte)
	//   [2:4]  Reserved   (2 bytes)
	//   [4:8]  PageID     (4 bytes, uint32 LE)
	//   [8:16] LSN        (8 bytes, uint64 LE)
	//   [16:20] CRC32     (4 bytes, uint32 LE)
	//   [20:32] Reserved  (12 bytes)
	PageHeaderSize = 32

	// InvalidPageID represents a null/invalid page pointer.
	InvalidPageID PageID = 0

	// OverflowThreshold is the max inline value size (bytes) before an
	// overflow page chain is used. Set to 1/4 of usable leaf space.
	// Adjusted dynamically based on page size, but this is the default.
	OverflowThreshold = 1024
)

// ───────────────────────────────────────────────────────────────────────────
// Page types
// ───────────────────────────────────────────────────────────────────────────

// PageType identifies the kind of data stored in a page.
type PageType uint8

const (
	PageTypeSuperblock    PageType = 0x01
	PageTypeBTreeInternal PageType = 0x02
	PageTypeBTreeLeaf     PageType = 0x03
	PageTypeOverflow      PageType = 0x04
	PageTypeFreeList      PageType = 0x05
)

// String returns a human-readable label for the page type.
func (pt PageType) String() string {
	switch pt {
	case PageTypeSuperblock:
		return "Superblock"
	case PageTypeBTreeInternal:
		return "BTree-Internal"
	case PageTypeBTreeLeaf:
		return "BTree-Leaf"
	case PageTypeOverflow:
		return "Overflow"
	case PageTypeFreeList:
		return "FreeList"
	default:
		return fmt.Sprintf("Unknown(0x%02x)", uint8(pt))
	}
}

// ───────────────────────────────────────────────────────────────────────────
// Core types
// ───────────────────────────────────────────────────────────────────────────

// PageID is a 32-bit page identifier. Page 0 is always the superblock.
type PageID uint32

// LSN is a monotonically increasing Log Sequence Number.
type LSN uint64

// TxID is a transaction identifier.
type TxID uint64

// ───────────────────────────────────────────────────────────────────────────
// Page header
// ───────────────────────────────────────────────────────────────────────────

// PageHeader is the 32-byte header present at the start of every page.
type PageHeader struct {
	Type     PageType // 1 byte
	Flags    uint8    // 1 byte
	Reserved uint16   // 2 bytes
	ID       PageID   // 4 bytes
	LSN      LSN      // 8 bytes
	CRC      uint32   // 4 bytes — CRC32 of the entire page (with CRC field zeroed)
	Pad      [12]byte // reserved for future use
}

// MarshalHeader writes a PageHeader into the first PageHeaderSize bytes of buf.
func MarshalHeader(h *PageHeader, buf []byte) {
	if len(buf) < PageHeaderSize {
		panic("buffer too small for PageHeader")
	}
	buf[0] = byte(h.Type)
	buf[1] = h.Flags
	binary.LittleEndian.PutUint16(buf[2:4], h.Reserved)
	binary.LittleEndian.PutUint32(buf[4:8], uint32(h.ID))
	binary.LittleEndian.PutUint64(buf[8:16], uint64(h.LSN))
	binary.LittleEndian.PutUint32(buf[16:20], h.CRC)
	copy(buf[20:32], h.Pad[:])
}

// UnmarshalHeader reads a PageHeader from the first PageHeaderSize bytes of buf.
func UnmarshalHeader(buf []byte) PageHeader {
	var h PageHeader
	h.Type = PageType(buf[0])
	h.Flags = buf[1]
	h.Reserved = binary.LittleEndian.Uint16(buf[2:4])
	h.ID = PageID(binary.LittleEndian.Uint32(buf[4:8]))
	h.LSN = LSN(binary.LittleEndian.Uint64(buf[8:16]))
	h.CRC = binary.LittleEndian.Uint32(buf[16:20])
	copy(h.Pad[:], buf[20:32])
	return h
}

// ───────────────────────────────────────────────────────────────────────────
// CRC helpers
// ───────────────────────────────────────────────────────────────────────────

// crcTable is the CRC32 (Castagnoli) table used throughout.
var crcTable = crc32.MakeTable(crc32.Castagnoli)

// ComputePageCRC computes the CRC32-C of a full page, treating the CRC
// field (bytes 16..20) as zero during computation.
func ComputePageCRC(page []byte) uint32 {
	h := crc32.New(crcTable)
	h.Write(page[:16])          // header up to CRC field
	h.Write([]byte{0, 0, 0, 0}) // zeroed CRC placeholder
	h.Write(page[20:])          // rest of page
	return h.Sum32()
}

// SetPageCRC computes and writes the CRC into the page header.
func SetPageCRC(page []byte) {
	c := ComputePageCRC(page)
	binary.LittleEndian.PutUint32(page[16:20], c)
}

// VerifyPageCRC checks the CRC32 checksum of a page.
func VerifyPageCRC(page []byte) error {
	stored := binary.LittleEndian.Uint32(page[16:20])
	computed := ComputePageCRC(page)
	if stored != computed {
		pid := PageID(binary.LittleEndian.Uint32(page[4:8]))
		return fmt.Errorf("CRC mismatch on page %d: stored=%08x computed=%08x", pid, stored, computed)
	}
	return nil
}

// ───────────────────────────────────────────────────────────────────────────
// Page helper
// ───────────────────────────────────────────────────────────────────────────

// NewPage allocates a zeroed page buffer at the given size and writes its header.
func NewPage(pageSize int, pt PageType, id PageID) []byte {
	buf := make([]byte, pageSize)
	h := &PageHeader{Type: pt, ID: id}
	MarshalHeader(h, buf)
	return buf
}
