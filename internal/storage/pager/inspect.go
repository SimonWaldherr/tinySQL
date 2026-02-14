package pager

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"strings"
)

// ───────────────────────────────────────────────────────────────────────────
// Inspection & Verification Tools
// ───────────────────────────────────────────────────────────────────────────

// PageInfo holds inspection information about a single page.
type PageInfo struct {
	ID       PageID
	Type     PageType
	TypeStr  string
	LSN      LSN
	CRC      uint32
	CRCValid bool
	Flags    uint8
	// B+Tree specifics
	IsLeaf     bool
	KeyCount   int
	RightChild PageID
	NextLeaf   PageID
	PrevLeaf   PageID
	// Slotted page stats
	SlotCount int
	FreeSpace int
	// Overflow
	NextOverflow PageID
	DataLen      int
	// FreeList
	NextFreeList PageID
	EntryCount   int
}

// InspectPage reads a single page and returns detailed information.
func InspectPage(dbPath string, pageID PageID, pageSize int) (*PageInfo, error) {
	f, err := os.Open(dbPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	buf := make([]byte, pageSize)
	off := int64(pageID) * int64(pageSize)
	if _, err := f.ReadAt(buf, off); err != nil {
		return nil, fmt.Errorf("read page %d: %w", pageID, err)
	}

	hdr := UnmarshalHeader(buf)
	crcValid := VerifyPageCRC(buf) == nil

	info := &PageInfo{
		ID:       hdr.ID,
		Type:     hdr.Type,
		TypeStr:  hdr.Type.String(),
		LSN:      hdr.LSN,
		CRC:      hdr.CRC,
		CRCValid: crcValid,
		Flags:    hdr.Flags,
	}

	switch hdr.Type {
	case PageTypeBTreeInternal, PageTypeBTreeLeaf:
		bp := WrapBTreePage(buf)
		info.IsLeaf = bp.IsLeaf()
		info.KeyCount = bp.KeyCount()
		info.RightChild = bp.RightChild()
		info.NextLeaf = bp.NextLeaf()
		info.PrevLeaf = bp.PrevLeaf()
		info.SlotCount = bp.slotCount()
		info.FreeSpace = bp.freeSpace()

	case PageTypeOverflow:
		op := WrapOverflowPage(buf)
		info.NextOverflow = op.NextOverflow()
		info.DataLen = op.DataLen()

	case PageTypeFreeList:
		fl := WrapFreeListPage(buf)
		info.NextFreeList = fl.NextFreeList()
		info.EntryCount = fl.EntryCount()
	}

	return info, nil
}

// VerifyDB checks the integrity of an entire database file.
// Returns a list of issues found (empty = healthy).
func VerifyDB(dbPath string) ([]string, error) {
	f, err := os.Open(dbPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}

	var issues []string

	// Read superblock and determine page size.
	sbBuf := make([]byte, MaxPageSize) // read max possible
	n, err := f.ReadAt(sbBuf, 0)
	if err != nil && err != io.EOF {
		return nil, err
	}
	if n < MinPageSize {
		return []string{"file too small to contain a superblock"}, nil
	}

	// Peek at the page size field so we can trim the buffer to the
	// actual page size before CRC verification.
	peekPS := int(binary.LittleEndian.Uint32(sbBuf[sbPageSizeOff:]))
	if peekPS >= MinPageSize && peekPS <= MaxPageSize && peekPS <= n {
		sbBuf = sbBuf[:peekPS]
	} else {
		sbBuf = sbBuf[:n]
	}

	sb, err := UnmarshalSuperblock(sbBuf)
	if err != nil {
		return []string{fmt.Sprintf("superblock: %v", err)}, nil
	}

	pageSize := int(sb.PageSize)
	totalPages := fi.Size() / int64(pageSize)
	if fi.Size()%int64(pageSize) != 0 {
		issues = append(issues, fmt.Sprintf("file size %d not a multiple of page size %d",
			fi.Size(), pageSize))
	}

	if uint64(totalPages) != sb.PageCount && uint64(totalPages) > sb.PageCount {
		// Allow file to be larger (pages may have been allocated).
	}

	// Check each page's CRC.
	buf := make([]byte, pageSize)
	for i := int64(0); i < totalPages; i++ {
		if _, err := f.ReadAt(buf, i*int64(pageSize)); err != nil {
			issues = append(issues, fmt.Sprintf("page %d: read error: %v", i, err))
			continue
		}
		if err := VerifyPageCRC(buf); err != nil {
			issues = append(issues, fmt.Sprintf("page %d: %v", i, err))
		}

		// Type-specific checks.
		hdr := UnmarshalHeader(buf)
		if hdr.ID != PageID(i) && i > 0 { // superblock always has ID 0
			issues = append(issues, fmt.Sprintf("page %d: header ID mismatch (says %d)", i, hdr.ID))
		}
	}

	return issues, nil
}

// DumpTree produces a human-readable dump of a B+Tree starting at root.
func DumpTree(dbPath string, rootPageID PageID, pageSize int) (string, error) {
	f, err := os.Open(dbPath)
	if err != nil {
		return "", err
	}
	defer f.Close()

	var sb strings.Builder
	var dump func(pid PageID, depth int) error

	readPage := func(pid PageID) ([]byte, error) {
		buf := make([]byte, pageSize)
		off := int64(pid) * int64(pageSize)
		if _, err := f.ReadAt(buf, off); err != nil {
			return nil, err
		}
		return buf, nil
	}

	dump = func(pid PageID, depth int) error {
		buf, err := readPage(pid)
		if err != nil {
			return err
		}
		indent := strings.Repeat("  ", depth)
		hdr := UnmarshalHeader(buf)
		bp := WrapBTreePage(buf)

		if bp.IsLeaf() {
			fmt.Fprintf(&sb, "%sLeaf[%d] keys=%d next=%d prev=%d\n",
				indent, pid, bp.KeyCount(), bp.NextLeaf(), bp.PrevLeaf())
			sc := bp.slotCount()
			for i := 0; i < sc; i++ {
				entry := bp.GetLeafEntry(i)
				if entry.Overflow {
					fmt.Fprintf(&sb, "%s  [%d] key=%q overflow=page%d size=%d\n",
						indent, i, entry.Key, entry.OverflowPageID, entry.TotalSize)
				} else {
					fmt.Fprintf(&sb, "%s  [%d] key=%q val=%d bytes\n",
						indent, i, entry.Key, len(entry.Value))
				}
			}
		} else {
			fmt.Fprintf(&sb, "%sInternal[%d] keys=%d rightChild=%d lsn=%d\n",
				indent, pid, bp.KeyCount(), bp.RightChild(), hdr.LSN)
			sc := bp.slotCount()
			for i := 0; i < sc; i++ {
				entry := bp.GetInternalEntry(i)
				fmt.Fprintf(&sb, "%s  child=%d sep=%q\n", indent, entry.ChildID, entry.Key)
				if err := dump(entry.ChildID, depth+1); err != nil {
					return err
				}
			}
			// Dump right child.
			rc := bp.RightChild()
			if rc != InvalidPageID {
				fmt.Fprintf(&sb, "%s  rightChild=%d\n", indent, rc)
				if err := dump(rc, depth+1); err != nil {
					return err
				}
			}
		}
		return nil
	}

	if err := dump(rootPageID, 0); err != nil {
		return "", err
	}
	return sb.String(), nil
}

// WALInfo holds information about a WAL file.
type WALInfo struct {
	PageSize   int
	Records    int
	MinLSN     LSN
	MaxLSN     LSN
	TxCount    int
	Committed  int
	Aborted    int
	PageImages int
}

// InspectWAL reads and summarises a WAL file.
func InspectWAL(walPath string) (*WALInfo, error) {
	records, err := ReadAllRecords(walPath)
	if err != nil {
		return nil, err
	}

	info := &WALInfo{Records: len(records)}
	txSet := make(map[TxID]bool)

	for _, rec := range records {
		if info.MinLSN == 0 || rec.LSN < info.MinLSN {
			info.MinLSN = rec.LSN
		}
		if rec.LSN > info.MaxLSN {
			info.MaxLSN = rec.LSN
		}
		txSet[rec.TxID] = true

		switch rec.Type {
		case WALRecordCommit:
			info.Committed++
		case WALRecordAbort:
			info.Aborted++
		case WALRecordPageImage:
			info.PageImages++
		}
	}
	info.TxCount = len(txSet)

	// Read page size from WAL header.
	f, err := os.Open(walPath)
	if err == nil {
		var hdr [WALFileHdrSize]byte
		if _, err := f.ReadAt(hdr[:], 0); err == nil {
			info.PageSize = int(binary.LittleEndian.Uint32(hdr[12:16]))
		}
		f.Close()
	}

	return info, nil
}

// SuperblockInfo holds display-friendly superblock data.
type SuperblockInfo struct {
	FormatVersion uint32
	PageSize      uint32
	PageCount     uint64
	FeatureFlags  uint64
	CatalogRoot   PageID
	FreeListRoot  PageID
	CheckpointLSN LSN
	NextTxID      TxID
	NextPageID    PageID
	CRCValid      bool
}

// InspectSuperblock reads and returns the superblock metadata.
func InspectSuperblock(dbPath string) (*SuperblockInfo, error) {
	f, err := os.Open(dbPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	buf := make([]byte, MaxPageSize)
	n, err := f.ReadAt(buf, 0)
	if err != nil && err != io.EOF {
		return nil, err
	}
	// Trim to actual page size before CRC check.
	if n >= int(sbPageSizeOff)+4 {
		ps := int(binary.LittleEndian.Uint32(buf[sbPageSizeOff:]))
		if ps >= MinPageSize && ps <= MaxPageSize && ps <= n {
			buf = buf[:ps]
		} else {
			buf = buf[:n]
		}
	} else {
		buf = buf[:n]
	}

	crcValid := VerifyPageCRC(buf) == nil
	sb, err := UnmarshalSuperblock(buf)
	if err != nil {
		return &SuperblockInfo{CRCValid: crcValid}, err
	}

	return &SuperblockInfo{
		FormatVersion: sb.FormatVersion,
		PageSize:      sb.PageSize,
		PageCount:     sb.PageCount,
		FeatureFlags:  uint64(sb.FeatureFlags),
		CatalogRoot:   sb.CatalogRoot,
		FreeListRoot:  sb.FreeListRoot,
		CheckpointLSN: sb.CheckpointLSN,
		NextTxID:      sb.NextTxID,
		NextPageID:    sb.NextPageID,
		CRCValid:      crcValid,
	}, nil
}
