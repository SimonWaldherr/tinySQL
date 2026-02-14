package pager

import (
	"encoding/binary"
	"fmt"
)

// ───────────────────────────────────────────────────────────────────────────
// Superblock – Page 0
// ───────────────────────────────────────────────────────────────────────────
//
// Layout (fits in one page, default 8 KiB):
//
//  Offset  Size  Field
//  ──────  ────  ───────────────────
//  0       32    Common PageHeader (Type=Superblock, ID=0)
//  32      8     Magic            [8]byte "TNSQLDB\x00"
//  40      4     FormatVersion    uint32 LE
//  44      4     PageSize         uint32 LE
//  48      8     PageCount        uint64 LE  (total pages in file)
//  56      8     FeatureFlags     uint64 LE  (bitmask)
//  64      4     CatalogRoot      uint32 LE  (PageID of system catalog B+Tree root)
//  68      4     FreeListRoot     uint32 LE  (PageID of free-list head)
//  72      8     CheckpointLSN    uint64 LE
//  80      8     NextTxID         uint64 LE
//  84      4     NextPageID       uint32 LE
//  88      4     Reserved0        uint32 LE
//  92      164   Reserved         [164]byte  (future use — zero-filled)
//
// The CRC in the common header covers the entire page.

const (
	// SuperblockMagic identifies a valid tinySQL database file.
	SuperblockMagic = "TNSQLDB\x00"

	// CurrentFormatVersion is the on-disk format version.
	CurrentFormatVersion uint32 = 1

	// Superblock field offsets (relative to page start).
	sbMagicOff         = PageHeaderSize         // 32
	sbFormatVersionOff = sbMagicOff + 8         // 40
	sbPageSizeOff      = sbFormatVersionOff + 4 // 44
	sbPageCountOff     = sbPageSizeOff + 4      // 48
	sbFeatureFlagsOff  = sbPageCountOff + 8     // 56
	sbCatalogRootOff   = sbFeatureFlagsOff + 8  // 64
	sbFreeListRootOff  = sbCatalogRootOff + 4   // 68
	sbCheckpointLSNOff = sbFreeListRootOff + 4  // 72
	sbNextTxIDOff      = sbCheckpointLSNOff + 8 // 80
	sbNextPageIDOff    = sbNextTxIDOff + 8      // 88
	// Remaining bytes up to end of page are reserved.
)

// FeatureFlag bits (bitmask). Version 1 has no flags set.
const (
	FeatureCompression FeatureFlag = 1 << iota // reserved: page-level compression
	FeatureEncryption                          // reserved: page-level encryption
	FeatureMVCC                                // reserved: multi-version concurrency
	FeaturePartitions                          // reserved: range partitioning
)

// FeatureFlag is a bitmask of optional format features.
type FeatureFlag uint64

// SupportedFeatures is the set of features understood by this build.
// Any flag outside of this set causes the file to be rejected.
const SupportedFeatures FeatureFlag = 0 // v1: none

// Superblock holds the parsed contents of page 0.
type Superblock struct {
	FormatVersion uint32
	PageSize      uint32
	PageCount     uint64
	FeatureFlags  FeatureFlag
	CatalogRoot   PageID
	FreeListRoot  PageID
	CheckpointLSN LSN
	NextTxID      TxID
	NextPageID    PageID
}

// MarshalSuperblock serializes a Superblock into a full page buffer.
// The buffer must be at least PageSize bytes. The common PageHeader is set
// (Type=Superblock, ID=0) and the CRC computed.
func MarshalSuperblock(sb *Superblock, pageSize int) []byte {
	buf := NewPage(pageSize, PageTypeSuperblock, 0)

	// Magic bytes
	copy(buf[sbMagicOff:sbMagicOff+8], SuperblockMagic)

	// Fields
	binary.LittleEndian.PutUint32(buf[sbFormatVersionOff:], sb.FormatVersion)
	binary.LittleEndian.PutUint32(buf[sbPageSizeOff:], sb.PageSize)
	binary.LittleEndian.PutUint64(buf[sbPageCountOff:], sb.PageCount)
	binary.LittleEndian.PutUint64(buf[sbFeatureFlagsOff:], uint64(sb.FeatureFlags))
	binary.LittleEndian.PutUint32(buf[sbCatalogRootOff:], uint32(sb.CatalogRoot))
	binary.LittleEndian.PutUint32(buf[sbFreeListRootOff:], uint32(sb.FreeListRoot))
	binary.LittleEndian.PutUint64(buf[sbCheckpointLSNOff:], uint64(sb.CheckpointLSN))
	binary.LittleEndian.PutUint64(buf[sbNextTxIDOff:], uint64(sb.NextTxID))
	binary.LittleEndian.PutUint32(buf[sbNextPageIDOff:], uint32(sb.NextPageID))

	SetPageCRC(buf)
	return buf
}

// UnmarshalSuperblock decodes page 0 from buf. It validates magic bytes,
// format version, feature flags, and CRC. Returns an error on any mismatch.
func UnmarshalSuperblock(buf []byte) (*Superblock, error) {
	if len(buf) < MinPageSize {
		return nil, fmt.Errorf("superblock too small: %d bytes", len(buf))
	}
	// Verify CRC first.
	if err := VerifyPageCRC(buf); err != nil {
		return nil, fmt.Errorf("superblock CRC: %w", err)
	}
	// Check magic.
	magic := string(buf[sbMagicOff : sbMagicOff+8])
	if magic != SuperblockMagic {
		return nil, fmt.Errorf("bad magic %q, expected %q", magic, SuperblockMagic)
	}
	sb := &Superblock{
		FormatVersion: binary.LittleEndian.Uint32(buf[sbFormatVersionOff:]),
		PageSize:      binary.LittleEndian.Uint32(buf[sbPageSizeOff:]),
		PageCount:     binary.LittleEndian.Uint64(buf[sbPageCountOff:]),
		FeatureFlags:  FeatureFlag(binary.LittleEndian.Uint64(buf[sbFeatureFlagsOff:])),
		CatalogRoot:   PageID(binary.LittleEndian.Uint32(buf[sbCatalogRootOff:])),
		FreeListRoot:  PageID(binary.LittleEndian.Uint32(buf[sbFreeListRootOff:])),
		CheckpointLSN: LSN(binary.LittleEndian.Uint64(buf[sbCheckpointLSNOff:])),
		NextTxID:      TxID(binary.LittleEndian.Uint64(buf[sbNextTxIDOff:])),
		NextPageID:    PageID(binary.LittleEndian.Uint32(buf[sbNextPageIDOff:])),
	}

	// Validate format version.
	if sb.FormatVersion != CurrentFormatVersion {
		return nil, fmt.Errorf("unsupported format version %d (this build supports %d)",
			sb.FormatVersion, CurrentFormatVersion)
	}
	// Validate page size.
	if sb.PageSize < MinPageSize || sb.PageSize > MaxPageSize {
		return nil, fmt.Errorf("page size %d out of range [%d..%d]",
			sb.PageSize, MinPageSize, MaxPageSize)
	}
	// Power-of-two check.
	if sb.PageSize&(sb.PageSize-1) != 0 {
		return nil, fmt.Errorf("page size %d is not a power of two", sb.PageSize)
	}
	// Feature flags — reject unknown.
	if sb.FeatureFlags & ^SupportedFeatures != 0 {
		return nil, fmt.Errorf("unsupported feature flags: %016x", sb.FeatureFlags)
	}

	return sb, nil
}

// NewSuperblock creates a default Superblock for a new database.
func NewSuperblock(pageSize uint32) *Superblock {
	return &Superblock{
		FormatVersion: CurrentFormatVersion,
		PageSize:      pageSize,
		PageCount:     1, // only superblock so far
		FeatureFlags:  0,
		CatalogRoot:   InvalidPageID,
		FreeListRoot:  InvalidPageID,
		CheckpointLSN: 0,
		NextTxID:      1,
		NextPageID:    1, // page 0 is superblock
	}
}
