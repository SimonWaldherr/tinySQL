package pager

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
)

// ───────────────────────────────────────────────────────────────────────────
// WAL file format
// ───────────────────────────────────────────────────────────────────────────
//
// The WAL is an append-only file of fixed-format records. This first version
// uses full page images (physical logging) for simplicity.
//
// WAL file header (first 32 bytes):
//   [0:8]   Magic       "TNSQWAL\x00"
//   [8:12]  Version     uint32 LE (currently 1)
//   [12:16] PageSize    uint32 LE
//   [16:24] Reserved    8 bytes
//   [24:28] HeaderCRC   uint32 LE (CRC of bytes 0:24)
//   [28:32] Padding     4 bytes
//
// WAL record (variable-length, follows header):
//   [0]     RecordType  (1 byte)
//   [1:5]   Reserved    (4 bytes — will hold flags)
//   [5:13]  LSN         (uint64 LE)
//   [13:21] TxID        (uint64 LE)
//   [21:25] PageID      (uint32 LE) — only for PAGE_IMAGE
//   [25:29] DataLen     (uint32 LE) — payload length (= PageSize for PAGE_IMAGE)
//   [29:33] RecordCRC   (uint32 LE) — CRC of header + data
//   [33:33+DataLen]     Data (page image for PAGE_IMAGE, empty for BEGIN/COMMIT/ABORT)
//
// Record types: BEGIN (0x01), PAGE_IMAGE (0x02), COMMIT (0x03), ABORT (0x04),
//               CHECKPOINT (0x05).

const (
	WALMagic       = "TNSQWAL\x00"
	WALVersion     = uint32(1)
	WALFileHdrSize = 32
	WALRecHdrSize  = 33
)

// WALRecordType identifies the kind of WAL record.
type WALRecordType uint8

const (
	WALRecordBegin      WALRecordType = 0x01
	WALRecordPageImage  WALRecordType = 0x02
	WALRecordCommit     WALRecordType = 0x03
	WALRecordAbort      WALRecordType = 0x04
	WALRecordCheckpoint WALRecordType = 0x05
)

func (rt WALRecordType) String() string {
	switch rt {
	case WALRecordBegin:
		return "BEGIN"
	case WALRecordPageImage:
		return "PAGE_IMAGE"
	case WALRecordCommit:
		return "COMMIT"
	case WALRecordAbort:
		return "ABORT"
	case WALRecordCheckpoint:
		return "CHECKPOINT"
	default:
		return fmt.Sprintf("UNKNOWN(0x%02x)", uint8(rt))
	}
}

// WALRecord is an in-memory representation of a WAL record.
type WALRecord struct {
	Type   WALRecordType
	LSN    LSN
	TxID   TxID
	PageID PageID
	Data   []byte // full page image for PAGE_IMAGE, nil otherwise
}

// ───────────────────────────────────────────────────────────────────────────
// WAL writer/reader
// ───────────────────────────────────────────────────────────────────────────

// WALFile manages the append-only WAL file.
type WALFile struct {
	mu       sync.Mutex
	f        *os.File
	path     string
	pageSize int
	nextLSN  LSN
	writePos int64 // current write offset — avoids Seek syscall
}

// OpenWALFile opens or creates a WAL file. If the file exists, it validates
// the header. If it does not exist, it writes a new header.
func OpenWALFile(path string, pageSize int) (*WALFile, error) {
	exists := true
	if _, err := os.Stat(path); os.IsNotExist(err) {
		exists = false
	}

	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("open WAL: %w", err)
	}

	wf := &WALFile{f: f, path: path, pageSize: pageSize, nextLSN: 1}

	if exists {
		if err := wf.validateHeader(); err != nil {
			f.Close()
			return nil, err
		}
	} else {
		if err := wf.writeHeader(); err != nil {
			f.Close()
			return nil, err
		}
	}

	// Initialise writePos to the end of the file.
	endPos, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("seek WAL end: %w", err)
	}
	wf.writePos = endPos

	return wf, nil
}

func (wf *WALFile) writeHeader() error {
	var hdr [WALFileHdrSize]byte
	copy(hdr[0:8], WALMagic)
	binary.LittleEndian.PutUint32(hdr[8:12], WALVersion)
	binary.LittleEndian.PutUint32(hdr[12:16], uint32(wf.pageSize))
	// CRC of first 24 bytes
	c := crc32.Checksum(hdr[:24], crcTable)
	binary.LittleEndian.PutUint32(hdr[24:28], c)
	if _, err := wf.f.WriteAt(hdr[:], 0); err != nil {
		return fmt.Errorf("write WAL header: %w", err)
	}
	return wf.f.Sync()
}

func (wf *WALFile) validateHeader() error {
	var hdr [WALFileHdrSize]byte
	n, err := wf.f.ReadAt(hdr[:], 0)
	if err != nil && err != io.EOF {
		return fmt.Errorf("read WAL header: %w", err)
	}
	if n < WALFileHdrSize {
		return fmt.Errorf("WAL header too short: %d bytes", n)
	}
	if string(hdr[0:8]) != WALMagic {
		return fmt.Errorf("bad WAL magic")
	}
	ver := binary.LittleEndian.Uint32(hdr[8:12])
	if ver != WALVersion {
		return fmt.Errorf("unsupported WAL version %d", ver)
	}
	ps := binary.LittleEndian.Uint32(hdr[12:16])
	if int(ps) != wf.pageSize {
		return fmt.Errorf("WAL page size %d != expected %d", ps, wf.pageSize)
	}
	stored := binary.LittleEndian.Uint32(hdr[24:28])
	computed := crc32.Checksum(hdr[:24], crcTable)
	if stored != computed {
		return fmt.Errorf("WAL header CRC mismatch")
	}
	return nil
}

// AppendRecord writes a WAL record and assigns it a monotonic LSN.
// Returns the assigned LSN.
func (wf *WALFile) AppendRecord(rec *WALRecord) (LSN, error) {
	wf.mu.Lock()
	defer wf.mu.Unlock()

	lsn := wf.nextLSN
	wf.nextLSN++
	rec.LSN = lsn

	data := marshalWALRecord(rec)
	n, err := wf.f.WriteAt(data, wf.writePos)
	if err != nil {
		return 0, fmt.Errorf("WAL append: %w", err)
	}
	wf.writePos += int64(n)
	return lsn, nil
}

// Sync fsyncs the WAL file to guarantee durability.
func (wf *WALFile) Sync() error {
	wf.mu.Lock()
	defer wf.mu.Unlock()
	return wf.f.Sync()
}

// Close closes the WAL file.
func (wf *WALFile) Close() error {
	wf.mu.Lock()
	defer wf.mu.Unlock()
	return wf.f.Close()
}

// Truncate resets the WAL file to just the header (after a checkpoint).
func (wf *WALFile) Truncate() error {
	wf.mu.Lock()
	defer wf.mu.Unlock()
	if err := wf.f.Truncate(WALFileHdrSize); err != nil {
		return err
	}
	wf.writePos = WALFileHdrSize
	return wf.f.Sync()
}

// NextLSN returns the next LSN that will be assigned.
func (wf *WALFile) NextLSN() LSN {
	wf.mu.Lock()
	defer wf.mu.Unlock()
	return wf.nextLSN
}

// SetNextLSN allows recovery to set the LSN counter.
func (wf *WALFile) SetNextLSN(lsn LSN) {
	wf.mu.Lock()
	defer wf.mu.Unlock()
	wf.nextLSN = lsn
}

// ───────────────────────────────────────────────────────────────────────────
// Serialization
// ───────────────────────────────────────────────────────────────────────────

func marshalWALRecord(rec *WALRecord) []byte {
	dataLen := len(rec.Data)
	buf := make([]byte, WALRecHdrSize+dataLen)
	buf[0] = byte(rec.Type)
	// bytes 1..4 reserved (flags)
	binary.LittleEndian.PutUint64(buf[5:13], uint64(rec.LSN))
	binary.LittleEndian.PutUint64(buf[13:21], uint64(rec.TxID))
	binary.LittleEndian.PutUint32(buf[21:25], uint32(rec.PageID))
	binary.LittleEndian.PutUint32(buf[25:29], uint32(dataLen))
	// CRC placeholder at [29:33]
	if dataLen > 0 {
		copy(buf[WALRecHdrSize:], rec.Data)
	}
	// Compute CRC over entire record with CRC field zeroed.
	h := crc32.New(crcTable)
	h.Write(buf[:29])
	h.Write([]byte{0, 0, 0, 0})
	h.Write(buf[WALRecHdrSize:])
	binary.LittleEndian.PutUint32(buf[29:33], h.Sum32())
	return buf
}

func unmarshalWALRecord(r io.Reader) (*WALRecord, error) {
	var hdr [WALRecHdrSize]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, err
	}
	rec := &WALRecord{
		Type:   WALRecordType(hdr[0]),
		LSN:    LSN(binary.LittleEndian.Uint64(hdr[5:13])),
		TxID:   TxID(binary.LittleEndian.Uint64(hdr[13:21])),
		PageID: PageID(binary.LittleEndian.Uint32(hdr[21:25])),
	}
	dataLen := int(binary.LittleEndian.Uint32(hdr[25:29]))
	storedCRC := binary.LittleEndian.Uint32(hdr[29:33])

	var data []byte
	if dataLen > 0 {
		data = make([]byte, dataLen)
		if _, err := io.ReadFull(r, data); err != nil {
			return nil, fmt.Errorf("WAL record data: %w", err)
		}
		rec.Data = data
	}

	// Verify CRC.
	h := crc32.New(crcTable)
	h.Write(hdr[:29])
	h.Write([]byte{0, 0, 0, 0})
	if data != nil {
		h.Write(data)
	}
	if h.Sum32() != storedCRC {
		return nil, fmt.Errorf("WAL record CRC mismatch at LSN %d", rec.LSN)
	}

	return rec, nil
}

// ReadAllRecords reads all WAL records from the file (after the header).
// Partial/corrupt records at the tail are silently ignored (crash truncation).
func ReadAllRecords(path string) ([]*WALRecord, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	// Skip the file header.
	if _, err := f.Seek(WALFileHdrSize, io.SeekStart); err != nil {
		return nil, err
	}

	var records []*WALRecord
	for {
		rec, err := unmarshalWALRecord(f)
		if err != nil {
			// EOF or corrupt tail — stop.
			break
		}
		records = append(records, rec)
	}
	return records, nil
}
