package storage

import (
	"encoding/binary"
	"hash/crc32"
	"io"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Freeze the previous complete CRC stream, including header and descriptors.
func referenceWALChecksum(record *WALRecord) uint32 {
	h := crc32.New(walCRCTable)
	var b [8]byte
	writeU64 := func(v uint64) {
		binary.LittleEndian.PutUint64(b[:], v)
		_, _ = h.Write(b[:])
	}
	writeU64(uint64(record.LSN))
	writeU64(uint64(record.TxID))
	_, _ = h.Write([]byte{byte(record.OpType)})
	_, _ = io.WriteString(h, record.Tenant)
	_, _ = h.Write([]byte{0})
	_, _ = io.WriteString(h, record.Table)
	_, _ = h.Write([]byte{0})
	writeU64(uint64(record.RowID))
	writeU64(uint64(record.Timestamp.UnixNano()))
	// scratch is reused across every value/column below instead of each
	// hashWALValue call declaring its own local buffer: a buffer whose slice
	// is handed to h.Write (an interface method) escapes to the heap, so one
	// shared, heap-allocated-once buffer beats one fresh escape per value.
	var scratch [40]byte
	hashWALImage(h, record.BeforeImage, &scratch)
	hashWALImage(h, record.AfterImage, &scratch)
	// One buffer + one Write per column instead of five separate calls
	// (three of them single-byte literals, one a dynamic-string conversion):
	// each separate call to h.Write on the interface is a distinct potential
	// heap escape, whereas building "c<name>;<type>;" once in scratch and
	// writing it in a single call amortizes to the one shared allocation.
	for _, c := range record.Columns {
		b := append(scratch[:0], 'c')
		b = append(b, c.Name...)
		b = append(b, ';')
		b = strconv.AppendInt(b, int64(c.Type), 10)
		b = append(b, ';')
		_, _ = h.Write(b)
	}
	return h.Sum32()
}

func TestWALChecksumWriterMatchesPreviousCRC(t *testing.T) {
	images := [][]any{nil, {}, {nil, true, false, int64(-1), uint32(7), 1.25},
		{strings.Repeat("payload", 1000), []byte{0, 1, 255}, time.Unix(123, 456)},
		{[]float64{1, 2, 3}, []any{nil, "nested"}, map[string]any{"z": 7, "a": "first"}}}
	for _, before := range images {
		for _, after := range images {
			r := &WALRecord{LSN: 77, TxID: 9, OpType: WALOpUpdate, Tenant: strings.Repeat("tenant", 100), Table: "Mixed", RowID: -1, Timestamp: time.Unix(33, 55), BeforeImage: before, AfterImage: after, Columns: []Column{{Name: strings.Repeat("column", 100), Type: TextType}, {Name: "id", Type: IntType}}}
			if got, want := (&AdvancedWAL{}).calculateChecksum(r), referenceWALChecksum(r); got != want {
				t.Fatalf("CRC changed: %x != %x", got, want)
			}
		}
	}
}
