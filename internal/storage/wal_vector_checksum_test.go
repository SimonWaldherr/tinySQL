package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math"
	"testing"
	"time"
)

// historicalWALVectorBytes reconstructs the vector's pre-batching wire bytes
// independently of hashWALValue, including the exact IEEE-754 bit patterns.
func historicalWALVectorBytes(vector []float64) []byte {
	data := []byte{'V'}
	for _, value := range vector {
		data = binary.LittleEndian.AppendUint64(data, math.Float64bits(value))
	}
	return data
}

func TestWALVectorChecksumByteStream(t *testing.T) {
	bits := []uint64{
		0, 1 << 63, 0x7ff0000000000000, 0xfff0000000000000,
		0x7ff8000000000001, 0x7ff8000000001234, 0x0000000000000001,
		0x3ff0000000000000, 0xbff8000000000000,
	}
	for _, n := range []int{0, 1, 4, 5, 6, 9, 10, 11, 64, 768, 1536} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			vector := make([]float64, n)
			for i := range vector {
				vector[i] = math.Float64frombits(bits[i%len(bits)])
			}
			vectorBytes := historicalWALVectorBytes(vector)
			value := []any{vector, "tail", map[string]any{"z": vector, "a": int64(-7)}, []any{vector, nil}}
			want := []byte{'['}
			want = append(want, vectorBytes...)
			want = append(want, `stringtail;{"a":int64-7;"z":`...)
			want = append(want, vectorBytes...)
			want = append(want, '}', '[')
			want = append(want, vectorBytes...)
			want = append(want, []byte("n;]]")...)

			var got bytes.Buffer
			var scratch [40]byte
			hashWALValue(&got, value, &scratch)
			if !bytes.Equal(got.Bytes(), want) {
				t.Fatalf("vector length %d changed the historical WAL bytes", n)
			}

			// Exercise the complete checksum boundary, including the shared
			// scratch being reused for a preceding value and column descriptor.
			record := &WALRecord{
				LSN: 77, TxID: 9, OpType: WALOpUpdate, Tenant: "tenant", Table: "vectors",
				RowID: -1, Timestamp: time.Unix(33, 55), BeforeImage: nil,
				AfterImage: []any{int64(42), value}, Columns: []Column{{Name: "embedding", Type: VectorType}},
			}
			var stream []byte
			stream = binary.LittleEndian.AppendUint64(stream, uint64(record.LSN))
			stream = binary.LittleEndian.AppendUint64(stream, uint64(record.TxID))
			stream = append(stream, byte(record.OpType))
			stream = append(stream, "tenant\x00vectors\x00"...)
			stream = binary.LittleEndian.AppendUint64(stream, uint64(record.RowID))
			stream = binary.LittleEndian.AppendUint64(stream, uint64(record.Timestamp.UnixNano()))
			stream = append(stream, "~[int6442;"...)
			stream = append(stream, want...)
			stream = append(stream, ']')
			stream = fmt.Appendf(stream, "cembedding;%d;", VectorType)
			expected := crc32.Checksum(stream, crc32.MakeTable(crc32.Castagnoli))
			if got := (&AdvancedWAL{}).calculateChecksum(record); got != expected {
				t.Fatalf("vector length %d checksum = %08x, want %08x", n, got, expected)
			}
		})
	}
}

var walVectorChecksumBenchmarkSink uint32

func BenchmarkAdvancedWALCalculateChecksumVector(b *testing.B) {
	for _, dims := range []int{0, 4, 8, 64, 768, 1536} {
		b.Run(fmt.Sprintf("dims%d", dims), func(b *testing.B) {
			vector := make([]float64, dims)
			for i := range vector {
				vector[i] = float64(i) / 16
			}
			record := &WALRecord{
				LSN: 1, TxID: 1, OpType: WALOpInsert, Tenant: "default", Table: "docs",
				RowID: 1, AfterImage: []any{int64(1), vector},
				Columns: []Column{{Name: "id", Type: IntType}, {Name: "embedding", Type: VectorType}},
			}
			wal := &AdvancedWAL{}
			b.ReportAllocs()
			for b.Loop() {
				walVectorChecksumBenchmarkSink = wal.calculateChecksum(record)
			}
		})
	}
}
