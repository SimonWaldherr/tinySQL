package pager

import (
	"bytes"
	"math"
	"testing"
)

func TestRowCodec_RoundTrip(t *testing.T) {
	tests := []struct {
		name string
		row  []any
	}{
		{"nil-only", []any{nil, nil}},
		{"int-string-float", []any{float64(42), "hello", 3.14}},
		{"bool-values", []any{true, false}},
		{"empty-string", []any{""}},
		{"bytes", []any{[]byte{0xDE, 0xAD}}},
		{"empty-bytes", []any{[]byte{}}},
		{"large-int", []any{float64(math.MaxInt32)}},
		{"negative-float", []any{float64(-1.5)}},
		{"mixed", []any{float64(1), "two", 3.0, nil, true, []byte("bin")}},
		{"empty-row", []any{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := MarshalRow(tt.row, nil)
			decoded, err := UnmarshalRow(encoded)
			if err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			if len(decoded) != len(tt.row) {
				t.Fatalf("length mismatch: got %d, want %d", len(decoded), len(tt.row))
			}
			for i := range tt.row {
				got := decoded[i]
				want := tt.row[i]
				switch w := want.(type) {
				case nil:
					if got != nil {
						t.Errorf("[%d] got %v, want nil", i, got)
					}
				case bool:
					if g, ok := got.(bool); !ok || g != w {
						t.Errorf("[%d] got %v, want %v", i, got, want)
					}
				case float64:
					if g, ok := got.(float64); !ok || g != w {
						t.Errorf("[%d] got %v, want %v", i, got, want)
					}
				case string:
					if g, ok := got.(string); !ok || g != w {
						t.Errorf("[%d] got %q, want %q", i, got, want)
					}
				case []byte:
					g, ok := got.([]byte)
					if !ok || !bytes.Equal(g, w) {
						t.Errorf("[%d] got %v, want %v", i, got, want)
					}
				}
			}
		})
	}
}

func TestRowCodecRejectsTrailingAndOversizeValues(t *testing.T) {
	valid := MarshalRow([]any{[]byte("tile")}, nil)
	if _, err := UnmarshalRow(append(valid, 0xff)); err == nil {
		t.Fatal("trailing row data unexpectedly accepted")
	}
	// One BLOB column with the long-byte tag and a corrupt 64 MiB+1 length.
	corrupt := []byte{1, 0, tagLongBytes, 1, 0, 0, 4}
	if _, err := UnmarshalRow(corrupt); err == nil {
		t.Fatal("oversize BLOB length unexpectedly accepted")
	}
}

func TestRowCodec_VectorRoundTrip(t *testing.T) {
	vec := make([]float64, 384)
	for i := range vec {
		vec[i] = math.Sin(float64(i)) * 1e3
	}
	row := []any{float64(7), "doc-1", vec, []float64{}, nil}
	decoded, err := UnmarshalRow(MarshalRow(row, nil))
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	got, ok := decoded[2].([]float64)
	if !ok {
		t.Fatalf("expected []float64, got %T", decoded[2])
	}
	if len(got) != len(vec) {
		t.Fatalf("length mismatch: got %d, want %d", len(got), len(vec))
	}
	for i := range vec {
		if got[i] != vec[i] {
			t.Fatalf("[%d] got %v, want %v (must be bit-exact)", i, got[i], vec[i])
		}
	}
	if empty, ok := decoded[3].([]float64); !ok || len(empty) != 0 {
		t.Fatalf("empty vector round-trip failed: %#v", decoded[3])
	}
}

func TestRowCodec_LongStringAndBytes(t *testing.T) {
	long := string(make([]byte, 70000))
	longBytes := make([]byte, 70000)
	longBytes[69999] = 0xAB
	row := []any{long, longBytes}
	decoded, err := UnmarshalRow(MarshalRow(row, nil))
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if s, ok := decoded[0].(string); !ok || len(s) != 70000 {
		t.Fatalf("long string round-trip failed: %T len=%d", decoded[0], len(decoded[0].(string)))
	}
	b, ok := decoded[1].([]byte)
	if !ok || len(b) != 70000 || b[69999] != 0xAB {
		t.Fatalf("long bytes round-trip failed")
	}
}

func BenchmarkMarshalRowVector(b *testing.B) {
	vec := make([]float64, 768)
	for i := range vec {
		vec[i] = float64(i) * 0.001
	}
	row := []any{float64(1), "doc", vec}
	var buf []byte
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		buf = MarshalRow(row, buf)
	}
}

func TestRowCodec_BufferReuse(t *testing.T) {
	row := []any{float64(1), "test", 2.5}
	buf := MarshalRow(row, nil)
	// Reuse the buffer.
	buf2 := MarshalRow(row, buf)
	if &buf[0] != &buf2[0] || len(buf) != len(buf2) {
		t.Fatalf("expected MarshalRow to reuse the supplied buffer")
	}
	decoded, err := UnmarshalRow(buf2)
	if err != nil {
		t.Fatal(err)
	}
	if len(decoded) != 3 {
		t.Fatalf("expected 3 columns, got %d", len(decoded))
	}
}

func BenchmarkMarshalRow(b *testing.B) {
	row := []any{float64(42), "user_12345", 98.7}
	var buf []byte
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		buf = MarshalRow(row, buf)
	}
}

func BenchmarkUnmarshalRow(b *testing.B) {
	row := []any{float64(42), "user_12345", 98.7}
	data := MarshalRow(row, nil)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = UnmarshalRow(data)
	}
}
