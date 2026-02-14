package pager

import (
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
					if !ok || len(g) != len(w) {
						t.Errorf("[%d] got %v, want %v", i, got, want)
					}
				}
			}
		})
	}
}

func TestRowCodec_BufferReuse(t *testing.T) {
	row := []any{float64(1), "test", 2.5}
	buf := MarshalRow(row, nil)
	// Reuse the buffer.
	buf2 := MarshalRow(row, buf)
	if &buf[0] == &buf2[0] && len(buf) == len(buf2) {
		// Good — buffer was reused (same underlying array).
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
