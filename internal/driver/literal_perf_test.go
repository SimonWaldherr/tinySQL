package driver

import (
	"database/sql/driver"
	"encoding/hex"
	"fmt"
	"math/rand"
	"strings"
	"testing"
)

func TestSQLLiteralEscaping(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	sizes := []int{0, 1, 255, 256, 257, 511, 512, 513, 4096}
	for size := 0; size < 2048; size += 17 {
		sizes = append(sizes, size)
	}
	for _, size := range sizes {
		data := make([]byte, size)
		rng.Read(data)
		for _, s := range []string{string(data), strings.Repeat("'", size), "Grüße '世界'\x00"} {
			want := "'" + strings.ReplaceAll(s, "'", "''") + "'"
			if got := sqlLiteral(s); got != want {
				t.Fatalf("string size %d: got %q, want %q", size, got, want)
			}
		}
		want := "X'" + hex.EncodeToString(data) + "'"
		if got := sqlLiteral(data); got != want {
			t.Fatalf("blob size %d: mismatch", size)
		}
	}
	if got := sqlLiteral([]byte(nil)); got != "X''" {
		t.Fatalf("nil blob: %q", got)
	}
	if got := sqlLiteral(map[string]string{"key": "it's"}); got != "'{\"key\":\"it''s\"}'" {
		t.Fatalf("JSON: %q", got)
	}
	value := complex(1, 2)
	if got, want := sqlLiteral(value), "'"+fmt.Sprint(value)+"'"; got != want {
		t.Fatalf("fallback: %q != %q", got, want)
	}
	args := []driver.NamedValue{{Value: "it's"}, {Value: []byte{0, 255}}}
	if got, err := bindPlaceholders("SELECT $2, :1, $2", args); err != nil || got != "SELECT X'00ff', 'it''s', X'00ff'" {
		t.Fatalf("repeated binding: %q, %v", got, err)
	}
}

var literalBenchmarkResult string

func BenchmarkDriverLiteralEncoding(b *testing.B) {
	for _, tc := range []struct {
		name  string
		value any
	}{
		{"PlainString", strings.Repeat("abcdef", 100)},
		{"QuotedString", strings.Repeat("it's a 'test' ", 50)},
		{"Blob4K", []byte(strings.Repeat("abcdef01", 512))},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				literalBenchmarkResult = sqlLiteral(tc.value)
			}
		})
	}
}
