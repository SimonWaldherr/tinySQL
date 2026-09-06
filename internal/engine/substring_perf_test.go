package engine

import (
	"strings"
	"testing"
)

func BenchmarkSubstringShortWindow(b *testing.B) {
	for _, text := range []struct{ name, value string }{
		{"ascii", strings.Repeat("abcdef", 10000)},
		{"unicode", strings.Repeat("é漢🙂", 10000)},
	} {
		b.Run(text.name, func(b *testing.B) {
			args := []Expr{&Literal{Val: text.value}, &Literal{Val: 3}, &Literal{Val: 4}}
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := evalSubstring(ExecEnv{}, args, nil); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestSubstringWindowMatchesRuneReference(t *testing.T) {
	maxInt := int(^uint(0) >> 1)
	for _, text := range []string{"", "abcdef", "é漢🙂abc", "\xffa\xc0\xaf漢\xfe", strings.Repeat("é漢🙂", 1000)} {
		chars := []rune(text)
		for _, start := range []int{-maxInt - 1, -len(chars) - 1, -2, -1, 0, 1, 2, len(chars), len(chars) + 1, maxInt} {
			offset := start
			if offset > 0 {
				offset--
			} else if offset < 0 {
				offset += len(chars)
			}
			if offset < 0 {
				offset = 0
			}
			for _, length := range []int{-1, 0, 1, 4, maxInt} {
				for _, hasLength := range []bool{false, true} {
					want := ""
					if offset < len(chars) {
						end := len(chars)
						if hasLength {
							if length <= 0 {
								end = offset
							} else if length < end-offset {
								end = offset + length
							}
						}
						want = string(chars[offset:end])
					}
					args := []Expr{&Literal{Val: text}, &Literal{Val: start}}
					if hasLength {
						args = append(args, &Literal{Val: length})
					}
					got, err := evalSubstring(ExecEnv{}, args, nil)
					if err != nil || got != want {
						t.Fatalf("start=%d length=%d supplied=%t: got %q, %v; want %q", start, length, hasLength, got, err, want)
					}
				}
			}
		}
	}
}
