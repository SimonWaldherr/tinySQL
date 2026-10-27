package main

import (
	"math/rand"
	"strings"
	"testing"

	tinysql "github.com/SimonWaldherr/tinySQL"
)

func TestResultContainsFoldedMatchesGoLowercase(t *testing.T) {
	cases := []struct{ text, needle string }{
		{"", ""}, {"ASCII Result", "result"}, {"ASCII Result", "RESULT"},
		{"İstanbul", "istanbul"}, {"Kelvin", "kelvin"}, {"Straße", "straße"},
		{"AKB", "akb"}, {"abc\xffXYZ", "xyz"},
		{strings.Repeat("A", 128), strings.Repeat("a", 16)},
		{strings.Repeat("A", 129), strings.Repeat("a", 17)},
		{strings.Repeat("a", 127) + "b", strings.Repeat("a", 15) + "c"},
	}
	for _, tc := range cases {
		want := strings.Contains(strings.ToLower(tc.text), tc.needle)
		if got := resultContainsFolded(tc.text, tc.needle); got != want {
			t.Errorf("text %q needle %q: got %t, want %t", tc.text, tc.needle, got, want)
		}
	}
	// Exercise arbitrary byte sequences as well as malformed UTF-8, as the
	// Unicode fallback must preserve strings.ToLower's exact behavior.
	rng := rand.New(rand.NewSource(1))
	alphabet := []byte("abcXYZ019- \xc4\xb0\xe2\x84\xaa\xff")
	for i := 0; i < 10000; i++ {
		text := make([]byte, rng.Intn(150))
		needle := make([]byte, rng.Intn(22))
		for j := range text {
			text[j] = alphabet[rng.Intn(len(alphabet))]
		}
		for j := range needle {
			needle[j] = alphabet[rng.Intn(len(alphabet))]
		}
		want := strings.Contains(strings.ToLower(string(text)), string(needle))
		if got := resultContainsFolded(string(text), string(needle)); got != want {
			t.Fatalf("text %q needle %q: got %t, want %t", text, needle, got, want)
		}
	}
}

func BenchmarkResultViewCaseInsensitiveFilter(b *testing.B) {
	for _, tc := range []struct{ name, text, needle string }{
		{"mixed_ascii", "Jane Smith, Berlin, Germany", "smith"},
		{"lower_ascii", "jane smith, berlin, germany", "smith"},
		{"unicode", "İstanbul München Kelvin", "istanbul"},
		{"long_text", strings.Repeat("a", 4096) + "B", strings.Repeat("a", 16) + "c"},
	} {
		b.Run(tc.name, func(b *testing.B) {
			row := tinysql.Row{"text": tc.text}
			columns := []string{"text"}
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				resultRowContains(row, columns, tc.needle)
			}
		})
	}
}
