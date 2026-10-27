package engine

import (
	"math/rand"
	"strings"
	"testing"
)

func TestLikeAnchorsMatchRuneReference(t *testing.T) {
	patterns := []string{
		"%event_2026.json", "doc_%event_2026.json", "a_%_b", "_%_", "__%__", "a_%b_%c_",
		"%é_🙂", "é_%_🙂", "%_", "_%", "a_%", "%__x", "%___", "%%%_%%%_", "%_%%_x",
		`a\_%_b`, `a_%\_b`, `a\%%_b`, `a_%\%b`, `%_\\`, `%_\`, `%_\é`, `%_\_`, `%_\%`,
		"%İ_Σ", "%�_", "%\xff_", "%i", "%Σ", "%İ", "%K", "%ß", "_%i", "i_%Σ",
		"%_i", "i_", "_%i_%Σ", "%i_Σ%tail_", "doc_%i_Σ.json",
	}
	texts := []string{
		"", "a", "ab", "abc", "a_b", "a%b", "aab", "abcd", "a%ab", "aa_b", "aa%b",
		"doc_Xevent_2026.json", "doc_event_2026.json", "a\nb", "a\x00b", "a\xffb", "a\xff\xfeb",
		"é漢🙂", "é\xff🙂", "é🙂", "a\\", "\\", "aé", "a_", "a%", "aİxΣ", "aixς", "�x", "\xffx",
		"İ", "i", "Σ", "σ", "ς", "K", "K", "ß", "ẞ", "İ漢Σ", "İ\xffΣ", "\xffİ", "İ\xff",
		"doc_Xİ漢Σ.json", "İ漢ΣpayloadTAIL日", "éİ漢Σ", "İaİbΣ",
	}
	for _, pattern := range patterns {
		for _, insensitive := range []bool{false, true} {
			match := compileLikeStringMatcher(pattern, insensitive)
			for _, text := range texts {
				assertLikeReference(t, match, pattern, text, insensitive)
			}
		}
	}

	// Generate matching and failing anchors around arbitrary wildcard cores.
	// Include malformed UTF-8 input: _ must still consume one decoded rune.
	rng := rand.New(rand.NewSource(20261027))
	alphabet := []string{"a", "A", "é", "🙂", "%", "_", "\\", "\xff", "\n"}
	word := func() string {
		var b strings.Builder
		for n := rng.Intn(10); n > 0; n-- {
			b.WriteString(alphabet[rng.Intn(len(alphabet))])
		}
		return b.String()
	}
	for range 10000 {
		pattern, text := "a_"+word()+"%"+word()+"_é", "a"+word()+word()+"é"
		for _, insensitive := range []bool{false, true} {
			assertLikeReference(t, compileLikeStringMatcher(pattern, insensitive), pattern, text, insensitive)
		}
	}
}

func TestILikeAnchorsAvoidInputCopies(t *testing.T) {
	body := strings.Repeat("PAYLOAD ", 512)
	for _, tc := range []struct {
		name, pattern, text string
		want                bool
	}{
		{"literal_suffix", "%EVENT.JSON", body + "event.json", true},
		{"mixed_suffix", "%EVENT_2026.JSON", body + "event漢2026.json", true},
		{"suffix_miss", "%EVENT_2026.JSON", body + "event漢2025.json", false},
		{"both_anchors", "DOC_%EVENT_2026.JSON", "doc_漢" + body + "event漢2026.json", true},
		{"invalid_utf8", "%İ_Σ.JSON", body + "İ\xffΣ.json", true},
		{"prefix_miss", "DOC_%İ_Σ.JSON", "mismatch" + body + "İ漢Σ.json", false},
		{"fixed_width", "DOCUMENT____.JSON", "document２０２６.json", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			match := compileLikeStringMatcher(tc.pattern, true)
			assertLikeReference(t, match, tc.pattern, tc.text, true)
			allocs := testing.AllocsPerRun(100, func() {
				if got := match(tc.text); got != tc.want {
					t.Fatalf("match = %v, want %v", got, tc.want)
				}
			})
			if allocs != 0 {
				t.Fatalf("matcher allocated %.0f times per call", allocs)
			}
		})
	}
}

func assertLikeReference(t *testing.T, match func(string) bool, pattern, text string, insensitive bool) {
	t.Helper()
	s, p := text, pattern
	if insensitive {
		s, p = strings.ToLower(s), strings.ToLower(p)
	}
	if got, want := match(text), matchLikePattern(s, p, '\\'); got != want {
		t.Fatalf("%q LIKE %q, insensitive=%v: got %v, want %v", text, pattern, insensitive, got, want)
	}
}

func FuzzLikeAnchors(f *testing.F) {
	for _, seed := range [][2]string{{"%é_🙂", "é漢🙂"}, {"a_%_b", "a\xffb"}, {`%_\%`, "x%"}, {"_%_", "🙂"}, {"doc_%İ_Σ.json", "DOC_漢İ\xffΣ.JSON"}} {
		f.Add(seed[0], seed[1], false)
		f.Add(seed[0], seed[1], true)
	}
	f.Fuzz(func(t *testing.T, pattern, text string, insensitive bool) {
		// Bound the reference matcher's quadratic worst case during fuzzing.
		if len(pattern) > 512 || len(text) > 2048 {
			t.Skip()
		}
		assertLikeReference(t, compileLikeStringMatcher(pattern, insensitive), pattern, text, insensitive)
	})
}
