package engine

import (
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func BenchmarkLikeMixedAnchors(b *testing.B) {
	for _, tc := range []struct {
		name, pattern, text string
		want                bool
	}{
		{"suffix_match", "%event_2026.json", strings.Repeat("payload ", 512) + "event_2026.json", true},
		{"suffix_miss", "%event_2026.json", strings.Repeat("payload ", 512) + "event_2025.json", false},
		{"repeated_prefix", "%" + strings.Repeat("a", 128) + "_b", strings.Repeat("a", 4096), false},
		{"both_anchors", "doc_%event_2026.json", "doc_X" + strings.Repeat("payload ", 512) + "event_2026.json", true},
		{"interior_control", "%event_2026%", strings.Repeat("payload ", 64) + "event_2026.json", true},
		{"literal_control", "%event.json", strings.Repeat("payload ", 512) + "event.json", true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			match := compileLikeStringMatcher(tc.pattern, false)
			b.ReportAllocs()
			for b.Loop() {
				if got := match(tc.text); got != tc.want {
					b.Fatal(got)
				}
			}
		})
	}
}

func BenchmarkLikeMixedAnchorScan(b *testing.B) {
	benchmarkLikeMixedAnchorScan(b, "LIKE")
}

func BenchmarkILikeMixedAnchorScan(b *testing.B) {
	benchmarkLikeMixedAnchorScan(b, "ILIKE")
}

func benchmarkLikeMixedAnchorScan(b *testing.B, operator string) {
	db := likeAnchorScanFixture(b)
	pattern := "%event_2026.json"
	if operator == "ILIKE" {
		pattern = "%EVENT_2026.JSON"
	}
	stmt := mustParse("SELECT COUNT(*) AS n FROM messages WHERE body " + operator + " '" + pattern + "'")
	b.ReportAllocs()
	for b.Loop() {
		rs, err := Execute(b.Context(), db, "default", stmt)
		if err != nil || len(rs.Rows) != 1 || rs.Rows[0]["n"] != 5000 {
			b.Fatalf("unexpected result: %#v, %v", rs, err)
		}
	}
}

func BenchmarkILikeAnchors(b *testing.B) {
	for _, tc := range []struct {
		name, pattern, text string
		want                bool
	}{
		{"literal_suffix", "%EVENT.JSON", strings.Repeat("PAYLOAD ", 512) + "event.json", true},
		{"mixed_suffix", "%EVENT_2026.JSON", strings.Repeat("PAYLOAD ", 512) + "event漢2026.json", true},
		{"suffix_miss", "%EVENT_2026.JSON", strings.Repeat("PAYLOAD ", 512) + "event漢2025.json", false},
		{"both_anchors", "DOC_%EVENT_2026.JSON", "doc_漢" + strings.Repeat("PAYLOAD ", 512) + "event漢2026.json", true},
		{"unicode_suffix", "%İ_Σ.JSON", strings.Repeat("PAYLOAD ", 512) + "İ漢Σ.json", true},
		{"interior_control", "%EVENT_2026%", strings.Repeat("PAYLOAD ", 64) + "event漢2026.json", true},
		{"prefix_control", "PAYLOAD%", strings.Repeat("PAYLOAD ", 512), true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			match := compileLikeStringMatcher(tc.pattern, true)
			b.ReportAllocs()
			for b.Loop() {
				if got := match(tc.text); got != tc.want {
					b.Fatal(got)
				}
			}
		})
	}
}

func likeAnchorScanFixture(t testing.TB) *storage.DB {
	t.Helper()
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	table := storage.NewTable("messages", []storage.Column{{Name: "body", Type: storage.TextType}}, false)
	prefix := strings.Repeat("payload ", 64)
	for i := range 10000 {
		suffix := "event_2025.json"
		if i%2 == 0 {
			suffix = "event_2026.json"
		}
		table.Rows = append(table.Rows, []any{prefix + suffix})
	}
	if err := db.Put("default", table); err != nil {
		t.Fatal(err)
	}
	return db
}

func TestLikeMixedAnchorQueryProfile(t *testing.T) {
	db := likeAnchorScanFixture(t)
	rs := execSQL(t, db, "EXPLAIN ANALYZE SELECT COUNT(*) AS n FROM messages WHERE body LIKE '%event_2026.json'")
	found := false
	for _, row := range rs.Rows {
		if row["total_ns"] != nil {
			t.Logf("%s: self=%v ns, share=%.2f%%, bottleneck=%v", row["operation"], row["self_ns"], row["percent"], row["bottleneck"])
		}
		if row["operation"] == "SCAN/FILTER/AGGREGATE" {
			found = true
			if row["actual_rows"] != int64(1) {
				t.Fatalf("unexpected aggregate profile: %v", row)
			}
		}
	}
	if !found {
		t.Fatal("missing aggregate scan profile")
	}
}
