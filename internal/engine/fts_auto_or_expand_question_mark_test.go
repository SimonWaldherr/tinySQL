// Regression coverage for a real-world bug report: a trailing "?" on a
// natural-language question silently zeroed the keyword half of every
// hybrid-search score, since ftsAutoOrExpand's word splitter deliberately
// keeps '?' out of its split set (to let a genuine mid-word wildcard like
// "wom?n" survive), but was not trimming a leftover leading/trailing '?'
// off each extracted term the way it already trimmed '-'. The last word of
// almost any real question ("...menu?") became "menu?", which
// ftsCompileWildcard reads as "menu" plus exactly one more required
// character — a pattern the plain indexed token "menu" can never satisfy.
package engine

import (
	"context"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestFtsAutoOrExpandTrailingQuestionMark(t *testing.T) {
	withMark := ftsAutoOrExpand("was steht auf der Speisekarte?")
	withoutMark := ftsAutoOrExpand("was steht auf der Speisekarte")

	if withMark != withoutMark {
		t.Fatalf("trailing '?' changed the expanded query: %q (with) vs %q (without)", withMark, withoutMark)
	}
	if strings.Contains(withMark, "?") {
		t.Fatalf("expanded query still contains a stray '?': %q", withMark)
	}
	if !strings.Contains(withMark, "speisekarte") {
		t.Fatalf("expected \"speisekarte\" as a plain OR term, got %q", withMark)
	}

	// A genuine mid-word wildcard must still survive: only a leading/
	// trailing '?' is punctuation noise, not one that appears mid-token.
	mid := ftsAutoOrExpand("find the wom?n in accounting")
	if !strings.Contains(mid, "wom?n") {
		t.Fatalf("expected mid-word wildcard \"wom?n\" preserved, got %q", mid)
	}

	// A deliberate trailing prefix wildcard ('*') must still survive —
	// only '?' collides with natural sentence-ending punctuation often
	// enough to warrant trimming it specifically.
	prefix := ftsAutoOrExpand("find anything starting with program*")
	if !strings.Contains(prefix, "program*") {
		t.Fatalf("expected trailing wildcard \"program*\" preserved, got %q", prefix)
	}
}

// TestHybridSearchTrailingQuestionMarkDoesNotZeroKeywordScore reproduces the
// downstream failure end to end through HYBRID_SEARCH (which defaults
// auto_or_expand to true): the same natural-language question, with and
// without a trailing "?", must both surface the same document via the
// keyword (FTS) pass, matching the reported "keyword=0.996 without '?' vs
// keyword=0.000 with '?', same chunk" symptom.
func TestHybridSearchTrailingQuestionMarkDoesNotZeroKeywordScore(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()

	Execute(ctx, db, "default", mustParse(`
		CREATE TABLE menu_docs (id INT PRIMARY KEY, content TEXT, embedding VECTOR)
	`))
	Execute(ctx, db, "default", mustParse(`
		INSERT INTO menu_docs VALUES
			(1, 'Heute gibt es Schnitzel auf der Speisekarte', '[1.0, 0.0]'),
			(2, 'Das Wetter ist heute schoen', '[0.0, 1.0]')
	`))

	runQuery := func(query string) *ResultSet {
		return execSQL(t, db, `
			SELECT * FROM HYBRID_SEARCH('menu_docs', 'embedding', 'content', '`+query+`',
				VEC_FROM_JSON('[1.0, 0.0]'), 2)
		`)
	}

	ftsRankFor := func(rs *ResultSet, id int) (any, bool) {
		for _, r := range rs.Rows {
			if r["id"] == id {
				v, ok := r["_fts_rank"]
				return v, ok && v != nil
			}
		}
		return nil, false
	}

	withoutMark := runQuery("was steht auf der Speisekarte")
	_, hasRankWithout := ftsRankFor(withoutMark, 1)
	if !hasRankWithout {
		t.Fatalf("expected doc 1 to carry a non-null _fts_rank without trailing '?', rows: %#v", withoutMark.Rows)
	}

	withMark := runQuery("was steht auf der Speisekarte?")
	_, hasRankWith := ftsRankFor(withMark, 1)
	if !hasRankWith {
		t.Fatalf("expected doc 1 to carry a non-null _fts_rank WITH trailing '?' too (regression), rows: %#v", withMark.Rows)
	}
}
