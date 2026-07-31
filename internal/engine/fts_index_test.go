package engine

// Tests for the FTS inverted index (fts_index.go).
//
// The central claim these check is that restricting the candidate set is
// invisible: FTS_SEARCH must return exactly what a full corpus scan would.
// TestFTSCandidateRestrictionMatchesFullScan verifies that differentially
// against a reference implementation rather than by inspecting the index, which
// is what makes it robust to future changes in the restriction strategy.

import (
	"context"
	"fmt"
	"math"
	"sort"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// ftsReferenceTopK is the deliberately naive implementation: score every
// document in the corpus, keep the k best. It is what FTS_SEARCH did before the
// inverted index existed, and it is the oracle the index is checked against.
func ftsReferenceTopK(cache ftsDocCacheEntry, node *ftsQueryNode, k int) []ftsScored {
	idf := ftsIDFLookup(cache)
	heapRows := make(ftsScoredHeap, 0, k)
	for ri, doc := range cache.docs {
		if !doc.valid {
			continue
		}
		if node != nil && !ftsMatchNode(node, doc.freq, doc.tokens) {
			continue
		}
		normDocLen := doc.docLen
		if cache.avgDocLen > 0 {
			normDocLen = doc.docLen / cache.avgDocLen
		}
		ftsPushTopK(&heapRows, ri, ftsScoreNode(node, doc.freq, normDocLen, idf), k)
	}
	return ftsTopKFromHeap(&heapRows, k)
}

// TestFTSCandidateRestrictionMatchesFullScan is the core correctness test for
// the inverted index. For every query shape, the rows and scores FTS_SEARCH
// returns must equal those of a full scan over the identical (expanded) query
// tree — same rows, same order, same scores, bit for bit.
func TestFTSCandidateRestrictionMatchesFullScan(t *testing.T) {
	db := ragBenchCorpus(t)
	table, err := db.Get("default", "rag_chunks")
	if err != nil {
		t.Fatal(err)
	}
	textIdx, err := table.ColIndex("search_text")
	if err != nil {
		t.Fatal(err)
	}
	cache := getFTSDocCache("default", table, []int{textIdx})
	ctx := context.Background()

	const k = 12
	for _, tc := range ftsGoldenQueries {
		t.Run(tc.name, func(t *testing.T) {
			node := ftsParseQuery(tc.query)
			if node == nil {
				// Empty/all-stopword queries are short-circuited before the
				// index is consulted; nothing to compare.
				return
			}
			expanded := ftsExpandQuery(node, cache.postings)
			want := ftsReferenceTopK(cache, expanded, k)

			sql := fmt.Sprintf(
				`SELECT chunk_id, _fts_score FROM FTS_SEARCH('rag_chunks', %s, %d, 'search_text')`,
				sqlQuote(tc.query), k)
			rs, err := Execute(ctx, db, "default", mustParse(sql))
			if err != nil {
				t.Fatal(err)
			}

			if len(rs.Rows) != len(want) {
				t.Fatalf("FTS_SEARCH returned %d rows, full scan returned %d", len(rs.Rows), len(want))
			}
			idIdx, err := table.ColIndex("chunk_id")
			if err != nil {
				t.Fatal(err)
			}
			for i, r := range rs.Rows {
				gotID, _ := r["chunk_id"].(string)
				wantID, _ := table.Rows[want[i].rowIdx][idIdx].(string)
				if gotID != wantID {
					t.Errorf("rank %d: FTS_SEARCH returned %q, full scan returned %q",
						i+1, gotID, wantID)
				}
				if got := toFloatOrFail(t, r["_fts_score"]); got != want[i].score {
					t.Errorf("rank %d (%s): score %v != full-scan score %v",
						i+1, gotID, got, want[i].score)
				}
			}

			// Report whether this shape actually exercised restriction, so a
			// change that silently disables the index shows up as every shape
			// falling back.
			cands := ftsQueryCandidates(expanded, cache.postings, len(cache.docs))
			if cands.unrestricted {
				t.Logf("%s: unrestricted (full scan)", tc.query)
			} else {
				t.Logf("%s: restricted to %d of %d rows", tc.query, len(cands.rows), len(cache.docs))
			}
		})
	}
}

// TestFTSCandidatesAreSuperset checks the invariant restriction rests on
// directly: every row that genuinely matches must appear in the candidate set.
// A candidate set that is merely *equal* to the match set would also pass, but
// an unsound one that drops a real match fails here even if the top-k happens
// to be unaffected.
func TestFTSCandidatesAreSuperset(t *testing.T) {
	db := ragBenchCorpus(t)
	table, err := db.Get("default", "rag_chunks")
	if err != nil {
		t.Fatal(err)
	}
	textIdx, err := table.ColIndex("search_text")
	if err != nil {
		t.Fatal(err)
	}
	cache := getFTSDocCache("default", table, []int{textIdx})

	for _, tc := range ftsGoldenQueries {
		node := ftsParseQuery(tc.query)
		if node == nil {
			continue
		}
		expanded := ftsExpandQuery(node, cache.postings)
		cands := ftsQueryCandidates(expanded, cache.postings, len(cache.docs))
		if cands.unrestricted {
			continue // a full scan is trivially a superset
		}
		inCandidates := make(map[int32]bool, len(cands.rows))
		for _, ri := range cands.rows {
			inCandidates[ri] = true
		}
		// Candidate lists must be ascending and duplicate-free: the union and
		// intersection helpers require it of their inputs.
		for i := 1; i < len(cands.rows); i++ {
			if cands.rows[i] <= cands.rows[i-1] {
				t.Fatalf("%s: candidate rows not strictly ascending at %d: %d then %d",
					tc.query, i, cands.rows[i-1], cands.rows[i])
			}
		}
		missing := 0
		for ri, doc := range cache.docs {
			if !doc.valid || !ftsMatchNode(expanded, doc.freq, doc.tokens) {
				continue
			}
			if !inCandidates[int32(ri)] {
				missing++
				if missing <= 3 {
					t.Errorf("%s: row %d matches but is absent from the candidate set", tc.query, ri)
				}
			}
		}
		if missing > 0 {
			t.Errorf("%s: %d matching rows missing from candidates", tc.query, missing)
		}
	}
}

// TestFTSExpandedScoringMatchesPatternScoring checks the other half of the
// rewrite: resolving a wildcard against the dictionary must score a document
// the same as testing the pattern against that document's own tokens.
//
// The two sum the same set of per-term contributions but in different orders,
// and float64 addition is not associative, so an exact comparison would be
// wrong to demand. The tolerance is relative and tight — it catches a genuinely
// different term set while accepting last-bit reassociation.
func TestFTSExpandedScoringMatchesPatternScoring(t *testing.T) {
	db := ragBenchCorpus(t)
	table, err := db.Get("default", "rag_chunks")
	if err != nil {
		t.Fatal(err)
	}
	textIdx, err := table.ColIndex("search_text")
	if err != nil {
		t.Fatal(err)
	}
	cache := getFTSDocCache("default", table, []int{textIdx})
	idf := ftsIDFLookup(cache)

	patterns := []string{"needle4*", "term1*", "term1?", "term2_", "needle10%", "zzz*"}
	for _, p := range patterns {
		t.Run(p, func(t *testing.T) {
			original := ftsParseQuery(p)
			if original == nil {
				t.Fatalf("query %q did not parse", p)
			}
			expanded := ftsExpandQuery(original, cache.postings)

			compared := 0
			for _, doc := range cache.docs {
				if !doc.valid {
					continue
				}
				normDocLen := doc.docLen
				if cache.avgDocLen > 0 {
					normDocLen = doc.docLen / cache.avgDocLen
				}
				wantMatch := ftsMatchNode(original, doc.freq, doc.tokens)
				gotMatch := ftsMatchNode(expanded, doc.freq, doc.tokens)
				if wantMatch != gotMatch {
					t.Fatalf("match disagreement: pattern says %v, expanded says %v", wantMatch, gotMatch)
				}
				want := ftsScoreNode(original, doc.freq, normDocLen, idf)
				got := ftsScoreNode(expanded, doc.freq, normDocLen, idf)
				if want != 0 || got != 0 {
					if diff := math.Abs(want - got); diff > 1e-9*math.Max(math.Abs(want), 1) {
						t.Fatalf("score disagreement: pattern %v vs expanded %v (diff %g)", want, got, diff)
					}
					compared++
				}
			}
			t.Logf("%s: compared %d scored documents", p, compared)
		})
	}
}

// TestFTSExpandedTermsAreSorted pins the property that makes wildcard scores
// reproducible: the resolved term list is sorted, so the float sum always
// accumulates in the same order.
func TestFTSExpandedTermsAreSorted(t *testing.T) {
	postings := map[string][]int32{
		"delta": {1}, "alpha": {2}, "charlie": {3}, "bravo": {4}, "alphabet": {5},
	}
	node := ftsExpandQuery(&ftsQueryNode{op: "PREFIX", prefix: "alpha"}, postings)
	if node.op != ftsExpandedOp {
		t.Fatalf("PREFIX was not expanded, got op %q", node.op)
	}
	if !sort.StringsAreSorted(node.phrase) {
		t.Errorf("expanded terms not sorted: %v", node.phrase)
	}
	want := []string{"alpha", "alphabet"}
	if fmt.Sprint(node.phrase) != fmt.Sprint(want) {
		t.Errorf("expanded terms = %v, want %v", node.phrase, want)
	}
}

// TestFTSNegationForcesFullScan pins the soundness rule for NOT. A negated
// branch cannot bound the match set, and that must propagate: `a OR NOT b` has
// to fall back to a full scan, while `a AND NOT b` may still restrict to a.
func TestFTSNegationForcesFullScan(t *testing.T) {
	postings := map[string][]int32{
		"alpha": {1, 5, 9},
		"bravo": {2, 5},
	}
	const numRows = 100

	cases := []struct {
		query            string
		wantUnrestricted bool
		wantRows         []int32
	}{
		{"alpha", false, []int32{1, 5, 9}},
		{"NOT alpha", true, nil},
		{"alpha OR NOT bravo", true, nil},
		{"alpha AND NOT bravo", false, []int32{1, 5, 9}},
		{"alpha AND bravo", false, []int32{5}},
		{"alpha OR bravo", false, []int32{1, 2, 5, 9}},
	}
	for _, tc := range cases {
		node := ftsExpandQuery(ftsParseQuery(tc.query), postings)
		got := ftsQueryCandidates(node, postings, numRows)
		if got.unrestricted != tc.wantUnrestricted {
			t.Errorf("%s: unrestricted = %v, want %v", tc.query, got.unrestricted, tc.wantUnrestricted)
			continue
		}
		if tc.wantUnrestricted {
			continue
		}
		if fmt.Sprint(got.rows) != fmt.Sprint(tc.wantRows) {
			t.Errorf("%s: rows = %v, want %v", tc.query, got.rows, tc.wantRows)
		}
	}
}

// TestFTSBroadCandidateSetFallsBackWithoutAllocating documents the cost rule:
// a term covering most of the corpus yields unrestricted rather than a
// corpus-sized candidate list, because materializing one saves no scoring work
// and costs allocation a plain scan does not.
func TestFTSBroadCandidateSetFallsBackWithoutAllocating(t *testing.T) {
	const numRows = 1000
	everywhere := make([]int32, numRows)
	for i := range everywhere {
		everywhere[i] = int32(i)
	}
	postings := map[string][]int32{
		"common": everywhere,
		"rare":   {7, 42},
	}

	if got := ftsQueryCandidates(ftsParseQuery("common"), postings, numRows); !got.unrestricted {
		t.Errorf("a term in every row should fall back to a full scan, got %d rows", len(got.rows))
	}
	if got := ftsQueryCandidates(ftsParseQuery("common OR rare"), postings, numRows); !got.unrestricted {
		t.Errorf("OR with a corpus-wide term should fall back, got %d rows", len(got.rows))
	}
	// An intersection only shrinks, so a selective sibling must still restrict
	// even when the other side is corpus-wide.
	got := ftsQueryCandidates(ftsParseQuery("common AND rare"), postings, numRows)
	if got.unrestricted {
		t.Fatal("AND with a selective term should restrict, got unrestricted")
	}
	if fmt.Sprint(got.rows) != fmt.Sprint([]int32{7, 42}) {
		t.Errorf("rows = %v, want [7 42]", got.rows)
	}
}

func TestFTSUnionIntersectHelpers(t *testing.T) {
	cases := []struct {
		a, b                 []int32
		wantUnion, wantXsect []int32
	}{
		{nil, nil, nil, nil},
		{[]int32{1, 2, 3}, nil, []int32{1, 2, 3}, nil},
		{nil, []int32{4, 5}, []int32{4, 5}, nil},
		{[]int32{1, 3, 5}, []int32{2, 4, 6}, []int32{1, 2, 3, 4, 5, 6}, nil},
		{[]int32{1, 2, 3}, []int32{2, 3, 4}, []int32{1, 2, 3, 4}, []int32{2, 3}},
		{[]int32{1, 2, 3}, []int32{1, 2, 3}, []int32{1, 2, 3}, []int32{1, 2, 3}},
	}
	for i, tc := range cases {
		if got := ftsUnion(tc.a, tc.b); fmt.Sprint(got) != fmt.Sprint(tc.wantUnion) {
			t.Errorf("case %d: union(%v,%v) = %v, want %v", i, tc.a, tc.b, got, tc.wantUnion)
		}
		if got := ftsIntersect(tc.a, tc.b); fmt.Sprint(got) != fmt.Sprint(tc.wantXsect) {
			t.Errorf("case %d: intersect(%v,%v) = %v, want %v", i, tc.a, tc.b, got, tc.wantXsect)
		}
	}

	// ftsUnionAll has two strategies (pairwise merge for a few short lists, a
	// presence bitmap otherwise); both must produce the same ascending,
	// duplicate-free result.
	lists := [][]int32{{1, 9}, {2, 9}, {3}, {4}, {5}, {6}, {7}, {8}}
	want := []int32{1, 2, 3, 4, 5, 6, 7, 8, 9}
	if got := ftsUnionAll(lists, 10); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("unionAll bitmap path = %v, want %v", got, want)
	}
	if got := ftsUnionAll(lists[:2], 1000); fmt.Sprint(got) != fmt.Sprint([]int32{1, 2, 9}) {
		t.Errorf("unionAll pairwise path = %v, want [1 2 9]", got)
	}
}

// TestFTSPostingsMatchDocumentFrequency checks the consolidation of docFreq into
// postings: a term's document frequency must equal its postings length, since
// IDF now derives from it.
func TestFTSPostingsMatchDocumentFrequency(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()
	if _, err := Execute(ctx, db, "default", mustParse(
		`CREATE TABLE docs (id INT, body TEXT)`)); err != nil {
		t.Fatal(err)
	}
	bodies := []string{
		"alpha bravo charlie",
		"alpha bravo",
		"alpha",
		"delta delta delta alpha", // repeated term must count its document once
		"",                        // no tokens: contributes to no postings list
	}
	for i, body := range bodies {
		if _, err := Execute(ctx, db, "default", mustParse(
			fmt.Sprintf(`INSERT INTO docs VALUES (%d, '%s')`, i, body))); err != nil {
			t.Fatal(err)
		}
	}
	table, err := db.Get("default", "docs")
	if err != nil {
		t.Fatal(err)
	}
	bodyIdx, err := table.ColIndex("body")
	if err != nil {
		t.Fatal(err)
	}
	cache := getFTSDocCache("default", table, []int{bodyIdx})

	want := map[string]int{"alpha": 4, "bravo": 2, "charlie": 1, "delta": 1}
	for term, df := range want {
		if got := cache.docFreq(term); got != df {
			t.Errorf("docFreq(%q) = %d, want %d", term, got, df)
		}
	}
	if got := cache.docFreq("absent"); got != 0 {
		t.Errorf("docFreq of an absent term = %d, want 0", got)
	}
	// Recount document frequency straight from the cached documents.
	for term := range cache.postings {
		n := 0
		for _, doc := range cache.docs {
			if doc.valid && doc.freq[term] > 0 {
				n++
			}
		}
		if n != len(cache.postings[term]) {
			t.Errorf("term %q: postings has %d entries, %d documents contain it",
				term, len(cache.postings[term]), n)
		}
	}
}
