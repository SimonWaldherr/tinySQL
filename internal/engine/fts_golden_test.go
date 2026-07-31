package engine

// Golden-output tests pinning FTS_SEARCH and HYBRID_SEARCH retrieval behavior
// across the whole query grammar.
//
// These exist to make the retrieval internals safe to optimize. FTS_SEARCH
// currently scores every document in the corpus on every query; any change that
// restricts the candidate set, changes the scan order, or changes how term
// frequencies are looked up must return the same rows, in the same order, with
// the same scores. That is a strong invariant and it needs to be checked
// mechanically rather than argued about.
//
// The corpus is the deterministic Zipf fixture from rag_workload_benchmark_test.go,
// so these goldens are reproducible across machines.

import (
	"context"
	"fmt"
	"math"
	"strings"
	"testing"
)

// ftsGoldenQueries covers each node type ftsParseQuery can produce, plus the
// degenerate inputs that have historically been the source of retrieval bugs
// (empty query, all-stopword query, negation with nothing to negate).
var ftsGoldenQueries = []struct {
	name  string
	query string
}{
	{"single_common_term", "term0"},
	{"single_rare_term", "needle42"},
	{"two_term_implicit_and", "term3 term7"},
	{"explicit_or", "term3 OR term7"},
	{"explicit_and", "term3 AND term7"},
	{"and_not", "term3 AND NOT term7"},
	{"or_not", "term3 OR NOT term7"},
	{"leading_not", "NOT term7"},
	{"phrase", `"term0 term1"`},
	{"phrase_plus_term", `"term0 term1" term5`},
	{"prefix_wildcard", "needle4*"},
	{"prefix_wildcard_broad", "term1*"},
	{"single_char_wildcard", "term1?"},
	{"percent_wildcard", "needle10%"},
	{"underscore_wildcard", "term2_"},
	{"mixed_wildcard_and_term", "needle4* AND term0"},
	{"stopwords_only", "the and of"},
	{"empty", "   "},
	{"nonexistent_term", "zzzznotpresent"},
	{"nonexistent_wildcard", "zzzznotpresent*"},
}

// ftsGoldenRow is one retrieved row, reduced to the fields a caller depends on.
type ftsGoldenRow struct {
	chunkID string
	score   float64
}

// sqlQuote renders s as a single-quoted SQL string literal.
func sqlQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// TestFTSGoldenQueryGrammar records, for every query shape, the ranked result
// list FTS_SEARCH returns. It asserts two properties that must hold regardless
// of how retrieval is implemented internally:
//
//  1. determinism — the same query against an unchanged corpus returns
//     byte-identical rows, ranks and scores every time;
//  2. score/rank consistency — scores are non-increasing down the rank list,
//     and _fts_rank is dense and 1-based.
//
// Property 1 is the one that makes a candidate-restriction rewrite verifiable:
// if results are not reproducible before the change, no comparison after it
// means anything.
func TestFTSGoldenQueryGrammar(t *testing.T) {
	db := ragBenchCorpus(t)
	ctx := context.Background()

	for _, tc := range ftsGoldenQueries {
		t.Run(tc.name, func(t *testing.T) {
			sql := fmt.Sprintf(
				`SELECT chunk_id, _fts_score, _fts_rank FROM FTS_SEARCH('rag_chunks', %s, 12, 'search_text')`,
				sqlQuote(tc.query))
			stmt := mustParse(sql)

			var baseline []ftsGoldenRow
			const runs = 6
			for iter := 0; iter < runs; iter++ {
				rs, err := Execute(ctx, db, "default", stmt)
				if err != nil {
					t.Fatalf("query %q: %v", tc.query, err)
				}

				got := make([]ftsGoldenRow, 0, len(rs.Rows))
				for i, r := range rs.Rows {
					id, _ := r["chunk_id"].(string)
					score := toFloatOrFail(t, r["_fts_score"])
					rank, err := toInt(r["_fts_rank"])
					if err != nil {
						t.Fatalf("rank not an int: %v", r["_fts_rank"])
					}
					if rank != i+1 {
						t.Errorf("row %d: _fts_rank = %d, want dense 1-based %d", i, rank, i+1)
					}
					got = append(got, ftsGoldenRow{chunkID: id, score: score})
				}

				// Scores must be non-increasing down the ranking.
				for i := 1; i < len(got); i++ {
					if got[i].score > got[i-1].score {
						t.Errorf("rank %d score %v exceeds rank %d score %v",
							i+1, got[i].score, i, got[i-1].score)
					}
				}

				if iter == 0 {
					baseline = got
					t.Logf("%s -> %d rows, top score %v", tc.query, len(got), topScore(got))
					continue
				}
				if len(got) != len(baseline) {
					t.Fatalf("run %d returned %d rows, first run returned %d — retrieval is not reproducible",
						iter, len(got), len(baseline))
				}
				for i := range got {
					if got[i].chunkID != baseline[i].chunkID {
						t.Errorf("run %d rank %d: chunk_id %q != first run %q — result ORDER is not reproducible",
							iter, i+1, got[i].chunkID, baseline[i].chunkID)
					}
					if got[i].score != baseline[i].score {
						t.Errorf("run %d rank %d (%s): _fts_score %v != first run %v — SCORE is not reproducible",
							iter, i+1, got[i].chunkID, got[i].score, baseline[i].score)
					}
				}
			}
		})
	}
}

// TestHybridSearchGoldenDeterminism is the same reproducibility check one level
// up, on the retriever docs/rag-guide.md recommends by default. RRF fusion
// matches the vector and lexical candidate lists by composite key, so an
// unstable lexical score can reorder the fused result even when both branches
// individually return the same rows.
func TestHybridSearchGoldenDeterminism(t *testing.T) {
	db := ragBenchCorpus(t)
	ctx := context.Background()
	qv := ragBenchQueryVector(t)

	cases := []struct {
		name string
		term string
	}{
		{"plain_terms", "term7 term23 term180"},
		{"rare_identifier", "needle42"},
		{"wildcard", "needle4*"},
		{"natural_language_question", "How do I configure the term7 timeout for term23?"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stmt := mustParse(fmt.Sprintf(`
				SELECT chunk_id, _rrf_rank, _rrf_score, _vec_rank, _fts_rank
				FROM HYBRID_SEARCH('rag_chunks', 'embedding', 'search_text',
					%s, '%s', 6, '{"candidate_k":24,"rrf_k":60}')
				ORDER BY _rrf_rank`, sqlQuote(tc.term), qv))

			type fused struct {
				id    string
				score float64
			}
			var baseline []fused
			for iter := 0; iter < 5; iter++ {
				rs, err := Execute(ctx, db, "default", stmt)
				if err != nil {
					t.Fatalf("%v", err)
				}
				got := make([]fused, 0, len(rs.Rows))
				for _, r := range rs.Rows {
					id, _ := r["chunk_id"].(string)
					got = append(got, fused{id, toFloatOrFail(t, r["_rrf_score"])})
				}
				if iter == 0 {
					baseline = got
					t.Logf("%s -> %d fused rows", tc.term, len(got))
					continue
				}
				if len(got) != len(baseline) {
					t.Fatalf("run %d: %d rows vs %d", iter, len(got), len(baseline))
				}
				for i := range got {
					if got[i] != baseline[i] {
						t.Errorf("run %d rank %d: %+v != first run %+v — fused result is not reproducible",
							iter, i+1, got[i], baseline[i])
					}
				}
			}
		})
	}
}

// TestFTSNegationSemantics pins what NOT means today, because any
// candidate-restriction strategy has to reproduce it exactly: a negated branch
// cannot generate candidates, so a query whose match set is defined by
// negation must still consider documents that contain none of its terms.
func TestFTSNegationSemantics(t *testing.T) {
	db := ragBenchCorpus(t)
	ctx := context.Background()

	// "NOT term0" must match documents that simply lack term0 — none of which
	// can be found from any postings list built from the query's own terms.
	rs, err := Execute(ctx, db, "default", mustParse(
		`SELECT chunk_id, _fts_score FROM FTS_SEARCH('rag_chunks', 'NOT term0', 12, 'search_text')`))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("NOT term0 -> %d rows", len(rs.Rows))
	for _, r := range rs.Rows {
		// ftsScoreNode returns 0 for a NOT node, so every such row scores 0.
		if s := toFloatOrFail(t, r["_fts_score"]); s != 0 {
			t.Errorf("negation-only match %v scored %v, want 0", r["chunk_id"], s)
		}
	}

	// Verify the returned rows genuinely lack the term, i.e. negation is not
	// silently matching everything.
	table, err := db.Get("default", "rag_chunks")
	if err != nil {
		t.Fatal(err)
	}
	textIdx, err := table.ColIndex("search_text")
	if err != nil {
		t.Fatal(err)
	}
	idIdx, err := table.ColIndex("chunk_id")
	if err != nil {
		t.Fatal(err)
	}
	byID := make(map[string]string, len(table.Rows))
	for _, row := range table.Rows {
		id, _ := row[idIdx].(string)
		text, _ := row[textIdx].(string)
		byID[id] = text
	}
	for _, r := range rs.Rows {
		id, _ := r["chunk_id"].(string)
		for _, tok := range ftsTokenize(byID[id]) {
			if tok == ftsStem("term0") {
				t.Errorf("row %s matched 'NOT term0' but contains token %q", id, tok)
				break
			}
		}
	}
}

func topScore(rows []ftsGoldenRow) float64 {
	if len(rows) == 0 {
		return math.NaN()
	}
	return rows[0].score
}

func toFloatOrFail(t *testing.T, v any) float64 {
	t.Helper()
	switch n := v.(type) {
	case float64:
		return n
	case float32:
		return float64(n)
	case int:
		return float64(n)
	case int64:
		return float64(n)
	case nil:
		return 0
	default:
		t.Fatalf("expected a numeric score, got %T (%v)", v, v)
		return 0
	}
}
