package engine

import (
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestFTSBoundDisjunctionMatchesReference(t *testing.T) {
	table := storage.NewTable("fts_disjunction_reference", []storage.Column{{Name: "body", Type: storage.TextType}}, false)
	for _, body := range []string{"alpha", "beta beta gamma", "alpha alpha beta gamma gamma gamma", "delta", "", "gamma delta epsilon epsilon"} {
		table.Rows = append(table.Rows, []any{body})
	}
	table.Version++
	cache := getFTSDocCache("default", table, []int{0})
	for _, query := range []string{
		"alpha OR beta OR gamma", "alpha OR alpha OR missing", "missing OR absent",
		"(alpha OR beta) AND gamma", "NOT (alpha OR beta)",
		`(alpha OR beta) OR "delta epsilon"`, "(alpha OR beta) OR gam*",
	} {
		node := ftsExpandQuery(ftsParseQuery(query), cache.postings)
		idf := ftsIDFLookup(cache)
		bound := ftsBindIDF(node, idf, cache.termIDs)
		// Filtered RAG rebinds a prepared tree to its local IDF statistics.
		for _, prepared := range []*ftsQueryNode{bound, ftsBindIDF(bound, idf, cache.termIDs)} {
			for row, doc := range cache.docs {
				freq, tokens := ftsDocStrings(table, []int{0}, row)
				norm := doc.DocLen
				if cache.avgDocLen > 0 {
					norm /= cache.avgDocLen
				}
				wantMatch := ftsMatchNode(node, freq, tokens)
				wantScore := ftsScoreNode(node, freq, norm, idf)
				gotMatch, gotScore := ftsEvalNode(cache, doc, prepared, norm)
				if gotMatch != wantMatch || (wantMatch && gotScore != wantScore) {
					t.Fatalf("query=%q row=%d got=(%t,%v) want=(%t,%v)", query, row, gotMatch, gotScore, wantMatch, wantScore)
				}
			}
		}
	}
}
