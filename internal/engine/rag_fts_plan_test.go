package engine

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// The oracle builds a separate corpus from authorized rows, so it does not
// share the cached plan's candidates, term weights, or corpus statistics.
func referenceAuthorizedFTS(t *testing.T, table *storage.Table, filter *ragRowFilter, query string, k int, cols []int) []ftsScored {
	t.Helper()
	authorized := storage.NewTable("fts_plan_oracle", table.Cols, false)
	for _, row := range filter.rows {
		authorized.Rows = append(authorized.Rows, table.Rows[row])
	}
	hits, err := ftsSearchCandidates(t.Context(), "oracle", authorized, query, k, cols)
	if err != nil {
		t.Fatal(err)
	}
	for i := range hits {
		hits[i].rowIdx = filter.rows[hits[i].rowIdx]
	}
	return hits
}

func TestFilteredFTSPlanMatchesAuthorizedCorpus(t *testing.T) {
	table := storage.NewTable("fts_plan_scopes", []storage.Column{
		{Name: "body", Type: storage.TextType}, {Name: "summary", Type: storage.TextType},
	}, false)
	table.Rows = [][]any{
		{"alpha beta", "gamma gamma"}, {"beta beta gamma", "alpha beta"},
		{"alpha alpha alpha", "gamma"}, {"gamma", "alpha alpha"},
		{"beta", "beta gamma"}, {nil, nil},
	}
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	if err := db.Put("default", table); err != nil {
		t.Fatal(err)
	}
	filters := []*ragRowFilter{{rows: []int{0, 1, 3, 5}}, {rows: []int{2, 4}}, {rows: []int{0}}, {rows: []int{5}}}
	queries := []string{"alpha", "alpha OR beta", "alpha AND beta", `"alpha beta"`, "NOT alpha", "al*", "absent", ""}
	check := func() {
		for _, filter := range filters {
			for _, cols := range [][]int{{0}, {1}, {0, 1}} {
				for _, query := range queries {
					want := referenceAuthorizedFTS(t, table, filter, query, 10, cols)
					// Reuse the same plan for different k values and then repeat the query.
					for _, k := range []int{10, 1, 10} {
						got, err := ragFTSSearchCandidatesFiltered(t.Context(), "default", table, query, k, cols, filter)
						if err != nil {
							t.Fatal(err)
						}
						expected := want
						if len(expected) > k {
							expected = expected[:k]
						}
						if !reflect.DeepEqual(got, expected) {
							t.Fatalf("version=%d rows=%v cols=%v query=%q k=%d: got=%v want=%v", table.Version, filter.rows, cols, query, k, got, expected)
						}
					}
				}
			}
		}
	}
	check()
	// Keep filter identities fixed: the version must invalidate bound weights,
	// dictionary IDs, and empty candidate lists after content changes.
	execSQL(t, db, `UPDATE fts_plan_scopes SET body = 'absent gamma gamma' WHERE body = 'alpha beta'`)
	execSQL(t, db, `UPDATE fts_plan_scopes SET summary = 'beta beta' WHERE body = 'gamma'`)
	var err error
	table, err = db.Get("default", "fts_plan_scopes")
	if err != nil {
		t.Fatal(err)
	}
	check()
	// Same name and version, different physical table (e.g. another tenant).
	other := storage.NewTable(table.Name, table.Cols, false)
	other.Version = table.Version
	other.Rows = [][]any{{"beta beta", "gamma"}, {"gamma", "beta"}, {"alpha", "gamma"}, {"alpha", "alpha"}, {"beta", "gamma"}, {nil, nil}}
	want := referenceAuthorizedFTS(t, other, filters[0], "alpha OR beta", 10, []int{0})
	got, err := ragFTSSearchCandidatesFiltered(t.Context(), "other", other, "alpha OR beta", 10, []int{0}, filters[0])
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("table isolation: got=%v want=%v err=%v", got, want, err)
	}
}

func TestFilteredFTSPlanConcurrentQueries(t *testing.T) {
	table := storage.NewTable("fts_plan_concurrent", []storage.Column{{Name: "body", Type: storage.TextType}}, false)
	for i := range 300 {
		table.Rows = append(table.Rows, []any{fmt.Sprintf("alpha beta term%d", i%7)})
	}
	filters := []*ragRowFilter{{rows: []int{0, 3, 9, 100, 200}}, {rows: []int{1, 7, 42, 250}}}
	queries := []string{"alpha OR term3", "beta AND term0"}
	// Warm only the shared document arena; goroutines still race to prepare
	// each authorization-specific plan on their first request.
	getFTSDocCache("default", table, []int{0})
	expected := make([][]ftsScored, len(filters))
	for i, filter := range filters {
		expected[i] = referenceAuthorizedFTS(t, table, filter, queries[i], 3, []int{0})
	}
	var wg sync.WaitGroup
	for i := range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			pick := i % len(filters)
			for range 20 {
				got, err := ragFTSSearchCandidatesFiltered(t.Context(), "default", table, queries[pick], 3, []int{0}, filters[pick])
				if err != nil || !reflect.DeepEqual(got, expected[pick]) {
					t.Errorf("concurrent query: got=%v want=%v err=%v", got, expected[pick], err)
					return
				}
			}
		}()
	}
	wg.Wait()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := ragFTSSearchCandidatesFiltered(ctx, "default", table, queries[0], 3, []int{0}, filters[0]); err == nil {
		t.Fatal("cached plan ignored cancellation")
	}
}

func BenchmarkRAGFTSFilteredBranch(b *testing.B) {
	db := ragBenchCorpus(b)
	b.Cleanup(func() { _ = db.Close() })
	table, err := db.Get("default", "rag_chunks")
	if err != nil {
		b.Fatal(err)
	}
	if err := table.CreateSecondaryIndex("fts_plan_document_type", []string{"document_type"}, false); err != nil {
		b.Fatal(err)
	}
	runRAGBench(b, db, `SELECT chunk_id, _fts_score FROM FTS_SEARCH_FILTERED('rag_chunks',
 'term7 OR term23 OR term180 OR needle42',24,
 '{"pre_filter":{"equals":{"document_type":"guide"}}}','search_text')`, 24)
}

func BenchmarkFTSSingleTermQuery(b *testing.B) {
	for _, n := range []int{64, 4096} {
		b.Run(fmt.Sprintf("rows%d", n), func(b *testing.B) {
			table := storage.NewTable("fts_single_term_bench", []storage.Column{{Name: "body", Type: storage.TextType}}, false)
			for i := range n {
				table.Rows = append(table.Rows, []any{fmt.Sprintf("needle term%d", i%11)})
			}
			if _, err := ftsSearchCandidates(b.Context(), "default", table, "needle", 10, []int{0}); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				hits, err := ftsSearchCandidates(b.Context(), "default", table, "needle", 10, []int{0})
				if err != nil || len(hits) != 10 {
					b.Fatalf("hits=%d err=%v", len(hits), err)
				}
			}
		})
	}
}
