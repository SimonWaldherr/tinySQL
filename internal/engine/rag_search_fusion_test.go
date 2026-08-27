package engine

import (
	"math"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// TestRAGFuseCandidatesNativeRows verifies the compact row-ID fusion path
// directly. End-to-end RAG_SEARCH tests cover SQL parsing and branch
// execution; this pins the important RRF invariants that let the hot path
// avoid materializing the two intermediate candidate result sets.
func TestRAGFuseCandidatesNativeRows(t *testing.T) {
	table := &storage.Table{
		Cols: []storage.Column{
			{Name: "id", Type: storage.IntType},
			{Name: "body", Type: storage.TextType},
		},
		Rows: [][]any{
			{0, "zero"},
			{1, "one"},
			{2, "two"},
			{3, "three"},
		},
	}

	// Row 1 is retrieved by both branches; its combined score must outrank
	// the vector-only row 3 and FTS-only row 2. The candidate slices already
	// carry best-first ranks, as vecSearchCandidates/ftsSearchCandidates do.
	got := ragFuseCandidates(table,
		[]vecScoredRow{{rowIdx: 3, distance: 0.1}, {rowIdx: 1, distance: 0.2}},
		[]ftsScored{{rowIdx: 1, score: 4.0}, {rowIdx: 2, score: 3.0}},
		"cosine", 60, 3,
	)
	if len(got.Rows) != 3 {
		t.Fatalf("fused row count = %d, want 3", len(got.Rows))
	}
	for i, wantID := range []int{1, 3, 2} {
		if gotID, ok := got.Rows[i]["id"].(int); !ok || gotID != wantID {
			t.Fatalf("row %d id = %v, want %d; rows=%#v", i, got.Rows[i]["id"], wantID, got.Rows)
		}
		if gotRank := got.Rows[i]["_rrf_rank"]; gotRank != i+1 {
			t.Errorf("row %d _rrf_rank = %v, want %d", i, gotRank, i+1)
		}
	}

	both := got.Rows[0]
	if both["_vec_rank"] != 2 || both["_fts_rank"] != 1 {
		t.Errorf("row retrieved by both branches has ranks vec=%v fts=%v, want 2/1", both["_vec_rank"], both["_fts_rank"])
	}
	wantScore := 1.0/62.0 + 1.0/61.0
	if score, ok := both["_rrf_score"].(float64); !ok || math.Abs(score-wantScore) > 1e-15 {
		t.Errorf("combined RRF score = %v, want %.17g", both["_rrf_score"], wantScore)
	}
	if _, ok := got.Rows[1]["_fts_rank"]; ok {
		t.Errorf("vector-only row unexpectedly has _fts_rank: %#v", got.Rows[1])
	}
	if _, ok := got.Rows[2]["_vec_rank"]; ok {
		t.Errorf("FTS-only row unexpectedly has _vec_rank: %#v", got.Rows[2])
	}
}

var ragFuseBenchmarkSink *ResultSet

// BenchmarkRAGFuseCandidates isolates reciprocal-rank fusion after the two
// retrieval branches have already produced their compact row-ID candidates.
// The small sub-benchmark is the normal RAG default (candidate_k=4*k); the
// larger one catches avoidable per-candidate allocations in high-recall
// deployments that deliberately retrieve wider candidate sets.
func BenchmarkRAGFuseCandidates(b *testing.B) {
	for _, tc := range []struct {
		name       string
		candidates int
		k          int
	}{
		{name: "candidate_k_24_return_6", candidates: 24, k: 6},
		{name: "candidate_k_256_return_24", candidates: 256, k: 24},
	} {
		b.Run(tc.name, func(b *testing.B) {
			table := benchmarkRAGFuseTable(tc.candidates * 2)
			vecRows := make([]vecScoredRow, tc.candidates)
			ftsRows := make([]ftsScored, tc.candidates)
			for i := 0; i < tc.candidates; i++ {
				vecRows[i] = vecScoredRow{rowIdx: i, distance: float64(i) / float64(tc.candidates)}
				// Half the rows overlap the vector list, and half are text-only.
				ftsRows[i] = ftsScored{rowIdx: tc.candidates/2 + i, score: float64(tc.candidates - i)}
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				ragFuseBenchmarkSink = ragFuseCandidates(table, vecRows, ftsRows, "cosine", 60, tc.k)
			}
		})
	}
}

func benchmarkRAGFuseTable(rows int) *storage.Table {
	table := &storage.Table{
		Cols: []storage.Column{
			{Name: "chunk_id", Type: storage.TextType},
			{Name: "doc_id", Type: storage.TextType},
			{Name: "chunk_text", Type: storage.TextType},
			{Name: "quality", Type: storage.FloatType},
		},
		Rows: make([][]any, rows),
	}
	for i := range table.Rows {
		table.Rows[i] = []any{"chunk", "doc", "text", float64(i)}
	}
	return table
}
