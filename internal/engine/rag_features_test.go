// Tests for improved FTS (phrase search, boolean ops, wildcards, FTS_SEARCH TVF),
// BLOB functions, vector binary encoding, and text utilities.
package engine

import (
	"bytes"
	"context"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// ─────────────────────────── FTS boolean query parser ────────────────────────

func TestFTSMatchSimpleTerm(t *testing.T) {
	db := storage.NewDB()
	rs := execSQL(t, db, `SELECT FTS_MATCH('the quick brown fox', 'fox') as m`)
	if rs.Rows[0]["m"] != true {
		t.Errorf("expected true for matching term")
	}
}

func TestFTSMatchMissingTerm(t *testing.T) {
	db := storage.NewDB()
	rs := execSQL(t, db, `SELECT FTS_MATCH('the quick brown fox', 'cat') as m`)
	if rs.Rows[0]["m"] == true {
		t.Errorf("expected false for missing term")
	}
}

func TestFTSMatchBooleanAND(t *testing.T) {
	db := storage.NewDB()
	// Both present → true
	rs := execSQL(t, db, `SELECT FTS_MATCH('the quick brown fox', 'quick AND fox') as m`)
	if rs.Rows[0]["m"] != true {
		t.Errorf("expected true for AND with both terms present")
	}
	// One missing → false
	rs = execSQL(t, db, `SELECT FTS_MATCH('the quick brown fox', 'quick AND cat') as m`)
	if rs.Rows[0]["m"] == true {
		t.Errorf("expected false for AND with one term missing")
	}
}

func TestFTSMatchBooleanOR(t *testing.T) {
	db := storage.NewDB()
	// Either present → true
	rs := execSQL(t, db, `SELECT FTS_MATCH('the quick brown fox', 'cat OR fox') as m`)
	if rs.Rows[0]["m"] != true {
		t.Errorf("expected true for OR with one term present")
	}
	// Neither present → false
	rs = execSQL(t, db, `SELECT FTS_MATCH('the quick brown fox', 'cat OR dog') as m`)
	if rs.Rows[0]["m"] == true {
		t.Errorf("expected false for OR with neither term present")
	}
}

func TestFTSMatchBooleanNOT(t *testing.T) {
	db := storage.NewDB()
	// NOT missing term → true
	rs := execSQL(t, db, `SELECT FTS_MATCH('the quick brown fox', 'NOT cat') as m`)
	if rs.Rows[0]["m"] != true {
		t.Errorf("expected true for NOT on missing term")
	}
	// NOT present term → false
	rs = execSQL(t, db, `SELECT FTS_MATCH('the quick brown fox', 'NOT fox') as m`)
	if rs.Rows[0]["m"] == true {
		t.Errorf("expected false for NOT on present term")
	}
}

func TestFTSMatchPhraseSearch(t *testing.T) {
	db := storage.NewDB()
	// Exact phrase present
	rs := execSQL(t, db, `SELECT FTS_MATCH('the quick brown fox jumps', '"quick brown"') as m`)
	if rs.Rows[0]["m"] != true {
		t.Errorf("expected true for exact phrase match")
	}
	// Same words but not consecutive
	rs = execSQL(t, db, `SELECT FTS_MATCH('the quick jumps brown fox', '"quick brown"') as m`)
	if rs.Rows[0]["m"] == true {
		t.Errorf("expected false for non-consecutive phrase words")
	}
}

func TestFTSMatchWildcard(t *testing.T) {
	db := storage.NewDB()
	// Prefix wildcard
	rs := execSQL(t, db, `SELECT FTS_MATCH('the database stores information', 'inform*') as m`)
	if rs.Rows[0]["m"] != true {
		t.Errorf("expected true for prefix wildcard match")
	}
}

func TestFTSRankBooleanQuery(t *testing.T) {
	db := storage.NewDB()
	rs := execSQL(t, db, `SELECT FTS_RANK('the quick brown fox', 'fox AND quick') as r`)
	r, ok := rs.Rows[0]["r"].(float64)
	if !ok {
		t.Fatalf("expected float64, got %T", rs.Rows[0]["r"])
	}
	if r <= 0 {
		t.Errorf("expected positive rank score, got %v", r)
	}
}

func TestFTSSnippetHighlightsQuery(t *testing.T) {
	db := storage.NewDB()
	rs := execSQL(t, db, `SELECT FTS_SNIPPET('the quick brown fox', 'fox', '<em>', '</em>', '', 10) as s`)
	s, ok := rs.Rows[0]["s"].(string)
	if !ok {
		t.Fatalf("expected string, got %T", rs.Rows[0]["s"])
	}
	if !strings.Contains(s, "<em>") {
		t.Errorf("expected highlight tags in snippet, got %q", s)
	}
}

func TestFTSSearchTVF(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()

	Execute(ctx, db, "default", mustParse(`CREATE TABLE articles (id INT, title TEXT, body TEXT)`))
	Execute(ctx, db, "default", mustParse(`INSERT INTO articles VALUES (1, 'Go Programming', 'Go is a fast compiled language for systems programming')`))
	Execute(ctx, db, "default", mustParse(`INSERT INTO articles VALUES (2, 'Python Tutorial', 'Python is a dynamic scripting language popular for data science')`))
	Execute(ctx, db, "default", mustParse(`INSERT INTO articles VALUES (3, 'Database Design', 'Relational databases store data in tables with relationships')`))

	rs := execSQL(t, db, `SELECT * FROM FTS_SEARCH('articles', 'programming language', 2)`)
	if len(rs.Rows) == 0 {
		t.Fatal("expected at least 1 result from FTS_SEARCH")
	}
	if len(rs.Rows) > 2 {
		t.Errorf("expected at most 2 results (k=2), got %d", len(rs.Rows))
	}
	// Result should contain _fts_score and _fts_rank
	if _, ok := rs.Rows[0]["_fts_score"]; !ok {
		t.Error("expected _fts_score column in results")
	}
	if _, ok := rs.Rows[0]["_fts_rank"]; !ok {
		t.Error("expected _fts_rank column in results")
	}
	// Top result should have rank 1
	if rs.Rows[0]["_fts_rank"] != 1 {
		t.Errorf("expected top result to have rank 1, got %v", rs.Rows[0]["_fts_rank"])
	}
}

func TestFTSWordCount(t *testing.T) {
	db := storage.NewDB()
	rs := execSQL(t, db, `SELECT FTS_WORD_COUNT('hello world foo') as n`)
	expectInt(t, rs.Rows[0]["n"], 3, "FTS_WORD_COUNT")
}

// ─────────────────────────── BLOB functions ───────────────────────────────────

func TestBlobLength(t *testing.T) {
	db := storage.NewDB()
	// hex 'deadbeef' = 4 bytes
	rs := execSQL(t, db, `SELECT BLOB_LENGTH('deadbeef') as n`)
	expectInt(t, rs.Rows[0]["n"], 4, "BLOB_LENGTH")
}

func TestBlobHex(t *testing.T) {
	db := storage.NewDB()
	rs := execSQL(t, db, `SELECT BLOB_HEX('deadbeef') as h`)
	h, ok := rs.Rows[0]["h"].(string)
	if !ok {
		t.Fatalf("expected string, got %T", rs.Rows[0]["h"])
	}
	if h != "deadbeef" {
		t.Errorf("expected 'deadbeef', got %q", h)
	}
}

// blobResultBytes asserts a BLOB-returning function produced raw bytes.
//
// The functions that return a blob return []byte, not a hex string, so their
// result can be inserted into a BLOB column (see the BLOB helpers section in
// extra_types.go). BLOB_HEX renders bytes as hex when text is wanted.
func blobResultBytes(t *testing.T, v any) []byte {
	t.Helper()
	b, ok := v.([]byte)
	if !ok {
		t.Fatalf("expected []byte from a BLOB-returning function, got %T (%v)", v, v)
	}
	return b
}

func TestBlobFromHex(t *testing.T) {
	db := storage.NewDB()
	rs := execSQL(t, db, `SELECT BLOB_FROM_HEX('deadbeef') as b`)
	if got := blobResultBytes(t, rs.Rows[0]["b"]); !bytes.Equal(got, []byte{0xde, 0xad, 0xbe, 0xef}) {
		t.Errorf("expected bytes deadbeef, got %x", got)
	}
	// Rendering the same value as hex reproduces the historical output.
	rs = execSQL(t, db, `SELECT BLOB_HEX(BLOB_FROM_HEX('deadbeef')) as h`)
	if rs.Rows[0]["h"] != "deadbeef" {
		t.Errorf("BLOB_HEX(BLOB_FROM_HEX(..)) = %v, want 'deadbeef'", rs.Rows[0]["h"])
	}
}

func TestBlobSubstr(t *testing.T) {
	db := storage.NewDB()
	// 'deadbeef' decodes to [0xde, 0xad, 0xbe, 0xef]; substr(1, 2) = [0xad, 0xbe].
	rs := execSQL(t, db, `SELECT BLOB_SUBSTR('deadbeef', 1, 2) as s`)
	if got := blobResultBytes(t, rs.Rows[0]["s"]); !bytes.Equal(got, []byte{0xad, 0xbe}) {
		t.Errorf("expected bytes adbe, got %x", got)
	}
	// Out of range yields an empty blob, not NULL.
	rs = execSQL(t, db, `SELECT BLOB_SUBSTR('deadbeef', 99, 2) as s`)
	if got := blobResultBytes(t, rs.Rows[0]["s"]); len(got) != 0 {
		t.Errorf("out-of-range substr returned %x, want an empty blob", got)
	}
}

func TestBlobConcat(t *testing.T) {
	db := storage.NewDB()
	rs := execSQL(t, db, `SELECT BLOB_CONCAT('dead', 'beef') as b`)
	if got := blobResultBytes(t, rs.Rows[0]["b"]); !bytes.Equal(got, []byte{0xde, 0xad, 0xbe, 0xef}) {
		t.Errorf("expected bytes deadbeef, got %x", got)
	}
	// Concatenating blob-typed values (not hex text) must work the same way,
	// which is what makes the functions composable.
	rs = execSQL(t, db, `SELECT BLOB_HEX(BLOB_CONCAT(X'dead', X'beef')) as h`)
	if rs.Rows[0]["h"] != "deadbeef" {
		t.Errorf("BLOB_CONCAT over X'' literals = %v, want 'deadbeef'", rs.Rows[0]["h"])
	}
}

func TestBlobBase64RoundTrip(t *testing.T) {
	db := storage.NewDB()
	// Encode then decode should give back the original bytes.
	rs := execSQL(t, db, `SELECT BLOB_FROM_BASE64(BLOB_TO_BASE64('deadbeef')) as b`)
	if got := blobResultBytes(t, rs.Rows[0]["b"]); !bytes.Equal(got, []byte{0xde, 0xad, 0xbe, 0xef}) {
		t.Errorf("BLOB base64 round-trip failed: got %x", got)
	}
	// BLOB_TO_BASE64 renders text, so it stays a string.
	rs = execSQL(t, db, `SELECT BLOB_TO_BASE64(X'deadbeef') as s`)
	if _, ok := rs.Rows[0]["s"].(string); !ok {
		t.Errorf("BLOB_TO_BASE64 returned %T, want a string", rs.Rows[0]["s"])
	}
}

func TestBlobEqual(t *testing.T) {
	db := storage.NewDB()
	rs := execSQL(t, db, `SELECT BLOB_EQUAL('deadbeef', 'deadbeef') as eq`)
	if rs.Rows[0]["eq"] != true {
		t.Errorf("expected BLOB_EQUAL to return true for identical blobs")
	}
	rs = execSQL(t, db, `SELECT BLOB_EQUAL('deadbeef', 'dead0000') as eq`)
	if rs.Rows[0]["eq"] == true {
		t.Errorf("expected BLOB_EQUAL to return false for different blobs")
	}
}

// ─────────────────────────── Vector binary encoding ──────────────────────────

func TestVecToFromBytes(t *testing.T) {
	db := storage.NewDB()
	// Round-trip: encode then decode, should get back ≈ original (float32 precision loss).
	rs := execSQL(t, db, `SELECT VEC_FROM_BYTES(VEC_TO_BYTES(VEC_FROM_JSON('[1.0, -0.5, 0.25]'))) as v`)
	vec, ok := rs.Rows[0]["v"].([]float64)
	if !ok {
		t.Fatalf("expected []float64, got %T", rs.Rows[0]["v"])
	}
	if len(vec) != 3 {
		t.Fatalf("expected 3 dimensions, got %d", len(vec))
	}
	// Float32 precision: within 1e-6.
	expectFloat(t, vec[0], 1.0, 1e-6, "VEC_TO/FROM_BYTES[0]")
	expectFloat(t, vec[1], -0.5, 1e-6, "VEC_TO/FROM_BYTES[1]")
	expectFloat(t, vec[2], 0.25, 1e-6, "VEC_TO/FROM_BYTES[2]")
}

func TestVecBinaryQuantize(t *testing.T) {
	db := storage.NewDB()
	rs := execSQL(t, db, `SELECT VEC_BINARY_QUANTIZE(VEC_FROM_JSON('[1.0, -0.5, 0.0, 2.0]')) as v`)
	vec, ok := rs.Rows[0]["v"].([]float64)
	if !ok {
		t.Fatalf("expected []float64, got %T", rs.Rows[0]["v"])
	}
	if vec[0] != 1.0 || vec[1] != 0.0 || vec[2] != 0.0 || vec[3] != 1.0 {
		t.Errorf("VEC_BINARY_QUANTIZE: expected [1,0,0,1], got %v", vec)
	}
}

func TestVecHammingDistance(t *testing.T) {
	db := storage.NewDB()
	// [1,0,1,0] vs [1,1,0,0] → 2 differences
	rs := execSQL(t, db, `SELECT VEC_HAMMING_DISTANCE(VEC_FROM_JSON('[1,0,1,0]'), VEC_FROM_JSON('[1,1,0,0]')) as d`)
	expectInt(t, rs.Rows[0]["d"], 2, "VEC_HAMMING_DISTANCE")
}

func TestVecCentroid(t *testing.T) {
	db := storage.NewDB()
	rs := execSQL(t, db, `SELECT VEC_CENTROID(VEC_FROM_JSON('[0,0]'), VEC_FROM_JSON('[2,4]'), VEC_FROM_JSON('[4,2]')) as v`)
	vec, ok := rs.Rows[0]["v"].([]float64)
	if !ok {
		t.Fatalf("expected []float64, got %T", rs.Rows[0]["v"])
	}
	expectFloat(t, vec[0], 2.0, 1e-9, "VEC_CENTROID[0]")
	expectFloat(t, vec[1], 2.0, 1e-9, "VEC_CENTROID[1]")
}

func TestVecHammingVsBinaryQuantize(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()

	Execute(ctx, db, "default", mustParse(`CREATE TABLE bin_vecs (id INT, vec VECTOR)`))
	Execute(ctx, db, "default", mustParse(`INSERT INTO bin_vecs VALUES (1, '[1.0, 0.0, 1.0, 0.0]')`))
	Execute(ctx, db, "default", mustParse(`INSERT INTO bin_vecs VALUES (2, '[1.0, 1.0, 0.0, 0.0]')`))

	// Query: [1,0,1,0] – should have distance 0 to row1 and 2 to row2
	rs := execSQL(t, db, `
		SELECT id,
		       VEC_HAMMING_DISTANCE(VEC_BINARY_QUANTIZE(vec), VEC_FROM_JSON('[1,0,1,0]')) as hdist
		FROM bin_vecs ORDER BY hdist
	`)
	if len(rs.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rs.Rows))
	}
	expectInt(t, rs.Rows[0]["hdist"], 0, "hamming to row1")
	expectInt(t, rs.Rows[1]["hdist"], 2, "hamming to row2")
}

// ─────────────────────────── TEXT functions ──────────────────────────────────

func TestTextWordCount(t *testing.T) {
	db := storage.NewDB()
	rs := execSQL(t, db, `SELECT TEXT_WORD_COUNT('hello world foo bar') as n`)
	expectInt(t, rs.Rows[0]["n"], 4, "TEXT_WORD_COUNT")
}

func TestTextCharCount(t *testing.T) {
	db := storage.NewDB()
	rs := execSQL(t, db, `SELECT TEXT_CHAR_COUNT('hello') as n`)
	expectInt(t, rs.Rows[0]["n"], 5, "TEXT_CHAR_COUNT")
}

func TestTextTruncate(t *testing.T) {
	db := storage.NewDB()
	rs := execSQL(t, db, `SELECT TEXT_TRUNCATE('hello world', 8) as t`)
	s, ok := rs.Rows[0]["t"].(string)
	if !ok {
		t.Fatalf("expected string, got %T", rs.Rows[0]["t"])
	}
	if len([]rune(s)) > 8 {
		t.Errorf("TEXT_TRUNCATE: expected max 8 chars, got %q (%d chars)", s, len([]rune(s)))
	}
	if !strings.HasSuffix(s, "...") {
		t.Errorf("TEXT_TRUNCATE: expected ellipsis, got %q", s)
	}
}

func TestTextChunksTVF(t *testing.T) {
	db := storage.NewDB()
	text := "one two three four five six seven eight nine ten eleven twelve"
	rs := execSQL(t, db, `SELECT * FROM TEXT_CHUNKS('`+text+`', 4, 1)`)
	if len(rs.Rows) == 0 {
		t.Fatal("expected chunks, got none")
	}
	// First chunk should have 4 words.
	chunk0 := rs.Rows[0]["chunk_text"].(string)
	if len(strings.Fields(chunk0)) != 4 {
		t.Errorf("expected 4 words in first chunk, got %q", chunk0)
	}
	// chunk_index of first chunk should be 0.
	expectInt(t, rs.Rows[0]["chunk_index"], 0, "TEXT_CHUNKS chunk_index")
	// Overlap of 1: last word of chunk0 should equal first word of chunk1.
	if len(rs.Rows) >= 2 {
		words0 := strings.Fields(rs.Rows[0]["chunk_text"].(string))
		words1 := strings.Fields(rs.Rows[1]["chunk_text"].(string))
		lastOf0 := words0[len(words0)-1]
		firstOf1 := words1[0]
		if lastOf0 != firstOf1 {
			t.Errorf("expected overlap: last word of chunk0 %q should equal first word of chunk1 %q", lastOf0, firstOf1)
		}
	}
}

func TestTextChunksCharUnit(t *testing.T) {
	db := storage.NewDB()
	rs := execSQL(t, db, `SELECT * FROM TEXT_CHUNKS('abcdefghij', 4, 0, 'chars')`)
	if len(rs.Rows) == 0 {
		t.Fatal("expected chunks")
	}
	// First chunk should be exactly 4 characters.
	if rs.Rows[0]["chunk_text"] != "abcd" {
		t.Errorf("expected 'abcd', got %q", rs.Rows[0]["chunk_text"])
	}
}

// ─────────────────────────── RAG end-to-end workflow ─────────────────────────

func TestRAGHybridWorkflow(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()

	// Simulate a RAG knowledge base with both text and vector embeddings.
	Execute(ctx, db, "default", mustParse(`
		CREATE TABLE rag_docs (id INT, content TEXT, embedding VECTOR)
	`))
	Execute(ctx, db, "default", mustParse(`INSERT INTO rag_docs VALUES (1, 'Go is a compiled systems programming language', '[1.0, 0.1, 0.0]')`))
	Execute(ctx, db, "default", mustParse(`INSERT INTO rag_docs VALUES (2, 'Python is popular for data science and machine learning', '[0.0, 0.9, 0.1]')`))
	Execute(ctx, db, "default", mustParse(`INSERT INTO rag_docs VALUES (3, 'Rust provides memory safety without garbage collection', '[0.1, 0.0, 1.0]')`))
	Execute(ctx, db, "default", mustParse(`INSERT INTO rag_docs VALUES (4, 'Go programming with goroutines for concurrency', '[0.9, 0.2, 0.0]')`))

	// 1. Vector search for "Go" embedding.
	rsVec := execSQL(t, db, `SELECT * FROM VEC_SEARCH('rag_docs', 'embedding', VEC_FROM_JSON('[1.0, 0.0, 0.0]'), 2)`)
	if len(rsVec.Rows) != 2 {
		t.Errorf("VEC_SEARCH: expected 2 results, got %d", len(rsVec.Rows))
	}

	// 2. FTS search for "programming language".
	rsFTS := execSQL(t, db, `SELECT * FROM FTS_SEARCH('rag_docs', 'programming language', 2)`)
	if len(rsFTS.Rows) == 0 {
		t.Error("FTS_SEARCH: expected at least 1 result for 'programming language'")
	}

	// 3. Combined WHERE: FTS_MATCH + cosine similarity filter.
	rsHybrid := execSQL(t, db, `
		SELECT id, content,
		       FTS_RANK(content, 'programming') as rank,
		       VEC_COSINE_SIMILARITY(embedding, VEC_FROM_JSON('[1.0, 0.0, 0.0]')) as sim
		FROM rag_docs
		WHERE FTS_MATCH(content, 'programming')
		ORDER BY rank DESC
	`)
	if len(rsHybrid.Rows) == 0 {
		t.Error("hybrid search: expected at least 1 result")
	}

	// 4. Text chunking for a longer document.
	rsChunks := execSQL(t, db, `SELECT * FROM TEXT_CHUNKS('one two three four five six seven eight nine ten', 4, 1)`)
	if len(rsChunks.Rows) == 0 {
		t.Error("TEXT_CHUNKS: expected at least 1 chunk")
	}

	// 5. BLOB encoding round-trip for vector storage.
	rsBlob := execSQL(t, db, `SELECT VEC_FROM_BYTES(VEC_TO_BYTES(VEC_FROM_JSON('[0.5, 0.5, 0.5]'))) as v`)
	vec := rsBlob.Rows[0]["v"].([]float64)
	if math.Abs(vec[0]-0.5) > 1e-6 {
		t.Errorf("BLOB round-trip: expected 0.5, got %v", vec[0])
	}
}

func TestRecencyScore(t *testing.T) {
	db := storage.NewDB()
	rs := execSQL(t, db, `
		SELECT RECENCY_SCORE('2026-01-01 00:00:00', 30, '2026-01-31 00:00:00') as score
	`)
	score := rs.Rows[0]["score"].(float64)
	expectFloat(t, score, 0.5, 1e-9, "RECENCY_SCORE 30-day half-life")

	rs = execSQL(t, db, `
		SELECT RECENCY_SCORE('2026-02-01 00:00:00', 30, '2026-01-31 00:00:00') as score
	`)
	// future timestamp yields full freshness
	expectFloat(t, rs.Rows[0]["score"], 1.0, 1e-12, "RECENCY_SCORE future timestamp")
}

// TestRAGScorersDefaultToStatementClock guards the per-statement evaluation
// clock: RECENCY_SCORE/RAG_HYBRID_SCORE/RAG_RANK_SCORE must default the
// implicit `now` to env.now (set once per statement) rather than a fresh
// time.Now() call. env.now is pinned to 2020 here; if any of the three
// regressed to time.Now(), the computed age would jump from exactly one
// half-life to several years, collapsing the score toward 0 instead of ~0.5.
func TestRAGScorersDefaultToStatementClock(t *testing.T) {
	fixedNow := time.Date(2020, 6, 15, 12, 0, 0, 0, time.UTC)
	tsOneHalfLifeAgo := fixedNow.Add(-30 * 24 * time.Hour).Format("2006-01-02 15:04:05")
	env := ExecEnv{now: fixedNow}
	row := Row{}

	t.Run("RECENCY_SCORE", func(t *testing.T) {
		ex := &FuncCall{Name: "RECENCY_SCORE", Args: []Expr{
			&Literal{Val: tsOneHalfLifeAgo},
			&Literal{Val: 30.0},
		}}
		got, err := evalRecencyScore(env, ex, row)
		if err != nil {
			t.Fatalf("RECENCY_SCORE: %v", err)
		}
		expectFloat(t, got, 0.5, 1e-9, "RECENCY_SCORE against statement clock")
	})

	t.Run("RAG_HYBRID_SCORE", func(t *testing.T) {
		ex := &FuncCall{Name: "RAG_HYBRID_SCORE", Args: []Expr{
			&Literal{Val: 1.0}, // similarity=1.0 -> simNorm=1.0
			&Literal{Val: tsOneHalfLifeAgo},
			&Literal{Val: 30.0},
			&Literal{Val: 0.0}, // sim_weight=0 isolates the recency term
		}}
		got, err := evalRAGHybridScore(env, ex, row)
		if err != nil {
			t.Fatalf("RAG_HYBRID_SCORE: %v", err)
		}
		expectFloat(t, got, 0.5, 1e-9, "RAG_HYBRID_SCORE recency against statement clock")
	})

	t.Run("RAG_RANK_SCORE", func(t *testing.T) {
		ex := &FuncCall{Name: "RAG_RANK_SCORE", Args: []Expr{
			&Literal{Val: 1.0},              // similarity
			&Literal{Val: tsOneHalfLifeAgo}, // ts
			&Literal{Val: 30.0},             // half_life_days
			&Literal{Val: 0.0},              // quality
			&Literal{Val: 0.0},              // sim_weight
			&Literal{Val: 1.0},              // recency_weight isolates the recency term
			&Literal{Val: 0.0},              // quality_weight
		}}
		got, err := evalRAGRankScore(env, ex, row)
		if err != nil {
			t.Fatalf("RAG_RANK_SCORE: %v", err)
		}
		expectFloat(t, got, 0.5, 1e-9, "RAG_RANK_SCORE recency against statement clock")
	})
}

func TestRAGHybridScore(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()

	Execute(ctx, db, "default", mustParse(`
		CREATE TABLE rag_hybrid (
			id INT,
			created_at TEXT,
			embedding VECTOR
		)
	`))
	Execute(ctx, db, "default", mustParse(`
		INSERT INTO rag_hybrid VALUES
			(1, '2026-01-01 00:00:00', '[1.0, 0.0]'),
			(2, '2026-01-31 00:00:00', '[0.2, 1.0]')
	`))

	// Newer but slightly less similar doc should win due recency weighting.
	rs := execSQL(t, db, `
		SELECT id,
		       RAG_HYBRID_SCORE(
		           VEC_COSINE_SIMILARITY(embedding, VEC_FROM_JSON('[1.0, 0.0]')),
		           created_at,
		           7,
		           0.6,
		           '2026-01-31 00:00:00'
		       ) AS rag_score
		FROM rag_hybrid
		ORDER BY rag_score DESC
	`)
	if len(rs.Rows) != 2 {
		t.Fatalf("RAG_HYBRID_SCORE: expected 2 results, got %d", len(rs.Rows))
	}
	if rs.Rows[0]["id"] != 2 {
		t.Fatalf("RAG_HYBRID_SCORE expected id=2 on top, got %v", rs.Rows[0]["id"])
	}

	// Add a recency filter and keep both scoring and filtering in one combined expression.
	rsFiltered := execSQL(t, db, `
		SELECT id,
		       RAG_HYBRID_SCORE(
		           VEC_COSINE_SIMILARITY(embedding, VEC_FROM_JSON('[1.0, 0.0]')),
		           created_at,
		           7,
		           0.6,
		           '2026-01-31 00:00:00'
		       ) AS rag_score
		FROM rag_hybrid
		WHERE RAG_HYBRID_SCORE(
		          VEC_COSINE_SIMILARITY(embedding, VEC_FROM_JSON('[1.0, 0.0]')),
		          created_at,
		          7,
		          0.6,
		          '2026-01-31 00:00:00'
		      ) > 0.7
		ORDER BY rag_score DESC
	`)
	if len(rsFiltered.Rows) != 1 {
		t.Fatalf("RAG_HYBRID_SCORE with threshold: expected 1 row, got %d", len(rsFiltered.Rows))
	}
}

func TestRAGRankScoreWithQuality(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()

	Execute(ctx, db, "default", mustParse(`
		CREATE TABLE rag_ranked (
			id INT,
			created_at TEXT,
			quality FLOAT,
			embedding VECTOR
		)
	`))
	Execute(ctx, db, "default", mustParse(`
		INSERT INTO rag_ranked VALUES
			(1, '2026-01-01 00:00:00', 0.2, '[1.0, 0.0]'),
			(2, '2026-01-31 00:00:00', 0.1, '[0.8, 0.2]'),
			(3, '2026-01-15 00:00:00', 1.0, '[0.6, 0.8]')
	`))

	rs := execSQL(t, db, `
		SELECT id,
		       RAG_RANK_SCORE(
		           VEC_COSINE_SIMILARITY(embedding, VEC_FROM_JSON('[1.0, 0.0]')),
		           created_at,
		           30,
		           quality,
		           0.45,
		           0.15,
		           0.40,
		           '2026-01-31 00:00:00'
		       ) AS rag_score
		FROM rag_ranked
		ORDER BY rag_score DESC
	`)
	if len(rs.Rows) != 3 {
		t.Fatalf("RAG_RANK_SCORE: expected 3 rows, got %d", len(rs.Rows))
	}
	if rs.Rows[0]["id"] != 3 {
		t.Fatalf("RAG_RANK_SCORE expected quality-boosted id=3 on top, got %v", rs.Rows[0]["id"])
	}
}

func TestRAGContextLoadsPreviousChunks(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()

	Execute(ctx, db, "default", mustParse(`
		CREATE TABLE rag_chunks (
			doc_id TEXT,
			chunk_index INT,
			chunk_text TEXT,
			quality FLOAT,
			created_at TEXT,
			embedding VECTOR
		)
	`))
	Execute(ctx, db, "default", mustParse(`
		INSERT INTO rag_chunks VALUES
			('doc-a', 0, 'intro', 0.7, '2026-01-01 00:00:00', '[0.0, 1.0]'),
			('doc-a', 1, 'setup', 0.8, '2026-01-02 00:00:00', '[0.8, 0.2]'),
			('doc-a', 2, 'answer', 0.9, '2026-01-03 00:00:00', '[1.0, 0.0]'),
			('doc-b', 0, 'other', 0.9, '2026-01-03 00:00:00', '[0.0, 1.0]')
	`))

	rs := execSQL(t, db, `
		SELECT doc_id, chunk_index, chunk_text, _context_offset
		FROM RAG_CONTEXT('rag_chunks', 'doc_id', 'chunk_index', 'doc-a', 2, 1)
		ORDER BY chunk_index
	`)
	if len(rs.Rows) != 2 {
		t.Fatalf("RAG_CONTEXT: expected 2 rows, got %d", len(rs.Rows))
	}
	if rs.Rows[0]["chunk_index"] != 1 || rs.Rows[1]["chunk_index"] != 2 {
		t.Fatalf("RAG_CONTEXT expected chunks 1 and 2, got %v / %v", rs.Rows[0]["chunk_index"], rs.Rows[1]["chunk_index"])
	}
	if rs.Rows[0]["_context_offset"] != -1 || rs.Rows[1]["_context_offset"] != 0 {
		t.Fatalf("RAG_CONTEXT unexpected offsets: %v / %v", rs.Rows[0]["_context_offset"], rs.Rows[1]["_context_offset"])
	}
}

func TestRAGContextFromCTEHits(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()

	Execute(ctx, db, "default", mustParse(`
		CREATE TABLE rag_chunks (
			doc_id TEXT,
			chunk_index INT,
			chunk_text TEXT,
			embedding VECTOR
		)
	`))
	Execute(ctx, db, "default", mustParse(`
		INSERT INTO rag_chunks VALUES
			('doc-a', 0, 'intro', '[0.0, 1.0]'),
			('doc-a', 1, 'setup', '[0.8, 0.2]'),
			('doc-a', 2, 'answer', '[1.0, 0.0]'),
			('doc-b', 0, 'other', '[0.0, 1.0]')
	`))

	rs := execSQL(t, db, `
		WITH topk AS (
			SELECT doc_id, chunk_index, _vec_rank
			FROM VEC_SEARCH('rag_chunks', 'embedding', VEC_FROM_JSON('[1.0, 0.0]'), 1, 'cosine')
		)
		SELECT doc_id, chunk_index, chunk_text, _hit_rank, _context_offset
		FROM RAG_CONTEXT_FROM('rag_chunks', 'doc_id', 'chunk_index', 'topk', 'doc_id', 'chunk_index', 1)
		ORDER BY _context_rank
	`)
	if len(rs.Rows) != 2 {
		t.Fatalf("RAG_CONTEXT_FROM: expected 2 rows, got %d", len(rs.Rows))
	}
	if rs.Rows[0]["chunk_index"] != 1 || rs.Rows[1]["chunk_index"] != 2 {
		t.Fatalf("RAG_CONTEXT_FROM expected previous and hit chunks, got %v / %v", rs.Rows[0]["chunk_index"], rs.Rows[1]["chunk_index"])
	}
	if rs.Rows[0]["_hit_rank"] != 1 || rs.Rows[1]["_hit_rank"] != 1 {
		t.Fatalf("RAG_CONTEXT_FROM expected hit rank 1 for context rows")
	}
}

func TestRAGContextFromMergesOverlappingHitProvenance(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()

	Execute(ctx, db, "default", mustParse(`
		CREATE TABLE rag_chunks (
			doc_id TEXT,
			chunk_index INT,
			chunk_text TEXT
		)
	`))
	Execute(ctx, db, "default", mustParse(`
		INSERT INTO rag_chunks VALUES
			('doc-a', 0, 'zero'),
			('doc-a', 1, 'one'),
			('doc-a', 2, 'two'),
			('doc-a', 3, 'three')
	`))
	Execute(ctx, db, "default", mustParse(`
		CREATE TABLE rag_hits (
			doc_id TEXT,
			chunk_index INT,
			rank INT
		)
	`))
	Execute(ctx, db, "default", mustParse(`
		INSERT INTO rag_hits VALUES
			('doc-a', 1, 2),
			('doc-a', 2, 1)
	`))

	rs := execSQL(t, db, `
		SELECT chunk_index, _hit_rank, _context_offset, _context_rank, _context_hits
		FROM RAG_CONTEXT_FROM('rag_chunks', 'doc_id', 'chunk_index', 'rag_hits', 'doc_id', 'chunk_index', 1, 1)
		ORDER BY _context_rank
	`)
	if len(rs.Rows) != 4 {
		t.Fatalf("RAG_CONTEXT_FROM: expected 4 unique context rows, got %d", len(rs.Rows))
	}

	want := []struct {
		chunk, hitRank, offset, contextRank, hitCount int
	}{
		{1, 1, -1, 1, 2},
		{2, 1, 0, 2, 2},
		{3, 1, 1, 3, 1},
		{0, 2, -1, 4, 1},
	}
	for i, expected := range want {
		got := rs.Rows[i]
		if got["chunk_index"] != expected.chunk ||
			got["_hit_rank"] != expected.hitRank ||
			got["_context_offset"] != expected.offset ||
			got["_context_rank"] != expected.contextRank ||
			got["_context_hits"] != expected.hitCount {
			t.Fatalf("row %d: got %#v, want chunk=%d rank=%d offset=%d context_rank=%d hits=%d", i, got, expected.chunk, expected.hitRank, expected.offset, expected.contextRank, expected.hitCount)
		}
	}
}

// ─────────────────────────── RAG_SEARCH (composed retrieval) ────────────────

// TestRAGSearchVectorOnly checks that a plain (no-options, or metric-only)
// RAG_SEARCH call behaves exactly like VEC_SEARCH truncated to k: results
// come back in descending-similarity order and carry VEC_SEARCH's own
// trailing columns.
func TestRAGSearchVectorOnly(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()

	Execute(ctx, db, "default", mustParse(`
		CREATE TABLE rag_docs (id INT, content TEXT, embedding VECTOR)
	`))
	Execute(ctx, db, "default", mustParse(`
		INSERT INTO rag_docs VALUES
			(1, 'Go is a compiled systems programming language', '[1.0, 0.1, 0.0]'),
			(2, 'Python is popular for data science and machine learning', '[0.0, 0.9, 0.1]'),
			(3, 'Rust provides memory safety without garbage collection', '[0.1, 0.0, 1.0]'),
			(4, 'Go programming with goroutines for concurrency', '[0.9, 0.2, 0.0]')
	`))

	// Query vector closest to id=1, then id=4 (cosine similarity).
	rs := execSQL(t, db, `
		SELECT * FROM RAG_SEARCH('rag_docs', 'embedding', VEC_FROM_JSON('[1.0, 0.0, 0.0]'), 2)
	`)
	if len(rs.Rows) != 2 {
		t.Fatalf("RAG_SEARCH vector-only: expected 2 rows, got %d", len(rs.Rows))
	}
	if rs.Rows[0]["id"] != 1 || rs.Rows[1]["id"] != 4 {
		t.Fatalf("RAG_SEARCH vector-only: expected ids [1,4] in similarity order, got [%v,%v]", rs.Rows[0]["id"], rs.Rows[1]["id"])
	}
	for _, col := range []string{"_vec_distance", "_vec_similarity", "_vec_rank"} {
		if _, ok := rs.Rows[0][col]; !ok {
			t.Errorf("RAG_SEARCH vector-only: expected column %q in result", col)
		}
	}
	if rs.Rows[0]["_vec_rank"] != 1 {
		t.Errorf("RAG_SEARCH vector-only: expected top row _vec_rank=1, got %v", rs.Rows[0]["_vec_rank"])
	}

	// Same, but with an explicit metric-only options object (no hybrid/expand).
	rsMetric := execSQL(t, db, `
		SELECT * FROM RAG_SEARCH('rag_docs', 'embedding', VEC_FROM_JSON('[1.0, 0.0, 0.0]'), 2, '{"metric": "cosine"}')
	`)
	if len(rsMetric.Rows) != 2 || rsMetric.Rows[0]["id"] != 1 || rsMetric.Rows[1]["id"] != 4 {
		t.Fatalf("RAG_SEARCH with metric-only options: expected ids [1,4], got %v/%v (%d rows)", rsMetric.Rows[0]["id"], rsMetric.Rows[1]["id"], len(rsMetric.Rows))
	}
}

// TestRAGSearchHybridRRF verifies that turning on hybrid text+vector fusion
// changes the ranking versus a vector-only call in a way that demonstrates
// the FTS signal actually contributed: a row that's a strong text match but
// weak vector match should climb into the top-k under hybrid mode even
// though it does not appear in the vector-only top-k.
func TestRAGSearchHybridRRF(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()

	Execute(ctx, db, "default", mustParse(`
		CREATE TABLE hybrid_docs (id INT, content TEXT, embedding VECTOR)
	`))
	Execute(ctx, db, "default", mustParse(`
		INSERT INTO hybrid_docs VALUES
			(1, 'quantum entanglement physics breakthrough', '[1.0, 0.0, 0.0]'),
			(2, 'cooking recipes for dinner',                 '[0.0, 0.0, 1.0]'),
			(3, 'gardening tips for spring',                  '[0.0, 0.1, 0.9]'),
			(4, 'financial markets update',                   '[0.9, 0.0, 0.1]')
	`))

	// Vector-only: query vector [0,0,1] is closest to id=2, then id=3; id=1
	// (perpendicular to the query) ranks last.
	rsVecOnly := execSQL(t, db, `
		SELECT * FROM RAG_SEARCH('hybrid_docs', 'embedding', VEC_FROM_JSON('[0.0, 0.0, 1.0]'), 2)
	`)
	if len(rsVecOnly.Rows) != 2 || rsVecOnly.Rows[0]["id"] != 2 || rsVecOnly.Rows[1]["id"] != 3 {
		t.Fatalf("RAG_SEARCH vector-only baseline: expected ids [2,3], got %v/%v", rsVecOnly.Rows[0]["id"], rsVecOnly.Rows[1]["id"])
	}

	// Hybrid: id=1 is the only document matching the text query ("quantum
	// entanglement"), so it should be pulled into the top-2 by RRF fusion
	// even though its vector similarity to the query is the weakest of all.
	rsHybrid := execSQL(t, db, `
		SELECT * FROM RAG_SEARCH('hybrid_docs', 'embedding', VEC_FROM_JSON('[0.0, 0.0, 1.0]'), 2, '{
			"text_column": "content",
			"text_query": "quantum entanglement",
			"key_columns": ["id"]
		}')
	`)
	if len(rsHybrid.Rows) != 2 {
		t.Fatalf("RAG_SEARCH hybrid: expected 2 rows, got %d", len(rsHybrid.Rows))
	}
	if rsHybrid.Rows[0]["id"] != 1 {
		t.Fatalf("RAG_SEARCH hybrid: expected text-strong id=1 to rank first, got %v (full: %v/%v)", rsHybrid.Rows[0]["id"], rsHybrid.Rows[0]["id"], rsHybrid.Rows[1]["id"])
	}
	if rsHybrid.Rows[1]["id"] == 3 {
		t.Fatalf("RAG_SEARCH hybrid: expected id=3 (pure vector runner-up) to be displaced by the text-strong id=1, got %v/%v", rsHybrid.Rows[0]["id"], rsHybrid.Rows[1]["id"])
	}
	for _, col := range []string{"_vec_rank", "_fts_rank", "_rrf_score", "_rrf_rank"} {
		if _, ok := rsHybrid.Rows[0][col]; !ok {
			t.Errorf("RAG_SEARCH hybrid: expected column %q on top row, got row %#v", col, rsHybrid.Rows[0])
		}
	}
	if rsHybrid.Rows[0]["_rrf_rank"] != 1 {
		t.Errorf("RAG_SEARCH hybrid: expected top row _rrf_rank=1, got %v", rsHybrid.Rows[0]["_rrf_rank"])
	}
}

// TestRAGSearchAutoOrExpansion proves that RAG_SEARCH's hybrid text pass
// defaults to ftsAutoOrExpand's OR-expansion: a verbose natural-language
// query that FTS_SEARCH's own implicit-AND parsing would match against
// nothing (every stray stopword like "what"/"is" becomes a required AND
// term) still contributes an FTS hit once routed through RAG_SEARCH.
func TestRAGSearchAutoOrExpansion(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()

	Execute(ctx, db, "default", mustParse(`
		CREATE TABLE geo_docs (id INT, content TEXT, embedding VECTOR)
	`))
	Execute(ctx, db, "default", mustParse(`
		INSERT INTO geo_docs VALUES
			(1, 'Paris is the capital of France', '[1.0, 0.0]'),
			(2, 'Random unrelated text about weather', '[0.0, 1.0]')
	`))

	nlQuery := "what is the capital of France"

	// Baseline: FTS_SEARCH's own implicit-AND parsing treats every token in
	// the raw query (including stopwords like "what"/"is") as a required
	// term, so nothing in this corpus matches all of them at once.
	rsFTSDirect := execSQL(t, db, `SELECT * FROM FTS_SEARCH('geo_docs', '`+nlQuery+`', 5)`)
	if len(rsFTSDirect.Rows) != 0 {
		t.Fatalf("FTS_SEARCH direct implicit-AND: expected 0 matches for verbose NL query, got %d", len(rsFTSDirect.Rows))
	}

	// RAG_SEARCH hybrid (default auto_or_expand=true) should still surface an
	// FTS contribution: id=1 ("capital of France") must carry _fts_rank,
	// proving the OR-expanded query ("capital OR france", after stopword
	// removal) matched it, while id=2 must not.
	rsHybrid := execSQL(t, db, `
		SELECT * FROM RAG_SEARCH('geo_docs', 'embedding', VEC_FROM_JSON('[1.0, 0.0]'), 2, '{
			"text_column": "content",
			"text_query": "`+nlQuery+`",
			"key_columns": ["id"]
		}')
	`)
	if len(rsHybrid.Rows) != 2 {
		t.Fatalf("RAG_SEARCH hybrid auto-or: expected 2 rows, got %d", len(rsHybrid.Rows))
	}
	var row1, row2 Row
	for _, r := range rsHybrid.Rows {
		switch r["id"] {
		case 1:
			row1 = r
		case 2:
			row2 = r
		}
	}
	if row1 == nil {
		t.Fatalf("RAG_SEARCH hybrid auto-or: expected id=1 in results, got %#v", rsHybrid.Rows)
	}
	if _, ok := row1["_fts_rank"]; !ok {
		t.Errorf("RAG_SEARCH hybrid auto-or: expected id=1 to carry _fts_rank (OR-expanded match), row=%#v", row1)
	}
	if row2 != nil {
		if _, ok := row2["_fts_rank"]; ok {
			t.Errorf("RAG_SEARCH hybrid auto-or: expected id=2 to have no _fts_rank (no OR-term match), row=%#v", row2)
		}
	}
}

// TestRAGSearchExpandsContext checks that expand_before/expand_after on
// RAG_SEARCH produces the same neighbor-expansion contract as
// RAG_CONTEXT_FROM: _hit_rank/_context_offset/_context_rank/_context_hits.
func TestRAGSearchExpandsContext(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()

	Execute(ctx, db, "default", mustParse(`
		CREATE TABLE rag_chunks (
			doc_id TEXT,
			chunk_index INT,
			chunk_text TEXT,
			embedding VECTOR
		)
	`))
	Execute(ctx, db, "default", mustParse(`
		INSERT INTO rag_chunks VALUES
			('doc-a', 0, 'intro', '[0.0, 1.0]'),
			('doc-a', 1, 'setup', '[0.8, 0.2]'),
			('doc-a', 2, 'answer', '[1.0, 0.0]'),
			('doc-b', 0, 'other', '[0.0, 1.0]')
	`))

	rs := execSQL(t, db, `
		SELECT doc_id, chunk_index, chunk_text, _hit_rank, _context_offset, _context_rank, _context_hits
		FROM RAG_SEARCH('rag_chunks', 'embedding', VEC_FROM_JSON('[1.0, 0.0]'), 1, '{
			"expand_before": 1,
			"expand_after": 0,
			"doc_id_column": "doc_id",
			"chunk_index_column": "chunk_index"
		}')
		ORDER BY _context_rank
	`)
	if len(rs.Rows) != 2 {
		t.Fatalf("RAG_SEARCH expand: expected 2 rows, got %d", len(rs.Rows))
	}
	if rs.Rows[0]["chunk_index"] != 1 || rs.Rows[1]["chunk_index"] != 2 {
		t.Fatalf("RAG_SEARCH expand: expected chunks 1 and 2, got %v / %v", rs.Rows[0]["chunk_index"], rs.Rows[1]["chunk_index"])
	}
	if rs.Rows[0]["_context_offset"] != -1 || rs.Rows[1]["_context_offset"] != 0 {
		t.Fatalf("RAG_SEARCH expand: unexpected offsets: %v / %v", rs.Rows[0]["_context_offset"], rs.Rows[1]["_context_offset"])
	}
	if rs.Rows[0]["_hit_rank"] != 1 || rs.Rows[1]["_hit_rank"] != 1 {
		t.Fatalf("RAG_SEARCH expand: expected hit rank 1 for both context rows, got %v / %v", rs.Rows[0]["_hit_rank"], rs.Rows[1]["_hit_rank"])
	}
	if rs.Rows[0]["_context_rank"] != 1 || rs.Rows[1]["_context_rank"] != 2 {
		t.Fatalf("RAG_SEARCH expand: unexpected context rank ordering: %v / %v", rs.Rows[0]["_context_rank"], rs.Rows[1]["_context_rank"])
	}
	if rs.Rows[0]["_context_hits"] != 1 || rs.Rows[1]["_context_hits"] != 1 {
		t.Fatalf("RAG_SEARCH expand: expected _context_hits=1 for both rows, got %v / %v", rs.Rows[0]["_context_hits"], rs.Rows[1]["_context_hits"])
	}
	if rs.Rows[0]["chunk_text"] != "setup" || rs.Rows[1]["chunk_text"] != "answer" {
		t.Fatalf("RAG_SEARCH expand: unexpected chunk_text: %v / %v", rs.Rows[0]["chunk_text"], rs.Rows[1]["chunk_text"])
	}
}

// TestRAGSearchRequiresKeyColumnsForHybrid checks that enabling hybrid mode
// (text_column+text_query) without key_columns fails with a clear error
// instead of silently fusing rows with an empty composite key.
// TestRAGSearchHybridMultipleTextColumns covers text_columns: a chunked-
// document corpus carries its most discriminative words in a short heading,
// not the body, and a single-column BM25 pass cannot see them. Here the only
// occurrence of "goroutines" is in a heading, and the row whose heading
// matches must be fused into the top-k even though its vector similarity is
// the weakest in the table.
func TestRAGSearchHybridMultipleTextColumns(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()

	Execute(ctx, db, "default", mustParse(`
		CREATE TABLE multicol_docs (id INT, heading TEXT, content TEXT, embedding VECTOR)
	`))
	Execute(ctx, db, "default", mustParse(`
		INSERT INTO multicol_docs VALUES
			(1, 'Concurrency with goroutines', 'lightweight threads scheduled by the runtime', '[1.0, 0.0, 0.0]'),
			(2, 'Cooking',                     'recipes for dinner',                          '[0.0, 0.0, 1.0]'),
			(3, 'Gardening',                   'tips for spring planting',                    '[0.0, 0.1, 0.9]')
	`))

	// Body-only BM25 cannot match "goroutines", so id=1 stays out of the top-2.
	rsBodyOnly := execSQL(t, db, `
		SELECT * FROM RAG_SEARCH('multicol_docs', 'embedding', VEC_FROM_JSON('[0.0, 0.0, 1.0]'), 2, '{
			"text_column": "content",
			"text_query": "goroutines",
			"key_columns": ["id"]
		}')
	`)
	for _, r := range rsBodyOnly.Rows {
		if r["id"] == 1 {
			t.Fatalf("body-only BM25 should not have matched the heading-only term, got %#v", rsBodyOnly.Rows)
		}
	}

	// Searching the heading too surfaces it.
	rsBoth := execSQL(t, db, `
		SELECT * FROM RAG_SEARCH('multicol_docs', 'embedding', VEC_FROM_JSON('[0.0, 0.0, 1.0]'), 2, '{
			"text_columns": ["heading", "content"],
			"text_query": "goroutines",
			"key_columns": ["id"]
		}')
	`)
	if len(rsBoth.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rsBoth.Rows))
	}
	if rsBoth.Rows[0]["id"] != 1 {
		t.Fatalf("expected heading match id=1 to rank first, got %#v", rsBoth.Rows)
	}
	if _, ok := rsBoth.Rows[0]["_fts_rank"]; !ok {
		t.Errorf("expected _fts_rank on the heading-matched row, got %#v", rsBoth.Rows[0])
	}

	// text_column and text_columns compose, and naming a column in both must
	// not double-count it: the result is identical either way.
	rsMerged := execSQL(t, db, `
		SELECT * FROM RAG_SEARCH('multicol_docs', 'embedding', VEC_FROM_JSON('[0.0, 0.0, 1.0]'), 2, '{
			"text_column": "heading",
			"text_columns": ["heading", "content"],
			"text_query": "goroutines",
			"key_columns": ["id"]
		}')
	`)
	if len(rsMerged.Rows) != len(rsBoth.Rows) {
		t.Fatalf("merged column list changed the row count: %d vs %d", len(rsMerged.Rows), len(rsBoth.Rows))
	}
	for i := range rsMerged.Rows {
		if rsMerged.Rows[i]["id"] != rsBoth.Rows[i]["id"] {
			t.Fatalf("merged column list changed ranking at %d: %v vs %v", i, rsMerged.Rows[i]["id"], rsBoth.Rows[i]["id"])
		}
	}
}

func TestRAGSearchRequiresKeyColumnsForHybrid(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()

	Execute(ctx, db, "default", mustParse(`CREATE TABLE nokey_docs (id INT, content TEXT, embedding VECTOR)`))
	Execute(ctx, db, "default", mustParse(`INSERT INTO nokey_docs VALUES (1, 'hello world', '[1.0, 0.0]')`))

	_, err := Execute(ctx, db, "default", mustParse(`
		SELECT * FROM RAG_SEARCH('nokey_docs', 'embedding', VEC_FROM_JSON('[1.0, 0.0]'), 1, '{
			"text_column": "content",
			"text_query": "hello"
		}')
	`))
	if err == nil {
		t.Fatal("RAG_SEARCH: expected error when hybrid mode is missing key_columns, got nil")
	}
	if !strings.Contains(err.Error(), "key_columns") {
		t.Errorf("RAG_SEARCH: expected error to mention key_columns, got: %v", err)
	}
}

// TestRAGSearchInvalidMetric checks that an unknown metric in the options
// JSON produces a clear error.
func TestRAGSearchInvalidMetric(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()

	Execute(ctx, db, "default", mustParse(`CREATE TABLE metric_docs (id INT, embedding VECTOR)`))
	Execute(ctx, db, "default", mustParse(`INSERT INTO metric_docs VALUES (1, '[1.0, 0.0]')`))

	_, err := Execute(ctx, db, "default", mustParse(`
		SELECT * FROM RAG_SEARCH('metric_docs', 'embedding', VEC_FROM_JSON('[1.0, 0.0]'), 1, '{"metric": "bogus"}')
	`))
	if err == nil {
		t.Fatal("RAG_SEARCH: expected error for unknown metric, got nil")
	}
	if !strings.Contains(err.Error(), "unknown metric") {
		t.Errorf("RAG_SEARCH: expected error to mention unknown metric, got: %v", err)
	}
}

// TestRAGSearchInvalidOptionsJSON checks that malformed options JSON produces
// a clear error rather than a panic or silent misbehavior.
func TestRAGSearchInvalidOptionsJSON(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()

	Execute(ctx, db, "default", mustParse(`CREATE TABLE badjson_docs (id INT, embedding VECTOR)`))
	Execute(ctx, db, "default", mustParse(`INSERT INTO badjson_docs VALUES (1, '[1.0, 0.0]')`))

	_, err := Execute(ctx, db, "default", mustParse(`
		SELECT * FROM RAG_SEARCH('badjson_docs', 'embedding', VEC_FROM_JSON('[1.0, 0.0]'), 1, '{not valid json')
	`))
	if err == nil {
		t.Fatal("RAG_SEARCH: expected error for invalid options JSON, got nil")
	}
	if !strings.Contains(err.Error(), "invalid options JSON") {
		t.Errorf("RAG_SEARCH: expected error to mention invalid options JSON, got: %v", err)
	}
}

// TestRAGSearchViaJoin is a sanity check that RAG_SEARCH works generically as
// a FROM-clause table function used inside a JOIN (not just a standalone
// SELECT FROM RAG_SEARCH(...)), since the TableFunction registry dispatches
// any registered function through the same processJoins code path.
func TestRAGSearchViaJoin(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()

	Execute(ctx, db, "default", mustParse(`
		CREATE TABLE rag_docs (id INT, content TEXT, embedding VECTOR)
	`))
	Execute(ctx, db, "default", mustParse(`
		INSERT INTO rag_docs VALUES
			(1, 'Go is a compiled systems programming language', '[1.0, 0.1, 0.0]'),
			(2, 'Python is popular for data science and machine learning', '[0.0, 0.9, 0.1]'),
			(3, 'Rust provides memory safety without garbage collection', '[0.1, 0.0, 1.0]'),
			(4, 'Go programming with goroutines for concurrency', '[0.9, 0.2, 0.0]')
	`))
	Execute(ctx, db, "default", mustParse(`
		CREATE TABLE doc_meta (id INT, label TEXT)
	`))
	Execute(ctx, db, "default", mustParse(`
		INSERT INTO doc_meta VALUES (1, 'lang-go'), (2, 'lang-py'), (3, 'lang-rust'), (4, 'lang-go2')
	`))

	rs := execSQL(t, db, `
		SELECT m.label AS label, r.id AS doc_id, r._vec_rank AS vec_rank
		FROM doc_meta AS m
		JOIN RAG_SEARCH('rag_docs', 'embedding', VEC_FROM_JSON('[1.0, 0.0, 0.0]'), 2) AS r ON r.id = m.id
		ORDER BY vec_rank
	`)
	if len(rs.Rows) != 2 {
		t.Fatalf("RAG_SEARCH via JOIN: expected 2 rows, got %d", len(rs.Rows))
	}
	if rs.Rows[0]["doc_id"] != 1 || rs.Rows[0]["label"] != "lang-go" {
		t.Fatalf("RAG_SEARCH via JOIN: expected top row id=1/label=lang-go, got id=%v label=%v", rs.Rows[0]["doc_id"], rs.Rows[0]["label"])
	}
	if rs.Rows[1]["doc_id"] != 4 || rs.Rows[1]["label"] != "lang-go2" {
		t.Fatalf("RAG_SEARCH via JOIN: expected second row id=4/label=lang-go2, got id=%v label=%v", rs.Rows[1]["doc_id"], rs.Rows[1]["label"])
	}
}
