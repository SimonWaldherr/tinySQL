// Tests for improved FTS (phrase search, boolean ops, wildcards, FTS_SEARCH TVF),
// BLOB functions, vector binary encoding, and text utilities.
package engine

import (
	"context"
	"math"
	"strings"
	"testing"

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

func TestBlobFromHex(t *testing.T) {
	db := storage.NewDB()
	rs := execSQL(t, db, `SELECT BLOB_FROM_HEX('deadbeef') as b`)
	b, ok := rs.Rows[0]["b"].(string)
	if !ok {
		t.Fatalf("expected string, got %T", rs.Rows[0]["b"])
	}
	if b != "deadbeef" {
		t.Errorf("expected 'deadbeef', got %q", b)
	}
}

func TestBlobSubstr(t *testing.T) {
	db := storage.NewDB()
	// 'deadbeef' = [0xde, 0xad, 0xbe, 0xef], substr(1, 2) = [0xad, 0xbe] = 'adbe'
	rs := execSQL(t, db, `SELECT BLOB_SUBSTR('deadbeef', 1, 2) as s`)
	s, ok := rs.Rows[0]["s"].(string)
	if !ok {
		t.Fatalf("expected string, got %T", rs.Rows[0]["s"])
	}
	if s != "adbe" {
		t.Errorf("expected 'adbe', got %q", s)
	}
}

func TestBlobConcat(t *testing.T) {
	db := storage.NewDB()
	// 'dead' + 'beef' = 'deadbeef'
	rs := execSQL(t, db, `SELECT BLOB_CONCAT('dead', 'beef') as b`)
	b, ok := rs.Rows[0]["b"].(string)
	if !ok {
		t.Fatalf("expected string, got %T", rs.Rows[0]["b"])
	}
	if b != "deadbeef" {
		t.Errorf("expected 'deadbeef', got %q", b)
	}
}

func TestBlobBase64RoundTrip(t *testing.T) {
	db := storage.NewDB()
	// Encode then decode should give back the original.
	rs := execSQL(t, db, `SELECT BLOB_FROM_BASE64(BLOB_TO_BASE64('deadbeef')) as b`)
	b, ok := rs.Rows[0]["b"].(string)
	if !ok {
		t.Fatalf("expected string, got %T", rs.Rows[0]["b"])
	}
	if b != "deadbeef" {
		t.Errorf("BLOB base64 round-trip failed: got %q", b)
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
