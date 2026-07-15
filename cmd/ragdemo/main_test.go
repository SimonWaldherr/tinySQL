package main

import (
	"context"
	"strings"
	"testing"

	tsql "github.com/SimonWaldherr/tinySQL"
)

func TestChunkMarkdownKeepsHeadingAndBounds(t *testing.T) {
	doc := "# Title\nintro\n## Vector Search\n" + strings.Repeat("word ", 80) + "\n## FTS\nkeywords"
	chunks := chunkMarkdown(doc, 120, 20)
	if len(chunks) < 4 {
		t.Fatalf("expected multiple chunks, got %d", len(chunks))
	}
	if chunks[0].Heading != "Title" {
		t.Fatalf("first heading = %q", chunks[0].Heading)
	}
	for _, c := range chunks {
		if len([]rune(c.Text)) > 120 {
			t.Fatalf("chunk exceeds bound: %d", len([]rune(c.Text)))
		}
	}
	if chunks[len(chunks)-1].Heading != "FTS" {
		t.Fatalf("last heading = %q", chunks[len(chunks)-1].Heading)
	}
}

func TestRelevantRankRequiresDocumentAndMarker(t *testing.T) {
	hits := []hit{
		{Chunk: chunk{DocID: "docs/other.md", Text: "RAG_CONTEXT_FROM"}},
		{Chunk: chunk{DocID: "docs/rag-guide.md", Text: "Use RAG_CONTEXT_FROM here"}},
	}
	tc := evalCase{DocSuffix: "rag-guide.md", Marker: "RAG_CONTEXT_FROM"}
	if got := relevantRank(hits, tc); got != 2 {
		t.Fatalf("rank = %d, want 2", got)
	}
}

func TestSplitRunesHandlesUnicode(t *testing.T) {
	parts := splitRunes(strings.Repeat("ä", 250), 100, 10)
	if len(parts) != 3 {
		t.Fatalf("parts = %d, want 3", len(parts))
	}
	for _, p := range parts {
		if len([]rune(p)) > 100 {
			t.Fatalf("too large: %d", len([]rune(p)))
		}
	}
}

// TestBuildDBInsertsQueryableRows guards buildDB's INSERT-based load path
// (it previously appended directly to table.Rows, bypassing coerceToVector
// and the table-version bump vector/FTS caches key on). A quote in doc_id/
// heading must round-trip through the escaped INSERT, and the embedding
// must be queryable via VEC_SEARCH, not just stored as a raw Go slice.
func TestBuildDBInsertsQueryableRows(t *testing.T) {
	chunks := []chunk{
		{DocID: "doc's guide.md", Index: 0, Heading: "Intro's Section", Text: "widgets are great products", Embedding: []float64{1, 0, 0}},
		{DocID: "doc2.md", Index: 0, Heading: "Other", Text: "bananas and apples", Embedding: []float64{0, 1, 0}},
	}
	db, err := buildDB(chunks)
	if err != nil {
		t.Fatalf("buildDB: %v", err)
	}

	table, err := db.Get("default", "rag_chunks")
	if err != nil {
		t.Fatalf("get table: %v", err)
	}
	if len(table.Rows) != len(chunks) {
		t.Fatalf("expected %d rows, got %d", len(chunks), len(table.Rows))
	}

	ctx := context.Background()
	stmt, err := tsql.ParseSQL(`SELECT doc_id, heading FROM rag_chunks WHERE chunk_index = 0 AND doc_id = 'doc''s guide.md'`)
	if err != nil {
		t.Fatalf("parse select: %v", err)
	}
	rs, err := tsql.Execute(ctx, db, "default", stmt)
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	if len(rs.Rows) != 1 {
		t.Fatalf("expected 1 row for quoted doc_id, got %d", len(rs.Rows))
	}
	if rs.Rows[0]["heading"] != "Intro's Section" {
		t.Fatalf("heading = %v", rs.Rows[0]["heading"])
	}

	// The embedding must be a real, queryable vector (inserted via
	// VEC_FROM_JSON), not a Go []float64 that only happens to render.
	vecStmt, err := tsql.ParseSQL(`SELECT doc_id FROM VEC_SEARCH('rag_chunks', 'embedding', VEC_FROM_JSON('[1,0,0]'), 1, 'cosine')`)
	if err != nil {
		t.Fatalf("parse vec search: %v", err)
	}
	vecRS, err := tsql.Execute(ctx, db, "default", vecStmt)
	if err != nil {
		t.Fatalf("vec search: %v", err)
	}
	if len(vecRS.Rows) != 1 || vecRS.Rows[0]["doc_id"] != "doc's guide.md" {
		t.Fatalf("vec search result = %+v", vecRS.Rows)
	}
}

func TestFTSQueryUsesKeywordsWithOR(t *testing.T) {
	got := ftsQuery("How do I prebuild the HNSW vector index after loading?")
	want := "prebuild OR hnsw OR vector OR index OR after OR loading"
	if got != want {
		t.Fatalf("ftsQuery = %q, want %q", got, want)
	}
}
