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
	// Nested headings carry their parent path: "FTS" alone is far less
	// informative to an embedding model than "Title › FTS".
	if chunks[len(chunks)-1].Heading != "Title › FTS" {
		t.Fatalf("last heading = %q", chunks[len(chunks)-1].Heading)
	}
}

// A "#" line inside a fenced code block is a shell comment, not a heading.
// Treating it as one splits a code example into fragments and mislabels the
// section it belongs to — a real risk on a corpus of CLI documentation.
func TestChunkMarkdownIgnoresHeadingsInsideCodeFences(t *testing.T) {
	doc := "# CLI\nrun it:\n```bash\n# build the index\ntinysql warm --table docs\n```\ndone\n## Next\ntext"
	chunks := chunkMarkdown(doc, 900, 100)
	if len(chunks) != 2 {
		t.Fatalf("expected 2 sections, got %d: %+v", len(chunks), chunks)
	}
	if chunks[0].Heading != "CLI" {
		t.Fatalf("first heading = %q, want %q", chunks[0].Heading, "CLI")
	}
	if !strings.Contains(chunks[0].Text, "# build the index") || !strings.Contains(chunks[0].Text, "tinysql warm") {
		t.Fatalf("code fence was split out of its section: %q", chunks[0].Text)
	}
	if chunks[1].Heading != "CLI › Next" {
		t.Fatalf("second heading = %q, want %q", chunks[1].Heading, "CLI › Next")
	}
}

// Deeper-then-shallower headings must pop the trail rather than accumulate, so
// a sibling section does not inherit its predecessor's children.
func TestChunkMarkdownPopsHeadingTrail(t *testing.T) {
	doc := "# A\n### deep\nx\n## B\ny"
	chunks := chunkMarkdown(doc, 900, 100)
	if len(chunks) != 2 {
		t.Fatalf("expected 2 chunks, got %d: %+v", len(chunks), chunks)
	}
	if chunks[1].Heading != "A › B" {
		t.Fatalf("heading = %q, want %q", chunks[1].Heading, "A › B")
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

// TestRetrieveSQLHybridRequestsFusedColumns pins the contract retrieve()
// depends on: hybrid mode must ask RAG_SEARCH for both passes plus the fused
// score, and must identify rows across those passes with key_columns. Without
// key_columns RAG_SEARCH rejects the call; without _rrf_score the demo would
// silently fall back to ranking a fused result by vector similarity alone.
func TestRetrieveSQLHybridRequestsFusedColumns(t *testing.T) {
	cfg := config{topK: 5, candidateK: 15, hybrid: true}
	sql, err := retrieveSQL(cfg, []float64{0.5, -0.25}, "How do I set a query timeout?")
	if err != nil {
		t.Fatalf("retrieveSQL: %v", err)
	}
	for _, want := range []string{
		"_rrf_score", "_fts_rank", "_vec_similarity",
		`"text_columns":["heading","chunk_text"]`,
		`"key_columns":["doc_id","chunk_index"]`,
		`"candidate_k":15`,
		"VEC_FROM_JSON('[0.5,-0.25]')",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("hybrid SQL missing %q:\n%s", want, sql)
		}
	}
	if _, err := tsql.ParseSQL(sql); err != nil {
		t.Fatalf("hybrid SQL does not parse: %v\n%s", err, sql)
	}
}

// A vector-only run must not select the fusion columns: RAG_SEARCH only emits
// _fts_rank/_rrf_score when hybrid options are supplied, so selecting them
// unconditionally would fail the query outright.
func TestRetrieveSQLVectorOnlyOmitsFusionColumns(t *testing.T) {
	sql, err := retrieveSQL(config{topK: 3, candidateK: 9}, []float64{1}, "anything")
	if err != nil {
		t.Fatalf("retrieveSQL: %v", err)
	}
	for _, unwanted := range []string{"_rrf_score", "_fts_rank", "text_query"} {
		if strings.Contains(sql, unwanted) {
			t.Fatalf("vector-only SQL should not mention %q:\n%s", unwanted, sql)
		}
	}
	if _, err := tsql.ParseSQL(sql); err != nil {
		t.Fatalf("vector-only SQL does not parse: %v\n%s", err, sql)
	}
}

// A quote in the question must survive into the options JSON as SQL-escaped
// text; an unescaped one would terminate the options string literal early and
// turn a user question into a parse error (or worse, stray SQL).
func TestRetrieveSQLEscapesQuotesInQuery(t *testing.T) {
	sql, err := retrieveSQL(config{topK: 2, candidateK: 4, hybrid: true}, []float64{1}, "what's a chunk's index?")
	if err != nil {
		t.Fatalf("retrieveSQL: %v", err)
	}
	if strings.Contains(sql, "what's") {
		t.Fatalf("query quote left unescaped:\n%s", sql)
	}
	if _, err := tsql.ParseSQL(sql); err != nil {
		t.Fatalf("SQL with quoted query does not parse: %v\n%s", err, sql)
	}
}
