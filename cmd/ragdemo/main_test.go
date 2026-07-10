package main

import (
	"strings"
	"testing"
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

func TestFTSQueryUsesKeywordsWithOR(t *testing.T) {
	got := ftsQuery("How do I prebuild the HNSW vector index after loading?")
	want := "prebuild OR hnsw OR vector OR index OR after OR loading"
	if got != want {
		t.Fatalf("ftsQuery = %q, want %q", got, want)
	}
}
