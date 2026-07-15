package mcpserver

import (
	"strings"
	"testing"
)

// TestFunctionsCatalogListsRAGSurface guards discoverability: this catalog
// (served as the tinysql://functions resource) is currently the only way an
// MCP client learns that tinySQL's vector/full-text/RAG functions exist at
// all, since no tool description previously mentioned them. Losing any of
// these names from the catalog text silently makes that function
// undiscoverable over MCP again.
func TestFunctionsCatalogListsRAGSurface(t *testing.T) {
	mustContain := []string{
		"VEC_SEARCH", "VEC_TOP_K", "VEC_WARM",
		"FTS_SEARCH", "FTS_MATCH", "FTS_RANK",
		"RAG_CONTEXT", "RAG_CONTEXT_FROM",
		"RECENCY_SCORE", "RAG_HYBRID_SCORE", "RAG_RANK_SCORE",
		"_vec_similarity", "_vec_distance", "_vec_rank",
		"_fts_score", "_fts_rank",
	}
	for _, name := range mustContain {
		if !strings.Contains(functionsCatalog, name) {
			t.Errorf("functionsCatalog missing %q", name)
		}
	}
}
