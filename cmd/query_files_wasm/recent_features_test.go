package main

import (
	"context"
	"testing"

	tinysql "github.com/SimonWaldherr/tinySQL"
)

func executeRecentFeatureSQL(t *testing.T, db *tinysql.DB, sql string) *tinysql.ResultSet {
	t.Helper()
	stmt, err := tinysql.ParseSQL(sql)
	if err != nil {
		t.Fatalf("parse %q: %v", sql, err)
	}
	result, err := tinysql.Execute(context.Background(), db, "default", stmt)
	if err != nil {
		t.Fatalf("execute %q: %v", sql, err)
	}
	return result
}

func TestRecentEngineFeaturesAreAvailableToTheWASMModule(t *testing.T) {
	db := tinysql.NewDB()
	executeRecentFeatureSQL(t, db, `CREATE TABLE docs (doc_id TEXT, chunk_index INT, chunk_text TEXT, embedding VECTOR)`)
	executeRecentFeatureSQL(t, db, `INSERT INTO docs VALUES
		('guide', 0, 'Vector search retrieves semantically related chunks.', '[1.0, 0.0, 0.0]'),
		('guide', 1, 'RAG search can add neighboring context chunks.', '[0.8, 0.2, 0.0]'),
		('other', 0, 'Geodata uses routes and coordinates.', '[0.0, 0.0, 1.0]')`)

	contains := executeRecentFeatureSQL(t, db, `SELECT CONTAINS_ALL(chunk_text, 'vector', 'search') AS all_terms,
		CONTAINS_ANY(chunk_text, 'routes', 'search') AS any_term,
		CONTAINS_SCORE(chunk_text, 'vector', 'search', 'rag') AS score
		FROM docs WHERE doc_id = 'guide' AND chunk_index = 0`)
	if len(contains.Rows) != 1 || contains.Rows[0]["all_terms"] != true || contains.Rows[0]["any_term"] != true {
		t.Fatalf("CONTAINS_* result = %#v", contains)
	}

	vectors := executeRecentFeatureSQL(t, db, `SELECT
		VEC_HAMMING_DISTANCE(VEC_BINARY_QUANTIZE(embedding), VEC_FROM_JSON('[1,0,0]')) AS distance,
		VEC_CENTROID(embedding, VEC_FROM_JSON('[1,0,0]')) AS centroid
		FROM docs WHERE doc_id = 'guide' AND chunk_index = 0`)
	if len(vectors.Rows) != 1 || vectors.Rows[0]["distance"] == nil || vectors.Rows[0]["centroid"] == nil {
		t.Fatalf("vector helper result = %#v", vectors)
	}

	rag := executeRecentFeatureSQL(t, db, `SELECT doc_id, chunk_index, _hit_rank, _context_rank
		FROM RAG_SEARCH('docs', 'embedding', VEC_FROM_JSON('[1.0,0.0,0.0]'), 2, '{
			"text_column": "chunk_text",
			"text_query": "vector search",
			"key_columns": ["doc_id", "chunk_index"],
			"expand_before": 1,
			"expand_after": 1,
			"doc_id_column": "doc_id",
			"chunk_index_column": "chunk_index"
		}')`)
	if len(rag.Rows) == 0 || rag.Rows[0]["_hit_rank"] == nil || rag.Rows[0]["_context_rank"] == nil {
		t.Fatalf("RAG_SEARCH result = %#v", rag)
	}

	executeRecentFeatureSQL(t, db, `ANALYZE docs`)
	statistics := executeRecentFeatureSQL(t, db, `SELECT column_name, distinct_count FROM sys.statistics WHERE table_name = 'docs'`)
	if len(statistics.Rows) == 0 {
		t.Fatal("ANALYZE did not expose statistics through sys.statistics")
	}
}
