package engine

import (
	"context"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestFTSWildcardsSingleAndMultiCharacter(t *testing.T) {
	db := storage.NewDB()

	tests := []struct {
		query string
		want  bool
	}{
		{`SELECT FTS_MATCH('colour palette', 'colo?r') AS matched`, true},
		{`SELECT FTS_MATCH('color palette', 'colo?r') AS matched`, false},
		{`SELECT FTS_MATCH('colour palette', 'colo_r') AS matched`, true},
		{`SELECT FTS_MATCH('database systems', 'data*') AS matched`, true},
		{`SELECT FTS_MATCH('database systems', 'data%') AS matched`, true},
		{`SELECT FTS_MATCH('programming guide', 'programming%') AS matched`, true},
		{`SELECT FTS_MATCH('database systems', 'd?t?base') AS matched`, true},
	}
	for _, tt := range tests {
		rs := execSQL(t, db, tt.query)
		if len(rs.Rows) != 1 || rs.Rows[0]["matched"] != tt.want {
			t.Fatalf("%s: matched=%v, want %v", tt.query, rs.Rows, tt.want)
		}
	}

	rs := execSQL(t, db, `SELECT FTS_RANK('colour palette', 'colo?r') AS score`)
	score, ok := rs.Rows[0]["score"].(float64)
	if !ok || score <= 0 {
		t.Fatalf("wildcard FTS_RANK score=%v, want > 0", rs.Rows[0]["score"])
	}
}

func TestFTSSearchWildcardRanking(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()
	execSQL(t, db, `CREATE TABLE wildcard_docs (id INT PRIMARY KEY, body TEXT)`)
	execSQL(t, db, `
		INSERT INTO wildcard_docs VALUES
			(1, 'database database internals'),
			(2, 'databese typo'),
			(3, 'unrelated material')
	`)

	rs, err := Execute(ctx, db, "default", mustParse(`
		SELECT id, _fts_rank, _fts_score
		FROM FTS_SEARCH('wildcard_docs', 'dat?base*', 5, 'body')
	`))
	if err != nil {
		t.Fatalf("FTS_SEARCH wildcard: %v", err)
	}
	if len(rs.Rows) != 1 || rs.Rows[0]["id"] != 1 {
		t.Fatalf("FTS_SEARCH wildcard rows=%#v, want only id=1", rs.Rows)
	}
	if rs.Rows[0]["_fts_rank"] != 1 {
		t.Fatalf("FTS_SEARCH wildcard rank=%v, want 1", rs.Rows[0]["_fts_rank"])
	}
}

func TestHybridSearchFusesSemanticAndWildcardTextResults(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `
		CREATE TABLE search_docs (
			id INT PRIMARY KEY,
			content TEXT,
			embedding VECTOR
		)
	`)
	execSQL(t, db, `
		INSERT INTO search_docs VALUES
			(1, 'astronomy guide',   '[1.0, 0.0]'),
			(2, 'database handbook', '[0.0, 1.0]'),
			(3, 'cooking recipes',   '[0.2, 0.8]')
	`)

	rs := execSQL(t, db, `
		SELECT id, _vec_rank, _fts_rank, _rrf_rank, _rrf_score
		FROM HYBRID_SEARCH(
			'search_docs', 'embedding', 'content', 'dat?base*',
			VEC_FROM_JSON('[1.0, 0.0]'), 2
		)
	`)
	if len(rs.Rows) != 2 {
		t.Fatalf("HYBRID_SEARCH returned %d rows, want 2", len(rs.Rows))
	}
	if rs.Rows[0]["id"] != 2 {
		t.Fatalf("HYBRID_SEARCH top id=%v, want wildcard text hit id=2; rows=%#v", rs.Rows[0]["id"], rs.Rows)
	}
	for _, column := range []string{"_vec_rank", "_fts_rank", "_rrf_rank", "_rrf_score"} {
		if _, ok := rs.Rows[0][column]; !ok {
			t.Errorf("HYBRID_SEARCH top row missing %s: %#v", column, rs.Rows[0])
		}
	}

	alias := execSQL(t, db, `
		SELECT id
		FROM VEC_HYBRID_SEARCH(
			'search_docs', 'embedding', 'content', 'data%',
			VEC_FROM_JSON('[1.0, 0.0]'), 1
		)
	`)
	if len(alias.Rows) != 1 || alias.Rows[0]["id"] != 2 {
		t.Fatalf("VEC_HYBRID_SEARCH alias rows=%#v, want id=2", alias.Rows)
	}
}

func TestHybridSearchExplicitKeyColumnsWithoutPrimaryKey(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE no_pk_docs (slug TEXT, content TEXT, embedding VECTOR)`)
	execSQL(t, db, `
		INSERT INTO no_pk_docs VALUES
			('a', 'vector databases', '[1.0, 0.0]'),
			('b', 'other material',   '[0.0, 1.0]')
	`)

	if _, err := Execute(context.Background(), db, "default", mustParse(`
		SELECT * FROM HYBRID_SEARCH(
			'no_pk_docs', 'embedding', 'content', 'vect?r',
			VEC_FROM_JSON('[1.0, 0.0]'), 1
		)
	`)); err == nil {
		t.Fatal("HYBRID_SEARCH without primary key/key_columns unexpectedly succeeded")
	}

	rs := execSQL(t, db, `
		SELECT slug FROM HYBRID_SEARCH(
			'no_pk_docs', 'embedding', 'content', 'vect?r',
			VEC_FROM_JSON('[1.0, 0.0]'), 1,
			'{"key_columns":["slug"]}'
		)
	`)
	if len(rs.Rows) != 1 || rs.Rows[0]["slug"] != "a" {
		t.Fatalf("HYBRID_SEARCH explicit key columns rows=%#v, want slug=a", rs.Rows)
	}
}
