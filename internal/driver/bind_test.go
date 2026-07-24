package driver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"testing"
)

func nv(v any) driver.NamedValue { return driver.NamedValue{Value: v} }

func TestBindPlaceholders_Sequential(t *testing.T) {
	q := "INSERT INTO t (a,b) VALUES (?,?)"
	out, err := bindPlaceholders(q, []driver.NamedValue{nv(1), nv("O'Hara")})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := "INSERT INTO t (a,b) VALUES (1,'O''Hara')"
	if out != want {
		t.Fatalf("got %q want %q", out, want)
	}
}

func TestBindPlaceholders_Numbered(t *testing.T) {
	q := "SELECT $2, $1"
	out, err := bindPlaceholders(q, []driver.NamedValue{nv(10), nv("x")})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := "SELECT 'x', 10"
	if out != want {
		t.Fatalf("got %q want %q", out, want)
	}
}

func TestBindPlaceholders_QuotedLiteral(t *testing.T) {
	q := "SELECT '?' as q, col FROM t WHERE name='it's'"
	out, err := bindPlaceholders(q, []driver.NamedValue{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out != q {
		t.Fatalf("quoted literal changed: got %q want %q", out, q)
	}
}

// TestBindPlaceholders_TableValuedFunctionArgumentPosition verifies that a
// bound `?` parameter works when substituted into a table-valued function's
// argument list in the FROM clause (VEC_SEARCH's table/column/query-vector/k
// arguments here) — not just a WHERE-clause literal. bindPlaceholders is a
// purely textual substitution before parsing (see bindPlaceholders/sqlLiteral
// above), so nothing about the substitution itself is WHERE-clause-specific,
// but that needs confirming end-to-end through the parser and engine rather
// than assumed.
func TestBindPlaceholders_TableValuedFunctionArgumentPosition(t *testing.T) {
	db, err := sql.Open("tinysql", "mem://?tenant=bind_tvf_test")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `CREATE TABLE bind_vecs (id INT, label TEXT, embedding VECTOR)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO bind_vecs VALUES (1, 'a', '[1.0, 0.0]')`); err != nil {
		t.Fatalf("insert a: %v", err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO bind_vecs VALUES (2, 'b', '[0.0, 1.0]')`); err != nil {
		t.Fatalf("insert b: %v", err)
	}

	// table name, column name, query-vector JSON, and k are all bound as `?`
	// placeholders inside VEC_SEARCH's table-valued-function argument list.
	rows, err := db.QueryContext(ctx,
		`SELECT id, label FROM VEC_SEARCH(?, ?, VEC_FROM_JSON(?), ?) ORDER BY _vec_rank`,
		"bind_vecs", "embedding", "[1.0, 0.0]", 1)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()

	var got []string
	for rows.Next() {
		var id int
		var label string
		if err := rows.Scan(&id, &label); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, label)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows err: %v", err)
	}
	if len(got) != 1 || got[0] != "a" {
		t.Fatalf("expected [a] (nearest to [1.0, 0.0]), got %v", got)
	}
}

// TestBindPlaceholders_RagSearchOptionsArgumentPosition extends the check
// above to RAG_SEARCH's 5th (options_json) argument: a JSON-object string
// bound as a `?` placeholder must survive substitution and subsequent
// parsing as a single string literal — not be split or mis-escaped by the
// SQL-literal quoting bindPlaceholders performs for the surrounding table/
// column/k arguments in the same call.
func TestBindPlaceholders_RagSearchOptionsArgumentPosition(t *testing.T) {
	db, err := sql.Open("tinysql", "mem://?tenant=bind_tvf_rag_test")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `CREATE TABLE bind_rag (doc_id TEXT, chunk_index INT, chunk_text TEXT, embedding VECTOR)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := db.ExecContext(ctx,
		`INSERT INTO bind_rag VALUES ('doc-1', 0, 'vector search basics', '[0.9, 0.1, 0.0]')`); err != nil {
		t.Fatalf("insert 1: %v", err)
	}
	if _, err := db.ExecContext(ctx,
		`INSERT INTO bind_rag VALUES ('doc-1', 1, 'full-text search ranking', '[0.1, 0.9, 0.0]')`); err != nil {
		t.Fatalf("insert 2: %v", err)
	}

	options := `{"text_column":"chunk_text","text_query":"full-text search","key_columns":["doc_id","chunk_index"]}`
	rows, err := db.QueryContext(ctx,
		`SELECT doc_id, chunk_index FROM RAG_SEARCH(?, ?, VEC_FROM_JSON(?), ?, ?) ORDER BY _rrf_rank`,
		"bind_rag", "embedding", "[0.9, 0.1, 0.0]", 2, options)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()

	n := 0
	for rows.Next() {
		var docID string
		var chunkIndex int
		if err := rows.Scan(&docID, &chunkIndex); err != nil {
			t.Fatalf("scan: %v", err)
		}
		n++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows err: %v", err)
	}
	if n != 2 {
		t.Fatalf("expected 2 fused rows, got %d", n)
	}
}

func TestSqlLiteral_Complex(t *testing.T) {
	if got := sqlLiteral(nil); got != "NULL" {
		t.Fatalf("nil literal: %s", got)
	}
	if got := sqlLiteral(true); got != "TRUE" {
		t.Fatalf("bool literal: %s", got)
	}
	if got := sqlLiteral(42); got != "42" {
		t.Fatalf("int literal: %s", got)
	}
	if got := sqlLiteral(3.14); got != "3.14" {
		t.Fatalf("float literal: %s", got)
	}
	if got := sqlLiteral("a'b"); got != "'a''b'" {
		t.Fatalf("string literal escaping: %s", got)
	}
}
