package engine

// Invalidation tests for the neighbor-chunk index cache (getRAGContextIndex in
// rag_context.go).
//
// A stale expansion index is a nasty failure mode: RAG_CONTEXT_FROM would return
// neighbors from a previous version of the corpus, or index rows that no longer
// exist, and the query would still look successful. These tests drive the cache
// through every mutation path that must invalidate it.

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// newRAGCacheDB builds a three-chunk single-document corpus: doc-1 chunks 0,1,2
// plus an unrelated document, so a neighbor window has something to exclude.
func newRAGCacheDB(t *testing.T) *storage.DB {
	t.Helper()
	db := storage.NewDB()
	ctx := context.Background()
	stmts := []string{
		`CREATE TABLE chunks (doc_id TEXT, chunk_index INT, chunk_text TEXT)`,
		`INSERT INTO chunks VALUES ('doc-1', 0, 'chunk zero')`,
		`INSERT INTO chunks VALUES ('doc-1', 1, 'chunk one')`,
		`INSERT INTO chunks VALUES ('doc-1', 2, 'chunk two')`,
		`INSERT INTO chunks VALUES ('doc-2', 0, 'other document')`,
	}
	for _, sql := range stmts {
		if _, err := Execute(ctx, db, "default", mustParse(sql)); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	return db
}

func ragCacheSetup(t *testing.T) (*storage.DB, context.Context) {
	t.Helper()
	return newRAGCacheDB(t), context.Background()
}

func mustExec(t *testing.T, db *storage.DB, ctx context.Context, sql string) *ResultSet {
	t.Helper()
	rs, err := Execute(ctx, db, "default", mustParse(sql))
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return rs
}

// contextChunkIndices returns the chunk_index values RAG_CONTEXT returns for one
// hit, in result order.
func contextChunkIndices(t *testing.T, db *storage.DB, ctx context.Context, docID string, center, before, after int) []int {
	t.Helper()
	rs := mustExec(t, db, ctx, fmt.Sprintf(
		`SELECT chunk_index FROM RAG_CONTEXT('chunks', 'doc_id', 'chunk_index', '%s', %d, %d, %d)`,
		docID, center, before, after))
	out := make([]int, 0, len(rs.Rows))
	for _, r := range rs.Rows {
		v, _ := ragValue(r, "chunk_index")
		n, err := toInt(v)
		if err != nil {
			t.Fatalf("chunk_index %v is not an int: %v", v, err)
		}
		out = append(out, n)
	}
	return out
}

func fmtInts(v []int) string {
	parts := make([]string, len(v))
	for i, n := range v {
		parts[i] = fmt.Sprint(n)
	}
	return "[" + strings.Join(parts, " ") + "]"
}

// TestRAGContextIndexInvalidatesOnInsert is the core invalidation case: a chunk
// inserted after the index was cached must appear as a neighbor.
func TestRAGContextIndexInvalidatesOnInsert(t *testing.T) {
	db, ctx := ragCacheSetup(t)

	// Populate the cache. doc-1 has chunks 0..2 so far.
	if got := contextChunkIndices(t, db, ctx, "doc-1", 1, 1, 1); fmtInts(got) != "[0 1 2]" {
		t.Fatalf("before insert: got %s, want [0 1 2]", fmtInts(got))
	}

	// Chunk 3 becomes the right-hand neighbor of chunk 2.
	mustExec(t, db, ctx, `INSERT INTO chunks VALUES ('doc-1', 3, 'inserted chunk three')`)

	if got := contextChunkIndices(t, db, ctx, "doc-1", 2, 1, 1); fmtInts(got) != "[1 2 3]" {
		t.Errorf("after insert: got %s, want [1 2 3] — the cached index is stale", fmtInts(got))
	}
}

// TestRAGContextIndexInvalidatesOnDelete covers the direction that can return
// rows which no longer exist.
func TestRAGContextIndexInvalidatesOnDelete(t *testing.T) {
	db, ctx := ragCacheSetup(t)

	if got := contextChunkIndices(t, db, ctx, "doc-1", 1, 1, 1); fmtInts(got) != "[0 1 2]" {
		t.Fatalf("before delete: got %s, want [0 1 2]", fmtInts(got))
	}

	mustExec(t, db, ctx, `DELETE FROM chunks WHERE doc_id = 'doc-1' AND chunk_index = 2`)

	if got := contextChunkIndices(t, db, ctx, "doc-1", 1, 1, 1); fmtInts(got) != "[0 1]" {
		t.Errorf("after delete: got %s, want [0 1] — a deleted chunk is still indexed", fmtInts(got))
	}
}

// TestRAGContextIndexInvalidatesOnUpdate covers a chunk moving position, which
// changes the neighbor window without changing the row count.
func TestRAGContextIndexInvalidatesOnUpdate(t *testing.T) {
	db, ctx := ragCacheSetup(t)

	if got := contextChunkIndices(t, db, ctx, "doc-1", 1, 1, 1); fmtInts(got) != "[0 1 2]" {
		t.Fatalf("before update: got %s, want [0 1 2]", fmtInts(got))
	}

	// Move chunk 2 far away; it must drop out of chunk 1's window.
	mustExec(t, db, ctx, `UPDATE chunks SET chunk_index = 99 WHERE doc_id = 'doc-1' AND chunk_index = 2`)

	if got := contextChunkIndices(t, db, ctx, "doc-1", 1, 1, 1); fmtInts(got) != "[0 1]" {
		t.Errorf("after update: got %s, want [0 1] — the moved chunk is still at its old position", fmtInts(got))
	}
	if got := contextChunkIndices(t, db, ctx, "doc-1", 99, 0, 0); fmtInts(got) != "[99]" {
		t.Errorf("moved chunk not findable at its new position: got %s, want [99]", fmtInts(got))
	}
}

// TestRAGContextIndexInvalidatesOnDropAndRecreate checks the purge hook. Without
// it, a dropped table's index stays reachable under the same name and can index
// row positions that do not exist in the replacement table.
func TestRAGContextIndexInvalidatesOnDropAndRecreate(t *testing.T) {
	db, ctx := ragCacheSetup(t)

	if got := contextChunkIndices(t, db, ctx, "doc-1", 1, 1, 1); fmtInts(got) != "[0 1 2]" {
		t.Fatalf("before drop: got %s, want [0 1 2]", fmtInts(got))
	}

	mustExec(t, db, ctx, `DROP TABLE chunks`)
	mustExec(t, db, ctx, `CREATE TABLE chunks (doc_id TEXT, chunk_index INT, chunk_text TEXT)`)
	mustExec(t, db, ctx, `INSERT INTO chunks VALUES ('doc-1', 7, 'only chunk')`)

	if got := contextChunkIndices(t, db, ctx, "doc-1", 7, 2, 2); fmtInts(got) != "[7]" {
		t.Errorf("after drop and recreate: got %s, want [7]", fmtInts(got))
	}
	if got := contextChunkIndices(t, db, ctx, "doc-1", 1, 1, 1); len(got) != 0 {
		t.Errorf("old chunks still reachable after drop: got %s, want none", fmtInts(got))
	}
}

// TestRAGContextIndexSeparatesColumnPairsAndTenants checks the cache key: two
// different column pairs, or the same table name in two tenants, must not share
// an entry.
func TestRAGContextIndexSeparatesColumnPairsAndTenants(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()
	// A second grouping column that spans every row, so grouping by it yields a
	// different index than grouping by doc_id.
	for _, sql := range []string{
		`CREATE TABLE chunks (doc_id TEXT, chunk_index INT, chunk_text TEXT, section TEXT)`,
		`INSERT INTO chunks VALUES ('doc-1', 0, 'chunk zero', 'sec-a')`,
		`INSERT INTO chunks VALUES ('doc-1', 1, 'chunk one', 'sec-a')`,
		`INSERT INTO chunks VALUES ('doc-1', 2, 'chunk two', 'sec-a')`,
		`INSERT INTO chunks VALUES ('doc-2', 3, 'other document', 'sec-a')`,
	} {
		mustExec(t, db, ctx, sql)
	}

	byDoc := mustExec(t, db, ctx,
		`SELECT chunk_index FROM RAG_CONTEXT('chunks', 'doc_id', 'chunk_index', 'doc-1', 1, 5, 5)`)
	bySection := mustExec(t, db, ctx,
		`SELECT chunk_index FROM RAG_CONTEXT('chunks', 'section', 'chunk_index', 'sec-a', 1, 5, 5)`)

	// doc-1 has 3 chunks; sec-a spans every row in the table.
	if len(byDoc.Rows) == len(bySection.Rows) {
		t.Errorf("grouping by doc_id and by section returned the same %d rows — "+
			"the cache key does not distinguish the column pair", len(byDoc.Rows))
	}

	// Same table name, different tenant, different contents.
	if _, err := Execute(ctx, db, "other", mustParse(
		`CREATE TABLE chunks (doc_id TEXT, chunk_index INT, chunk_text TEXT)`)); err != nil {
		t.Fatal(err)
	}
	if _, err := Execute(ctx, db, "other", mustParse(
		`INSERT INTO chunks VALUES ('doc-1', 42, 'other tenant')`)); err != nil {
		t.Fatal(err)
	}
	rs, err := Execute(ctx, db, "other", mustParse(
		`SELECT chunk_index FROM RAG_CONTEXT('chunks', 'doc_id', 'chunk_index', 'doc-1', 42, 1, 1)`))
	if err != nil {
		t.Fatal(err)
	}
	if len(rs.Rows) != 1 {
		t.Errorf("tenant 'other' returned %d rows, want 1 — tenants share a cache entry", len(rs.Rows))
	}
	v, _ := ragValue(rs.Rows[0], "chunk_index")
	if n, _ := toInt(v); n != 42 {
		t.Errorf("tenant 'other' returned chunk_index %v, want 42", v)
	}
}

// TestRAGContextDirectTableScanSemantics pins the raw-row fast path used by
// single-hit RAG_CONTEXT. It must retain the generic path's cross-numeric
// document equality, stable ordering for duplicate chunk positions, and
// tolerance of short/malformed rows.
func TestRAGContextDirectTableScanSemantics(t *testing.T) {
	source := ragSource{
		cols:        []string{"doc_id", "chunk_index"},
		rawRows:     [][]any{{7, 2}, {int64(7), 1}, {float64(7), 1}, {7, "bad"}, {8, 1}, {7}},
		columnIdx:   map[string]int{"doc_id": 0, "chunk_index": 1},
		tableSource: true,
	}

	matches := ragFindContextRows(source, "doc_id", "chunk_index", int64(7), 1, 0, 1)
	if len(matches) != 3 {
		t.Fatalf("matches = %#v, want 3 rows", matches)
	}
	// The two chunk_index=1 entries preserve their input order, as the previous
	// stable generic sorting path did; chunk 2 follows them.
	for i, want := range []struct {
		row, chunk int
	}{{1, 1}, {2, 1}, {0, 2}} {
		if matches[i].sourceRow != want.row || matches[i].chunkIndex != want.chunk {
			t.Errorf("match %d = row %d/chunk %d, want row %d/chunk %d", i, matches[i].sourceRow, matches[i].chunkIndex, want.row, want.chunk)
		}
	}

	if got := ragFindContextRows(source, "missing", "chunk_index", 7, 1, 1, 1); len(got) != 0 {
		t.Fatalf("missing doc column returned %#v, want no rows", got)
	}
}
