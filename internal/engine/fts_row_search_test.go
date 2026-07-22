// Tests for whole-row full-text search: FTS_SEARCH's default-to-all-columns
// behavior, its document cache, and the ROW_TO_TEXT() ad-hoc search helper.
package engine

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestFTSSearchDefaultSearchesWholeRow(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()

	Execute(ctx, db, "default", mustParse(`CREATE TABLE orders (id INT, sku TEXT, note TEXT)`))
	Execute(ctx, db, "default", mustParse(`INSERT INTO orders VALUES (42, 'WIDGET-1', 'urgent customer request')`))
	Execute(ctx, db, "default", mustParse(`INSERT INTO orders VALUES (7, 'GADGET-2', 'standard delivery')`))

	// The numeric id column (42) is not TEXT/STRING, so under the old
	// "text columns only" default it was never searchable. It must be
	// findable now that the default is the whole row.
	rs := execSQL(t, db, `SELECT * FROM FTS_SEARCH('orders', '42', 5)`)
	if len(rs.Rows) != 1 {
		t.Fatalf("expected 1 row matching numeric id 42, got %d", len(rs.Rows))
	}
	if rs.Rows[0]["sku"] != "WIDGET-1" {
		t.Errorf("expected the WIDGET-1 row, got %v", rs.Rows[0]["sku"])
	}

	// A term only present in a TEXT column still works.
	rs = execSQL(t, db, `SELECT * FROM FTS_SEARCH('orders', 'urgent', 5)`)
	if len(rs.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d rows: %+v", len(rs.Rows), rs.Rows)
	}
	expectInt(t, rs.Rows[0]["id"], 42, "id")
}

func TestFTSSearchEmptyQueryMatchesNothing(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()
	Execute(ctx, db, "default", mustParse(`CREATE TABLE docs (id INT, body TEXT)`))
	Execute(ctx, db, "default", mustParse(`INSERT INTO docs VALUES (1, 'hello world')`))
	Execute(ctx, db, "default", mustParse(`INSERT INTO docs VALUES (2, 'the quick brown fox')`))

	// An empty or whitespace-only query carries no searchable terms. It must
	// return zero rows, not k arbitrary rows scored 0 (which would silently
	// poison a RAG context window).
	for _, q := range []string{``, `   `} {
		rs := execSQL(t, db, `SELECT * FROM FTS_SEARCH('docs', '`+q+`', 5)`)
		if len(rs.Rows) != 0 {
			t.Fatalf("query %q: expected 0 rows, got %d", q, len(rs.Rows))
		}
	}
}

// TestFTSSearchIDFWeightsRareTermsHigher guards BM25 IDF weighting in
// FTS_SEARCH. "product" appears in every doc (common, low IDF); "widget"
// appears in only one (rare, high IDF). Without IDF, doc 1's extra token
// makes it longer than average, and the BM25 length penalty alone would
// rank it *below* the shorter product-only docs — so this only passes if
// the rare term's IDF is actually outweighing the length penalty.
func TestFTSSearchIDFWeightsRareTermsHigher(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()
	Execute(ctx, db, "default", mustParse(`CREATE TABLE docs (id INT, body TEXT)`))
	Execute(ctx, db, "default", mustParse(`INSERT INTO docs VALUES (1, 'widget product alpha')`))
	Execute(ctx, db, "default", mustParse(`INSERT INTO docs VALUES (2, 'product beta')`))
	Execute(ctx, db, "default", mustParse(`INSERT INTO docs VALUES (3, 'product gamma')`))
	Execute(ctx, db, "default", mustParse(`INSERT INTO docs VALUES (4, 'product delta')`))
	Execute(ctx, db, "default", mustParse(`INSERT INTO docs VALUES (5, 'product epsilon')`))

	rs := execSQL(t, db, `SELECT id FROM FTS_SEARCH('docs', 'widget OR product', 5, 'body')`)
	if len(rs.Rows) != 5 {
		t.Fatalf("expected all 5 docs to match, got %d", len(rs.Rows))
	}
	expectInt(t, rs.Rows[0]["id"], 1, "doc containing the rare term 'widget' should rank first")
}

func TestFTSSearchExplicitColumnsStillWork(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()
	Execute(ctx, db, "default", mustParse(`CREATE TABLE t (id INT, a TEXT, b TEXT)`))
	Execute(ctx, db, "default", mustParse(`INSERT INTO t VALUES (1, 'apple', 'banana')`))
	Execute(ctx, db, "default", mustParse(`INSERT INTO t VALUES (2, 'banana', 'apple')`))

	// Restricting to column 'a' only should exclude matches only present in 'b'.
	rs := execSQL(t, db, `SELECT id FROM FTS_SEARCH('t', 'apple', 5, 'a')`)
	if len(rs.Rows) != 1 {
		t.Fatalf("expected only row 1 (a='apple'), got %+v", rs.Rows)
	}
	expectInt(t, rs.Rows[0]["id"], 1, "id")
}

func TestFTSSearchCacheInvalidatesOnMutation(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()
	Execute(ctx, db, "default", mustParse(`CREATE TABLE docs (id INT, body TEXT)`))
	Execute(ctx, db, "default", mustParse(`INSERT INTO docs VALUES (1, 'hello world')`))

	rs := execSQL(t, db, `SELECT * FROM FTS_SEARCH('docs', 'zebra', 5)`)
	if len(rs.Rows) != 0 {
		t.Fatalf("expected 0 matches for 'zebra', got %d", len(rs.Rows))
	}

	// Insert a row containing the term; the doc cache (keyed by table
	// version) must be rebuilt, not served stale from before the insert.
	Execute(ctx, db, "default", mustParse(`INSERT INTO docs VALUES (2, 'a zebra crossing')`))
	rs = execSQL(t, db, `SELECT * FROM FTS_SEARCH('docs', 'zebra', 5)`)
	if len(rs.Rows) != 1 {
		t.Fatalf("expected 1 match for 'zebra' after insert, got %d", len(rs.Rows))
	}

	// Update the matching row so it no longer contains the term.
	Execute(ctx, db, "default", mustParse(`UPDATE docs SET body = 'no animals here' WHERE id = 2`))
	rs = execSQL(t, db, `SELECT * FROM FTS_SEARCH('docs', 'zebra', 5)`)
	if len(rs.Rows) != 0 {
		t.Fatalf("expected 0 matches for 'zebra' after update, got %d", len(rs.Rows))
	}
}

func TestRowToTextEnablesWholeRowLike(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()
	Execute(ctx, db, "default", mustParse(`CREATE TABLE orders (id INT, customer TEXT, status TEXT)`))
	Execute(ctx, db, "default", mustParse(`INSERT INTO orders VALUES (1, 'Acme Corp', 'open')`))
	Execute(ctx, db, "default", mustParse(`INSERT INTO orders VALUES (2, 'Globex', 'closed')`))

	// Substring search across the whole row, combined with a normal
	// column-scoped predicate in the same WHERE clause. LIKE is
	// case-sensitive by default, so match the stored casing.
	rs := execSQL(t, db, `SELECT id FROM orders WHERE ROW_TO_TEXT() LIKE '%Acme%' AND status = 'open'`)
	if len(rs.Rows) != 1 {
		t.Fatalf("expected only row 1, got %+v", rs.Rows)
	}
	expectInt(t, rs.Rows[0]["id"], 1, "id")

	// A numeric id is included in the searchable text.
	rs = execSQL(t, db, `SELECT id FROM orders WHERE ROW_TO_TEXT() LIKE '%2%'`)
	if len(rs.Rows) != 1 {
		t.Fatalf("expected only row 2, got %+v", rs.Rows)
	}
	expectInt(t, rs.Rows[0]["id"], 2, "id")

	// Case-insensitive keyword usage: row_to_text() in lowercase must also work.
	rs = execSQL(t, db, `SELECT id FROM orders WHERE row_to_text() LIKE '%Globex%'`)
	if len(rs.Rows) != 1 {
		t.Fatalf("lowercase row_to_text(): expected only row 2, got %+v", rs.Rows)
	}
	expectInt(t, rs.Rows[0]["id"], 2, "id")

	if _, err := Execute(ctx, db, "default", mustParse(`SELECT ROW_TO_TEXT(id) FROM orders`)); err == nil {
		t.Fatal("expected error for ROW_TO_TEXT with arguments")
	}
}

func TestRowToTextUsesRawFastPathCorrectly(t *testing.T) {
	// A single-table whole-row predicate can now use the raw path. It must
	// preserve ROW_TO_TEXT's output rather than evaluating against the empty
	// placeholder row used by ordinary raw function calls.
	db := storage.NewDB()
	ctx := context.Background()
	Execute(ctx, db, "default", mustParse(`CREATE TABLE t (id INT, note TEXT)`))
	for i := 0; i < 50; i++ {
		s := strconv.Itoa(i)
		Execute(ctx, db, "default", mustParse(`INSERT INTO t VALUES (`+s+`, 'note-`+s+`')`))
	}
	stmt := mustParse(`SELECT id FROM t WHERE ROW_TO_TEXT() LIKE '%note-7%'`).(*Select)
	plan, ok, err := buildSimpleSelectPlan(ExecEnv{ctx: ctx, tenant: "default", db: db}, stmt)
	if err != nil || !ok {
		t.Fatalf("ROW_TO_TEXT raw plan = %#v, ok=%v, err=%v", plan, ok, err)
	}
	rs, err := Execute(ctx, db, "default", stmt)
	if err != nil {
		t.Fatal(err)
	}
	if len(rs.Rows) == 0 {
		t.Fatal("ROW_TO_TEXT() returned no matches from the raw fast path")
	}

	// ROW_TO_TEXT's observable order is sorted by unqualified column name,
	// rather than storage position. Verify the raw implementation keeps that
	// behavior both in a WHERE predicate and as a projected value.
	execSQL(t, db, `CREATE TABLE row_text_order (zeta TEXT, alpha TEXT)`)
	execSQL(t, db, `INSERT INTO row_text_order VALUES ('z', 'a')`)
	rs = execSQL(t, db, `SELECT ROW_TO_TEXT() AS text FROM row_text_order WHERE ROW_TO_TEXT() LIKE 'a z'`)
	if len(rs.Rows) != 1 || rs.Rows[0]["text"] != "a z" {
		t.Fatalf("raw ROW_TO_TEXT order = %#v, want a z", rs.Rows)
	}
}

// TestFTSSearchTopKOrderWithManyMatches guards the bounded top-k heap
// selection in FTSSearchTableFunc.Execute (replacing collect-all-then-sort):
// with far more matching docs than k requested, the returned rows must still
// be exactly the k highest-scoring ones, in strictly descending score order.
//
// Every doc has the same total token count (n), so BM25's length
// normalization (normDocLen) is identical across all of them and the
// term-frequency component alone determines the ranking: doc i repeats
// "common" i times, padded with a filler word to keep doc length constant,
// so its score strictly increases with i and every doc's score is distinct.
func TestFTSSearchTopKOrderWithManyMatches(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()
	Execute(ctx, db, "default", mustParse(`CREATE TABLE docs (id INT, body TEXT)`))

	const n = 30
	for i := 1; i <= n; i++ {
		body := strings.TrimSpace(strings.Repeat("common ", i) + strings.Repeat("filler ", n-i))
		Execute(ctx, db, "default", mustParse(fmt.Sprintf(`INSERT INTO docs VALUES (%d, '%s')`, i, body)))
	}

	const k = 5
	rs := execSQL(t, db, fmt.Sprintf(`SELECT id, _fts_score FROM FTS_SEARCH('docs', 'common', %d, 'body')`, k))
	if len(rs.Rows) != k {
		t.Fatalf("expected %d rows, got %d: %+v", k, len(rs.Rows), rs.Rows)
	}

	wantIDs := []int{n, n - 1, n - 2, n - 3, n - 4}
	lastScore := math.MaxFloat64
	for i, row := range rs.Rows {
		expectInt(t, row["id"], wantIDs[i], fmt.Sprintf("rank %d id", i+1))
		score, ok := row["_fts_score"].(float64)
		if !ok {
			t.Fatalf("rank %d: _fts_score not a float64: %T", i+1, row["_fts_score"])
		}
		if score > lastScore {
			t.Fatalf("rank %d score %v exceeds previous rank's score %v; results not sorted descending", i+1, score, lastScore)
		}
		lastScore = score
	}
}
