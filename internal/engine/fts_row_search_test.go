// Tests for whole-row full-text search: FTS_SEARCH's default-to-all-columns
// behavior, its document cache, and the ROW_TO_TEXT() ad-hoc search helper.
package engine

import (
	"context"
	"strconv"
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

func TestRowToTextBypassesRawFastPathCorrectly(t *testing.T) {
	// A single-table, no-join query is exactly the shape that would be
	// eligible for the raw/fast execution path. If ROW_TO_TEXT() were not
	// excluded from it, the raw path would evaluate it against an empty
	// substituted row and every comparison would silently return false.
	db := storage.NewDB()
	ctx := context.Background()
	Execute(ctx, db, "default", mustParse(`CREATE TABLE t (id INT, note TEXT)`))
	for i := 0; i < 50; i++ {
		s := strconv.Itoa(i)
		Execute(ctx, db, "default", mustParse(`INSERT INTO t VALUES (`+s+`, 'note-`+s+`')`))
	}
	rs := execSQL(t, db, `SELECT id FROM t WHERE ROW_TO_TEXT() LIKE '%note-7%'`)
	if len(rs.Rows) == 0 {
		t.Fatal("ROW_TO_TEXT() returned no matches — likely evaluated against an empty row in the raw fast path")
	}
}
