package engine

import (
	"context"
	"strconv"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// A trigger that searches the table it fires on runs inside the statement and
// caches a tokenized view of rows that the rollback then takes back. The restore
// keeps the table object and rewinds Version, so neither the pointer nor the
// version can tell the cache entry from a good one: the rollback has to drop it.
func TestRollbackDropsFTSCacheBuiltInsideFailedStatement(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE rb_docs (id INT, loaded_at INT, body TEXT)`)
	execSQL(t, db, `CREATE TABLE rb_audit (n INT)`)
	for i := 1; i <= 4; i++ {
		execSQL(t, db, `INSERT INTO rb_docs VALUES (`+strconv.Itoa(i)+`, 1, 'common word')`)
	}
	execSQL(t, db, `CREATE TRIGGER rb_search AFTER UPDATE ON rb_docs
		BEGIN INSERT INTO rb_audit SELECT COUNT(*) FROM FTS_SEARCH('rb_docs', 'phantom', 100, 'body'); END`)
	// Warm the persistent index first so the statement works on a known base.
	if rs := execSQL(t, db, `SELECT * FROM FTS_SEARCH('rb_docs', 'phantom', 10, 'body')`); len(rs.Rows) != 0 {
		t.Fatalf("setup: %d phantom rows", len(rs.Rows))
	}

	// Row 1 is rewritten and searched by the trigger; row 3 divides by zero.
	_, err := Execute(context.Background(), db, "default", mustParse(
		`UPDATE rb_docs SET body = 'phantom word', loaded_at = 1 / (id - 3)`))
	if err == nil {
		t.Fatal("the failing statement succeeded")
	}
	// The restore rewinds Version, so the next successful statement reuses the
	// version number the discarded cache entry was stamped with.
	execSQL(t, db, `UPDATE rb_docs SET body = 'other word' WHERE id = 2`)
	if rs := execSQL(t, db, `SELECT * FROM FTS_SEARCH('rb_docs', 'phantom', 10, 'body')`); len(rs.Rows) != 0 {
		t.Fatalf("a rolled-back rewrite is still searchable: %d rows", len(rs.Rows))
	}
	if rs := execSQL(t, db, `SELECT * FROM FTS_SEARCH('rb_docs', 'other', 10, 'body')`); len(rs.Rows) != 1 {
		t.Fatalf("the committed rewrite is not searchable: %d rows, want 1", len(rs.Rows))
	}
	if rs := execSQL(t, db, `SELECT * FROM FTS_SEARCH('rb_docs', 'common', 10, 'body')`); len(rs.Rows) != 3 {
		t.Fatalf("the restored rows are not searchable: %d rows, want 3", len(rs.Rows))
	}
}
