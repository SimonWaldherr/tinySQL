package engine

import (
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestRAGContextDiscardsRolledBackNeighbors(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	for _, sql := range []string{
		`CREATE TABLE rollback_context (id INT PRIMARY KEY, doc TEXT, chunk INT)`,
		`INSERT INTO rollback_context VALUES (1,'a',0)`,
		`CREATE TABLE rollback_context_hits (doc TEXT, chunk INT)`,
		`INSERT INTO rollback_context_hits VALUES ('a',0)`,
		`CREATE TABLE rollback_context_audit (id INT PRIMARY KEY)`,
		`INSERT INTO rollback_context_audit VALUES (1)`,
		`CREATE TRIGGER context_before_failure AFTER INSERT ON rollback_context BEGIN
			SELECT * FROM RAG_CONTEXT_FROM('rollback_context','doc','chunk','rollback_context_hits','doc','chunk',0,1);
			INSERT INTO rollback_context_audit VALUES (1);
		END`,
	} {
		execSQL(t, db, sql)
	}
	_, err := Execute(t.Context(), db, "default", mustParse(`INSERT INTO rollback_context VALUES (2,'a',1)`))
	if err == nil || !strings.Contains(err.Error(), "PRIMARY KEY") {
		t.Fatalf("expected rejected trigger write: %v", err)
	}
	// Reuse the rolled-back version and physical row position with a chunk
	// outside the window. A cached neighbor from the failed statement would
	// incorrectly expose this row as chunk 1.
	execSQL(t, db, `DROP TRIGGER context_before_failure`)
	execSQL(t, db, `INSERT INTO rollback_context VALUES (3,'a',2)`)
	result := execSQL(t, db, `SELECT id FROM RAG_CONTEXT_FROM('rollback_context','doc','chunk','rollback_context_hits','doc','chunk',0,1)`)
	if len(result.Rows) != 1 || result.Rows[0]["id"] != 1 {
		t.Fatalf("context retained a rolled-back neighbor: %v", result.Rows)
	}
}
