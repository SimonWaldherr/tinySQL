package engine

import (
	"context"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestColumnConstraintsRejectDuplicatePrimaryAndUniqueValues(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()

	execConstraintSQL(t, ctx, db, "CREATE TABLE accounts (id INT PRIMARY KEY, email TEXT UNIQUE)")
	execConstraintSQL(t, ctx, db, "INSERT INTO accounts VALUES (1, 'a@example.test'), (2, 'b@example.test')")

	expectConstraintErr(t, ctx, db, "INSERT INTO accounts VALUES (1, 'c@example.test')", "PRIMARY KEY")
	expectConstraintErr(t, ctx, db, "INSERT INTO accounts VALUES (3, 'a@example.test')", "UNIQUE")
	expectConstraintErr(t, ctx, db, "INSERT INTO accounts VALUES (NULL, 'null-id@example.test')", "PRIMARY KEY")
}

func TestColumnConstraintsValidateSimpleUpdateWithoutMutatingOnFailure(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()

	execConstraintSQL(t, ctx, db, "CREATE TABLE accounts (id INT PRIMARY KEY, email TEXT UNIQUE)")
	execConstraintSQL(t, ctx, db, "INSERT INTO accounts VALUES (1, 'a@example.test'), (2, 'b@example.test')")

	expectConstraintErr(t, ctx, db, "UPDATE accounts SET email = 'a@example.test' WHERE id = 2", "UNIQUE")

	rs := queryConstraintSQL(t, ctx, db, "SELECT email FROM accounts WHERE id = 2")
	if len(rs.Rows) != 1 || rs.Rows[0]["email"] != "b@example.test" {
		t.Fatalf("failed update mutated row: %#v", rs.Rows)
	}
}

func TestForeignKeyConstraintValidatesInsertAndUpdate(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()

	execConstraintSQL(t, ctx, db, "CREATE TABLE parents (id INT PRIMARY KEY)")
	execConstraintSQL(t, ctx, db, "INSERT INTO parents VALUES (1)")
	execConstraintSQL(t, ctx, db, "CREATE TABLE children (id INT PRIMARY KEY, parent_id INT FOREIGN KEY REFERENCES parents(id))")
	execConstraintSQL(t, ctx, db, "INSERT INTO children VALUES (10, 1)")

	expectConstraintErr(t, ctx, db, "INSERT INTO children VALUES (11, 99)", "FOREIGN KEY")
	expectConstraintErr(t, ctx, db, "UPDATE children SET parent_id = 99 WHERE id = 10", "FOREIGN KEY")

	rs := queryConstraintSQL(t, ctx, db, "SELECT parent_id FROM children WHERE id = 10")
	if len(rs.Rows) != 1 || rs.Rows[0]["parent_id"] != 1 {
		t.Fatalf("failed foreign-key update mutated row: %#v", rs.Rows)
	}
}

// TestConstraintIndexCatchesIntraBatchDuplicate guards both the incremental
// constraint index and statement-level atomicity: a duplicate appearing later
// in one multi-row INSERT must be caught, and none of the earlier rows may be
// visible after the statement fails.
func TestConstraintIndexCatchesIntraBatchDuplicate(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()
	execConstraintSQL(t, ctx, db, "CREATE TABLE t (id INT PRIMARY KEY)")
	expectConstraintErr(t, ctx, db, "INSERT INTO t VALUES (1), (2), (1)", "PRIMARY KEY")

	// The index sees earlier rows in the same statement, while Execute's DML
	// snapshot makes the complete batch disappear on the resulting error.
	rs := queryConstraintSQL(t, ctx, db, "SELECT COUNT(*) as n FROM t")
	expectInt(t, rs.Rows[0]["n"], 0, "row count after rejected batch")
}

// TestConstraintIndexUpdateThenReinsert guards patchConstraintIndexRow:
// after UPDATE moves a UNIQUE value off of one row, that value must be
// immediately reusable (the cached index shouldn't still think the old row
// holds it), and re-using the row's original value from a different row
// must still be rejected (the cached index shouldn't have "forgotten" the
// update happened).
func TestConstraintIndexUpdateThenReinsert(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()
	execConstraintSQL(t, ctx, db, "CREATE TABLE accounts (id INT PRIMARY KEY, email TEXT UNIQUE)")
	execConstraintSQL(t, ctx, db, "INSERT INTO accounts VALUES (1, 'a@example.test'), (2, 'b@example.test')")

	// Free up 'a@example.test' by changing row 1's email.
	execConstraintSQL(t, ctx, db, "UPDATE accounts SET email = 'a2@example.test' WHERE id = 1")

	// Now a fresh row can take the freed-up value.
	execConstraintSQL(t, ctx, db, "INSERT INTO accounts VALUES (3, 'a@example.test')")

	// And the value UPDATE just moved row 1 to must now be seen as taken.
	expectConstraintErr(t, ctx, db, "INSERT INTO accounts VALUES (4, 'a2@example.test')", "UNIQUE")

	rs := queryConstraintSQL(t, ctx, db, "SELECT id FROM accounts WHERE email = 'a@example.test'")
	expectInt(t, rs.Rows[0]["id"], 3, "email a@example.test now belongs to id 3")
}

// TestConstraintIndexDeleteFreesValueForReuse guards
// invalidateConstraintIndexes: after DELETE removes a row, its PRIMARY KEY
// value must be immediately reusable by a new INSERT.
func TestConstraintIndexDeleteFreesValueForReuse(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()
	execConstraintSQL(t, ctx, db, "CREATE TABLE t (id INT PRIMARY KEY)")
	execConstraintSQL(t, ctx, db, "INSERT INTO t VALUES (1), (2), (3)")
	execConstraintSQL(t, ctx, db, "DELETE FROM t WHERE id = 2")
	execConstraintSQL(t, ctx, db, "INSERT INTO t VALUES (2)")

	rs := queryConstraintSQL(t, ctx, db, "SELECT id FROM t ORDER BY id")
	if len(rs.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d: %+v", len(rs.Rows), rs.Rows)
	}

	// The freed value must still correctly detect a genuine duplicate.
	expectConstraintErr(t, ctx, db, "INSERT INTO t VALUES (2)", "PRIMARY KEY")
}

// TestConstraintIndexSurvivesDropAndRecreate guards the DROP TABLE cleanup:
// a table dropped and recreated under the same name must not see stale
// constraint state from the old table object leak through.
func TestConstraintIndexSurvivesDropAndRecreate(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()
	execConstraintSQL(t, ctx, db, "CREATE TABLE t (id INT PRIMARY KEY)")
	execConstraintSQL(t, ctx, db, "INSERT INTO t VALUES (1)")
	execConstraintSQL(t, ctx, db, "DROP TABLE t")

	execConstraintSQL(t, ctx, db, "CREATE TABLE t (id INT PRIMARY KEY)")
	// Must succeed: this is a fresh table, id=1 was never inserted into it.
	execConstraintSQL(t, ctx, db, "INSERT INTO t VALUES (1)")
	rs := queryConstraintSQL(t, ctx, db, "SELECT COUNT(*) as n FROM t")
	expectInt(t, rs.Rows[0]["n"], 1, "row count in recreated table")
}

func execConstraintSQL(t *testing.T, ctx context.Context, db *storage.DB, sql string) {
	t.Helper()
	if _, err := Execute(ctx, db, "default", mustParse(sql)); err != nil {
		t.Fatalf("SQL failed: %s\n  error: %v", sql, err)
	}
}

func queryConstraintSQL(t *testing.T, ctx context.Context, db *storage.DB, sql string) *ResultSet {
	t.Helper()
	rs, err := Execute(ctx, db, "default", mustParse(sql))
	if err != nil {
		t.Fatalf("SQL failed: %s\n  error: %v", sql, err)
	}
	return rs
}

func expectConstraintErr(t *testing.T, ctx context.Context, db *storage.DB, sql, want string) {
	t.Helper()
	_, err := Execute(ctx, db, "default", mustParse(sql))
	if err == nil {
		t.Fatalf("SQL succeeded, expected %s error: %s", want, sql)
	}
	if want != "" && !strings.Contains(strings.ToUpper(err.Error()), strings.ToUpper(want)) {
		t.Fatalf("SQL error = %q, want substring %q", err.Error(), want)
	}
}
