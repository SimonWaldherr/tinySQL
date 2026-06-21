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
