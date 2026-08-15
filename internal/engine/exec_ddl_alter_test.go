// Tests for ALTER TABLE ADD COLUMN.
//
// Nothing exercised the success path before these: the only existing mentions
// of ALTER TABLE in the suite assert that it is *rejected* (read-only mode in
// readonly_test.go, the MCP server's read-query guard). Meanwhile
// cmd/formigo/schema_tinysql.sql uses it three times to migrate a schema, so
// it is a live feature.
package engine

import (
	"context"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// TestAlterTableAddColumnSucceedsAndBackfillsNull is the base case: the
// statement must report success, and every row that existed before it must
// gain a NULL in the new column.
func TestAlterTableAddColumnSucceedsAndBackfillsNull(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE kv (id INT, name TEXT)`)
	execSQL(t, db, `INSERT INTO kv VALUES (1, 'one')`)
	execSQL(t, db, `INSERT INTO kv VALUES (2, 'two')`)

	if _, err := Execute(context.Background(), db, "default",
		mustParse(`ALTER TABLE kv ADD COLUMN extra TEXT`)); err != nil {
		t.Fatalf("ALTER TABLE ADD COLUMN failed: %v", err)
	}

	table, err := db.Get("default", "kv")
	if err != nil {
		t.Fatal(err)
	}
	if len(table.Cols) != 3 || table.Cols[2].Name != "extra" {
		t.Fatalf("columns after ALTER = %#v, want a third column named extra", table.Cols)
	}
	for i, row := range table.Rows {
		if len(row) != 3 {
			t.Fatalf("row %d has %d cells after ALTER, want 3", i, len(row))
		}
		if row[2] != nil {
			t.Fatalf("row %d backfilled with %#v, want NULL", i, row[2])
		}
	}
}

// TestAlterTableAddColumnIsUsableAfterwards is the point of the feature: the
// new column has to resolve by name. Table.ColIndex answers from an index
// built alongside Cols, so a schema change that appends to one without
// updating the other leaves a column that exists but cannot be addressed.
func TestAlterTableAddColumnIsUsableAfterwards(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE kv (id INT, name TEXT)`)
	execSQL(t, db, `INSERT INTO kv VALUES (1, 'one')`)
	execSQL(t, db, `ALTER TABLE kv ADD COLUMN extra TEXT`)

	table, err := db.Get("default", "kv")
	if err != nil {
		t.Fatal(err)
	}
	idx, err := table.ColIndex("extra")
	if err != nil {
		t.Fatalf("ColIndex on the added column: %v", err)
	}
	if idx != 2 {
		t.Fatalf("ColIndex(extra) = %d, want 2", idx)
	}
	// Case-insensitively too, like every other column.
	if idx, err := table.ColIndex("EXTRA"); err != nil || idx != 2 {
		t.Fatalf("ColIndex(EXTRA) = %d, %v; want 2, nil", idx, err)
	}

	// And through SQL: writing it by name, filtering on it, projecting it.
	execSQL(t, db, `UPDATE kv SET extra = 'filled' WHERE id = 1`)
	execSQL(t, db, `INSERT INTO kv (id, name, extra) VALUES (2, 'two', 'inserted')`)
	execSQL(t, db, `INSERT INTO kv VALUES (3, 'three', 'positional')`)

	rs := execSQL(t, db, `SELECT id, extra FROM kv ORDER BY id`)
	if len(rs.Rows) != 3 {
		t.Fatalf("rows after writes = %d, want 3", len(rs.Rows))
	}
	want := []string{"filled", "inserted", "positional"}
	for i, w := range want {
		if got := rs.Rows[i]["extra"]; got != w {
			t.Fatalf("row %d extra = %#v, want %q", i, got, w)
		}
	}

	rs = execSQL(t, db, `SELECT id FROM kv WHERE extra = 'inserted'`)
	if len(rs.Rows) != 1 || expectAsInt(t, rs.Rows[0]["id"]) != 2 {
		t.Fatalf("WHERE on the added column returned %+v, want just id 2", rs.Rows)
	}
}

// TestAlterTableAddDuplicateColumnLeavesTableUntouched covers the rejection
// path. The check runs before any mutation, so a rejected statement must not
// have widened the schema or the rows.
func TestAlterTableAddDuplicateColumnLeavesTableUntouched(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE kv (id INT, name TEXT)`)
	execSQL(t, db, `INSERT INTO kv VALUES (1, 'one')`)

	if _, err := Execute(context.Background(), db, "default",
		mustParse(`ALTER TABLE kv ADD COLUMN name TEXT`)); err == nil {
		t.Fatal("adding an existing column was accepted")
	}

	table, err := db.Get("default", "kv")
	if err != nil {
		t.Fatal(err)
	}
	if len(table.Cols) != 2 {
		t.Fatalf("columns after a rejected ALTER = %d, want 2", len(table.Cols))
	}
	if len(table.Rows[0]) != 2 {
		t.Fatalf("row width after a rejected ALTER = %d, want 2", len(table.Rows[0]))
	}
}

// TestAlterTableAddColumnUnknownTable keeps the not-found error reaching the
// caller unchanged.
func TestAlterTableAddColumnUnknownTable(t *testing.T) {
	db := storage.NewDB()
	if _, err := Execute(context.Background(), db, "default",
		mustParse(`ALTER TABLE nope ADD COLUMN x INT`)); err == nil {
		t.Fatal("ALTER TABLE on a missing table was accepted")
	}
}

// TestAlterTableAddColumnKeepsSecondaryIndexes confirms the widened rows are
// still consistent with the indexes built over the columns that existed
// before — index keys address columns positionally, and appending never moves
// an existing one.
func TestAlterTableAddColumnKeepsSecondaryIndexes(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE kv (id INT, bucket INT)`)
	execSQL(t, db, `INSERT INTO kv VALUES (1, 10)`)
	execSQL(t, db, `INSERT INTO kv VALUES (2, 20)`)
	execSQL(t, db, `CREATE INDEX idx_kv_bucket ON kv(bucket)`)
	execSQL(t, db, `ALTER TABLE kv ADD COLUMN extra TEXT`)

	rs := execSQL(t, db, `SELECT id FROM kv WHERE bucket = 20`)
	if len(rs.Rows) != 1 || expectAsInt(t, rs.Rows[0]["id"]) != 2 {
		t.Fatalf("indexed lookup after ALTER returned %+v, want just id 2", rs.Rows)
	}
	execSQL(t, db, `INSERT INTO kv VALUES (3, 30, 'x')`)
	rs = execSQL(t, db, `SELECT id FROM kv WHERE bucket = 30`)
	if len(rs.Rows) != 1 || expectAsInt(t, rs.Rows[0]["id"]) != 3 {
		t.Fatalf("indexed lookup for a post-ALTER row returned %+v, want just id 3", rs.Rows)
	}
}
