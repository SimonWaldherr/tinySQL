package engine

import (
	"context"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestInsertSelectAndOnConflictDoNothing(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE source_rows (id INT, name TEXT)`)
	execSQL(t, db, `INSERT INTO source_rows VALUES (1, 'one'), (2, 'two'), (3, 'three')`)
	execSQL(t, db, `CREATE TABLE target_rows (id INT PRIMARY KEY, name TEXT NOT NULL)`)
	execSQL(t, db, `INSERT INTO target_rows VALUES (1, 'existing')`)

	rs := execSQL(t, db, `INSERT INTO target_rows (id, name)
		SELECT id, name FROM source_rows
		ON CONFLICT DO NOTHING
		RETURNING id`)
	if len(rs.Rows) != 2 || expectAsInt(t, rs.Rows[0]["id"]) != 2 || expectAsInt(t, rs.Rows[1]["id"]) != 3 {
		t.Fatalf("INSERT SELECT RETURNING = %#v, want ids 2 and 3", rs.Rows)
	}

	rs = execSQL(t, db, `INSERT INTO target_rows VALUES (3, 'ignored'), (4, 'four') ON CONFLICT DO NOTHING RETURNING id`)
	if len(rs.Rows) != 1 || expectAsInt(t, rs.Rows[0]["id"]) != 4 {
		t.Fatalf("VALUES ON CONFLICT RETURNING = %#v, want id 4", rs.Rows)
	}

	rs = execSQL(t, db, `SELECT id, name FROM target_rows ORDER BY id`)
	if len(rs.Rows) != 4 || rs.Rows[0]["name"] != "existing" || rs.Rows[2]["name"] != "three" || rs.Rows[3]["name"] != "four" {
		t.Fatalf("target rows = %#v", rs.Rows)
	}

	_, err := Execute(context.Background(), db, "default", mustParse(`INSERT INTO target_rows VALUES (1, NULL) ON CONFLICT DO NOTHING`))
	if err == nil || !strings.Contains(err.Error(), "NOT NULL") {
		t.Fatalf("ON CONFLICT suppressed non-conflict error: %v", err)
	}
	if _, err := NewParser(`INSERT INTO target_rows VALUES (5, 'five') ON CONFLICT DO UPDATE`).ParseStatement(); err == nil {
		t.Fatal("ON CONFLICT DO UPDATE unexpectedly parsed")
	}
}

func TestAlterTableRenameColumnUpdatesIndexesStatisticsAndForeignKeys(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE parent_rows (id INT PRIMARY KEY, name TEXT)`)
	execSQL(t, db, `CREATE TABLE child_rows (id INT PRIMARY KEY, parent_id INT REFERENCES parent_rows(id))`)
	execSQL(t, db, `INSERT INTO parent_rows VALUES (1, 'one'), (2, 'two')`)
	execSQL(t, db, `CREATE INDEX idx_parent_name ON parent_rows(name)`)
	execSQL(t, db, `ANALYZE parent_rows`)

	execSQL(t, db, `ALTER TABLE parent_rows RENAME COLUMN name TO label`)

	rs := execSQL(t, db, `SELECT id FROM parent_rows WHERE label = 'two'`)
	if len(rs.Rows) != 1 || expectAsInt(t, rs.Rows[0]["id"]) != 2 {
		t.Fatalf("renamed indexed column lookup = %#v", rs.Rows)
	}
	if _, err := Execute(context.Background(), db, "default", mustParse(`SELECT name FROM parent_rows`)); err == nil {
		t.Fatal("old column name remained queryable after rename")
	}

	stats := execSQL(t, db, `SELECT column_name FROM sys.statistics WHERE table_name = 'parent_rows'`)
	if len(stats.Rows) != 2 {
		t.Fatalf("statistics rows = %#v", stats.Rows)
	}
	foundLabel := false
	for _, row := range stats.Rows {
		if row["column_name"] == "label" {
			foundLabel = true
		}
		if row["column_name"] == "name" {
			t.Fatalf("old statistics name survived rename: %#v", stats.Rows)
		}
	}
	if !foundLabel {
		t.Fatalf("renamed statistics column missing: %#v", stats.Rows)
	}

	indexes := execSQL(t, db, `SELECT columns FROM sys.indexes WHERE name = 'idx_parent_name'`)
	if len(indexes.Rows) != 1 || indexes.Rows[0]["columns"] != "label" {
		t.Fatalf("catalog index after rename = %#v", indexes.Rows)
	}

	// The child definition must reference the new parent column so later DML
	// continues to validate the relationship.
	execSQL(t, db, `INSERT INTO child_rows VALUES (1, 1)`)
	if _, err := Execute(context.Background(), db, "default", mustParse(`INSERT INTO child_rows VALUES (2, 99)`)); err == nil {
		t.Fatal("foreign key reference stopped validating after parent rename")
	}
}
