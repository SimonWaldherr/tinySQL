package engine

import (
	"reflect"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestInsertSelectColumnarSelfInsert(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	execSQL(t, db, `CREATE TABLE copied (id INT, label TEXT)`)
	execSQL(t, db, `CREATE INDEX copied_id ON copied(id)`)
	execSQL(t, db, `INSERT INTO copied VALUES (3,'a'),(1,NULL),(2,'b'),(4,'skip')`)
	result := execSQL(t, db, `INSERT INTO copied (label, id)
		SELECT label AS "Collected", id AS "Entry" FROM copied WHERE id >= 1 LIMIT 2 OFFSET 1
		RETURNING id, label`)
	want := []Row{{"id": 1, "label": nil}, {"id": 2, "label": "b"}}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("self-insert returned %#v, want %#v", result.Rows, want)
	}
	table, err := db.Get("default", "copied")
	if err != nil {
		t.Fatal(err)
	}
	if len(table.Rows) != 6 {
		t.Fatalf("self-insert appended %d rows, want 2", len(table.Rows)-4)
	}
	indexed := execSQL(t, db, `SELECT label FROM copied WHERE id = 2`)
	if !reflect.DeepEqual(indexed.Rows, []Row{{"label": "b"}, {"label": "b"}}) {
		t.Fatalf("index lost an inserted row: %#v", indexed.Rows)
	}
}

func TestInsertSelectColumnarFallbackAndRollback(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	execSQL(t, db, `CREATE TABLE source_rows (id INT, first TEXT, second TEXT)`)
	execSQL(t, db, `INSERT INTO source_rows VALUES (1,'a','b'),(2,'c','d')`)
	execSQL(t, db, `CREATE TABLE destination (id INT PRIMARY KEY, first TEXT, second TEXT)`)
	execSQL(t, db, `CREATE INDEX destination_first ON destination(first)`)
	result := execSQL(t, db, `INSERT INTO destination
		SELECT id, first AS shared, second AS shared FROM source_rows WHERE id = 2
		RETURNING id, first, second`)
	if !reflect.DeepEqual(result.Rows, []Row{{"id": 2, "first": "d", "second": "d"}}) {
		t.Fatalf("duplicate projection names changed semantics: %#v", result.Rows)
	}
	_, err := Execute(t.Context(), db, "default", mustParse(`INSERT INTO destination SELECT * FROM source_rows`))
	if err == nil {
		t.Fatal("expected duplicate key in second source row")
	}
	result = execSQL(t, db, `SELECT id FROM destination WHERE first = 'a'`)
	if len(result.Rows) != 0 {
		t.Fatalf("index exposes a rolled back row: %#v", result.Rows)
	}
	execSQL(t, db, `INSERT INTO destination SELECT * FROM source_rows WHERE id = 1`)
	result = execSQL(t, db, `SELECT id FROM destination WHERE first = 'a'`)
	if !reflect.DeepEqual(result.Rows, []Row{{"id": 1}}) {
		t.Fatalf("retry failed after rollback: %#v", result.Rows)
	}
}
