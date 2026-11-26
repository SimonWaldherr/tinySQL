package engine

import (
	"reflect"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestCachedInsertBatch(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	for _, sql := range []string{
		`CREATE TABLE docs (id INT PRIMARY KEY, title TEXT DEFAULT 'untitled', embedding VECTOR)`,
		`CREATE INDEX docs_title ON docs(title)`,
		`CREATE TABLE audit (id INT)`,
		`CREATE TRIGGER logged AFTER INSERT ON docs BEGIN INSERT INTO audit VALUES (NEW.id); END`,
		`CREATE TABLE source (id INT, embedding VECTOR)`,
		`INSERT INTO source VALUES (3, '[0,1]')`,
	} {
		execSQL(t, db, sql)
	}
	if got := execSQL(t, db, `INSERT CACHED INTO docs (id, embedding) VALUES (1, '[1,0]')`).Rows[0]["cached"]; got != 1 {
		t.Fatalf("staged %v rows, want 1", got)
	}
	execSQL(t, db, `INSERT CACHED INTO docs VALUES (2, NULL, '[2,0]')`)
	execSQL(t, db, `INSERT CACHED INTO docs (id, embedding) SELECT id, embedding FROM source`)
	execSQL(t, db, `UPDATE source SET embedding = '[8,9]' WHERE id = 3`)
	if len(execSQL(t, db, `SELECT * FROM docs`).Rows) != 0 || len(execSQL(t, db, `SELECT * FROM audit`).Rows) != 0 {
		t.Fatal("staging reached the target or fired triggers")
	}
	// A rejected enqueue must leave the previously staged batch intact.
	if _, err := Execute(t.Context(), db, "default", mustParse(`INSERT CACHED INTO docs VALUES (4, 'more', '[3,0]'), (5, 'bad', 'invalid')`)); err == nil {
		t.Fatal("expected a vector conversion error")
	}
	if got := execSQL(t, db, `FLUSH INSERTS INTO DOCS`).Rows[0]["inserted"]; got != 3 {
		t.Fatalf("flushed %v rows, want 3", got)
	}
	table, err := db.Get("default", "docs")
	if err != nil {
		t.Fatal(err)
	}
	want := [][]any{{1, "untitled", []float64{1, 0}}, {2, nil, []float64{2, 0}}, {3, "untitled", []float64{0, 1}}}
	if !reflect.DeepEqual(table.Rows, want) {
		t.Fatalf("stored rows = %#v, want %#v", table.Rows, want)
	}
	if len(execSQL(t, db, `SELECT id FROM docs WHERE title = 'untitled'`).Rows) != 2 || len(execSQL(t, db, `SELECT * FROM audit`).Rows) != 3 {
		t.Fatal("flush failed to update indexes or fire triggers")
	}
	if got := execSQL(t, db, `FLUSH INSERTS docs`).Rows[0]["inserted"]; got != 0 {
		t.Fatalf("flushed the same batch again: %v", got)
	}
}

func TestCachedInsertFlushRollbackAndRetry(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	for _, sql := range []string{
		`CREATE TABLE t (id INT PRIMARY KEY)`,
		`CREATE TABLE audit (id INT)`,
		`INSERT INTO t VALUES (2)`,
		`CREATE TRIGGER log_rows AFTER INSERT ON t BEGIN INSERT INTO audit VALUES (NEW.id); END`,
		`INSERT CACHED INTO t VALUES (1), (2)`,
	} {
		execSQL(t, db, sql)
	}
	if _, err := Execute(t.Context(), db, "default", mustParse(`FLUSH INSERTS t`)); err == nil {
		t.Fatal("expected a primary-key conflict")
	}
	if len(execSQL(t, db, `SELECT * FROM t`).Rows) != 1 || len(execSQL(t, db, `SELECT * FROM audit`).Rows) != 0 {
		t.Fatal("failed flush published some rows")
	}
	execSQL(t, db, `DELETE FROM t WHERE id = 2`)
	if got := execSQL(t, db, `FLUSH INSERTS t`).Rows[0]["inserted"]; got != 2 {
		t.Fatalf("retry inserted %v rows, want 2", got)
	}
}

func TestCachedInsertIntoEmptyTableRetainsBufferOnFailure(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	execSQL(t, db, `CREATE TABLE docs (id INT, tag TEXT)`)
	execSQL(t, db, `CREATE UNIQUE INDEX unique_tag ON docs(tag)`)
	execSQL(t, db, `INSERT CACHED INTO docs VALUES (1, 'x'), (2, 'x')`)
	table, err := db.Get("default", "docs")
	if err != nil {
		t.Fatal(err)
	}
	db.LockContentForWrite()
	stage := db.StagedInsertTable("default", "docs", "")
	db.UnlockContentForWrite()
	if _, err := Execute(t.Context(), db, "default", mustParse(`FLUSH INSERTS docs`)); err == nil {
		t.Fatal("expected a unique index violation")
	}
	if table.Rows != nil || !reflect.DeepEqual(stage.Rows, [][]any{{1, "x"}, {2, "x"}}) {
		t.Fatalf("rollback left destination rows %v and staged rows %v", table.Rows, stage.Rows)
	}
	execSQL(t, db, `DROP INDEX unique_tag ON docs`)
	execSQL(t, db, `FLUSH INSERTS docs`)
	if len(table.Rows) != 2 || &table.Rows[0] != &stage.Rows[0] {
		t.Fatal("successful transfer did not reuse the staged row headers")
	}
}

func TestCachedInsertSchemaAndOwners(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	execSQL(t, db, `CREATE TABLE docs (id INT)`)
	run := func(tenant, owner, query string) *ResultSet {
		t.Helper()
		rs, err := Execute(WithUser(t.Context(), owner), db, tenant, mustParse(query))
		if err != nil {
			t.Fatal(err)
		}
		return rs
	}
	run("default", "alice", `INSERT CACHED INTO docs VALUES (1)`)
	if n := run("other", "alice", `FLUSH INSERTS docs`).Rows[0]["inserted"]; n != 0 {
		t.Fatalf("different tenant flushed %v rows", n)
	}
	if n := run("default", "bob", `FLUSH INSERTS docs`).Rows[0]["inserted"]; n != 0 {
		t.Fatalf("different owner flushed %v rows", n)
	}
	execSQL(t, db, `ALTER TABLE docs ADD COLUMN extra TEXT`)
	if _, err := Execute(WithUser(t.Context(), "alice"), db, "default", mustParse(`FLUSH INSERTS docs`)); err == nil || !strings.Contains(err.Error(), "schema") {
		t.Fatalf("flush after a schema change: %v", err)
	}
	if n := run("default", "ALICE", `DISCARD INSERTS FROM docs`).Rows[0]["discarded"]; n != 1 {
		t.Fatalf("discard after schema change = %v, want 1", n)
	}
	execSQL(t, db, `INSERT CACHED INTO docs VALUES (2, 'old')`)
	execSQL(t, db, `DROP TABLE docs`)
	execSQL(t, db, `CREATE TABLE docs (id INT, extra TEXT)`)
	if n := execSQL(t, db, `FLUSH INSERTS docs`).Rows[0]["inserted"]; n != 0 {
		t.Fatalf("flushed %v rows into a replacement table", n)
	}
}

func TestCachedInsertOwnsBoundVectors(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	execSQL(t, db, `CREATE TABLE docs (embedding VECTOR)`)
	vector := []float64{1, 0}
	stmt := &Insert{Cached: true, Table: "docs", Rows: [][]Expr{{&Literal{Val: vector}}}}
	if _, err := Execute(t.Context(), db, "default", stmt); err != nil {
		t.Fatal(err)
	}
	vector[0] = 9
	execSQL(t, db, `FLUSH INSERTS docs`)
	got := execSQL(t, db, `SELECT embedding FROM docs`).Rows[0]["embedding"]
	if !reflect.DeepEqual(got, []float64{1, 0}) {
		t.Fatalf("staged vector was modified: %v", got)
	}
}

func TestCachedInsertStorageModes(t *testing.T) {
	for _, mode := range []storage.StorageMode{storage.ModeDisk, storage.ModeWAL, storage.ModeAdvancedWAL} {
		t.Run(mode.String(), func(t *testing.T) {
			cfg := storage.DefaultStorageConfig(mode)
			cfg.Path = t.TempDir() + "/db"
			db, err := storage.OpenDB(cfg)
			if err != nil {
				t.Fatal(err)
			}
			execSQL(t, db, `CREATE TABLE docs (id INT PRIMARY KEY)`)
			execSQL(t, db, `INSERT CACHED INTO docs VALUES (1)`)
			execSQL(t, db, `FLUSH INSERTS docs`)
			execSQL(t, db, `INSERT CACHED INTO docs VALUES (2)`)
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			db, err = storage.OpenDB(cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if n := execSQL(t, db, `FLUSH INSERTS docs`).Rows[0]["inserted"]; n != 0 {
				t.Fatalf("flushed %v uncommitted rows after closing the database", n)
			}
			rows := execSQL(t, db, `SELECT id FROM docs`).Rows
			if len(rows) != 1 || rows[0]["id"] != 1 {
				t.Fatalf("reopened rows: %v", rows)
			}
		})
	}
}

func TestCachedInsertFlushRetainsRowsAfterRejectedAdmission(t *testing.T) {
	cfg := storage.DefaultStorageConfig(storage.ModeHybrid)
	cfg.Path = t.TempDir()
	cfg.MaxMemoryBytes = 1024
	db, err := storage.OpenDB(cfg)
	if err != nil {
		t.Fatal(err)
	}
	execSQL(t, db, `CREATE TABLE docs (id INT, body TEXT)`)
	execSQL(t, db, `INSERT INTO docs VALUES (1, '`+strings.Repeat("x", 8192)+`')`)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = storage.OpenDB(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	execSQL(t, db, `INSERT CACHED INTO docs VALUES (2, 'pending')`)
	if _, err := Execute(t.Context(), db, "default", mustParse(`FLUSH INSERTS docs`)); err == nil || !strings.Contains(err.Error(), "did not fit") {
		t.Fatalf("flush after rejected admission: %v", err)
	}
	if n := execSQL(t, db, `DISCARD INSERTS FROM docs`).Rows[0]["discarded"]; n != 1 {
		t.Fatalf("discarded %v rows after rejected flush, want 1", n)
	}
}
