package engine

import (
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestDeleteReturningRawScanMatchesTriggerPath(t *testing.T) {
	for _, predicate := range []string{"id < 0", "id = 3", "id >= 2 AND id <= 4"} {
		for _, indexed := range []bool{false, true} {
			var reference, remaining *ResultSet
			for _, triggered := range []bool{true, false} {
				db := storage.NewDB()
				t.Cleanup(func() { _ = db.Close() })
				execSQL(t, db, "CREATE TABLE deleted_rows (id INT, label TEXT)")
				execSQL(t, db, "INSERT INTO deleted_rows VALUES (1,'a'),(2,'b'),(3,NULL),(4,'d'),(5,'e')")
				if indexed {
					execSQL(t, db, "CREATE INDEX deleted_label ON deleted_rows(label)")
				}
				if triggered {
					execSQL(t, db, "CREATE TABLE delete_audit (id INT)")
					execSQL(t, db, "CREATE TRIGGER delete_log AFTER DELETE ON deleted_rows BEGIN INSERT INTO delete_audit VALUES (OLD.id); END")
				}
				got := execSQL(t, db, "DELETE FROM deleted_rows WHERE "+predicate+" RETURNING deleted_rows.id AS id, label")
				left := execSQL(t, db, "SELECT id,label FROM deleted_rows")
				if triggered {
					reference, remaining = got, left
				} else if !reflect.DeepEqual(got, reference) || !reflect.DeepEqual(left, remaining) {
					t.Fatalf("predicate=%s indexed=%t: returned %v / %v, want %v / %v", predicate, indexed, got, left, reference, remaining)
				}
				if _, err := Execute(t.Context(), db, "default", mustParse("DELETE FROM deleted_rows WHERE id > 0 RETURNING missing")); err == nil {
					t.Fatal("expected a projection error")
				}
				if got := execSQL(t, db, "SELECT id,label FROM deleted_rows"); !reflect.DeepEqual(got, left) {
					t.Fatal("failed RETURNING changed retained rows", got)
				}
			}
		}
	}
}

func TestDeleteReturningFailureRestoresWALReplay(t *testing.T) {
	for _, mode := range []storage.StorageMode{storage.ModeWAL, storage.ModeAdvancedWAL} {
		t.Run(mode.String(), func(t *testing.T) {
			config := storage.StorageConfig{Mode: mode, Path: filepath.Join(t.TempDir(), "returning")}
			db, err := storage.OpenDB(config)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = db.Close() })
			execSQL(t, db, "CREATE TABLE durable_delete (id INT, label TEXT)")
			execSQL(t, db, "INSERT INTO durable_delete VALUES (1,'a'),(2,'b'),(3,'c')")
			execSQL(t, db, "CREATE INDEX durable_label ON durable_delete(label)")
			if _, err := Execute(t.Context(), db, "default", mustParse("DELETE FROM durable_delete WHERE id>=2 RETURNING missing")); err == nil {
				t.Fatal("expected a RETURNING error after deletion")
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := storage.OpenDB(config)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = reopened.Close() })
			got := execSQL(t, reopened, "SELECT id FROM durable_delete WHERE label >= 'a' ORDER BY id")
			if !reflect.DeepEqual(got.Rows, []Row{{"id": 1}, {"id": 2}, {"id": 3}}) {
				t.Fatal("failed DELETE survived WAL replay", got)
			}
		})
	}
}

func TestCTEQualifiedFilterKeepsScopeAndParsedStatement(t *testing.T) {
	db := storage.NewDB()
	defer db.Close()
	query := "WITH c(n,flag) AS (SELECT 1,NULL UNION ALL SELECT 2,1 UNION ALL SELECT 3,0) SELECT q.n FROM c q WHERE q.n >= 2 AND NOT (q.flag IS NULL) LIMIT 1 OFFSET 1"
	stmt, original := mustParse(query), mustParse(query)
	var wg sync.WaitGroup
	errors := make(chan error, 4)
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 5 {
				rs, err := Execute(t.Context(), db, "default", stmt)
				if err != nil || rs == nil || len(rs.Rows) != 1 || rs.Rows[0]["q.n"] != 3 {
					errors <- fmt.Errorf("result=%v, error=%v", rs, err)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errors)
	for err := range errors {
		t.Error(err)
	}
	if !reflect.DeepEqual(stmt, original) {
		t.Fatal("qualified filter changed the shared statement")
	}
	for _, query := range []string{
		"WITH c AS (SELECT 1 AS n) SELECT q.n FROM c q WHERE q.missing=1 LIMIT 0",
		"WITH c AS (SELECT 1 AS n) SELECT q.n FROM c q WHERE wrong.n=1",
	} {
		if _, err := Execute(t.Context(), db, "default", mustParse(query)); err == nil {
			t.Fatalf("invalid qualifier/column was accepted: %s", query)
		}
	}
	rs := execSQL(t, db, "WITH c AS (SELECT 1 AS n), d AS (WITH c AS (SELECT 2 AS n) SELECT q.n AS n FROM c q WHERE q.n=2) SELECT z.n FROM d z")
	if len(rs.Rows) != 1 || rs.Rows[0]["z.n"] != 2 {
		t.Fatal("nested WITH lost its scope", rs)
	}
}

func TestCTEWideQualifiedBindingAndMaterializationBoundaries(t *testing.T) {
	db := storage.NewDB()
	defer db.Close()
	values, columns := make([]string, 128), make([]string, 128)
	for i := range values {
		values[i] = fmt.Sprintf("%d AS C%03d", i, i)
		columns[i] = fmt.Sprintf("q.C%03d", i)
	}
	rs := execSQL(t, db, "WITH c AS (SELECT "+strings.Join(values, ",")+") SELECT "+strings.Join(columns, ",")+" FROM c q WHERE q.C127=127")
	if len(rs.Cols) != 128 || len(rs.Rows) != 1 {
		t.Fatal("wide schema lost columns or rows", rs)
	}
	for i, column := range columns {
		if rs.Rows[0][strings.ToLower(column)] != i {
			t.Fatalf("incorrect bound value for %s", column)
		}
	}
	// Filtering before the producer's LIMIT would incorrectly return 2.
	rs = execSQL(t, db, "WITH c AS (SELECT 1 AS n UNION ALL SELECT 2 LIMIT 1) SELECT q.n FROM c q WHERE q.n=2")
	if len(rs.Rows) != 0 {
		t.Fatal("filter crossed the CTE materialization/LIMIT boundary", rs)
	}
	// Reusing a CTE must preserve one evaluation of a volatile source.
	rs = execSQL(t, db, "WITH c AS (SELECT UUID() AS token) SELECT a.token AS a,b.token AS b FROM c a JOIN c b ON a.token=b.token")
	if len(rs.Rows) != 1 || rs.Rows[0]["a"] != rs.Rows[0]["b"] {
		t.Fatal("CTE reuse re-evaluated its source", rs)
	}
	// Exercise the documented staged-load/index/statistics workflow.
	execSQL(t, db, "CREATE TEMP TABLE research_stage AS WITH source(id,label) AS (SELECT 1,'a' UNION ALL SELECT 2,'b') SELECT s.id AS id,s.label AS label FROM source s WHERE s.id>0")
	execSQL(t, db, "CREATE INDEX research_stage_id ON research_stage(id)")
	execSQL(t, db, "ANALYZE research_stage")
	execSQL(t, db, "EXPLAIN ANALYZE SELECT label FROM research_stage WHERE id=2")
}

func TestSelectedInsertCursorKeepsReturningAndRollback(t *testing.T) {
	for _, specific := range []bool{false, true} {
		db := storage.NewDB()
		t.Cleanup(func() { _ = db.Close() })
		execSQL(t, db, "CREATE TABLE source_rows (id INT PRIMARY KEY, label TEXT)")
		execSQL(t, db, "CREATE TABLE target_rows (id INT PRIMARY KEY, label TEXT)")
		values := make([]string, 130)
		for i := range values {
			values[i] = fmt.Sprintf("(%d,'row-%d')", i, i)
		}
		execSQL(t, db, "INSERT INTO source_rows VALUES "+strings.Join(values, ","))
		columns := ""
		if specific {
			columns = " (id,label)"
		}
		query := "INSERT INTO target_rows" + columns + " WITH c AS (SELECT id,label FROM source_rows) SELECT c.id,c.label FROM c WHERE c.id>=0 RETURNING id,label"
		got := execSQL(t, db, query)
		if len(got.Rows) != 130 || got.Rows[0]["label"] != "row-0" || got.Rows[129]["label"] != "row-129" {
			t.Fatal("selected values were overwritten while advancing the cursor", got)
		}
		// A self-insert reads the pre-statement result, including across blocks.
		execSQL(t, db, "INSERT INTO target_rows SELECT id+1000,label FROM target_rows")
		if _, err := Execute(t.Context(), db, "default", mustParse("INSERT INTO target_rows SELECT id+2000,label FROM source_rows RETURNING missing")); err == nil {
			t.Fatal("expected a late RETURNING error")
		}
		got = execSQL(t, db, "SELECT COUNT(*) AS n FROM target_rows")
		if got.Rows[0]["n"] != 260 {
			t.Fatal("failed selected insert retained rows", got)
		}
	}
}

func TestCreateSelectedRowsOwnHeadersAndTempFlag(t *testing.T) {
	for _, temporary := range []bool{false, true} {
		db := storage.NewDB()
		t.Cleanup(func() { _ = db.Close() })
		execSQL(t, db, "CREATE TABLE original_rows (id INT, label TEXT)")
		values := make([]string, 130)
		for i := range values {
			values[i] = fmt.Sprintf("(%d,'before')", i)
		}
		execSQL(t, db, "INSERT INTO original_rows VALUES "+strings.Join(values, ","))
		prefix := "CREATE TABLE"
		if temporary {
			prefix = "CREATE TEMP TABLE"
		}
		execSQL(t, db, prefix+" selected_rows AS WITH c AS (SELECT id,label FROM original_rows) SELECT c.id AS id,c.label AS label FROM c WHERE c.id>=0")
		table, _ := db.Get("default", "selected_rows")
		if table.IsTemp != temporary || len(table.Rows) != 130 {
			t.Fatal("lost materialized rows or TEMP flag")
		}
		execSQL(t, db, prefix+" IF NOT EXISTS selected_rows AS WITH c AS (SELECT 1/0 AS n) SELECT n FROM c")
		retained := append([][]any(nil), table.Rows...)
		for i, row := range table.Rows {
			extended := append(row, "extra")
			extended[len(row)] = "changed"
			if table.Rows[i][0] != i {
				t.Fatal("appending to one row changed a neighboring row")
			}
		}
		execSQL(t, db, "UPDATE selected_rows SET label='after'")
		if retained[64][1] != "before" || table.Rows[64][1] != "after" {
			t.Fatal("UPDATE changed retained CTAS rows")
		}
		source, _ := db.Get("default", "original_rows")
		if source.Rows[64][1] != "before" {
			t.Fatal("materialized table changed its source")
		}
		execSQL(t, db, prefix+" empty_rows AS SELECT id FROM original_rows WHERE id < 0")
		empty, _ := db.Get("default", "empty_rows")
		if len(empty.Rows) != 0 || len(empty.Cols) != 1 || empty.IsTemp != temporary {
			t.Fatal("empty CTAS changed schema or TEMP flag")
		}
		if _, err := Execute(t.Context(), db, "default", mustParse(prefix+" failed_rows AS WITH c AS (SELECT 1/0 AS n) SELECT n FROM c")); err == nil {
			t.Fatal("expected a source-query failure")
		}
		if _, err := db.Get("default", "failed_rows"); err == nil {
			t.Fatal("failed CTAS published a partial table")
		}
	}
}
