// Tests for the SELECT * raw fast path (buildSimpleSelectStarProjections).
// Star projections previously disqualified a query from the raw fast path
// entirely, forcing the much slower general Row-map path (see
// BenchmarkSelectStarFullScan vs BenchmarkSelectProjectedFullScan). These
// tests guard the correctness of the new fast path across WHERE, ORDER BY,
// LIMIT/OFFSET, NULLs, and empty tables.
package engine

import (
	"context"
	"strconv"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestSelectStarFastPathBasic(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()
	Execute(ctx, db, "default", mustParse(`CREATE TABLE t (id INT, name TEXT, score FLOAT)`))
	Execute(ctx, db, "default", mustParse(`INSERT INTO t VALUES (1, 'Alice', 9.5)`))
	Execute(ctx, db, "default", mustParse(`INSERT INTO t VALUES (2, 'Bob', 7.25)`))
	Execute(ctx, db, "default", mustParse(`INSERT INTO t VALUES (3, NULL, NULL)`))

	rs := execSQL(t, db, `SELECT * FROM t`)
	if len(rs.Cols) != 3 || rs.Cols[0] != "id" || rs.Cols[1] != "name" || rs.Cols[2] != "score" {
		t.Fatalf("unexpected columns: %+v", rs.Cols)
	}
	if len(rs.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rs.Rows))
	}
	expectInt(t, rs.Rows[0]["id"], 1, "row0.id")
	if rs.Rows[0]["name"] != "Alice" {
		t.Errorf("row0.name = %v", rs.Rows[0]["name"])
	}
	if rs.Rows[2]["name"] != nil || rs.Rows[2]["score"] != nil {
		t.Errorf("row2 expected NULLs, got name=%v score=%v", rs.Rows[2]["name"], rs.Rows[2]["score"])
	}
}

func TestSelectStarFastPathWhere(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()
	Execute(ctx, db, "default", mustParse(`CREATE TABLE t (id INT, val INT)`))
	for i := 1; i <= 10; i++ {
		execSQL(t, db, `INSERT INTO t VALUES (`+strconv.Itoa(i)+`, `+strconv.Itoa(i*10)+`)`)
	}
	rs := execSQL(t, db, `SELECT * FROM t WHERE val > 50`)
	if len(rs.Rows) != 5 {
		t.Fatalf("expected 5 rows (val>50), got %d", len(rs.Rows))
	}
	// 2 output columns in Cols, but each row map carries both the
	// unqualified ("id") and qualified ("t.id") key per column, matching
	// rowsFromTable's dual-key behavior for the general path.
	for _, r := range rs.Rows {
		if len(r) != 4 {
			t.Fatalf("expected 4 keys per row (2 cols x qualified+unqualified), got %d: %+v", len(r), r)
		}
		if _, ok := r["id"]; !ok {
			t.Errorf("missing unqualified key 'id' in %+v", r)
		}
		if _, ok := r["t.id"]; !ok {
			t.Errorf("missing qualified key 't.id' in %+v", r)
		}
	}
}

func TestSelectStarFastPathOrderByAndLimit(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()
	Execute(ctx, db, "default", mustParse(`CREATE TABLE t (id INT, val INT)`))
	for i := 1; i <= 10; i++ {
		execSQL(t, db, `INSERT INTO t VALUES (`+strconv.Itoa(i)+`, `+strconv.Itoa(11-i)+`)`)
	}

	rs := execSQL(t, db, `SELECT * FROM t ORDER BY val ASC LIMIT 3`)
	if len(rs.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rs.Rows))
	}
	// val = 11-id, ascending val means descending id: 10, 9, 8
	expectInt(t, rs.Rows[0]["id"], 10, "rank0.id")
	expectInt(t, rs.Rows[1]["id"], 9, "rank1.id")
	expectInt(t, rs.Rows[2]["id"], 8, "rank2.id")

	rs = execSQL(t, db, `SELECT * FROM t ORDER BY id LIMIT 3 OFFSET 5`)
	if len(rs.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rs.Rows))
	}
	expectInt(t, rs.Rows[0]["id"], 6, "offset rank0.id")
}

// TestSelectStarFastPathQualifiedKeys guards a specific regression: the
// general path (rowsFromTable) writes both "col" and "table.col" keys into
// each Row; GetVal(row, "table.col") is a documented, tested public-API
// pattern (see example_test.go's TestFilePersistence and
// TestDistinctAndLimitOffset). The star fast path must match this exactly.
func TestSelectStarFastPathQualifiedKeys(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()
	Execute(ctx, db, "default", mustParse(`CREATE TABLE orders (id INT, total FLOAT)`))
	Execute(ctx, db, "default", mustParse(`INSERT INTO orders VALUES (1, 99.5)`))

	rs := execSQL(t, db, `SELECT * FROM orders`)
	if _, ok := rs.Rows[0]["orders.id"]; !ok {
		t.Fatal("missing qualified key orders.id")
	}
	expectInt(t, rs.Rows[0]["orders.id"], 1, "orders.id")
	if _, ok := rs.Rows[0]["id"]; !ok {
		t.Fatal("missing unqualified key id")
	}
	expectInt(t, rs.Rows[0]["id"], 1, "id")

	// An explicit alias must be reflected in the qualified key too.
	rs = execSQL(t, db, `SELECT * FROM orders AS o`)
	if _, ok := rs.Rows[0]["o.id"]; !ok {
		t.Fatal("missing aliased qualified key o.id")
	}
	expectInt(t, rs.Rows[0]["o.id"], 1, "o.id")
}

func TestSelectStarFastPathEmptyTable(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()
	Execute(ctx, db, "default", mustParse(`CREATE TABLE t (id INT, val TEXT)`))
	rs := execSQL(t, db, `SELECT * FROM t`)
	if len(rs.Rows) != 0 {
		t.Fatalf("expected 0 rows, got %d", len(rs.Rows))
	}
	if len(rs.Cols) != 2 {
		t.Fatalf("expected 2 columns even for empty table, got %+v", rs.Cols)
	}
}

func TestSelectStarMatchesGeneralPathForJoins(t *testing.T) {
	// A join still must use the general path (or the join fast path) since
	// buildSimpleSelectPlan requires len(s.Joins) == 0; this just confirms
	// SELECT * across a join still works after the star fast-path change.
	db := storage.NewDB()
	ctx := context.Background()
	Execute(ctx, db, "default", mustParse(`CREATE TABLE a (id INT, x TEXT)`))
	Execute(ctx, db, "default", mustParse(`CREATE TABLE b (id INT, y TEXT)`))
	Execute(ctx, db, "default", mustParse(`INSERT INTO a VALUES (1, 'foo')`))
	Execute(ctx, db, "default", mustParse(`INSERT INTO b VALUES (1, 'bar')`))
	rs := execSQL(t, db, `SELECT * FROM a JOIN b ON a.id = b.id`)
	if len(rs.Rows) != 1 {
		t.Fatalf("expected 1 joined row, got %d", len(rs.Rows))
	}
}
