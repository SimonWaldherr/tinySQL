package engine

import (
	"context"
	"fmt"
	"testing"
)

func rowsOnlyEngineTable(t *testing.T) (db interface{}, run func(sql string) (*ResultSet, error)) {
	t.Helper()
	d := seekTestDB(t)
	t.Cleanup(func() { _ = d.Close() })
	run = func(sql string) (*ResultSet, error) {
		return Execute(context.Background(), d, "default", mustParse(sql))
	}
	return d, run
}

func columnInts(t *testing.T, rs *ResultSet, col string) []int {
	t.Helper()
	var out []int
	for _, r := range rs.Rows {
		switch v := r[col].(type) {
		case int:
			out = append(out, v)
		case int64:
			out = append(out, int(v))
		default:
			t.Fatalf("column %s: unexpected %T", col, r[col])
		}
	}
	return out
}

// A multi-row UPDATE that fails on a later row (a UNIQUE index violation) must
// leave every row and the index exactly as before. The failing statement has
// already replaced the earlier rows and moved their index entries when it stops.
func TestFailedMultiRowUpdateRollsBackRowsAndIndex(t *testing.T) {
	_, run := rowsOnlyEngineTable(t)
	for _, sql := range []string{
		`CREATE TABLE items (id INT, k INT, note TEXT)`,
		`CREATE UNIQUE INDEX idx_items_k ON items(k)`,
	} {
		if _, err := run(sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	for i := 1; i <= 6; i++ {
		if _, err := run(fmt.Sprintf(`INSERT INTO items VALUES (%d, %d, 'n%d')`, i, i*10, i)); err != nil {
			t.Fatal(err)
		}
	}
	// k of every row becomes 99: the first row succeeds, the second collides.
	if _, err := run(`UPDATE items SET k = 99, note = 'neu' WHERE id >= 1`); err == nil {
		t.Fatal("the update must fail on the UNIQUE index")
	}
	rs, err := run(`SELECT id FROM items ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	if got := columnInts(t, rs, "id"); len(got) != 6 {
		t.Fatalf("rows after the failed update: %v", got)
	}
	rs, err = run(`SELECT note FROM items WHERE id = 1`)
	if err != nil || len(rs.Rows) != 1 || rs.Rows[0]["note"] != "n1" {
		t.Fatalf("the first row must be back to n1: %v %v", rs, err)
	}
	// The index must answer for the old keys and must not know the new one.
	for i := 1; i <= 6; i++ {
		rs, err := run(fmt.Sprintf(`SELECT id FROM items WHERE k = %d`, i*10))
		if err != nil {
			t.Fatal(err)
		}
		if got := columnInts(t, rs, "id"); len(got) != 1 || got[0] != i {
			t.Errorf("k=%d: ids %v, want [%d]", i*10, got, i)
		}
	}
	rs, err = run(`SELECT id FROM items WHERE k = 99`)
	if err != nil || len(rs.Rows) != 0 {
		t.Fatalf("the rolled-back key must not be indexed: %v %v", rs, err)
	}
	// And the table still works afterwards.
	if _, err := run(`UPDATE items SET note = 'ok' WHERE k = 30`); err != nil {
		t.Fatal(err)
	}
	rs, _ = run(`SELECT note FROM items WHERE id = 3`)
	if len(rs.Rows) != 1 || rs.Rows[0]["note"] != "ok" {
		t.Fatalf("update after the rollback: %v", rs)
	}
}

// The ordinary, successful paths stay correct without the deep copy: a
// multi-row UPDATE and DELETE on a table with a secondary index and no key.
func TestMultiRowUpdateAndDeleteOnIndexedTableWithoutKey(t *testing.T) {
	_, run := rowsOnlyEngineTable(t)
	for _, sql := range []string{
		`CREATE TABLE chunks (id INT, src TEXT, pos INT, body TEXT)`,
		`CREATE INDEX idx_chunks_src ON chunks(src, pos)`,
	} {
		if _, err := run(sql); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 60; i++ {
		if _, err := run(fmt.Sprintf(`INSERT INTO chunks VALUES (%d, 's%d', %d, 'b%d')`, i, i/6, i%6, i)); err != nil {
			t.Fatal(err)
		}
	}
	rs, err := run(`UPDATE chunks SET body = 'x' WHERE src = 's3'`)
	if err != nil || rs.Rows[0]["updated"] != 6 {
		t.Fatalf("update: %v %v", rs, err)
	}
	rs, err = run(`DELETE FROM chunks WHERE src IN ('s1', 's2')`)
	if err != nil || rs.Rows[0]["deleted"] != 12 {
		t.Fatalf("delete: %v %v", rs, err)
	}
	rs, _ = run(`SELECT id FROM chunks WHERE src = 's3' ORDER BY pos`)
	if got := columnInts(t, rs, "id"); len(got) != 6 || got[0] != 18 {
		t.Fatalf("index after update+delete: %v", got)
	}
	rs, _ = run(`SELECT id FROM chunks WHERE src = 's2'`)
	if len(rs.Rows) != 0 {
		t.Fatalf("deleted source still indexed: %v", rs.Rows)
	}
	rs, _ = run(`SELECT id FROM chunks WHERE src = 's9'`)
	if got := columnInts(t, rs, "id"); len(got) != 6 {
		t.Fatalf("rows shifted by the delete must still be found: %v", got)
	}
	// Matching nothing changes nothing and does not error.
	if _, err := run(`DELETE FROM chunks WHERE src = 'gibt-es-nicht'`); err != nil {
		t.Fatal(err)
	}
}
