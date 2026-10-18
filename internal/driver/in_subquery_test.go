package driver

import (
	"database/sql"
	"testing"
)

func TestSQLPreparedInSubqueryBindingsAndFetch(t *testing.T) {
	db := orderByFixture(t)
	for _, query := range []string{
		`CREATE TABLE candidates (v INT, bucket INT)`,
		`INSERT INTO candidates VALUES (NULL, 1), (2, 1), (3, 1), (1, 2)`,
	} {
		if _, err := db.Exec(query); err != nil {
			t.Fatalf("%s: %v", query, err)
		}
	}
	membership, err := db.Prepare(`SELECT id, id IN (SELECT v FROM candidates WHERE bucket = ?) AS matches FROM t ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer membership.Close()
	first, err := db.Prepare(`SELECT id FROM t WHERE id > 0 AND id IN (SELECT v FROM candidates WHERE bucket = ?) ORDER BY id FETCH FIRST ROW ONLY`)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()

	check := func(bucket int, want []any, wantFirst int) {
		t.Helper()
		rows, err := membership.Query(bucket)
		if err != nil {
			t.Fatalf("bucket %d: %v", bucket, err)
		}
		defer rows.Close()
		count := 0
		for rows.Next() {
			var id int
			var match sql.NullBool
			if err := rows.Scan(&id, &match); err != nil {
				t.Fatalf("bucket %d: %v", bucket, err)
			}
			if count >= len(want) || id != count+1 {
				t.Fatalf("bucket %d: unexpected row %d with id %d", bucket, count, id)
			}
			if value := want[count]; match.Valid != (value != nil) || value != nil && match.Bool != value.(bool) {
				t.Fatalf("bucket %d, id %d: got %#v, want %#v", bucket, id, match, value)
			}
			count++
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		if count != len(want) {
			t.Fatalf("bucket %d: got %d rows, want %d", bucket, count, len(want))
		}
		limited, err := first.Query(bucket)
		if err != nil {
			t.Fatalf("bucket %d FETCH: %v", bucket, err)
		}
		defer limited.Close()
		count = 0
		for limited.Next() {
			var id int
			if err := limited.Scan(&id); err != nil {
				t.Fatal(err)
			}
			count++
			if count > 1 || id != wantFirst {
				t.Fatalf("bucket %d FETCH: row %d has id %d, want at most one row with id %d", bucket, count, id, wantFirst)
			}
		}
		if err := limited.Err(); err != nil {
			t.Fatal(err)
		}
		if wantFirst != 0 && count != 1 {
			t.Fatalf("bucket %d FETCH: got %d rows, want one", bucket, count)
		}
	}
	check(1, []any{nil, true, true}, 2)
	check(2, []any{true, false, false}, 1)
	check(3, []any{false, false, false}, 0)
	check(1, []any{nil, true, true}, 2)

	// Reuse both prepared statements after mutation: bindings and subquery
	// result caches belong to an execution, rather than the prepared AST.
	if _, err := db.Exec(`INSERT INTO candidates VALUES (?, ?)`, 1, 1); err != nil {
		t.Fatal(err)
	}
	check(1, []any{true, true, true}, 1)
	if _, err := db.Exec(`DELETE FROM candidates WHERE bucket = ?`, 1); err != nil {
		t.Fatal(err)
	}
	check(1, []any{false, false, false}, 0)
	check(2, []any{true, false, false}, 1)
}
