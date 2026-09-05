package driver

import (
	"context"
	"database/sql/driver"
	"io"
	"testing"
)

func TestOptimizedPredicatesSQL(t *testing.T) {
	raw, err := (&drv{}).Open("mem://")
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	c := raw.(*conn)
	ctx := context.Background()
	for _, q := range []string{"CREATE TABLE predicate_values (v INT, name TEXT)", "INSERT INTO predicate_values VALUES (1,'document2026.json'),(2,'other'),(NULL,NULL)"} {
		if _, err := c.ExecContext(ctx, q, nil); err != nil {
			t.Fatal(err)
		}
	}
	for _, tc := range []struct {
		predicate string
		count     int
	}{
		{"v IN (1,NULL)", 1}, {"v NOT IN (1,NULL)", 0}, {"NOT (v IN (1,NULL))", 0},
		{"v IN (1,2)", 2}, {"v NOT IN (1)", 1}, {"v BETWEEN 1 AND 2", 2}, {"v NOT BETWEEN 1 AND 1", 1},
		{"name LIKE 'document____.json'", 1}, {"name NOT LIKE 'document____.json'", 1},
		{"UPPER(name) LIKE 'DOCUMENT____.JSON'", 1},
	} {
		rows, err := c.QueryContext(ctx, "SELECT v FROM predicate_values WHERE "+tc.predicate, nil)
		if err != nil {
			t.Fatal(tc.predicate, err)
		}
		n := 0
		for {
			err = rows.Next(make([]driver.Value, 1))
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			n++
		}
		rows.Close()
		if n != tc.count {
			t.Fatalf("%s: got %d rows want %d", tc.predicate, n, tc.count)
		}
	}
	stmt, err := c.Prepare("SELECT v FROM predicate_values WHERE v IN (?)")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	for _, v := range []int64{1, 2, 1} {
		rows, err := stmt.Query([]driver.Value{v})
		if err != nil {
			t.Fatal(err)
		}
		values := make([]driver.Value, 1)
		if err := rows.Next(values); err != nil {
			t.Fatal(err)
		}
		if values[0] != v {
			t.Fatalf("parameter %d: %v", v, values[0])
		}
		rows.Close()
	}
}
