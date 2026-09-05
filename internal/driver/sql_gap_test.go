package driver

import (
	"context"
	"database/sql/driver"
	"testing"
)

func TestPreparedLeadingWithInsertAndModulo(t *testing.T) {
	raw, err := (&drv{}).Open("mem://")
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	c := raw.(*conn)
	ctx := context.Background()
	if _, err := c.ExecContext(ctx, `CREATE TABLE values_table (id INT)`, nil); err != nil {
		t.Fatal(err)
	}
	insert, err := c.Prepare(`WITH c AS (SELECT ? AS n) INSERT INTO values_table SELECT n FROM c`)
	if err != nil {
		t.Fatal(err)
	}
	defer insert.Close()
	for _, v := range []int64{5, 8} {
		if _, err := insert.Exec([]driver.Value{v}); err != nil {
			t.Fatal(err)
		}
	}
	selectStmt, err := c.Prepare(`SELECT id % ? AS remainder FROM values_table ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer selectStmt.Close()
	for _, tc := range []struct {
		divisor int64
		want    []float64
	}{{2, []float64{1, 0}}, {3, []float64{2, 2}}} {
		rows, err := selectStmt.Query([]driver.Value{tc.divisor})
		if err != nil {
			t.Fatal(err)
		}
		for _, want := range tc.want {
			values := make([]driver.Value, 1)
			if err := rows.Next(values); err != nil {
				t.Fatal(err)
			}
			if values[0] != want {
				t.Fatalf("got %v want %v", values[0], want)
			}
		}
		rows.Close()
	}
}
