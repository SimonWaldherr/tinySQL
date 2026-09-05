package driver

import (
	"context"
	"database/sql/driver"
	"fmt"
	"testing"
)

func TestPreparedLikePatternsRemainDynamic(t *testing.T) {
	raw, err := (&drv{}).Open("mem://")
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	c := raw.(*conn)
	ctx := context.Background()
	for _, sql := range []string{`CREATE TABLE texts (v TEXT)`, `INSERT INTO texts VALUES ('alpha-middle-tail'), ('beta-middle-tail'), (NULL)`} {
		if _, err := c.ExecContext(ctx, sql, nil); err != nil {
			t.Fatal(err)
		}
	}
	for _, predicate := range []string{"v LIKE ?", "v ILIKE ?", "ROW_TO_TEXT() LIKE ?"} {
		stmt, err := c.Prepare("SELECT v FROM texts WHERE " + predicate)
		if err != nil {
			t.Fatal(err)
		}
		for _, prefix := range []string{"alpha", "beta"} {
			rows, err := stmt.Query([]driver.Value{prefix + "%middle%tail"})
			if err != nil {
				t.Fatal(err)
			}
			values := make([]driver.Value, 1)
			if err := rows.Next(values); err != nil {
				t.Fatal(err)
			}
			if got := fmt.Sprint(values[0]); got != prefix+"-middle-tail" {
				t.Fatalf("%s got %s", predicate, got)
			}
			rows.Close()
		}
		stmt.Close()
	}
}
