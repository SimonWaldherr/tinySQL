package driver

import (
	"context"
	"database/sql"
	"reflect"
	"testing"
)

func TestSQLPreparedExplainAnalyze(t *testing.T) {
	db := orderByFixture(t)
	stmt, err := db.Prepare("EXPLAIN ANALYZE SELECT id FROM t WHERE id>=? ORDER BY score")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	for _, bound := range []int{1, 3, 2} {
		rows, err := stmt.Query(bound)
		if err != nil {
			t.Fatal(err)
		}
		columns, err := rows.Columns()
		wantColumns := []string{"step", "operation", "detail", "parent_step", "calls", "actual_rows", "total_ns", "self_ns", "percent", "bottleneck"}
		if err != nil || !reflect.DeepEqual(columns, wantColumns) {
			t.Fatalf("columns %v, %v", columns, err)
		}
		roots, hottest := 0, 0
		for rows.Next() {
			var step int
			var operation, detail string
			var parent, calls, count, total, self sql.NullInt64
			var percent sql.NullFloat64
			var bottleneck sql.NullBool
			if err := rows.Scan(&step, &operation, &detail, &parent, &calls, &count, &total, &self, &percent, &bottleneck); err != nil {
				t.Fatal(err)
			}
			if operation == "EXECUTE" {
				roots++
				if !count.Valid || count.Int64 != int64(4-bound) || !calls.Valid || calls.Int64 != 1 || parent.Valid {
					t.Fatalf("bound %d: count=%v calls=%v parent=%v", bound, count, calls, parent)
				}
			}
			if bottleneck.Valid && bottleneck.Bool {
				hottest++
			}
		}
		err = rows.Err()
		rows.Close()
		if err != nil || roots != 1 || hottest != 1 {
			t.Fatalf("roots=%d hottest=%d error=%v", roots, hottest, err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if rows, err := stmt.QueryContext(ctx, 1); err == nil {
		rows.Close()
		t.Fatal("canceled analyze succeeded")
	}
}
