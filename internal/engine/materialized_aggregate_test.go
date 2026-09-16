package engine

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestMaterializedAggregateOwnsGroupKeys(t *testing.T) {
	short := "group-one"
	long := strings.Repeat("long-group-", 40)
	rows := []Row{
		{"grp": short, "sub": 1, "val": 2.0},
		{"grp": long, "sub": 2, "val": 3.0},
		{"grp": nil, "sub": 1, "val": nil},
		{"grp": short, "sub": 1, "val": 4.0},
		{"grp": long, "sub": 2, "val": 5.0},
		{"grp": nil, "sub": 1, "val": 7.0},
		{"grp": short, "sub": 2, "val": 8.0},
	}
	stmt := mustParse(`SELECT grp, sub, COUNT(*) AS n, SUM(val) AS total FROM t GROUP BY grp, sub`).(*Select)
	got, cols, err := processAggregateQuery(ExecEnv{ctx: context.Background()}, stmt, rows)
	if err != nil {
		t.Fatal(err)
	}
	want := []Row{
		{"grp": short, "sub": 1, "n": 2, "total": 6.0},
		{"grp": long, "sub": 2, "n": 2, "total": 8.0},
		{"grp": nil, "sub": 1, "n": 2, "total": 7.0},
		{"grp": short, "sub": 2, "n": 1, "total": 8.0},
	}
	if !reflect.DeepEqual(got, want) || !reflect.DeepEqual(cols, []string{"grp", "sub", "n", "total"}) {
		t.Fatalf("got %#v (%v), want %#v", got, cols, want)
	}
}

func TestMaterializedUngroupedAggregate(t *testing.T) {
	rows := []Row{{"val": 3.0}, {"val": nil}, {"val": 1.0}, {"val": 2.0}}
	original := make([]Row, len(rows))
	for i, row := range rows {
		original[i] = cloneRow(row)
	}
	for _, tc := range []struct {
		name, sql string
		input     []Row
		want      []Row
		wantError bool
	}{
		{"simple", `SELECT COUNT(*) AS n, SUM(val) AS total FROM t`, rows, []Row{{"n": 4, "total": 6.0}}, false},
		{"having", `SELECT COUNT(*) AS n, SUM(val) AS total FROM t HAVING COUNT(*) > 0`, rows, []Row{{"n": 4, "total": 6.0}}, false},
		{"median-expression", `SELECT COUNT(*) + 1 AS n, MEDIAN(val) AS median FROM t`, rows, []Row{{"n": 5.0, "median": 2.0}}, false},
		{"empty-simple", `SELECT COUNT(*) AS n, SUM(val) AS total FROM t`, nil, []Row{{"n": 0, "total": nil}}, false},
		{"empty-having", `SELECT COUNT(*) AS n, SUM(val) AS total FROM t HAVING COUNT(*) IS NOT NULL`, nil, []Row{{"n": 0, "total": nil}}, false},
		{"empty-rejected", `SELECT COUNT(*) AS n FROM t HAVING COUNT(*) > 0`, nil, nil, false},
		{"having-error", `SELECT COUNT(*) AS n FROM t HAVING SUM(missing) > 0`, rows, nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stmt := mustParse(tc.sql).(*Select)
			got, _, err := processAggregateQuery(ExecEnv{ctx: context.Background()}, stmt, tc.input)
			if tc.wantError {
				if err == nil {
					t.Fatal("expected unknown-column error")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != len(tc.want) {
				t.Fatalf("got %#v, want %#v", got, tc.want)
			}
			for i := range got {
				if !reflect.DeepEqual(got[i], tc.want[i]) {
					t.Fatalf("got %#v, want %#v", got, tc.want)
				}
			}
			if !reflect.DeepEqual(rows, original) {
				t.Fatal("aggregation changed its input rows or their order")
			}
		})
	}
}

func TestMaterializedUngroupedAggregateCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, sql := range []string{
		`SELECT COUNT(*) AS n FROM t`,
		`SELECT COUNT(*) AS n FROM t HAVING COUNT(*) > 0`,
	} {
		_, _, err := processAggregateQuery(ExecEnv{ctx: ctx}, mustParse(sql).(*Select), []Row{{"val": 1}})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("%s: got %v, want cancellation", sql, err)
		}
	}
}
