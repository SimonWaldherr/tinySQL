package engine

import (
	"context"
	"errors"
	"math"
	"math/big"
	"reflect"
	"testing"
)

func TestAggregateBatchesMatchScalar(t *testing.T) {
	for _, dataset := range []string{"numeric", "mixed", "decimal_first", "decimal_boundary", "decimal_late", "nonnumeric_late", "null"} {
		t.Run(dataset, func(t *testing.T) {
			db := columnarFixture(t, 4099, 4)
			table, _ := db.Get("default", "batch_facts")
			for i, row := range table.Rows {
				// Exercise NULL groups and distinct Go numeric group types.
				if i%7 == 0 {
					row[1] = nil
				}
				if i%11 == 0 {
					row[1] = int64(i % 16)
				}
				if i%13 == 0 {
					row[1] = float64(i % 16)
				}
				if dataset == "mixed" {
					row[2] = []any{nil, int(2), int64(-3), float64(0.1), "4.5", true, "bad"}[i%7]
				}
				if dataset == "null" {
					row[2] = nil
				}
			}
			switch dataset {
			case "decimal_first":
				table.Rows[0][2] = big.NewRat(1, 3)
			case "decimal_boundary":
				table.Rows[511][2] = big.NewRat(1, 3)
			case "decimal_late":
				table.Rows[1025][2] = big.NewRat(1, 3)
			case "nonnumeric_late":
				table.Rows[1025][2] = "not a number"
			}
			for _, sql := range []string{
				`SELECT SUM(amount),AVG(amount),COUNT(amount),SUM(quantity),COUNT(*) FROM batch_facts`,
				`SELECT COUNT(amount),AVG(amount),SUM(amount),AVG(amount) AS again,COUNT(*) AS n,COUNT(*) AS again_n FROM batch_facts`,
				`SELECT grp,SUM(amount),AVG(amount),COUNT(*) FROM batch_facts GROUP BY grp`,
				`SELECT grp,COUNT(amount) AS n,AVG(amount) AS a,SUM(amount) AS s,COUNT(amount) AS again FROM batch_facts GROUP BY grp HAVING COUNT(amount)>0 ORDER BY a DESC`,
				`SELECT grp,quantity,SUM(amount),COUNT(*) FROM batch_facts GROUP BY grp,quantity`,
				`SELECT grp,SUM(amount) AS total,COUNT(*) AS n FROM batch_facts WHERE id>=1024 GROUP BY grp HAVING COUNT(*)>5 ORDER BY total DESC LIMIT 7 OFFSET 2`,
				`SELECT SUM(amount),COUNT(*) FROM batch_facts WHERE id<0`,
				`SELECT grp,SUM(amount),COUNT(*) FROM batch_facts WHERE id<0 GROUP BY grp`,
				`SELECT SUM(amount),COUNT(*) FROM batch_facts WHERE id>=4096`,
			} {
				env := ExecEnv{ctx: t.Context(), db: db, tenant: "default"}
				plan, ok, err := buildSimpleAggregatePlan(env, mustParse(sql).(*Select))
				if err != nil || !ok {
					t.Fatalf("plan %s: %v", sql, err)
				}
				source, err := buildSimpleAggregateSourcePlan(plan)
				if err != nil {
					t.Fatal(err)
				}
				cols, ops, ok := buildAggregateBatch(plan, source)
				if !ok {
					t.Fatalf("batch not selected: %s", sql)
				}
				got, _, err := executeAggregateBatches(env, plan, source, cols, ops)
				if err != nil {
					t.Fatal(err)
				}
				var want *ResultSet
				if len(plan.groupCols) == 1 {
					want, _, err = executeSimpleSingleGroupAggregate(env, plan, source)
				} else {
					want, _, err = executeSimpleMultiGroupAggregate(env, plan, source)
				}
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("%s: batch %v; scalar %v", sql, got, want)
				}
			}
		})
	}
}

func TestAggregateBatchFloatOrder(t *testing.T) {
	db := columnarFixture(t, 4097, 4)
	table, _ := db.Get("default", "batch_facts")
	for i, row := range table.Rows {
		row[2] = []float64{1e16, 1, -1e16, 0.1}[i%4]
	}
	for _, special := range []float64{0.1, math.Inf(1), math.Inf(-1), math.NaN()} {
		table.Rows[2048][2] = special
		env := ExecEnv{ctx: t.Context(), db: db, tenant: "default"}
		plan, _, _ := buildSimpleAggregatePlan(env, mustParse(`SELECT SUM(amount) AS s,AVG(amount) AS a FROM batch_facts`).(*Select))
		source, _ := buildSimpleAggregateSourcePlan(plan)
		cols, ops, _ := buildAggregateBatch(plan, source)
		got, _, err := executeAggregateBatches(env, plan, source, cols, ops)
		if err != nil {
			t.Fatal(err)
		}
		want, _, err := executeSimpleMultiGroupAggregate(env, plan, source)
		if err != nil {
			t.Fatal(err)
		}
		for _, key := range []string{"s", "a"} {
			x, y := got.Rows[0][key].(float64), want.Rows[0][key].(float64)
			if math.Float64bits(x) != math.Float64bits(y) {
				t.Fatalf("%s bits differ: %v %v", key, x, y)
			}
		}
	}
}

func TestAggregateBatchCandidatesAndCancellation(t *testing.T) {
	db := columnarFixture(t, 4099, 4)
	env := ExecEnv{ctx: t.Context(), db: db, tenant: "default"}
	plan, _, _ := buildSimpleAggregatePlan(env, mustParse(`SELECT SUM(amount),COUNT(*) FROM batch_facts`).(*Select))
	source, _ := buildSimpleAggregateSourcePlan(plan)
	source.rowIDs = make([]int, 2049)
	for i := range source.rowIDs {
		source.rowIDs[i] = 2 * i
	}
	cols, ops, ok := buildAggregateBatch(plan, source)
	if !ok {
		t.Fatal("expected batch")
	}
	got, _, err := executeAggregateBatches(env, plan, source, cols, ops)
	if err != nil {
		t.Fatal(err)
	}
	want, _, err := executeSimpleMultiGroupAggregate(env, plan, source)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatal(got, want, err)
	}
	source.rowIDs[100] = -1
	if _, _, err := executeAggregateBatches(env, plan, source, cols, ops); err == nil {
		t.Fatal("invalid candidate accepted")
	}
	source.rowIDs = nil
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	env.ctx = ctx
	calls := 0
	source.filter = func([]any) (bool, error) {
		calls++
		if calls == 70 {
			cancel()
		}
		return true, nil
	}
	if _, _, err := executeAggregateBatches(env, plan, source, cols, ops); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if calls > 128 {
		t.Fatalf("cancellation delayed to %d predicates", calls)
	}
}

func TestAggregateBatchFallbackShapes(t *testing.T) {
	db := columnarFixture(t, 4099, 4)
	env := ExecEnv{ctx: t.Context(), db: db, tenant: "default"}
	for _, sql := range []string{
		`SELECT MIN(amount),SUM(amount) FROM batch_facts`,
		`SELECT COUNT(DISTINCT amount),SUM(amount) FROM batch_facts`,
		`SELECT SUM(amount+1) FROM batch_facts`,
		`SELECT COUNT(*) FROM batch_facts`,
	} {
		plan, ok, err := buildSimpleAggregatePlan(env, mustParse(sql).(*Select))
		if !ok || err != nil {
			t.Fatal(sql, ok, err)
		}
		source, _ := buildSimpleAggregateSourcePlan(plan)
		if _, _, ok := buildAggregateBatch(plan, source); ok {
			t.Fatal("unexpected batch", sql)
		}
	}
}
