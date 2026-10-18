package engine

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"testing"
)

func TestAggregateCountBatchesMatchScalar(t *testing.T) {
	db := columnarFixture(t, 4099, 4)
	table, _ := db.Get("default", "batch_facts")
	// COUNT accepts every non-NULL SQL value, including values that require
	// exact-decimal or nonnumeric fallback when used in SUM/AVG.
	for i, row := range table.Rows {
		row[2] = []any{nil, 2, int64(math.MaxInt64), 0.25, "text", false, big.NewRat(1, 3), []byte{}, map[string]any{}}[i%9]
		if i%11 == 0 {
			row[1] = nil
		}
	}
	for _, sql := range []string{
		`SELECT COUNT(amount) AS n FROM batch_facts`,
		`SELECT COUNT(amount) AS n,COUNT(amount) AS again,COUNT(*) AS total,COUNT(*) AS again_total,COUNT(quantity) AS q FROM batch_facts`,
		`SELECT COUNT(*) AS n FROM batch_facts WHERE id>=2048`,
		`SELECT COUNT(amount) AS n FROM batch_facts WHERE id%3=0`,
		`SELECT COUNT(amount) AS n FROM batch_facts WHERE id<0`,
		`SELECT COUNT(amount) AS n FROM batch_facts WHERE id>=4096`,
		`SELECT grp,COUNT(amount) AS n,COUNT(amount) AS again,COUNT(*) AS total FROM batch_facts GROUP BY grp`,
		`SELECT grp,quantity,COUNT(amount) AS n,COUNT(*) AS total FROM batch_facts GROUP BY grp,quantity`,
		`SELECT grp,COUNT(amount) AS n,COUNT(*) AS total FROM batch_facts WHERE id>=2048 GROUP BY grp HAVING COUNT(amount)>0 ORDER BY n DESC LIMIT 5 OFFSET 2`,
		`SELECT grp,COUNT(amount) AS n FROM batch_facts WHERE id<0 GROUP BY grp`,
	} {
		t.Run(sql, func(t *testing.T) {
			env := ExecEnv{ctx: t.Context(), db: db, tenant: "default"}
			plan, ok, err := buildSimpleAggregatePlan(env, mustParse(sql).(*Select))
			if err != nil || !ok {
				t.Fatal(ok, err)
			}
			source, err := buildSimpleAggregateSourcePlan(plan)
			if err != nil {
				t.Fatal(err)
			}
			cols, ops, ok := buildAggregateBatch(plan, source)
			if !ok {
				t.Fatal("count batch not selected")
			}
			got, _, err := executeAggregateBatches(env, plan, source, cols, ops)
			if err != nil {
				t.Fatal(err)
			}
			want, _, err := executeSimpleMultiGroupAggregate(env, plan, source)
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("batch %v; scalar %v; error %v", got, want, err)
			}
		})
	}
}

func TestAggregateCountBatchesCandidatesAndErrors(t *testing.T) {
	db := columnarFixture(t, 4099, 4)
	env := ExecEnv{ctx: t.Context(), db: db, tenant: "default"}
	plan, _, _ := buildSimpleAggregatePlan(env, mustParse(`SELECT COUNT(amount) AS n,COUNT(*) AS total FROM batch_facts`).(*Select))
	source, _ := buildSimpleAggregateSourcePlan(plan)
	source.rowIDs = make([]int, 2049)
	for i := range source.rowIDs {
		source.rowIDs[i] = i * 2
	}
	cols, ops, ok := buildAggregateBatch(plan, source)
	if !ok {
		t.Fatal("count batch not selected")
	}
	got, _, err := executeAggregateBatches(env, plan, source, cols, ops)
	want, _, wantErr := executeSimpleMultiGroupAggregate(env, plan, source)
	if err != nil || wantErr != nil || !reflect.DeepEqual(got, want) {
		t.Fatal(got, want, err, wantErr)
	}
	source.rowIDs[512] = -1
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
	env.ctx, source.filter = t.Context(), nil
	plan.table.Rows[2048] = []any{2048, "short"}
	_, _, err = executeAggregateBatches(env, plan, source, cols, ops)
	if err == nil || err.Error() != `column "amount" is out of range` {
		t.Fatalf("short row error: %v", err)
	}
}

func TestAggregateCountBatchesPreserveNumericHistory(t *testing.T) {
	for _, dataset := range []string{"overflow", "decimal_after_overflow", "null"} {
		t.Run(dataset, func(t *testing.T) {
			db := columnarFixture(t, 4099, 4)
			table, _ := db.Get("default", "batch_facts")
			for i, row := range table.Rows {
				row[2] = int64(math.MaxInt64)
				if i%7 == 0 || dataset == "null" {
					row[2] = nil
				}
			}
			if dataset == "decimal_after_overflow" {
				table.Rows[1025][2] = big.NewRat(1, 3)
			}
			for _, suffix := range []string{"", " GROUP BY grp"} {
				env := ExecEnv{ctx: t.Context(), db: db, tenant: "default"}
				plan, _, _ := buildSimpleAggregatePlan(env, mustParse(`SELECT COUNT(amount) AS n,SUM(amount) AS total,AVG(amount) AS mean,COUNT(amount) AS again,COUNT(*) AS rows FROM batch_facts`+suffix).(*Select))
				source, _ := buildSimpleAggregateSourcePlan(plan)
				cols, ops, ok := buildAggregateBatch(plan, source)
				if !ok {
					t.Fatal("numeric batch not selected")
				}
				got, _, err := executeAggregateBatches(env, plan, source, cols, ops)
				want, _, wantErr := executeSimpleMultiGroupAggregate(env, plan, source)
				if err != nil || wantErr != nil || !reflect.DeepEqual(got, want) {
					t.Fatalf("batch %v; scalar %v; errors %v/%v", got, want, err, wantErr)
				}
			}
		})
	}
}

func TestAggregateBatchesTextPairGroups(t *testing.T) {
	db := columnarFixture(t, 4099, 4)
	table, _ := db.Get("default", "batch_facts")
	pairs := [][2]any{{"a\x1fb", "c"}, {"a", "b\x1fc"}, {"", ""}, {nil, ""}, {"", nil}, {1, "x"}}
	for i, row := range table.Rows {
		row[1], row[3] = pairs[i%len(pairs)][0], pairs[i%len(pairs)][1]
	}
	for _, query := range []string{
		`SELECT grp,quantity,SUM(amount) AS total,AVG(amount) AS mean,COUNT(amount) AS n FROM batch_facts GROUP BY grp,quantity`,
		`SELECT grp,quantity,COUNT(amount) AS n,COUNT(*) AS total FROM batch_facts GROUP BY grp,quantity`,
	} {
		env := ExecEnv{ctx: t.Context(), db: db, tenant: "default"}
		plan, _, _ := buildSimpleAggregatePlan(env, mustParse(query).(*Select))
		source, _ := buildSimpleAggregateSourcePlan(plan)
		cols, ops, _ := buildAggregateBatch(plan, source)
		got, _, err := executeAggregateBatches(env, plan, source, cols, ops)
		want, _, wantErr := executeSimpleMultiGroupAggregate(env, plan, source)
		if err != nil || wantErr != nil || !reflect.DeepEqual(got, want) || len(got.Rows) != len(pairs) {
			t.Fatalf("%s: batch %v; scalar %v; errors %v/%v", query, got, want, err, wantErr)
		}
	}
}

func BenchmarkAggregateCountBatches(b *testing.B) {
	db := columnarFixture(b, 131072, 4)
	for _, query := range []struct{ name, sql string }{
		{"column", `SELECT COUNT(amount) FROM batch_facts`},
		{"multiple", `SELECT COUNT(*),COUNT(amount),COUNT(quantity),COUNT(amount) AS again FROM batch_facts`},
		{"filtered_star", `SELECT COUNT(*) FROM batch_facts WHERE id>=65536`},
		{"grouped_multiple", `SELECT grp,COUNT(*),COUNT(amount),COUNT(quantity),COUNT(amount) AS again FROM batch_facts GROUP BY grp`},
		{"grouped_star", `SELECT grp,COUNT(*) FROM batch_facts GROUP BY grp`},
	} {
		b.Run(query.name, func(b *testing.B) {
			stmt := mustParse(query.sql)
			b.ReportAllocs()
			for b.Loop() {
				if _, err := Execute(b.Context(), db, "default", stmt); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkAggregateCountKernel(b *testing.B) {
	db := columnarFixture(b, 131072, 4)
	for _, query := range []struct{ name, sql string }{
		{"column", `SELECT COUNT(amount) FROM batch_facts`},
		{"multiple", `SELECT COUNT(*),COUNT(amount),COUNT(quantity),COUNT(amount) AS again FROM batch_facts`},
		{"grouped_star", `SELECT grp,COUNT(*) FROM batch_facts GROUP BY grp`},
		{"grouped_column", `SELECT grp,COUNT(amount) FROM batch_facts GROUP BY grp`},
		{"grouped_pair", `SELECT grp,COUNT(*),COUNT(amount) FROM batch_facts GROUP BY grp`},
		{"grouped_multiple", `SELECT grp,COUNT(*),COUNT(amount),COUNT(quantity),COUNT(amount) AS again FROM batch_facts GROUP BY grp`},
	} {
		b.Run(query.name, func(b *testing.B) {
			env := ExecEnv{ctx: b.Context(), db: db, tenant: "default"}
			plan, _, _ := buildSimpleAggregatePlan(env, mustParse(query.sql).(*Select))
			source, _ := buildSimpleAggregateSourcePlan(plan)
			for _, batch := range []bool{false, true} {
				name := "scalar"
				if batch {
					name = "batches"
				}
				b.Run(name, func(b *testing.B) {
					b.ReportAllocs()
					for b.Loop() {
						var err error
						if batch {
							cols, ops, ok := buildAggregateBatch(plan, source)
							if !ok {
								b.Fatal("count batch not selected")
							}
							_, _, err = executeAggregateBatches(env, plan, source, cols, ops)
						} else {
							_, _, err = executeSimpleMultiGroupAggregate(env, plan, source)
						}
						if err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		})
	}
}

func BenchmarkAggregateBatchTextPairs(b *testing.B) {
	db := columnarFixture(b, 131072, 4)
	table, _ := db.Get("default", "batch_facts")
	for i, row := range table.Rows {
		row[1] = fmt.Sprintf("region-%d", i%16)
		row[3] = fmt.Sprintf("category-%d", i%64)
	}
	stmt := mustParse(`SELECT grp,quantity,SUM(amount) AS total,COUNT(*) AS n FROM batch_facts GROUP BY grp,quantity`)
	b.ReportAllocs()
	for b.Loop() {
		if _, err := Execute(b.Context(), db, "default", stmt); err != nil {
			b.Fatal(err)
		}
	}
}
