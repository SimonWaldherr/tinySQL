package engine

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestColumnarMatchesRows(t *testing.T) {
	db := columnarFixture(t, 4099, 4)
	for _, sql := range []string{
		`SELECT * FROM batch_facts LIMIT 5`,
		`SELECT id,amount FROM batch_facts`,
		`SELECT id AS "Identifier",amount FROM batch_facts WHERE id>=1024 LIMIT 513 OFFSET 7`,
		`SELECT id,amount FROM batch_facts WHERE id<0`,
		`SELECT id,amount FROM batch_facts LIMIT 0`,
		`SELECT id,amount FROM batch_facts LIMIT 8 OFFSET 99999`,
		`SELECT id AS x,amount AS x FROM batch_facts LIMIT 3`,
		`SELECT COALESCE(amount,quantity),CASE WHEN id<5 THEN amount ELSE NULL END FROM batch_facts LIMIT 10`,
		`SELECT id+1,COALESCE(amount,quantity),CASE WHEN id<2050 THEN amount ELSE NULL END FROM batch_facts WHERE id>=17 LIMIT 2051 OFFSET 13`,
		`SELECT id,amount FROM batch_facts WHERE quantity=0`,
		`SELECT SUM(amount),AVG(amount),COUNT(*) FROM batch_facts`,
		`SELECT grp,SUM(amount) AS total FROM batch_facts GROUP BY grp HAVING SUM(amount)>100 ORDER BY total DESC LIMIT 3`,
		`SELECT id,amount FROM batch_facts ORDER BY amount DESC LIMIT 7 OFFSET 2`,
		`SELECT DISTINCT grp FROM batch_facts`,
		`WITH f AS (SELECT id,grp FROM batch_facts WHERE id<5) SELECT * FROM f`,
		`SELECT a.id,b.grp FROM batch_facts a JOIN batch_facts b ON a.id=b.id WHERE a.id<5`,
		`SELECT id FROM batch_facts WHERE id<3 UNION SELECT id FROM batch_facts WHERE id<5`,
		`SELECT 1 AS n,NULL AS missing`,
	} {
		t.Run(sql, func(t *testing.T) {
			stmt := mustParse(sql)
			rows, err := Execute(t.Context(), db, "default", stmt)
			if err != nil {
				t.Fatal(err)
			}
			got, err := ExecuteColumnar(t.Context(), db, "default", stmt)
			if err != nil {
				t.Fatal(err)
			}
			want := columnarFromRows(rows)
			if !reflect.DeepEqual(*got, want) {
				t.Fatalf("columnar differs for %s", sql)
			}
		})
	}
	// Changes must be visible when reusing a parsed statement; no column cache
	// may survive a mutation or retain stale row positions.
	stmt := mustParse(`SELECT id,amount FROM batch_facts WHERE id=100`)
	first, err := ExecuteColumnar(t.Context(), db, "default", stmt)
	if err != nil {
		t.Fatal(err)
	}
	execSQL(t, db, `UPDATE batch_facts SET amount=999 WHERE id=100`)
	execSQL(t, db, `CREATE INDEX compact_id ON batch_facts(id)`)
	second, err := ExecuteColumnar(t.Context(), db, "default", stmt)
	if err != nil {
		t.Fatal(second, err)
	}
	if value, ok := numeric(second.Values[1][0]); !ok || value != 999 {
		t.Fatal(second)
	}
	if first.Values[1][0] == second.Values[1][0] {
		t.Fatal("prior result changed")
	}
	execSQL(t, db, `DELETE FROM batch_facts WHERE id=100`)
	third, err := ExecuteColumnar(t.Context(), db, "default", stmt)
	if err != nil || third.RowCount != 0 {
		t.Fatal(third, err)
	}
}

func TestColumnarErrorsPermissionsAndAudit(t *testing.T) {
	db := columnarFixture(t, 4099, 4)
	for _, sql := range []string{
		`SELECT missing FROM batch_facts`,
		`SELECT missing FROM batch_facts LIMIT 0`,
		`SELECT 1/(id-id) FROM batch_facts WHERE id>=0 LIMIT 1 OFFSET 1`,
		`SELECT id,1/(id-257) FROM batch_facts WHERE id>=0`,
		`SELECT * FROM missing_table`,
	} {
		_, want := Execute(t.Context(), db, "default", mustParse(sql))
		got, err := ExecuteColumnar(t.Context(), db, "default", mustParse(sql))
		if want == nil || err == nil || err.Error() != want.Error() || got != nil {
			t.Fatalf("%s: %v / %v", sql, err, want)
		}
	}
	if _, err := ExecuteColumnar(t.Context(), db, "default", mustParse(`DELETE FROM batch_facts`)); err == nil {
		t.Fatal("DML accepted")
	}
	table, _ := db.Get("default", "batch_facts")
	if len(table.Rows) != 4099 {
		t.Fatal("DML executed")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := ExecuteColumnar(ctx, db, "default", mustParse(`SELECT id FROM batch_facts`)); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	audit := storage.NewAuditLog()
	db.AttachAuditLog(audit)
	if _, err := ExecuteColumnar(t.Context(), db, "default", mustParse(`SELECT id FROM batch_facts LIMIT 1`)); err != nil {
		t.Fatal(err)
	}
	if entries := audit.Entries(); len(entries) != 1 || !entries[0].Success {
		t.Fatal(entries)
	}
	execSQL(t, db, `CREATE ROLE admin_role`)
	execSQL(t, db, `GRANT ALL ON * TO ROLE admin_role`)
	execSQL(t, db, `CREATE USER admin WITH PASSWORD 'pw' ROLE admin_role`)
	if _, err := ExecuteColumnar(t.Context(), db, "default", mustParse(`SELECT id FROM batch_facts`)); err == nil {
		t.Fatal("permission check bypassed")
	}
	entries := audit.Entries()
	if entries[len(entries)-1].Success {
		t.Fatal("denied query not audited")
	}
}

func TestColumnarConcurrentQueries(t *testing.T) {
	db := columnarFixture(t, 4099, 4)
	stmt := mustParse(`SELECT id,amount FROM batch_facts WHERE id>=2000 LIMIT 99`)
	agg := mustParse(`SELECT SUM(amount),AVG(amount),COUNT(*) FROM batch_facts`)
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for range 10 {
				rs, err := ExecuteColumnar(t.Context(), db, "default", stmt)
				if err != nil || rs.RowCount != 99 {
					t.Error(err)
					return
				}
				rs.Values[0][0] = "caller-owned"
				if _, err := Execute(t.Context(), db, "default", agg); err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	wg.Go(func() {
		for range 10 {
			if _, err := Execute(t.Context(), db, "default", mustParse(`UPDATE batch_facts SET amount=quantity WHERE id=2001`)); err != nil {
				t.Error(err)
				return
			}
		}
	})
	wg.Wait()
}

func TestAggregateBatchExplain(t *testing.T) {
	db := columnarFixture(t, 4099, 4)
	rs := execSQL(t, db, `EXPLAIN SELECT SUM(amount),COUNT(*) FROM batch_facts`)
	found := false
	for _, row := range rs.Rows {
		for _, v := range row {
			if s, ok := v.(string); ok && strings.Contains(s, "BATCH AGGREGATE") {
				found = true
			}
		}
	}
	if !found {
		t.Fatal(rs)
	}
}

// Both APIs execute identical statements against the same fixture. In addition
// to the before/after engine suite, this isolates the opt-in result layout.
func BenchmarkColumnarOutput(b *testing.B) {
	db := columnarFixture(b, 131072, 32)
	for _, q := range []struct{ name, sql string }{
		{"all", `SELECT id,amount FROM batch_facts`},
		{"half", `SELECT id,amount FROM batch_facts WHERE id>=65536`},
		{"sparse", `SELECT id,amount FROM batch_facts WHERE id>=129761`},
		{"aggregate_fallback", `SELECT grp,SUM(amount) FROM batch_facts GROUP BY grp`},
	} {
		b.Run(q.name, func(b *testing.B) {
			stmt := mustParse(q.sql)
			b.Run("maps", func(b *testing.B) { runColumnarBenchmark(b, db, q.sql) })
			b.Run("columns", func(b *testing.B) {
				if _, err := ExecuteColumnar(b.Context(), db, "default", stmt); err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				for b.Loop() {
					if _, err := ExecuteColumnar(b.Context(), db, "default", stmt); err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}

func BenchmarkColumnarGrowth(b *testing.B) {
	db := columnarFixture(b, 131072, 32)
	for _, q := range []struct{ name, sql string }{
		{"all_match", `SELECT id,amount FROM batch_facts WHERE id>=0`},
		{"uniform", `SELECT id,amount FROM batch_facts WHERE quantity=0`},
		{"sparse_uniform", `SELECT id,amount FROM batch_facts WHERE id%100=0`},
		{"limited", `SELECT id,amount FROM batch_facts WHERE id>=65536 LIMIT 1025 OFFSET 17`},
	} {
		b.Run(q.name, func(b *testing.B) {
			stmt := mustParse(q.sql)
			if _, err := ExecuteColumnar(b.Context(), db, "default", stmt); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			for b.Loop() {
				if _, err := ExecuteColumnar(b.Context(), db, "default", stmt); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// Run one format per fresh process with TINYSQL_COLUMNAR_MEMORY=maps|columns.
// Baseline heap includes the row fixture. The forced-GC delta measures retained
// result memory; alloc_delta includes transient query allocations. Neither is
// mislabeled as peak RSS (record that separately with /usr/bin/time -l).
func TestColumnarMemory(t *testing.T) {
	format := os.Getenv("TINYSQL_COLUMNAR_MEMORY")
	if format == "" {
		t.Skip("opt-in memory measurement")
	}
	if format != "maps" && format != "columns" {
		t.Fatal("unknown format", format)
	}
	db := columnarFixture(t, 131072, 32)
	stmt := mustParse(`SELECT id,amount FROM batch_facts`)
	runtime.GC()
	var before, after, live runtime.MemStats
	runtime.ReadMemStats(&before)
	var result any
	var err error
	if format == "maps" {
		result, err = Execute(t.Context(), db, "default", stmt)
	} else {
		result, err = ExecuteColumnar(t.Context(), db, "default", stmt)
	}
	if err != nil {
		t.Fatal(err)
	}
	runtime.ReadMemStats(&after)
	runtime.GC()
	runtime.ReadMemStats(&live)
	runtime.KeepAlive(result)
	fmt.Printf("format=%s fixture_heap=%d alloc_delta=%d retained_delta=%d heap_after_query=%d\n", format, before.HeapAlloc, after.TotalAlloc-before.TotalAlloc, int64(live.HeapAlloc)-int64(before.HeapAlloc), after.HeapAlloc)
}

// Separate the benefit of typed batches from eliminating the old ungrouped
// path's empty-key hash lookup. This scalar reference has one accumulator and
// no grouping map, but still uses the existing per-row aggregate evaluator.
func BenchmarkAggregateKernelAblation(b *testing.B) {
	db := columnarFixture(b, 131072, 4)
	for _, q := range []struct{ name, sql string }{
		{"sum", `SELECT SUM(amount) FROM batch_facts`},
		{"multi", `SELECT SUM(amount),AVG(amount),COUNT(amount),SUM(quantity),COUNT(*) FROM batch_facts`},
	} {
		b.Run(q.name, func(b *testing.B) {
			env := ExecEnv{ctx: b.Context(), db: db, tenant: "default"}
			plan, _, _ := buildSimpleAggregatePlan(env, mustParse(q.sql).(*Select))
			source, _ := buildSimpleAggregateSourcePlan(plan)
			b.Run("scalar_no_group_map", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					state := newSimpleAggregateState(nil, len(plan.projs))
					for i, row := range plan.table.Rows {
						if i&63 == 0 {
							if err := checkCtx(env.ctx); err != nil {
								b.Fatal(err)
							}
						}
						match, err := evalRawWhere(source, row)
						if err != nil {
							b.Fatal(err)
						}
						if match {
							if err := accumulateSimpleAggregateState(env, source, row, state, plan.projs); err != nil {
								b.Fatal(err)
							}
						}
					}
					if _, err := finalizeSimpleAggregateResultSet(env, plan, []*simpleAggregateState{state}); err != nil {
						b.Fatal(err)
					}
				}
			})
			b.Run("typed_batches", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					cols, ops, _ := buildAggregateBatch(plan, source)
					if _, _, err := executeAggregateBatches(env, plan, source, cols, ops); err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}
