package engine

import (
	"context"
	"fmt"
	"math/big"
	"reflect"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func filteredAggregateFixture(tb testing.TB, n int, indexed bool) *storage.DB {
	tb.Helper()
	db := storage.NewDB()
	table := storage.NewTable("facts", []storage.Column{
		{Name: "id", Type: storage.IntType, Constraint: storage.PrimaryKey},
		{Name: "region", Type: storage.TextType}, {Name: "category", Type: storage.TextType},
		{Name: "time", Type: storage.IntType}, {Name: "amount", Type: storage.FloatType},
		{Name: "lat", Type: storage.FloatType}, {Name: "lon", Type: storage.FloatType},
	}, false)
	for i := 0; i < n; i++ {
		var amount any = float64(i % 100)
		if i%17 == 0 {
			amount = nil
		}
		table.Rows = append(table.Rows, []any{i, fmt.Sprintf("r%d", i%10), fmt.Sprintf("c%d", i%3), i, amount, 48 + float64(i)/10000, 11 + float64(i%10)/10})
	}
	if indexed {
		for _, cols := range [][]string{{"region", "time"}, {"lat", "lon"}} {
			if err := table.CreateSecondaryIndex("idx_"+cols[0], cols, false); err != nil {
				tb.Fatal(err)
			}
		}
	}
	if err := db.Put("default", table); err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { db.Close() })
	return db
}

func TestFilteredAggregatesMatchFullScan(t *testing.T) {
	indexed := filteredAggregateFixture(t, 600, true)
	scanned := filteredAggregateFixture(t, 600, false)
	queries := []string{
		`SELECT COUNT(*) AS n,SUM(amount) AS total,AVG(amount) AS mean,MIN(amount) AS low,MAX(amount) AS high FROM facts WHERE region='r1' AND time>=100 AND time<400`,
		`SELECT category,COUNT(amount) AS n,SUM(amount) AS total FROM facts WHERE region='r1' AND time>=100 AND time<400 GROUP BY category HAVING SUM(amount)>0 ORDER BY total DESC LIMIT 2 OFFSET 1`,
		`SELECT region,category,COUNT(*) AS n FROM facts WHERE lat>=48.01 AND lat<48.04 AND lon<=11.5 GROUP BY region,category ORDER BY region,category`,
		`SELECT region,COUNT(*) AS n FROM facts WHERE region='missing' GROUP BY region`,
		`SELECT COUNT(*) AS n,SUM(amount) AS total FROM facts WHERE region='missing'`,
		`SELECT COUNT(*) AS n,SUM(amount) AS total FROM facts WHERE lat>90.0`,
		`SELECT COUNT(*) AS n FROM facts WHERE time<0 OR category='c1'`,
		`SELECT category,COUNT(*) AS n FROM facts WHERE region='r2' GROUP BY category`,
		`SELECT COUNT(*) AS n FROM facts WHERE id=123`,
		`SELECT COUNT(*) AS n FROM facts WHERE id=9999`,
		`SELECT f.category,SUM(f.amount*2) AS total FROM facts f WHERE f.region='r1' AND f.time>=100 AND f.time<150 GROUP BY f.category ORDER BY f.category`,
	}
	for _, q := range queries {
		got := rangeExec(t, indexed, q)
		want := rangeExec(t, scanned, q)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("%s\ngot %v\nwant %v", q, got, want)
		}
	}
	// The same parsed statement must resolve candidates again after mutations.
	q := mustParse(`SELECT SUM(amount) AS total FROM facts WHERE region='r1' AND time>=100 AND time<150`)
	for _, db := range []*storage.DB{indexed, scanned} {
		if _, err := Execute(context.Background(), db, "default", q); err != nil {
			t.Fatal(err)
		}
		rangeExec(t, db, `UPDATE facts SET region='r1',amount=1000 WHERE id=102`)
		rangeExec(t, db, `DELETE FROM facts WHERE id=111`)
	}
	got, err := Execute(context.Background(), indexed, "default", q)
	if err != nil {
		t.Fatal(err)
	}
	want, err := Execute(context.Background(), scanned, "default", q)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("changed data mismatch: %v %v", got, want)
	}
}

func TestFilteredDecimalAggregate(t *testing.T) {
	db := storage.NewDB()
	defer db.Close()
	rangeExec(t, db, `CREATE TABLE amounts (region TEXT, time INT, amount DECIMAL)`)
	// Rational inputs enter through the Go value API; SQL numeric literals
	// currently follow the engine's floating-point literal semantics.
	table, err := db.Get("default", "amounts")
	if err != nil {
		t.Fatal(err)
	}
	table.Rows = [][]any{{"a", 1, big.NewRat(1, 10)}, {"a", 2, big.NewRat(2, 10)}, {"b", 2, big.NewRat(10, 1)}}
	rangeExec(t, db, `CREATE INDEX amount_window ON amounts(region,time)`)
	rs := rangeExec(t, db, `SELECT SUM(amount) AS total FROM amounts WHERE region='a' AND time>=1 AND time<=2`)
	got, ok := rs.Rows[0]["total"].(*big.Rat)
	if !ok || got.Cmp(big.NewRat(3, 10)) != 0 {
		t.Fatalf("inexact sum: %v", rs.Rows)
	}
}

func BenchmarkSelectiveAggregates(b *testing.B) {
	for _, indexed := range []bool{false, true} {
		db := filteredAggregateFixture(b, 50000, indexed)
		for _, query := range []struct{ name, sql string }{
			{"time", `SELECT category,SUM(amount) AS total,COUNT(*) AS n FROM facts WHERE region='r1' AND time>=20000 AND time<20100 GROUP BY category`},
			{"spatial", `SELECT category,COUNT(*) AS n FROM facts WHERE lat>=50.0 AND lat<50.01 AND lon<11.5 GROUP BY category`},
		} {
			b.Run(fmt.Sprintf("%s/indexed%v", query.name, indexed), func(b *testing.B) {
				stmt := mustParse(query.sql)
				if _, err := Execute(context.Background(), db, "default", stmt); err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				for b.Loop() {
					if _, err := Execute(context.Background(), db, "default", stmt); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

func TestFilteredAggregateExplain(t *testing.T) {
	db := filteredAggregateFixture(t, 600, true)
	q := mustParse(`SELECT category,SUM(amount) AS total FROM facts WHERE region='r1' AND time>=100 AND time<200 GROUP BY category`).(*Select)
	env := ExecEnv{db: db, tenant: "default", ctx: context.Background()}
	aggregate, ok, err := buildSimpleAggregatePlan(env, q)
	if err != nil || !ok {
		t.Fatal(ok, err)
	}
	source, err := buildSimpleAggregateSourcePlan(aggregate)
	if err != nil || source.scanType != "INDEX RANGE SCAN" || len(source.rowIDs) != 10 {
		t.Fatalf("source: %+v %v", source, err)
	}
	explained := rangeExec(t, db, `EXPLAIN SELECT category,SUM(amount) AS total FROM facts WHERE region='r1' AND time>=100 AND time<200 GROUP BY category`)
	if !strings.Contains(fmt.Sprint(explained.Rows), "INDEX RANGE SCAN") || !strings.Contains(fmt.Sprint(explained.Rows), "estimated_rows=10") {
		t.Fatal(explained.Rows)
	}
	missing := mustParse(`SELECT COUNT(*) AS n FROM facts WHERE region='missing'`).(*Select)
	aggregate, ok, err = buildSimpleAggregatePlan(env, missing)
	if err != nil || !ok {
		t.Fatal(ok, err)
	}
	source, err = buildSimpleAggregateSourcePlan(aggregate)
	if err != nil || source.rowIDs == nil || len(source.rowIDs) != 0 {
		t.Fatalf("empty seek became full scan: %+v %v", source, err)
	}
}
