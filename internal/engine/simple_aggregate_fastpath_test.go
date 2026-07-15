// Tests for the raw-row GROUP BY fast path (executeSimpleAggregateFastPath
// in exec.go). Originally this fast path only handled COUNT; SUM, AVG, MIN,
// and MAX fell back to the general row-map evaluator. These tests lock in
// that SUM/AVG/MIN/MAX now produce the same results via the fast path,
// including the big.Rat accumulation path used for DECIMAL/MONEY values.
package engine

import (
	"context"
	"math/big"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func setupOrdersTable(t *testing.T) *storage.DB {
	t.Helper()
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE orders (customer_id INT, amount FLOAT64)`)
	rows := []string{
		`INSERT INTO orders VALUES (1, 10.0)`,
		`INSERT INTO orders VALUES (1, 20.0)`,
		`INSERT INTO orders VALUES (2, 5.0)`,
		`INSERT INTO orders VALUES (2, 15.0)`,
		`INSERT INTO orders VALUES (2, 25.0)`,
		`INSERT INTO orders VALUES (3, 100.0)`,
	}
	for _, r := range rows {
		execSQL(t, db, r)
	}
	return db
}

func TestAggregateFastPathSumAvgMinMax(t *testing.T) {
	db := setupOrdersTable(t)
	rs := execSQL(t, db, `
		SELECT customer_id,
		       COUNT(*) AS cnt, SUM(amount) AS total, AVG(amount) AS avg_amt,
		       MIN(amount) AS min_amt, MAX(amount) AS max_amt
		FROM orders
		GROUP BY customer_id
	`)
	if len(rs.Rows) != 3 {
		t.Fatalf("expected 3 groups, got %d: %+v", len(rs.Rows), rs.Rows)
	}
	byCustomer := map[int]Row{}
	for _, r := range rs.Rows {
		byCustomer[expectAsInt(t, r["customer_id"])] = r
	}

	want := map[int]struct {
		count    int
		sum, avg float64
		min, max float64
	}{
		1: {2, 30, 15, 10, 20},
		2: {3, 45, 15, 5, 25},
		3: {1, 100, 100, 100, 100},
	}
	for cust, w := range want {
		r, ok := byCustomer[cust]
		if !ok {
			t.Fatalf("missing group for customer %d: %+v", cust, rs.Rows)
		}
		expectInt(t, r["cnt"], w.count, "count")
		expectFloat(t, r["total"], w.sum, 1e-9, "sum")
		expectFloat(t, r["avg_amt"], w.avg, 1e-9, "avg")
		expectFloat(t, r["min_amt"], w.min, 1e-9, "min")
		expectFloat(t, r["max_amt"], w.max, 1e-9, "max")
	}
}

func TestAggregateFastPathSumOnlyNoCount(t *testing.T) {
	// The fast path previously required a COUNT projection to activate at
	// all; SUM/AVG/MIN/MAX alone (no COUNT) must also take the fast path.
	db := setupOrdersTable(t)
	rs := execSQL(t, db, `SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id`)
	if len(rs.Rows) != 3 {
		t.Fatalf("expected 3 groups, got %d", len(rs.Rows))
	}
	byCustomer := map[int]Row{}
	for _, r := range rs.Rows {
		byCustomer[expectAsInt(t, r["customer_id"])] = r
	}
	expectFloat(t, byCustomer[1]["total"], 30, 1e-9, "sum cust 1")
	expectFloat(t, byCustomer[2]["total"], 45, 1e-9, "sum cust 2")
	expectFloat(t, byCustomer[3]["total"], 100, 1e-9, "sum cust 3")
}

func TestAggregateFastPathMultipleGroupColumns(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE sales (region TEXT, category TEXT, amount FLOAT64)`)
	for _, sql := range []string{
		`INSERT INTO sales VALUES ('east', 'books', 10.0)`,
		`INSERT INTO sales VALUES ('east', 'books', 20.0)`,
		`INSERT INTO sales VALUES ('east', 'games', 5.0)`,
		`INSERT INTO sales VALUES ('west', 'books', 7.0)`,
	} {
		execSQL(t, db, sql)
	}

	stmt := mustParse(`
		SELECT region, category, COUNT(*) AS count, SUM(amount) AS total
		FROM sales
		GROUP BY region, category
	`).(*Select)
	plan, ok, err := buildSimpleAggregatePlan(ExecEnv{ctx: context.Background(), tenant: "default", db: db}, stmt)
	if err != nil || !ok {
		t.Fatalf("multi-column aggregate plan = %#v, ok=%v, err=%v", plan, ok, err)
	}
	if len(plan.groupCols) != 2 {
		t.Fatalf("group columns = %#v, want two", plan.groupCols)
	}

	rs := execSQL(t, db, `
		SELECT region, category, COUNT(*) AS count, SUM(amount) AS total
		FROM sales
		GROUP BY region, category
	`)
	byGroup := make(map[string]Row, len(rs.Rows))
	for _, row := range rs.Rows {
		byGroup[row["region"].(string)+"/"+row["category"].(string)] = row
	}
	if len(byGroup) != 3 {
		t.Fatalf("groups = %#v, want three", rs.Rows)
	}
	for group, want := range map[string]struct {
		count int
		total float64
	}{
		"east/books": {count: 2, total: 30},
		"east/games": {count: 1, total: 5},
		"west/books": {count: 1, total: 7},
	} {
		row, ok := byGroup[group]
		if !ok {
			t.Errorf("missing group %q in %#v", group, rs.Rows)
			continue
		}
		expectInt(t, row["count"], want.count, group+" count")
		expectFloat(t, row["total"], want.total, 1e-9, group+" total")
	}
}

func TestAggregateFastPathMultipleGroupColumnsWithSeparatorValues(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE pair_groups (a TEXT, b TEXT)`)
	// These pairs produced the same old delimiter-based composite key. SQL
	// text can contain the unit separator, so group keys must length-frame
	// each typed value rather than trusting a delimiter to be absent.
	execSQL(t, db, "INSERT INTO pair_groups VALUES ('x', 'y\x1fS:z')")
	execSQL(t, db, "INSERT INTO pair_groups VALUES ('x\x1fS:y', 'z')")

	rs := execSQL(t, db, `SELECT a, b, COUNT(*) AS count FROM pair_groups GROUP BY a, b`)
	if len(rs.Rows) != 2 {
		t.Fatalf("groups = %#v, want two distinct groups", rs.Rows)
	}
	groups := make(map[[2]string]Row, len(rs.Rows))
	for _, row := range rs.Rows {
		groups[[2]string{row["a"].(string), row["b"].(string)}] = row
	}
	for _, pair := range [][2]string{{"x", "y\x1fS:z"}, {"x\x1fS:y", "z"}} {
		row, ok := groups[pair]
		if !ok {
			t.Errorf("missing group %#v in %#v", pair, rs.Rows)
			continue
		}
		expectInt(t, row["count"], 1, "separator group count")
	}
}

func TestAggregateFastPathWithWhere(t *testing.T) {
	db := setupOrdersTable(t)
	rs := execSQL(t, db, `
		SELECT customer_id, SUM(amount) AS total
		FROM orders
		WHERE amount > 10
		GROUP BY customer_id
	`)
	byCustomer := map[int]Row{}
	for _, r := range rs.Rows {
		byCustomer[expectAsInt(t, r["customer_id"])] = r
	}
	// customer 1: only the amount=20 row qualifies (>10)
	expectFloat(t, byCustomer[1]["total"], 20, 1e-9, "sum cust 1 filtered")
	// customer 2: 15 and 25 qualify (5 excluded)
	expectFloat(t, byCustomer[2]["total"], 40, 1e-9, "sum cust 2 filtered")
	// customer 3: 100 qualifies
	expectFloat(t, byCustomer[3]["total"], 100, 1e-9, "sum cust 3 filtered")
}

// TestAggregateFastPathDecimalSum verifies that SUM/AVG over raw *big.Rat
// values (as produced by DECIMAL/MONEY columns elsewhere in the engine)
// still promotes to exact rational accumulation in the fast path, matching
// evalAggregateSumAvg's behavior in the general evaluator. tinySQL's SQL
// literals don't currently produce big.Rat values directly, so this test
// injects them by writing raw rows straight into the table.
func TestAggregateFastPathDecimalSum(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE ledger (grp INT, amt FLOAT64)`)
	table, err := db.Get("default", "ledger")
	if err != nil {
		t.Fatalf("get table: %v", err)
	}
	table.Rows = [][]any{
		{1, mustRat("10.10")},
		{1, mustRat("20.20")},
		{2, mustRat("5.05")},
	}

	rs := execSQL(t, db, `SELECT grp, SUM(amt) AS total, AVG(amt) AS avg_amt FROM ledger GROUP BY grp`)
	byGroup := map[int]Row{}
	for _, r := range rs.Rows {
		byGroup[expectAsInt(t, r["grp"])] = r
	}

	sum1, ok := byGroup[1]["total"].(*big.Rat)
	if !ok {
		t.Fatalf("expected *big.Rat sum for group 1, got %T (%v)", byGroup[1]["total"], byGroup[1]["total"])
	}
	if want := mustRat("30.30"); sum1.Cmp(want) != 0 {
		t.Errorf("group 1 sum: got %s, want %s", sum1.RatString(), want.RatString())
	}

	avg1, ok := byGroup[1]["avg_amt"].(*big.Rat)
	if !ok {
		t.Fatalf("expected *big.Rat avg for group 1, got %T", byGroup[1]["avg_amt"])
	}
	if want := new(big.Rat).Quo(mustRat("30.30"), big.NewRat(2, 1)); avg1.Cmp(want) != 0 {
		t.Errorf("group 1 avg: got %s, want %s", avg1.RatString(), want.RatString())
	}

	sum2, ok := byGroup[2]["total"].(*big.Rat)
	if !ok {
		t.Fatalf("expected *big.Rat sum for group 2, got %T", byGroup[2]["total"])
	}
	if want := mustRat("5.05"); sum2.Cmp(want) != 0 {
		t.Errorf("group 2 sum: got %s, want %s", sum2.RatString(), want.RatString())
	}
}

func mustRat(s string) *big.Rat {
	r := new(big.Rat)
	if _, ok := r.SetString(s); !ok {
		panic("bad decimal literal: " + s)
	}
	return r
}

func expectAsInt(t *testing.T, v any) int {
	t.Helper()
	switch x := v.(type) {
	case int:
		return x
	case int64:
		return int(x)
	case float64:
		return int(x)
	default:
		t.Fatalf("expected int-like value, got %T (%v)", v, v)
		return 0
	}
}
