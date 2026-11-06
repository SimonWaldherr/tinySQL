package engine

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestAggregateExpressionsMatchGeneral(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	execSQL(t, db, `CREATE TABLE expr_facts (day TEXT, category INT, amount INT, denominator INT)`)
	execSQL(t, db, `INSERT INTO expr_facts VALUES ('2024-01-01', 1, 10, 2), ('2024-02-15', 1, 20, 4), ('2023-01-01', 2, NULL, 1), (NULL, 2, 3, 0)`)
	queries := []string{
		`SELECT YEAR(day) AS y, COUNT(*) AS n, SUM(amount) AS total FROM expr_facts GROUP BY YEAR(day) ORDER BY y`,
		`SELECT DATE_TRUNC('MONTH', day) AS m, category, COUNT(*) AS n FROM expr_facts GROUP BY DATE_TRUNC('MONTH', day), category ORDER BY m, category`,
		`SELECT category + 1 AS k, SUM(amount) AS s FROM expr_facts GROUP BY category + 1 ORDER BY k`,
		`SELECT category, SUM(amount) / COUNT(*) AS ratio, SUM(amount) + SUM(amount) AS twice FROM expr_facts GROUP BY category HAVING SUM(amount) > 5 ORDER BY ratio DESC LIMIT 1`,
		`SELECT SUM(amount) / COUNT(*) AS ratio, COUNT(*) + 1 AS n FROM expr_facts WHERE category = 9`,
		`SELECT category, SUM(amount) FILTER (WHERE denominator > 0) / COUNT(*) AS ratio FROM expr_facts GROUP BY category ORDER BY category`,
		`SELECT category, -SUM(amount) AS neg, SUM(amount) IS NULL AS missing FROM expr_facts GROUP BY category ORDER BY category`,
	}
	env := ExecEnv{ctx: t.Context(), db: db, tenant: "default"}
	for _, query := range queries {
		t.Run(query, func(t *testing.T) {
			stmt := mustParse(query).(*Select)
			if _, ok, err := buildSimpleAggregatePlan(env, stmt); !ok || err != nil {
				t.Fatalf("fast path: %v %v", ok, err)
			}
			got, err := Execute(t.Context(), db, "default", stmt)
			if err != nil {
				t.Fatal(err)
			}
			general := strings.Replace(query, "FROM expr_facts", "FROM (SELECT * FROM expr_facts) AS source", 1)
			want, err := Execute(t.Context(), db, "default", mustParse(general))
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("got %#v, want %#v", got, want)
			}
		})
	}
}

func TestAggregateExpressionErrorsAndFallback(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	execSQL(t, db, `CREATE TABLE expr_errors (g INT, v INT)`)
	execSQL(t, db, `INSERT INTO expr_errors VALUES (1, 1), (2, 0)`)
	env := ExecEnv{ctx: t.Context(), db: db, tenant: "default"}
	for _, query := range []string{
		`SELECT g, 1 / SUM(v) AS x FROM expr_errors GROUP BY g LIMIT 1`,
		`SELECT 1 / v AS k, COUNT(*) AS n FROM expr_errors GROUP BY 1 / v`,
	} {
		stmt := mustParse(query).(*Select)
		if _, ok, err := buildSimpleAggregatePlan(env, stmt); !ok || err != nil {
			t.Fatalf("%s: plan %v %v", query, ok, err)
		}
		_, got := Execute(t.Context(), db, "default", stmt)
		_, want := Execute(t.Context(), db, "default", mustParse(strings.Replace(query, "FROM expr_errors", "FROM (SELECT * FROM expr_errors) AS source", 1)))
		if got == nil || want == nil || got.Error() != want.Error() {
			t.Fatalf("%s: got %v, want %v", query, got, want)
		}
	}
	for _, query := range []string{
		`SELECT RANDOM() AS r, COUNT(*) FROM expr_errors GROUP BY RANDOM()`,
		`SELECT COALESCE(SUM(v), 0) FROM expr_errors`,
		`SELECT 1 = 1 OR SUM(1 / v) > 0 FROM expr_errors`,
		`SELECT g, SUM(1 / v) + 1 FROM expr_errors GROUP BY g HAVING COUNT(*) > 100`,
		`SELECT g + 1, SUM(1 / v) FROM expr_errors GROUP BY g + 1`,
	} {
		if _, ok, _ := buildSimpleAggregatePlan(env, mustParse(query).(*Select)); ok {
			t.Errorf("unexpected fast path: %s", query)
		}
	}
}

func TestAggregateExpressionBatchMatchesGeneral(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	table := storage.NewTable("expr_batch", []storage.Column{{Name: "g", Type: storage.IntType}, {Name: "v", Type: storage.IntType}}, false)
	for i := range 4097 {
		table.Rows = append(table.Rows, []any{i % 7, i % 13})
	}
	if err := db.Put("default", table); err != nil {
		t.Fatal(err)
	}
	query := `SELECT g, SUM(v) / COUNT(*) AS x FROM expr_batch GROUP BY g ORDER BY g`
	got, err := Execute(t.Context(), db, "default", mustParse(query))
	if err != nil {
		t.Fatal(err)
	}
	want, err := Execute(t.Context(), db, "default", mustParse(strings.Replace(query, "FROM expr_batch", "FROM (SELECT * FROM expr_batch) AS source", 1)))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatal(fmt.Sprintf("got %#v, want %#v", got, want))
	}
}

func TestAggregateExpressionDecimal(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	execSQL(t, db, `CREATE TABLE expr_decimal (g INT, v DECIMAL)`)
	execSQL(t, db, `INSERT INTO expr_decimal VALUES (1, 1.25), (1, 2.50), (2, NULL)`)
	for _, query := range []string{
		`SELECT g, -SUM(v) AS x, SUM(v) / COUNT(*) AS y FROM expr_decimal GROUP BY g ORDER BY g`,
		`SELECT g, SUM(v) + SUM(v) AS x FROM expr_decimal GROUP BY g HAVING -SUM(v) < 0 ORDER BY g`,
	} {
		env := ExecEnv{ctx: t.Context(), db: db, tenant: "default"}
		if _, ok, err := buildSimpleAggregatePlan(env, mustParse(query).(*Select)); !ok || err != nil {
			t.Fatalf("plan: %v %v", ok, err)
		}
		got, err := Execute(t.Context(), db, "default", mustParse(query))
		if err != nil {
			t.Fatal(err)
		}
		want, err := Execute(t.Context(), db, "default", mustParse(strings.Replace(query, "FROM expr_decimal", "FROM (SELECT * FROM expr_decimal) AS source", 1)))
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("%s: got %#v want %#v", query, got, want)
		}
	}
}

func TestAggregateDateGroupCacheIsBounded(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	table := storage.NewTable("dates_cache", []storage.Column{{Name: "day", Type: storage.TextType}}, false)
	for i := 0; i < 1100; i++ {
		table.Rows = append(table.Rows, []any{fmt.Sprintf("%04d-01-01", i+1000)})
	}
	if err := db.Put("default", table); err != nil {
		t.Fatal(err)
	}
	stmt := mustParse(`SELECT YEAR(day) AS y, COUNT(*) AS n FROM dates_cache GROUP BY YEAR(day)`).(*Select)
	env := ExecEnv{ctx: t.Context(), db: db, tenant: "default"}
	plan, ok, err := buildSimpleAggregatePlan(env, stmt)
	if !ok || err != nil {
		t.Fatalf("plan: %v %v", ok, err)
	}
	rawPlan, err := buildSimpleAggregateSourcePlan(plan)
	if err != nil {
		t.Fatal(err)
	}
	groups := newAggregateGroups(plan.projs, len(plan.groupCols))
	values := make([]any, len(plan.groupExprs))
	cache := make([]map[string]any, len(plan.groupExprs))
	for _, raw := range table.Rows {
		if _, err := simpleAggregateGroupState(groupsStateArgs{groups: &groups, plan: plan, rawPlan: rawPlan, raw: raw, values: values, cache: cache}); err != nil {
			t.Fatal(err)
		}
	}
	if len(cache[0]) != 1024 {
		t.Fatalf("cache size %d, want 1024", len(cache[0]))
	}
	if len(groups.order) != 1100 {
		t.Fatalf("groups %d, want 1100", len(groups.order))
	}
}
