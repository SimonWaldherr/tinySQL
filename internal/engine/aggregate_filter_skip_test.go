package engine

import (
	"reflect"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestAggregateUnfilteredAndConstraintCandidates(t *testing.T) {
	db := columnarFixture(t, 4099, 4)
	table, err := db.Get("default", "batch_facts")
	if err != nil {
		t.Fatal(err)
	}
	table.Cols[0].Constraint = storage.PrimaryKey
	for _, tc := range []struct{ projection, group string }{
		{"COUNT(amount) AS n, SUM(amount) AS total, AVG(amount) AS mean", ""},
		{"COUNT(amount) AS n, MIN(amount) AS lo, MAX(amount) AS hi", ""},
		{"grp, COUNT(amount) AS n, MIN(amount) AS lo", " GROUP BY grp"},
		{"grp, quantity, COUNT(amount) AS n, MAX(amount) AS hi", " GROUP BY grp, quantity"},
	} {
		query := "SELECT " + tc.projection + " FROM batch_facts"
		unfiltered := execSQL(t, db, query+tc.group)
		allRows := execSQL(t, db, query+" WHERE id >= 0"+tc.group)
		if !reflect.DeepEqual(unfiltered, allRows) {
			t.Fatalf("%s: unfiltered %v, filtered %v", query+tc.group, unfiltered, allRows)
		}
		for _, predicate := range []string{"id = 2050", "id = 2050 AND amount IS NULL", "id = -1"} {
			stmt := mustParse(query + " WHERE " + predicate + tc.group).(*Select)
			env := ExecEnv{ctx: t.Context(), db: db, tenant: "default"}
			plan, ok, err := buildSimpleAggregatePlan(env, stmt)
			if err != nil || !ok {
				t.Fatal(ok, err)
			}
			source, err := buildSimpleAggregateSourcePlan(plan)
			if err != nil {
				t.Fatal(err)
			}
			if predicate == "id = 2050" && !source.filterFullyCovered {
				t.Fatal("constraint lookup did not cover the predicate")
			}
			if predicate == "id = 2050 AND amount IS NULL" && source.filterFullyCovered {
				t.Fatal("residual NULL check was treated as covered")
			}
			got := execSQL(t, db, query+" WHERE "+predicate+tc.group)
			// Rechecking candidates remains a reference for both scalar grouping
			// paths, including empty candidates and residual predicates.
			source.filterFullyCovered = false
			var want *ResultSet
			if len(plan.groupCols) == 1 {
				want, _, err = executeSimpleSingleGroupAggregate(env, plan, source)
			} else {
				want, _, err = executeSimpleMultiGroupAggregate(env, plan, source)
			}
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("%s: got %v, rechecked %v, error %v", query+" WHERE "+predicate+tc.group, got, want, err)
			}
		}
	}
}
