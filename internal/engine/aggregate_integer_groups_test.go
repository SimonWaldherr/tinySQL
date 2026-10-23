package engine

import (
	"fmt"
	"math/big"
	"reflect"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestAggregateIntegerPairGroupsMatchGeneral(t *testing.T) {
	maxInt := int(^uint(0) >> 1)
	minInt := -maxInt - 1
	keys := [][2]any{
		{0, 0}, {0, 1}, {1, 0}, {1, 1}, {-1, 1}, {1, -1},
		{minInt, maxInt}, {maxInt, minInt},
		{int64(1), 1}, {1, int64(1)}, {1.0, 1}, {1, 1.0},
		{nil, 1}, {1, nil}, {"1", 1}, {1, "1"}, {[]byte("1"), 1},
	}
	for _, fallbackAt := range []int{-1, 255, 256, 4096} {
		t.Run(fmt.Sprint(fallbackAt), func(t *testing.T) {
			db := storage.NewDB()
			t.Cleanup(func() { _ = db.Close() })
			table := storage.NewTable("integer_pairs", []storage.Column{
				{Name: "a", Type: storage.IntType}, {Name: "b", Type: storage.IntType},
				{Name: "v", Type: storage.IntType},
			}, false)
			for i := range 4103 {
				pair := keys[i%len(keys)]
				var value any = i
				if i%11 == 0 {
					value = nil
				}
				if i == fallbackAt {
					value = big.NewRat(1, 3)
				}
				table.Rows = append(table.Rows, []any{pair[0], pair[1], value})
			}
			if err := db.Put("default", table); err != nil {
				t.Fatal(err)
			}
			for _, groupBy := range []string{"a,b", "b,a", "a,a", "a,b,a"} {
				// HAVING keeps the derived-table oracle on the general encoded
				// grouping path rather than the materialized aggregate fast path.
				query := "SELECT COUNT(*) AS n,COUNT(v) AS nn,SUM(v) AS total,AVG(v) AS mean FROM integer_pairs GROUP BY " + groupBy
				general := strings.Replace(query, "FROM integer_pairs", "FROM (SELECT * FROM integer_pairs) AS integer_pairs", 1)
				want := execSQL(t, db, general+" HAVING COUNT(*)>=0")
				for _, sql := range []string{query, general} {
					got := execSQL(t, db, sql)
					if !reflect.DeepEqual(got, want) {
						t.Fatalf("%s: got %v, want %v", sql, got, want)
					}
				}
				env := ExecEnv{ctx: t.Context(), db: db, tenant: "default"}
				plan, ok, err := buildSimpleAggregatePlan(env, mustParse(query).(*Select))
				if err != nil || !ok {
					t.Fatal("aggregate plan", ok, err)
				}
				source, err := buildSimpleAggregateSourcePlan(plan)
				if err != nil {
					t.Fatal(err)
				}
				got, _, err := executeSimpleMultiGroupAggregate(env, plan, source)
				if err != nil || !reflect.DeepEqual(got, want) {
					t.Fatalf("scalar %s: got %v, want %v, error %v", query, got, want, err)
				}
			}
		})
	}
}

func BenchmarkAggregateIntegerPairs(b *testing.B) {
	for _, cardinality := range []int{350, 20000} {
		b.Run(fmt.Sprint(cardinality), func(b *testing.B) {
			db := storage.NewDB()
			b.Cleanup(func() { _ = db.Close() })
			table := storage.NewTable("integer_pairs", []storage.Column{
				{Name: "a", Type: storage.IntType}, {Name: "b", Type: storage.IntType},
				{Name: "v", Type: storage.IntType},
			}, false)
			for i := range 20000 {
				group := i % cardinality
				table.Rows = append(table.Rows, []any{group % 50, group / 50, i})
			}
			if err := db.Put("default", table); err != nil {
				b.Fatal(err)
			}
			for _, query := range []struct{ name, sql string }{
				{"Batch", "SELECT a,b,COUNT(*) AS n,SUM(v) AS total,AVG(v) AS mean FROM integer_pairs GROUP BY a,b"},
				{"Scalar", "SELECT a,b,COUNT(*) AS n,SUM(v) AS total,MIN(v) AS lo FROM integer_pairs GROUP BY a,b"},
			} {
				b.Run(query.name, func(b *testing.B) {
					runBench(b, db, query.sql)
				})
			}
		})
	}
}
