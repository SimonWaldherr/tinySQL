package engine

import (
	"fmt"
	"math/big"
	"reflect"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func BenchmarkBatchTextGroups(b *testing.B) {
	for _, kind := range []string{"ShortText", "LongText", "UniqueText", "IntegerControl", "MixedControl"} {
		b.Run(kind, func(b *testing.B) {
			db := storage.NewDB()
			b.Cleanup(func() { _ = db.Close() })
			table := storage.NewTable("pairs", []storage.Column{{Name: "a", Type: storage.TextType}, {Name: "b", Type: storage.TextType}, {Name: "v", Type: storage.IntType}}, false)
			table.Rows = make([][]any, 20000)
			for i := range table.Rows {
				var a, c any = fmt.Sprint(i % 50), fmt.Sprint(i % 7)
				switch kind {
				case "LongText":
					a, c = strings.Repeat("left", 16)+a.(string), strings.Repeat("right", 16)+c.(string)
				case "UniqueText":
					a = fmt.Sprintf("unique-dimension-%d", i)
				case "IntegerControl":
					a, c = i%50, i%7
				case "MixedControl":
					a = i % 50
				}
				table.Rows[i] = []any{a, c, i}
			}
			if err := db.Put("default", table); err != nil {
				b.Fatal(err)
			}
			runBench(b, db, "SELECT a,b,COUNT(*) AS n,SUM(v) AS s,AVG(v) AS mean FROM pairs GROUP BY a,b")
		})
	}
}

func TestBatchTextGroupsMatchScalar(t *testing.T) {
	keys := [][2]any{
		{"", ""}, {"a", "b"}, {"a\x1fb", "c"}, {"a", "b\x1fc"},
		{"é漢", "🙂"}, {"\xff", "\x00"}, {nil, "b"}, {"a", nil},
		{1, "b"}, {"1", "b"}, {int64(1), "b"}, {1.0, "b"},
		{[]byte("a"), "b"}, {true, false},
	}
	for _, fallbackAt := range []int{-1, 0, 255, 256, 1025, 4096} {
		t.Run(fmt.Sprint(fallbackAt), func(t *testing.T) {
			db := storage.NewDB()
			t.Cleanup(func() { _ = db.Close() })
			table := storage.NewTable("pairs", []storage.Column{{Name: "a", Type: storage.TextType}, {Name: "b", Type: storage.TextType}, {Name: "v", Type: storage.IntType}}, false)
			for i := range 4099 {
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
			env := ExecEnv{ctx: t.Context(), db: db, tenant: "default"}
			for _, query := range []string{
				"SELECT a,b,COUNT(*) AS n,COUNT(v) AS nn,SUM(v) AS s,AVG(v) AS mean FROM pairs GROUP BY a,b",
				"SELECT a,b,COUNT(*) AS n,SUM(v) AS s FROM pairs GROUP BY a,b HAVING COUNT(*) > 1 ORDER BY s LIMIT 7 OFFSET 2",
				"SELECT a,a AS again,COUNT(*) AS n,SUM(v) AS s FROM pairs GROUP BY a,a",
			} {
				plan, ok, err := buildSimpleAggregatePlan(env, mustParse(query).(*Select))
				if !ok || err != nil {
					t.Fatal("plan", query, err)
				}
				source, err := buildSimpleAggregateSourcePlan(plan)
				if err != nil {
					t.Fatal(err)
				}
				cols, ops, ok := buildAggregateBatch(plan, source)
				if !ok {
					t.Fatal("batch declined", query)
				}
				got, _, err := executeAggregateBatches(env, plan, source, cols, ops)
				if err != nil {
					t.Fatal(err)
				}
				want, _, err := executeSimpleMultiGroupAggregate(env, plan, source)
				if err != nil || !reflect.DeepEqual(got, want) {
					t.Fatalf("%s: batch %v, scalar %v, error %v", query, got, want, err)
				}
			}
		})
	}
}
