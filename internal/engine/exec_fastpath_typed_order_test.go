package engine

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// typedOrderFixture stores the Go types the INSERT path produces: int for INT,
// float64 for FLOAT and string for TEXT. NULLs, duplicates and signed zeros are
// common so that ties and NULL placement are exercised.
func typedOrderFixture(t *testing.T, rows int) *storage.DB {
	t.Helper()
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	execSQL(t, db, "CREATE TABLE typed_order (id INT, n INT, f FLOAT, s TEXT, k INT)")
	table, err := db.Get("default", "typed_order")
	if err != nil {
		t.Fatal(err)
	}
	rng := rand.New(rand.NewPCG(20260301, 7))
	for i := range rows {
		var n, f, s any = rng.IntN(9) - 4, float64(rng.IntN(7)) / 2, fmt.Sprintf("s%02d", rng.IntN(10))
		if i%7 == 3 {
			n = nil
		}
		if i%5 == 1 {
			f = nil
		}
		if i%11 == 4 {
			s = nil
		}
		if i%13 == 2 {
			f = math.Copysign(0, -1)
		}
		table.Rows = append(table.Rows, []any{i, n, f, s, rng.IntN(5)})
	}
	table.Version++
	execSQL(t, db, "CREATE INDEX typed_order_k ON typed_order(k)")
	return db
}

// typedOrderReference returns a statement with the same ordering that sorts
// through the generic path. The key is a select alias over arithmetic (or
// LOWER for text), which is not a direct column reference. Each non-NULL value
// keeps its ordering, so only the id sequence has to match.
func typedOrderReference(column string) string {
	if column == "s" {
		return "SELECT id, LOWER(s) AS sk FROM typed_order"
	}
	return "SELECT id, " + column + " + 0 AS sk FROM typed_order"
}

// typedOrderIDs extracts the id sequence of a result, dropping any sort key.
func typedOrderIDs(rs *ResultSet) []any {
	ids := make([]any, 0, len(rs.Rows))
	for _, row := range rs.Rows {
		ids = append(ids, row["id"])
	}
	return ids
}

func TestTypedOrderMatchesGenericPath(t *testing.T) {
	db := typedOrderFixture(t, 300)
	filters := []string{"", " WHERE k <> 2", " WHERE k = 2"}
	orders := []string{
		"n", "n DESC", "n NULLS FIRST", "n DESC NULLS LAST", "n DESC NULLS FIRST",
		"f", "f DESC", "f NULLS FIRST", "f DESC NULLS LAST",
		"s", "s DESC", "s NULLS FIRST", "s DESC NULLS LAST",
		"id DESC", "k", "k DESC NULLS FIRST",
	}
	pages := [][2]int{{-1, 0}, {1, 0}, {3, 0}, {5, 2}, {20, 0}, {20, 15}, {150, 0}, {290, 0}, {400, 0}, {2, 500}}
	for _, filter := range filters {
		for _, order := range orders {
			column, rest, hasRest := strings.Cut(order, " ")
			tail := ""
			if hasRest {
				tail = " " + rest
			}
			for _, page := range pages {
				limit := ""
				if page[0] >= 0 {
					limit = fmt.Sprintf(" LIMIT %d OFFSET %d", page[0], page[1])
				}
				typed := fmt.Sprintf("SELECT id FROM typed_order%s ORDER BY %s%s%s", filter, column, tail, limit)
				generic := typedOrderReference(column) + filter + " ORDER BY sk" + tail + limit
				got := execSQL(t, db, typed)
				want := execSQL(t, db, generic)
				if !reflect.DeepEqual(typedOrderIDs(got), typedOrderIDs(want)) {
					t.Fatalf("%s:\n got %v\nwant %v (%s)", typed, typedOrderIDs(got), typedOrderIDs(want), generic)
				}
			}
		}
	}
}

func TestTypedOrderHandlesSupportedColumns(t *testing.T) {
	db := typedOrderFixture(t, 300)
	env := ExecEnv{ctx: t.Context(), tenant: "default", db: db}
	for _, tc := range []struct {
		sql, reference string
		column         int
		rowIDs         bool
	}{
		{"SELECT id FROM typed_order ORDER BY n LIMIT 7", "SELECT id, n + 0 AS sk FROM typed_order ORDER BY sk LIMIT 7", 1, false},
		{"SELECT id FROM typed_order ORDER BY f DESC LIMIT 7", "SELECT id, f + 0 AS sk FROM typed_order ORDER BY sk DESC LIMIT 7", 2, false},
		{"SELECT id FROM typed_order ORDER BY s NULLS FIRST LIMIT 7", "SELECT id, LOWER(s) AS sk FROM typed_order ORDER BY sk NULLS FIRST LIMIT 7", 3, false},
		{"SELECT id FROM typed_order ORDER BY id DESC", "SELECT id, id + 0 AS sk FROM typed_order ORDER BY sk DESC", 0, false},
		{"SELECT id FROM typed_order WHERE k = 2 ORDER BY n LIMIT 7", "SELECT id, n + 0 AS sk FROM typed_order WHERE k = 2 ORDER BY sk LIMIT 7", 1, true},
	} {
		plan, ok, err := buildSimpleSelectPlan(env, mustParse(tc.sql).(*Select))
		if err != nil || !ok {
			t.Fatalf("%s: plan ok=%v err=%v", tc.sql, ok, err)
		}
		if (plan.rowIDs != nil) != tc.rowIDs {
			t.Fatalf("%s: rowIDs=%v, want %v", tc.sql, plan.rowIDs != nil, tc.rowIDs)
		}
		column, ok := simpleTypedOrderColumn(plan)
		if !ok || column != tc.column {
			t.Fatalf("%s: typed column = %d, %v; want %d", tc.sql, column, ok, tc.column)
		}
		candidates := len(simplePlanRows(plan))
		if plan.rowIDs != nil {
			candidates = len(plan.rowIDs)
		}
		keep := -1
		if plan.limit != nil {
			keep = boundedLimitRows(plan.limit, plan.offset, candidates)
		}
		rs, handled, err := executeTypedOrderedSelect(env, plan, simplePlanRows(plan), column, keep)
		if err != nil || !handled {
			t.Fatalf("%s: handled=%v err=%v; want the typed path", tc.sql, handled, err)
		}
		want := execSQL(t, db, tc.reference)
		if !reflect.DeepEqual(typedOrderIDs(rs), typedOrderIDs(want)) {
			t.Fatalf("%s: typed ids %v, generic ids %v", tc.sql, typedOrderIDs(rs), typedOrderIDs(want))
		}
	}
}

func TestTypedOrderDeclinesUnsupportedKeys(t *testing.T) {
	for _, tc := range []struct {
		name, column string
		rows         [][]any
	}{
		// Storage users may write any Go type; a column must share one type.
		{"mixed_integer_types", "INT", [][]any{{1}, {int64(2)}, {nil}}},
		{"mixed_int_and_float", "INT", [][]any{{1}, {float64(0.5)}, {2}}},
		{"nan", "FLOAT", [][]any{{1.0}, {math.NaN()}, {2.0}}},
		{"boolean", "BOOL", [][]any{{true}, {nil}, {false}}},
		{"all_null", "INT", [][]any{{nil}, {nil}, {nil}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := storage.NewDB()
			t.Cleanup(func() { _ = db.Close() })
			execSQL(t, db, "CREATE TABLE typed_mixed (v "+tc.column+")")
			table, err := db.Get("default", "typed_mixed")
			if err != nil {
				t.Fatal(err)
			}
			table.Rows = tc.rows
			table.Version++
			env := ExecEnv{ctx: t.Context(), tenant: "default", db: db}
			plan, ok, err := buildSimpleSelectPlan(env, mustParse("SELECT v FROM typed_mixed ORDER BY v LIMIT 2").(*Select))
			if err != nil || !ok {
				t.Fatalf("plan ok=%v err=%v", ok, err)
			}
			_, handled, err := executeTypedOrderedSelect(env, plan, simplePlanRows(plan), 0, 2)
			if err != nil || handled {
				t.Fatalf("handled=%v err=%v; want the generic path", handled, err)
			}
		})
	}
}

func TestTypedOrderFilterErrorsMatchGenericPath(t *testing.T) {
	db := typedOrderFixture(t, 300)
	for _, limit := range []string{" LIMIT 3", ""} {
		typed := "SELECT id FROM typed_order WHERE 10 / (id - 5) > 0 ORDER BY n" + limit
		generic := "SELECT id, n + 0 AS sk FROM typed_order WHERE 10 / (id - 5) > 0 ORDER BY sk" + limit
		_, errTyped := Execute(t.Context(), db, "default", mustParse(typed))
		_, errGeneric := Execute(t.Context(), db, "default", mustParse(generic))
		if errTyped == nil || errGeneric == nil {
			t.Fatalf("%q: expected a division error, got %v and %v", typed, errTyped, errGeneric)
		}
		if errTyped.Error() != errGeneric.Error() {
			t.Fatalf("%q: error %q, want %q", typed, errTyped, errGeneric)
		}
	}
}

func TestTypedOrderHonorsCancellation(t *testing.T) {
	db := typedOrderFixture(t, 300)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	env := ExecEnv{ctx: ctx, tenant: "default", db: db}
	plan, ok, err := buildSimpleSelectPlan(env, mustParse("SELECT id FROM typed_order ORDER BY n LIMIT 3").(*Select))
	if err != nil || !ok {
		t.Fatalf("plan ok=%v err=%v", ok, err)
	}
	if _, _, err := executeTypedOrderedSelect(env, plan, simplePlanRows(plan), 1, 3); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled typed order returned %v", err)
	}
}

// typedOrderRefCompare is the generic comparator applied to boxed keys, with
// scan position as the final tie-break. The typed comparator must agree with it.
func typedOrderRefCompare[K typedOrderKey](a, b typedOrderItem[K], desc bool, nulls NullOrder) int {
	var av, bv any
	if !a.null {
		av = a.key
	}
	if !b.null {
		bv = b.key
	}
	if c := compareOrderedValueNulls(av, bv, desc, nulls); c != 0 {
		return c
	}
	switch {
	case a.pos < b.pos:
		return -1
	case a.pos > b.pos:
		return 1
	}
	return 0
}

// checkTypedOrderAgainstGeneric verifies the bounded heap and the ordering
// comparator for one key type against the generic comparator. Keys are drawn
// from a small domain so that ties, NULLs and each direction are frequent.
func checkTypedOrderAgainstGeneric[K typedOrderKey](t *testing.T, rng *rand.Rand, draw func() K) {
	t.Helper()
	for _, desc := range []bool{false, true} {
		for _, nulls := range []NullOrder{NullsDefault, NullsFirst, NullsLast} {
			nullsFirst := nulls == NullsFirst
			for _, size := range []int{1, 2, 9, 64, 257} {
				items := make([]typedOrderItem[K], size)
				for i := range items {
					items[i] = typedOrderItem[K]{pos: int32(i), row: int32(i), null: rng.IntN(6) == 0}
					if !items[i].null {
						items[i].key = draw()
					}
				}
				if size <= 64 {
					for i := range items {
						for j := range items {
							if i == j {
								continue
							}
							want := typedOrderRefCompare(items[i], items[j], desc, nulls) > 0
							if got := typedItemWorse(&items[i], &items[j], desc, nullsFirst); got != want {
								t.Fatalf("K=%T desc=%v nulls=%v: worse(%+v, %+v)=%v, want %v", items[i].key, desc, nulls, items[i], items[j], got, want)
							}
						}
					}
				}
				ordered := slices.Clone(items)
				slices.SortFunc(ordered, func(a, b typedOrderItem[K]) int {
					return typedOrderRefCompare(a, b, desc, nulls)
				})
				for _, keep := range []int{1, 2, 5, 40, size} {
					heap := typedOrderHeap[K]{desc: desc, nullsFirst: nullsFirst}
					for _, item := range items {
						heap.pushBounded(item, keep)
					}
					got := slices.Clone(heap.items)
					slices.SortFunc(got, func(a, b typedOrderItem[K]) int {
						return typedOrderSortCmp(a, b, desc, nullsFirst)
					})
					if want := ordered[:min(keep, size)]; !reflect.DeepEqual(got, want) {
						t.Fatalf("K=%T desc=%v nulls=%v size=%d keep=%d:\n got %+v\nwant %+v", items[0].key, desc, nulls, size, keep, got, want)
					}
				}
			}
		}
	}
}

func TestTypedOrderHeapAndComparatorMatchGenericOrdering(t *testing.T) {
	rng := rand.New(rand.NewPCG(11, 13))
	checkTypedOrderAgainstGeneric(t, rng, func() int { return rng.IntN(7) - 3 })
	checkTypedOrderAgainstGeneric(t, rng, func() int64 { return int64(rng.IntN(5)) })
	checkTypedOrderAgainstGeneric(t, rng, func() float64 {
		return []float64{-1.5, math.Copysign(0, -1), 0, 0.5, 2, 2}[rng.IntN(6)]
	})
	checkTypedOrderAgainstGeneric(t, rng, func() string { return fmt.Sprintf("k%d", rng.IntN(4)) })
}
