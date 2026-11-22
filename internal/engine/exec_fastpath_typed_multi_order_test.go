package engine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"reflect"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// drawTypedOrderValue returns a key from a small domain of one family, with
// ties, NULLs, signed zeros and infinities, for differential checks.
func drawTypedOrderValue(rng *rand.Rand, family typedOrderFamily) any {
	if rng.IntN(5) == 0 {
		return nil
	}
	switch family {
	case typedFamilyInteger:
		if rng.IntN(2) == 0 {
			return rng.IntN(7) - 3
		}
		return int64(rng.IntN(7) - 3)
	case typedFamilyFloat:
		return []float64{-1.5, math.Copysign(0, -1), 0, 0.5, 2, math.Inf(1), math.Inf(-1)}[rng.IntN(7)]
	default:
		return []string{"", "A", "a", "a\x00", "a\x00b", "ab", "b", "\x01", "\xff"}[rng.IntN(9)]
	}
}

// TestTypedMultiOrderEncodingMatchesGenericOrdering compares the byte order of
// two encoded key tuples with the generic per-key comparison, then scan position.
func TestTypedMultiOrderEncodingMatchesGenericOrdering(t *testing.T) {
	rng := rand.New(rand.NewPCG(17, 19))
	families := []typedOrderFamily{typedFamilyInteger, typedFamilyFloat, typedFamilyText}
	for round := 0; round < 20000; round++ {
		width := 1 + rng.IntN(3)
		order := typedMultiOrder{
			columns:    make([]int, width),
			desc:       make([]bool, width),
			nullsFirst: make([]bool, width),
		}
		nulls := make([]NullOrder, width)
		columnFamily := make([]typedOrderFamily, width)
		for k := range width {
			order.columns[k] = k
			order.desc[k] = rng.IntN(2) == 0
			order.nullsFirst[k] = rng.IntN(2) == 0
			nulls[k] = NullsLast
			if order.nullsFirst[k] {
				nulls[k] = NullsFirst
			} else if rng.IntN(2) == 0 {
				// NULLS LAST and the default placement must order identically.
				nulls[k] = NullsDefault
			}
			columnFamily[k] = families[rng.IntN(len(families))]
		}
		rows := [2][]any{make([]any, width), make([]any, width)}
		for r := range rows {
			for k := range width {
				rows[r][k] = drawTypedOrderValue(rng, columnFamily[k])
			}
		}
		positions := [2]int32{0, 1}
		if rng.IntN(2) == 0 {
			positions = [2]int32{1, 0}
		}

		encodedA := appendTypedMultiOrderKey(nil, rows[0], &order, positions[0])
		encodedB := appendTypedMultiOrderKey(nil, rows[1], &order, positions[1])
		got := bytes.Compare(encodedA, encodedB)

		want := 0
		for k := range width {
			if c := compareOrderedValueNulls(rows[0][k], rows[1][k], order.desc[k], nulls[k]); c != 0 {
				want = c
				break
			}
		}
		if want == 0 {
			switch {
			case positions[0] < positions[1]:
				want = -1
			case positions[0] > positions[1]:
				want = 1
			}
		}
		if got != want {
			t.Fatalf("round %d: rows %v vs %v (desc=%v nullsFirst=%v nulls=%v): encoded %d, generic %d",
				round, rows[0], rows[1], order.desc, order.nullsFirst, nulls, got, want)
		}
	}
}

// typedMultiOrderTerms builds the typed statement and the generic reference for
// one multi-column ORDER BY. The reference orders by select aliases over
// arithmetic or LOWER, which the generic path sorts.
func typedMultiOrderTerms(filter string, terms []string) (typed, reference string) {
	var selects, sortTerms []string
	for i, term := range terms {
		column, rest, hasRest := strings.Cut(term, " ")
		expr := column + " + 0"
		if column == "s" {
			expr = "LOWER(s)"
		}
		alias := fmt.Sprintf("sk%d", i)
		selects = append(selects, expr+" AS "+alias)
		tail := ""
		if hasRest {
			tail = " " + rest
		}
		sortTerms = append(sortTerms, alias+tail)
	}
	typed = "SELECT id FROM typed_order" + filter + " ORDER BY " + strings.Join(terms, ", ")
	reference = "SELECT id, " + strings.Join(selects, ", ") + " FROM typed_order" + filter + " ORDER BY " + strings.Join(sortTerms, ", ")
	return typed, reference
}

func TestTypedMultiOrderMatchesGenericPath(t *testing.T) {
	db := typedOrderFixture(t, 300)
	filters := []string{"", " WHERE k <> 2", " WHERE k = 2"}
	orders := [][]string{
		{"n", "s DESC"},
		{"f DESC NULLS FIRST", "n", "s NULLS LAST"},
		{"s", "n DESC NULLS FIRST", "f"},
		{"k", "n DESC", "f DESC"},
		{"n DESC NULLS FIRST", "f NULLS FIRST", "s DESC NULLS LAST"},
	}
	pages := [][2]int{{-1, 0}, {1, 0}, {3, 0}, {5, 2}, {20, 0}, {20, 15}, {150, 0}, {290, 0}, {400, 0}, {2, 500}}
	for _, filter := range filters {
		for _, terms := range orders {
			typed, reference := typedMultiOrderTerms(filter, terms)
			for _, page := range pages {
				limit := ""
				if page[0] >= 0 {
					limit = fmt.Sprintf(" LIMIT %d OFFSET %d", page[0], page[1])
				}
				got := execSQL(t, db, typed+limit)
				want := execSQL(t, db, reference+limit)
				if !reflect.DeepEqual(typedOrderIDs(got), typedOrderIDs(want)) {
					t.Fatalf("%s:\n got %v\nwant %v", typed+limit, typedOrderIDs(got), typedOrderIDs(want))
				}
			}
		}
	}
}

func TestTypedMultiOrderHandlesAndDeclinesColumns(t *testing.T) {
	db := typedOrderFixture(t, 300)
	env := ExecEnv{ctx: t.Context(), tenant: "default", db: db}
	execSQL(t, db, "CREATE TABLE typed_multi_mixed (a INT, v TEXT)")
	mixed, err := db.Get("default", "typed_multi_mixed")
	if err != nil {
		t.Fatal(err)
	}
	mixed.Rows = [][]any{{1, "x"}, {2, 7}, {3, "y"}}
	mixed.Version++

	for _, tc := range []struct {
		sql     string
		handled bool
	}{
		{"SELECT id FROM typed_order ORDER BY f DESC, n, s NULLS FIRST", true},
		{"SELECT id FROM typed_order WHERE k = 2 ORDER BY n, f", true},
		{"SELECT id FROM typed_order ORDER BY n, s LIMIT 290", true},
		// A bounded page keeps the generic heap, which needs no encoding.
		{"SELECT id FROM typed_order ORDER BY n, s DESC LIMIT 7", false},
		{"SELECT a FROM typed_multi_mixed ORDER BY a, v", false},
	} {
		plan, ok, err := buildSimpleSelectPlan(env, mustParse(tc.sql).(*Select))
		if err != nil || !ok {
			t.Fatalf("%s: plan ok=%v err=%v", tc.sql, ok, err)
		}
		columns, ok := simpleTypedMultiOrderColumns(plan)
		if !ok {
			t.Fatalf("%s: multi-column order not recognized", tc.sql)
		}
		candidates := len(simplePlanRows(plan))
		if plan.rowIDs != nil {
			candidates = len(plan.rowIDs)
		}
		keep := -1
		if plan.limit != nil {
			keep = boundedLimitRows(plan.limit, plan.offset, candidates)
		}
		_, handled, err := executeTypedMultiOrderedSelect(env, plan, simplePlanRows(plan), columns, keep)
		if err != nil || handled != tc.handled {
			t.Fatalf("%s: handled=%v err=%v; want %v", tc.sql, handled, err, tc.handled)
		}
	}
}

func TestTypedMultiOrderHonorsCancellation(t *testing.T) {
	db := typedOrderFixture(t, 300)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	env := ExecEnv{ctx: ctx, tenant: "default", db: db}
	plan, ok, err := buildSimpleSelectPlan(env, mustParse("SELECT id FROM typed_order ORDER BY n, s").(*Select))
	if err != nil || !ok {
		t.Fatalf("plan ok=%v err=%v", ok, err)
	}
	columns, _ := simpleTypedMultiOrderColumns(plan)
	if _, _, err := executeTypedMultiOrderedSelect(env, plan, simplePlanRows(plan), columns, -1); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled multi-column order returned %v", err)
	}
}

// TestTypedMultiOrderKeepsFilterErrorOrder pins that a filter error surfaces
// at the same candidate as on the generic path.
func TestTypedMultiOrderKeepsFilterErrorOrder(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	typed := "SELECT id FROM typed_order WHERE 10 / (id - 5) > 0 ORDER BY n, s"
	execSQL(t, db, "CREATE TABLE typed_order (id INT, n INT, s TEXT)")
	table, err := db.Get("default", "typed_order")
	if err != nil {
		t.Fatal(err)
	}
	for i := range 20 {
		table.Rows = append(table.Rows, []any{i, i % 3, fmt.Sprintf("s%d", i%4)})
	}
	table.Version++
	_, errTyped := Execute(t.Context(), db, "default", mustParse(typed))
	_, errGeneric := Execute(t.Context(), db, "default", mustParse("SELECT id, n + 0 AS sk0, LOWER(s) AS sk1 FROM typed_order WHERE 10 / (id - 5) > 0 ORDER BY sk0, sk1"))
	if errTyped == nil || errGeneric == nil || errTyped.Error() != errGeneric.Error() {
		t.Fatalf("errors %v and %v; want the same division error", errTyped, errGeneric)
	}
}
