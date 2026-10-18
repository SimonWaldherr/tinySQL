package engine

import (
	"context"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"strings"
	"testing"
)

func TestMaterializedGroupStoreMatchesGeneralEvaluator(t *testing.T) {
	keys := []any{
		nil, false, true, 1, int64(1), float64(1), "1", "", "N;", "a\x1fb",
		float64(0), math.Copysign(0, -1), math.NaN(),
		math.Float64frombits(0x7ff8000000000001), math.Inf(1), math.Inf(-1),
		[]byte("x"), []byte(nil), big.NewRat(1, 3), map[string]any{"x": 1},
	}
	var rows []Row
	for repeat := 0; repeat < 7; repeat++ {
		for i, key := range keys {
			var value any = i + repeat
			switch repeat {
			case 0:
				value = nil
			case 2:
				value = float64(i) + 0.5
			case 3:
				value = big.NewRat(int64(i+1), 3)
			case 4:
				value = "ignored by SUM"
			case 5:
				value = int64(1<<54) + int64(i)
			}
			rows = append(rows, Row{
				"g": key, "tag": repeat % 2, "label": []string{"", "\x1f", "N;"}[repeat%3],
				"seq": i, "v": value,
			})
		}
	}
	for _, grouping := range []string{"g", "g, tag", "g, label"} {
		for _, projection := range []string{
			`MIN(seq) AS first, COUNT(*) AS n, COUNT(v) AS present, SUM(v) AS s, AVG(v) AS a, MIN(v) AS lo, MAX(v) AS hi`,
			`COUNT(*) AS n, COUNT(v) AS present`,
			`SUM(v) AS s, AVG(v) AS a`,
		} {
			t.Run(projection+"/"+grouping, func(t *testing.T) {
				assertMaterializedGroupMatchesGeneral(t, "SELECT "+projection+" FROM t GROUP BY "+grouping, rows)
			})
		}
	}
}

func TestMaterializedGroupStoreOwnsArenaValues(t *testing.T) {
	var rows []Row
	for repeat := 0; repeat < 5; repeat++ {
		for group := 0; group < 257; group++ {
			var value any = group + repeat
			if repeat == 4 && group%3 == 0 {
				value = big.NewRat(int64(group+1), 3)
			}
			rows = append(rows, Row{
				"g":   fmt.Sprintf("%s%d", strings.Repeat("group-", group%7+1), group),
				"tag": group % 3, "v": value,
			})
		}
	}
	for _, grouping := range []string{"g", "g, tag"} {
		query := "SELECT g, COUNT(*) AS n, SUM(v) AS s, AVG(v) AS a, MIN(v) AS lo, MAX(v) AS hi FROM t GROUP BY " + grouping
		assertMaterializedGroupMatchesGeneral(t, query, rows)
	}
}

func TestMaterializedGroupStoreOverflowAndDecimalPromotion(t *testing.T) {
	rows := []Row{
		{"g": "positive", "v": int64(math.MaxInt64)},
		{"g": "positive", "v": int64(1)},
		{"g": "positive", "v": big.NewRat(1, 3)},
		{"g": "negative", "v": int64(math.MinInt64)},
		{"g": "negative", "v": int64(-1)},
		{"g": "negative", "v": big.NewRat(-1, 3)},
		{"g": "null", "v": nil},
	}
	assertMaterializedGroupMatchesGeneral(t, "SELECT g, COUNT(*) AS n, SUM(v) AS s, AVG(v) AS a, MIN(v) AS lo, MAX(v) AS hi FROM t GROUP BY g", rows)
}

func assertMaterializedGroupMatchesGeneral(t *testing.T, sql string, rows []Row) {
	t.Helper()
	stmt := mustParse(sql).(*Select)
	env := ExecEnv{ctx: context.Background()}
	got, cols, handled, err := processAggregateQueryFastPath(env, stmt, rows)
	if err != nil || !handled {
		t.Fatalf("materialized fast path: handled=%v, err=%v", handled, err)
	}
	// A constant true HAVING forces independent group construction and the
	// general aggregate evaluators without changing the expected result.
	general := *stmt
	general.Having = &Literal{Val: true}
	want, wantCols, err := processAggregateQuery(env, &general, rows)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) || !reflect.DeepEqual(cols, wantCols) {
		t.Fatalf("%s: got %#v (%v), want %#v (%v)", sql, got, cols, want, wantCols)
	}
	// A later execution uses different keys and values. Reusing an arena or
	// its scratch slices across calls would now alter the retained result.
	otherRows := make([]Row, len(rows))
	for i, row := range rows {
		other := make(Row, len(row))
		for key, value := range row {
			other[key] = value
		}
		other["g"] = fmt.Sprintf("other-%d", i%2)
		other["v"] = -i - 1
		otherRows[i] = other
	}
	if _, _, _, err := processAggregateQueryFastPath(env, stmt, otherRows); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatal("later materialized aggregation changed a retained result")
	}
}
