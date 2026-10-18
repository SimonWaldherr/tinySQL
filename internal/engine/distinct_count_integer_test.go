package engine

import (
	"math"
	"math/big"
	"reflect"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestDistinctCountIntegerMixedTransitions(t *testing.T) {
	// Integers retain all 64 bits; other SQL values keep COUNT(DISTINCT)'s
	// existing text equivalence, including numeric text and exact decimals.
	values := []any{
		nil, int64(math.MinInt64), int64(math.MaxInt64), int(0), int64(0),
		int64(9007199254740992), int64(9007199254740993), int(-1), int64(-1),
		"0", "-1", "9223372036854775807", "-9223372036854775808",
		float64(9007199254740992), 0.0, math.Copysign(0, -1), "-0",
		big.NewRat(1, 2), "1/2", true, "true", []byte("value"),
	}
	for shift := range values {
		var set distinctCountSet
		seen := make(map[string]bool)
		for round := 0; round < 2; round++ {
			for offset := range values {
				value := values[(offset+shift)%len(values)]
				want := false
				if value != nil {
					key := valueText(value)
					want = !seen[key]
					seen[key] = true
				}
				if got := set.add(value); got != want {
					t.Fatalf("shift %d, round %d, %T(%v): got %v, want %v", shift, round, value, value, got, want)
				}
			}
		}
	}
}

func TestCountDistinctIntegerSQLPreservesValues(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	table := storage.NewTable("integer_counts", []storage.Column{{Name: "g", Type: storage.IntType}, {Name: "v", Type: storage.InterfaceType}}, false)
	table.Rows = [][]any{
		{1, nil}, {1, int64(9007199254740992)}, {1, int64(9007199254740993)},
		{1, int64(9007199254740993)}, {1, "9007199254740993"},
		{2, int64(math.MinInt64)}, {2, int64(math.MaxInt64)}, {2, "9223372036854775807"},
		{2, int(0)}, {2, big.NewRat(1, 2)}, {2, "1/2"}, {3, nil},
	}
	if err := db.Put("default", table); err != nil {
		t.Fatal(err)
	}
	want := []Row{{"g": 1, "n": 2}, {"g": 2, "n": 4}, {"g": 3, "n": 0}}
	for _, source := range []string{"integer_counts", "(SELECT * FROM integer_counts) AS source"} {
		rs := execSQL(t, db, `SELECT g, COUNT(DISTINCT v) AS n FROM `+source+` GROUP BY g ORDER BY g`)
		if !reflect.DeepEqual(rs.Rows, want) {
			t.Fatalf("source %s: got %#v, want %#v", source, rs.Rows, want)
		}
	}
	// The direct evaluator is also used by aggregate expressions that do not
	// qualify for the raw path. Count across an integer-only group explicitly.
	count, err := evalAggregateCount(ExecEnv{ctx: t.Context()}, &FuncCall{Name: "COUNT", Distinct: true, Args: []Expr{newVarRef("v")}}, []Row{
		{"v": int64(9007199254740992)}, {"v": int64(9007199254740993)}, {"v": nil}, {"v": int64(9007199254740992)},
	})
	if err != nil || count != 2 {
		t.Fatalf("integer-only general evaluator: got %v, %v; want 2", count, err)
	}
}
