package engine

import (
	"context"
	"math/big"
	"reflect"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

const decimalOrderAnalysisQuery = `SELECT id, amount FROM decimal_order ORDER BY amount, id`

func decimalOrderAnalysisFixture(tb testing.TB, rows int) *storage.DB {
	tb.Helper()
	db := storage.NewDB()
	tb.Cleanup(func() { _ = db.Close() })
	if _, err := Execute(context.Background(), db, "default", mustParse(
		`CREATE TABLE decimal_order (id INT, amount DECIMAL)`)); err != nil {
		tb.Fatal(err)
	}
	table, err := db.Get("default", "decimal_order")
	if err != nil {
		tb.Fatal(err)
	}
	table.Rows = make([][]any, rows)
	for i := range table.Rows {
		// A permutation puts sorting work ahead of already ordered input;
		// repeated prices exercise stable ties and different reduced denominators.
		numerator := int64((i * 7919) % 10007)
		table.Rows[i] = []any{i, big.NewRat(numerator, 100)}
	}
	table.Version++
	return db
}

func BenchmarkDecimalOrderAnalysis(b *testing.B) {
	db := decimalOrderAnalysisFixture(b, 20000)
	runBench(b, db, decimalOrderAnalysisQuery)
}

func TestAnalyzeDecimalOrder(t *testing.T) {
	db := decimalOrderAnalysisFixture(t, 20000)
	result := execSQL(t, db, "EXPLAIN ANALYZE "+decimalOrderAnalysisQuery)
	foundSort, foundExecute, bottlenecks := false, false, 0
	for _, row := range result.Rows {
		if _, measured := row["self_ns"]; measured {
			if row["operation"] == "SORT" || row["operation"] == "EXECUTE" {
				if row["actual_rows"] != int64(20000) || row["calls"] != int64(1) {
					t.Fatalf("unexpected row/call counts: %v", row)
				}
				foundSort = foundSort || row["operation"] == "SORT"
				foundExecute = foundExecute || row["operation"] == "EXECUTE"
			}
			if row["bottleneck"] == true {
				bottlenecks++
			}
			t.Logf("%s: self_ns=%v total_ns=%v rows=%v calls=%v percent=%.2f bottleneck=%v",
				row["operation"], row["self_ns"], row["total_ns"], row["actual_rows"],
				row["calls"], row["percent"], row["bottleneck"])
		}
	}
	if !foundSort || !foundExecute || bottlenecks != 1 {
		t.Fatalf("incomplete profile: sort=%v execute=%v bottlenecks=%d", foundSort, foundExecute, bottlenecks)
	}
}

func TestDecimalComparisonDoesNotMutateInputs(t *testing.T) {
	left := new(big.Rat)
	left.SetString("123456789012345678901234567890.123456789")
	right := new(big.Rat)
	right.SetString("123456789012345678901234567891.123456789")
	leftBefore, rightBefore := left.RatString(), right.RatString()
	for _, rhs := range []any{right, *right} {
		got, err := compare(left, rhs)
		if err != nil || got >= 0 {
			t.Fatalf("compare(%T) = %d, %v; want less", rhs, got, err)
		}
		if left.RatString() != leftBefore || right.RatString() != rightBefore {
			t.Fatal("comparison changed a borrowed decimal")
		}
	}
	for _, tc := range []struct {
		right any
		want  int
	}{
		{big.NewRat(3, 4), -1},
		{big.NewRat(1, 3), 1},
		{big.NewRat(-1, 2), 1},
		{big.Rat{}, 1},
		{1, -1},
		{int64(0), 1},
		{0.5, 0},
		{"0.5", 0},
		{"1/3", 1},
	} {
		got, err := compare(big.NewRat(1, 2), tc.right)
		if err != nil || got != tc.want {
			t.Errorf("compare(1/2, %T(%v)) = %d, %v; want %d", tc.right, tc.right, got, err, tc.want)
		}
	}
	for _, right := range []any{"invalid", true, []byte("1")} {
		if _, err := compare(big.NewRat(1, 2), right); err == nil {
			t.Errorf("comparison with %T should fail", right)
		}
	}
}

func TestDecimalOrderPreservesValuesAndTies(t *testing.T) {
	db := decimalOrderAnalysisFixture(t, 257)
	table, err := db.Get("default", "decimal_order")
	if err != nil {
		t.Fatal(err)
	}
	for i := range table.Rows {
		table.Rows[i][1] = big.NewRat(int64(i%17-8), 3)
	}
	before := make([]string, len(table.Rows))
	for i, raw := range table.Rows {
		before[i] = raw[1].(*big.Rat).RatString()
	}
	for _, query := range []string{
		`SELECT id, amount FROM decimal_order ORDER BY amount`,
		`SELECT id, amount FROM decimal_order ORDER BY amount DESC`,
		`SELECT id, amount FROM decimal_order ORDER BY amount, id LIMIT 100 OFFSET 7`,
	} {
		first := execSQL(t, db, query)
		second := execSQL(t, db, query)
		if !reflect.DeepEqual(first, second) {
			t.Fatalf("repeated result changed for %s", query)
		}
		wantIDs := make([]int, 0, len(table.Rows))
		for group := 0; group < 17; group++ {
			if strings.Contains(query, " DESC") {
				for id := 16 - group; id < len(table.Rows); id += 17 {
					wantIDs = append(wantIDs, id)
				}
			} else {
				for id := group; id < len(table.Rows); id += 17 {
					wantIDs = append(wantIDs, id)
				}
			}
		}
		if strings.Contains(query, " LIMIT") {
			wantIDs = wantIDs[7:107]
		}
		if len(first.Rows) != len(wantIDs) {
			t.Fatalf("query returned %d rows, want %d", len(first.Rows), len(wantIDs))
		}
		for i, row := range first.Rows {
			if row["id"] != wantIDs[i] {
				t.Fatalf("row %d for %s: got id %v, want %d", i, query, row["id"], wantIDs[i])
			}
		}
		for i := 1; i < len(first.Rows); i++ {
			previous, current := first.Rows[i-1], first.Rows[i]
			cmp := previous["amount"].(*big.Rat).Cmp(current["amount"].(*big.Rat))
			if cmp == 0 && previous["id"].(int) >= current["id"].(int) {
				t.Fatalf("unstable equal-price rows for %s: %v, %v", query, previous, current)
			}
		}
	}
	for i, raw := range table.Rows {
		if got := raw[1].(*big.Rat).RatString(); got != before[i] {
			t.Fatalf("source row %d changed: %s, want %s", i, got, before[i])
		}
	}
}
