package engine

import (
	"context"
	"fmt"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// TestClassificationMethodsDisagree uses a deliberately unevenly-spaced
// dataset -- a dense low cluster (1..12) and a small tight high cluster
// (90..92), with nothing in between -- to show that EQUAL_INTERVAL,
// NATURAL_BREAKS and NTILE genuinely produce different classifications, not
// three names for the same bucketing.
func TestClassificationMethodsDisagree(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE kpi (value FLOAT)`)
	values := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 90, 91, 92}
	for _, v := range values {
		execSQL(t, db, fmt.Sprintf(`INSERT INTO kpi VALUES (%v)`, v))
	}

	rs, err := Execute(context.Background(), db, "default", mustParse(`
		SELECT value,
			NATURAL_BREAKS(3) OVER (ORDER BY value) AS nb,
			EQUAL_INTERVAL(3) OVER (ORDER BY value) AS ei,
			NTILE(3) OVER (ORDER BY value) AS nt
		FROM kpi ORDER BY value
	`))
	if err != nil {
		t.Fatalf("classification query: %v", err)
	}
	if len(rs.Rows) != len(values) {
		t.Fatalf("got %d rows, want %d", len(rs.Rows), len(values))
	}

	// EQUAL_INTERVAL over range [1,92] into 3 equal-width bins leaves the
	// middle bin [31.33, 61.67) empty -- nothing in the data falls there.
	eiClasses := map[int]bool{}
	nbClasses := map[int]bool{}
	var ntOfEleven, ntOfNinety any
	for _, row := range rs.Rows {
		ei, _ := row["ei"].(int)
		eiClasses[ei] = true
		nb, _ := row["nb"].(int)
		nbClasses[nb] = true
		if row["value"] == float64(11) {
			ntOfEleven = row["nt"]
		}
		if row["value"] == float64(90) {
			ntOfNinety = row["nt"]
		}
	}
	if eiClasses[2] {
		t.Errorf("EQUAL_INTERVAL unexpectedly used the empty middle bin (class 2); classes seen: %v", eiClasses)
	}
	if !nbClasses[1] || !nbClasses[2] || !nbClasses[3] {
		t.Errorf("NATURAL_BREAKS should use all 3 classes (no forced-empty class by construction); classes seen: %v", nbClasses)
	}

	// NTILE(3) on 15 rows splits strictly by sorted position (5 rows per
	// bucket), so value=11 (position 11 of 15) and value=90 (position 13)
	// land in the SAME bucket -- a purely positional split that ignores
	// the huge gap between them, unlike either value-based method.
	if ntOfEleven != ntOfNinety {
		t.Errorf("NTILE(3) put value=11 in bucket %v and value=90 in bucket %v, want the same bucket (position-based split)", ntOfEleven, ntOfNinety)
	}
}

// TestNaturalBreaksCacheSurvivesCompiledStatementReuse guards against a real
// bug found while building the choropleth demo: tinysql's compiled-statement
// cache (NewQueryCache/CompiledQuery, used by e.g. the WASM query bridge)
// reuses the same parsed *FuncCall AST node across separate executions of
// identical SQL text. natBreaksCache used to key purely on (AST node,
// partition, n) -- when a later execution's table had more rows than an
// earlier one, that stale key served a too-short cached classes slice, and
// indexing it with the new, larger currentIdx panicked with "index out of
// range". The fix adds a content signature to the cache key; this test
// re-runs the identical compiled query after the table's row count changes
// and asserts it succeeds instead of panicking/erroring.
func TestNaturalBreaksCacheSurvivesCompiledStatementReuse(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE kpi (value FLOAT)`)
	for _, v := range []float64{1, 2, 3, 4, 5} {
		execSQL(t, db, fmt.Sprintf(`INSERT INTO kpi VALUES (%v)`, v))
	}

	cache := NewQueryCache(10)
	sql := `SELECT value, NATURAL_BREAKS(3) OVER (ORDER BY value) AS bucket FROM kpi ORDER BY value`
	compiled, err := cache.Compile(sql)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	if _, err := compiled.Execute(context.Background(), db, "default"); err != nil {
		t.Fatalf("first execution (5 rows): %v", err)
	}

	// Grow the table well past the first run's row count using the SAME
	// compiled statement object (same *FuncCall AST node) for the second run.
	for _, v := range []float64{6, 7, 8, 9, 10, 90, 91, 92, 93, 94} {
		execSQL(t, db, fmt.Sprintf(`INSERT INTO kpi VALUES (%v)`, v))
	}
	rs, err := compiled.Execute(context.Background(), db, "default")
	if err != nil {
		t.Fatalf("second execution (15 rows, same compiled statement): %v", err)
	}
	if len(rs.Rows) != 15 {
		t.Fatalf("second execution returned %d rows, want 15", len(rs.Rows))
	}
	for _, row := range rs.Rows {
		if _, ok := row["bucket"].(int); !ok {
			t.Errorf("row %v: bucket = %T, want int (classification should have run, not silently skipped)", row["value"], row["bucket"])
		}
	}
}
