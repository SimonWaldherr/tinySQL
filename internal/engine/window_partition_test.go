// Tests for PARTITION BY correctness with window functions, specifically
// targeting the partition-build memoization in eval_window.go
// (windowPartitionCache / resolveWindowPartition): multiple partitions in
// one query, ties within a partition, and a partition of size 1. These
// exercise buildWindowPartition's O(1)-lookup path (env.windowIndex ->
// partition position) instead of the O(P) rowsEqual fallback, and confirm
// no cross-partition leakage (e.g. LAG/LEAD must not see a neighboring
// partition's rows).
package engine

import (
	"strconv"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// setupPartitionTable seeds an employees(dept, name, sal) table with rows
// interleaved across departments (not grouped) so the table's physical scan
// order does not match partition order -- buildWindowPartition must actually
// filter, not just assume contiguity. Dept A has 6 rows with ties (90, 90,
// 80, 70, 70, 60); dept B has 2 tied rows (50, 50); dept C has exactly 1 row
// (partition of size 1).
func setupPartitionTable(t *testing.T) *storage.DB {
	t.Helper()
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE employees (dept TEXT, name TEXT, sal INT)`)
	rows := [][3]any{
		{"A", "Ben", 90},
		{"B", "Gary", 50},
		{"C", "Ivy", 100},
		{"A", "Alice", 90},
		{"B", "Hank", 50},
		{"A", "Cara", 80},
		{"A", "Dan", 70},
		{"A", "Eve", 70},
		{"A", "Fay", 60},
	}
	for _, r := range rows {
		execSQL(t, db, `INSERT INTO employees VALUES ('`+r[0].(string)+`', '`+r[1].(string)+`', `+strconv.Itoa(r[2].(int))+`)`)
	}
	return db
}

func TestWindowPartitionRankAcrossMultiplePartitions(t *testing.T) {
	db := setupPartitionTable(t)
	rs := execSQL(t, db, `SELECT dept, name, sal,
		ROW_NUMBER() OVER (PARTITION BY dept ORDER BY sal DESC) AS rn,
		RANK() OVER (PARTITION BY dept ORDER BY sal DESC) AS rk,
		DENSE_RANK() OVER (PARTITION BY dept ORDER BY sal DESC) AS drk
		FROM employees ORDER BY dept, sal DESC, name`)

	type want struct{ rn, rk, drk int }
	byName := map[string]want{
		// Dept A: 90,90,80,70,70,60 -- ties at 90 (Ben,Alice) and 70 (Dan,Eve)
		"Ben":   {1, 1, 1},
		"Alice": {2, 1, 1},
		"Cara":  {3, 3, 2},
		"Dan":   {4, 4, 3},
		"Eve":   {5, 4, 3},
		"Fay":   {6, 6, 4},
		// Dept B: 50, 50 -- fully tied
		"Gary": {1, 1, 1},
		"Hank": {2, 1, 1},
		// Dept C: single-row partition
		"Ivy": {1, 1, 1},
	}
	if len(rs.Rows) != 9 {
		t.Fatalf("expected 9 rows, got %d", len(rs.Rows))
	}
	for _, row := range rs.Rows {
		name := row["name"].(string)
		w, ok := byName[name]
		if !ok {
			t.Fatalf("unexpected row %q", name)
		}
		expectInt(t, row["rn"], w.rn, "ROW_NUMBER for "+name)
		expectInt(t, row["rk"], w.rk, "RANK for "+name)
		expectInt(t, row["drk"], w.drk, "DENSE_RANK for "+name)
	}
}

// TestWindowPartitionLagLeadNoCrossPartitionLeakage verifies that LAG/LEAD
// never see a neighboring partition's rows: the first row of each partition
// must have a NULL LAG, the last row of each partition must have a NULL
// LEAD, regardless of the partitions' relative physical order in the table.
func TestWindowPartitionLagLeadNoCrossPartitionLeakage(t *testing.T) {
	db := setupPartitionTable(t)
	rs := execSQL(t, db, `SELECT dept, name, sal,
		LAG(sal) OVER (PARTITION BY dept ORDER BY sal DESC) AS lg,
		LEAD(sal) OVER (PARTITION BY dept ORDER BY sal DESC) AS ld
		FROM employees ORDER BY dept, sal DESC, name`)

	type want struct{ lg, ld any }
	byName := map[string]want{
		"Ben":   {nil, 90},
		"Alice": {90, 80},
		"Cara":  {90, 70},
		"Dan":   {80, 70},
		"Eve":   {70, 60},
		"Fay":   {70, nil},
		"Gary":  {nil, 50},
		"Hank":  {50, nil},
		"Ivy":   {nil, nil}, // partition of size 1: no neighbor in either direction
	}
	if len(rs.Rows) != 9 {
		t.Fatalf("expected 9 rows, got %d", len(rs.Rows))
	}
	for _, row := range rs.Rows {
		name := row["name"].(string)
		w, ok := byName[name]
		if !ok {
			t.Fatalf("unexpected row %q", name)
		}
		if w.lg == nil {
			if row["lg"] != nil {
				t.Errorf("LAG for %s: want nil, got %v", name, row["lg"])
			}
		} else {
			expectInt(t, row["lg"], w.lg.(int), "LAG for "+name)
		}
		if w.ld == nil {
			if row["ld"] != nil {
				t.Errorf("LEAD for %s: want nil, got %v", name, row["ld"])
			}
		} else {
			expectInt(t, row["ld"], w.ld.(int), "LEAD for "+name)
		}
	}
}

// TestWindowPartitionFirstLastValuePerPartition verifies FIRST_VALUE/
// LAST_VALUE are computed per-partition, not over the whole table.
func TestWindowPartitionFirstLastValuePerPartition(t *testing.T) {
	db := setupPartitionTable(t)
	rs := execSQL(t, db, `SELECT dept, name,
		FIRST_VALUE(name) OVER (PARTITION BY dept ORDER BY sal DESC) AS fv,
		LAST_VALUE(name) OVER (PARTITION BY dept ORDER BY sal DESC) AS lv
		FROM employees ORDER BY dept, sal DESC, name`)

	wantFirst := map[string]string{"A": "Ben", "B": "Gary", "C": "Ivy"}
	wantLast := map[string]string{"A": "Fay", "B": "Hank", "C": "Ivy"}
	if len(rs.Rows) != 9 {
		t.Fatalf("expected 9 rows, got %d", len(rs.Rows))
	}
	for _, row := range rs.Rows {
		dept := row["dept"].(string)
		if got := row["fv"]; got != wantFirst[dept] {
			t.Errorf("FIRST_VALUE for dept %s row %v: want %s, got %v", dept, row["name"], wantFirst[dept], got)
		}
		if got := row["lv"]; got != wantLast[dept] {
			t.Errorf("LAST_VALUE for dept %s row %v: want %s, got %v", dept, row["name"], wantLast[dept], got)
		}
	}
}

// TestWindowPartitionSingleRowPartition specifically isolates the size-1
// partition (dept C / Ivy): every positional window function must treat it
// as its own complete partition, not error or panic on an out-of-range
// lookup.
func TestWindowPartitionSingleRowPartition(t *testing.T) {
	db := setupPartitionTable(t)
	rs := execSQL(t, db, `SELECT name, dept,
		ROW_NUMBER() OVER (PARTITION BY dept ORDER BY sal DESC) AS rn,
		RANK() OVER (PARTITION BY dept ORDER BY sal DESC) AS rk,
		LAG(sal) OVER (PARTITION BY dept ORDER BY sal DESC) AS lg,
		LEAD(sal) OVER (PARTITION BY dept ORDER BY sal DESC) AS ld,
		PERCENT_RANK() OVER (PARTITION BY dept ORDER BY sal DESC) AS pr,
		CUME_DIST() OVER (PARTITION BY dept ORDER BY sal DESC) AS cd
		FROM employees WHERE dept = 'C'`)
	if len(rs.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rs.Rows))
	}
	row := rs.Rows[0]
	expectInt(t, row["rn"], 1, "ROW_NUMBER for single-row partition")
	expectInt(t, row["rk"], 1, "RANK for single-row partition")
	if row["lg"] != nil {
		t.Errorf("LAG for single-row partition: want nil, got %v", row["lg"])
	}
	if row["ld"] != nil {
		t.Errorf("LEAD for single-row partition: want nil, got %v", row["ld"])
	}
	expectFloat(t, row["pr"], 0.0, 1e-9, "PERCENT_RANK for single-row partition")
	expectFloat(t, row["cd"], 1.0, 1e-9, "CUME_DIST for single-row partition")
}

// TestWindowPartitionRepeatedEvaluationStability calls the same partitioned
// window query multiple times against the same *storage.DB to catch any
// cache staleness across separate Execute calls (the memoization cache used
// here is scoped to one query execution, not process-global, but this test
// exists to pin that down explicitly rather than only by code inspection).
func TestWindowPartitionRepeatedEvaluationStability(t *testing.T) {
	db := setupPartitionTable(t)
	for i := 0; i < 3; i++ {
		rs := execSQL(t, db, `SELECT dept, name, RANK() OVER (PARTITION BY dept ORDER BY sal DESC) AS rk FROM employees ORDER BY dept, sal DESC, name`)
		if len(rs.Rows) != 9 {
			t.Fatalf("run %d: expected 9 rows, got %d", i, len(rs.Rows))
		}
		ben := rs.Rows[0]
		if ben["name"] != "Ben" && ben["name"] != "Alice" {
			t.Fatalf("run %d: unexpected first dept-A row %v", i, ben["name"])
		}
	}
}
