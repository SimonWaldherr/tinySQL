package engine

import (
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// TestAggregateEmptyInputsUseSQLNullSemantics exercises the row-accumulator
// aggregate fast path. It must not expose SUM's zero-value accumulator for an
// empty input (or an input containing only NULLs): SQL defines SUM/AVG/MIN/MAX
// as NULL in both cases, while COUNT remains numeric.
func TestAggregateEmptyInputsUseSQLNullSemantics(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE aggregate_empty (v INT)`)

	assertAggregateEmptyInputRow(t, execSQL(t, db, `
		SELECT SUM(v) AS sum_value, AVG(v) AS avg_value,
		       MIN(v) AS min_value, MAX(v) AS max_value,
		       COUNT(v) AS count_value, COUNT(*) AS count_rows
		FROM aggregate_empty
	`), 0)

	// An all-NULL argument has the same value-aggregate result as no rows,
	// but COUNT(*) still sees the physical row.
	execSQL(t, db, `INSERT INTO aggregate_empty VALUES (NULL)`)
	assertAggregateEmptyInputRow(t, execSQL(t, db, `
		SELECT SUM(v) AS sum_value, AVG(v) AS avg_value,
		       MIN(v) AS min_value, MAX(v) AS max_value,
		       COUNT(v) AS count_value, COUNT(*) AS count_rows
		FROM aggregate_empty
	`), 1)
}

func assertAggregateEmptyInputRow(t *testing.T, rs *ResultSet, wantCountStar int) {
	t.Helper()
	if len(rs.Rows) != 1 {
		t.Fatalf("aggregate result rows = %d, want 1: %+v", len(rs.Rows), rs.Rows)
	}
	for _, column := range []string{"sum_value", "avg_value", "min_value", "max_value"} {
		if got := rs.Rows[0][column]; got != nil {
			t.Errorf("%s = %v, want NULL", column, got)
		}
	}
	expectInt(t, rs.Rows[0]["count_value"], 0, "COUNT(value)")
	expectInt(t, rs.Rows[0]["count_rows"], wantCountStar, "COUNT(*)")
}
