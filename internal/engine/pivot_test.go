// Tests for the PIVOT clause. Before this, PIVOT had zero implementation
// anywhere (lexer, parser, or executor) — see internal/engine/parser.go's
// parsePivotClause and exec.go's processPivot.
package engine

import (
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func setupPivotSalesTable(t *testing.T) *storage.DB {
	t.Helper()
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE sales (region TEXT, category TEXT, amount INT)`)
	rows := []string{
		`INSERT INTO sales VALUES ('East', 'Electronics', 100)`,
		`INSERT INTO sales VALUES ('East', 'Furniture', 50)`,
		`INSERT INTO sales VALUES ('East', 'Electronics', 25)`,
		`INSERT INTO sales VALUES ('West', 'Electronics', 200)`,
		`INSERT INTO sales VALUES ('West', 'Furniture', 75)`,
	}
	for _, r := range rows {
		execSQL(t, db, r)
	}
	return db
}

func TestPivotBasicSum(t *testing.T) {
	db := setupPivotSalesTable(t)
	rs := execSQL(t, db, `
		SELECT *
		FROM sales
		PIVOT (SUM(amount) FOR category IN ('Electronics', 'Furniture'))
	`)
	if len(rs.Rows) != 2 {
		t.Fatalf("expected 2 rows (East, West), got %d: %+v", len(rs.Rows), rs.Rows)
	}
	byRegion := map[string]Row{}
	for _, r := range rs.Rows {
		byRegion[r["region"].(string)] = r
	}
	east, ok := byRegion["East"]
	if !ok {
		t.Fatalf("missing East row: %+v", rs.Rows)
	}
	// Row map keys are always lowercased (putVal/getValLower convention),
	// including pivot output columns named after their literal value.
	expectFloat(t, east["electronics"], 125, 1e-9, "East Electronics")
	expectFloat(t, east["furniture"], 50, 1e-9, "East Furniture")

	west := byRegion["West"]
	expectFloat(t, west["electronics"], 200, 1e-9, "West Electronics")
	expectFloat(t, west["furniture"], 75, 1e-9, "West Furniture")
}

func TestPivotWithAliases(t *testing.T) {
	db := setupPivotSalesTable(t)
	rs := execSQL(t, db, `
		SELECT region, elec, furn
		FROM sales
		PIVOT (SUM(amount) FOR category IN ('Electronics' AS elec, 'Furniture' AS furn))
		ORDER BY region
	`)
	if len(rs.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rs.Rows))
	}
	expectFloat(t, rs.Rows[0]["elec"], 125, 1e-9, "East elec (row0=East alphabetically)")
	expectFloat(t, rs.Rows[0]["furn"], 50, 1e-9, "East furn")
	expectFloat(t, rs.Rows[1]["elec"], 200, 1e-9, "West elec")
	expectFloat(t, rs.Rows[1]["furn"], 75, 1e-9, "West furn")
}

func TestPivotCount(t *testing.T) {
	db := setupPivotSalesTable(t)
	rs := execSQL(t, db, `
		SELECT region, elec, furn
		FROM sales
		PIVOT (COUNT(amount) FOR category IN ('Electronics' AS elec, 'Furniture' AS furn))
		ORDER BY region
	`)
	// East has 2 Electronics rows, 1 Furniture row.
	expectInt(t, rs.Rows[0]["elec"], 2, "East elec count")
	expectInt(t, rs.Rows[0]["furn"], 1, "East furn count")
	// West has 1 Electronics row, 1 Furniture row.
	expectInt(t, rs.Rows[1]["elec"], 1, "West elec count")
	expectInt(t, rs.Rows[1]["furn"], 1, "West furn count")
}

func TestPivotMissingValueIsNullOrZero(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE t (grp TEXT, cat TEXT, val INT)`)
	execSQL(t, db, `INSERT INTO t VALUES ('A', 'x', 10)`)
	// Group 'A' has no 'y' rows at all.
	rs := execSQL(t, db, `SELECT * FROM t PIVOT (MAX(val) FOR cat IN ('x', 'y'))`)
	if len(rs.Rows) != 1 {
		t.Fatalf("expected 1 group, got %d", len(rs.Rows))
	}
	// MAX/MIN return the raw stored value untouched (no float64 coercion,
	// unlike SUM/AVG), so it comes back as whatever type INSERT stored.
	expectInt(t, rs.Rows[0]["x"], 10, "x")
	if rs.Rows[0]["y"] != nil {
		t.Errorf("expected nil for unmatched pivot value (MAX), got %v", rs.Rows[0]["y"])
	}
}

func TestPivotWithWhereFiltersSourceRows(t *testing.T) {
	db := setupPivotSalesTable(t)
	// WHERE filters the source rows before pivoting.
	rs := execSQL(t, db, `
		SELECT region, elec
		FROM sales
		WHERE amount >= 100
		PIVOT (SUM(amount) FOR category IN ('Electronics' AS elec))
		ORDER BY region
	`)
	if len(rs.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d: %+v", len(rs.Rows), rs.Rows)
	}
	expectFloat(t, rs.Rows[0]["elec"], 100, 1e-9, "East elec filtered")
	expectFloat(t, rs.Rows[1]["elec"], 200, 1e-9, "West elec filtered")
}

func TestPivotSelectSpecificColumn(t *testing.T) {
	db := setupPivotSalesTable(t)
	// Selecting just one pivoted column (not *) must still work.
	rs := execSQL(t, db, `
		SELECT region
		FROM sales
		PIVOT (SUM(amount) FOR category IN ('Electronics' AS elec, 'Furniture' AS furn))
		ORDER BY region
	`)
	if len(rs.Cols) != 1 || rs.Cols[0] != "region" {
		t.Fatalf("expected only 'region' column selected, got %+v", rs.Cols)
	}
}

func TestPivotRequiresAtLeastOneValue(t *testing.T) {
	p := NewParser(`SELECT * FROM sales PIVOT (SUM(amount) FOR category IN ())`)
	if _, err := p.ParseStatement(); err == nil {
		t.Fatal("expected parse error for empty PIVOT IN-list")
	}
}

// TestTriggerForKeywordIsCaseInsensitive guards a bug found while adding the
// PIVOT clause's own "FOR" keyword: "FOR" was missing from the lexer's
// keyword table entirely, so tokenizeIdentOrKeyword never upcased it and
// parseTriggerForEachRow's `p.cur.Val != "FOR"` check only matched a
// trigger's "FOR EACH ROW" clause when typed in that exact uppercase form.
// This only checks that CREATE TRIGGER itself parses with a lowercase
// clause — firing the trigger hits a separate, pre-existing bug in trigger
// body re-serialization/re-parsing (triggers.go's stmtToSQL is explicitly
// documented as a non-round-trip-safe placeholder) unrelated to this fix.
func TestTriggerForKeywordIsCaseInsensitive(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE orders (id INT, status TEXT)`)
	execSQL(t, db, `CREATE TABLE audit_log (order_id INT, msg TEXT)`)
	p := NewParser(`
		CREATE TRIGGER orders_after_insert
		AFTER INSERT ON orders
		for each row
		BEGIN
		  INSERT INTO audit_log VALUES (1, 'created');
		END;
	`)
	if _, err := p.ParseStatement(); err != nil {
		t.Fatalf("lowercase 'for each row' should parse, got: %v", err)
	}
}
