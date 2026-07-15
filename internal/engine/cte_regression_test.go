package engine

import (
	"context"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestCTEReferencesAreCaseInsensitiveAndCarryAliasesIntoDerivedQueries(t *testing.T) {
	db := storage.NewDB()

	for _, sql := range []string{
		`WITH X AS (SELECT 1 AS n) SELECT n FROM x`,
		`WITH c AS (SELECT 1 AS n) SELECT q.n FROM c AS q`,
		`WITH c AS (SELECT 1 AS n) SELECT n FROM (SELECT n FROM c) AS q`,
		`WITH c AS (SELECT 1 AS n), d AS (SELECT n + 1 AS n FROM c) SELECT n FROM d`,
		`WITH RECURSIVE c AS (SELECT 1 AS n) SELECT n FROM c`,
		`WITH c(x) AS (SELECT 1 AS n) SELECT x FROM c`,
	} {
		rs := execSQL(t, db, sql)
		if len(rs.Rows) != 1 {
			t.Fatalf("%q returned %#v", sql, rs.Rows)
		}
	}
}

func TestCTECanAppearOnRightSideOfJoin(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE left_values (id INT)`)
	execSQL(t, db, `INSERT INTO left_values VALUES (1), (2)`)

	rs := execSQL(t, db, `
		WITH right_values AS (SELECT 1 AS id, 'matched' AS label)
		SELECT l.id AS id, r.label AS label
		FROM left_values AS l
		LEFT JOIN right_values AS r ON l.id = r.id
		ORDER BY l.id
	`)
	if len(rs.Rows) != 2 {
		t.Fatalf("joined CTE rows = %#v", rs.Rows)
	}
	expectInt(t, rs.Rows[0]["id"], 1, "matched id")
	if got := rs.Rows[0]["label"]; got != "matched" {
		t.Fatalf("matched label = %#v", got)
	}
	expectInt(t, rs.Rows[1]["id"], 2, "unmatched id")
	if got := rs.Rows[1]["label"]; got != nil {
		t.Fatalf("unmatched CTE label = %#v, want NULL", got)
	}
}

func TestCTEColumnAliasCountMustMatchResult(t *testing.T) {
	db := storage.NewDB()
	stmt, err := NewParser(`WITH c(x, y) AS (SELECT 1 AS n) SELECT x FROM c`).ParseStatement()
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if _, err := Execute(context.Background(), db, "default", stmt); err == nil {
		t.Fatal("CTE with mismatched alias count unexpectedly executed")
	}
}

func TestRecursiveCTEBypassesPhysicalTableFastPath(t *testing.T) {
	db := storage.NewDB()
	rs := execSQL(t, db, `
		WITH RECURSIVE cnt AS (
			SELECT 1 AS n
			UNION ALL
			SELECT n + 1 AS n FROM cnt WHERE n < 3
		)
		SELECT n FROM cnt ORDER BY n
	`)
	if len(rs.Rows) != 3 {
		t.Fatalf("recursive CTE rows = %#v", rs.Rows)
	}
	for i, want := range []int{1, 2, 3} {
		if got := expectAsInt(t, rs.Rows[i]["n"]); got != want {
			t.Fatalf("recursive CTE row %d = %d, want %d", i, got, want)
		}
	}
}

func TestRecursiveCTEUnionAllPreservesDuplicateFrontierRows(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE two (x INT)`)
	execSQL(t, db, `INSERT INTO two VALUES (1), (2)`)
	rs := execSQL(t, db, `
		WITH RECURSIVE cnt AS (
			SELECT 1 AS n
			UNION ALL
			SELECT cnt.n + 1 AS n FROM cnt LEFT JOIN two ON 1 = 1 WHERE cnt.n < 2
		)
		SELECT n FROM cnt ORDER BY n
	`)
	if len(rs.Rows) != 3 {
		t.Fatalf("recursive UNION ALL rows = %#v", rs.Rows)
	}
	if got := []int{expectAsInt(t, rs.Rows[0]["n"]), expectAsInt(t, rs.Rows[1]["n"]), expectAsInt(t, rs.Rows[2]["n"])}; got[0] != 1 || got[1] != 2 || got[2] != 2 {
		t.Fatalf("recursive UNION ALL values = %#v, want [1 2 2]", got)
	}
}
