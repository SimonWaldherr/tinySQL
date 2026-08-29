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

func TestCTESimpleScanAppliesWhereProjectionOffsetAndLimit(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE items (id INT, label TEXT)`)
	execSQL(t, db, `INSERT INTO items VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')`)

	rs := execSQL(t, db, `
		WITH c AS (SELECT id, label FROM items)
		SELECT label AS name FROM c WHERE id >= 2 LIMIT 2 OFFSET 1`)
	if len(rs.Rows) != 2 || rs.Cols[0] != "name" {
		t.Fatalf("CTE page = cols %#v rows %#v", rs.Cols, rs.Rows)
	}
	if rs.Rows[0]["name"] != "c" || rs.Rows[1]["name"] != "d" {
		t.Fatalf("CTE page rows = %#v", rs.Rows)
	}
}

func TestCTESimpleScanFallsBackForQualifiedReferences(t *testing.T) {
	db := storage.NewDB()
	rs := execSQL(t, db, `WITH c AS (SELECT 1 AS n) SELECT q.n FROM c AS q WHERE q.n = 1`)
	if len(rs.Rows) != 1 {
		t.Fatalf("qualified CTE rows = %#v", rs.Rows)
	}
	expectInt(t, rs.Rows[0]["q.n"], 1, "qualified CTE value")
}

func TestCTESimpleScanKeepsGeneralDuplicateAliasSemantics(t *testing.T) {
	db := storage.NewDB()
	rs := execSQL(t, db, `WITH c AS (SELECT 1 AS a, 2 AS b) SELECT a AS x, b AS x FROM c`)
	if len(rs.Cols) != 1 || rs.Cols[0] != "x" || len(rs.Rows) != 1 {
		t.Fatalf("duplicate alias result = cols %#v rows %#v", rs.Cols, rs.Rows)
	}
	expectInt(t, rs.Rows[0]["x"], 2, "duplicate alias last value")
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

func TestCTERowCacheReusesSameSourceAndKeepsAliasMapsSeparate(t *testing.T) {
	cteResult := &ResultSet{
		Cols:         []string{"id"},
		Rows:         []Row{{"id": 7}},
		cteCacheable: true,
	}
	env := ExecEnv{cteRowCache: newCTERowCache()}

	first := rowsFromCTEResult(env, cteResult, FromItem{Table: "items", Alias: "i"})
	second := rowsFromCTEResult(env, cteResult, FromItem{Table: "items", Alias: "i"})
	if len(first) != 1 || len(second) != 1 || &first[0] != &second[0] {
		t.Fatal("matching CTE sources should reuse their materialized row maps")
	}
	if first[0]["id"] != 7 || first[0]["items.id"] != 7 || first[0]["i.id"] != 7 {
		t.Fatalf("cached CTE row = %#v", first[0])
	}

	otherAlias := rowsFromCTEResult(env, cteResult, FromItem{Table: "items", Alias: "j"})
	if len(otherAlias) != 1 || &first[0] == &otherAlias[0] {
		t.Fatal("different CTE aliases require separate qualified row maps")
	}
	if otherAlias[0]["j.id"] != 7 {
		t.Fatalf("alias-qualified CTE row = %#v", otherAlias[0])
	}
	if got := len(env.cteRowCache.entries); got != 2 {
		t.Fatalf("cached CTE sources = %d, want 2", got)
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

func TestRecursiveCTECompoundLimitBoundsWholeCTE(t *testing.T) {
	db := storage.NewDB()
	for _, tc := range []struct {
		name string
		sql  string
		want []int
	}{
		{
			name: "limit-stops-an-unbounded-recursion",
			sql: `
				WITH RECURSIVE cnt AS (
					SELECT 1 AS n
					UNION ALL
					SELECT n + 1 AS n FROM cnt
					LIMIT 3
				)
				SELECT n FROM cnt ORDER BY n`,
			want: []int{1, 2, 3},
		},
		{
			name: "limit-and-offset-apply-after-recursive-work",
			sql: `
				WITH RECURSIVE cnt AS (
					SELECT 1 AS n
					UNION ALL
					SELECT n + 1 AS n FROM cnt
					LIMIT 3 OFFSET 1
				)
				SELECT n FROM cnt ORDER BY n`,
			want: []int{2, 3, 4},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rs := execSQL(t, db, tc.sql)
			if len(rs.Rows) != len(tc.want) {
				t.Fatalf("rows = %#v, want %d rows", rs.Rows, len(tc.want))
			}
			for i, want := range tc.want {
				if got := expectAsInt(t, rs.Rows[i]["n"]); got != want {
					t.Fatalf("row %d = %d, want %d", i, got, want)
				}
			}
		})
	}
}

func TestRecursiveCTEFanOutJoinExceedsRowLimitInsteadOfExhaustingMemory(t *testing.T) {
	// Lower the row cap for the duration of this test so it can prove the
	// limit trips without actually allocating millions of rows.
	orig := recursiveCTEMaxRows
	recursiveCTEMaxRows = 50
	defer func() { recursiveCTEMaxRows = orig }()

	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE two (x INT)`)
	execSQL(t, db, `INSERT INTO two VALUES (1), (2)`)

	// Each iteration self-joins the frontier against a 2-row table with a
	// trivially-true ON condition, doubling the frontier every round. With
	// iterLimit at 1024 this would keep doubling well past any reasonable
	// memory budget (2^30+ rows) long before the iteration cap ever kicks in.
	stmt, err := NewParser(`
		WITH RECURSIVE cnt AS (
			SELECT 1 AS n
			UNION ALL
			SELECT cnt.n + 1 AS n FROM cnt LEFT JOIN two ON 1 = 1 WHERE cnt.n < 1000000
		)
		SELECT n FROM cnt
	`).ParseStatement()
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, err = Execute(context.Background(), db, "default", stmt)
	if err == nil {
		t.Fatal("expected exponential-fan-out recursive CTE to error out once it exceeds the row limit")
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
