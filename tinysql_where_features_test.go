package tinysql

import (
	"context"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/engine"
	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// execQuery is a helper that parses and executes a single SQL string.
func execQuery(t *testing.T, ctx context.Context, db *storage.DB, sql string) *engine.ResultSet {
	t.Helper()
	p := engine.NewParser(sql)
	st, err := p.ParseStatement()
	if err != nil {
		t.Fatalf("parse %q: %v", sql, err)
	}
	rs, err := engine.Execute(ctx, db, "default", st)
	if err != nil {
		t.Fatalf("execute %q: %v", sql, err)
	}
	return rs
}

// setupWhereDB creates a fresh DB with a small table used by WHERE feature tests.
func setupWhereDB(t *testing.T) (*storage.DB, context.Context) {
	t.Helper()
	db := storage.NewDB()
	ctx := context.Background()

	for _, sql := range []string{
		`CREATE TABLE words (id INT, word TEXT, score INT)`,
		`INSERT INTO words VALUES (1,  'apple',   10)`,
		`INSERT INTO words VALUES (2,  'Apricot',  20)`,
		`INSERT INTO words VALUES (3,  'banana',  30)`,
		`INSERT INTO words VALUES (4,  'blueberry', 40)`,
		`INSERT INTO words VALUES (5,  'cherry',  50)`,
		`INSERT INTO words VALUES (6,  'Cherry',  60)`,
		`INSERT INTO words VALUES (7,  'date',    70)`,
		`INSERT INTO words VALUES (8,  'elderberry', 80)`,
		`INSERT INTO words VALUES (9,  'fig',     90)`,
		`INSERT INTO words VALUES (10, 'grape',   100)`,
	} {
		execQuery(t, ctx, db, sql)
	}
	return db, ctx
}

// TestILIKE verifies case-insensitive LIKE matching.
func TestILIKE(t *testing.T) {
	db, ctx := setupWhereDB(t)

	t.Run("basic match", func(t *testing.T) {
		rs := execQuery(t, ctx, db, `SELECT word FROM words WHERE word ILIKE 'cherry'`)
		if len(rs.Rows) != 2 {
			t.Fatalf("expected 2 rows (cherry, Cherry), got %d", len(rs.Rows))
		}
	})

	t.Run("prefix wildcard", func(t *testing.T) {
		rs := execQuery(t, ctx, db, `SELECT word FROM words WHERE word ILIKE 'ap%'`)
		if len(rs.Rows) != 2 {
			t.Fatalf("expected 2 rows (apple, Apricot), got %d", len(rs.Rows))
		}
	})

	t.Run("suffix wildcard", func(t *testing.T) {
		rs := execQuery(t, ctx, db, `SELECT word FROM words WHERE word ILIKE '%berry'`)
		// blueberry, elderberry (cherry ends in 'erry' not 'berry')
		if len(rs.Rows) != 2 {
			t.Fatalf("expected 2 rows (*berry), got %d: %v", len(rs.Rows), rs.Rows)
		}
	})

	t.Run("NOT ILIKE", func(t *testing.T) {
		rs := execQuery(t, ctx, db, `SELECT word FROM words WHERE word NOT ILIKE '%e%'`)
		// words without 'e' or 'E' (case-insensitive):
		// apple has 'e', Apricot no 'e', banana no, blueberry has 'e', cherry has 'e',
		// Cherry has 'e', date has 'e', elderberry has 'e', fig no, grape has 'e'
		// no 'e': Apricot, banana, fig → 3
		if len(rs.Rows) != 3 {
			t.Fatalf("expected 3 rows (Apricot, banana, fig), got %d", len(rs.Rows))
		}
	})
}

// TestGLOB verifies GLOB (* / ?) pattern matching.
func TestGLOB(t *testing.T) {
	db, ctx := setupWhereDB(t)

	t.Run("star wildcard", func(t *testing.T) {
		rs := execQuery(t, ctx, db, `SELECT word FROM words WHERE word GLOB 'b*'`)
		if len(rs.Rows) != 2 {
			t.Fatalf("expected 2 rows (banana, blueberry), got %d", len(rs.Rows))
		}
	})

	t.Run("question mark wildcard", func(t *testing.T) {
		rs := execQuery(t, ctx, db, `SELECT word FROM words WHERE word GLOB '???'`)
		// words of exactly 3 chars: fig
		if len(rs.Rows) != 1 {
			t.Fatalf("expected 1 row (fig), got %d", len(rs.Rows))
		}
	})

	t.Run("case sensitive", func(t *testing.T) {
		rs := execQuery(t, ctx, db, `SELECT word FROM words WHERE word GLOB 'Cherry'`)
		if len(rs.Rows) != 1 {
			t.Fatalf("expected 1 row (Cherry exact), got %d", len(rs.Rows))
		}
	})

	t.Run("NOT GLOB", func(t *testing.T) {
		rs := execQuery(t, ctx, db, `SELECT word FROM words WHERE word NOT GLOB '*berry'`)
		// berry-words: blueberry, elderberry → 10 - 2 = 8
		if len(rs.Rows) != 8 {
			t.Fatalf("expected 8 rows (non-berry), got %d", len(rs.Rows))
		}
	})
}

// TestREGEXP verifies REGEXP / RLIKE infix operator.
func TestREGEXP(t *testing.T) {
	db, ctx := setupWhereDB(t)

	t.Run("REGEXP basic", func(t *testing.T) {
		rs := execQuery(t, ctx, db, `SELECT word FROM words WHERE word REGEXP '^[aA]'`)
		// apple, Apricot
		if len(rs.Rows) != 2 {
			t.Fatalf("expected 2 rows, got %d", len(rs.Rows))
		}
	})

	t.Run("RLIKE alias", func(t *testing.T) {
		rs := execQuery(t, ctx, db, `SELECT word FROM words WHERE word RLIKE 'berry$'`)
		// blueberry, elderberry
		if len(rs.Rows) != 2 {
			t.Fatalf("expected 2 rows (blueberry, elderberry), got %d", len(rs.Rows))
		}
	})

	t.Run("NOT REGEXP", func(t *testing.T) {
		rs := execQuery(t, ctx, db, `SELECT word FROM words WHERE word NOT REGEXP 'berry$'`)
		if len(rs.Rows) != 8 {
			t.Fatalf("expected 8 rows, got %d", len(rs.Rows))
		}
	})
}

// TestSIMILARTO verifies the SIMILAR TO operator.
func TestSIMILARTO(t *testing.T) {
	db, ctx := setupWhereDB(t)

	t.Run("percent wildcard", func(t *testing.T) {
		rs := execQuery(t, ctx, db, `SELECT word FROM words WHERE word SIMILAR TO 'c%'`)
		// cherry (lowercase) — SIMILAR TO is case-sensitive
		if len(rs.Rows) != 1 {
			t.Fatalf("expected 1 row (cherry), got %d", len(rs.Rows))
		}
	})

	t.Run("alternation", func(t *testing.T) {
		rs := execQuery(t, ctx, db, `SELECT word FROM words WHERE word SIMILAR TO 'fig|date'`)
		if len(rs.Rows) != 2 {
			t.Fatalf("expected 2 rows (fig, date), got %d", len(rs.Rows))
		}
	})

	t.Run("NOT SIMILAR TO", func(t *testing.T) {
		rs := execQuery(t, ctx, db, `SELECT word FROM words WHERE word NOT SIMILAR TO '%berry'`)
		if len(rs.Rows) != 8 {
			t.Fatalf("expected 8 rows, got %d", len(rs.Rows))
		}
	})
}

// TestEXISTS verifies the EXISTS (subquery) predicate.
func TestEXISTS(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()

	for _, sql := range []string{
		`CREATE TABLE items (id INT, name TEXT)`,
		`CREATE TABLE tags (item_id INT, tag TEXT)`,
		`INSERT INTO items VALUES (1, 'Alpha')`,
		`INSERT INTO items VALUES (2, 'Beta')`,
		`INSERT INTO tags VALUES (1, 'hot')`,
	} {
		execQuery(t, ctx, db, sql)
	}

	t.Run("EXISTS non-empty subquery is true", func(t *testing.T) {
		// All items are returned because the subquery always has rows.
		rs := execQuery(t, ctx, db, `SELECT name FROM items WHERE EXISTS (SELECT 1 FROM tags)`)
		if len(rs.Rows) != 2 {
			t.Fatalf("expected 2 rows (all items), got %d", len(rs.Rows))
		}
	})

	t.Run("EXISTS empty subquery is false", func(t *testing.T) {
		// No items returned because the subquery is empty.
		rs := execQuery(t, ctx, db, `SELECT name FROM items WHERE EXISTS (SELECT 1 FROM tags WHERE tag = 'nonexistent')`)
		if len(rs.Rows) != 0 {
			t.Fatalf("expected 0 rows, got %d", len(rs.Rows))
		}
	})

	t.Run("NOT EXISTS non-empty subquery is false", func(t *testing.T) {
		rs := execQuery(t, ctx, db, `SELECT name FROM items WHERE NOT EXISTS (SELECT 1 FROM tags)`)
		if len(rs.Rows) != 0 {
			t.Fatalf("expected 0 rows, got %d", len(rs.Rows))
		}
	})

	t.Run("NOT EXISTS empty subquery is true", func(t *testing.T) {
		rs := execQuery(t, ctx, db, `SELECT name FROM items WHERE NOT EXISTS (SELECT 1 FROM tags WHERE tag = 'none')`)
		if len(rs.Rows) != 2 {
			t.Fatalf("expected 2 rows, got %d", len(rs.Rows))
		}
	})
}

// TestLEVENSHTEIN verifies the LEVENSHTEIN / EDIT_DISTANCE functions.
func TestLEVENSHTEIN(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()

	execQuery(t, ctx, db, `CREATE TABLE t (s TEXT)`)
	execQuery(t, ctx, db, `INSERT INTO t VALUES ('kitten')`)
	execQuery(t, ctx, db, `INSERT INTO t VALUES ('sitting')`)
	execQuery(t, ctx, db, `INSERT INTO t VALUES ('abc')`)

	t.Run("kitten→sitting", func(t *testing.T) {
		rs := execQuery(t, ctx, db, `SELECT LEVENSHTEIN(s, 'sitting') AS d FROM t WHERE s = 'kitten'`)
		if rs.Rows[0]["d"] != 3 {
			t.Fatalf("expected Levenshtein(kitten,sitting)=3, got %v", rs.Rows[0]["d"])
		}
	})

	t.Run("identical strings", func(t *testing.T) {
		rs := execQuery(t, ctx, db, `SELECT LEVENSHTEIN('hello', 'hello') AS d FROM t LIMIT 1`)
		if rs.Rows[0]["d"] != 0 {
			t.Fatalf("expected 0 for identical strings, got %v", rs.Rows[0]["d"])
		}
	})

	t.Run("EDIT_DISTANCE alias", func(t *testing.T) {
		rs := execQuery(t, ctx, db, `SELECT EDIT_DISTANCE('abc', 'xyz') AS d FROM t LIMIT 1`)
		if rs.Rows[0]["d"] != 3 {
			t.Fatalf("expected 3, got %v", rs.Rows[0]["d"])
		}
	})

	t.Run("WHERE LEVENSHTEIN filter", func(t *testing.T) {
		rs := execQuery(t, ctx, db, `SELECT s FROM t WHERE LEVENSHTEIN(s, 'abc') <= 1`)
		if len(rs.Rows) != 1 || rs.Rows[0]["s"] != "abc" {
			t.Fatalf("expected only 'abc', got %v", rs.Rows)
		}
	})
}

// TestSTRING_PREDICATES verifies CONTAINS, STARTS_WITH, ENDS_WITH.
func TestSTRING_PREDICATES(t *testing.T) {
	db, ctx := setupWhereDB(t)

	t.Run("CONTAINS", func(t *testing.T) {
		rs := execQuery(t, ctx, db, `SELECT word FROM words WHERE CONTAINS(word, 'err')`)
		// blueberry, cherry, Cherry, elderberry
		if len(rs.Rows) != 4 {
			t.Fatalf("expected 4 rows, got %d: %v", len(rs.Rows), rs.Rows)
		}
	})

	t.Run("STARTS_WITH", func(t *testing.T) {
		rs := execQuery(t, ctx, db, `SELECT word FROM words WHERE STARTS_WITH(word, 'b')`)
		// banana, blueberry
		if len(rs.Rows) != 2 {
			t.Fatalf("expected 2 rows, got %d", len(rs.Rows))
		}
	})

	t.Run("ENDS_WITH", func(t *testing.T) {
		rs := execQuery(t, ctx, db, `SELECT word FROM words WHERE ENDS_WITH(word, 'erry')`)
		// cherry, Cherry (no berry ending without b)
		// actually: cherry ends in 'erry', Cherry ends in 'erry', blueberry ends in 'erry', elderberry ends in 'erry'
		if len(rs.Rows) != 4 {
			t.Fatalf("expected 4 rows (*erry), got %d", len(rs.Rows))
		}
	})

	t.Run("CONTAINS false", func(t *testing.T) {
		rs := execQuery(t, ctx, db, `SELECT word FROM words WHERE CONTAINS(word, 'xyz')`)
		if len(rs.Rows) != 0 {
			t.Fatalf("expected 0 rows, got %d", len(rs.Rows))
		}
	})
}
