package engine

import (
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// The general path discovers a star's columns by ranging over the row map, and
// Go randomizes map iteration order. The values were already guarded against
// that; the column list was not, so the same query returned its columns in a
// different order from run to run. cmd/server's /api/query happened to hide it
// by sorting the keys it derived itself.
//
// 60 runs is enough to make a coin-flip ordering essentially certain to show up
// (it appeared roughly half the time when this was broken).
func TestStarColumnOrderIsDeterministic(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE users (id INT, name TEXT)`)
	execSQL(t, db, `INSERT INTO users VALUES (1,'Ada'),(2,'Grace')`)
	execSQL(t, db, `CREATE TABLE orders (order_id INT, user_id INT, total FLOAT)`)
	execSQL(t, db, `INSERT INTO orders VALUES (10,1,9.5),(11,2,4.25)`)

	queries := []string{
		// Self-join: both sides contribute the same bare column names.
		`SELECT * FROM users u1 CROSS JOIN users u2`,
		// Distinct column names across the two tables.
		`SELECT * FROM users u JOIN orders o ON u.id = o.user_id`,
	}

	for _, q := range queries {
		first := strings.Join(execSQL(t, db, q).Cols, ",")
		for i := 0; i < 60; i++ {
			got := strings.Join(execSQL(t, db, q).Cols, ",")
			if got != first {
				t.Fatalf("%s\n  run %d returned columns %q\n  first run returned %q",
					q, i, got, first)
			}
		}
	}
}

// The values a star projection produces must not depend on iteration order
// either. This guards the behaviour the existing comment in the star branch
// describes, alongside the ordering fix.
func TestStarValuesAreStableAcrossRuns(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE a (id INT, tag TEXT)`)
	execSQL(t, db, `CREATE TABLE b (id INT, tag TEXT)`)
	execSQL(t, db, `INSERT INTO a VALUES (1,'left')`)
	execSQL(t, db, `INSERT INTO b VALUES (2,'right')`)

	const q = `SELECT * FROM a CROSS JOIN b`
	first := rowsAsKeys(execSQL(t, db, q))
	for i := 0; i < 60; i++ {
		if got := rowsAsKeys(execSQL(t, db, q)); strings.Join(got, ";") != strings.Join(first, ";") {
			t.Fatalf("run %d produced %v, first run produced %v", i, got, first)
		}
	}
}
