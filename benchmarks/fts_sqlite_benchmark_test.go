package benchmarks

import (
	"database/sql"
	"math"
	"strings"
	"testing"
)

// Both engines use database/sql, one connection, in-memory storage and the same
// ASCII corpus. SQLite is modernc FTS5 without stemming; the chosen words avoid
// stemming/stop-word differences. BM25 scores and tie order are not equated.
type ftsParityEngine struct{ name, driver, dsn, ddl, search string }

var ftsParityEngines = []ftsParityEngine{
	{"tinySQL", "tinysql", "mem://?tenant=default", `CREATE TABLE docs (id INT, body TEXT)`, `SELECT id, _fts_score FROM FTS_SEARCH('docs', ?, 10, 'body')`},
	{"SQLite-FTS5", "sqlite", ":memory:", `CREATE VIRTUAL TABLE docs USING fts5(id UNINDEXED, body, tokenize='ascii')`, `SELECT id, rank FROM docs WHERE docs MATCH ? ORDER BY rank LIMIT 10`},
}

func ftsParityOpen(tb testing.TB, e ftsParityEngine) *sql.DB {
	tb.Helper()
	db, err := sql.Open(e.driver, e.dsn)
	if err != nil {
		tb.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if _, err = db.Exec(e.ddl); err != nil {
		db.Close()
		tb.Fatal(err)
	}
	return db
}

func ftsParityTexts(n int) []string {
	texts := make([]string, n)
	for i := range texts {
		term := "gamma"
		if i%100 == 0 {
			term = "needle42"
		}
		texts[i] = strings.Repeat("alpha beta delta epsilon ", 1+i%8) + term + " " + strings.Repeat("kappa lambda ", i%5)
	}
	return texts
}

func ftsParityLoad(tb testing.TB, db *sql.DB, e ftsParityEngine, texts []string) {
	tb.Helper()
	tx, err := db.Begin()
	if err != nil {
		tb.Fatal(err)
	}
	defer tx.Rollback()
	stmt, err := tx.Prepare(`INSERT INTO docs (id, body) VALUES (?, ?)`)
	if err != nil {
		tb.Fatal(err)
	}
	defer stmt.Close()
	for i, text := range texts {
		if _, err = stmt.Exec(i, text); err != nil {
			tb.Fatal(err)
		}
	}
	if err = tx.Commit(); err != nil {
		tb.Fatal(err)
	}
	if e.driver == "tinysql" {
		if _, err = db.Exec(`SELECT * FROM FTS_WARM('docs','body')`); err != nil {
			tb.Fatal(err)
		}
	}
}

func ftsParityQuery(tb testing.TB, db *sql.DB, e ftsParityEngine, query string, n int) {
	tb.Helper()
	rows, err := db.Query(e.search, query)
	if err != nil {
		tb.Fatal(err)
	}
	defer rows.Close()
	seen := make(map[int]bool, 10)
	previous := math.Inf(1)
	for rows.Next() {
		var id int
		var score float64
		if err = rows.Scan(&id, &score); err != nil {
			tb.Fatal(err)
		}
		if e.driver == "sqlite" {
			score = -score
		}
		if id < 0 || id >= n || seen[id] || math.IsNaN(score) || score > previous {
			tb.Fatalf("invalid ranked result: id=%d score=%g", id, score)
		}
		if query == "needle42" && id%100 != 0 {
			tb.Fatalf("nonmatching row %d", id)
		}
		previous = score
		seen[id] = true
	}
	if err = rows.Err(); err != nil {
		tb.Fatal(err)
	}
	want := 10
	if query == "needle42" {
		want = min(10, (n+99)/100)
	}
	if len(seen) != want {
		tb.Fatalf("got %d rows, want %d", len(seen), want)
	}
}

func TestFTSSQLiteParityFixture(t *testing.T) {
	for _, e := range ftsParityEngines {
		t.Run(e.name, func(t *testing.T) {
			db := ftsParityOpen(t, e)
			defer db.Close()
			ftsParityLoad(t, db, e, ftsParityTexts(1000))
			if e.driver == "sqlite" {
				var version string
				if err := db.QueryRow(`SELECT sqlite_version()`).Scan(&version); err != nil {
					t.Fatal(err)
				}
				t.Logf("SQLite %s", version)
			}
			for _, query := range []string{"needle42", "beta", `"alpha beta"`} {
				ftsParityQuery(t, db, e, query, 1000)
			}
		})
	}
}

func BenchmarkParityFTSWarm(b *testing.B) {
	const n = 10000
	texts := ftsParityTexts(n)
	for _, q := range []struct{ name, text string }{{"selective", "needle42"}, {"common", "beta"}, {"phrase", `"alpha beta"`}} {
		b.Run(q.name, func(b *testing.B) {
			for _, e := range ftsParityEngines {
				b.Run(e.name, func(b *testing.B) {
					db := ftsParityOpen(b, e)
					defer db.Close()
					ftsParityLoad(b, db, e, texts)
					if e.driver == "sqlite" {
						mustExec(b, db, `INSERT INTO docs(docs) VALUES('optimize')`)
					}
					ftsParityQuery(b, db, e, q.text, n)
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						ftsParityQuery(b, db, e, q.text, n)
					}
				})
			}
		})
	}
}

// Measures the complete transition from an empty table to searchable documents:
// transaction + prepared INSERTs + commit + tinySQL's lazy FTS build. SQLite
// maintains FTS5 during INSERT. Connection/DDL setup and closing are excluded.
func BenchmarkParityFTSIngest(b *testing.B) {
	texts := ftsParityTexts(1000)
	for _, e := range ftsParityEngines {
		b.Run(e.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				db := ftsParityOpen(b, e)
				b.StartTimer()
				ftsParityLoad(b, db, e, texts)
				b.StopTimer()
				ftsParityQuery(b, db, e, "needle42", len(texts))
				if err := db.Close(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
