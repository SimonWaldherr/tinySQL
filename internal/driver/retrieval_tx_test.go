package driver

import (
	"database/sql"
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"testing"
)

// Full-text and vector search answer from structures maintained incrementally
// across UPDATEs. A transaction works on its own copy of the table, and a
// rolled-back or committed transaction must leave searches answering exactly
// what a database loaded with the final rows answers.

type retrievalTxQuerier interface {
	Query(query string, args ...any) (*sql.Rows, error)
}

func retrievalTxAnswers(t *testing.T, q retrievalTxQuerier) [][]string {
	t.Helper()
	queries := []string{
		`SELECT id, _fts_score FROM FTS_SEARCH('docs', 'alpha OR gamma', 10, 'content')`,
		`SELECT id, _fts_score FROM FTS_SEARCH('docs', 'zeta', 10, 'content')`,
		`SELECT id, _vec_distance FROM VEC_SEARCH('docs', 'embedding', '[1, 0.5, 0.25]', 6, 'cosine')`,
		`SELECT id, _vec_distance FROM VEC_SEARCH('docs', 'embedding', '[0.1, 0.9, 0.4]', 6, 'l2')`,
		`SELECT id, _vec_distance FROM VEC_SEARCH('docs', 'embedding', '[1, 0.5, 0.25]', 6, 'cosine', 'hnsw')`,
	}
	var out [][]string
	for _, query := range queries {
		rows, err := q.Query(query)
		if err != nil {
			t.Fatalf("%s: %v", query, err)
		}
		var answer []string
		for rows.Next() {
			var id, score any
			if err := rows.Scan(&id, &score); err != nil {
				_ = rows.Close()
				t.Fatalf("%s: %v", query, err)
			}
			answer = append(answer, fmt.Sprintf("%v=%v", id, score))
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("%s: %v", query, err)
		}
		_ = rows.Close()
		out = append(out, answer)
	}
	return out
}

type retrievalTxDoc struct {
	loaded  int
	content string
	vec     string
}

func retrievalTxLoad(t *testing.T, tenant string, docs map[int]retrievalTxDoc) *sql.DB {
	t.Helper()
	db, err := sql.Open("tinysql", "mem://?tenant="+tenant)
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(4)
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(`CREATE TABLE docs (id INT, loaded_at INT, content TEXT, embedding VECTOR)`); err != nil {
		t.Fatal(err)
	}
	for id := 0; id < len(docs); id++ {
		d := docs[id]
		if _, err := db.Exec(fmt.Sprintf(`INSERT INTO docs VALUES (%d, %d, '%s', '%s')`, id, d.loaded, d.content, d.vec)); err != nil {
			t.Fatal(err)
		}
	}
	return db
}

func TestRetrievalStructuresAcrossTransactions(t *testing.T) {
	rng := rand.New(rand.NewSource(21))
	words := []string{"alpha", "beta", "gamma", "delta", "epsilon", "zeta"}
	docs := map[int]retrievalTxDoc{}
	for id := 0; id < 70; id++ {
		var content []string
		for j := 0; j < 3+rng.Intn(4); j++ {
			content = append(content, words[rng.Intn(len(words)-1)])
		}
		docs[id] = retrievalTxDoc{loaded: 1, content: strings.Join(content, " "), vec: fmt.Sprintf("[%d, %d, %d]", rng.Intn(9)+1, rng.Intn(9)+1, rng.Intn(9)+1)}
	}
	live := retrievalTxLoad(t, "retr_tx_live", docs)
	base := retrievalTxAnswers(t, live)
	for _, i := range []int{0, 2, 3} { // the "zeta" query matches nothing yet
		a := base[i]
		if len(a) == 0 {
			t.Fatalf("setup: an answer is empty: %v", base)
		}
	}

	exec := func(x interface {
		Exec(string, ...any) (sql.Result, error)
	}, q string) {
		t.Helper()
		if _, err := x.Exec(q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}

	// Rolled back: a bookkeeping update, a text rewrite, an embedding rewrite
	// and a failing statement. The answers inside the transaction follow the
	// uncommitted rows; afterwards everything is as before.
	for round, stmts := range [][]string{
		{`UPDATE docs SET loaded_at = 9`},
		{`UPDATE docs SET loaded_at = 9`, `UPDATE docs SET content = 'zeta zeta alpha' WHERE id < 5`},
		{`UPDATE docs SET embedding = '[9, 9, 9]' WHERE id = 3`, `DELETE FROM docs WHERE id = 8`},
		{`INSERT INTO docs VALUES (500, 1, 'zeta zeta zeta', '[1, 0.5, 0.25]')`},
	} {
		tx, err := live.Begin()
		if err != nil {
			t.Fatal(err)
		}
		for _, stmt := range stmts {
			exec(tx, stmt)
		}
		_ = retrievalTxAnswers(t, tx) // builds or maintains the transaction's own structures
		if err := tx.Rollback(); err != nil {
			t.Fatal(err)
		}
		if got := retrievalTxAnswers(t, live); !reflect.DeepEqual(got, base) {
			t.Fatalf("round %d: answers changed by a rolled-back transaction:\n got %v\nwant %v", round, got, base)
		}
	}

	// Committed: bookkeeping and text updates, then compare with a database
	// loaded with the final rows.
	tx, err := live.Begin()
	if err != nil {
		t.Fatal(err)
	}
	exec(tx, `UPDATE docs SET loaded_at = 7`)
	_ = retrievalTxAnswers(t, tx)
	exec(tx, `UPDATE docs SET content = 'zeta alpha gamma', loaded_at = 8 WHERE id = 11`)
	exec(tx, `UPDATE docs SET embedding = '[1, 0.5, 0.25]' WHERE id = 12`)
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	docs[11] = retrievalTxDoc{loaded: 8, content: "zeta alpha gamma", vec: docs[11].vec}
	docs[12] = retrievalTxDoc{loaded: 7, content: docs[12].content, vec: "[1, 0.5, 0.25]"}
	for id := range docs {
		if id != 11 && id != 12 {
			d := docs[id]
			d.loaded = 7
			docs[id] = d
		}
	}
	cold := retrievalTxLoad(t, "retr_tx_cold", docs)
	want := retrievalTxAnswers(t, cold)
	if got := retrievalTxAnswers(t, live); !reflect.DeepEqual(got[:4], want[:4]) {
		t.Fatalf("after commit the answers differ from a cold database:\n got %v\nwant %v", got[:4], want[:4])
	}
	// A rollback after the commit still leaves the committed state.
	tx, err = live.Begin()
	if err != nil {
		t.Fatal(err)
	}
	exec(tx, `UPDATE docs SET content = 'alpha' WHERE id >= 0`)
	_ = retrievalTxAnswers(t, tx)
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if got := retrievalTxAnswers(t, live); !reflect.DeepEqual(got[:4], want[:4]) {
		t.Fatalf("after commit and rollback the answers differ from a cold database:\n got %v\nwant %v", got[:4], want[:4])
	}
}
