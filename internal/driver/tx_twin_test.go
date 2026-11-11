package driver

import (
	"database/sql"
	"errors"
	"fmt"
	"math/rand"
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// A transaction's first write to a table no longer copies the table's row
// headers or its secondary-index skip lists: it is handed the arrays of the
// table the previous commit replaced, brought up to date (storage/row_twin.go,
// storage/index_twin.go). These tests pin that the recycled structures always
// hold exactly what a fresh copy would.

type twinRow struct {
	label string
	n     int
}

func openTwinDB(t *testing.T, rows int) (*sql.DB, map[int]twinRow) {
	t.Helper()
	db, err := sql.Open("tinysql", fmt.Sprintf("mem://?tenant=twin_%s", t.Name()))
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(4)
	t.Cleanup(func() { _ = db.Close() })
	for _, stmt := range []string{
		`CREATE TABLE tw (id INT PRIMARY KEY, label TEXT, n INT)`,
		`CREATE INDEX tw_label ON tw (label)`,
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	model := make(map[int]twinRow, rows)
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < rows; i++ {
		r := twinRow{label: fmt.Sprintf("l%d", i%37), n: i}
		if _, err := tx.Exec(`INSERT INTO tw VALUES (?, ?, ?)`, i, r.label, r.n); err != nil {
			t.Fatal(err)
		}
		model[i] = r
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	return db, model
}

type twinQuerier interface {
	Query(string, ...any) (*sql.Rows, error)
	QueryRow(string, ...any) *sql.Row
}

// checkTwinModel compares the table with the model three ways: a full scan, a
// primary-key lookup of sampled rows and a secondary-index lookup per label.
func checkTwinModel(t *testing.T, q twinQuerier, model map[int]twinRow, rng *rand.Rand, where string) {
	t.Helper()
	rows, err := q.Query(`SELECT id, label, n FROM tw`)
	if err != nil {
		t.Fatalf("%s: scan: %v", where, err)
	}
	seen := 0
	for rows.Next() {
		var id, n int
		var label string
		if err := rows.Scan(&id, &label, &n); err != nil {
			t.Fatal(err)
		}
		want, ok := model[id]
		if !ok || want.label != label || want.n != n {
			t.Fatalf("%s: scan returned id=%d label=%q n=%d, model has %+v (present=%v)", where, id, label, n, want, ok)
		}
		seen++
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if seen != len(model) {
		t.Fatalf("%s: scan returned %d rows, model has %d", where, seen, len(model))
	}

	ids := make([]int, 0, len(model))
	byLabel := map[string][]int{}
	for id, r := range model {
		ids = append(ids, id)
		byLabel[r.label] = append(byLabel[r.label], id)
	}
	sort.Ints(ids)
	for i := 0; i < 8 && len(ids) > 0; i++ {
		id := ids[rng.Intn(len(ids))]
		var label string
		var n int
		if err := q.QueryRow(`SELECT label, n FROM tw WHERE id = ?`, id).Scan(&label, &n); err != nil {
			t.Fatalf("%s: lookup id=%d: %v", where, id, err)
		}
		if want := model[id]; want.label != label || want.n != n {
			t.Fatalf("%s: lookup id=%d returned %q/%d, want %+v", where, id, label, n, want)
		}
	}
	for label, want := range byLabel {
		got := []int{}
		rs, err := q.Query(`SELECT id FROM tw WHERE label = ?`, label)
		if err != nil {
			t.Fatalf("%s: index lookup %q: %v", where, label, err)
		}
		for rs.Next() {
			var id int
			if err := rs.Scan(&id); err != nil {
				t.Fatal(err)
			}
			got = append(got, id)
		}
		_ = rs.Close()
		sort.Ints(got)
		sort.Ints(want)
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("%s: index lookup label=%q returned %v, want %v", where, label, got, want)
		}
	}
}

func cloneTwinModel(m map[int]twinRow) map[int]twinRow {
	out := make(map[int]twinRow, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

type twinExecer interface {
	Exec(string, ...any) (sql.Result, error)
}

// applyRandomTwinStatement runs one random statement against q and, when it
// succeeds, against model. Statements that must fail (duplicate keys) are
// included on purpose: their rollback restores rows and index entries in place.
func applyRandomTwinStatement(t *testing.T, q twinExecer, model map[int]twinRow, rng *rand.Rand, nextID *int) {
	t.Helper()
	ids := make([]int, 0, len(model))
	for id := range model {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	pick := func() int { return ids[rng.Intn(len(ids))] }
	switch op := rng.Intn(100); {
	case op < 30: // UPDATE of a column no index covers
		id, n := pick(), rng.Intn(1_000_000)
		if _, err := q.Exec(`UPDATE tw SET n = ? WHERE id = ?`, n, id); err != nil {
			t.Fatalf("update n: %v", err)
		}
		r := model[id]
		r.n = n
		model[id] = r
	case op < 50: // UPDATE of the indexed column
		id, label := pick(), fmt.Sprintf("l%d", rng.Intn(41))
		if _, err := q.Exec(`UPDATE tw SET label = ? WHERE id = ?`, label, id); err != nil {
			t.Fatalf("update label: %v", err)
		}
		r := model[id]
		r.label = label
		model[id] = r
	case op < 75: // INSERT
		id := *nextID
		*nextID++
		r := twinRow{label: fmt.Sprintf("l%d", rng.Intn(41)), n: id}
		if _, err := q.Exec(`INSERT INTO tw VALUES (?, ?, ?)`, id, r.label, r.n); err != nil {
			t.Fatalf("insert: %v", err)
		}
		model[id] = r
	case op < 82: // multi-row INSERT whose second row violates the primary key
		id := *nextID
		*nextID++
		if _, err := q.Exec(`INSERT INTO tw VALUES (?, 'dup', 1), (?, 'dup', 2)`, id, pick()); err == nil {
			t.Fatal("duplicate primary key was accepted")
		}
	case op < 88: // range UPDATE through the secondary index
		label, n := fmt.Sprintf("l%d", rng.Intn(41)), rng.Intn(1_000_000)
		if _, err := q.Exec(`UPDATE tw SET n = ? WHERE label = ?`, n, label); err != nil {
			t.Fatalf("update by label: %v", err)
		}
		for id, r := range model {
			if r.label == label {
				r.n = n
				model[id] = r
			}
		}
	default: // DELETE
		if len(ids) < 300 {
			return
		}
		id := pick()
		if _, err := q.Exec(`DELETE FROM tw WHERE id = ?`, id); err != nil {
			t.Fatalf("delete: %v", err)
		}
		delete(model, id)
	}
}

func TestTwinRecyclingMatchesModel(t *testing.T) {
	defer storage.SetTwinVerification(storage.SetTwinVerification(true))
	for seed := int64(1); seed <= 6; seed++ {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed))
			db, model := openTwinDB(t, 600)
			nextID := 1_000_000
			rowsBefore, indexesBefore := storage.TwinTakes()
			for round := 0; round < 250; round++ {
				where := fmt.Sprintf("seed %d round %d", seed, round)
				switch kind := rng.Intn(100); {
				case kind < 15: // autocommit between transactions
					applyRandomTwinStatement(t, db, model, rng, &nextID)
				case kind < 25: // a reader holds the BEGIN snapshot across a commit
					reader, err := db.Begin()
					if err != nil {
						t.Fatal(err)
					}
					snapshot := cloneTwinModel(model)
					writer, err := db.Begin()
					if err != nil {
						t.Fatal(err)
					}
					for i := 0; i < 1+rng.Intn(4); i++ {
						applyRandomTwinStatement(t, writer, model, rng, &nextID)
					}
					if err := writer.Commit(); err != nil {
						t.Fatalf("%s: commit: %v", where, err)
					}
					checkTwinModel(t, reader, snapshot, rng, where+" (reader snapshot)")
					if err := reader.Rollback(); err != nil {
						t.Fatal(err)
					}
				case kind < 35: // two writers: the second to commit must conflict
					first, err := db.Begin()
					if err != nil {
						t.Fatal(err)
					}
					second, err := db.Begin()
					if err != nil {
						t.Fatal(err)
					}
					loser := cloneTwinModel(model)
					applyRandomTwinStatement(t, second, loser, rng, &nextID)
					// Whatever the random statements do, give both
					// transactions a change to commit.
					for _, side := range []struct {
						tx    *sql.Tx
						model map[int]twinRow
					}{{second, loser}, {first, model}} {
						id := nextID
						nextID++
						if _, err := side.tx.Exec(`INSERT INTO tw VALUES (?, 'both', 0)`, id); err != nil {
							t.Fatal(err)
						}
						side.model[id] = twinRow{label: "both"}
					}
					for i := 0; i < 1+rng.Intn(3); i++ {
						applyRandomTwinStatement(t, first, model, rng, &nextID)
					}
					if err := first.Commit(); err != nil {
						t.Fatalf("%s: first commit: %v", where, err)
					}
					checkTwinModel(t, second, loser, rng, where+" (second writer)")
					if err := second.Commit(); !errors.Is(err, ErrTransactionConflict) {
						t.Fatalf("%s: second commit = %v, want a conflict", where, err)
					}
				default:
					tx, err := db.Begin()
					if err != nil {
						t.Fatal(err)
					}
					pending := cloneTwinModel(model)
					for i := 0; i < 1+rng.Intn(6); i++ {
						applyRandomTwinStatement(t, tx, pending, rng, &nextID)
					}
					checkTwinModel(t, tx, pending, rng, where+" (inside tx)")
					if rng.Intn(5) == 0 {
						if err := tx.Rollback(); err != nil {
							t.Fatal(err)
						}
					} else {
						if err := tx.Commit(); err != nil {
							t.Fatalf("%s: commit: %v", where, err)
						}
						model = pending
					}
				}
				checkTwinModel(t, db, model, rng, where)
			}
			rowsAfter, indexesAfter := storage.TwinTakes()
			if rowsAfter == rowsBefore || indexesAfter == indexesBefore {
				t.Fatalf("no twin was ever used (rows %d, indexes %d): the test no longer covers recycling",
					rowsAfter-rowsBefore, indexesAfter-indexesBefore)
			}
		})
	}
}

// A one-row transaction must not allocate in proportion to the table: that is
// what copying the row headers (24 bytes a row) or an index skip list costs.
func TestSmallTxAllocationIndependentOfTableSize(t *testing.T) {
	if testing.Short() {
		t.Skip("seeds a large table")
	}
	// The full comparison allocates what the twins save.
	defer storage.SetTwinVerification(storage.SetTwinVerification(false))
	db, _ := openTwinDB(t, 60_000)
	db.SetMaxOpenConns(1)
	const limit = 400 << 10
	i := 0
	run := func(stmt func(tx *sql.Tx) error) float64 {
		return bytesPerRun(40, func() {
			tx, err := db.Begin()
			if err != nil {
				t.Fatal(err)
			}
			if err := stmt(tx); err != nil {
				t.Fatal(err)
			}
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
			i++
		})
	}
	update := run(func(tx *sql.Tx) error {
		_, err := tx.Exec(`UPDATE tw SET n = ? WHERE id = ?`, 7_000_000+i, i%60_000)
		return err
	})
	insert := run(func(tx *sql.Tx) error {
		_, err := tx.Exec(`INSERT INTO tw VALUES (?, ?, ?)`, 5_000_000+i, "fresh", i)
		return err
	})
	// A transaction that changes nothing hands its copy back instead of
	// costing the next one a full copy.
	noop := run(func(tx *sql.Tx) error {
		_, err := tx.Exec(`UPDATE tw SET n = 1 WHERE id = -5`)
		return err
	})
	rolledBack := bytesPerRun(40, func() {
		tx, err := db.Begin()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(`UPDATE tw SET n = 1 WHERE id = -5`); err != nil {
			t.Fatal(err)
		}
		if err := tx.Rollback(); err != nil {
			t.Fatal(err)
		}
	})
	if noop > limit || rolledBack > limit {
		t.Fatalf("bytes per transaction that changes nothing: committed %.0f, rolled back %.0f; want below %d", noop, rolledBack, limit)
	}
	// The row headers alone are 1.4 MB for this table.
	if update > limit || insert > limit {
		t.Fatalf("bytes per one-row transaction: UPDATE %.0f, INSERT %.0f; want below %d", update, insert, limit)
	}
}

// bytesPerRun reports the average number of bytes fn allocates, after a few
// warm-up calls that let the twins form.
func bytesPerRun(runs int, fn func()) float64 {
	for i := 0; i < 4; i++ {
		fn()
	}
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for i := 0; i < runs; i++ {
		fn()
	}
	runtime.ReadMemStats(&after)
	return float64(after.TotalAlloc-before.TotalAlloc) / float64(runs)
}

// Writers, autocommit statements and snapshot readers run concurrently while
// the arrays change hands at every commit. Each transfer moves an amount
// between two rows, so every snapshot must see the same total; -race checks
// that a recycled array is never written while someone can still read it.
func TestTwinRecyclingUnderConcurrency(t *testing.T) {
	defer storage.SetTwinVerification(storage.SetTwinVerification(true))
	const rows = 1500
	db, model := openTwinDB(t, rows)
	total := 0
	for _, r := range model {
		total += r.n
	}
	done := make(chan struct{})
	errs := make(chan error, 16)
	var wg sync.WaitGroup
	fail := func(err error) {
		select {
		case errs <- err:
		default:
		}
	}
	for w := 0; w < 3; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(w) + 100))
			for i := 0; ; i++ {
				select {
				case <-done:
					return
				default:
				}
				a, b, amount := rng.Intn(rows), rng.Intn(rows), rng.Intn(50)
				if w == 2 && i%3 == 0 { // autocommit, one statement keeps the total
					if _, err := db.Exec(`UPDATE tw SET label = ? WHERE id = ?`, fmt.Sprintf("l%d", rng.Intn(41)), a); err != nil {
						fail(fmt.Errorf("autocommit: %w", err))
						return
					}
					continue
				}
				tx, err := db.Begin()
				if err != nil {
					fail(err)
					return
				}
				_, err1 := tx.Exec(`UPDATE tw SET n = n - ? WHERE id = ?`, amount, a)
				_, err2 := tx.Exec(`UPDATE tw SET n = n + ? WHERE id = ?`, amount, b)
				if err1 != nil || err2 != nil {
					_ = tx.Rollback()
					fail(fmt.Errorf("transfer: %v / %v", err1, err2))
					return
				}
				if i%7 == 0 {
					_ = tx.Rollback()
					continue
				}
				if err := tx.Commit(); err != nil && !errors.Is(err, ErrTransactionConflict) {
					fail(fmt.Errorf("commit: %w", err))
					return
				}
			}
		}(w)
	}
	for r := 0; r < 2; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
				}
				tx, err := db.Begin()
				if err != nil {
					fail(err)
					return
				}
				for pass := 0; pass < 2; pass++ {
					var sum, count int
					if err := tx.QueryRow(`SELECT SUM(n), COUNT(*) FROM tw`).Scan(&sum, &count); err != nil {
						fail(err)
						break
					}
					if sum != total || count != rows {
						fail(fmt.Errorf("snapshot saw sum=%d count=%d, want %d/%d", sum, count, total, rows))
						break
					}
				}
				_ = tx.Rollback()
			}
		}()
	}
	time.Sleep(400 * time.Millisecond)
	close(done)
	wg.Wait()
	select {
	case err := <-errs:
		t.Fatal(err)
	default:
	}
	var sum int
	if err := db.QueryRow(`SELECT SUM(n) FROM tw`).Scan(&sum); err != nil || sum != total {
		t.Fatalf("final sum = %d (%v), want %d", sum, err, total)
	}
}

func TestTwinRecyclingAfterJournalOverflow(t *testing.T) {
	defer storage.SetTwinVerification(storage.SetTwinVerification(true))
	db, model := openTwinDB(t, 5000)
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	// Each indexed update journals a removal and an insertion, exceeding
	// both the index journal and the row update log in this transaction.
	if _, err := tx.Exec("UPDATE tw SET label='bulk',n=n+1"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	for id, row := range model {
		row.label, row.n = "bulk", row.n+1
		model[id] = row
	}
	checkTwinModel(t, db, model, rand.New(rand.NewSource(71)), "journal overflow")
	// The following commits must work whether the buffers were recycled or
	// the incomplete journals forced ordinary copies.
	for id := range 5 {
		tx, err := db.Begin()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec("UPDATE tw SET label='after' WHERE id=?", id); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
		row := model[id]
		row.label = "after"
		model[id] = row
	}
	checkTwinModel(t, db, model, rand.New(rand.NewSource(72)), "after journal overflow")
}
