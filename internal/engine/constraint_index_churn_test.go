package engine

import (
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// freshConstraintBuckets computes, from the table's rows alone, what a
// constraint index over column colIdx must contain: key -> row positions.
func freshConstraintBuckets(table *storage.Table, colIdx int) map[any][]int {
	want := map[any][]int{}
	for i, row := range table.Rows {
		if colIdx >= len(row) || row[colIdx] == nil {
			continue
		}
		k := comparableKeyPart(row[colIdx])
		want[k] = append(want[k], i)
	}
	return want
}

func sortedBuckets(m map[any][]int) map[any][]int {
	out := make(map[any][]int, len(m))
	for k, v := range m {
		c := append([]int(nil), v...)
		sort.Ints(c)
		out[k] = c
	}
	return out
}

// An INSERT leaves the table's constraint index one row behind until the next
// constrained check. A point DELETE right after must catch that entry up and
// patch it, not drop it: dropping made every INSERT/DELETE pair on a large
// table rebuild the whole index.
func TestInsertThenPointDeleteKeepsConstraintIndexWarm(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE q (id INT PRIMARY KEY, name TEXT)`)
	for i := 0; i < 50; i++ {
		execSQL(t, db, fmt.Sprintf(`INSERT INTO q VALUES (%d, 'r%d')`, i, i))
	}
	table, err := db.Get("default", "q")
	if err != nil {
		t.Fatal(err)
	}
	getConstraintIndex(table, 0) // warm

	for round := 0; round < 5; round++ {
		id := 1000 + round
		execSQL(t, db, fmt.Sprintf(`INSERT INTO q VALUES (%d, 'x')`, id))
		execSQL(t, db, fmt.Sprintf(`DELETE FROM q WHERE id = %d`, id))

		table.DerivedLock()
		set, _ := table.Derived().(*constraintIndexSet)
		var entry *constraintIndexEntry
		if set != nil {
			entry = set.cols[0]
		}
		table.DerivedUnlock()
		if entry == nil {
			t.Fatalf("round %d: constraint index was dropped by INSERT followed by point DELETE", round)
		}
		if entry.rowCount != len(table.Rows) {
			t.Fatalf("round %d: index covers %d rows, table has %d", round, entry.rowCount, len(table.Rows))
		}
		if got, want := sortedBuckets(entry.buckets()), sortedBuckets(freshConstraintBuckets(table, 0)); !reflect.DeepEqual(got, want) {
			t.Fatalf("round %d: index diverged from table rows\n got %v\nwant %v", round, got, want)
		}
	}
}

// patchConstraintIndexSwapRemove must bring a lagging entry up to date before
// patching it, and leave exactly what a rebuild would produce.
func TestSwapRemovePatchCatchesUpLaggingIndex(t *testing.T) {
	table := storage.NewTable("lag", []storage.Column{
		{Name: "id", Type: storage.IntType, Constraint: storage.PrimaryKey},
		{Name: "code", Type: storage.TextType, Constraint: storage.Unique},
	}, false)
	for i := 0; i < 10; i++ {
		table.Rows = append(table.Rows, []any{i, fmt.Sprintf("c%d", i)})
	}
	getConstraintIndex(table, 0)
	getConstraintIndex(table, 1)
	// Append three rows the index has not seen yet, like INSERT does.
	for i := 10; i < 13; i++ {
		table.Rows = append(table.Rows, []any{i, fmt.Sprintf("c%d", i)})
	}
	// middle row, then the last row, then the first
	for _, deleteRow := range []int{4, 11, 0} {
		last := len(table.Rows) - 1
		patchConstraintIndexSwapRemove(table, deleteRow, table.Rows[deleteRow], last, table.Rows[last], len(table.Rows))
		table.Rows[deleteRow] = table.Rows[last]
		table.Rows[last] = nil
		table.Rows = table.Rows[:last]
		for col := 0; col < 2; col++ {
			got := sortedBuckets(getConstraintIndex(table, col).buckets())
			want := sortedBuckets(freshConstraintBuckets(table, col))
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("after deleting row %d, column %d: index %v, rebuilt %v", deleteRow, col, got, want)
			}
		}
	}
}

// Random INSERT / point DELETE / UPDATE traffic against a model. Every step
// checks uniqueness enforcement, point lookups, and that the maintained index
// equals one rebuilt from the rows.
func TestConstraintIndexRandomChurnMatchesModel(t *testing.T) {
	for seed := int64(1); seed <= 6; seed++ {
		rng := rand.New(rand.NewSource(seed))
		db := storage.NewDB()
		ctx := context.Background()
		exec := func(sql string) (*ResultSet, error) { return Execute(ctx, db, "default", mustParse(sql)) }
		if _, err := exec(`CREATE TABLE c (id INT PRIMARY KEY, code TEXT UNIQUE, payload TEXT)`); err != nil {
			t.Fatal(err)
		}
		type rec struct{ code, payload string }
		model := map[int]rec{}
		codes := map[string]int{}
		table, err := db.Get("default", "c")
		if err != nil {
			t.Fatal(err)
		}

		for step := 0; step < 400; step++ {
			id := rng.Intn(60)
			code := fmt.Sprintf("k%d", rng.Intn(60))
			switch rng.Intn(5) {
			case 0, 1: // INSERT
				_, err := exec(fmt.Sprintf(`INSERT INTO c VALUES (%d, '%s', 'p%d')`, id, code, step))
				_, idTaken := model[id]
				_, codeTaken := codes[code]
				if idTaken || codeTaken {
					if err == nil {
						t.Fatalf("seed %d step %d: duplicate insert (%d,%s) accepted", seed, step, id, code)
					}
				} else {
					if err != nil {
						t.Fatalf("seed %d step %d: insert (%d,%s): %v", seed, step, id, code, err)
					}
					model[id] = rec{code, fmt.Sprintf("p%d", step)}
					codes[code] = id
				}
			case 2, 3: // point DELETE
				if _, err := exec(fmt.Sprintf(`DELETE FROM c WHERE id = %d`, id)); err != nil {
					t.Fatalf("seed %d step %d: delete: %v", seed, step, err)
				}
				if r, ok := model[id]; ok {
					delete(codes, r.code)
					delete(model, id)
				}
			default: // UPDATE the unique code
				_, err := exec(fmt.Sprintf(`UPDATE c SET code = '%s' WHERE id = %d`, code, id))
				r, ok := model[id]
				if !ok {
					if err != nil {
						t.Fatalf("seed %d step %d: update of missing row: %v", seed, step, err)
					}
					break
				}
				if owner, taken := codes[code]; taken && owner != id {
					if err == nil {
						t.Fatalf("seed %d step %d: update to taken code %s accepted", seed, step, code)
					}
					break
				}
				if err != nil {
					t.Fatalf("seed %d step %d: update: %v", seed, step, err)
				}
				delete(codes, r.code)
				r.code = code
				model[id] = r
				codes[code] = id
			}

			// Point lookup for an arbitrary id must agree with the model.
			probe := rng.Intn(60)
			rs, err := exec(fmt.Sprintf(`SELECT code FROM c WHERE id = %d`, probe))
			if err != nil {
				t.Fatal(err)
			}
			if want, ok := model[probe]; ok {
				if len(rs.Rows) != 1 || rs.Rows[0]["code"] != want.code {
					t.Fatalf("seed %d step %d: lookup id=%d got %v want code %s", seed, step, probe, rs.Rows, want.code)
				}
			} else if len(rs.Rows) != 0 {
				t.Fatalf("seed %d step %d: lookup of absent id=%d returned %v", seed, step, probe, rs.Rows)
			}
			for col := 0; col < 2; col++ {
				got := sortedBuckets(getConstraintIndex(table, col).buckets())
				want := sortedBuckets(freshConstraintBuckets(table, col))
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("seed %d step %d col %d: index %v != rebuilt %v", seed, step, col, got, want)
				}
			}
		}
		if len(table.Rows) != len(model) {
			t.Fatalf("seed %d: table has %d rows, model %d", seed, len(table.Rows), len(model))
		}
	}
}

// BenchmarkInsertDeleteChurnLargePKTable is queue-like traffic: insert a row,
// delete it by primary key, on a table that already holds many rows. The cost
// must not depend on the table's size.
func BenchmarkInsertDeleteChurnLargePKTable(b *testing.B) {
	for _, seedRows := range []int{10_000, 100_000} {
		b.Run(fmt.Sprintf("rows=%d", seedRows), func(b *testing.B) {
			db := storage.NewDB()
			ctx := context.Background()
			if _, err := Execute(ctx, db, "default", mustParse(`CREATE TABLE q (id INT PRIMARY KEY, name TEXT)`)); err != nil {
				b.Fatal(err)
			}
			table, err := db.Get("default", "q")
			if err != nil {
				b.Fatal(err)
			}
			table.Rows = make([][]any, seedRows)
			for i := range table.Rows {
				table.Rows[i] = []any{i, "r"}
			}
			ins := mustParse(`INSERT INTO q VALUES (-1, 'x')`)
			del := mustParse(`DELETE FROM q WHERE id = -1`)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := Execute(ctx, db, "default", ins); err != nil {
					b.Fatal(err)
				}
				if _, err := Execute(ctx, db, "default", del); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
