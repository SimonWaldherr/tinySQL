package engine

import (
	"context"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestDeleteRawScanRetainsRowsAndIndex(t *testing.T) {
	for _, indexed := range []bool{false, true} {
		for _, tc := range []struct {
			name, predicate string
			want            []int
		}{
			{"none", "id < 0", []int{0, 1, 2, 3, 4, 5}},
			{"first", "id = 0", []int{1, 2, 3, 4, 5}},
			{"middle", "id = 3", []int{0, 1, 2, 4, 5}},
			{"last", "id = 5", []int{0, 1, 2, 3, 4}},
			{"multiple", "id >= 2 AND id <= 4", []int{0, 1, 5}},
		} {
			t.Run(fmt.Sprintf("indexed=%t/%s", indexed, tc.name), func(t *testing.T) {
				db := storage.NewDB()
				t.Cleanup(func() { _ = db.Close() })
				execSQL(t, db, `CREATE TABLE lazy_delete (id INT, category TEXT)`)
				if indexed {
					execSQL(t, db, `CREATE INDEX lazy_category ON lazy_delete(category)`)
				}
				for i := 0; i < 6; i++ {
					execSQL(t, db, fmt.Sprintf(`INSERT INTO lazy_delete VALUES (%d,'c%d')`, i, i%2))
				}
				table, err := db.Get("default", "lazy_delete")
				if err != nil {
					t.Fatal(err)
				}
				version := table.Version
				result := execSQL(t, db, `DELETE FROM lazy_delete WHERE `+tc.predicate)
				if got := result.Rows[0]["deleted"]; got != 6-len(tc.want) {
					t.Fatalf("deleted = %v", got)
				}
				if table.Version != version+1 {
					t.Fatalf("version = %d, want %d", table.Version, version+1)
				}
				for category := 0; category < 2; category++ {
					rs := execSQL(t, db, fmt.Sprintf(`SELECT id FROM lazy_delete WHERE category = 'c%d' ORDER BY id`, category))
					got := make([]int, 0, len(rs.Rows))
					for _, r := range rs.Rows {
						got = append(got, r["id"].(int))
					}
					want := []int{}
					for _, id := range tc.want {
						if id%2 == category {
							want = append(want, id)
						}
					}
					if !reflect.DeepEqual(got, want) {
						t.Fatalf("category %d: got %v want %v", category, got, want)
					}
				}
			})
		}
	}
}

// An error after a matched row cannot publish a partial deletion or WAL record.
func TestDeleteRawScanFailureAfterMatchWithWAL(t *testing.T) {
	cfg := storage.StorageConfig{Mode: storage.ModeWAL, Path: filepath.Join(t.TempDir(), "db.wal")}
	db, err := storage.OpenDB(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{`CREATE TABLE lazy_fail (id INT, category TEXT)`, `CREATE INDEX lazy_fail_category ON lazy_fail(category)`, `INSERT INTO lazy_fail VALUES (0,'a'), (1,'b'), (2,'a')`} {
		execSQL(t, db, q)
	}
	_, err = Execute(context.Background(), db, "default", mustParse(`DELETE FROM lazy_fail WHERE id = 0 OR 1 / (id - 1) > 0`))
	if err == nil {
		t.Fatal("expected error after first matching row")
	}
	for _, q := range []string{`SELECT id FROM lazy_fail WHERE category = 'a' ORDER BY id`, `SELECT id FROM lazy_fail WHERE category = 'b' ORDER BY id`} {
		rs := execSQL(t, db, q)
		if len(rs.Rows) == 0 {
			t.Fatalf("missing rows after failed delete: %s", q)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := storage.OpenDB(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	rs := execSQL(t, reopened, `SELECT id FROM lazy_fail ORDER BY id`)
	if len(rs.Rows) != 3 || rs.Rows[0]["id"] != 0 || rs.Rows[1]["id"] != 1 || rs.Rows[2]["id"] != 2 {
		t.Fatalf("rows after WAL replay: %v", rs.Rows)
	}
}

type cancelAfterTwoChecks struct {
	context.Context
	done  chan struct{}
	calls int
}

func (c *cancelAfterTwoChecks) Done() <-chan struct{} {
	c.calls++
	if c.calls == 2 {
		close(c.done)
	}
	return c.done
}
func (c *cancelAfterTwoChecks) Err() error {
	if c.calls >= 2 {
		return context.Canceled
	}
	return nil
}

// Cancellation on the second scan check happens after row zero matched, but
// before the temporary kept slice can be published.
func TestDeleteRawScanCancellationAfterMatch(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	table := storage.NewTable("lazy_cancel", []storage.Column{{Name: "id", Type: storage.IntType}}, false)
	for i := 0; i < 130; i++ {
		table.Rows = append(table.Rows, []any{i})
	}
	if err := db.Put("default", table); err != nil {
		t.Fatal(err)
	}
	ctx := &cancelAfterTwoChecks{Context: context.Background(), done: make(chan struct{})}
	stmt := mustParse(`DELETE FROM lazy_cancel WHERE id = 0`).(*Delete)
	_, err := executeDelete(ExecEnv{ctx: ctx, db: db, tenant: "default"}, stmt)
	if err != context.Canceled || ctx.calls != 2 {
		t.Fatalf("got %v after %d checks", err, ctx.calls)
	}
	if len(table.Rows) != 130 || table.Rows[0][0] != 0 || table.Rows[129][0] != 129 {
		t.Fatalf("partial delete after cancellation: %v", len(table.Rows))
	}
}
