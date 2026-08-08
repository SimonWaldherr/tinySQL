// Coverage for exec_dml_insert.go's use of storage.DB.AppendRowsFast: ordinary
// SQL INSERT against a ModePagedIndex-backed table should durably persist new
// rows to the backend's on-disk B+Trees as part of the INSERT statement
// itself (via the incremental-append fast path), without ever needing an
// explicit db.Sync -- and constraint enforcement (PRIMARY KEY / UNIQUE /
// FOREIGN KEY / NOT NULL) must remain exactly as strict as it is for any
// other storage mode, rejecting a bad row before it ever reaches that fast
// path.
package engine

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// TestPagedIndexInsertUsesFastPathAppend confirms INSERT durably writes new
// rows to the paged-index backend immediately, without an explicit Sync.
// ScanRowsFast reads straight from the backend's pager, bypassing the
// in-memory Table entirely, so it only sees rows if AppendRowsFast (not just
// the in-memory t.Rows append) actually ran.
func TestPagedIndexInsertUsesFastPathAppend(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "fastpath")
	db, err := storage.OpenDB(storage.StorageConfig{Mode: storage.ModePagedIndex, Path: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	execConstraintSQL(t, ctx, db, `CREATE TABLE t (id INT PRIMARY KEY, val TEXT)`)
	execConstraintSQL(t, ctx, db, `INSERT INTO t VALUES (1, 'a')`)
	execConstraintSQL(t, ctx, db, `INSERT INTO t VALUES (2, 'b'), (3, 'c')`)

	var onDisk [][]any
	ok, err := db.ScanRowsFast("default", "t", func(row []any) bool {
		onDisk = append(onDisk, row)
		return true
	})
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("ScanRowsFast unavailable for a ModePagedIndex table")
	}
	if len(onDisk) != 3 {
		t.Fatalf("rows visible on the backend without ever calling db.Sync = %d, want 3 (%#v)", len(onDisk), onDisk)
	}

	// The in-memory view must agree with the on-disk fast-path view.
	rs := queryConstraintSQL(t, ctx, db, `SELECT COUNT(*) AS n FROM t`)
	expectInt(t, rs.Rows[0]["n"], 3, "in-memory row count")
}

// TestPagedIndexInsertConstraintViolationsStillRejected is the ModePagedIndex
// analogue of the storage.NewDB() constraint tests in constraints_test.go: a
// backend fast-append path for successful rows must not weaken PRIMARY KEY,
// UNIQUE, FOREIGN KEY, or NOT NULL enforcement, and a rejected row must reach
// neither the in-memory table nor the backend.
func TestPagedIndexInsertConstraintViolationsStillRejected(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "constraints")
	db, err := storage.OpenDB(storage.StorageConfig{Mode: storage.ModePagedIndex, Path: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	execConstraintSQL(t, ctx, db, `CREATE TABLE parents (id INT PRIMARY KEY)`)
	execConstraintSQL(t, ctx, db, `INSERT INTO parents VALUES (1)`)
	execConstraintSQL(t, ctx, db,
		`CREATE TABLE children (id INT PRIMARY KEY, parent_id INT FOREIGN KEY REFERENCES parents(id), email TEXT UNIQUE, name TEXT NOT NULL)`)
	execConstraintSQL(t, ctx, db, `INSERT INTO children VALUES (10, 1, 'a@example.test', 'Alice')`)

	expectConstraintErr(t, ctx, db, `INSERT INTO children VALUES (10, 1, 'z@example.test', 'Zed')`, "PRIMARY KEY")
	expectConstraintErr(t, ctx, db, `INSERT INTO children VALUES (11, 1, 'a@example.test', 'Zed')`, "UNIQUE")
	expectConstraintErr(t, ctx, db, `INSERT INTO children VALUES (12, 99, 'c@example.test', 'Carl')`, "FOREIGN KEY")
	expectConstraintErr(t, ctx, db, `INSERT INTO children (id, parent_id, email) VALUES (13, 1, 'd@example.test')`, "NOT NULL")

	rs := queryConstraintSQL(t, ctx, db, `SELECT COUNT(*) AS n FROM children`)
	expectInt(t, rs.Rows[0]["n"], 1, "in-memory row count after rejected inserts")

	// None of the rejected rows may have reached the backend either: this
	// exercises that AppendRowsFast is only ever called (from
	// tryFastPathAppend) with rows that already passed every constraint
	// check, exactly as it would be if this fast path did not exist and
	// everything relied on db.Sync's full backend.SaveTable rewrite instead.
	var onDisk int
	ok, err := db.ScanRowsFast("default", "children", func(row []any) bool { onDisk++; return true })
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("ScanRowsFast unavailable for a ModePagedIndex table")
	}
	if onDisk != 1 {
		t.Fatalf("rows durably persisted after rejected inserts = %d, want 1", onDisk)
	}

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	reader, err := storage.OpenDB(storage.StorageConfig{Mode: storage.ModePagedIndex, Path: dir, ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	rs2, err := Execute(ctx, reader, "default", mustParse(`SELECT id, name, email FROM children ORDER BY id`))
	if err != nil {
		t.Fatal(err)
	}
	if len(rs2.Rows) != 1 || rs2.Rows[0]["id"] != float64(10) || rs2.Rows[0]["name"] != "Alice" {
		t.Fatalf("reopened children rows after close = %#v, want exactly Alice's row", rs2.Rows)
	}
}

// TestPagedIndexInsertFastPathIgnoredForOtherModes guards the fall-through
// side of tryFastPathAppend: AppendRowsFast reports ok=false for anything
// other than a ModePagedIndex backend, and ordinary in-memory INSERT must
// keep working exactly as before when that happens.
func TestPagedIndexInsertFastPathIgnoredForOtherModes(t *testing.T) {
	ctx := context.Background()
	db := storage.NewDB()

	execConstraintSQL(t, ctx, db, `CREATE TABLE t (id INT PRIMARY KEY, val TEXT)`)
	execConstraintSQL(t, ctx, db, `INSERT INTO t VALUES (1, 'a'), (2, 'b')`)

	rs := queryConstraintSQL(t, ctx, db, `SELECT COUNT(*) AS n FROM t`)
	expectInt(t, rs.Rows[0]["n"], 2, "row count on a non-paged-index backend")

	ok, err := db.AppendRowsFast("default", "t", [][]any{{float64(3), "c"}})
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("AppendRowsFast reported ok=true against a non-paged-index DB")
	}
}

// benchmarkPagedIndexInsertOneRowAtATime seeds a ModePagedIndex-backed table
// with seedRows rows (persisted once via db.Sync, so it is genuinely on disk
// -- not just resident in the process that wrote it), then times b.N
// separate single-row INSERT statements against it, one at a time. With
// tryFastPathAppend wired in, each iteration's own backend cost is an
// incremental B+Tree append proportional to the one new row, not to
// seedRows -- so ns/op and B/op should stay essentially flat as seedRows
// grows, unlike the full backend.SaveTable rewrite this fast path replaces
// (which is proportional to the whole table and would show up here as
// ns/op growing with seedRows).
func benchmarkPagedIndexInsertOneRowAtATime(b *testing.B, seedRows int) {
	ctx := context.Background()
	dir := filepath.Join(b.TempDir(), "insert_scaling")
	db, err := storage.OpenDB(storage.StorageConfig{Mode: storage.ModePagedIndex, Path: dir})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	if _, err := Execute(ctx, db, "default", mustParse(`CREATE TABLE t (id INT PRIMARY KEY, val TEXT)`)); err != nil {
		b.Fatal(err)
	}
	table, err := db.Get("default", "t")
	if err != nil {
		b.Fatal(err)
	}
	table.Rows = make([][]any, seedRows)
	for i := 0; i < seedRows; i++ {
		table.Rows[i] = []any{float64(i), fmt.Sprintf("val-%d", i)}
	}
	table.Version++
	if err := db.Sync(); err != nil {
		b.Fatal(err)
	}

	stmts := make([]Statement, b.N)
	for i := 0; i < b.N; i++ {
		stmts[i] = mustParse(fmt.Sprintf(`INSERT INTO t VALUES (%d, 'new-%d')`, seedRows+i, i))
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := Execute(ctx, db, "default", stmts[i]); err != nil {
			b.Fatal(err)
		}
	}
	// Stop the timer before the deferred db.Close() above runs: Close does a
	// final Sync (which, absent this fast path, is a full backend.SaveTable
	// rewrite whose cost is proportional to the whole table). Leaving the
	// timer running would attribute that one-time, table-size-proportional
	// cost across all b.N iterations and make this benchmark's whole point
	// -- that a single INSERT's own cost does not grow with table size --
	// impossible to measure correctly.
	b.StopTimer()
}

// BenchmarkPagedIndexInsertOneRowAtATimeSmallTable and
// ...LargeTable seed the same shape of table at two very different sizes so
// their ns/op and (more reliably, given this machine's run-to-run timing
// noise) B/op can be compared directly against each other, in addition to
// the usual before/after-this-change comparison: if later inserts truly
// cost O(new rows) and not O(existing table size), these two should be
// close to each other despite a 50x difference in seedRows.
func BenchmarkPagedIndexInsertOneRowAtATimeSmallTable(b *testing.B) {
	benchmarkPagedIndexInsertOneRowAtATime(b, 2_000)
}

func BenchmarkPagedIndexInsertOneRowAtATimeLargeTable(b *testing.B) {
	benchmarkPagedIndexInsertOneRowAtATime(b, 100_000)
}
