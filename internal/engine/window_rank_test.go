// Tests for the RANK, DENSE_RANK, PERCENT_RANK, CUME_DIST, and NTILE window
// functions. Before this, isWindowFuncName() (virtual_tables.go) already
// listed these as "known" window functions — used for introspection/
// validation — but evalWindowFunction had no case for them, so any query
// using them parsed fine and then failed at execution with "unsupported
// window function". These tests exercise the actual implementations.
package engine

import (
	"strconv"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func setupRankTable(t *testing.T) *storage.DB {
	t.Helper()
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE scores (name TEXT, score INT)`)
	// Ties on score: 90 (Alice, Ben), 80 (Cara), 70 (Dan, Eve), 60 (Fay)
	rows := [][2]any{
		{"Alice", 90}, {"Ben", 90}, {"Cara", 80},
		{"Dan", 70}, {"Eve", 70}, {"Fay", 60},
	}
	for _, r := range rows {
		execSQL(t, db, `INSERT INTO scores VALUES ('`+r[0].(string)+`', `+strconv.Itoa(r[1].(int))+`)`)
	}
	return db
}

func TestRankWithTies(t *testing.T) {
	db := setupRankTable(t)
	rs := execSQL(t, db, `SELECT name, score, RANK() OVER (ORDER BY score DESC) AS r FROM scores ORDER BY score DESC, name`)
	want := map[string]int{
		"Alice": 1, "Ben": 1, // tied for 1st
		"Cara": 3,           // skips to 3rd (rank isn't dense)
		"Dan":  4, "Eve": 4, // tied for 4th
		"Fay": 6, // skips to 6th
	}
	if len(rs.Rows) != 6 {
		t.Fatalf("expected 6 rows, got %d", len(rs.Rows))
	}
	for _, row := range rs.Rows {
		name := row["name"].(string)
		expectInt(t, row["r"], want[name], "RANK for "+name)
	}
}

func TestDenseRankWithTies(t *testing.T) {
	db := setupRankTable(t)
	rs := execSQL(t, db, `SELECT name, DENSE_RANK() OVER (ORDER BY score DESC) AS r FROM scores ORDER BY score DESC, name`)
	want := map[string]int{
		"Alice": 1, "Ben": 1,
		"Cara": 2, // no gap
		"Dan":  3, "Eve": 3,
		"Fay": 4, // no gap
	}
	for _, row := range rs.Rows {
		name := row["name"].(string)
		expectInt(t, row["r"], want[name], "DENSE_RANK for "+name)
	}
}

func TestRankNoOrderByFallsBackToPosition(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE t (id INT)`)
	execSQL(t, db, `INSERT INTO t VALUES (1)`)
	execSQL(t, db, `INSERT INTO t VALUES (2)`)
	rs := execSQL(t, db, `SELECT id, RANK() OVER () AS r FROM t ORDER BY id`)
	for i, row := range rs.Rows {
		expectInt(t, row["r"], i+1, "RANK without ORDER BY")
	}
}

func TestPercentRankAndCumeDist(t *testing.T) {
	db := setupRankTable(t)
	rs := execSQL(t, db, `SELECT name, PERCENT_RANK() OVER (ORDER BY score DESC) AS pr, CUME_DIST() OVER (ORDER BY score DESC) AS cd FROM scores ORDER BY score DESC, name`)
	if len(rs.Rows) != 6 {
		t.Fatalf("expected 6 rows, got %d", len(rs.Rows))
	}
	// Alice/Ben: rank 1 -> percent_rank = 0/(6-1) = 0; cume_dist covers both tied rows = 2/6
	expectFloat(t, rs.Rows[0]["pr"], 0.0, 1e-9, "Alice percent_rank")
	expectFloat(t, rs.Rows[0]["cd"], 2.0/6.0, 1e-9, "Alice cume_dist")
	expectFloat(t, rs.Rows[1]["pr"], 0.0, 1e-9, "Ben percent_rank")
	expectFloat(t, rs.Rows[1]["cd"], 2.0/6.0, 1e-9, "Ben cume_dist")
	// Fay: last row, rank 6 -> percent_rank = (6-1)/(6-1) = 1; cume_dist = 6/6 = 1
	last := rs.Rows[5]
	expectFloat(t, last["pr"], 1.0, 1e-9, "Fay percent_rank")
	expectFloat(t, last["cd"], 1.0, 1e-9, "Fay cume_dist")
}

func TestNtileEvenAndUnevenSplit(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE t (id INT)`)
	for i := 1; i <= 10; i++ {
		execSQL(t, db, `INSERT INTO t VALUES (`+strconv.Itoa(i)+`)`)
	}
	// 10 rows into 4 buckets: sizes 3,3,2,2 (remainder=2 buckets get +1)
	rs := execSQL(t, db, `SELECT id, NTILE(4) OVER (ORDER BY id) AS bucket FROM t ORDER BY id`)
	wantBucket := []int{1, 1, 1, 2, 2, 2, 3, 3, 4, 4}
	if len(rs.Rows) != 10 {
		t.Fatalf("expected 10 rows, got %d", len(rs.Rows))
	}
	for i, row := range rs.Rows {
		expectInt(t, row["bucket"], wantBucket[i], "NTILE bucket for row "+strconv.Itoa(i))
	}

	// Even split: 10 rows into 5 buckets of 2 each.
	rs = execSQL(t, db, `SELECT id, NTILE(5) OVER (ORDER BY id) AS bucket FROM t ORDER BY id`)
	wantEven := []int{1, 1, 2, 2, 3, 3, 4, 4, 5, 5}
	for i, row := range rs.Rows {
		expectInt(t, row["bucket"], wantEven[i], "NTILE(5) bucket for row "+strconv.Itoa(i))
	}
}

func TestNtileMoreBucketsThanRows(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE t (id INT)`)
	execSQL(t, db, `INSERT INTO t VALUES (1)`)
	execSQL(t, db, `INSERT INTO t VALUES (2)`)
	rs := execSQL(t, db, `SELECT id, NTILE(5) OVER (ORDER BY id) AS bucket FROM t ORDER BY id`)
	// 2 rows, 5 buckets: each row gets its own bucket (1, 2), no divide-by-zero.
	expectInt(t, rs.Rows[0]["bucket"], 1, "row0 bucket")
	expectInt(t, rs.Rows[1]["bucket"], 2, "row1 bucket")
}

func TestLagLeadNegativeOffsetReturnsDefaultInsteadOfPanicking(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE t (id INT)`)
	execSQL(t, db, `INSERT INTO t VALUES (1)`)
	execSQL(t, db, `INSERT INTO t VALUES (2)`)
	execSQL(t, db, `INSERT INTO t VALUES (3)`)

	// A negative offset drives lagIdx/leadIdx out of [0, len(partitionRows))
	// on the side the pre-fix bounds check didn't cover (lagIdx's upper bound,
	// leadIdx's lower bound), indexing partitionRows out of range instead of
	// returning the supplied default.
	rs := execSQL(t, db, `SELECT id, LAG(id, -5, -1) OVER (ORDER BY id) AS lg, LEAD(id, -5, -1) OVER (ORDER BY id) AS ld FROM t ORDER BY id`)
	if len(rs.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rs.Rows))
	}
	for _, row := range rs.Rows {
		expectInt(t, row["lg"], -1, "LAG with out-of-range negative offset")
		expectInt(t, row["ld"], -1, "LEAD with out-of-range negative offset")
	}
}

func TestNtileRejectsNonPositive(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE t (id INT)`)
	execSQL(t, db, `INSERT INTO t VALUES (1)`)
	if _, err := Execute(t.Context(), db, "default", mustParse(`SELECT NTILE(0) OVER (ORDER BY id) FROM t`)); err == nil {
		t.Fatal("expected error for NTILE(0)")
	}
}
