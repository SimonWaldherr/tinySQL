package engine

import (
	"context"
	"math/big"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestModuloOperator(t *testing.T) {
	db := storage.NewDB()
	for _, tc := range []struct {
		expr string
		want any
	}{
		{"17 % 5", float64(2)}, {"-17 % 5", float64(-2)}, {"17 % -5", float64(2)},
		{"5.5 % 2", float64(1.5)}, {"2 + 17 % 5 * 3", float64(8)},
		{"20 / 2 % 3", float64(1)}, {"NULL % 3", nil}, {"3 % NULL", nil},
	} {
		if got := queryScalar(t, db, tc.expr); got != tc.want {
			t.Errorf("%s = %#v, want %#v", tc.expr, got, tc.want)
		}
	}
	if _, err := Execute(context.Background(), db, "default", mustParse(`SELECT 3 % 0`)); err == nil {
		t.Fatal("zero divisor accepted")
	}
	// Decimal operands remain exact and the input rationals are not mutated.
	a := big.NewRat(-11, 2)
	b := big.NewRat(2, 1)
	got, err := evalArithmeticBinary("%", a, b)
	if err != nil || got.(*big.Rat).Cmp(big.NewRat(-3, 2)) != 0 || a.Cmp(big.NewRat(-11, 2)) != 0 || b.Cmp(big.NewRat(2, 1)) != 0 {
		t.Fatal(got, err)
	}
	execSQL(t, db, `CREATE TABLE numbers (id INT)`)
	execSQL(t, db, `INSERT INTO numbers VALUES (1), (2), (3), (4)`)
	rows := execSQL(t, db, `SELECT id % 2 AS remainder FROM numbers WHERE id % 2 = 0`).Rows
	if len(rows) != 2 || rows[0]["remainder"] != float64(0) {
		t.Fatal(rows)
	}
	rows = execSQL(t, db, `SELECT a.id % b.id AS remainder FROM numbers a JOIN numbers b ON a.id = b.id`).Rows
	if len(rows) != 4 {
		t.Fatal(rows)
	}
	for _, r := range rows {
		if r["remainder"] != float64(0) {
			t.Fatal(r)
		}
	}
	execSQL(t, db, `UPDATE numbers SET id = id % 2`)
	if len(execSQL(t, db, `SELECT id FROM numbers WHERE id = 0`).Rows) != 2 {
		t.Fatal("UPDATE modulo failed")
	}
}

func TestLeadingWithInsertAndNestedCTEs(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE target (id INT PRIMARY KEY)`)
	rs := execSQL(t, db, `WITH RECURSIVE seq(n) AS (
 SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < 4
 ) INSERT INTO target SELECT n FROM seq RETURNING id`)
	if len(rs.Rows) != 4 {
		t.Fatal(rs.Rows)
	}
	execSQL(t, db, `WITH source AS (SELECT 4 AS n UNION ALL SELECT 5)
 INSERT INTO target SELECT n FROM source ON CONFLICT DO NOTHING`)
	if len(execSQL(t, db, `SELECT * FROM target`).Rows) != 5 {
		t.Fatal("WITH INSERT conflict policy failed")
	}
	for sql, want := range map[string]int{
		`WITH outer_cte AS (WITH inner_cte AS (SELECT 7 AS n) SELECT n FROM inner_cte) SELECT n FROM outer_cte`:  7,
		`WITH c AS (SELECT 3 AS n), d AS (WITH c AS (SELECT 9 AS n) SELECT n FROM c) SELECT n FROM d`:            9,
		`WITH c AS (SELECT 3 AS n), d AS (WITH e AS (SELECT n + 1 AS n FROM c) SELECT n FROM e) SELECT n FROM d`: 4,
	} {
		rs := execSQL(t, db, sql)
		expectInt(t, rs.Rows[0]["n"], want, sql)
	}
	// A duplicate later in the SELECT must roll back the whole INSERT.
	if _, err := Execute(context.Background(), db, "default", mustParse(`WITH src AS (SELECT 6 AS n UNION ALL SELECT 1) INSERT INTO target SELECT n FROM src`)); err == nil {
		t.Fatal("duplicate must fail")
	}
	if len(execSQL(t, db, `SELECT * FROM target`).Rows) != 5 {
		t.Fatal("partial WITH INSERT survived")
	}
	if _, err := NewParser(`WITH c AS (SELECT 1) INSERT INTO target VALUES (8)`).ParseStatement(); err == nil {
		t.Fatal("unsupported WITH VALUES shape silently ignored")
	}
	// The new spelling also enters the ordinary delayed INSERT path.
	execSQL(t, db, `CALL DELAY_INSERT('0s', 'WITH c AS (SELECT 8 AS n) INSERT INTO target SELECT n FROM c')`)
	execSQL(t, db, `CALL FLUSH_DELAYED_INSERTS(1)`)
	if len(execSQL(t, db, `SELECT id FROM target WHERE id = 8`).Rows) != 1 {
		t.Fatal("delayed WITH INSERT failed")
	}
}

func TestViewsAsJoinSources(t *testing.T) {
	db := storage.NewDB()
	execSQL(t, db, `CREATE TABLE left_data (id INT)`)
	execSQL(t, db, `CREATE TABLE right_data (id INT, label TEXT)`)
	execSQL(t, db, `INSERT INTO left_data VALUES (1), (2)`)
	execSQL(t, db, `INSERT INTO right_data VALUES (1, 'one')`)
	execSQL(t, db, `CREATE VIEW labels AS SELECT id, label FROM right_data`)
	execSQL(t, db, `CREATE VIEW empty_labels AS SELECT id, label FROM right_data WHERE id = 99`)
	execSQL(t, db, `CREATE MATERIALIZED VIEW saved_labels AS SELECT id, label FROM right_data`)
	for _, source := range []string{"labels", "saved_labels"} {
		rs := execSQL(t, db, `SELECT l.id AS id, r.label AS label FROM left_data l LEFT JOIN `+source+` r ON l.id = r.id ORDER BY l.id`)
		if len(rs.Rows) != 2 || rs.Rows[0]["label"] != "one" || rs.Rows[1]["label"] != nil {
			t.Fatalf("%s: %#v", source, rs.Rows)
		}
		rs = execSQL(t, db, `SELECT l.id AS id FROM left_data l JOIN `+source+` r ON l.id = r.id`)
		if len(rs.Rows) != 1 {
			t.Fatal(rs.Rows)
		}
		rs = execSQL(t, db, `SELECT l.id AS id, COUNT(r.id) AS n FROM left_data l JOIN `+source+` r ON l.id = r.id GROUP BY l.id`)
		if len(rs.Rows) != 1 {
			t.Fatal(rs.Rows)
		}
	}
	rs := execSQL(t, db, `SELECT l.id AS id, r.label AS label FROM left_data l LEFT JOIN empty_labels r ON l.id = r.id`)
	if len(rs.Rows) != 2 {
		t.Fatal(rs.Rows)
	}
	for _, row := range rs.Rows {
		if row["label"] != nil {
			t.Fatal(row)
		}
	}
	// CTEs still shadow a view of the same name on the right of a JOIN.
	rs = execSQL(t, db, `WITH labels AS (SELECT 2 AS id, 'two' AS label) SELECT r.label AS label FROM left_data l JOIN labels r ON l.id = r.id`)
	if len(rs.Rows) != 1 || rs.Rows[0]["label"] != "two" {
		t.Fatal(rs.Rows)
	}
	// Ordinary views must see current data, not a cached prior expansion.
	execSQL(t, db, `INSERT INTO right_data VALUES (2, 'two')`)
	if len(execSQL(t, db, `SELECT r.id AS id FROM left_data l JOIN labels r ON l.id = r.id`).Rows) != 2 {
		t.Fatal("stale view")
	}
}
