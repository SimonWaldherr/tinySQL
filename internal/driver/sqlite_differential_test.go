package driver

// This test suite compares the public database/sql behaviour of tinySQL with
// modernc SQLite for a deliberately small portable subset.  It is not a claim
// that every SQLite extension or documented tinySQL extension is shared; each
// query is chosen to be deterministic and supported by both engines.

import (
	"context"
	"database/sql"
	"encoding/base64"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"testing"

	_ "modernc.org/sqlite"
)

type sqliteDiffCell struct {
	kind  string
	value string
}

type sqliteDiffRows struct {
	columns []string
	rows    [][]sqliteDiffCell
}

type sqliteDiffQueryCase struct {
	name string
	sql  string
	args []any
}

func TestSQLiteDifferentialSelects(t *testing.T) {
	tests := []sqliteDiffQueryCase{
		{
			name: "bound-filter",
			sql: `SELECT id AS id, name AS name
				FROM customers
				WHERE credit >= ? AND name <> ?
				ORDER BY id`,
			args: []any{5.0, "Dora"},
		},
		{
			name: "null-coalesce",
			sql: `SELECT id AS id, COALESCE(nickname, ?) AS nickname
				FROM customers
				ORDER BY id`,
			args: []any{"(none)"},
		},
		{
			name: "three-valued-not",
			sql: `SELECT id AS id
				FROM customers
				WHERE NOT (credit = ?)
				ORDER BY id`,
			args: []any{0.0},
		},
		{
			name: "bound-in-and-range",
			sql: `SELECT id AS id, name AS name
				FROM customers
				WHERE id IN (?, ?, ?) AND credit BETWEEN ? AND ?
				ORDER BY id`,
			args: []any{1, 2, 4, 0.0, 10.0},
		},
		{
			name: "like-and-arithmetic",
			sql: `SELECT id AS id, amount * ? AS doubled
				FROM orders
				WHERE status LIKE ?
				ORDER BY id`,
			args: []any{2.0, "paid"},
		},
		{
			name: "case-expression",
			sql: `SELECT id AS id,
				CASE
					WHEN credit IS NULL THEN 'missing'
					WHEN credit >= ? THEN 'high'
					ELSE 'low'
				END AS band
				FROM customers
				ORDER BY id`,
			args: []any{10.0},
		},
		{
			name: "aggregate-having",
			sql: `SELECT status AS status, COUNT(*) AS n, SUM(amount) AS total
				FROM orders
				GROUP BY status
				HAVING COUNT(*) >= ?
				ORDER BY status`,
			args: []any{1},
		},
		{
			name: "count-distinct-ignores-null",
			sql:  `SELECT COUNT(DISTINCT customer_id) AS customer_count FROM orders`,
		},
		{
			name: "inner-join",
			sql: `SELECT c.id AS customer_id, c.name AS customer_name,
				o.id AS order_id, o.amount AS amount
				FROM customers AS c
				JOIN orders AS o ON c.id = o.customer_id
				WHERE o.status = ?
				ORDER BY o.id`,
			args: []any{"paid"},
		},
		{
			name: "left-join-grouped",
			sql: `SELECT c.id AS customer_id, COUNT(o.id) AS order_count
				FROM customers AS c
				LEFT JOIN orders AS o ON c.id = o.customer_id
				GROUP BY c.id
				ORDER BY c.id`,
		},
		{
			name: "cte-aggregate",
			sql: `WITH paid AS (
					SELECT customer_id, amount FROM orders WHERE status = ?
				)
				SELECT customer_id AS customer_id, SUM(amount) AS total
				FROM paid
				GROUP BY customer_id
				ORDER BY customer_id`,
			args: []any{"paid"},
		},
		{
			name: "single-row-in-subquery",
			sql: `SELECT id AS id, name AS name
				FROM customers
				WHERE id IN (SELECT customer_id FROM orders WHERE id = ?)
				ORDER BY id`,
			args: []any{12},
		},
		{
			name: "window-row-number",
			sql: `SELECT id AS id, status AS status,
				ROW_NUMBER() OVER (PARTITION BY status ORDER BY id) AS row_number
				FROM orders
				ORDER BY status, id`,
		},
		{
			name: "blob-binding",
			sql: `SELECT id AS id, payload AS payload
				FROM orders
				WHERE payload = ?
				ORDER BY id`,
			args: []any{[]byte{0x00, 0xff, 0x10}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tiny, sqlite := sqliteDiffOpenPair(t)
			sqliteDiffSeedFixture(t, tiny)
			sqliteDiffSeedFixture(t, sqlite)
			sqliteDiffCompareQuery(t, tiny, sqlite, tc.sql, tc.args...)
		})
	}
}

func TestSQLiteDifferentialDMLState(t *testing.T) {
	tiny, sqlite := sqliteDiffOpenPair(t)
	sqliteDiffSeedFixture(t, tiny)
	sqliteDiffSeedFixture(t, sqlite)

	const update = `UPDATE orders SET note = ? WHERE id = ?`
	for _, db := range []*sql.DB{tiny, sqlite} {
		if _, err := db.ExecContext(context.Background(), update, "revised", 11); err != nil {
			t.Fatalf("update %T: %v", db.Driver(), err)
		}
	}
	sqliteDiffCompareQuery(t, tiny, sqlite,
		`SELECT id AS id, note AS note FROM orders ORDER BY id`)
}

func TestSQLiteDifferentialReturning(t *testing.T) {
	tests := []sqliteDiffQueryCase{
		{
			name: "insert",
			sql: `INSERT INTO customers (id, name, region, credit, nickname)
				VALUES (?, ?, ?, ?, ?)
				RETURNING id AS id, name AS name, nickname AS nickname`,
			args: []any{99, "Eve", "eu", 12.5, "eve"},
		},
		{
			name: "update",
			sql: `UPDATE customers
				SET nickname = ?
				WHERE id = ?
				RETURNING id AS id, nickname AS nickname`,
			args: []any{"bee", 2},
		},
		{
			name: "delete",
			sql: `DELETE FROM customers
				WHERE id = ?
				RETURNING id AS id, name AS name`,
			args: []any{4},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tiny, sqlite := sqliteDiffOpenPair(t)
			sqliteDiffSeedFixture(t, tiny)
			sqliteDiffSeedFixture(t, sqlite)
			sqliteDiffCompareQuery(t, tiny, sqlite, tc.sql, tc.args...)
		})
	}
}

func TestSQLiteDifferentialPragmaTableInfo(t *testing.T) {
	tiny, sqlite := sqliteDiffOpenPair(t)
	const ddl = `CREATE TABLE pragma_probe (
		label TEXT NOT NULL DEFAULT 'general',
		retries INTEGER DEFAULT 3
	)`
	for _, db := range []*sql.DB{tiny, sqlite} {
		if _, err := db.ExecContext(context.Background(), ddl); err != nil {
			t.Fatalf("create pragma probe %T: %v", db.Driver(), err)
		}
	}
	sqliteDiffCompareQuery(t, tiny, sqlite, `PRAGMA table_info(pragma_probe)`)
}

func TestSQLiteDifferentialInsertRowsAffected(t *testing.T) {
	tiny, sqlite := sqliteDiffOpenPair(t)
	const ddl = `CREATE TABLE insert_counts (id INTEGER PRIMARY KEY, label TEXT)`
	const insert = `INSERT INTO insert_counts (id, label) VALUES (?, ?), (?, ?)`
	for _, db := range []*sql.DB{tiny, sqlite} {
		if _, err := db.ExecContext(context.Background(), ddl); err != nil {
			t.Fatalf("create insert_counts %T: %v", db.Driver(), err)
		}
	}

	tinyResult, err := tiny.ExecContext(context.Background(), insert, 1, "one", 2, "two")
	if err != nil {
		t.Fatalf("tinySQL insert: %v", err)
	}
	sqliteResult, err := sqlite.ExecContext(context.Background(), insert, 1, "one", 2, "two")
	if err != nil {
		t.Fatalf("SQLite insert: %v", err)
	}
	tinyCount, err := tinyResult.RowsAffected()
	if err != nil {
		t.Fatalf("tinySQL RowsAffected: %v", err)
	}
	sqliteCount, err := sqliteResult.RowsAffected()
	if err != nil {
		t.Fatalf("SQLite RowsAffected: %v", err)
	}
	if tinyCount != sqliteCount || tinyCount != 2 {
		t.Fatalf("INSERT RowsAffected: tinySQL=%d SQLite=%d, want 2", tinyCount, sqliteCount)
	}
}

func sqliteDiffOpenPair(t *testing.T) (*sql.DB, *sql.DB) {
	t.Helper()
	tiny, err := sql.Open("tinysql", "mem://?tenant=sqlite_differential")
	if err != nil {
		t.Fatalf("open tinySQL: %v", err)
	}
	sqlite, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		_ = tiny.Close()
		t.Fatalf("open SQLite: %v", err)
	}
	for _, db := range []*sql.DB{tiny, sqlite} {
		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(1)
		if err := db.PingContext(context.Background()); err != nil {
			_ = tiny.Close()
			_ = sqlite.Close()
			t.Fatalf("ping %T: %v", db.Driver(), err)
		}
	}
	t.Cleanup(func() {
		_ = tiny.Close()
		_ = sqlite.Close()
	})
	return tiny, sqlite
}

func sqliteDiffSeedFixture(t *testing.T, db *sql.DB) {
	t.Helper()
	ctx := context.Background()
	for _, ddl := range []string{
		`CREATE TABLE customers (
			id INTEGER PRIMARY KEY,
			name TEXT NOT NULL,
			region TEXT,
			credit REAL,
			nickname TEXT
		)`,
		`CREATE TABLE orders (
			id INTEGER PRIMARY KEY,
			customer_id INTEGER,
			amount REAL,
			status TEXT,
			note TEXT,
			payload BLOB
		)`,
		`CREATE INDEX orders_by_customer ON orders(customer_id)`,
	} {
		if _, err := db.ExecContext(ctx, ddl); err != nil {
			t.Fatalf("fixture DDL on %T: %v\n%s", db.Driver(), err, ddl)
		}
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin fixture transaction on %T: %v", db.Driver(), err)
	}
	fail := func(err error) {
		_ = tx.Rollback()
		t.Fatalf("fixture insert on %T: %v", db.Driver(), err)
	}
	for _, row := range [][]any{
		{1, "Ada", "eu", 100.5, "ada"},
		{2, "Bjarne", "us", 0.0, nil},
		{3, "Chloë", "eu", nil, "cee"},
		{4, "Dora", nil, 5.25, ""},
	} {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO customers (id, name, region, credit, nickname) VALUES (?, ?, ?, ?, ?)`, row...); err != nil {
			fail(err)
		}
	}
	for _, row := range [][]any{
		{10, 1, 20.0, "paid", "first", []byte{0x00, 0xff, 0x10}},
		{11, 1, 5.0, "open", nil, []byte{0x01, 0x02}},
		{12, 2, 7.5, "paid", "quoted ' text", []byte{0x03}},
		{13, 3, nil, "open", "unknown amount", nil},
		{14, nil, 3.0, "guest", "walk-in", []byte{0x00}},
	} {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO orders (id, customer_id, amount, status, note, payload) VALUES (?, ?, ?, ?, ?, ?)`, row...); err != nil {
			fail(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit fixture transaction on %T: %v", db.Driver(), err)
	}
}

func sqliteDiffCompareQuery(t *testing.T, tiny, sqlite *sql.DB, query string, args ...any) {
	t.Helper()
	tinyRows, err := sqliteDiffCollect(tiny, query, args...)
	if err != nil {
		t.Fatalf("tinySQL query failed: %v\n%s", err, query)
	}
	sqliteRows, err := sqliteDiffCollect(sqlite, query, args...)
	if err != nil {
		t.Fatalf("SQLite query failed: %v\n%s", err, query)
	}
	if !reflect.DeepEqual(tinyRows, sqliteRows) {
		t.Fatalf("query result mismatch\nSQL: %s\nargs: %#v\ntinySQL: %#v\nSQLite: %#v",
			query, args, tinyRows, sqliteRows)
	}
}

func sqliteDiffCollect(db *sql.DB, query string, args ...any) (sqliteDiffRows, error) {
	rows, err := db.QueryContext(context.Background(), query, args...)
	if err != nil {
		return sqliteDiffRows{}, err
	}
	columns, err := rows.Columns()
	if err != nil {
		_ = rows.Close()
		return sqliteDiffRows{}, err
	}
	result := sqliteDiffRows{
		columns: make([]string, len(columns)),
	}
	for i, column := range columns {
		result.columns[i] = strings.ToLower(column)
	}
	for rows.Next() {
		values := make([]any, len(columns))
		dest := make([]any, len(columns))
		for i := range values {
			dest[i] = &values[i]
		}
		if err := rows.Scan(dest...); err != nil {
			_ = rows.Close()
			return sqliteDiffRows{}, err
		}
		normalized := make([]sqliteDiffCell, len(values))
		for i, value := range values {
			cell, err := sqliteDiffNormalizeValue(value)
			if err != nil {
				_ = rows.Close()
				return sqliteDiffRows{}, fmt.Errorf("column %q: %w", columns[i], err)
			}
			normalized[i] = cell
		}
		result.rows = append(result.rows, normalized)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return sqliteDiffRows{}, err
	}
	if err := rows.Close(); err != nil {
		return sqliteDiffRows{}, err
	}
	return result, nil
}

func sqliteDiffNormalizeValue(value any) (sqliteDiffCell, error) {
	switch value := value.(type) {
	case nil:
		return sqliteDiffCell{kind: "null"}, nil
	case int:
		return sqliteDiffCell{kind: "number", value: strconv.Itoa(value)}, nil
	case int64:
		return sqliteDiffCell{kind: "number", value: strconv.FormatInt(value, 10)}, nil
	case float64:
		return sqliteDiffCell{kind: "number", value: strconv.FormatFloat(value, 'g', -1, 64)}, nil
	case bool:
		return sqliteDiffCell{kind: "bool", value: strconv.FormatBool(value)}, nil
	case string:
		return sqliteDiffCell{kind: "text", value: value}, nil
	case []byte:
		return sqliteDiffCell{kind: "blob", value: base64.StdEncoding.EncodeToString(value)}, nil
	default:
		return sqliteDiffCell{}, fmt.Errorf("unsupported scan type %T", value)
	}
}
