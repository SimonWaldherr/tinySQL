package engine

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestApplyCorrelatedSources(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	execSQL(t, db, `CREATE TABLE customers (id INT, name TEXT)`)
	execSQL(t, db, `INSERT INTO customers VALUES (1,'first'),(2,'second'),(3,'third')`)
	execSQL(t, db, `CREATE TABLE orders (customer INT, amount INT)`)
	execSQL(t, db, `INSERT INTO orders VALUES (1,17),(1,20),(3,13),(NULL,22)`)
	for _, tc := range []struct {
		typeSQL string
		want    []Row
	}{
		{"CROSS APPLY", []Row{{"id": 1, "amount": 20}, {"id": 3, "amount": 13}}},
		{"OUTER APPLY", []Row{{"id": 1, "amount": 20}, {"id": 2, "amount": nil}, {"id": 3, "amount": 13}}},
	} {
		sql := `SELECT c.id AS id, recent.amount AS amount FROM customers AS c ` + tc.typeSQL + `
			(SELECT o.amount FROM orders AS o WHERE o.customer = c.id ORDER BY amount DESC LIMIT 1) AS recent
			ORDER BY id`
		got := execSQL(t, db, sql)
		if !reflect.DeepEqual(got.Rows, tc.want) {
			t.Fatalf("%s rows = %#v, want %#v", tc.typeSQL, got.Rows, tc.want)
		}
	}
	got := execSQL(t, db, `SELECT c.id AS id, y.n AS n FROM customers AS c
		CROSS APPLY (SELECT (SELECT id) AS x) AS a
		CROSS APPLY (SELECT a.x + c.id AS n) AS y ORDER BY id`)
	for i, row := range got.Rows {
		if row["id"] != i+1 || row["n"] != 2*(i+1) {
			t.Fatalf("unexpected nested binding: %#v", got.Rows)
		}
	}
	got = execSQL(t, db, `SELECT * FROM customers AS c
		CROSS APPLY (SELECT c.id AS order_id WHERE c.id < 0) AS x`)
	if len(got.Rows) != 0 || !reflect.DeepEqual(got.Cols, []string{"id", "name", "order_id"}) {
		t.Fatalf("unexpected empty result schema: %#v", got)
	}
	got = execSQL(t, db, `SELECT * FROM (SELECT id, name FROM customers WHERE id < 0) AS c
		OUTER APPLY (SELECT c.id AS order_id) AS x`)
	if len(got.Rows) != 0 || !reflect.DeepEqual(got.Cols, []string{"id", "name", "order_id"}) {
		t.Fatalf("unexpected empty left schema: %#v", got)
	}
	got = execSQL(t, db, `SELECT c.id AS id, o.amount AS amount FROM customers AS c OUTER APPLY orders AS o ORDER BY id, amount`)
	if len(got.Rows) != 12 {
		t.Fatalf("static APPLY rows = %d, want 12", len(got.Rows))
	}
	execSQL(t, db, `DELETE FROM orders`)
	got = execSQL(t, db, `SELECT c.id AS id, o.amount AS amount FROM customers AS c OUTER APPLY orders AS o ORDER BY id`)
	if len(got.Rows) != 3 || got.Rows[0]["amount"] != nil {
		t.Fatalf("unexpected empty static APPLY: %#v", got.Rows)
	}
}

type applyTestTableFunc struct{ calls int }

func (*applyTestTableFunc) Name() string { return "APPLY_TEST_ROWS" }
func (*applyTestTableFunc) ValidateArgCount(n int) error {
	if n != 1 {
		return fmt.Errorf("expected one argument")
	}
	return nil
}

func (fn *applyTestTableFunc) Execute(_ context.Context, args []any) (*ResultSet, error) {
	fn.calls++
	result := &ResultSet{Cols: []string{"value"}}
	if n := args[0].(int); n%2 == 1 {
		result.Rows = []Row{{"value": n * 10}}
	}
	return result, nil
}

func TestApplyTableFunction(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	fn := new(applyTestTableFunc)
	RegisterExternalTableFunc(fn)
	t.Cleanup(func() { delete(tableFuncRegistry, fn.Name()) })
	execSQL(t, db, `CREATE TABLE numbers (id INT)`)
	execSQL(t, db, `INSERT INTO numbers VALUES (1),(2),(3)`)
	got := execSQL(t, db, `SELECT n.id AS id, r.value AS value FROM numbers AS n OUTER APPLY APPLY_TEST_ROWS(n.id) AS r ORDER BY id`)
	want := []Row{{"id": 1, "value": 10}, {"id": 2, "value": nil}, {"id": 3, "value": 30}}
	if fn.calls != 3 || !reflect.DeepEqual(got.Rows, want) {
		t.Fatalf("table function calls=%d rows=%#v, want 3 calls and %#v", fn.calls, got.Rows, want)
	}
	got = execSQL(t, db, `SELECT * FROM (SELECT id FROM numbers WHERE id = 2) AS n CROSS APPLY APPLY_TEST_ROWS(n.id) AS r`)
	if fn.calls != 4 || len(got.Rows) != 0 || !reflect.DeepEqual(got.Cols, []string{"id", "value"}) {
		t.Fatalf("empty table function calls=%d result=%#v", fn.calls, got)
	}
}

func TestApplyQualifierAndViewSchema(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	execSQL(t, db, `CREATE TABLE accounts (id INT, name TEXT)`)
	execSQL(t, db, `CREATE TABLE amounts (amount INT)`)
	execSQL(t, db, `CREATE VIEW amount_view AS SELECT * FROM amounts`)
	execSQL(t, db, `INSERT INTO accounts VALUES (1, 'first')`)
	execSQL(t, db, `INSERT INTO amounts VALUES (5)`)
	_, err := Execute(t.Context(), db, "default", mustParse(`SELECT r.name FROM accounts AS q
		CROSS APPLY (SELECT q.name FROM amounts AS q) AS r`))
	if err == nil || !strings.Contains(err.Error(), "q.name") {
		t.Fatalf("a local qualifier must shadow the outer qualifier, got %v", err)
	}
	got := execSQL(t, db, `SELECT * FROM (SELECT id FROM accounts WHERE id < 0) AS q
		CROSS APPLY (SELECT * FROM amount_view) AS r`)
	if len(got.Rows) != 0 || !reflect.DeepEqual(got.Cols, []string{"amount", "id"}) {
		t.Fatalf("unexpected view schema: %#v", got)
	}
	got = execSQL(t, db, `SELECT r.amount AS amount FROM accounts AS q
		CROSS APPLY (SELECT amount FROM amounts ORDER BY q.id LIMIT 1) AS r`)
	if !reflect.DeepEqual(got.Rows, []Row{{"amount": 5}}) {
		t.Fatalf("unexpected sorted right-hand result: %#v", got)
	}
}

const applyLookupQuery = `SELECT c.id AS id, a.amount AS amount FROM customers AS c OUTER APPLY (
	SELECT o.amount FROM orders AS o WHERE c.id = o.customer ORDER BY amount DESC LIMIT 1
) AS a`

func setupApplyLookup(tb testing.TB) *storage.DB {
	tb.Helper()
	db := storage.NewDB()
	tb.Cleanup(func() { _ = db.Close() })
	customers := storage.NewTable("customers", []storage.Column{{Name: "id", Type: storage.IntType}}, false)
	orders := storage.NewTable("orders", []storage.Column{{Name: "customer", Type: storage.IntType}, {Name: "amount", Type: storage.IntType}}, false)
	for i := range 120 {
		customers.Rows = append(customers.Rows, []any{i})
		for j := range 8 {
			if i%3 != 0 {
				orders.Rows = append(orders.Rows, []any{i, i*10 + j})
			}
		}
	}
	orders.Rows = append(orders.Rows, []any{nil, 1000})
	if err := db.Put("default", customers); err != nil {
		tb.Fatal(err)
	}
	if err := db.Put("default", orders); err != nil {
		tb.Fatal(err)
	}
	return db
}

func TestApplySharedLookup(t *testing.T) {
	db := setupApplyLookup(t)
	result, profile, err := ExecuteWithProfile(t.Context(), db, "default", mustParse(applyLookupQuery))
	if err != nil {
		t.Fatal(err)
	}
	stages := checkExecutionProfile(t, profile, 120)
	if stages["APPLY LOOKUP"].Calls != 1 || stages["OUTER APPLY"].Rows != 120 {
		t.Fatalf("query missed APPLY lookup: %+v", profile)
	}
	for i, row := range result.Rows {
		var amount any
		if i%3 != 0 {
			amount = i*10 + 7
		}
		if row["id"] != i || row["amount"] != amount {
			t.Fatalf("row %d = %#v, want amount %v", i, row, amount)
		}
	}
	_, err = Execute(t.Context(), db, "default", mustParse(strings.Replace(applyLookupQuery, "o.customer", "o.unknown", 1)))
	if err == nil || !strings.Contains(err.Error(), "o.unknown") {
		t.Fatalf("unknown right column error = %v", err)
	}
}

func BenchmarkApplySharedLookup(b *testing.B) {
	db := setupApplyLookup(b)
	runBench(b, db, applyLookupQuery)
}
