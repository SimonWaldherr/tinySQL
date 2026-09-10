package engine

import (
	"reflect"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestSubscriptionGeneralSelectMatchesExecutor(t *testing.T) {
	queries := []string{
		"SELECT * FROM summary",
		"SELECT * FROM nested_summary",
		"SELECT * FROM (SELECT category, SUM(amount) AS total FROM sales GROUP BY category) AS totals",
		"SELECT * FROM sales s JOIN labels l ON s.category = l.id",
		"SELECT category FROM sales EXCEPT SELECT id FROM labels",
		"SELECT category FROM sales INTERSECT SELECT id FROM labels",
		"SELECT category, COUNT(*) AS n, SUM(amount) AS total FROM sales GROUP BY category",
		"SELECT category, SUM(amount) AS total FROM sales GROUP BY category HAVING SUM(amount) > 10",
		"SELECT COUNT(*) AS n FROM sales",
		"SELECT s.id, l.name FROM sales s JOIN labels l ON s.category = l.id",
		"SELECT s.id, l.name FROM sales s LEFT JOIN labels l ON s.category = l.id",
		"SELECT DISTINCT category FROM sales",
		"SELECT id, amount * 2 AS doubled FROM sales WHERE amount > 0",
		"SELECT id FROM sales ORDER BY amount DESC, id LIMIT 2 OFFSET 1",
		"WITH totals AS (SELECT category, SUM(amount) AS total FROM sales GROUP BY category) SELECT * FROM totals",
		"SELECT category FROM sales UNION ALL SELECT category FROM sales",
		"SELECT category FROM sales UNION SELECT id FROM labels",
		"SELECT id FROM sales WHERE amount > (SELECT MIN(amount) FROM sales)",
		"SELECT id, ROW_NUMBER() OVER (ORDER BY amount, id) AS position FROM sales",
	}
	for _, query := range queries {
		t.Run(query, func(t *testing.T) {
			db := storage.NewDB()
			defer db.Close()
			exec := func(q string) *ResultSet {
				t.Helper()
				rs, err := Execute(t.Context(), db, "default", mustParse(q))
				if err != nil {
					t.Fatal(err)
				}
				return rs
			}
			exec("CREATE TABLE sales (id INT, category INT, amount INT)")
			exec("CREATE TABLE labels (id INT, name TEXT)")
			exec("INSERT INTO labels VALUES (1, 'one'), (2, 'two')")
			exec("CREATE VIEW summary AS SELECT category, COUNT(*) AS n, SUM(amount) AS total FROM sales GROUP BY category")
			exec("CREATE VIEW nested_summary AS SELECT * FROM summary")
			state := &querySubscriptionState{db: db, tenant: "default", query: mustParse(query).(*Select), rows: make(map[int]Row)}
			var applied []Row
			steps := []string{"", "INSERT INTO sales VALUES (1, 1, 10), (2, 1, 5), (3, 2, 20), (4, 3, 1)", "UPDATE sales SET amount = 30 WHERE id = 1", "UPDATE sales SET category = 2 WHERE id = 2", "UPDATE labels SET name = 'changed' WHERE id = 1", "DELETE FROM labels WHERE id = 2", "DELETE FROM sales WHERE id = 1", "DELETE FROM sales"}
			for step, mutation := range steps {
				if mutation != "" {
					exec(mutation)
				}
				delta, err := state.refresh(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				if step == 0 && (delta == nil || !delta.Initial) {
					t.Fatal("missing initial snapshot")
				}
				if delta != nil {
					if len(delta.Cols) == 0 {
						t.Fatal("missing result schema")
					}
					if delta.ScannedRows != -1 {
						t.Fatalf("unexpected scan count %d", delta.ScannedRows)
					}
					for _, removed := range delta.Removed {
						found := false
						for i, row := range applied {
							if reflect.DeepEqual(row, removed) {
								applied = append(applied[:i], applied[i+1:]...)
								found = true
								break
							}
						}
						if !found {
							t.Fatalf("removed absent row: %v", removed)
						}
					}
					applied = append(applied, delta.Added...)
				}
				expected := exec(query)
				remaining := append([]Row(nil), applied...)
				for _, row := range expected.Rows {
					projected := make(Row)
					for _, col := range expected.Cols {
						value, _ := getVal(row, col)
						putVal(projected, col, value)
					}
					found := false
					for i, actual := range remaining {
						if reflect.DeepEqual(actual, projected) {
							remaining = append(remaining[:i], remaining[i+1:]...)
							found = true
							break
						}
					}
					if !found {
						t.Fatalf("step %d: missing %v in %v", step, projected, applied)
					}
				}
				if len(remaining) != 0 {
					t.Fatalf("step %d: extra rows %v", step, remaining)
				}
				if delta, err := state.refresh(t.Context()); err != nil || delta != nil {
					t.Fatalf("unchanged refresh: %v, %v", delta, err)
				}
			}
		})
	}
}

func TestSubscriptionViewOwnershipAndSchema(t *testing.T) {
	db := storage.NewDB()
	defer db.Close()
	exec := func(q string) {
		t.Helper()
		if _, err := Execute(t.Context(), db, "default", mustParse(q)); err != nil {
			t.Fatal(err)
		}
	}
	exec("CREATE TABLE items (id INT, payload BLOB)")
	exec("INSERT INTO items VALUES (1, X'01'), (2, X'01')")
	exec("CREATE VIEW visible AS SELECT payload FROM items")
	state := &querySubscriptionState{db: db, tenant: "default", query: mustParse("SELECT * FROM visible").(*Select)}
	initial, err := state.refresh(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	initial.Added[0]["payload"].([]byte)[0] = 255
	exec("DELETE FROM items WHERE id = 1")
	delta, err := state.refresh(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if delta == nil || len(delta.Removed) != 1 || len(delta.Added) != 0 || delta.Removed[0]["payload"].([]byte)[0] != 1 {
		t.Fatalf("duplicate/ownership delta: %v", delta)
	}
	delta.Removed[0]["payload"].([]byte)[0] = 254
	if delta, err := state.refresh(t.Context()); err != nil || delta != nil {
		t.Fatalf("mutated retained row: %v, %v", delta, err)
	}
	exec("DROP VIEW visible")
	exec("CREATE VIEW visible AS SELECT id FROM items")
	if _, err := state.refresh(t.Context()); err == nil {
		t.Fatal("schema change accepted")
	}
}
