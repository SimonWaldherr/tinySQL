package engine

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func BenchmarkUpdateSecondaryIndex(b *testing.B) {
	for _, returning := range []bool{false, true} {
		b.Run(fmt.Sprintf("returning=%t", returning), func(b *testing.B) {
			db := storage.NewDB()
			defer db.Close()
			if _, err := Execute(b.Context(), db, "default", mustParse("CREATE TABLE edits (id INT PRIMARY KEY, lookup TEXT, amount INT)")); err != nil {
				b.Fatal(err)
			}
			table, _ := db.Get("default", "edits")
			table.Rows = make([][]any, 20000)
			for i := range table.Rows {
				table.Rows[i] = []any{i, fmt.Sprintf("key-%d", i), i}
			}
			table.Version++
			if _, err := Execute(b.Context(), db, "default", mustParse("CREATE INDEX edits_lookup ON edits(lookup)")); err != nil {
				b.Fatal(err)
			}
			query := "UPDATE edits SET amount = amount + 1 WHERE lookup = 'key-10000'"
			if returning {
				query += " RETURNING id, amount"
			}
			stmt := mustParse(query)
			b.ReportAllocs()
			for b.Loop() {
				if _, err := Execute(b.Context(), db, "default", stmt); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkBatchInsertSparseDefaults(b *testing.B) {
	db := storage.NewDB()
	defer db.Close()
	columns := []string{"id INT"}
	for i := 1; i < 64; i++ {
		column := fmt.Sprintf("c%d INT", i)
		if i == 63 {
			column += " DEFAULT 7"
		}
		columns = append(columns, column)
	}
	if _, err := Execute(b.Context(), db, "default", mustParse("CREATE TABLE bulk_defaults ("+strings.Join(columns, ",")+")")); err != nil {
		b.Fatal(err)
	}
	table, _ := db.Get("default", "bulk_defaults")
	values := make([]string, 1000)
	for i := range values {
		values[i] = fmt.Sprintf("(%d)", i)
	}
	stmt := mustParse("INSERT INTO bulk_defaults (id) VALUES " + strings.Join(values, ","))
	b.ReportAllocs()
	for b.Loop() {
		if _, err := Execute(b.Context(), db, "default", stmt); err != nil {
			b.Fatal(err)
		}
		if len(table.Rows) != 1000 || table.Rows[999][63] != 7 {
			b.Fatal("batch changed row values")
		}
		table.Rows = nil
	}
}

func TestUpdateReturningIncludesUnchangedMatches(t *testing.T) {
	db := storage.NewDB()
	defer db.Close()
	executeIndexSQL(t, db, "CREATE TABLE unchanged (id INT PRIMARY KEY, amount INT)")
	executeIndexSQL(t, db, "INSERT INTO unchanged VALUES (1, 7), (2, 8)")
	table, _ := db.Get("default", "unchanged")
	version := table.Version
	got := executeIndexSQL(t, db, "UPDATE unchanged SET amount = amount RETURNING id, amount")
	want := []Row{{"id": 1, "amount": 7}, {"id": 2, "amount": 8}}
	if !reflect.DeepEqual(got.Rows, want) || table.Version != version {
		t.Fatalf("no-op RETURNING: rows=%v version=%d, want %v version=%d", got.Rows, table.Version, want, version)
	}
	got = executeIndexSQL(t, db, "UPDATE unchanged SET amount = 7 RETURNING id, amount + 1 AS next")
	want = []Row{{"id": 1, "next": 8}, {"id": 2, "next": 8}}
	if !reflect.DeepEqual(got.Rows, want) || table.Version != version+1 {
		t.Fatalf("mixed changed/unchanged RETURNING: rows=%v version=%d", got.Rows, table.Version)
	}
}

func TestUpdateSecondaryIndexCandidatesRemainStable(t *testing.T) {
	for _, prefix := range []bool{false, true} {
		t.Run(fmt.Sprintf("prefix=%t", prefix), func(t *testing.T) {
			db := storage.NewDB()
			defer db.Close()
			executeIndexSQL(t, db, "CREATE TABLE edits (id INT PRIMARY KEY, category TEXT, code INT, amount INT)")
			executeIndexSQL(t, db, "INSERT INTO edits VALUES (3,'a',20,0),(1,'b',10,0),(4,'a',10,0),(2,'a',30,NULL),(5,'a',10,0)")
			executeIndexSQL(t, db, "CREATE INDEX edits_category_code ON edits(category,code)")
			where := "category = 'a'"
			wantIDs := []int{3, 4, 2, 5}
			if !prefix {
				where += " AND code = 10"
				wantIDs = []int{4, 5}
			}
			// Moving keys out of the searched run must not skip a later row.
			got := executeIndexSQL(t, db, "UPDATE edits SET category = 'moved', code = code + 1 WHERE "+where+" RETURNING id")
			if len(got.Rows) != len(wantIDs) {
				t.Fatalf("returned %v, want IDs %v", got.Rows, wantIDs)
			}
			for i, id := range wantIDs {
				if got.Rows[i]["id"] != id {
					t.Fatalf("RETURNING order changed: %v", got.Rows)
				}
			}
			got = executeIndexSQL(t, db, "SELECT id FROM edits WHERE category = 'moved'")
			if len(got.Rows) != len(wantIDs) {
				t.Fatal("new index keys are missing", got.Rows)
			}
			got = executeIndexSQL(t, db, "UPDATE edits SET amount = 9 WHERE category = 'moved' AND amount IS NOT NULL RETURNING id")
			wantCount := len(wantIDs)
			if prefix {
				wantCount-- // NULL must still fail the residual predicate.
			}
			if len(got.Rows) != wantCount {
				t.Fatalf("residual predicate returned %v", got.Rows)
			}
			table, _ := db.Get("default", "edits")
			before := append([][]any(nil), table.Rows...)
			version := table.Version
			if _, err := Execute(t.Context(), db, "default", mustParse("UPDATE edits SET id = 999 WHERE category = 'moved'")); err == nil {
				t.Fatal("expected a duplicate key after the first mutation")
			}
			if !reflect.DeepEqual(table.Rows, before) || table.Version != version {
				t.Fatal("failed indexed update changed rows or version")
			}
			got = executeIndexSQL(t, db, "SELECT id FROM edits WHERE category = 'moved'")
			if len(got.Rows) != len(wantIDs) {
				t.Fatal("rollback lost index entries", got.Rows)
			}
			got = executeIndexSQL(t, db, "UPDATE edits SET amount = 42 WHERE category = 'absent' RETURNING id")
			if len(got.Rows) != 0 || table.Version != version {
				t.Fatal("negative index lookup changed the table")
			}
			executeIndexSQL(t, db, "UPDATE edits SET amount = 10 WHERE category = 'moved'")
		})
	}
}

func TestUpdateSecondaryIndexKeepsMixedNumericEquality(t *testing.T) {
	db := storage.NewDB()
	defer db.Close()
	executeIndexSQL(t, db, "CREATE TABLE mixed_edits (key FLOAT, amount INT)")
	table, _ := db.Get("default", "mixed_edits")
	table.Rows = [][]any{{int(1), 0}, {float64(1), 0}, {int(2), 0}}
	table.Version++
	executeIndexSQL(t, db, "CREATE INDEX mixed_edits_key ON mixed_edits(key)")
	got := executeIndexSQL(t, db, "UPDATE mixed_edits SET amount = amount + 1 WHERE key = 1 RETURNING amount")
	if len(got.Rows) != 2 || table.Rows[0][1] != 1 || table.Rows[1][1] != 1 || table.Rows[2][1] != 0 {
		t.Fatal("numeric index seek lost an equal value", table.Rows)
	}
}

type countingInsertDefault struct {
	calls *int
	next  *storage.Column
}

func (d countingInsertDefault) String() string {
	(*d.calls)++
	if d.next != nil {
		d.next.DefaultValue = *d.calls
	}
	return fmt.Sprint(*d.calls)
}

func TestBatchDefaultsPreserveOwnershipAndConversion(t *testing.T) {
	db := storage.NewDB()
	defer db.Close()
	executeIndexSQL(t, db, "CREATE TABLE default_rows (id INT PRIMARY KEY, label TEXT, payload BLOB, doc JSON, embedding VECTOR)")
	table, _ := db.Get("default", "default_rows")
	defaults := []any{nil, "initial", []byte{1, 2}, `{"nested":{"v":1}}`, `[1,2]`}
	for i := 1; i < len(table.Cols); i++ {
		table.Cols[i].HasDefault, table.Cols[i].DefaultValue = true, defaults[i]
	}
	values := make([]string, 130)
	for i := range values {
		values[i] = fmt.Sprintf("(%d)", i)
	}
	executeIndexSQL(t, db, "INSERT INTO default_rows (id) VALUES "+strings.Join(values, ","))
	if table.Rows[0][1] != "initial" || table.Rows[129][1] != "initial" {
		t.Fatal("primitive default changed")
	}
	table.Rows[0][2].([]byte)[0] = 9
	table.Rows[0][3].(map[string]any)["nested"].(map[string]any)["v"] = 9
	table.Rows[0][4].([]float64)[0] = 9
	if table.Rows[1][2].([]byte)[0] != 1 || table.Rows[1][3].(map[string]any)["nested"].(map[string]any)["v"] != float64(1) || table.Rows[1][4].([]float64)[0] != 1 || defaults[2].([]byte)[0] != 1 {
		t.Fatal("mutable defaults are shared across inserted rows")
	}
	// A later statement must read the current schema default, not a prior cache.
	table.Cols[1].DefaultValue = "fresh"
	executeIndexSQL(t, db, "INSERT INTO default_rows (id) VALUES (130),(131)")
	if table.Rows[130][1] != "fresh" || table.Rows[131][1] != "fresh" {
		t.Fatal("statement-local defaults outlived the statement")
	}
	if _, err := Execute(t.Context(), db, "default", mustParse("INSERT INTO default_rows (id) VALUES (132),(133),(0)")); err == nil {
		t.Fatal("expected a late duplicate-key error")
	}
	if len(table.Rows) != 132 {
		t.Fatal("failed batch retained appended rows", len(table.Rows))
	}
	executeIndexSQL(t, db, "INSERT INTO default_rows (id) VALUES (132),(133)")
	if len(table.Rows) != 134 || table.Rows[133][1] != "fresh" {
		t.Fatal("failed batch invalidated a later retry")
	}
}

func TestBatchDefaultsPreserveCustomConversionEffects(t *testing.T) {
	db := storage.NewDB()
	defer db.Close()
	executeIndexSQL(t, db, "CREATE TABLE custom_defaults (id INT, label TEXT, amount INT)")
	table, _ := db.Get("default", "custom_defaults")
	calls := 0
	table.Cols[1].HasDefault = true
	table.Cols[1].DefaultValue = countingInsertDefault{calls: &calls, next: &table.Cols[2]}
	table.Cols[2].HasDefault, table.Cols[2].DefaultValue = true, 0
	executeIndexSQL(t, db, "INSERT INTO custom_defaults (id) VALUES (1),(2),(3)")
	if calls != 3 {
		t.Fatalf("custom conversion called %d times, want 3", calls)
	}
	for i, row := range table.Rows {
		if row[1] != fmt.Sprint(i+1) || row[2] != i+1 {
			t.Fatalf("custom conversion effects were cached: %v", table.Rows)
		}
	}
}

func TestBatchDefaultsKeepErrorPrecedence(t *testing.T) {
	db := storage.NewDB()
	defer db.Close()
	executeIndexSQL(t, db, "CREATE TABLE invalid_default (id INT, amount INT)")
	table, _ := db.Get("default", "invalid_default")
	// Native strict coercion rejects text that SQLite integer affinity retains.
	table.Cols[1].Affinity = storage.AffinityDefault
	table.Cols[1].HasDefault, table.Cols[1].DefaultValue = true, "invalid integer"
	for _, tc := range []struct{ sql, message string }{
		{"INSERT INTO invalid_default (id, amount) VALUES (1)", "column/value mismatch"},
		{"INSERT INTO invalid_default (id, amount) VALUES (1,2)", `default for column "amount"`},
	} {
		if _, err := Execute(t.Context(), db, "default", mustParse(tc.sql)); err == nil || !strings.Contains(err.Error(), tc.message) {
			t.Fatalf("%s: got %v, want %s", tc.sql, err, tc.message)
		}
		if len(table.Rows) != 0 {
			t.Fatal("failed default inserted a row")
		}
	}
}
