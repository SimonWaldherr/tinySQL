package engine

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestCTEQualifiedProjectionMatchesDerivedTable(t *testing.T) {
	db := storage.NewDB()
	defer db.Close()
	executeIndexSQL(t, db, `CREATE TABLE source_rows (id INT, label TEXT)`)
	executeIndexSQL(t, db, `INSERT INTO source_rows VALUES (1, 'a'), (2, NULL), (3, 'c'), (4, 'd')`)
	for _, projection := range []string{"q.id, q.label", "Q.id AS n, q.label AS value", "q.id AS x, q.label AS x"} {
		for _, tail := range []string{"", " WHERE id >= 2 LIMIT 2 OFFSET 1", " WHERE id > 10", " LIMIT 0", " WHERE q.id >= 2"} {
			query := "WITH c AS (SELECT id, label FROM source_rows) SELECT " + projection + " FROM c q" + tail
			want := executeIndexSQL(t, db, "SELECT "+projection+" FROM (SELECT id, label FROM source_rows) q"+tail)
			stmt := mustParse(query)
			for range 2 {
				got, err := Execute(t.Context(), db, "default", stmt)
				if err != nil {
					t.Fatalf("%s: %v", query, err)
				}
				if !reflect.DeepEqual(got.Cols, want.Cols) || !reflect.DeepEqual(got.Rows, want.Rows) {
					t.Fatalf("%s: got %#v, want %#v", query, got, want)
				}
			}
		}
	}
	// The CTE's own qualifier remains available alongside its source alias.
	got := executeIndexSQL(t, db, `WITH c AS (SELECT id FROM source_rows) SELECT c.id FROM c q LIMIT 1`)
	if len(got.Rows) != 1 || got.Rows[0]["c.id"] != 1 {
		t.Fatal(got.Rows)
	}
}

func TestIndexedSelectCapacityKeepsResults(t *testing.T) {
	db := storage.NewDB()
	defer db.Close()
	executeIndexSQL(t, db, `CREATE TABLE selected_rows (id INT, label TEXT)`)
	executeIndexSQL(t, db, `INSERT INTO selected_rows VALUES (1, 'a'), (2, 'b'), (2, 'c')`)
	executeIndexSQL(t, db, `CREATE INDEX selected_id ON selected_rows(id)`)
	for _, tail := range []string{"WHERE id = 2 LIMIT 10000", "WHERE id = 99 LIMIT 10000", "WHERE id = 2 LIMIT 1 OFFSET 1", "WHERE id = 2 ORDER BY label DESC LIMIT 10000", "WHERE id = 2 LIMIT 0"} {
		got := executeIndexSQL(t, db, "SELECT id, label FROM selected_rows "+tail)
		want := executeIndexSQL(t, db, "SELECT id, label FROM (SELECT id, label FROM selected_rows) q "+tail)
		if !reflect.DeepEqual(got.Cols, want.Cols) || !reflect.DeepEqual(got.Rows, want.Rows) {
			t.Fatalf("%s: got %#v, want %#v", tail, got, want)
		}
	}
}

func TestSelectAdaptiveCapacityMatchesDerivedTable(t *testing.T) {
	db := storage.NewDB()
	defer db.Close()
	executeIndexSQL(t, db, "CREATE TABLE adaptive_rows (id INT)")
	values := make([]string, 256)
	for i := range values {
		values[i] = fmt.Sprintf("(%d)", i)
	}
	executeIndexSQL(t, db, "INSERT INTO adaptive_rows VALUES "+strings.Join(values, ","))
	for _, predicate := range []string{"id >= 0", "id = 128", "id >= 128", "id < 64", "id > 999"} {
		for _, limit := range []int{0, 1, 64, 65, 200, 10000} {
			for _, offset := range []int{0, 17} {
				tail := fmt.Sprintf(" WHERE %s LIMIT %d OFFSET %d", predicate, limit, offset)
				got := executeIndexSQL(t, db, "SELECT id FROM adaptive_rows"+tail)
				want := executeIndexSQL(t, db, "SELECT id FROM (SELECT id FROM adaptive_rows) q"+tail)
				if !reflect.DeepEqual(got.Cols, want.Cols) || !reflect.DeepEqual(got.Rows, want.Rows) {
					t.Fatalf("%s: results differ", tail)
				}
			}
		}
	}
}
