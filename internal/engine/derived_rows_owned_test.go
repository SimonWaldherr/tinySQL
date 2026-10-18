package engine

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func BenchmarkDerivedSubqueryRows(b *testing.B) {
	for _, width := range []int{2, 8} {
		b.Run(fmt.Sprint(width), func(b *testing.B) {
			db := storage.NewDB()
			b.Cleanup(func() { _ = db.Close() })
			cols := make([]storage.Column, width)
			names := make([]string, width)
			for col := range cols {
				names[col] = fmt.Sprintf("c%d", col)
				cols[col] = storage.Column{Name: names[col], Type: storage.IntType}
			}
			table := storage.NewTable("derived_source", cols, false)
			for i := range 50000 {
				row := make([]any, width)
				for col := range row {
					row[col] = i + col
				}
				table.Rows = append(table.Rows, row)
			}
			if err := db.Put("default", table); err != nil {
				b.Fatal(err)
			}
			s := mustParse("SELECT q.c0 FROM (SELECT " + strings.Join(names, ",") + " FROM derived_source) q").(*Select)
			env := ExecEnv{ctx: b.Context(), db: db, tenant: "default"}
			b.ReportAllocs()
			for b.Loop() {
				rows, err := resolveSubquery(env, s)
				if err != nil {
					b.Fatal(err)
				}
				if len(rows) != len(table.Rows) || rows[len(rows)-1]["q.c0"] != 49999 {
					b.Fatal("changed derived rows")
				}
			}
		})
	}
}

func TestDerivedSubqueryAliasingKeepsSourceResults(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	execSQL(t, db, `CREATE TABLE derived_source (id INT, label TEXT)`)
	execSQL(t, db, `INSERT INTO derived_source VALUES (1, 'one'), (2, 'two'), (3, NULL)`)
	for _, source := range []string{
		`(SELECT id, label FROM derived_source) q`,
		`(SELECT * FROM derived_source) q`,
		`(SELECT id AS "ID", label AS "Name" FROM derived_source) q`,
	} {
		stmt := mustParse("SELECT q.id FROM " + source + " ORDER BY q.id")
		for range 2 {
			got, err := Execute(t.Context(), db, "default", stmt)
			if err != nil {
				t.Fatal(err)
			}
			want := []Row{{"q.id": 1}, {"q.id": 2}, {"q.id": 3}}
			if !reflect.DeepEqual(got.Rows, want) {
				t.Fatalf("%s: got %v, want %v", source, got.Rows, want)
			}
			got.Rows[0]["q.id"] = -1
		}
	}
	cte := &ResultSet{Cols: []string{"id"}, Rows: []Row{{"id": 1}, {"id": 2}}}
	env := ExecEnv{ctx: t.Context(), db: db, tenant: "default", ctes: map[string]*ResultSet{"c": cte}}
	s := mustParse(`SELECT q.id FROM (SELECT id FROM c) q`).(*Select)
	if _, err := resolveSubquery(env, s); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(cte.Rows, []Row{{"id": 1}, {"id": 2}}) {
		t.Fatalf("subquery aliasing mutated a shared CTE: %v", cte.Rows)
	}
}

func TestOwnedSubqueryAliasingMatchesCopy(t *testing.T) {
	for _, alias := range []string{"", "q", "OUTER", "Über"} {
		t.Run(alias, func(t *testing.T) {
			rows := []Row{
				{"ID": 1, "NäMe": "one", "inner.label": "inner", "empty": nil},
				{"id": 2, "label": "two", "other": []byte("value")},
				nil, {},
			}
			want := aliasRows(rows, alias)
			owned := make([]Row, len(rows))
			for i, row := range rows {
				if row != nil {
					owned[i] = cloneRow(row)
				}
			}
			got := aliasOwnedRows(owned, alias)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("got %#v, want %#v", got, want)
			}
		})
	}
}

func TestOwnedSubqueryAliasingWideFallbackAndDottedKeys(t *testing.T) {
	rows := []Row{{"id": 1}, {"a": 1, "b": 2, "c": 3, "d": 4, "e": 5}}
	before := []Row{cloneRow(rows[0]), cloneRow(rows[1])}
	want := aliasRows(rows, "q")
	if got := aliasOwnedRows(rows, "q"); !reflect.DeepEqual(got, want) || !reflect.DeepEqual(rows, before) {
		t.Fatalf("wide fallback changed input or output: got %#v; source %#v", got, rows)
	}
	// Qualified targets may collide with an original dotted output name.
	// The next qualification must still use that original value, never one
	// inserted earlier while editing the same map.
	got := aliasOwnedRows([]Row{{"id": 1, "q.id": 2}}, "q")[0]
	if len(got) != 3 || got["id"] != 1 || got["q.q.id"] != 2 {
		t.Fatalf("dotted source key was overwritten before qualification: %#v", got)
	}
}

func TestDerivedSubqueryJoinPreservesNullsAndColumns(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	execSQL(t, db, `CREATE TABLE derived_left (id INT)`)
	execSQL(t, db, `INSERT INTO derived_left VALUES (1), (2), (3), (NULL)`)
	execSQL(t, db, `CREATE TABLE derived_right (id INT, label TEXT)`)
	execSQL(t, db, `INSERT INTO derived_right VALUES (1, 'one'), (1, 'again'), (2, NULL), (NULL, 'missing')`)
	for _, join := range []string{"JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL JOIN"} {
		for _, subquery := range []string{
			`SELECT id, label FROM derived_right`,
			`SELECT * FROM derived_right`,
			`SELECT id, label FROM derived_right WHERE FALSE`,
		} {
			query := "SELECT l.id AS left_id, q.id AS right_id, q.label AS label FROM derived_left l " + join + " (" + subquery + ") q ON l.id=q.id"
			stmt := mustParse(query).(*Select)
			env := ExecEnv{ctx: t.Context(), db: db, tenant: "default"}
			left, err := resolveFromClause(env, env, stmt)
			if err != nil {
				t.Fatal(err)
			}
			inner, err := executeSelect(env, stmt.Joins[0].Right.Subquery)
			if err != nil {
				t.Fatal(err)
			}
			right := aliasRows(inner.Rows, "q")
			meta := resultSetTable("q", inner.Cols)
			var want []Row
			switch join {
			case "JOIN":
				want, err = processInnerJoin(env, left, right, stmt.Joins[0].On)
			case "LEFT JOIN":
				want, err = processLeftJoin(env, left, right, stmt.Joins[0].On, "q", meta)
			case "RIGHT JOIN":
				want, err = processRightJoin(env, left, right, stmt.Joins[0].On)
			case "FULL JOIN":
				want, err = processFullOuterJoin(env, left, right, stmt.Joins[0].On, "q", meta)
			}
			if err != nil {
				t.Fatal(err)
			}
			got, err := processJoins(env, stmt.Joins, left)
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("%s: got %#v, %v; want %#v", query, got, err, want)
			}
		}
	}
}
