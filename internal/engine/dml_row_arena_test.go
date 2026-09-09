package engine

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func BenchmarkInsertBatchRows(b *testing.B) {
	db := storage.NewDB()
	defer db.Close()
	if _, err := Execute(b.Context(), db, "default", mustParse("CREATE TABLE batch_rows (id INT, label TEXT)")); err != nil {
		b.Fatal(err)
	}
	table, _ := db.Get("default", "batch_rows")
	stmt := mustParse("INSERT INTO batch_rows VALUES " + strings.TrimSuffix(strings.Repeat("(1000, 'payload'),", 1000), ","))
	b.ReportAllocs()
	for b.Loop() {
		if _, err := Execute(b.Context(), db, "default", stmt); err != nil {
			b.Fatal(err)
		}
		if len(table.Rows) != 1000 {
			b.Fatal(len(table.Rows))
		}
		table.Rows = nil
	}
}

func BenchmarkUpdateAllRows(b *testing.B) {
	db := setupPerfTable(b, 20000)
	defer db.Close()
	runBench(b, db, "UPDATE t SET val = val + 1")
}

func BenchmarkInsertSelectAdapt(b *testing.B) {
	rs := &ResultSet{Cols: []string{"ID", "label"}, Rows: make([]Row, 1000)}
	for i := range rs.Rows {
		rs.Rows[i] = Row{"id": i, "label": "payload"}
	}
	b.ReportAllocs()
	for b.Loop() {
		if len(insertRowsFromResultSet(rs)) != 1000 {
			b.Fatal("rows")
		}
	}
}

func TestDMLPackedRowsRemainIndependent(t *testing.T) {
	for _, specific := range []bool{false, true} {
		t.Run(fmt.Sprint(specific), func(t *testing.T) {
			db := storage.NewDB()
			defer db.Close()
			executeIndexSQL(t, db, "CREATE TABLE packed (id INT PRIMARY KEY, label TEXT)")
			values := make([]string, 130)
			for i := range values {
				values[i] = fmt.Sprintf("(%d, 'before')", i)
			}
			columns := ""
			if specific {
				columns = " (id, label)"
			}
			executeIndexSQL(t, db, "INSERT INTO packed"+columns+" VALUES "+strings.Join(values, ","))
			table, _ := db.Get("default", "packed")
			executeIndexSQL(t, db, "CREATE TABLE copied (id INT PRIMARY KEY, label TEXT)")
			executeIndexSQL(t, db, "INSERT INTO copied SELECT id, label FROM packed")
			copied, _ := db.Get("default", "copied")
			if !reflect.DeepEqual(copied.Rows, table.Rows) {
				t.Fatal("INSERT SELECT changed values across blocks")
			}
			old := append([][]any(nil), table.Rows...)
			executeIndexSQL(t, db, "UPDATE packed SET label = 'after'")
			for i, row := range table.Rows {
				if !reflect.DeepEqual(row, []any{i, "after"}) || !reflect.DeepEqual(old[i], []any{i, "before"}) {
					t.Fatalf("row %d: current=%v old=%v", i, row, old[i])
				}
				// Appending through a retained row slice must not overwrite its neighbour.
				extended := append(row, "sentinel")
				extended[len(row)] = "changed"
			}
			got := executeIndexSQL(t, db, "SELECT label FROM packed WHERE id = 64")
			if len(got.Rows) != 1 || got.Rows[0]["label"] != "after" {
				t.Fatal(got.Rows)
			}
			// A constraint failure must restore the original rows and index.
			if _, err := Execute(t.Context(), db, "default", mustParse("UPDATE packed SET id = 999")); err == nil {
				t.Fatal("expected duplicate key")
			}
			got = executeIndexSQL(t, db, "SELECT label FROM packed WHERE id = 64")
			if len(got.Rows) != 1 || got.Rows[0]["label"] != "after" {
				t.Fatal(got.Rows)
			}
			values = append(values, "(0, 'duplicate')")
			executeIndexSQL(t, db, "DELETE FROM packed")
			if _, err := Execute(t.Context(), db, "default", mustParse("INSERT INTO packed"+columns+" VALUES "+strings.Join(values, ","))); err == nil {
				t.Fatal("expected duplicate key")
			}
			got = executeIndexSQL(t, db, "SELECT * FROM packed")
			if len(got.Rows) != 0 {
				t.Fatal("partial insert survived rollback")
			}
			executeIndexSQL(t, db, "INSERT INTO packed VALUES (64, 'retry')")
		})
	}
}

func TestInsertSelectAdaptIndependentCells(t *testing.T) {
	rs := &ResultSet{Cols: []string{"ID", "LaBeL", "missing"}, Rows: make([]Row, 130)}
	for i := range rs.Rows {
		rs.Rows[i] = Row{"id": i, "label": fmt.Sprint(i)}
	}
	rows := insertRowsFromResultSet(rs)
	for i, row := range rows {
		for j, want := range []any{i, fmt.Sprint(i), nil} {
			if got := row[j].(*Literal).Val; got != want {
				t.Fatalf("cell %d,%d=%v, want %v", i, j, got, want)
			}
		}
		row[0].(*Literal).Val = -1
		extended := append(row, &Literal{Val: "sentinel"})
		extended[len(row)] = &Literal{Val: "changed"}
	}
	if len(insertRowsFromResultSet(&ResultSet{})) != 0 {
		t.Fatal("empty result")
	}
}
