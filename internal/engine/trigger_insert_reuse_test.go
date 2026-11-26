package engine

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func BenchmarkTriggerBoundedInsert(b *testing.B) {
	benchmarkTriggerInsertWidth(b, 2)
}

func BenchmarkTriggerWideInsert(b *testing.B) {
	benchmarkTriggerInsertWidth(b, 64)
}

func benchmarkTriggerInsertWidth(b *testing.B, width int) {
	db := storage.NewDB()
	b.Cleanup(func() { db.Close() })
	ctx := context.Background()
	cols := []string{"id INT", "amount INT"}
	for i := 2; i < width; i++ {
		cols = append(cols, fmt.Sprintf("col%d INT", i))
	}
	for _, sql := range []string{"CREATE TABLE source (" + strings.Join(cols, ",") + ")", `CREATE TABLE audit (id INT, amount INT)`, `CREATE TRIGGER audit_insert AFTER INSERT ON source BEGIN INSERT INTO audit VALUES (NEW.id, NEW.amount); END`} {
		if _, err := Execute(ctx, db, "default", mustParse(sql)); err != nil {
			b.Fatal(err)
		}
	}
	values := make([]string, 100)
	extraValues := strings.Repeat(",0", width-2)
	for i := range values {
		values[i] = fmt.Sprintf("(%d,%d%s)", i, i*10, extraValues)
	}
	stmt := mustParse("INSERT INTO source VALUES " + strings.Join(values, ",")).(*Insert)
	source, _ := db.Get("default", "source")
	audit, _ := db.Get("default", "audit")
	env := ExecEnv{ctx: ctx, tenant: "default", db: db}
	b.ReportAllocs()
	for b.Loop() {
		if _, err := executeInsert(env, stmt); err != nil {
			b.Fatal(err)
		}
		if len(audit.Rows) != 100 {
			b.Fatal("missing audit rows")
		}
		clear(source.Rows)
		source.Rows = source.Rows[:0]
		clear(audit.Rows)
		audit.Rows = audit.Rows[:0]
	}
}

func TestRawInsertTriggerBindingMatchesMap(t *testing.T) {
	keys := newTableRowKeys([]storage.Column{{Name: "id"}, {Name: "source.id"}, {Name: "amount"}}, "source.")
	values := []any{1, 2, nil}
	mapped := triggerRowBinding{newRow: buildTableRow(keys, values)}
	raw := triggerRowBinding{newValues: values, newCols: indexInsertTriggerColumns(keys)}
	for _, name := range []string{"new.id", "id", "new.source.id", "source.id", "new.source.source.id", "new.amount", "new.missing", "old.id"} {
		got, ok := lookupTriggerRow(&raw, name)
		want, found := lookupTriggerRow(&mapped, name)
		if got != want || ok != found {
			t.Errorf("%s: got (%v, %t), want (%v, %t)", name, got, ok, want, found)
		}
	}
	if got, want := triggerRowSuggestionRow(&raw), triggerRowSuggestionRow(&mapped); !reflect.DeepEqual(got, want) {
		t.Fatalf("suggestion bindings: got %v, want %v", got, want)
	}
}

func TestTriggerBatchNewSnapshotSurvivesProcedure(t *testing.T) {
	const proc = "test_trigger_replace_inserted_cell"
	if err := RegisterStoredProcedure(proc, func(pc ProcedureContext, _ []any) (*ResultSet, error) {
		table, err := pc.env.db.Get(pc.Tenant(), "source")
		if err != nil {
			return nil, err
		}
		table.Rows[len(table.Rows)-1][0] = 99
		return nil, nil
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { UnregisterStoredProcedure(proc) })
	for _, columns := range []string{"", "(id)"} {
		db := storage.NewDB()
		defer db.Close()
		for _, sql := range []string{
			`CREATE TABLE source (id INT)`,
			`CREATE TABLE audit (id INT)`,
			`CREATE TRIGGER audit_insert AFTER INSERT ON source BEGIN CALL ` + proc + `(); INSERT INTO audit VALUES (NEW.id); END`,
			`INSERT INTO source ` + columns + ` VALUES (1),(2)`,
		} {
			if _, err := Execute(t.Context(), db, "default", mustParse(sql)); err != nil {
				t.Fatal(sql, err)
			}
		}
		source, err := db.Get("default", "source")
		if err != nil {
			t.Fatal(err)
		}
		audit, err := db.Get("default", "audit")
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(source.Rows, [][]any{{99}, {99}}) || !reflect.DeepEqual(audit.Rows, [][]any{{1}, {2}}) {
			t.Fatalf("columns %q: source %v, audit %v", columns, source.Rows, audit.Rows)
		}
	}
}

func TestTriggerBatchBindingsRemainIndependent(t *testing.T) {
	for _, specific := range []bool{false, true} {
		for _, returning := range []bool{false, true} {
			t.Run(fmt.Sprintf("specific=%t/returning=%t", specific, returning), func(t *testing.T) {
				db := storage.NewDB()
				for _, sql := range []string{
					`CREATE TABLE source (id INT, amount INT DEFAULT 7)`,
					`CREATE TABLE refs (id INT, amount INT)`,
					`INSERT INTO refs VALUES (1,10),(2,20),(3,30)`,
					`CREATE TABLE audit (id INT, amount INT)`,
					`CREATE TABLE nested (id INT)`,
					`CREATE TRIGGER inner_audit AFTER INSERT ON audit BEGIN INSERT INTO nested VALUES (NEW.id+100); END`,
					`CREATE TRIGGER outer_audit AFTER INSERT ON source WHEN (NEW.amount > 0) BEGIN INSERT INTO audit VALUES (NEW.id,(SELECT amount FROM refs WHERE id=CASE WHEN 1=1 THEN NEW.id ELSE 0 END)); INSERT INTO nested VALUES (NEW.id); END`,
				} {
					if _, err := Execute(t.Context(), db, "default", mustParse(sql)); err != nil {
						t.Fatal(sql, err)
					}
				}
				sql := "INSERT INTO source VALUES (1,7),(2,7),(3,7)"
				if specific {
					sql = "INSERT INTO source (id) VALUES (1),(2),(3)"
				}
				if returning {
					sql += " RETURNING id"
				}
				result, err := Execute(t.Context(), db, "default", mustParse(sql))
				if err != nil {
					t.Fatal(err)
				}
				if returning {
					if len(result.Rows) != 3 {
						t.Fatalf("RETURNING: expected 3 rows, got %d", len(result.Rows))
					}
					for i, row := range result.Rows {
						if row["id"] != i+1 {
							t.Fatalf("aliased RETURNING row: %v", result.Rows)
						}
					}
				}
				audit, _ := db.Get("default", "audit")
				nested, _ := db.Get("default", "nested")
				if len(audit.Rows) != 3 || len(nested.Rows) != 6 {
					t.Fatal("missing triggered rows")
				}
				for i, row := range audit.Rows {
					if row[0] != i+1 || row[1] != (i+1)*10 {
						t.Fatalf("stale correlated binding: %v", audit.Rows)
					}
				}
				for i, want := range []int{101, 1, 102, 2, 103, 3} {
					if nested.Rows[i][0] != want {
						t.Fatalf("nested trigger changed parent binding: %v", nested.Rows)
					}
				}
			})
		}
	}
}
