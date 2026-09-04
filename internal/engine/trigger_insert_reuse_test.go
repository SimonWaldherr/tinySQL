package engine

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func BenchmarkTriggerBoundedInsert(b *testing.B) {
	db := storage.NewDB()
	ctx := context.Background()
	for _, sql := range []string{`CREATE TABLE source (id INT, amount INT)`, `CREATE TABLE audit (id INT, amount INT)`, `CREATE TRIGGER audit_insert AFTER INSERT ON source BEGIN INSERT INTO audit VALUES (NEW.id, NEW.amount); END`} {
		if _, err := Execute(ctx, db, "default", mustParse(sql)); err != nil {
			b.Fatal(err)
		}
	}
	values := make([]string, 100)
	for i := range values {
		values[i] = fmt.Sprintf("(%d,%d)", i, i*10)
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
