package driver

import (
	"context"
	"database/sql"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/engine"
)

func TestDatabaseSQLQueryReturnsStoredProcedureRows(t *testing.T) {
	const name = "driver_readonly_procedure_test"
	engine.UnregisterStoredProcedure(name)
	t.Cleanup(func() { engine.UnregisterStoredProcedure(name) })
	if err := engine.RegisterStoredProcedureWithOptions(name, engine.StoredProcedureOptions{
		ReadOnly: true,
		Parameters: []engine.StoredProcedureParameter{
			{Name: "value", Required: true},
		},
	}, func(ctx engine.ProcedureContext, args []any) (*engine.ResultSet, error) {
		return ctx.ExecuteSQLArgs(`SELECT ? AS value`, args[0])
	}); err != nil {
		t.Fatal(err)
	}

	db, err := sql.Open("tinysql", "mem://?tenant=driver_readonly_procedure")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var got string
	if err := db.QueryRowContext(context.Background(), `CALL driver_readonly_procedure_test('ok')`).Scan(&got); err != nil {
		t.Fatalf("QueryRow CALL: %v", err)
	}
	if got != "ok" {
		t.Fatalf("CALL value = %q, want ok", got)
	}
}

func TestDatabaseSQLMutatingStoredProcedureUsesWriterPath(t *testing.T) {
	const name = "driver_mutating_procedure_test"
	engine.UnregisterStoredProcedure(name)
	t.Cleanup(func() { engine.UnregisterStoredProcedure(name) })
	if err := engine.RegisterStoredProcedureWithOptions(name, engine.StoredProcedureOptions{
		Atomic: true,
		Parameters: []engine.StoredProcedureParameter{
			{Name: "id", Required: true},
		},
	}, func(ctx engine.ProcedureContext, args []any) (*engine.ResultSet, error) {
		if _, err := ctx.ExecuteSQLArgs(`INSERT INTO procedure_rows VALUES (?)`, args[0]); err != nil {
			return nil, err
		}
		return ctx.ExecuteSQLArgs(`SELECT COUNT(*) AS inserted FROM procedure_rows`)
	}); err != nil {
		t.Fatal(err)
	}

	db, err := sql.Open("tinysql", "mem://?tenant=driver_mutating_procedure")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE procedure_rows (id INT)`); err != nil {
		t.Fatal(err)
	}
	var inserted int
	if err := db.QueryRow(`CALL driver_mutating_procedure_test(7)`).Scan(&inserted); err != nil {
		t.Fatalf("mutating QueryRow CALL: %v", err)
	}
	if inserted != 1 {
		t.Fatalf("inserted = %d, want 1", inserted)
	}
	var id int
	if err := db.QueryRow(`SELECT id FROM procedure_rows`).Scan(&id); err != nil {
		t.Fatal(err)
	}
	if id != 7 {
		t.Fatalf("stored id = %d, want 7", id)
	}
}
