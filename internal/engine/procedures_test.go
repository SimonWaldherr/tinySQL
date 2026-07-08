package engine

import (
	"context"
	"fmt"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestCallStoredProcedureReturnsRows(t *testing.T) {
	name := "proc_add_numbers_test"
	UnregisterStoredProcedure(name)
	t.Cleanup(func() { UnregisterStoredProcedure(name) })

	if err := RegisterStoredProcedure(name, func(ctx ProcedureContext, args []any) (*ResultSet, error) {
		if len(args) != 2 {
			return nil, fmt.Errorf("expected 2 args")
		}
		return &ResultSet{
			Cols: []string{"sum"},
			Rows: []Row{{"sum": toFloat(args[0]) + toFloat(args[1])}},
		}, nil
	}); err != nil {
		t.Fatalf("RegisterStoredProcedure: %v", err)
	}

	db := storage.NewDB()
	rs, err := Execute(context.Background(), db, "default", mustParse(`CALL proc_add_numbers_test(2, 3.5)`))
	if err != nil {
		t.Fatalf("CALL failed: %v", err)
	}
	if len(rs.Rows) != 1 || rs.Rows[0]["sum"] != 5.5 {
		t.Fatalf("unexpected result: %#v", rs)
	}
}

func TestCallStoredProcedureCanExecuteSQL(t *testing.T) {
	name := "proc_insert_log_test"
	UnregisterStoredProcedure(name)
	t.Cleanup(func() { UnregisterStoredProcedure(name) })

	if err := RegisterStoredProcedure(name, func(ctx ProcedureContext, args []any) (*ResultSet, error) {
		if len(args) != 2 {
			return nil, fmt.Errorf("expected id and message")
		}
		_, err := ctx.ExecuteSQL(fmt.Sprintf("INSERT INTO proc_logs VALUES (%v, '%v')", args[0], args[1]))
		if err != nil {
			return nil, err
		}
		return &ResultSet{Cols: []string{"rows_inserted"}, Rows: []Row{{"rows_inserted": int64(1)}}}, nil
	}); err != nil {
		t.Fatalf("RegisterStoredProcedure: %v", err)
	}

	db := storage.NewDB()
	if _, err := Execute(context.Background(), db, "default", mustParse(`CREATE TABLE proc_logs (id INT, msg TEXT)`)); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := Execute(context.Background(), db, "default", mustParse(`CALL proc_insert_log_test(7, 'ok')`)); err != nil {
		t.Fatalf("CALL failed: %v", err)
	}

	rs, err := Execute(context.Background(), db, "default", mustParse(`SELECT msg FROM proc_logs WHERE id = 7`))
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	if len(rs.Rows) != 1 || rs.Rows[0]["msg"] != "ok" {
		t.Fatalf("unexpected rows: %#v", rs.Rows)
	}
}

func TestSysProceduresListsRegisteredProcedures(t *testing.T) {
	name := "proc_sys_list_test"
	UnregisterStoredProcedure(name)
	t.Cleanup(func() { UnregisterStoredProcedure(name) })

	if err := RegisterStoredProcedure(name, func(ctx ProcedureContext, args []any) (*ResultSet, error) {
		return nil, nil
	}); err != nil {
		t.Fatalf("RegisterStoredProcedure: %v", err)
	}

	db := storage.NewDB()
	rs, err := Execute(context.Background(), db, "default", mustParse(`SELECT name FROM sys.procedures WHERE name = 'proc_sys_list_test'`))
	if err != nil {
		t.Fatalf("SELECT sys.procedures: %v", err)
	}
	if len(rs.Rows) != 1 || rs.Rows[0]["name"] != name {
		t.Fatalf("unexpected sys.procedures rows: %#v", rs.Rows)
	}
}

func toFloat(v any) float64 {
	switch n := v.(type) {
	case int:
		return float64(n)
	case int64:
		return float64(n)
	case float64:
		return n
	default:
		return 0
	}
}
