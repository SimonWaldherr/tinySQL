package engine

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

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

func TestStoredProcedureOptionsValidateArgsAndExposeStats(t *testing.T) {
	name := "proc_options_test"
	UnregisterStoredProcedure(name)
	t.Cleanup(func() { UnregisterStoredProcedure(name) })

	options := StoredProcedureOptions{
		Description: "Safely echoes one SQL value",
		ReadOnly:    true,
		Parameters: []StoredProcedureParameter{
			{Name: "value", Description: "value to echo", Required: true},
		},
	}
	if err := RegisterStoredProcedureWithOptions(name, options, func(ctx ProcedureContext, args []any) (*ResultSet, error) {
		return ctx.ExecuteSQLArgs(`SELECT ? AS value, '?' AS literal_question_mark`, args[0])
	}); err != nil {
		t.Fatalf("register: %v", err)
	}

	db := storage.NewDB()
	if !IsReadOnlyStatement(mustParse(`CALL proc_options_test('x')`)) {
		t.Fatal("declared read-only procedure was classified as mutating")
	}
	rs, err := Execute(context.Background(), db, "default", mustParse(`CALL proc_options_test('O''Reilly')`))
	if err != nil {
		t.Fatalf("CALL: %v", err)
	}
	if len(rs.Rows) != 1 || rs.Rows[0]["value"] != "O'Reilly" || rs.Rows[0]["literal_question_mark"] != "?" {
		t.Fatalf("unexpected safe binding result: %#v", rs.Rows)
	}
	if _, err := Execute(context.Background(), db, "default", mustParse(`CALL proc_options_test()`)); err == nil || !strings.Contains(err.Error(), "expects 1 arguments") {
		t.Fatalf("arity error = %v", err)
	}

	var info StoredProcedureInfo
	for _, candidate := range ListStoredProcedures() {
		if candidate.Name == name {
			info = candidate
			break
		}
	}
	if info.Name == "" || info.Description != options.Description || !info.ReadOnly || info.Atomic || info.MinArgs != 1 || info.MaxArgs != 1 {
		t.Fatalf("unexpected procedure info: %+v", info)
	}
	if info.Calls != 1 || info.Errors != 0 || info.LastCalledAt.IsZero() || info.TotalRuntime <= 0 {
		t.Fatalf("unexpected procedure stats: %+v", info)
	}
}

func TestReadOnlyStoredProcedureRejectsNestedMutation(t *testing.T) {
	name := "proc_readonly_guard_test"
	UnregisterStoredProcedure(name)
	t.Cleanup(func() { UnregisterStoredProcedure(name) })
	if err := RegisterStoredProcedureWithOptions(name, StoredProcedureOptions{
		ReadOnly:   true,
		Parameters: []StoredProcedureParameter{},
	}, func(ctx ProcedureContext, args []any) (*ResultSet, error) {
		return ctx.ExecuteSQL(`INSERT INTO proc_readonly_rows VALUES (1)`)
	}); err != nil {
		t.Fatal(err)
	}

	db := storage.NewDB()
	if _, err := Execute(context.Background(), db, "default", mustParse(`CREATE TABLE proc_readonly_rows (id INT)`)); err != nil {
		t.Fatal(err)
	}
	if _, err := Execute(context.Background(), db, "default", mustParse(`CALL proc_readonly_guard_test()`)); err == nil || !strings.Contains(err.Error(), "read-only stored procedure") {
		t.Fatalf("nested mutation error = %v", err)
	}
	rs, err := Execute(context.Background(), db, "default", mustParse(`SELECT COUNT(*) AS n FROM proc_readonly_rows`))
	if err != nil {
		t.Fatal(err)
	}
	if rs.Rows[0]["n"] != 0 {
		t.Fatalf("read-only procedure changed table: %#v", rs.Rows)
	}
}

func TestAtomicStoredProcedureRollsBackNestedStatements(t *testing.T) {
	name := "proc_atomic_rollback_test"
	UnregisterStoredProcedure(name)
	t.Cleanup(func() { UnregisterStoredProcedure(name) })
	if err := RegisterStoredProcedureWithOptions(name, StoredProcedureOptions{
		Atomic:     true,
		Parameters: []StoredProcedureParameter{},
	}, func(ctx ProcedureContext, args []any) (*ResultSet, error) {
		if _, err := ctx.ExecuteSQL(`CREATE TABLE proc_atomic_created (id INT)`); err != nil {
			return nil, err
		}
		if _, err := ctx.ExecuteSQL(`INSERT INTO proc_atomic_rows VALUES (1, 'first')`); err != nil {
			return nil, err
		}
		return ctx.ExecuteSQL(`INSERT INTO proc_atomic_rows VALUES (1, 'duplicate')`)
	}); err != nil {
		t.Fatal(err)
	}

	db := storage.NewDB()
	for _, sql := range []string{
		`CREATE TABLE proc_atomic_rows (id INT, label TEXT)`,
		`CREATE UNIQUE INDEX proc_atomic_rows_id ON proc_atomic_rows(id)`,
	} {
		if _, err := Execute(context.Background(), db, "default", mustParse(sql)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := Execute(context.Background(), db, "default", mustParse(`CALL proc_atomic_rollback_test()`)); err == nil {
		t.Fatal("atomic procedure unexpectedly succeeded")
	}
	rs, err := Execute(context.Background(), db, "default", mustParse(`SELECT COUNT(*) AS n FROM proc_atomic_rows`))
	if err != nil {
		t.Fatal(err)
	}
	if rs.Rows[0]["n"] != 0 {
		t.Fatalf("atomic procedure retained partial write: %#v", rs.Rows)
	}
	if _, err := db.Get("default", "proc_atomic_created"); err == nil {
		t.Fatal("atomic procedure retained a table created before its failure")
	}
}

func TestReadOnlyStoredProceduresExecuteConcurrently(t *testing.T) {
	name := "proc_parallel_read_test"
	UnregisterStoredProcedure(name)
	t.Cleanup(func() { UnregisterStoredProcedure(name) })
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	if err := RegisterStoredProcedureWithOptions(name, StoredProcedureOptions{
		ReadOnly:   true,
		Parameters: []StoredProcedureParameter{},
	}, func(ctx ProcedureContext, args []any) (*ResultSet, error) {
		entered <- struct{}{}
		select {
		case <-release:
			return &ResultSet{}, nil
		case <-ctx.Context().Done():
			return nil, ctx.Context().Err()
		}
	}); err != nil {
		t.Fatal(err)
	}

	db := storage.NewDB()
	errs := make(chan error, 2)
	for range 2 {
		go func() {
			_, err := Execute(context.Background(), db, "default", mustParse(`CALL proc_parallel_read_test()`))
			errs <- err
		}()
	}
	for i := 0; i < 2; i++ {
		select {
		case <-entered:
		case <-time.After(time.Second):
			close(release)
			t.Fatal("read-only procedure calls serialized on the content write lock")
		}
	}
	close(release)
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
}

func BenchmarkStoredProcedureParallel(b *testing.B) {
	for _, benchmark := range []struct {
		name     string
		options  StoredProcedureOptions
		procName string
	}{
		{
			name:     "declared_read_only",
			procName: "benchmark_procedure_readonly",
			options: StoredProcedureOptions{
				ReadOnly:   true,
				Parameters: []StoredProcedureParameter{},
			},
		},
		{
			name:     "conservative_mutating",
			procName: "benchmark_procedure_mutating",
			options: StoredProcedureOptions{
				Parameters: []StoredProcedureParameter{},
			},
		},
	} {
		benchmark := benchmark
		b.Run(benchmark.name, func(b *testing.B) {
			UnregisterStoredProcedure(benchmark.procName)
			b.Cleanup(func() { UnregisterStoredProcedure(benchmark.procName) })
			if err := RegisterStoredProcedureWithOptions(benchmark.procName, benchmark.options, func(ctx ProcedureContext, args []any) (*ResultSet, error) {
				return &ResultSet{}, nil
			}); err != nil {
				b.Fatal(err)
			}
			db := storage.NewDB()
			statement := mustParse("CALL " + benchmark.procName + "()")
			ctx := context.Background()
			b.ReportAllocs()
			b.SetParallelism(2)
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					if _, err := Execute(ctx, db, "default", statement); err != nil {
						b.Error(err)
						return
					}
				}
			})
		})
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

	if err := RegisterStoredProcedureWithOptions(name, StoredProcedureOptions{
		Description: "catalog test procedure",
		ReadOnly:    true,
		Parameters:  []StoredProcedureParameter{},
	}, func(ctx ProcedureContext, args []any) (*ResultSet, error) {
		return nil, nil
	}); err != nil {
		t.Fatalf("RegisterStoredProcedure: %v", err)
	}

	db := storage.NewDB()
	rs, err := Execute(context.Background(), db, "default", mustParse(`SELECT name, description, read_only, atomic, min_args, max_args, calls FROM sys.procedures WHERE name = 'proc_sys_list_test'`))
	if err != nil {
		t.Fatalf("SELECT sys.procedures: %v", err)
	}
	if len(rs.Rows) != 1 || rs.Rows[0]["name"] != name || rs.Rows[0]["description"] != "catalog test procedure" || rs.Rows[0]["read_only"] != true || rs.Rows[0]["atomic"] != false || rs.Rows[0]["min_args"] != 0 || rs.Rows[0]["max_args"] != 0 || rs.Rows[0]["calls"] != uint64(0) {
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
