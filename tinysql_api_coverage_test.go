package tinysql_test

import (
	"context"
	"testing"

	tsql "github.com/SimonWaldherr/tinySQL"
)

func TestPublicAPIWrappers(t *testing.T) {
	mode, err := tsql.ParseStorageMode("wal")
	if err != nil {
		t.Fatalf("ParseStorageMode failed: %v", err)
	}
	if mode != tsql.ModeWAL {
		t.Fatalf("ParseStorageMode = %v, want %v", mode, tsql.ModeWAL)
	}

	cfg := tsql.DefaultStorageConfig(tsql.ModeWAL)
	if cfg.Mode != tsql.ModeWAL || cfg.CheckpointEvery == 0 || cfg.CheckpointInterval == 0 {
		t.Fatalf("unexpected default WAL config: %#v", cfg)
	}

	if stmt := tsql.MustParseSQL("SELECT 1"); stmt == nil {
		t.Fatal("MustParseSQL returned nil")
	}
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("expected MustParseSQL to panic on invalid SQL")
			}
		}()
		_ = tsql.MustParseSQL("CREATE TABLE")
	}()
}

func TestPublicAPICompiledExecutionAndJobScheduler(t *testing.T) {
	db := tsql.NewDB()
	ctx := context.Background()

	create := tsql.MustParseSQL("CREATE TABLE api_items (id INT, name TEXT)")
	if _, err := tsql.Execute(ctx, db, "default", create); err != nil {
		t.Fatalf("create failed: %v", err)
	}
	insert := tsql.MustParseSQL("INSERT INTO api_items VALUES (1, 'one')")
	if _, err := tsql.Execute(ctx, db, "default", insert); err != nil {
		t.Fatalf("insert failed: %v", err)
	}

	cache := tsql.NewQueryCache(4)
	compiled, err := tsql.Compile(cache, "SELECT name FROM api_items WHERE id = 1")
	if err != nil {
		t.Fatalf("Compile failed: %v", err)
	}
	rs, err := tsql.ExecuteCompiled(ctx, db, "default", compiled)
	if err != nil {
		t.Fatalf("ExecuteCompiled failed: %v", err)
	}
	if len(rs.Rows) != 1 || rs.Rows[0]["name"] != "one" {
		t.Fatalf("unexpected compiled result: %#v", rs.Rows)
	}

	if _, err := tsql.Compile(cache, "CREATE TABLE"); err == nil {
		t.Fatal("expected Compile to reject invalid SQL")
	}
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("expected MustCompile to panic")
			}
		}()
		_ = tsql.MustCompile(cache, "CREATE TABLE")
	}()

	exec := tsql.NewSQLJobExecutor(db, "")
	if exec.Tenant != "default" {
		t.Fatalf("default executor tenant = %q", exec.Tenant)
	}
	if _, err := exec.ExecuteSQL(ctx, "SELECT name FROM api_items WHERE id = 1"); err != nil {
		t.Fatalf("ExecuteSQL failed: %v", err)
	}
	if _, err := exec.ExecuteSQL(ctx, "CREATE TABLE"); err == nil {
		t.Fatal("expected ExecuteSQL to reject invalid SQL")
	}
	if _, err := ((*tsql.SQLJobExecutor)(nil)).ExecuteSQL(ctx, "SELECT 1"); err == nil {
		t.Fatal("expected nil executor to fail")
	}

	if err := tsql.StartJobScheduler(nil, "default"); err == nil {
		t.Fatal("expected nil DB scheduler start to fail")
	}
	if err := tsql.StartJobScheduler(db, ""); err != nil {
		t.Fatalf("StartJobScheduler failed: %v", err)
	}
	if !tsql.HealthCheck(db).SchedulerRunning {
		t.Fatal("expected health check to report running scheduler")
	}
	if err := tsql.RestartJobScheduler(db, ""); err != nil {
		t.Fatalf("RestartJobScheduler failed: %v", err)
	}
	if !tsql.HealthCheck(db).SchedulerRunning {
		t.Fatal("expected health check to report restarted scheduler")
	}
	tsql.StopJobScheduler(db)
	tsql.StopJobScheduler(nil)
	if tsql.HealthCheck(nil).OK {
		t.Fatal("expected nil DB health to be unhealthy")
	}
}

func TestPublicAPIByteSnapshots(t *testing.T) {
	db := tsql.NewDB()
	ctx := context.Background()
	if _, err := tsql.Execute(ctx, db, "default", tsql.MustParseSQL("CREATE TABLE snap_items (id INT, name TEXT)")); err != nil {
		t.Fatalf("create failed: %v", err)
	}
	if _, err := tsql.Execute(ctx, db, "default", tsql.MustParseSQL("INSERT INTO snap_items VALUES (1, 'persisted')")); err != nil {
		t.Fatalf("insert failed: %v", err)
	}

	data, err := tsql.SaveToBytes(db)
	if err != nil {
		t.Fatalf("SaveToBytes failed: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("SaveToBytes returned empty snapshot")
	}

	restored, err := tsql.LoadFromBytes(data)
	if err != nil {
		t.Fatalf("LoadFromBytes failed: %v", err)
	}
	rs, err := tsql.Execute(ctx, restored, "default", tsql.MustParseSQL("SELECT name FROM snap_items WHERE id = 1"))
	if err != nil {
		t.Fatalf("query restored snapshot failed: %v", err)
	}
	if len(rs.Rows) != 1 || rs.Rows[0]["name"] != "persisted" {
		t.Fatalf("unexpected restored rows: %#v", rs.Rows)
	}
}
