package tinysql_test

import (
	"bytes"
	"context"
	"fmt"
	"strings"
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

func TestPublicSQLStateHelpers(t *testing.T) {
	base := fmt.Errorf("bad sql")
	err := tsql.WithSQLState(tsql.SQLStateSyntaxError, base)
	if got := tsql.SQLState(err); got != tsql.SQLStateSyntaxError {
		t.Fatalf("SQLState = %q, want %q", got, tsql.SQLStateSyntaxError)
	}
	if !strings.Contains(err.Error(), tsql.SQLStateSyntaxError) {
		t.Fatalf("expected SQLSTATE in error text, got %q", err.Error())
	}
}

func TestPublicExecSQL(t *testing.T) {
	db := tsql.NewDB()
	ctx := context.Background()
	if _, err := tsql.ExecSQL(ctx, db, "default", `CREATE TABLE exec_sql_users (id INTEGER, name TEXT)`); err != nil {
		t.Fatalf("CREATE via ExecSQL: %v", err)
	}
	if _, err := tsql.ExecSQL(ctx, db, "default", `INSERT INTO exec_sql_users VALUES (1, 'Ada')`); err != nil {
		t.Fatalf("INSERT via ExecSQL: %v", err)
	}
	rs, err := tsql.ExecSQL(ctx, db, "default", `SELECT name FROM exec_sql_users`)
	if err != nil || len(rs.Rows) != 1 || rs.Rows[0]["name"] != "Ada" {
		t.Fatalf("SELECT via ExecSQL: rows=%#v err=%v", rs, err)
	}
}

func TestPublicReaderWriterPersistence(t *testing.T) {
	db := tsql.NewDB()
	if _, err := tsql.ExecSQL(context.Background(), db, "default", `CREATE TABLE snapshots (id INTEGER)`); err != nil {
		t.Fatalf("create snapshot table: %v", err)
	}
	var snapshot bytes.Buffer
	if err := tsql.SaveToWriter(db, &snapshot); err != nil {
		t.Fatalf("SaveToWriter: %v", err)
	}
	restored, err := tsql.LoadFromReader(&snapshot)
	if err != nil {
		t.Fatalf("LoadFromReader: %v", err)
	}
	if _, err := tsql.ExecSQL(context.Background(), restored, "default", `SELECT * FROM snapshots`); err != nil {
		t.Fatalf("query restored snapshot: %v", err)
	}
}

func TestPublicStoredProcedureHelpers(t *testing.T) {
	const procName = "public_proc_echo_test"
	tsql.UnregisterStoredProcedure(procName)
	t.Cleanup(func() { tsql.UnregisterStoredProcedure(procName) })

	err := tsql.RegisterStoredProcedure(procName, func(ctx tsql.ProcedureContext, args []any) (*tsql.ResultSet, error) {
		return &tsql.ResultSet{
			Cols: []string{"value", "tenant"},
			Rows: []tsql.Row{{"value": args[0], "tenant": ctx.Tenant()}},
		}, nil
	})
	if err != nil {
		t.Fatalf("RegisterStoredProcedure failed: %v", err)
	}
	if len(tsql.ListStoredProcedures()) == 0 {
		t.Fatal("expected registered procedure to be listed")
	}

	rs, err := tsql.Execute(context.Background(), tsql.NewDB(), "default", tsql.MustParseSQL(`CALL public_proc_echo_test('x')`))
	if err != nil {
		t.Fatalf("CALL failed: %v", err)
	}
	if len(rs.Rows) != 1 || rs.Rows[0]["value"] != "x" || rs.Rows[0]["tenant"] != "default" {
		t.Fatalf("unexpected procedure result: %#v", rs.Rows)
	}
}

func TestPublicMapImportWrappers(t *testing.T) {
	ctx := context.Background()
	db := tsql.NewDB()

	if _, err := tsql.ImportYAML(ctx, db, "default", "yaml_public", strings.NewReader("- id: 1\n"), &tsql.ImportOptions{CreateTable: true}); err != nil {
		t.Fatalf("ImportYAML failed: %v", err)
	}
	if _, err := tsql.ImportXML(ctx, db, "default", "xml_public", strings.NewReader("<root><row id=\"1\"/></root>"), &tsql.ImportOptions{CreateTable: true}); err != nil {
		t.Fatalf("ImportXML failed: %v", err)
	}
	if _, err := tsql.ImportGeoJSON(ctx, db, "default", "geo_public", strings.NewReader(`{"type":"Feature","properties":{"name":"x"},"geometry":{"type":"Point","coordinates":[1,2]}}`), &tsql.ImportOptions{CreateTable: true}); err != nil {
		t.Fatalf("ImportGeoJSON failed: %v", err)
	}
	if _, err := tsql.ImportOSM(ctx, db, "default", "osm_public", strings.NewReader(`<osm><node id="1" lat="1" lon="2"/></osm>`), &tsql.ImportOptions{CreateTable: true}); err != nil {
		t.Fatalf("ImportOSM failed: %v", err)
	}
	if _, err := tsql.ImportRoutingGraph(ctx, db, "default", "rg_public", strings.NewReader(`[{"source":"a","target":"b","cost":1}]`), &tsql.ImportOptions{CreateTable: true}); err != nil {
		t.Fatalf("ImportRoutingGraph failed: %v", err)
	}
}

func TestPublicRBACHelpers(t *testing.T) {
	perm, err := tsql.ParsePermission("select")
	if err != nil {
		t.Fatalf("ParsePermission failed: %v", err)
	}
	if perm != tsql.PermSelect {
		t.Fatalf("ParsePermission = %q, want %q", perm, tsql.PermSelect)
	}

	db := tsql.NewDB()
	ctx := context.Background()
	if _, err := tsql.Execute(ctx, db, "default", tsql.MustParseSQL("CREATE TABLE public_rbac (id INT)")); err != nil {
		t.Fatalf("create failed: %v", err)
	}
	if err := db.Catalog().CreateRole("reader"); err != nil {
		t.Fatalf("CreateRole failed: %v", err)
	}
	if err := db.Catalog().GrantPermission("reader", tsql.PermSelect, "default", "public_rbac"); err != nil {
		t.Fatalf("GrantPermission failed: %v", err)
	}
	if err := db.Catalog().CreateUser("alice", "secret", []string{"reader"}); err != nil {
		t.Fatalf("CreateUser failed: %v", err)
	}
	if !db.Catalog().HasPermission("alice", tsql.PermSelect, "default", "public_rbac") {
		t.Fatal("expected public Permission alias to work with CatalogManager")
	}
	table := tsql.NewTable("api_constraints", []tsql.Column{
		{Name: "id", Type: tsql.IntType, Constraint: tsql.PrimaryKey},
	}, false)
	if table.Cols[0].Constraint != tsql.PrimaryKey {
		t.Fatal("expected public ConstraintType alias to work with Column")
	}
	var _ = tsql.Grant{Permission: tsql.PermSelect, Schema: "default", Table: "public_rbac"}
	var _ = db.Catalog()
	var _ = db.Catalog().GetTables()
	var _ = db.Catalog().ListRoles()
	var _ = db.Catalog().ListUsers()
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
