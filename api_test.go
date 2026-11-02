package tinysql_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	tsql "github.com/SimonWaldherr/tinySQL"
	"github.com/SimonWaldherr/tinySQL/driver"
	"github.com/SimonWaldherr/tinySQL/sqlutil"
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

func TestPublicRAGPreFilterJSON(t *testing.T) {
	options, err := tsql.RAGPreFilterJSON(tsql.RAGPreFilter{
		IDColumn:      "chunk_id",
		AllowedRowIDs: []any{"chunk-7", 42},
		Equals:        map[string]any{"tenant_id": "acme", "visibility": "published"},
		Spatial: &tsql.RAGSpatialFilter{
			GeometryColumn: "geom",
			BBox:           []float64{11, 48, 12, 49},
		},
	})
	if err != nil {
		t.Fatalf("RAGPreFilterJSON: %v", err)
	}

	var payload struct {
		PreFilter struct {
			IDColumn      string                 `json:"id_column"`
			AllowedRowIDs []any                  `json:"allowed_row_ids"`
			Equals        map[string]any         `json:"equals"`
			Spatial       *tsql.RAGSpatialFilter `json:"spatial"`
		} `json:"pre_filter"`
	}
	if err := json.Unmarshal([]byte(options), &payload); err != nil {
		t.Fatalf("decode generated options: %v", err)
	}
	if payload.PreFilter.IDColumn != "chunk_id" || len(payload.PreFilter.AllowedRowIDs) != 2 || payload.PreFilter.AllowedRowIDs[0] != "chunk-7" || payload.PreFilter.Equals["tenant_id"] != "acme" || payload.PreFilter.Spatial == nil || payload.PreFilter.Spatial.GeometryColumn != "geom" {
		t.Fatalf("unexpected pre-filter options: %#v", payload.PreFilter)
	}

	// An explicit empty list is the useful deny-all ACL form; it must not be
	// mistaken for an absent filter and omitted by JSON encoding.
	denyAll, err := tsql.RAGPreFilterJSON(tsql.RAGPreFilter{AllowedRowIDs: []any{}})
	if err != nil {
		t.Fatalf("encode deny-all ACL: %v", err)
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal([]byte(denyAll), &raw); err != nil {
		t.Fatalf("decode deny-all options: %v", err)
	}
	var filter map[string]json.RawMessage
	if err := json.Unmarshal(raw["pre_filter"], &filter); err != nil {
		t.Fatalf("decode deny-all filter: %v", err)
	}
	if got := string(filter["allowed_row_ids"]); got != "[]" {
		t.Fatalf("deny-all allowed_row_ids = %s, want []", got)
	}

	if _, err := tsql.RAGPreFilterJSON(tsql.RAGPreFilter{}); err == nil {
		t.Fatal("expected empty RAG pre-filter to be rejected")
	}
	if _, err := tsql.RAGPreFilterJSON(tsql.RAGPreFilter{IDColumn: "chunk_id", Equals: map[string]any{"tenant_id": "acme"}}); err == nil {
		t.Fatal("expected IDColumn without AllowedRowIDs to be rejected")
	}
	if _, err := tsql.RAGPreFilterJSON(tsql.RAGPreFilter{Spatial: &tsql.RAGSpatialFilter{GeometryColumn: "geom", BBox: []float64{1, 2}}}); err == nil {
		t.Fatal("expected invalid spatial BBox to be rejected")
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

func TestPublicStreamingSQL(t *testing.T) {
	db := tsql.NewDB()
	ctx := context.Background()
	if _, err := tsql.ExecSQL(ctx, db, "default", `CREATE TABLE stream_api (id INTEGER, name TEXT)`); err != nil {
		t.Fatal(err)
	}
	if _, err := tsql.ExecSQL(ctx, db, "default", `INSERT INTO stream_api VALUES (1, 'Ada'), (2, 'Grace')`); err != nil {
		t.Fatal(err)
	}

	stream, err := tsql.ExecSQLStream(ctx, db, "default", `SELECT id, name FROM stream_api`)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	var names []string
	for stream.Next() {
		names = append(names, stream.Row()["name"].(string))
	}
	if err := stream.Err(); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(names, ","); got != "Ada,Grace" {
		t.Fatalf("streamed names = %q", got)
	}

	compiled := tsql.NewQueryCache(4).MustCompile(`SELECT name FROM stream_api`)
	compiledStream, err := tsql.ExecuteCompiledStream(ctx, db, "default", compiled)
	if err != nil {
		t.Fatal(err)
	}
	if !compiledStream.Next() || compiledStream.Row()["name"] != "Ada" {
		t.Fatalf("compiled stream first row = %#v, err=%v", compiledStream.Row(), compiledStream.Err())
	}
	if err := compiledStream.Close(); err != nil {
		t.Fatal(err)
	}

	strict, err := tsql.ExecSQLStreamWithOptions(ctx, db, "default", `SELECT id FROM stream_api`, tsql.StreamOptions{Buffer: 0})
	if err != nil {
		t.Fatal(err)
	}
	for strict.Next() {
	}
	if err := strict.Err(); err != nil {
		t.Fatal(err)
	}
	if got := strict.Stats(); got.BufferCapacity != 0 || got.RowsProduced != 2 || !got.Complete {
		t.Fatalf("strict stream stats = %#v", got)
	}
	if tsql.APIVersion != 1 || tsql.Version() == "" {
		t.Fatalf("unexpected public package version: api=%d version=%q", tsql.APIVersion, tsql.Version())
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
	err = tsql.RegisterStoredProcedureWithOptions(procName, tsql.StoredProcedureOptions{
		Description: "public echo procedure",
		ReadOnly:    true,
		Parameters: []tsql.StoredProcedureParameter{
			{Name: "value", Required: true},
		},
	}, func(ctx tsql.ProcedureContext, args []any) (*tsql.ResultSet, error) {
		return ctx.ExecuteSQLArgs(`SELECT ? AS value, ? AS tenant`, args[0], ctx.Tenant())
	})
	if err != nil {
		t.Fatalf("RegisterStoredProcedureWithOptions failed: %v", err)
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
	if _, err := tsql.ExecuteCompiled(ctx, db, "default", nil); err == nil {
		t.Fatal("expected nil compiled execution to fail")
	}
	if _, err := tsql.ExecuteCompiledStream(ctx, db, "default", nil); err == nil {
		t.Fatal("expected nil compiled stream to fail")
	}

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

// --- Parameters and scripts ---

func ExampleExecSQLArgs() {
	ctx := context.Background()
	db := tsql.NewDB()
	defer db.Close()

	if _, err := tsql.ExecSQL(ctx, db, "default", "CREATE TABLE users (id INT, name TEXT)"); err != nil {
		panic(err)
	}
	// Values are bound as literals; quotes in user input cannot change the SQL.
	if _, err := tsql.ExecSQLArgs(ctx, db, "default", "INSERT INTO users VALUES (?, ?)", 1, "O'Hara"); err != nil {
		panic(err)
	}
	rs, err := tsql.ExecSQLArgs(ctx, db, "default", "SELECT name FROM users WHERE id = $1", 1)
	if err != nil {
		panic(err)
	}
	fmt.Println(rs.Rows[0]["name"])
	// Output: O'Hara
}

func ExampleExecScript() {
	ctx := context.Background()
	db := tsql.NewDB()
	defer db.Close()

	rs, err := tsql.ExecScript(ctx, db, "default", `
		-- Schema and seed data in one call.
		CREATE TABLE notes (id INT, body TEXT);
		INSERT INTO notes VALUES (1, 'semicolons; inside literals are fine');
		SELECT COUNT(*) AS n FROM notes;`)
	if err != nil {
		panic(err)
	}
	fmt.Println(rs.Rows[0]["n"])
	// Output: 1
}

func TestExecSQLArgsTypes(t *testing.T) {
	ctx := context.Background()
	db := tsql.NewDB()
	defer db.Close()
	if _, err := tsql.ExecSQL(ctx, db, "default", "CREATE TABLE v (i INT, f FLOAT, s TEXT, b BOOL, raw BLOB, n TEXT, ts TEXT)"); err != nil {
		t.Fatal(err)
	}
	when := time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)
	if _, err := tsql.ExecSQLArgs(ctx, db, "default", "INSERT INTO v VALUES (?, ?, ?, ?, ?, ?, ?)",
		int32(-7), 2.5, "a'b -- ? ;", true, []byte{0, 1, 255}, nil, when); err != nil {
		t.Fatal(err)
	}
	rs, err := tsql.ExecSQLArgs(ctx, db, "default", `SELECT * FROM v WHERE s = ? AND "i" = :2`, "a'b -- ? ;", -7)
	if err != nil {
		t.Fatal(err)
	}
	if len(rs.Rows) != 1 {
		t.Fatalf("rows: %#v", rs.Rows)
	}
	row := rs.Rows[0]
	if fmt.Sprint(row["i"]) != "-7" || row["s"] != "a'b -- ? ;" || row["b"] != true || row["n"] != nil {
		t.Fatalf("unexpected row: %#v", row)
	}
	if raw, ok := row["raw"].([]byte); !ok || string(raw) != "\x00\x01\xff" {
		t.Fatalf("blob: %#v", row["raw"])
	}
	if row["ts"] != "2026-09-27T12:00:00Z" {
		t.Fatalf("time: %#v", row["ts"])
	}
	for _, bad := range []struct {
		sql  string
		args []any
	}{{"SELECT ?", nil}, {"SELECT 1", []any{1}}, {"SELECT $2", []any{1}}} {
		if _, err := tsql.ExecSQLArgs(ctx, db, "default", bad.sql, bad.args...); err == nil {
			t.Errorf("%q with %d args succeeded", bad.sql, len(bad.args))
		}
	}
}

func TestExecScriptStopsAtFailingStatement(t *testing.T) {
	ctx := context.Background()
	db := tsql.NewDB()
	defer db.Close()
	_, err := tsql.ExecScript(ctx, db, "default", `
		CREATE TABLE t (id INT);
		INSERT INTO t VALUES (1);
		INSERT INTO missing VALUES (2);
		INSERT INTO t VALUES (3);`)
	if err == nil || !strings.Contains(err.Error(), "statement 3") {
		t.Fatalf("error %v, want statement 3", err)
	}
	rs, err := tsql.ExecSQL(ctx, db, "default", "SELECT COUNT(*) AS n FROM t")
	if err != nil || fmt.Sprint(rs.Rows[0]["n"]) != "1" {
		t.Fatalf("applied prefix: %#v, %v", rs, err)
	}
	if rs, err := tsql.ExecScript(ctx, db, "default", " ; -- nothing\n"); err != nil || rs != nil {
		t.Fatalf("empty script: %#v, %v", rs, err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := tsql.ExecScript(cancelled, db, "default", "SELECT 1"); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled script: %v", err)
	}
}

func TestExecScriptKeepsTriggerBodies(t *testing.T) {
	ctx := context.Background()
	db := tsql.NewDB()
	defer db.Close()
	script := `
CREATE TABLE src (id INT);
CREATE TABLE audit (id INT, kind TEXT);
CREATE TRIGGER copy AFTER INSERT ON src FOR EACH ROW BEGIN
	INSERT INTO audit VALUES (NEW.id, CASE WHEN NEW.id > 1 THEN 'big' ELSE 'small' END);
	INSERT INTO audit VALUES (NEW.id, 'copy;');
END;
INSERT INTO src VALUES (2);
SELECT COUNT(*) AS n FROM audit;`
	if got := len(sqlutil.SplitStatements(script)); got != 5 {
		t.Fatalf("split into %d statements", got)
	}
	rs, err := tsql.ExecScript(ctx, db, "default", script)
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(rs.Rows[0]["n"]) != "2" {
		t.Fatalf("trigger rows: %#v", rs.Rows)
	}
}

// --- Extensions ---

type testExtension struct {
	info       tsql.ExtensionInfo
	register   func(*tsql.DB) error
	registries int
}

func (e *testExtension) ExtensionInfo() tsql.ExtensionInfo { return e.info }

func (e *testExtension) Register(db *tsql.DB) error {
	e.registries++
	if e.register != nil {
		return e.register(db)
	}
	return nil
}

func TestDBUseRegistersStaticExtensionAndExposesMetadata(t *testing.T) {
	db := tsql.NewDB()
	extension := &testExtension{info: tsql.ExtensionInfo{
		Name:        "example.audit",
		Version:     "1.2.3",
		Description: "  Audit helpers for example data.  ",
		Capabilities: []tsql.ExtensionCapability{
			tsql.CapabilityWrite,
			tsql.CapabilityNetwork,
			tsql.CapabilityWrite,
		},
	}}

	if err := db.Use(extension); err != nil {
		t.Fatalf("db.Use: %v", err)
	}
	if extension.registries != 1 {
		t.Fatalf("Register calls = %d, want 1", extension.registries)
	}

	infos := db.Extensions()
	if len(infos) != 1 {
		t.Fatalf("Extensions length = %d, want 1", len(infos))
	}
	if got, want := infos[0].Description, "Audit helpers for example data."; got != want {
		t.Fatalf("description = %q, want %q", got, want)
	}
	if got, want := len(infos[0].Capabilities), 2; got != want {
		t.Fatalf("capability count = %d, want %d", got, want)
	}
	if infos[0].LoadedAt.IsZero() {
		t.Fatal("LoadedAt was not set")
	}

	// The result is a copy, not a mutable handle to the DB registry.
	infos[0].Capabilities[0] = "changed"
	if got := string(db.Extensions()[0].Capabilities[0]); got == "changed" {
		t.Fatal("Extensions returned mutable registry data")
	}

	stmt, err := tsql.ParseSQL("SELECT name, version, language, linkage, capabilities FROM sys.extensions")
	if err != nil {
		t.Fatalf("parse sys.extensions query: %v", err)
	}
	rs, err := tsql.Execute(context.Background(), db, "default", stmt)
	if err != nil {
		t.Fatalf("query sys.extensions: %v", err)
	}
	if len(rs.Rows) != 1 {
		t.Fatalf("sys.extensions rows = %#v", rs.Rows)
	}
	row := rs.Rows[0]
	if row["name"] != "example.audit" || row["version"] != "1.2.3" || row["language"] != "GO" || row["linkage"] != "STATIC" {
		t.Fatalf("unexpected sys.extensions row: %#v", row)
	}
	if got, want := row["capabilities"], "network,write"; got != want {
		t.Fatalf("capabilities = %#v, want %q", got, want)
	}

	objectStmt, err := tsql.ParseSQL("SELECT object_type, version FROM sys.objects WHERE name = 'example.audit'")
	if err != nil {
		t.Fatalf("parse sys.objects query: %v", err)
	}
	objects, err := tsql.Execute(context.Background(), db, "default", objectStmt)
	if err != nil {
		t.Fatalf("query sys.objects: %v", err)
	}
	if len(objects.Rows) != 1 || objects.Rows[0]["object_type"] != "EXTENSION" || objects.Rows[0]["version"] != "1.2.3" {
		t.Fatalf("unexpected extension object row: %#v", objects.Rows)
	}
}

func TestDBUseRejectsDuplicateAndFailedExtensions(t *testing.T) {
	db := tsql.NewDB()
	extension := &testExtension{info: tsql.ExtensionInfo{Name: "example.dupe", Version: "1.0.0"}}
	if err := db.Use(extension); err != nil {
		t.Fatalf("initial db.Use: %v", err)
	}
	if err := db.Use(extension); err == nil {
		t.Fatal("duplicate db.Use succeeded")
	}
	if extension.registries != 1 {
		t.Fatalf("duplicate extension Register calls = %d, want 1", extension.registries)
	}

	failed := &testExtension{
		info:     tsql.ExtensionInfo{Name: "example.broken", Version: "1.0.0"},
		register: func(*tsql.DB) error { return errors.New("test failure") },
	}
	if err := db.Use(failed); err == nil {
		t.Fatal("failing db.Use succeeded")
	}
	for _, info := range db.Extensions() {
		if info.Name == "example.broken" {
			t.Fatalf("failed extension remained active: %#v", info)
		}
	}
}

// --- Query subscriptions ---

func nextChange(t *testing.T, s *tsql.QuerySubscription) tsql.QueryChange {
	t.Helper()
	select {
	case c, ok := <-s.Changes:
		if !ok {
			t.Fatalf("subscription stopped: %v", s.Err())
		}
		return c
	case <-time.After(5 * time.Second):
		t.Fatal("subscription timeout")
		return tsql.QueryChange{}
	}
}
func execSubscriptionSQL(t *testing.T, db *tsql.DB, query string) {
	t.Helper()
	if _, err := tsql.ExecSQL(t.Context(), db, "default", query); err != nil {
		t.Fatal(err)
	}
}
func TestSubscriptionIncrementalAndOwnership(t *testing.T) {
	db := tsql.NewDB()
	defer db.Close()
	execSubscriptionSQL(t, db, `CREATE TABLE items (id INT PRIMARY KEY, payload BLOB)`)
	execSubscriptionSQL(t, db, `INSERT INTO items VALUES (1,X'01'), (2,X'02')`)
	sub, err := tsql.SubscribeSQL(t.Context(), db, "default", `SELECT id, payload FROM items WHERE id > 0`)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	first := nextChange(t, sub)
	if !first.Initial || len(first.Added) != 2 || first.ScannedRows != 2 {
		t.Fatal(first)
	}
	first.Added[0]["payload"].([]byte)[0] = 255
	execSubscriptionSQL(t, db, `INSERT INTO items VALUES (3,X'03'), (-1,X'00')`)
	add := nextChange(t, sub)
	if add.Initial || len(add.Added) != 1 || add.ScannedRows != 2 {
		t.Fatal(add)
	}
	execSubscriptionSQL(t, db, `UPDATE items SET payload = X'04' WHERE id = 1`)
	update := nextChange(t, sub)
	if update.ScannedRows != 1 || len(update.Added) != 1 || len(update.Removed) != 1 || update.Removed[0]["payload"].([]byte)[0] != 1 {
		t.Fatal(update)
	}
	execSubscriptionSQL(t, db, `DELETE FROM items WHERE id = 3`)
	deleted := nextChange(t, sub)
	if len(deleted.Removed) != 1 || deleted.Removed[0]["id"] != 3 {
		t.Fatal(deleted)
	}
	if sub.Err() != nil {
		t.Fatal(sub.Err())
	}
}
func TestSubscriptionTransactionsAndRollback(t *testing.T) {
	db := tsql.NewDB()
	defer db.Close()
	execSubscriptionSQL(t, db, `CREATE TABLE events (id INT PRIMARY KEY)`)
	sub, err := tsql.SubscribeSQL(t.Context(), db, "default", `SELECT id FROM events`)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	nextChange(t, sub)
	pool, err := driver.OpenWithDB(db)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	tx, err := pool.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO events VALUES (1)`); err != nil {
		t.Fatal(err)
	}
	select {
	case c := <-sub.Changes:
		t.Fatalf("uncommitted event: %v", c)
	default:
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	tx, err = pool.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO events VALUES (2), (3)`); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	change := nextChange(t, sub)
	if len(change.Added) != 2 || len(change.Removed) != 0 {
		t.Fatal(change)
	}
	for _, row := range change.Added {
		if row["id"] == 1 {
			t.Fatal("rolled back row leaked")
		}
	}
}
func TestSubscriptionSlowReaderAndShutdown(t *testing.T) {
	db := tsql.NewDB()
	execSubscriptionSQL(t, db, `CREATE TABLE events (id INT)`)
	sub, err := tsql.SubscribeSQL(t.Context(), db, "default", `SELECT id FROM events`)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	// Leave the initial result queued while many statements commit.
	for i := 0; i < 100; i++ {
		execSubscriptionSQL(t, db, fmt.Sprintf("INSERT INTO events VALUES (%d)", i))
	}
	total := 0
	for total < 100 {
		c := nextChange(t, sub)
		total += len(c.Added) - len(c.Removed)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case _, ok := <-sub.Changes:
		if ok {
			t.Fatal("unexpected event")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("close did not stop subscription")
	}
	if _, err := tsql.SubscribeSQL(context.Background(), db, "default", `SELECT id FROM events`); err == nil {
		t.Fatal("closed DB accepted")
	}
}
func TestSubscriptionRejectsUnsupportedQueries(t *testing.T) {
	db := tsql.NewDB()
	defer db.Close()
	execSubscriptionSQL(t, db, `CREATE TABLE items (id INT)`)
	for _, query := range []string{`DELETE FROM items`, `UPDATE items SET id = 1`, `CREATE TABLE other (id INT)`} {
		if s, err := tsql.SubscribeSQL(t.Context(), db, "default", query); err == nil {
			s.Close()
			t.Fatalf("accepted %s", query)
		}
	}
}

func TestSubscriptionDropStopsWithError(t *testing.T) {
	db := tsql.NewDB()
	defer db.Close()
	execSubscriptionSQL(t, db, `CREATE TABLE items (id INT)`)
	sub, err := tsql.SubscribeSQL(t.Context(), db, "default", `SELECT id FROM items`)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	nextChange(t, sub)
	execSubscriptionSQL(t, db, `DROP TABLE items`)
	select {
	case _, ok := <-sub.Changes:
		if ok || sub.Err() == nil {
			t.Fatal("missing terminal error")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("drop did not stop subscription")
	}
}
func TestSubscriptionCloseWithUnreadResults(t *testing.T) {
	db := tsql.NewDB()
	execSubscriptionSQL(t, db, `CREATE TABLE items (id INT)`)
	sub, err := tsql.SubscribeSQL(t.Context(), db, "default", `SELECT id FROM items`)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		execSubscriptionSQL(t, db, fmt.Sprintf("INSERT INTO items VALUES (%d)", i))
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	// Drain at most the bounded buffered result; DB closure must terminate
	// even if the worker was blocked delivering an additional change.
	timeout := time.After(5 * time.Second)
	for {
		select {
		case _, ok := <-sub.Changes:
			if !ok {
				sub.Close()
				return
			}
		case <-timeout:
			t.Fatal("close blocked by unread result")
		}
	}
}

func TestSubscriptionGroupedView(t *testing.T) {
	db := tsql.NewDB()
	defer db.Close()
	execSubscriptionSQL(t, db, `CREATE TABLE sales (category TEXT, amount INT)`)
	execSubscriptionSQL(t, db, `CREATE VIEW totals AS SELECT category, SUM(amount) AS total FROM sales GROUP BY category`)
	sub, err := tsql.SubscribeSQL(t.Context(), db, "default", `SELECT * FROM totals`)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	initial := nextChange(t, sub)
	if !initial.Initial || len(initial.Added) != 0 {
		t.Fatal(initial)
	}
	execSubscriptionSQL(t, db, `INSERT INTO sales VALUES ('books', 10), ('books', 20)`)
	added := nextChange(t, sub)
	if len(added.Added) != 1 || len(added.Removed) != 0 {
		t.Fatal(added)
	}
	execSubscriptionSQL(t, db, `UPDATE sales SET amount = 15 WHERE amount = 10`)
	updated := nextChange(t, sub)
	if len(updated.Added) != 1 || len(updated.Removed) != 1 || fmt.Sprint(updated.Added[0]["total"]) != "35" {
		t.Fatal(updated)
	}
	execSubscriptionSQL(t, db, `DROP VIEW totals`)
	select {
	case _, ok := <-sub.Changes:
		if ok || sub.Err() == nil {
			t.Fatal("missing dropped-view error")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("drop view did not stop subscription")
	}
}

func TestSubscriptionRoutesTableChangesAndViewReplacement(t *testing.T) {
	db := tsql.NewDB()
	defer db.Close()
	execSubscriptionSQL(t, db, `CREATE TABLE a (id INT)`)
	execSubscriptionSQL(t, db, `CREATE TABLE b (id INT)`)
	execSubscriptionSQL(t, db, `INSERT INTO a VALUES (1)`)
	execSubscriptionSQL(t, db, `INSERT INTO b VALUES (10)`)
	execSubscriptionSQL(t, db, `CREATE VIEW selected AS SELECT SUM(id) AS total FROM a`)
	sub, err := tsql.SubscribeSQL(t.Context(), db, "default", `SELECT * FROM selected`)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	nextChange(t, sub)
	before := sub.Stats()
	execSubscriptionSQL(t, db, `INSERT INTO b VALUES (20)`)
	if got := sub.Stats(); got.Notifications != before.Notifications {
		t.Fatalf("unrelated table woke view: %+v", got)
	}
	pool, err := driver.OpenWithDB(db)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	tx, err := pool.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tx.ExecContext(t.Context(), `DROP VIEW selected`); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.ExecContext(t.Context(), `CREATE VIEW selected AS SELECT SUM(id) AS total FROM b`); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	change := nextChange(t, sub)
	if len(change.Added) != 1 || fmt.Sprint(change.Added[0]["total"]) != "30" {
		t.Fatal(change)
	}
	before = sub.Stats()
	execSubscriptionSQL(t, db, `INSERT INTO a VALUES (2)`)
	if got := sub.Stats(); got.Notifications != before.Notifications {
		t.Fatalf("old view dependency retained: %+v", got)
	}
	execSubscriptionSQL(t, db, `INSERT INTO b VALUES (5)`)
	change = nextChange(t, sub)
	if len(change.Added) != 1 || fmt.Sprint(change.Added[0]["total"]) != "35" {
		t.Fatal(change)
	}
}

func TestSubscriptionResultLimits(t *testing.T) {
	db := tsql.NewDB()
	defer db.Close()
	execSubscriptionSQL(t, db, `CREATE TABLE a (id INT, payload TEXT)`)
	execSubscriptionSQL(t, db, `INSERT INTO a VALUES (1, 'abcdefghijklmnopqrstuvwxyz')`)
	if sub, err := tsql.SubscribeSQLWithOptions(t.Context(), db, "default", `SELECT payload FROM a`, tsql.SubscriptionOptions{MaxResultBytes: 8}); err == nil {
		sub.Close()
		t.Fatal("accepted oversized result")
	}
	sub, err := tsql.SubscribeSQLWithOptions(t.Context(), db, "default", `SELECT id FROM a`, tsql.SubscriptionOptions{MaxResultRows: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	nextChange(t, sub)
	execSubscriptionSQL(t, db, `INSERT INTO a VALUES (2, 'x')`)
	select {
	case _, ok := <-sub.Changes:
		if ok || sub.Err() == nil {
			t.Fatal("missing result-limit error")
		}
	case <-time.After(time.Second):
		t.Fatal("oversized subscription did not stop")
	}
	if sub, err := tsql.SubscribeSQLWithOptions(t.Context(), db, "default", `SELECT id + 1 AS next_id FROM a`, tsql.SubscriptionOptions{MaxResultRows: 1}); err == nil {
		sub.Close()
		t.Fatal("accepted oversized general SELECT")
	}
}

// --- JSON mode ---

// TestJSONModeReadmeExample exercises the README's ModeJSON example: open,
// write data, close, and confirm the table lands on disk as readable JSON.
func TestJSONModeReadmeExample(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "tinysql")

	db, err := tsql.OpenDB(tsql.StorageConfig{
		Mode: tsql.ModeJSON,
		Path: dir,
	})
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}

	ctx := context.Background()
	for _, sql := range []string{
		`CREATE TABLE notes (id INT, body TEXT)`,
		`INSERT INTO notes VALUES (1, 'hello json mode')`,
	} {
		stmt, err := tsql.ParseSQL(sql)
		if err != nil {
			t.Fatalf("parse %q: %v", sql, err)
		}
		if _, err := tsql.Execute(ctx, db, "default", stmt); err != nil {
			t.Fatalf("execute %q: %v", sql, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	data, err := os.ReadFile(filepath.Join(dir, "default", "notes.json"))
	if err != nil {
		t.Fatalf("expected notes.json on disk: %v", err)
	}
	if !strings.Contains(string(data), "hello json mode") {
		t.Fatalf("expected readable row content in JSON file, got: %s", data)
	}

	reopened, err := tsql.OpenDB(tsql.StorageConfig{Mode: tsql.ModeJSON, Path: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	selectStmt, _ := tsql.ParseSQL(`SELECT body FROM notes WHERE id = 1`)
	rs, err := tsql.Execute(ctx, reopened, "default", selectStmt)
	if err != nil {
		t.Fatalf("select after reopen: %v", err)
	}
	if len(rs.Rows) != 1 || rs.Rows[0]["body"] != "hello json mode" {
		t.Fatalf("unexpected result after reopen: %+v", rs.Rows)
	}
}

// --- JSON table API ---

func TestExportTableJSONPublicAPI(t *testing.T) {
	ctx := context.Background()
	db := tsql.NewDB()
	for _, sql := range []string{
		`CREATE TABLE events (id INT, name TEXT)`,
		`INSERT INTO events VALUES (1, 'launch')`,
	} {
		stmt, err := tsql.ParseSQL(sql)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tsql.Execute(ctx, db, "default", stmt); err != nil {
			t.Fatal(err)
		}
	}
	var out bytes.Buffer
	if err := tsql.ExportTableJSON(ctx, &out, db, "default", "events", tsql.ExportOptions{}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), `"launch"`) {
		t.Fatalf("JSON = %s", out.String())
	}
}

func TestExportTableJSONRestoresJSONColumns(t *testing.T) {
	ctx := context.Background()
	db := tsql.NewDB()
	for _, sql := range []string{
		`CREATE TABLE payloads (id INT, meta JSON)`,
		`INSERT INTO payloads VALUES (1, '{"source":"api"}')`,
	} {
		stmt, err := tsql.ParseSQL(sql)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tsql.Execute(ctx, db, "default", stmt); err != nil {
			t.Fatal(err)
		}
	}
	var out bytes.Buffer
	if err := tsql.ExportTableJSON(ctx, &out, db, "default", "payloads", tsql.ExportOptions{}); err != nil {
		t.Fatal(err)
	}
	var rows []map[string]any
	if err := json.Unmarshal(out.Bytes(), &rows); err != nil {
		t.Fatal(err)
	}
	meta, ok := rows[0]["meta"].(map[string]any)
	if !ok || meta["source"] != "api" {
		t.Fatalf("JSON column = %#v, want object", rows[0]["meta"])
	}
}

// --- Read-only databases ---

// TestReadOnlyReadmeExample exercises the exact load -> close -> reopen
// read-only -> warm cycle documented in the README's "Read-only mode for
// bulk-load / serve-only workloads" section, so the example stays accurate
// as the public API evolves.
func TestReadOnlyReadmeExample(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "db.gob")

	// Load phase: in-memory, with a path so Close() writes a full snapshot.
	// (ModeWAL/ModeAdvancedWAL do not currently log tsql.Execute mutations
	// automatically — see the README caveat under "Read-only mode".)
	db, err := tsql.OpenDB(tsql.StorageConfig{Mode: tsql.ModeMemory, Path: dbPath})
	if err != nil {
		t.Fatalf("open read-write: %v", err)
	}
	ctx := context.Background()
	for _, sql := range []string{
		`CREATE TABLE docs (id INT, body TEXT, embedding VECTOR)`,
		`INSERT INTO docs VALUES (1, 'hello world', '[0.1, 0.2, 0.3]')`,
		`INSERT INTO docs VALUES (2, 'goodbye world', '[0.4, 0.5, 0.6]')`,
	} {
		stmt, err := tsql.ParseSQL(sql)
		if err != nil {
			t.Fatalf("parse %q: %v", sql, err)
		}
		if _, err := tsql.Execute(ctx, db, "default", stmt); err != nil {
			t.Fatalf("execute %q: %v", sql, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close read-write db: %v", err)
	}

	// Serve phase: read-only, loaded from the same snapshot.
	serveDB, err := tsql.OpenDB(tsql.StorageConfig{Mode: tsql.ModeMemory, Path: dbPath, ReadOnly: true})
	if err != nil {
		t.Fatalf("open read-only: %v", err)
	}
	defer serveDB.Close()
	if !serveDB.IsReadOnly() {
		t.Fatal("expected IsReadOnly() = true")
	}

	warmStmt, err := tsql.ParseSQL(`SELECT * FROM VEC_WARM('docs', 'embedding', 'cosine', 'hnsw')`)
	if err != nil {
		t.Fatalf("parse VEC_WARM: %v", err)
	}
	rs, err := tsql.Execute(ctx, serveDB, "default", warmStmt)
	if err != nil {
		t.Fatalf("execute VEC_WARM: %v", err)
	}
	if len(rs.Rows) != 1 {
		t.Fatalf("expected 1 warm-up stats row, got %d", len(rs.Rows))
	}

	// A write must be rejected in read-only mode.
	insertStmt, _ := tsql.ParseSQL(`INSERT INTO docs VALUES (3, 'nope', '[0,0,0]')`)
	if _, err := tsql.Execute(ctx, serveDB, "default", insertStmt); err == nil {
		t.Fatal("expected write to be rejected in read-only mode")
	}

	// Reads (including the now-warmed vector search) still work.
	searchStmt, _ := tsql.ParseSQL(`SELECT id FROM VEC_SEARCH('docs', 'embedding', '[0.1, 0.2, 0.3]', 1, 'cosine', 'hnsw')`)
	rs, err = tsql.Execute(ctx, serveDB, "default", searchStmt)
	if err != nil {
		t.Fatalf("execute VEC_SEARCH: %v", err)
	}
	if len(rs.Rows) != 1 {
		t.Fatalf("expected 1 search result, got %d", len(rs.Rows))
	}
}

// --- JSON storage types ---

// SQL-declared INT/INTEGER columns carry SQLite integer affinity. ModeJSON
// used to reload their values as float64, changing the type and rounding
// integers above 2^53.
func TestModeJSONReloadKeepsIntegerColumns(t *testing.T) {
	if strconv.IntSize < 64 {
		t.Skip("requires 64-bit INT")
	}
	ctx := context.Background()
	dir := t.TempDir()
	open := func() *tsql.DB {
		db, err := tsql.OpenDB(tsql.StorageConfig{Mode: tsql.ModeJSON, Path: dir})
		if err != nil {
			t.Fatal(err)
		}
		return db
	}
	db := open()
	if _, err := tsql.ExecScript(ctx, db, "default", `
		CREATE TABLE t (id INT, big INTEGER, score FLOAT, loose INTEGER);
		INSERT INTO t VALUES (42, 9007199254740993, 1.0, 1.5);`); err != nil {
		t.Fatal(err)
	}
	before, err := tsql.ExecSQL(ctx, db, "default", "SELECT * FROM t")
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = open()
	defer db.Close()
	after, err := tsql.ExecSQL(ctx, db, "default", "SELECT * FROM t")
	if err != nil {
		t.Fatal(err)
	}
	for _, column := range []string{"id", "big", "score", "loose"} {
		want, got := before.Rows[0][column], after.Rows[0][column]
		if fmt.Sprintf("%T %v", want, want) != fmt.Sprintf("%T %v", got, got) {
			t.Errorf("%s: before reload %T %v, after %T %v", column, want, want, got, got)
		}
	}
	if after.Rows[0]["big"] != 9007199254740993 {
		t.Fatalf("big integer lost precision: %v", after.Rows[0]["big"])
	}
}

// --- Columnar execution ---

func TestPublicColumnarAPI(t *testing.T) {
	db := tsql.NewDB()
	defer db.Close()
	result, err := tsql.ExecSQLColumnar(t.Context(), db, "default", `SELECT 42 AS answer,NULL AS missing`)
	if err != nil {
		t.Fatal(err)
	}
	var typed *tsql.ColumnarResultSet = result
	if typed.RowCount != 1 || len(typed.Values) != 2 || typed.Values[1][0] != nil {
		t.Fatal(typed)
	}
	stmt, err := tsql.ParseSQL(`SELECT 'ok' AS status`)
	if err != nil {
		t.Fatal(err)
	}
	result, err = tsql.ExecuteColumnar(t.Context(), db, "default", stmt)
	if err != nil || result.Values[0][0] != "ok" {
		t.Fatal(result, err)
	}
	if _, err := tsql.ExecSQLColumnar(t.Context(), db, "default", `SELECT (`); err == nil {
		t.Fatal("parse error lost")
	}
}

// --- Index advisor ---

func TestPublicIndexAdvisor(t *testing.T) {
	db := tsql.NewDB()
	defer db.Close()
	advisor, err := tsql.NewIndexAdvisor(db, tsql.IndexAdvisorOptions{})
	if err != nil {
		t.Fatal(err)
	}
	stmt, err := tsql.ParseSQL("SELECT 1 AS n")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := advisor.Execute(t.Context(), "default", stmt); err != nil {
		t.Fatal(err)
	}
	advisor.SetAutoCreate(true)
	if len(advisor.Recommendations()) != 0 {
		t.Fatal("unexpected recommendation")
	}
}
