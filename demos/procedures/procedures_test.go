package procedures

import (
	"context"
	"fmt"
	"testing"

	tinysql "github.com/SimonWaldherr/tinySQL"
)

func TestRegisteredDemoProceduresExecute(t *testing.T) {
	if err := Register(); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{
		"demo_table_summary",
		"demo_runtime_status",
		"demo_geo_distance",
		"demo_find_functions",
		"demo_log_event",
		"demo_event_summary",
		"demo_release_features",
	} {
		name := name
		t.Cleanup(func() { tinysql.UnregisterStoredProcedure(name) })
	}

	db := tinysql.NewDB()
	ctx := context.Background()
	for _, query := range []string{
		`CALL demo_geo_distance(52.5200, 13.4050, 48.1372, 11.5755)`,
		`CALL demo_find_functions('RAG%')`,
		`CALL demo_log_event('test', 'event')`,
		`CALL demo_table_summary()`,
		`CALL demo_runtime_status()`,
		`CALL demo_release_features()`,
	} {
		result, err := tinysql.Execute(ctx, db, "default", tinysql.MustParseSQL(query))
		if err != nil {
			t.Fatalf("%s: %v", query, err)
		}
		if result == nil || len(result.Rows) == 0 {
			t.Fatalf("%s returned no demo rows", query)
		}
	}

	result, err := tinysql.Execute(ctx, db, "default", tinysql.MustParseSQL(
		`SELECT read_only, atomic, calls, errors FROM sys.procedures WHERE name = 'demo_log_event'`,
	))
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 1 || result.Rows[0]["atomic"] != true || result.Rows[0]["read_only"] != false || result.Rows[0]["calls"] != uint64(1) || result.Rows[0]["errors"] != uint64(0) {
		t.Fatalf("unexpected demo procedure metadata: %#v", result.Rows)
	}
}

func TestEventSummary(t *testing.T) {
	if err := Register(); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"demo_table_summary", "demo_runtime_status", "demo_geo_distance", "demo_find_functions", "demo_log_event", "demo_event_summary", "demo_release_features"} {
		name := name
		t.Cleanup(func() { tinysql.UnregisterStoredProcedure(name) })
	}
	db := tinysql.NewDB()
	ctx := context.Background()
	run := func(query string) *tinysql.ResultSet {
		t.Helper()
		result, err := tinysql.Execute(ctx, db, "default", tinysql.MustParseSQL(query))
		if err != nil {
			t.Fatalf("%s: %v", query, err)
		}
		return result
	}
	if rows := run(`CALL demo_event_summary()`).Rows; len(rows) != 0 {
		t.Fatalf("empty summary: %#v", rows)
	}
	if rows := run(`SELECT name FROM sys.tables WHERE name = 'demo_procedure_log'`).Rows; len(rows) != 0 {
		t.Fatal("read-only summary created a table")
	}
	run(`CALL demo_log_event('alpha', 'first')`)
	run(`CALL demo_log_event('alpha', 'second')`)
	run(`CALL demo_log_event('beta', 'third')`)
	if rows := run(`CALL demo_event_summary()`).Rows; len(rows) != 2 {
		t.Fatalf("summary: %#v", rows)
	}
	rows := run(`CALL demo_event_summary('alpha')`).Rows
	if len(rows) != 1 || rows[0]["category"] != "alpha" || fmt.Sprint(rows[0]["total"]) != "2" {
		t.Fatalf("filtered summary: %#v", rows)
	}
	if rows := run(`CALL demo_event_summary('missing')`).Rows; len(rows) != 0 {
		t.Fatalf("missing category: %#v", rows)
	}
}
