package engine

import (
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestQueryProfileGeneralSelectStages(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	table := storage.NewTable("items", []storage.Column{{Name: "a", Type: storage.TextType}, {Name: "v", Type: storage.IntType}}, false)
	table.Rows = [][]any{{"a", 1}, {"a", 2}, {"b", 3}, {"c", 4}}
	if err := db.Put("default", table); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, sql     string
		operations    []string
		child, parent string
	}{
		{
			name:       "group and tail",
			sql:        "SELECT DISTINCT a, SUM(v) AS s FROM (SELECT * FROM items) AS d WHERE v >= 0 GROUP BY a HAVING SUM(v) > 0 ORDER BY s DESC LIMIT 2 OFFSET 1",
			operations: []string{"SOURCE", "FILTER", "GROUP/PROJECT/HAVING", "DISTINCT", "SORT", "LIMIT"},
			child:      "SELECT", parent: "SOURCE",
		},
		{
			name:       "join",
			sql:        "SELECT d.a, e.v FROM (SELECT * FROM items) AS d JOIN items AS e ON d.v = e.v WHERE e.v > 1",
			operations: []string{"SOURCE", "JOIN", "FILTER", "PROJECT"},
			child:      "SELECT", parent: "SOURCE",
		},
		{
			name:       "compound",
			sql:        "SELECT 1 AS x UNION SELECT 2 AS x ORDER BY x LIMIT 1",
			operations: []string{"SOURCE", "PROJECT", "SET OPERATION", "SORT", "LIMIT"},
			child:      "SELECT", parent: "SET OPERATION",
		},
		{
			name:       "CTE",
			sql:        "WITH c AS (SELECT a FROM items) SELECT a FROM c",
			operations: []string{"CTE", "SCAN/FILTER/PROJECT"},
			child:      "SELECT", parent: "CTE",
		},
		{
			name:       "subquery predicate",
			sql:        "SELECT a FROM (SELECT * FROM items) AS d WHERE v > (SELECT MIN(v) FROM items)",
			operations: []string{"SOURCE", "FILTER", "PROJECT"},
			child:      "SELECT", parent: "FILTER",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := ExecEnv{ctx: t.Context(), db: db, tenant: "default"}
			stmt := mustParse(tc.sql).(*Select)
			want, err := executeSelect(env, stmt)
			if err != nil {
				t.Fatal(err)
			}
			env.profile = newQueryProfile()
			// A deterministic clock checks nesting and self-time accounting
			// without assertions about scheduler timing.
			var tick int64
			env.profile.clock = func() time.Time {
				tick++
				return time.Unix(0, tick)
			}
			got, err := executeSelect(env, stmt)
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("profiled result = %#v, error %v; want %#v", got, err, want)
			}
			found := make(map[string]bool)
			nested := false
			for _, node := range env.profile.nodes {
				found[node.operation] = true
				if node.calls < 1 || node.total <= 0 || node.children > node.total {
					t.Fatalf("unfinished or incorrectly nested phase: %#v", node)
				}
				if node.operation == tc.child && node.parent > 0 && env.profile.nodes[node.parent-1].operation == tc.parent {
					nested = true
				}
			}
			for _, operation := range tc.operations {
				if !found[operation] {
					t.Errorf("missing %s phase: %#v", operation, env.profile.nodes)
				}
			}
			if !nested {
				t.Errorf("missing %s child of %s", tc.child, tc.parent)
			}
			if root := env.profile.nodes[0]; root.rows != int64(len(got.Rows)) || root.parent != 0 {
				t.Errorf("SELECT root = %#v", root)
			}
		})
	}
}

func TestQueryProfileGeneralSelectErrorFinishesPhases(t *testing.T) {
	env := ExecEnv{ctx: t.Context(), db: storage.NewDB(), tenant: "default", profile: newQueryProfile()}
	t.Cleanup(func() { _ = env.db.Close() })
	_, err := executeSelect(env, mustParse("SELECT x FROM (SELECT x FROM missing) AS d").(*Select))
	if err == nil {
		t.Fatal("missing table query succeeded")
	}
	for _, node := range env.profile.nodes {
		if node.calls < 1 || node.children > node.total {
			t.Fatalf("unfinished or incorrectly nested phase after error: %#v", node)
		}
	}
}

func TestQueryProfileConcurrentProcedureReadsKeepSummary(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	table := storage.NewTable("items", []storage.Column{{Name: "v", Type: storage.IntType}}, false)
	table.Rows = [][]any{{1}, {2}}
	if err := db.Put("default", table); err != nil {
		t.Fatal(err)
	}
	const procedure = "profile_concurrent_reads_test"
	t.Cleanup(func() { UnregisterStoredProcedure(procedure) })
	if err := RegisterStoredProcedureWithOptions(procedure, StoredProcedureOptions{ReadOnly: true}, func(ctx ProcedureContext, _ []any) (*ResultSet, error) {
		// A Go procedure can schedule reads concurrently. The analyzer must
		// measure the whole CALL without sharing a synchronous phase collector
		// between these independently executing child statements.
		var workers sync.WaitGroup
		var errs [8]error
		start := make(chan struct{})
		for i := range errs {
			workers.Add(1)
			go func() {
				defer workers.Done()
				<-start
				for range 16 {
					rs, err := ctx.ExecuteSQL("SELECT SUM(v) AS total FROM items")
					if err != nil {
						errs[i] = err
						return
					}
					if len(rs.Rows) != 1 || toFloat(rs.Rows[0]["total"]) != 3 {
						errs[i] = fmt.Errorf("unexpected child result: %#v", rs.Rows)
						return
					}
				}
			}()
		}
		close(start)
		workers.Wait()
		for _, err := range errs {
			if err != nil {
				return nil, err
			}
		}
		return &ResultSet{Cols: []string{"reads"}, Rows: []Row{{"reads": 128}}}, nil
	}); err != nil {
		t.Fatal(err)
	}
	rs, err := Execute(t.Context(), db, "default", mustParse("EXPLAIN ANALYZE CALL "+procedure+"()"))
	if err != nil {
		t.Fatal(err)
	}
	measured := 0
	for _, row := range rs.Rows {
		if row["total_ns"] == nil {
			continue
		}
		measured++
		if row["operation"] != "EXECUTE" || row["parent_step"] != nil || row["calls"] != int64(1) || row["actual_rows"] != int64(1) {
			t.Errorf("CALL should expose only its execution summary: %#v", row)
		}
		if row["self_ns"] != row["total_ns"] || row["percent"] != float64(100) || row["bottleneck"] != true {
			t.Errorf("unexpected CALL timing: %#v", row)
		}
	}
	if measured != 1 {
		t.Errorf("measured phases = %d, want 1", measured)
	}
	if !reflect.DeepEqual(table.Rows, [][]any{{1}, {2}}) {
		t.Errorf("concurrent procedure reads modified source rows: %#v", table.Rows)
	}
}
