package engine

import (
	"reflect"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestQueryProfileFastPaths(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	left := storage.NewTable("profile_input", []storage.Column{
		{Name: "id", Type: storage.IntType},
		{Name: "grp", Type: storage.TextType},
		{Name: "value", Type: storage.Float64Type},
	}, false)
	for i := range 4096 {
		left.Rows = append(left.Rows, []any{i, []string{"a", "b", "c", "d"}[i%4], float64(4096 - i)})
	}
	right := storage.NewTable("profile_right", []storage.Column{
		{Name: "id", Type: storage.IntType},
		{Name: "category", Type: storage.TextType},
	}, false)
	for i := range 64 {
		right.Rows = append(right.Rows, []any{i, []string{"a", "b"}[i%2]})
	}
	for _, table := range []*storage.Table{left, right} {
		if err := db.Put("default", table); err != nil {
			t.Fatal(err)
		}
	}
	for _, tc := range []struct {
		name, sql string
		stages    map[string]int64
	}{
		{"limit_zero", "SELECT id FROM profile_input LIMIT 0", map[string]int64{}},
		{"distinct_limit_zero", "SELECT DISTINCT grp FROM profile_input LIMIT 0", map[string]int64{}},
		{"empty_filtered", "SELECT id FROM profile_input WHERE id < 0", map[string]int64{"SCAN/FILTER/PROJECT": 0}},
		{"unfiltered", "SELECT id FROM profile_input LIMIT 5", map[string]int64{"SCAN/PROJECT": 5}},
		{"filtered", "SELECT id FROM profile_input WHERE id > 4 LIMIT 5", map[string]int64{"SCAN/FILTER/PROJECT": 5}},
		{"distinct", "SELECT DISTINCT grp FROM profile_input ORDER BY grp", map[string]int64{"SCAN/FILTER/PROJECT/DISTINCT": 4, "SORT": 4}},
		{"ordered", "SELECT id FROM profile_input ORDER BY id DESC", map[string]int64{"SCAN/FILTER": 4096, "SORT": 4096, "PROJECT": 4096}},
		{"top_n", "SELECT id FROM profile_input ORDER BY id DESC LIMIT 5", map[string]int64{"SCAN/FILTER/TOP-N": 5, "SORT": 5, "PROJECT": 5}},
		{"float_top_n", "SELECT id FROM profile_input ORDER BY value LIMIT 5", map[string]int64{"VALIDATE": 0, "SCAN/TOP-N": 5, "SORT": 5, "PROJECT": 5}},
		{"scored_top_n", "SELECT id, FTS_RANK(grp, 'a') AS score FROM profile_input ORDER BY score DESC LIMIT 5", map[string]int64{"SCAN/FILTER/TOP-N": 5, "SORT": 5, "PROJECT": 5}},
		{"scalar_aggregate", "SELECT grp, MIN(value) FROM profile_input GROUP BY grp", map[string]int64{"PLAN/INDEX": 0, "SCAN/FILTER/AGGREGATE": 4, "PROJECT": 4}},
		{"batch_aggregate", "SELECT grp, SUM(value) AS total FROM profile_input GROUP BY grp HAVING SUM(value) > 0 ORDER BY total", map[string]int64{"PLAN/INDEX": 0, "SCAN/FILTER/AGGREGATE": 4, "HAVING": 4, "PROJECT": 4, "SORT": 4}},
		{"metadata_count", "SELECT COUNT(*) FROM profile_input", map[string]int64{"COUNT": 1, "PROJECT": 1}},
		{"join", "SELECT a.id, b.category FROM profile_input a JOIN profile_right b ON a.id = b.id", map[string]int64{"JOIN LOOKUP": 64, "JOIN PROBE/FILTER/PROJECT": 64}},
		{"join_count", "SELECT b.category, COUNT(*) FROM profile_input a JOIN profile_right b ON a.id = b.id GROUP BY b.category", map[string]int64{"JOIN LOOKUP": 64, "JOIN PROBE/FILTER/AGGREGATE": 2, "PROJECT": 2}},
		{"join_sum", "SELECT b.category, SUM(a.value) FROM profile_input a JOIN profile_right b ON a.id = b.id GROUP BY b.category", map[string]int64{"JOIN LOOKUP": 64, "JOIN PROBE/FILTER/AGGREGATE": 2, "PROJECT": 2}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			statement := mustParse(tc.sql).(*Select)
			env := ExecEnv{ctx: t.Context(), db: db, tenant: "default"}
			want, err := executeSelect(env, statement)
			if err != nil {
				t.Fatal(err)
			}
			env.profile = newQueryProfile()
			got, err := executeSelect(env, statement)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("profiling changed the result: got %v; want %v", got, want)
			}
			seen := make(map[string]int64)
			for _, node := range env.profile.nodes {
				seen[node.operation] += node.rows
				if node.calls != 1 || node.total < node.children || node.rows < 0 {
					t.Errorf("invalid completed stage: %+v", node)
				}
			}
			for operation, rows := range tc.stages {
				if actual, ok := seen[operation]; !ok || actual != rows {
					t.Errorf("stage %s: rows=%d present=%v; want %d (all stages: %v)", operation, actual, ok, rows, seen)
				}
			}
			if len(seen) != len(tc.stages)+1 { // SELECT contains the executed stages.
				t.Errorf("unexpected stages: %v; want SELECT and %v", seen, tc.stages)
			}
		})
	}
}

func TestQueryProfileConstraintMaximum(t *testing.T) {
	db := idUniqueFixture(t, 64)
	// Removing the remembered maximum retains the constraint index but makes
	// its maximum unknown until the first following MAX query.
	benefitExec(t, db, "DELETE FROM id_unique WHERE id = 63")
	env := ExecEnv{ctx: t.Context(), db: db, tenant: "default"}
	statement := mustParse("SELECT MAX(id) AS value FROM id_unique").(*Select)
	for _, cold := range []bool{true, false} {
		env.profile = newQueryProfile()
		result, err := executeSelect(env, statement)
		if err != nil || result.Rows[0]["value"] != 62 {
			t.Fatalf("MAX result=%v error=%v", result, err)
		}
		var cacheID int
		var scan *queryPhaseStats
		for i := range env.profile.nodes {
			node := &env.profile.nodes[i]
			if node.operation == "PLAN/INDEX" && node.detail == "constraint maximum cache lookup" {
				cacheID = i + 1
				if node.rows != 1 || node.calls != 1 || node.total < node.children {
					t.Fatalf("invalid cache stage: %+v", node)
				}
			}
			if node.operation == "SCAN/AGGREGATE" {
				scan = node
			}
		}
		if cacheID == 0 || (scan != nil) != cold {
			t.Fatalf("cold=%v: cache=%d scan=%+v", cold, cacheID, scan)
		}
		if scan != nil && (scan.parent != cacheID || scan.rows != 1) {
			t.Fatalf("cached maximum scan attached incorrectly: %+v", scan)
		}
	}
}
