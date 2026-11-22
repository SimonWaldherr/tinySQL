package engine

import (
	"math"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestQueryProfileAccounting(t *testing.T) {
	p := newQueryProfile()
	now := time.Unix(0, 0)
	p.clock = func() time.Time { return now }
	env, root := startQueryPhase(ExecEnv{profile: p}, "EXECUTE", "SELECT")
	now = now.Add(10 * time.Nanosecond)
	childEnv, child := startQueryPhase(env, "SORT", "keys")
	now = now.Add(20 * time.Nanosecond)
	_, nested := startQueryPhase(childEnv, "PROJECT", "expression")
	now = now.Add(30 * time.Nanosecond)
	nested.finish(3)
	now = now.Add(10 * time.Nanosecond)
	child.finish(3)
	child.finish(99) // Deferred error cleanup must not count a stage twice.
	now = now.Add(10 * time.Nanosecond)
	_, child = startQueryPhase(env, "SORT", "keys")
	now = now.Add(20 * time.Nanosecond)
	child.finish(2)
	now = now.Add(10 * time.Nanosecond)
	root.finish(5)
	rows := []Row{{"operation": "PLAN"}}
	p.appendRows(&rows)
	if len(rows) != 4 {
		t.Fatalf("repeated stages were not aggregated: %v", rows)
	}
	wantTotal := []int64{110, 80, 30}
	wantSelf := []int64{30, 50, 30}
	for i, row := range rows[1:] {
		if row["total_ns"] != wantTotal[i] || row["self_ns"] != wantSelf[i] || row["bottleneck"] != (i == 1) {
			t.Fatalf("stage %d: %#v", i, row)
		}
	}
	if rows[2]["calls"] != int64(2) || rows[2]["actual_rows"] != int64(5) || rows[2]["parent_step"] != 2 || rows[3]["parent_step"] != 3 {
		t.Fatalf("incorrect counts/parents: %#v", rows)
	}
	var disabled *queryPhase
	disabled.finish(0)
	original := ExecEnv{tenant: "example"}
	got, disabled := startQueryPhase(original, "SORT", "disabled")
	if disabled != nil || got.tenant != original.tenant || got.profile != nil {
		t.Fatal("disabled profiling changed the environment")
	}
}

func profileFixture(t *testing.T) *storage.DB {
	t.Helper()
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	execSQL(t, db, "CREATE TABLE profile_values (id INT, g TEXT, v INT)")
	execSQL(t, db, "INSERT INTO profile_values VALUES (1,'a',3),(2,'a',2),(3,'b',1),(4,'b',NULL)")
	return db
}

func checkExecutionProfile(t *testing.T, profile QueryProfile, rows int64) map[string]QueryStage {
	t.Helper()
	if profile.ParseTime != 0 || len(profile.Stages) == 0 {
		t.Fatalf("invalid execution profile: %+v", profile)
	}
	root := profile.Stages[0]
	if root.Operation != "EXECUTE" || root.Parent != 0 || root.Total != profile.Elapsed || root.Rows != rows {
		t.Fatalf("invalid root: %+v", root)
	}
	operations := make(map[string]QueryStage)
	var self time.Duration
	for i, stage := range profile.Stages {
		if stage.Calls < 1 || stage.Rows < 0 || stage.Self < 0 || stage.Self > stage.Total {
			t.Fatalf("invalid accounting: %+v", stage)
		}
		if i > 0 && (stage.Parent < 1 || stage.Parent > i || profile.Stages[stage.Parent-1].Total < stage.Total) {
			t.Fatalf("invalid parent at stage %d: %+v", i+1, stage)
		}
		self += stage.Self
		operations[stage.Operation] = stage
	}
	if self != profile.Elapsed {
		t.Fatalf("self time = %s, elapsed = %s", self, profile.Elapsed)
	}
	return operations
}

func TestExecuteWithProfileSelect(t *testing.T) {
	db := profileFixture(t)
	stmt := mustParse("SELECT id FROM profile_values WHERE v>1")
	want, err := Execute(t.Context(), db, "default", stmt)
	if err != nil {
		t.Fatal(err)
	}
	got, profile, err := ExecuteWithProfile(t.Context(), db, "default", stmt)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("profiled result = %#v, error %v; want %#v", got, err, want)
	}
	stages := checkExecutionProfile(t, profile, 2)
	for _, operation := range []string{"LOCK", "RUN", "SELECT", "PLAN/INDEX", "SCAN/FILTER/PROJECT"} {
		if _, ok := stages[operation]; !ok {
			t.Errorf("missing %s: %+v", operation, profile)
		}
	}
	if lock := stages["LOCK"]; lock.Parent != 1 || lock.Detail != "shared content lock" {
		t.Errorf("read lock stage: %+v", lock)
	}
	if parent := stages["PLAN/INDEX"].Parent; parent > 0 && profile.Stages[parent-1].Operation != "SELECT" {
		t.Errorf("planning stage parent: %+v", profile.Stages[parent-1])
	}
}

func TestExecuteWithProfileWriteAndError(t *testing.T) {
	db := profileFixture(t)
	execSQL(t, db, "CREATE TABLE writes (id INT PRIMARY KEY)")
	stmt := mustParse("INSERT INTO writes VALUES (1) RETURNING id")
	result, profile, err := ExecuteWithProfile(t.Context(), db, "default", stmt)
	if err != nil || result == nil || len(result.Rows) != 1 {
		t.Fatalf("profiled INSERT result = %#v, error %v", result, err)
	}
	stages := checkExecutionProfile(t, profile, 1)
	if lock := stages["LOCK"]; lock.Parent != 1 || lock.Detail != "exclusive content lock" {
		t.Errorf("write lock stage: %+v", lock)
	}
	for _, operation := range []string{"PREPARE", "RUN", "FINALIZE"} {
		if stages[operation].Parent != 1 {
			t.Fatalf("missing %s stage: %+v", operation, profile)
		}
	}
	_, profile, err = ExecuteWithProfile(t.Context(), db, "default", mustParse("INSERT INTO writes VALUES (2), (1)"))
	if err == nil {
		t.Fatal("duplicate key INSERT succeeded")
	}
	stages = checkExecutionProfile(t, profile, 0)
	if stages["RUN"].Parent != 1 {
		t.Fatalf("missing failure dispatch stage: %+v", profile)
	}
	if writes := execSQL(t, db, "SELECT id FROM writes"); len(writes.Rows) != 1 || writes.Rows[0]["id"] != 1 {
		t.Fatalf("failed statement did not roll back: %#v", writes.Rows)
	}
}

func TestExecuteWithProfileEarlyError(t *testing.T) {
	db := profileFixture(t)
	if err := db.Catalog().CreateUser("profile_user", "test-password", nil); err != nil {
		t.Fatal(err)
	}
	_, profile, err := ExecuteWithProfile(t.Context(), db, "default", mustParse("SELECT id FROM profile_values"))
	if err == nil {
		t.Fatal("unauthenticated SELECT succeeded")
	}
	checkExecutionProfile(t, profile, 0)
	if len(profile.Stages) != 1 {
		t.Fatalf("unauthorized query entered the executor: %+v", profile)
	}
}

func TestExecuteWithProfileSelectErrorFinishesScan(t *testing.T) {
	db := profileFixture(t)
	stmt := mustParse("SELECT id FROM profile_values WHERE 1 / (id-id) > 0")
	_, profile, err := ExecuteWithProfile(t.Context(), db, "default", stmt)
	if err == nil {
		t.Fatal("division by zero succeeded")
	}
	stages := checkExecutionProfile(t, profile, 0)
	if scan := stages["SCAN/FILTER/PROJECT"]; scan.Calls != 1 || scan.Rows != 0 {
		t.Fatalf("failed scan accounting: %+v", profile)
	}
}

func TestExecuteWithProfileRecoversFromSourcePanic(t *testing.T) {
	db := profileFixture(t)
	const source = "PROFILE_PANIC_SOURCE"
	RegisterTableFunc(explainRefreshTableFunc{name: source, run: func() (*ResultSet, error) {
		panic("interrupted source")
	}})
	t.Cleanup(func() { delete(tableFuncRegistry, source) })
	_, profile, err := ExecuteWithProfile(t.Context(), db, "default", mustParse("SELECT n FROM "+source+"()"))
	if err == nil {
		t.Fatal("panicking source returned success")
	}
	stages := checkExecutionProfile(t, profile, 0)
	if source := stages["SOURCE"]; source.Calls != 1 || source.Rows != 0 {
		t.Fatalf("interrupted source accounting: %+v", profile)
	}
}

func validateProfile(t *testing.T, rs *ResultSet, count int) map[string]bool {
	t.Helper()
	if len(rs.Cols) != 10 {
		t.Fatalf("columns: %v", rs.Cols)
	}
	if rs.Rows[len(rs.Rows)-1]["operation"] != "ANALYZE" {
		t.Fatal("missing final summary")
	}
	byStep := make(map[int]Row)
	operations := make(map[string]bool)
	var total, self int64
	percent := float64(0)
	hottest := 0
	for _, row := range rs.Rows {
		if row["calls"] == nil {
			continue
		}
		calls := row["calls"].(int64)
		duration := row["total_ns"].(int64)
		own := row["self_ns"].(int64)
		if calls < 1 || duration < 0 || own < 0 || own > duration {
			t.Fatalf("invalid counters: %v", row)
		}
		if row["parent_step"] == nil {
			total = duration
			if row["actual_rows"] != int64(count) {
				t.Fatalf("root rows: %v", row)
			}
		} else {
			parent, ok := byStep[row["parent_step"].(int)]
			if !ok || parent["total_ns"].(int64) < duration {
				t.Fatalf("invalid parent: %v", row)
			}
		}
		byStep[row["step"].(int)] = row
		self += own
		percent += row["percent"].(float64)
		if row["bottleneck"].(bool) {
			hottest++
		}
		operations[row["operation"].(string)] = true
	}
	if len(byStep) < 2 || self != total || math.Abs(percent-100) > 0.000001 || hottest != 1 {
		t.Fatalf("inconsistent profile: total=%d self=%d percent=%f hottest=%d rows=%v", total, self, percent, hottest, rs.Rows)
	}
	return operations
}

func TestExplainAnalyzeStages(t *testing.T) {
	db := profileFixture(t)
	for _, query := range []string{
		"SELECT id FROM profile_values WHERE v>1",
		"SELECT id,v FROM profile_values ORDER BY v DESC LIMIT 2",
		"SELECT g,SUM(v) FROM profile_values GROUP BY g ORDER BY g",
		"SELECT DISTINCT g FROM profile_values",
		"SELECT a.id,b.v FROM profile_values a JOIN profile_values b ON a.id=b.id",
		"SELECT a.g,SUM(b.v) FROM profile_values a JOIN profile_values b ON a.id=b.id GROUP BY a.g",
		"WITH q AS (SELECT id FROM profile_values) SELECT id FROM q WHERE id>1",
		"SELECT id FROM (SELECT id FROM profile_values) q WHERE id>1 ORDER BY id",
		"SELECT id FROM profile_values UNION SELECT v FROM profile_values ORDER BY id",
		"SELECT id,(SELECT MAX(b.v) FROM profile_values b WHERE b.v IS NOT NULL) AS prior FROM profile_values a",
		"SELECT id, ROW_NUMBER() OVER (ORDER BY v) AS n FROM profile_values",
	} {
		t.Run(query, func(t *testing.T) {
			want := execSQL(t, db, query)
			got := execSQL(t, db, "EXPLAIN ANALYZE "+query)
			validateProfile(t, got, len(want.Rows))
			again := execSQL(t, db, query)
			if !reflect.DeepEqual(want, again) {
				t.Fatalf("analyzer changed results: %v vs %v", want, again)
			}
		})
	}
	plain := execSQL(t, db, "EXPLAIN SELECT id FROM profile_values")
	if !reflect.DeepEqual(plain.Cols, []string{"step", "operation", "detail"}) {
		t.Fatalf("plain EXPLAIN changed: %v", plain.Cols)
	}
	for _, row := range plain.Rows {
		if row["calls"] != nil {
			t.Fatal("plain EXPLAIN collected timings")
		}
	}
}

func TestExplainAnalyzeExecutionIsolation(t *testing.T) {
	db := profileFixture(t)
	stmt := mustParse("EXPLAIN ANALYZE SELECT g,SUM(v) FROM profile_values GROUP BY g ORDER BY g")
	var wg sync.WaitGroup
	for range 6 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 5 {
				rs, err := Execute(t.Context(), db, "default", stmt)
				if err != nil {
					t.Error(err)
					return
				}
				validateProfile(t, rs, 2)
			}
		}()
	}
	wg.Wait()
}

func TestExplainAnalyzeCachedSubqueryCalls(t *testing.T) {
	db := profileFixture(t)
	rs := execSQL(t, db, "EXPLAIN ANALYZE SELECT a.id,(SELECT MAX(b.v) FROM profile_values b WHERE b.v IS NOT NULL) AS prior FROM profile_values a")
	validateProfile(t, rs, 4)
	repeated := false
	for _, row := range rs.Rows {
		if row["operation"] == "SELECT" && row["detail"] == "profile_values AS b" && row["calls"] == int64(1) {
			repeated = true
		}
	}
	if !repeated {
		t.Fatalf("missing single cached subquery execution: %v", rs.Rows)
	}
}
