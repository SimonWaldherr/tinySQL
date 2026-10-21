package engine

import (
	"context"
	"testing"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

type explainRefreshTableFunc struct {
	name string
	run  func() (*ResultSet, error)
}

func (f explainRefreshTableFunc) Name() string              { return f.name }
func (f explainRefreshTableFunc) ValidateArgs([]Expr) error { return nil }
func (f explainRefreshTableFunc) Execute(context.Context, []Expr, ExecEnv, Row) (*ResultSet, error) {
	return f.run()
}

func TestExplainDoesNotRefreshMaterializedView(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	refreshes := 0
	const sourceName = "EXPLAIN_VIEW_SOURCE"
	const expireName = "EXPLAIN_EXPIRE_VIEW"
	RegisterTableFunc(explainRefreshTableFunc{name: sourceName, run: func() (*ResultSet, error) {
		refreshes++
		return &ResultSet{Cols: []string{"n"}, Rows: []Row{{"n": refreshes}}}, nil
	}})
	RegisterTableFunc(explainRefreshTableFunc{name: expireName, run: func() (*ResultSet, error) {
		// Expire the cache after the measured query has resolved its source.
		// This models a refresh interval elapsing during a long projection,
		// without depending on scheduler timing or sleeping in a test.
		err := db.Catalog().FinishMaterializedViewRefresh("main", "explain_cached_view", time.Now().Add(-2*time.Hour), 0, 1, "")
		return &ResultSet{Cols: []string{"n"}, Rows: []Row{{"n": 1}}}, err
	}})
	t.Cleanup(func() {
		delete(tableFuncRegistry, sourceName)
		delete(tableFuncRegistry, expireName)
	})
	execSQL(t, db, "CREATE MATERIALIZED VIEW explain_cached_view AS SELECT n FROM "+sourceName+"() REFRESH ON STALE AFTER 1 HOURS WITH DATA")
	if refreshes != 1 {
		t.Fatalf("initial refresh count = %d, want 1", refreshes)
	}
	execSQL(t, db, "EXPLAIN ANALYZE SELECT (SELECT n FROM "+expireName+"()) AS n FROM explain_cached_view")
	if refreshes != 1 {
		t.Fatalf("rendering the measured plan refreshed the view: refresh count = %d, want 1", refreshes)
	}
	execSQL(t, db, "EXPLAIN SELECT n FROM explain_cached_view")
	execSQL(t, db, "EXPLAIN SELECT SUM(n) FROM explain_cached_view")
	if refreshes != 1 {
		t.Fatalf("plain EXPLAIN refreshed a stale view: refresh count = %d, want 1", refreshes)
	}
	execSQL(t, db, "EXPLAIN ANALYZE SELECT n FROM explain_cached_view")
	if refreshes != 2 {
		t.Fatalf("analyzed execution must refresh the stale view once: refresh count = %d, want 2", refreshes)
	}
	// Direct storage users can install a physical table with the same name.
	// Its normal table plan must retain precedence over the catalog entry.
	table := storage.NewTable("explain_cached_view", []storage.Column{{Name: "n", Type: storage.IntType}}, false)
	table.Rows = [][]any{{3}}
	if err := db.Put("default", table); err != nil {
		t.Fatal(err)
	}
	plan := execSQL(t, db, "EXPLAIN SELECT n FROM explain_cached_view")
	for _, row := range plan.Rows {
		if row["operation"] == "TABLE SCAN" {
			return
		}
	}
	t.Fatalf("missing physical table plan: %#v", plan.Rows)
}
