package engine

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// Runs the pre-optimization consumer pipeline, including qualified source maps
// and projection of every match before sorting and pagination.
func generalCTEOrder(env ExecEnv, stmt *Select) (*ResultSet, error) {
	rows := rowsFromCTEResult(env, env.ctes[strings.ToLower(stmt.From.Table)], stmt.From)
	rows, err := applyWhereClause(env, stmt.Where, rows)
	if err != nil {
		return nil, err
	}
	rows, columns, err := processNonAggregateQuery(env, stmt, rows)
	if err != nil {
		return nil, err
	}
	rows = applySortOrderWithLimit(stmt.OrderBy, rows, stmt.Limit, stmt.Offset)
	return &ResultSet{Cols: columns, Rows: applyOffsetLimit(stmt, rows)}, nil
}

func TestCTEOrderedSelectionMatchesGeneralPipeline(t *testing.T) {
	source := &ResultSet{Cols: []string{"id", "grp", "val"}, Rows: make([]Row, 2*rawKeyArenaChunkRows+17)}
	for i := range source.Rows {
		var value any = (i * 97) % 19
		if i%11 == 0 {
			value = nil
		}
		source.Rows[i] = Row{"id": i, "grp": i % 7, "val": value}
	}
	env := ExecEnv{ctx: t.Context(), ctes: map[string]*ResultSet{"c": source}}
	for _, query := range []string{
		"SELECT q.id AS id FROM c q ORDER BY q.val",
		"SELECT q.id AS id FROM c q ORDER BY c.val DESC NULLS FIRST",
		"SELECT q.id AS id FROM c q ORDER BY q.grp,q.val DESC NULLS LAST",
		"SELECT q.id AS id FROM c q ORDER BY q.grp DESC,q.val,q.grp",
		"SELECT q.id AS x,q.val AS x FROM c q ORDER BY x DESC",
		"SELECT q.id AS X,q.val AS x FROM c q ORDER BY X NULLS LAST",
		"SELECT q.val AS id,q.id AS source_id FROM c q ORDER BY id,source_id DESC",
		"SELECT q.id AS id,q.val AS score FROM c q ORDER BY 2 DESC,1",
		"SELECT Q.id AS ID,Q.grp AS category FROM c Q WHERE Q.val IS NOT NULL ORDER BY category,Q.val DESC",
		"SELECT q.id AS id FROM c q WHERE q.id>=19 AND NOT (q.val IS NULL) ORDER BY q.grp,q.val",
		"SELECT q.id AS id FROM c q WHERE q.id<0 ORDER BY q.val",
	} {
		for _, tail := range []string{"", " OFFSET 13", " LIMIT 1", " LIMIT 20 OFFSET 7", " LIMIT 257 OFFSET 17", " LIMIT 490", " LIMIT 999 OFFSET 999", " LIMIT 0"} {
			stmt := mustParse(query + tail).(*Select)
			want, err := generalCTEOrder(env, stmt)
			if err != nil {
				t.Fatal(query+tail, err)
			}
			got, err := executeSelect(env, stmt)
			if err != nil || !reflect.DeepEqual(got.Cols, want.Cols) || !reflect.DeepEqual(got.Rows, want.Rows) {
				t.Fatalf("%s%s: got %#v, err %v; want %#v", query, tail, got, err, want)
			}
		}
	}
	// Sorting a consumer must not change the source's physical order.
	for i, row := range source.Rows {
		if row["id"] != i {
			t.Fatal("consumer reordered materialized source", i, row)
		}
	}
}

func TestCTEOrderedSelectionKeepsErrorsAndFallbacks(t *testing.T) {
	for _, source := range []*ResultSet{
		{Cols: []string{"id", "val"}, Rows: []Row{{"id": 1, "val": 3}, {"id": 2, "val": 4}}},
		{Cols: []string{"id", "val"}, Rows: []Row{{"id": 1, "val": 3}, {"id": 2}}},
		{Cols: []string{"id", "val"}, Rows: []Row{{"id": 1}, {"id": 2, "val": 4}}},
		{Cols: []string{"id", "val"}, Rows: []Row{}},
		{Cols: []string{"id", "q.val"}, Rows: []Row{{"id": 1, "q.val": 3}}},
	} {
		env := ExecEnv{ctx: t.Context(), ctes: map[string]*ResultSet{"c": source}}
		for _, query := range []string{
			"SELECT q.val AS value FROM c q ORDER BY q.id LIMIT 1",
			"SELECT q.id AS id FROM c q ORDER BY q.val LIMIT 1",
			"SELECT missing AS value FROM c q ORDER BY q.id LIMIT 1",
			"SELECT q.id AS id FROM c q ORDER BY missing LIMIT 1",
			"SELECT q.id AS id FROM c q ORDER BY wrong.id LIMIT 1",
			"SELECT q.val AS x,q.id AS x FROM c q ORDER BY x LIMIT 1",
			"SELECT q.id AS id FROM c q WHERE q.val>0 ORDER BY q.id LIMIT 0",
			"SELECT q.id AS id FROM c q WHERE val>0 ORDER BY q.id LIMIT 1",
			"SELECT q.id AS id FROM c q WHERE vale>0 ORDER BY q.id LIMIT 1",
		} {
			stmt := mustParse(query).(*Select)
			want, wantErr := generalCTEOrder(env, stmt)
			got, gotErr := executeSelect(env, stmt)
			if fmt.Sprint(gotErr) != fmt.Sprint(wantErr) {
				t.Fatalf("%s, source=%v: error %v, want %v", query, source, gotErr, wantErr)
			}
			if gotErr == nil && (!reflect.DeepEqual(got.Cols, want.Cols) || !reflect.DeepEqual(got.Rows, want.Rows)) {
				t.Fatalf("%s, source=%v: got %#v, want %#v", query, source, got, want)
			}
		}
	}
}

func TestCTEOrderedSelectionOwnsOutputAndKeepsStatement(t *testing.T) {
	source := &ResultSet{Cols: []string{"id", "val"}, Rows: []Row{{"id": 3, "val": 1}, {"id": 1, "val": 1}, {"id": 2, "val": nil}}}
	env := ExecEnv{ctx: t.Context(), ctes: map[string]*ResultSet{"c": source}}
	query := "SELECT q.id AS id FROM c q WHERE q.id>0 ORDER BY q.val NULLS LAST,q.id DESC LIMIT 2"
	stmt, original := mustParse(query).(*Select), mustParse(query)
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for range 5 {
				got, ok, err := executeSimpleCTESelectFastPath(env, stmt)
				if err != nil || !ok || len(got.Rows) != 2 || got.Rows[0]["id"] != 3 || got.Rows[1]["id"] != 1 {
					t.Errorf("ordered selection: %#v / %v / %v", got, ok, err)
					return
				}
				got.Rows[0]["id"] = 999
				got.Rows[0]["q.val"] = 999
			}
		})
	}
	wg.Wait()
	if !reflect.DeepEqual(stmt, original) || source.Rows[0]["id"] != 3 || source.Rows[0]["val"] != 1 || len(source.Rows[0]) != 2 {
		t.Fatal("ordered consumer changed its shared statement or source")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	env.ctx = ctx
	if _, _, err := executeSimpleCTESelectFastPath(env, stmt); !errors.Is(err, context.Canceled) {
		t.Fatal("cancelled ordered scan", err)
	}
}

func TestCTEOrderedSelectionPreservesMaterialization(t *testing.T) {
	db := storage.NewDB()
	defer db.Close()
	// Reordering the producer or pushing the consumer's LIMIT into it would
	// incorrectly return 3 instead of the two-row producer's maximum, 2.
	got := execSQL(t, db, "WITH c AS (SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 LIMIT 2) SELECT q.n AS n FROM c q ORDER BY q.n DESC LIMIT 1")
	if len(got.Rows) != 1 || got.Rows[0]["n"] != 2 {
		t.Fatal("consumer crossed producer LIMIT", got)
	}
	// A sorted sibling may not reorder a CTE later consumed in source order.
	got = execSQL(t, db, "WITH c AS (SELECT 3 AS n UNION ALL SELECT 1 UNION ALL SELECT 2), d AS (SELECT n FROM c ORDER BY n), e AS (SELECT n FROM c LIMIT 1) SELECT d.n AS sorted,e.n AS original FROM d JOIN e ON 1=1 ORDER BY d.n")
	if len(got.Rows) != 3 || got.Rows[0]["sorted"] != 1 || got.Rows[0]["original"] != 3 {
		t.Fatal("ordered sibling mutated shared materialization", got)
	}
	// Expressions continue to evaluate before sorting, including errors in
	// rows that would be discarded. LIMIT must not hide an unknown column.
	if _, err := Execute(t.Context(), db, "default", mustParse("WITH c AS (SELECT 1 AS n UNION ALL SELECT 2) SELECT CASE WHEN n=2 THEN missing ELSE n END AS n FROM c ORDER BY n LIMIT 1")); err == nil {
		t.Fatal("LIMIT suppressed a projection error")
	}
}
