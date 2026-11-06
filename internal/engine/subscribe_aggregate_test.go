package engine

import (
	"fmt"
	"math"
	"math/big"
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestSubscriptionAggregateAppendAndRebuild(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	exec := func(q string) *ResultSet { t.Helper(); return execSQL(t, db, q) }
	exec(`CREATE TABLE live_facts (g TEXT, value INT)`)
	query := `SELECT g, COUNT(*) AS n, COUNT(value) AS present, SUM(value) AS total, AVG(value) AS mean, MIN(value) AS lo, MAX(value) AS hi FROM live_facts GROUP BY g`
	state := &querySubscriptionState{db: db, tenant: "default", query: mustParse(query).(*Select)}
	var snapshot []Row
	apply := func(change *QueryChange) {
		t.Helper()
		if change == nil {
			return
		}
		if change.Initial {
			snapshot = nil
		}
		for _, removed := range change.Removed {
			found := false
			for i, row := range snapshot {
				if reflect.DeepEqual(row, removed) {
					snapshot = append(snapshot[:i], snapshot[i+1:]...)
					found = true
					break
				}
			}
			if !found {
				t.Fatalf("missing removed row %v in %v", removed, snapshot)
			}
		}
		snapshot = append(snapshot, change.Added...)
	}
	check := func() {
		t.Helper()
		want := exec(query)
		if len(want.Rows) != len(snapshot) {
			t.Fatalf("got %v want %v", snapshot, want.Rows)
		}
		for _, row := range want.Rows {
			found := false
			for _, actual := range snapshot {
				if reflect.DeepEqual(row, actual) {
					found = true
					break
				}
			}
			if !found {
				t.Fatalf("missing %v in %v", row, snapshot)
			}
		}
	}
	change, err := state.refresh(t.Context())
	if err != nil || change == nil || !change.Initial || state.aggregate == nil {
		t.Fatalf("initial: %v %v", change, err)
	}
	apply(change)
	check()
	for _, tc := range []struct {
		sql     string
		scanned int
	}{
		{`INSERT INTO live_facts VALUES ('a', 2), ('a', NULL), ('b', 4)`, 3},
		{`INSERT INTO live_facts VALUES ('a', 8)`, 1},
		{`UPDATE live_facts SET g = 'b', value = 5 WHERE value = 2`, -2},
		{`DELETE FROM live_facts WHERE value = 4`, -2},
		{`INSERT INTO live_facts VALUES ('a', 7)`, 1},
		{`DELETE FROM live_facts`, -2},
		{`INSERT INTO live_facts VALUES ('c', 9)`, 1},
	} {
		exec(tc.sql)
		change, err = state.refresh(t.Context())
		if err != nil || change == nil || (tc.scanned == -2 && change.ScannedRows < 0 || tc.scanned != -2 && change.ScannedRows != tc.scanned) {
			t.Fatalf("%s: %v %v", tc.sql, change, err)
		}
		apply(change)
		check()
		if state.aggregate == nil {
			t.Fatalf("%s: aggregate not rebuilt", tc.sql)
		}
	}
}

func TestSubscriptionAggregateUnsupportedAndBudget(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	execSQL(t, db, `CREATE TABLE metrics_live (g TEXT, v DECIMAL)`)
	execSQL(t, db, `INSERT INTO metrics_live VALUES ('a', 1.25)`)
	table, err := db.Get("default", "metrics_live")
	if err != nil {
		t.Fatal(err)
	}
	table.Rows[0][1] = big.NewRat(5, 4)
	query := `SELECT g, SUM(v) AS total FROM metrics_live GROUP BY g`
	state := &querySubscriptionState{db: db, tenant: "default", query: mustParse(query).(*Select)}
	if _, err := state.refresh(t.Context()); err != nil || state.aggregate != nil {
		t.Fatalf("decimal must use full execution: %v", err)
	}
	execSQL(t, db, `INSERT INTO metrics_live VALUES ('a', 2.5)`)
	if change, err := state.refresh(t.Context()); err != nil || change == nil || change.ScannedRows != -1 {
		t.Fatalf("fallback: %v %v", change, err)
	}
	limited := &querySubscriptionState{db: db, tenant: "default", query: mustParse(`SELECT g, COUNT(*) AS n FROM metrics_live GROUP BY g`).(*Select), options: SubscriptionOptions{MaxResultRows: 1}}
	if _, err := limited.refresh(t.Context()); err != nil || limited.aggregate == nil {
		t.Fatalf("initial limited: %v", err)
	}
	execSQL(t, db, `INSERT INTO metrics_live VALUES ('b', 3.5)`)
	if _, err := limited.refresh(t.Context()); err == nil {
		t.Fatal("expected result limit error")
	}
}

func TestSubscriptionAggregateUngroupedEmpty(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	execSQL(t, db, `CREATE TABLE live_empty (v INT)`)
	state := &querySubscriptionState{db: db, tenant: "default", query: mustParse(`SELECT COUNT(*) AS n, SUM(v) AS s FROM live_empty`).(*Select)}
	change, err := state.refresh(t.Context())
	if err != nil || change == nil || change.Added[0]["n"] != 0 || change.Added[0]["s"] != nil || state.aggregate == nil {
		t.Fatalf("initial: %v %v", change, err)
	}
	execSQL(t, db, `INSERT INTO live_empty VALUES (5)`)
	change, err = state.refresh(t.Context())
	if err != nil || change == nil || change.ScannedRows != 1 || change.Added[0]["s"] != 5 {
		t.Fatalf("append: %v %v", change, err)
	}
}

func TestSubscriptionAggregateNotifications(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	execSQL(t, db, `CREATE TABLE live_events (v INT)`)
	sub, err := SubscribeSQL(t.Context(), db, "default", `SELECT COUNT(*) AS n, SUM(v) AS s FROM live_events`)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(sub.Close)
	receive := func() QueryChange {
		t.Helper()
		select {
		case change, ok := <-sub.Changes:
			if !ok {
				t.Fatalf("closed: %v", sub.Err())
			}
			return change
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for change")
			return QueryChange{}
		}
	}
	initial := receive()
	if !initial.Initial || initial.Added[0]["n"] != 0 {
		t.Fatalf("initial: %v", initial)
	}
	execSQL(t, db, `INSERT INTO live_events VALUES (3), (4)`)
	appended := receive()
	if appended.ScannedRows != 2 || appended.Added[0]["n"] != 2 || appended.Added[0]["s"] != 7 {
		t.Fatalf("append: %v", appended)
	}
	// Returned rows are owned by the consumer, not the retained state.
	appended.Added[0]["s"] = 999
	execSQL(t, db, `UPDATE live_events SET v = 5 WHERE v = 3`)
	updated := receive()
	if updated.ScannedRows < 1 || updated.Removed[0]["s"] != 7 || updated.Added[0]["s"] != 9 {
		t.Fatalf("update: %v", updated)
	}
	execSQL(t, db, `INSERT INTO live_events VALUES (1)`)
	appended = receive()
	if appended.ScannedRows != 1 || appended.Added[0]["s"] != 10 {
		t.Fatalf("append after rebuild: %v", appended)
	}
}

func TestSubscriptionAggregateFloatMatchesExecutor(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	table := storage.NewTable("live_float", []storage.Column{{Name: "g", Type: storage.TextType}, {Name: "v", Type: storage.FloatType}}, false)
	for i := 0; i < 2050; i++ {
		table.Rows = append(table.Rows, []any{[]string{"a", "b"}[i%2], float64(i%19) / 10})
	}
	if err := db.Put("default", table); err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{`SELECT SUM(v) AS total, AVG(v) AS mean FROM live_float`, `SELECT g, SUM(v) AS total, AVG(v) AS mean FROM live_float GROUP BY g`} {
		state := &querySubscriptionState{db: db, tenant: "default", query: mustParse(query).(*Select)}
		if _, err := state.refresh(t.Context()); err != nil || state.aggregate == nil {
			t.Fatalf("initial: %v", err)
		}
		for i := 0; i < 6; i++ {
			table.Rows = append(table.Rows, []any{"a", float64(i) / 7})
			table.Version++
			if _, err := state.refresh(t.Context()); err != nil {
				t.Fatal(err)
			}
			result := execSQL(t, db, query)
			if !subscriptionAggregateSameResult(result, state.snapshot) {
				t.Fatalf("%s: got %v want %v", query, state.snapshot, result.Rows)
			}
		}
	}
}

func TestSubscriptionAggregateUnsupportedShapesUseFullRefresh(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	execSQL(t, db, `CREATE TABLE live_shapes (g INT, v INT)`)
	execSQL(t, db, `INSERT INTO live_shapes VALUES (1, 2), (2, 3)`)
	for i, query := range []string{
		`SELECT g, SUM(v) AS s FROM live_shapes WHERE COALESCE(v, 0) > 0 GROUP BY g`,
		`SELECT g, SUM(v) AS s FROM live_shapes GROUP BY g HAVING COALESCE(SUM(v), 0) > 0`,
		`SELECT g, SUM(v) AS s FROM live_shapes GROUP BY g ORDER BY g`,
		`SELECT COUNT(DISTINCT v) AS n FROM live_shapes`,
		`SELECT SUM(v) + 1 AS s FROM live_shapes`,
	} {
		t.Run(query, func(t *testing.T) {
			state := &querySubscriptionState{db: db, tenant: "default", query: mustParse(query).(*Select)}
			if _, err := state.refresh(t.Context()); err != nil || state.aggregate != nil {
				t.Fatalf("initial: %v, aggregate %v", err, state.aggregate)
			}
			execSQL(t, db, fmt.Sprintf(`INSERT INTO live_shapes VALUES (1, %d)`, 10+i))
			change, err := state.refresh(t.Context())
			if err != nil || change == nil || change.ScannedRows != -1 {
				t.Fatalf("refresh: %v %v", change, err)
			}
		})
	}
}

func TestSubscriptionAggregateWhereMutations(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	execSQL(t, db, `CREATE TABLE live_filtered (id INT, g TEXT, v INT)`)
	execSQL(t, db, `INSERT INTO live_filtered VALUES (1,'a',5),(2,'a',20),(3,'b',30),(4,'b',NULL)`)
	query := `SELECT g, COUNT(*) AS n, SUM(v) AS total, AVG(v) AS mean, MIN(v) AS lo, MAX(v) AS hi FROM live_filtered WHERE v >= 10 GROUP BY g`
	state := &querySubscriptionState{db: db, tenant: "default", query: mustParse(query).(*Select)}
	if _, err := state.refresh(t.Context()); err != nil || state.aggregate == nil {
		t.Fatalf("initial: %v", err)
	}
	steps := [][]string{
		{`UPDATE live_filtered SET v = 15 WHERE id = 1`},
		{`UPDATE live_filtered SET v = 1 WHERE id = 2`},
		{`UPDATE live_filtered SET g = 'a', v = 45 WHERE id = 3`},
		{`DELETE FROM live_filtered WHERE id = 1`},
		{`INSERT INTO live_filtered VALUES (5,'b',11)`},
		// Coalesced delete, update and append clears the update log.
		{`DELETE FROM live_filtered WHERE id = 2`, `UPDATE live_filtered SET v = 17 WHERE id = 5`, `INSERT INTO live_filtered VALUES (6,'c',40)`},
		{`DELETE FROM live_filtered`},
		{`INSERT INTO live_filtered VALUES (7,'z',11)`},
	}
	for _, mutations := range steps {
		for _, q := range mutations {
			execSQL(t, db, q)
		}
		change, err := state.refresh(t.Context())
		if err != nil {
			t.Fatalf("%v: %v", mutations, err)
		}
		if change != nil && change.ScannedRows < 0 {
			t.Fatalf("%v unexpectedly rebuilt: %v", mutations, change)
		}
		if state.aggregate == nil {
			t.Fatalf("%v lost incremental state", mutations)
		}
		result := execSQL(t, db, query)
		if !subscriptionAggregateSameResult(result, state.snapshot) {
			t.Fatalf("%v: got %v want %v", mutations, state.snapshot, result.Rows)
		}
	}
}

func TestSubscriptionAggregateFloatingPointMutations(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	table := storage.NewTable("live_float_changes", []storage.Column{{Name: "g", Type: storage.TextType}, {Name: "v", Type: storage.FloatType}}, false)
	for i := 0; i < 2100; i++ {
		table.Rows = append(table.Rows, []any{[]string{"a", "b"}[i%2], []float64{1e16, 1, -1e16, 3}[i%4]})
	}
	if err := db.Put("default", table); err != nil {
		t.Fatal(err)
	}
	query := `SELECT g, SUM(v) AS s, AVG(v) AS a FROM live_float_changes WHERE v <> 0 GROUP BY g`
	state := &querySubscriptionState{db: db, tenant: "default", query: mustParse(query).(*Select)}
	if _, err := state.refresh(t.Context()); err != nil || state.aggregate == nil {
		t.Fatalf("initial: %v", err)
	}
	for _, q := range []string{
		`UPDATE live_float_changes SET v = 2 WHERE v = 3`,
		`DELETE FROM live_float_changes WHERE v = 1`,
		`INSERT INTO live_float_changes VALUES ('b', 0.1)`,
	} {
		execSQL(t, db, q)
		if _, err := state.refresh(t.Context()); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		want := execSQL(t, db, query)
		if !subscriptionAggregateSameResult(want, state.snapshot) {
			t.Fatalf("%s: got %v want %v", q, state.snapshot, want.Rows)
		}
	}
}

func TestSubscriptionAggregateReorderFallsBack(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	execSQL(t, db, `CREATE TABLE live_reorder (g INT, v INT)`)
	execSQL(t, db, `INSERT INTO live_reorder VALUES (1,1),(1,2),(2,3)`)
	query := `SELECT g, SUM(v) AS s FROM live_reorder GROUP BY g`
	state := &querySubscriptionState{db: db, tenant: "default", query: mustParse(query).(*Select)}
	if _, err := state.refresh(t.Context()); err != nil || state.aggregate == nil {
		t.Fatalf("initial: %v", err)
	}
	table, err := db.Get("default", "live_reorder")
	if err != nil {
		t.Fatal(err)
	}
	table.Rows[0], table.Rows[2] = table.Rows[2], table.Rows[0]
	table.MarkDirtyFrom(-1)
	table.Version++
	if _, err := state.refresh(t.Context()); err != nil {
		t.Fatal(err)
	}
	if state.fullRefreshes.Load() != 2 {
		t.Fatalf("expected full fallback, got %d full refreshes", state.fullRefreshes.Load())
	}
	if want := execSQL(t, db, query); !subscriptionAggregateSameResult(want, state.snapshot) {
		t.Fatalf("got %v want %v", state.snapshot, want.Rows)
	}
}

func TestSubscriptionAggregateRandomMutations(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	execSQL(t, db, `CREATE TABLE live_random (id INT, g INT, v INT)`)
	for i := 0; i < 25; i++ {
		execSQL(t, db, fmt.Sprintf(`INSERT INTO live_random VALUES (%d,%d,%d)`, i, i%4, i%11))
	}
	query := `SELECT g, COUNT(*) AS n, COUNT(v) AS present, SUM(v) AS s, AVG(v) AS a, MIN(v) AS lo, MAX(v) AS hi FROM live_random WHERE v >= 3 GROUP BY g`
	state := &querySubscriptionState{db: db, tenant: "default", query: mustParse(query).(*Select)}
	if _, err := state.refresh(t.Context()); err != nil || state.aggregate == nil {
		t.Fatalf("initial: %v", err)
	}
	rng := rand.New(rand.NewSource(7321))
	for step := 0; step < 180; step++ {
		id := rng.Intn(70)
		switch rng.Intn(4) {
		case 0:
			execSQL(t, db, fmt.Sprintf(`INSERT INTO live_random VALUES (%d,%d,%d)`, id, rng.Intn(5), rng.Intn(15)))
		case 1:
			execSQL(t, db, fmt.Sprintf(`UPDATE live_random SET v = %d WHERE id = %d`, rng.Intn(15), id))
		case 2:
			execSQL(t, db, fmt.Sprintf(`UPDATE live_random SET g = %d WHERE id = %d`, rng.Intn(5), id))
		case 3:
			execSQL(t, db, fmt.Sprintf(`DELETE FROM live_random WHERE id = %d`, id))
		}
		if _, err := state.refresh(t.Context()); err != nil {
			t.Fatalf("step %d: %v", step, err)
		}
		if state.aggregate == nil {
			t.Fatalf("step %d: lost incremental state", step)
		}
		want := execSQL(t, db, query)
		if !subscriptionAggregateSameResult(want, state.snapshot) {
			t.Fatalf("step %d: got %v want %v", step, state.snapshot, want.Rows)
		}
	}
}

func TestSubscriptionAggregateWhereNotifications(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	execSQL(t, db, `CREATE TABLE live_filtered_events (v INT)`)
	sub, err := SubscribeSQL(t.Context(), db, "default", `SELECT COUNT(*) AS n, SUM(v) AS s FROM live_filtered_events WHERE v >= 10`)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(sub.Close)
	receive := func() QueryChange {
		t.Helper()
		select {
		case c, ok := <-sub.Changes:
			if !ok {
				t.Fatalf("closed: %v", sub.Err())
			}
			return c
		case <-time.After(5 * time.Second):
			t.Fatal("timed out")
			return QueryChange{}
		}
	}
	if c := receive(); !c.Initial || c.Added[0]["n"] != 0 {
		t.Fatalf("initial: %v", c)
	}
	execSQL(t, db, `INSERT INTO live_filtered_events VALUES (5), (10), (20)`)
	if c := receive(); c.ScannedRows != 3 || c.Added[0]["s"] != 30 {
		t.Fatalf("insert: %v", c)
	}
	execSQL(t, db, `UPDATE live_filtered_events SET v = 15 WHERE v = 5`)
	if c := receive(); c.ScannedRows < 0 || c.Added[0]["s"] != 45 {
		t.Fatalf("update: %v", c)
	}
	execSQL(t, db, `DELETE FROM live_filtered_events WHERE v = 10`)
	if c := receive(); c.ScannedRows < 0 || c.Added[0]["s"] != 35 {
		t.Fatalf("delete: %v", c)
	}
}

func TestSubscriptionAggregateDuplicateOutputs(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	execSQL(t, db, `CREATE TABLE live_duplicate_groups (g INT, v INT)`)
	execSQL(t, db, `INSERT INTO live_duplicate_groups VALUES (1,1),(2,1),(3,1)`)
	query := `SELECT COUNT(*) AS n FROM live_duplicate_groups GROUP BY g`
	state := &querySubscriptionState{db: db, tenant: "default", query: mustParse(query).(*Select)}
	if _, err := state.refresh(t.Context()); err != nil || state.aggregate == nil {
		t.Fatalf("initial: %v", err)
	}
	execSQL(t, db, `DELETE FROM live_duplicate_groups WHERE g = 2`)
	change, err := state.refresh(t.Context())
	if err != nil || change == nil || len(change.Removed) != 1 || len(change.Added) != 0 || change.Removed[0]["n"] != 1 {
		t.Fatalf("delete: %v %v", change, err)
	}
	execSQL(t, db, `UPDATE live_duplicate_groups SET g = 1 WHERE g = 3`)
	change, err = state.refresh(t.Context())
	if err != nil || change == nil || len(change.Removed) != 2 || len(change.Added) != 1 || change.Added[0]["n"] != 2 {
		t.Fatalf("merge: %v %v", change, err)
	}
}

func TestSubscriptionAggregateExpiredHistory(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	execSQL(t, db, `CREATE TABLE live_old_history (g INT, v INT)`)
	execSQL(t, db, `INSERT INTO live_old_history VALUES (1,2),(1,3),(2,4)`)
	query := `SELECT g, SUM(v) AS s FROM live_old_history GROUP BY g`
	state := &querySubscriptionState{db: db, tenant: "default", query: mustParse(query).(*Select)}
	if _, err := state.refresh(t.Context()); err != nil || state.aggregate == nil {
		t.Fatalf("initial: %v", err)
	}
	table, err := db.Get("default", "live_old_history")
	if err != nil {
		t.Fatal(err)
	}
	table.Rows[0] = []any{1, 12}
	for i := 0; i < 4100; i++ {
		table.MarkRowUpdated(0)
	}
	table.Version++
	change, err := state.refresh(t.Context())
	if err != nil || change == nil || change.ScannedRows < 0 || state.aggregate == nil {
		t.Fatalf("refresh: %v %v", change, err)
	}
	if want := execSQL(t, db, query); !subscriptionAggregateSameResult(want, state.snapshot) {
		t.Fatalf("got %v want %v", state.snapshot, want.Rows)
	}
}

func TestSubscriptionAggregateSchemaChangeFallsBack(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	execSQL(t, db, `CREATE TABLE live_schema (g INT, v INT)`)
	execSQL(t, db, `INSERT INTO live_schema VALUES (1,2)`)
	query := `SELECT g, SUM(v) AS s FROM live_schema GROUP BY g`
	state := &querySubscriptionState{db: db, tenant: "default", query: mustParse(query).(*Select)}
	if _, err := state.refresh(t.Context()); err != nil || state.aggregate == nil {
		t.Fatalf("initial: %v", err)
	}
	execSQL(t, db, `ALTER TABLE live_schema ADD COLUMN extra INT`)
	if _, err := state.refresh(t.Context()); err != nil {
		t.Fatal(err)
	}
	if state.fullRefreshes.Load() != 2 {
		t.Fatalf("expected full refresh, got %d", state.fullRefreshes.Load())
	}
	if want := execSQL(t, db, query); !subscriptionAggregateSameResult(want, state.snapshot) {
		t.Fatalf("got %v want %v", state.snapshot, want.Rows)
	}
}

func TestSubscriptionAggregateIndexedSum(t *testing.T) {
	db := filteredAggregateFixture(t, 600, true)
	query := `SELECT category, SUM(amount) AS total FROM facts WHERE region='r1' AND time>=100 AND time<150 GROUP BY category`
	state := &querySubscriptionState{db: db, tenant: "default", query: mustParse(query).(*Select)}
	if _, err := state.refresh(t.Context()); err != nil || state.aggregate == nil {
		t.Fatalf("expected incremental plan: %v", err)
	}
	execSQL(t, db, `INSERT INTO facts VALUES (601,'r1','c0',120,99,48,11)`)
	change, err := state.refresh(t.Context())
	if err != nil || change == nil || change.ScannedRows < 0 {
		t.Fatalf("refresh: %v %v", change, err)
	}
	if want := execSQL(t, db, query); !subscriptionAggregateSameResult(want, state.snapshot) {
		t.Fatalf("got %v want %v", state.snapshot, want.Rows)
	}
}

func TestSubscriptionAggregateWhereNewIndex(t *testing.T) {
	db := filteredAggregateFixture(t, 600, false)
	query := `SELECT category, SUM(amount) AS total FROM facts WHERE region='r1' AND time>=100 AND time<150 GROUP BY category`
	state := &querySubscriptionState{db: db, tenant: "default", query: mustParse(query).(*Select)}
	if _, err := state.refresh(t.Context()); err != nil || state.aggregate == nil {
		t.Fatalf("initial: %v", err)
	}
	execSQL(t, db, `CREATE INDEX region_window ON facts(region,time)`)
	execSQL(t, db, `INSERT INTO facts VALUES (601,'r1','c0',120,99,48,11)`)
	change, err := state.refresh(t.Context())
	if err != nil || change == nil || change.ScannedRows < 0 || state.aggregate == nil {
		t.Fatalf("expected incremental plan: %v %v", change, err)
	}
	if want := execSQL(t, db, query); !subscriptionAggregateSameResult(want, state.snapshot) {
		t.Fatalf("got %v want %v", state.snapshot, want.Rows)
	}
}

func TestSubscriptionAggregateHavingTransitions(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	execSQL(t, db, `CREATE TABLE having_live (g INT, v INT)`)
	execSQL(t, db, `INSERT INTO having_live VALUES (1, 4), (2, 12)`)
	query := `SELECT g, SUM(v) AS total, COUNT(*) AS n FROM having_live GROUP BY g HAVING SUM(v) > 10`
	state := &querySubscriptionState{db: db, tenant: "default", query: mustParse(query).(*Select)}
	if _, err := state.refresh(t.Context()); err != nil || state.aggregate == nil {
		t.Fatalf("initial: %v", err)
	}
	for _, q := range []string{`INSERT INTO having_live VALUES (1, 8)`, `UPDATE having_live SET v = 1 WHERE v = 12`, `DELETE FROM having_live WHERE v = 8`, `INSERT INTO having_live VALUES (3, 20)`} {
		execSQL(t, db, q)
		change, err := state.refresh(t.Context())
		if err != nil || change == nil || change.ScannedRows < 0 {
			t.Fatalf("%s: %v %v", q, change, err)
		}
		if want := execSQL(t, db, query); !subscriptionAggregateSameResult(want, state.snapshot) {
			t.Fatalf("%s: got %v want %v", q, state.snapshot, want.Rows)
		}
	}
}

func TestSubscriptionAggregateIndexedCount(t *testing.T) {
	db := filteredAggregateFixture(t, 600, true)
	query := `SELECT category, COUNT(*) AS n, COUNT(amount) AS present FROM facts WHERE region='r1' AND time>=100 AND time<150 GROUP BY category HAVING COUNT(*) > 0`
	state := &querySubscriptionState{db: db, tenant: "default", query: mustParse(query).(*Select)}
	if _, err := state.refresh(t.Context()); err != nil || state.aggregate == nil {
		t.Fatalf("initial: %v", err)
	}
	for _, q := range []string{`INSERT INTO facts VALUES (601,'r1','c0',120,99,48,11)`, `UPDATE facts SET region='r1', time=120 WHERE id=102`, `DELETE FROM facts WHERE id=111`} {
		execSQL(t, db, q)
		_, err := state.refresh(t.Context())
		if err != nil || state.aggregate == nil {
			t.Fatalf("%s: %v", q, err)
		}
		if want := execSQL(t, db, query); !subscriptionAggregateSameResult(want, state.snapshot) {
			t.Fatalf("%s: got %v want %v", q, state.snapshot, want.Rows)
		}
	}
}

func TestSubscriptionAggregateHavingError(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	execSQL(t, db, `CREATE TABLE having_error (g INT, v INT)`)
	execSQL(t, db, `INSERT INTO having_error VALUES (1, 2)`)
	query := `SELECT g, SUM(v) AS total FROM having_error GROUP BY g HAVING 1 / SUM(v) > 0`
	state := &querySubscriptionState{db: db, tenant: "default", query: mustParse(query).(*Select)}
	if _, err := state.refresh(t.Context()); err != nil || state.aggregate == nil {
		t.Fatalf("initial: %v", err)
	}
	execSQL(t, db, `UPDATE having_error SET v = 0`)
	_, got := state.refresh(t.Context())
	_, want := Execute(t.Context(), db, "default", mustParse(query))
	if got == nil || want == nil || got.Error() != want.Error() {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestSubscriptionIndexedSumAvgExactOrder(t *testing.T) {
	db := filteredAggregateFixture(t, 3000, true)
	table, err := db.Get("default", "facts")
	if err != nil {
		t.Fatal(err)
	}
	// Alternating magnitudes make a changed addition order observable.
	for i, row := range table.Rows {
		if i%17 != 0 {
			row[4] = []float64{1e16, 1, -1e16, 0.1}[i%4]
		}
	}
	for _, query := range []string{
		`SELECT SUM(amount) AS total, AVG(amount) AS mean FROM facts WHERE region='r1' AND time>=100 AND time<2000`,
		`SELECT category, SUM(amount) AS total, AVG(amount) AS mean, COUNT(amount) AS n FROM facts WHERE region='r1' AND time>=100 AND time<2000 GROUP BY category`,
	} {
		state := &querySubscriptionState{db: db, tenant: "default", query: mustParse(query).(*Select)}
		if _, err := state.refresh(t.Context()); err != nil || state.aggregate == nil {
			t.Fatalf("initial %s: %v", query, err)
		}
		for _, q := range []string{
			`INSERT INTO facts VALUES (4001,'r1','c1',150,0.25,48,11)`,
			`UPDATE facts SET amount=0.3, category='c2' WHERE id=111`,
			`UPDATE facts SET region='r1', time=151 WHERE id=102`,
			`DELETE FROM facts WHERE id=121`,
		} {
			execSQL(t, db, q)
			if _, err := state.refresh(t.Context()); err != nil || state.aggregate == nil {
				t.Fatalf("%s: %v", q, err)
			}
			want := execSQL(t, db, query)
			if !subscriptionAggregateSameResult(want, state.snapshot) {
				t.Fatalf("%s after %s: got %v want %v", query, q, state.snapshot, want.Rows)
			}
		}
		execSQL(t, db, `DELETE FROM facts WHERE id=4001`)
	}
}

func TestSubscriptionIndexedSumFullyCoveredFallsBack(t *testing.T) {
	db := filteredAggregateFixture(t, 600, false)
	query := `SELECT SUM(amount) AS total, AVG(amount) AS mean FROM facts WHERE id=123`
	env := ExecEnv{ctx: t.Context(), db: db, tenant: "default"}
	plan, ok, err := buildSimpleAggregatePlan(env, mustParse(query).(*Select))
	if err != nil || !ok {
		t.Fatalf("plan: %v %v", ok, err)
	}
	source, err := buildSimpleAggregateSourcePlan(plan)
	if err != nil || !source.filterFullyCovered {
		t.Fatalf("expected covered seek: %v", err)
	}
	state := &querySubscriptionState{db: db, tenant: "default", query: mustParse(query).(*Select)}
	if _, err := state.refresh(t.Context()); err != nil || state.aggregate != nil {
		t.Fatalf("expected fallback: %v", err)
	}
	execSQL(t, db, `UPDATE facts SET amount=0.1 WHERE id=123`)
	change, err := state.refresh(t.Context())
	if err != nil || change == nil || change.ScannedRows != -1 {
		t.Fatalf("refresh: %v %v", change, err)
	}
}

func TestSubscriptionIndexedSumIntegerOverflow(t *testing.T) {
	db := storage.NewDB()
	t.Cleanup(func() { _ = db.Close() })
	table := storage.NewTable("indexed_overflow", []storage.Column{{Name: "k", Type: storage.IntType}, {Name: "v", Type: storage.IntType}}, false)
	table.Rows = [][]any{{1, math.MaxInt}, {1, 1}, {1, -1}, {2, 7}}
	if err := table.CreateSecondaryIndex("overflow_k", []string{"k"}, false); err != nil {
		t.Fatal(err)
	}
	if err := db.Put("default", table); err != nil {
		t.Fatal(err)
	}
	query := `SELECT SUM(v) AS total, AVG(v) AS mean FROM indexed_overflow WHERE k=1`
	state := &querySubscriptionState{db: db, tenant: "default", query: mustParse(query).(*Select)}
	if _, err := state.refresh(t.Context()); err != nil || state.aggregate == nil {
		t.Fatalf("initial: %v", err)
	}
	for _, q := range []string{`INSERT INTO indexed_overflow VALUES (1,2)`, `UPDATE indexed_overflow SET v=2 WHERE v=-1`, `DELETE FROM indexed_overflow WHERE v=1`} {
		execSQL(t, db, q)
		if _, err := state.refresh(t.Context()); err != nil || state.aggregate == nil {
			t.Fatalf("%s: %v", q, err)
		}
		if want := execSQL(t, db, query); !subscriptionAggregateSameResult(want, state.snapshot) {
			t.Fatalf("%s: got %v want %v", q, state.snapshot, want.Rows)
		}
	}
}
