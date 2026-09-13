package engine

import (
	"context"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func advisorFixture(tb testing.TB, n int) *storage.DB {
	tb.Helper()
	db := storage.NewDB()
	tb.Cleanup(func() { _ = db.Close() })
	table := storage.NewTable("events", []storage.Column{{Name: "lookup", Type: storage.IntType}, {Name: "bucket", Type: storage.IntType}}, false)
	for i := 0; i < n; i++ {
		table.Rows = append(table.Rows, []any{i, i % 2})
	}
	if err := db.Put("default", table); err != nil {
		tb.Fatal(err)
	}
	return db
}
func advisorForTest(t *testing.T, db *storage.DB, auto bool) *IndexAdvisor {
	t.Helper()
	a, err := NewIndexAdvisor(db, IndexAdvisorOptions{AutoCreate: auto, MinExecutions: 2, MinTableRows: 32})
	if err != nil {
		t.Fatal(err)
	}
	return a
}
func TestIndexAdvisorOptInApplyAndResultEquivalence(t *testing.T) {
	db := advisorFixture(t, 2048)
	a := advisorForTest(t, db, false)
	stmt := mustParse("SELECT lookup FROM events e WHERE e.lookup=17")
	want, err := Execute(t.Context(), db, "default", stmt)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if _, err := a.Execute(t.Context(), "default", stmt); err != nil {
			t.Fatal(err)
		}
	}
	rs := a.Recommendations()
	if len(rs) != 1 || rs[0].Status != "ready" || rs[0].Executions != 2 {
		t.Fatal(rs)
	}
	table, _ := db.Get("default", "events")
	if len(table.Indexes) != 0 {
		t.Fatal("advice created an index")
	}
	rs[0].Column = "tampered"
	r := a.Recommendations()[0]
	if err := a.Apply(t.Context(), "default", r.Name); err != nil {
		t.Fatal(err)
	}
	if err := a.Apply(t.Context(), "default", r.Name); err != nil {
		t.Fatal("not idempotent", err)
	}
	got, err := a.Execute(t.Context(), "default", stmt)
	if err != nil || !reflect.DeepEqual(got.Rows, want.Rows) {
		t.Fatal(got, err)
	}
	if len(table.Indexes) != 1 || table.Indexes[r.Name] == nil {
		t.Fatal("missing index")
	}
}
func TestIndexAdvisorAutoToggleAndExistingCoverage(t *testing.T) {
	db := advisorFixture(t, 2048)
	a := advisorForTest(t, db, false)
	stmt := mustParse("SELECT lookup FROM events WHERE 17=lookup")
	for i := 0; i < 2; i++ {
		if _, err := a.Execute(t.Context(), "default", stmt); err != nil {
			t.Fatal(err)
		}
	}
	a.SetAutoCreate(true)
	if _, err := a.Execute(t.Context(), "default", stmt); err != nil {
		t.Fatal(err)
	}
	if a.Recommendations()[0].Status != "created" {
		t.Fatal(a.Recommendations())
	}
	second := advisorForTest(t, db, true)
	for i := 0; i < 2; i++ {
		if _, err := second.Execute(t.Context(), "default", stmt); err != nil {
			t.Fatal(err)
		}
	}
	if second.Recommendations()[0].Status != "covered" {
		t.Fatal(second.Recommendations())
	}
}
func TestIndexAdvisorLimitsAndStaleRecommendation(t *testing.T) {
	db := advisorFixture(t, 2048)
	a, err := NewIndexAdvisor(db, IndexAdvisorOptions{MinExecutions: 1, MaxCandidates: 1, MinTableRows: 32, MaxTableRows: 3000})
	if err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{"SELECT lookup FROM events WHERE lookup=1", "SELECT lookup FROM events WHERE bucket=1"} {
		if _, err := a.Execute(t.Context(), "default", mustParse(q)); err != nil {
			t.Fatal(err)
		}
	}
	if len(a.Recommendations()) != 1 {
		t.Fatal("unbounded candidates")
	}
	r := a.Recommendations()[0]
	table, _ := db.Get("default", "events")
	db.LockContentForWrite()
	table.Rows = append(table.Rows, make([][]any, 1000)...)
	db.UnlockContentForWrite()
	if err := a.Apply(t.Context(), "default", r.Name); err == nil {
		t.Fatal("stale row limit ignored")
	}
	if len(table.Indexes) != 0 {
		t.Fatal("built oversized index")
	}
	b := advisorForTest(t, advisorFixture(t, 2048), true)
	for i := 0; i < 2; i++ {
		if _, err := b.Execute(t.Context(), "default", mustParse("SELECT bucket FROM events WHERE bucket=1")); err != nil {
			t.Fatal(err)
		}
	}
	if b.Recommendations()[0].Status != "skipped" {
		t.Fatal("low cardinality accepted")
	}
}
func TestIndexAdvisorPermissionFailureDoesNotFailRead(t *testing.T) {
	db := advisorFixture(t, 2048)
	cat := db.Catalog()
	if err := cat.CreateRole("reader"); err != nil {
		t.Fatal(err)
	}
	if err := cat.GrantPermission("reader", storage.PermSelect, "*", "*"); err != nil {
		t.Fatal(err)
	}
	if err := cat.CreateUser("reader", "password", []string{"reader"}); err != nil {
		t.Fatal(err)
	}
	a := advisorForTest(t, db, true)
	ctx := WithUser(t.Context(), "reader")
	for i := 0; i < 3; i++ {
		rs, err := a.Execute(ctx, "default", mustParse("SELECT lookup FROM events WHERE lookup=1"))
		if err != nil || len(rs.Rows) != 1 {
			t.Fatal(rs, err)
		}
	}
	r := a.Recommendations()[0]
	if r.Status != "failed" || r.LastError == "" {
		t.Fatal(r)
	}
	table, _ := db.Get("default", "events")
	if len(table.Indexes) != 0 {
		t.Fatal("DDL permission bypassed")
	}
	if err := a.Apply(ctx, "default", r.Name); err == nil {
		t.Fatal("explicit apply hid error")
	}
}
func TestIndexAdvisorConcurrentQueriesAndTenantIsolation(t *testing.T) {
	db := advisorFixture(t, 2048)
	table, _ := db.Get("default", "events")
	other := storage.NewTable("events", table.Cols, false)
	other.Rows = append(other.Rows, table.Rows...)
	if err := db.Put("other", other); err != nil {
		t.Fatal(err)
	}
	a := advisorForTest(t, db, true)
	stmt := mustParse("SELECT lookup FROM events WHERE lookup=1")
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := a.Execute(t.Context(), "default", stmt); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if len(table.Indexes) != 1 || len(other.Indexes) != 0 {
		t.Fatal("duplicate or cross-tenant index")
	}
	if err := a.Apply(t.Context(), "other", a.Recommendations()[0].Name); err == nil {
		t.Fatal("cross-tenant recommendation applied")
	}
}
func TestIndexAdvisorIgnoresUnsupportedQueries(t *testing.T) {
	db := advisorFixture(t, 2048)
	a := advisorForTest(t, db, true)
	for _, q := range []string{"SELECT lookup FROM events WHERE lookup>1", "SELECT lookup FROM events WHERE lookup=1 OR bucket=1", "WITH x AS (SELECT * FROM events) SELECT lookup FROM x WHERE lookup=1"} {
		if _, err := a.Execute(t.Context(), "default", mustParse(q)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := a.Execute(t.Context(), "default", mustParse("SELECT missing FROM events WHERE lookup=1")); err == nil {
		t.Fatal("expected bad query")
	}
	if len(a.Recommendations()) != 0 {
		t.Fatal(a.Recommendations())
	}
}
func BenchmarkIndexAdvisor(b *testing.B) {
	for _, mode := range []string{"plain", "observe", "indexed"} {
		b.Run(mode, func(b *testing.B) {
			db := advisorFixture(b, 20000)
			a, err := NewIndexAdvisor(db, IndexAdvisorOptions{MinExecutions: 1})
			if err != nil {
				b.Fatal(err)
			}
			stmt := mustParse("SELECT lookup FROM events WHERE lookup=12345")
			if _, err := a.Execute(b.Context(), "default", stmt); err != nil {
				b.Fatal(err)
			}
			if mode == "indexed" {
				if err := a.Apply(b.Context(), "default", a.Recommendations()[0].Name); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportAllocs()
			for b.Loop() {
				var rs *ResultSet
				var err error
				if mode == "plain" {
					rs, err = Execute(b.Context(), db, "default", stmt)
				} else {
					rs, err = a.Execute(b.Context(), "default", stmt)
				}
				if err != nil || len(rs.Rows) != 1 || rs.Rows[0]["lookup"] != 12345 {
					b.Fatal(fmt.Sprint(rs), err)
				}
			}
		})
	}
}

func TestIndexAdvisorWALPersistence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "advisor")
	db := walDDLDB(t, path)
	t.Cleanup(func() { _ = db.Close() })
	execWAL(t, db, "default", "CREATE TABLE events (lookup INT, bucket INT)")
	values := make([]string, 64)
	for i := range values {
		values[i] = fmt.Sprintf("(%d,0)", i)
	}
	execWAL(t, db, "default", "INSERT INTO events VALUES "+strings.Join(values, ","))
	a := advisorForTest(t, db, true)
	for i := 0; i < 2; i++ {
		if _, err := a.Execute(t.Context(), "default", mustParse("SELECT lookup FROM events WHERE lookup=17")); err != nil {
			t.Fatal(err)
		}
	}
	name := a.Recommendations()[0].Name
	if a.Recommendations()[0].Status != "created" {
		t.Fatal(a.Recommendations())
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := walDDLDB(t, path)
	defer reopened.Close()
	table, err := reopened.Get("default", "events")
	if err != nil {
		t.Fatal(err)
	}
	if table.Indexes[name] == nil {
		t.Fatal("auto index lost on reopen")
	}
	if n := countWAL(t, reopened, "default", "SELECT lookup FROM events WHERE lookup=17"); n != 1 {
		t.Fatal(n)
	}
}

func TestIndexAdvisorCancelledApply(t *testing.T) {
	db := advisorFixture(t, 2048)
	a := advisorForTest(t, db, false)
	for i := 0; i < 2; i++ {
		if _, err := a.Execute(t.Context(), "default", mustParse("SELECT lookup FROM events WHERE lookup=17")); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := a.Apply(ctx, "default", a.Recommendations()[0].Name); err == nil {
		t.Fatal("cancelled build succeeded")
	}
	table, _ := db.Get("default", "events")
	if len(table.Indexes) != 0 {
		t.Fatal("cancelled build changed table")
	}
}

// Include the live DDL path and its eligibility sample; fixture construction,
// query observation and dropping the previous index stay outside build timing.
func BenchmarkIndexAdvisorBuild(b *testing.B) {
	db := advisorFixture(b, 20000)
	stmt := mustParse("SELECT lookup FROM events WHERE lookup=12345")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		a, err := NewIndexAdvisor(db, IndexAdvisorOptions{MinExecutions: 1})
		if err != nil {
			b.Fatal(err)
		}
		if _, err := a.Execute(b.Context(), "default", stmt); err != nil {
			b.Fatal(err)
		}
		r := a.Recommendations()[0]
		b.StartTimer()
		if err := a.Apply(b.Context(), "default", r.Name); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		if _, err := Execute(b.Context(), db, "default", &DropIndex{Name: r.Name, Table: r.Table}); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
	}
}

func TestIndexAdvisorIndexLimitAndConcurrentCoverage(t *testing.T) {
	db := advisorFixture(t, 2048)
	table, _ := db.Get("default", "events")
	if _, err := Execute(t.Context(), db, "default", &CreateIndex{Name: "bucket_idx", Table: "events", Columns: []string{"bucket"}}); err != nil {
		t.Fatal(err)
	}
	a, err := NewIndexAdvisor(db, IndexAdvisorOptions{AutoCreate: true, MinExecutions: 1, MaxIndexesPerTable: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Execute(t.Context(), "default", mustParse("SELECT lookup FROM events WHERE lookup=17")); err != nil {
		t.Fatal(err)
	}
	if len(table.Indexes) != 1 || a.Recommendations()[0].Status != "skipped" {
		t.Fatal(a.Recommendations())
	}
	b, err := NewIndexAdvisor(db, IndexAdvisorOptions{MinExecutions: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.Execute(t.Context(), "default", mustParse("SELECT lookup FROM events WHERE lookup=17")); err != nil {
		t.Fatal(err)
	}
	r := b.Recommendations()[0]
	if r.Status != "ready" {
		t.Fatal(r)
	}
	if _, err := Execute(t.Context(), db, "default", &CreateIndex{Name: "manual_idx", Table: "events", Columns: []string{"lookup", "bucket"}}); err != nil {
		t.Fatal(err)
	}
	if err := b.Apply(t.Context(), "default", r.Name); err != nil {
		t.Fatal(err)
	}
	if len(table.Indexes) != 2 || b.Recommendations()[0].Status != "covered" {
		t.Fatal("redundant index built")
	}
}
