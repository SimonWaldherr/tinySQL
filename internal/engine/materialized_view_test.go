package engine

import (
	"context"
	"testing"
	"time"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func TestCreateViewSupportsCTE(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()

	mvExecSQL(t, ctx, db, "CREATE TABLE orders (customer_id INT, amount INT, status TEXT)")
	mvExecSQL(t, ctx, db, "INSERT INTO orders VALUES (1, 10, 'paid'), (1, 5, 'open'), (2, 7, 'paid')")
	mvExecSQL(t, ctx, db, `
		CREATE VIEW paid_totals AS
		WITH paid AS (
			SELECT customer_id, amount FROM orders WHERE status = 'paid'
		)
		SELECT customer_id, SUM(amount) AS total
		FROM paid
		GROUP BY customer_id
	`)

	rs := mvQuerySQL(t, ctx, db, "SELECT customer_id, total FROM paid_totals ORDER BY customer_id")
	if len(rs.Rows) != 2 {
		t.Fatalf("rows = %d, want 2: %#v", len(rs.Rows), rs.Rows)
	}
	if rs.Rows[0]["customer_id"] != 1 || rs.Rows[0]["total"] != float64(10) {
		t.Fatalf("first row = %#v", rs.Rows[0])
	}
	if rs.Rows[1]["customer_id"] != 2 || rs.Rows[1]["total"] != float64(7) {
		t.Fatalf("second row = %#v", rs.Rows[1])
	}
}

func TestMaterializedViewWithCTEAndManualRefresh(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()

	mvExecSQL(t, ctx, db, "CREATE TABLE events (kind TEXT, amount INT)")
	mvExecSQL(t, ctx, db, "INSERT INTO events VALUES ('sale', 10), ('ignore', 99)")
	mvExecSQL(t, ctx, db, `
		CREATE MATERIALIZED VIEW sale_totals AS
		WITH sales AS (
			SELECT amount FROM events WHERE kind = 'sale'
		)
		SELECT SUM(amount) AS total FROM sales
		WITH DATA
	`)

	rs := mvQuerySQL(t, ctx, db, "SELECT total FROM sale_totals")
	if len(rs.Rows) != 1 || rs.Rows[0]["total"] != float64(10) {
		t.Fatalf("initial materialized rows = %#v", rs.Rows)
	}

	mvExecSQL(t, ctx, db, "INSERT INTO events VALUES ('sale', 5)")
	rs = mvQuerySQL(t, ctx, db, "SELECT total FROM sale_totals")
	if len(rs.Rows) != 1 || rs.Rows[0]["total"] != float64(10) {
		t.Fatalf("cache should remain unchanged before manual refresh: %#v", rs.Rows)
	}

	mvExecSQL(t, ctx, db, "REFRESH MATERIALIZED VIEW sale_totals")
	rs = mvQuerySQL(t, ctx, db, "SELECT total FROM sale_totals")
	if len(rs.Rows) != 1 || rs.Rows[0]["total"] != float64(15) {
		t.Fatalf("refreshed materialized rows = %#v", rs.Rows)
	}
}

func TestMaterializedViewLazyStaleRefresh(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()

	mvExecSQL(t, ctx, db, "CREATE TABLE metrics (id INT)")
	mvExecSQL(t, ctx, db, "INSERT INTO metrics VALUES (1)")
	mvExecSQL(t, ctx, db, `
		CREATE MATERIALIZED VIEW metric_count AS
		SELECT COUNT(*) AS cnt FROM metrics
		REFRESH ON STALE AFTER 1 MINUTES
		WITH NO DATA
	`)

	rs := mvQuerySQL(t, ctx, db, "SELECT cnt FROM metric_count")
	if len(rs.Rows) != 1 || rs.Rows[0]["cnt"] != 1 {
		t.Fatalf("lazy initial refresh rows = %#v", rs.Rows)
	}

	mvExecSQL(t, ctx, db, "INSERT INTO metrics VALUES (2)")
	rs = mvQuerySQL(t, ctx, db, "SELECT cnt FROM metric_count")
	if len(rs.Rows) != 1 || rs.Rows[0]["cnt"] != 1 {
		t.Fatalf("non-stale cache should still have count 1: %#v", rs.Rows)
	}

	oldRefresh := time.Now().Add(-2 * time.Minute)
	if err := db.Catalog().FinishMaterializedViewRefresh("main", "metric_count", oldRefresh, 0, 1, ""); err != nil {
		t.Fatalf("forcing old refresh time failed: %v", err)
	}
	rs = mvQuerySQL(t, ctx, db, "SELECT cnt FROM metric_count")
	if len(rs.Rows) != 1 || rs.Rows[0]["cnt"] != 2 {
		t.Fatalf("stale cache should refresh to count 2: %#v", rs.Rows)
	}
}

func TestMaterializedViewRefreshPoliciesRegisterJobs(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()

	mvExecSQL(t, ctx, db, "CREATE TABLE metrics (id INT)")
	mvExecSQL(t, ctx, db, `
		CREATE MATERIALIZED VIEW metric_count_sched AS
		SELECT COUNT(*) AS cnt FROM metrics
		REFRESH EVERY 30 MINUTES
		REFRESH DAILY AT '02:15' TIMEZONE 'Europe/Berlin'
		WITH NO DATA
	`)

	intervalJob, err := db.Catalog().GetJob("__mv_refresh_metric_count_sched_interval")
	if err != nil {
		t.Fatalf("interval job missing: %v", err)
	}
	if intervalJob.ScheduleType != "INTERVAL" || intervalJob.IntervalMs != int64(30*time.Minute/time.Millisecond) {
		t.Fatalf("unexpected interval job: %#v", intervalJob)
	}
	dailyJob, err := db.Catalog().GetJob("__mv_refresh_metric_count_sched_daily")
	if err != nil {
		t.Fatalf("daily job missing: %v", err)
	}
	if dailyJob.ScheduleType != "CRON" || dailyJob.CronExpr != "0 15 2 * * *" || dailyJob.Timezone != "Europe/Berlin" {
		t.Fatalf("unexpected daily job: %#v", dailyJob)
	}

	rs := mvQuerySQL(t, ctx, db, "SELECT name, refresh_every_ms, daily_at FROM catalog.materialized_views WHERE name = 'metric_count_sched'")
	if len(rs.Rows) != 1 {
		t.Fatalf("catalog.materialized_views rows = %#v", rs.Rows)
	}
	if rs.Rows[0]["refresh_every_ms"] != int64(30*time.Minute/time.Millisecond) || rs.Rows[0]["daily_at"] != "02:15" {
		t.Fatalf("unexpected catalog materialized view row: %#v", rs.Rows[0])
	}
}

func TestDropMaterializedViewRemovesCacheAndJobs(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()

	mvExecSQL(t, ctx, db, "CREATE TABLE metrics (id INT)")
	mvExecSQL(t, ctx, db, "INSERT INTO metrics VALUES (1)")
	mvExecSQL(t, ctx, db, `
		CREATE MATERIALIZED VIEW metric_count_drop AS
		SELECT COUNT(*) AS cnt FROM metrics
		REFRESH EVERY 5 MINUTES
		WITH DATA
	`)

	if _, err := db.Get("default", "__mv_metric_count_drop"); err != nil {
		t.Fatalf("expected cache table before drop: %v", err)
	}
	mvExecSQL(t, ctx, db, "DROP MATERIALIZED VIEW metric_count_drop")
	if _, ok := db.Catalog().GetMaterializedView("main", "metric_count_drop"); ok {
		t.Fatal("materialized view catalog entry still exists after drop")
	}
	if _, err := db.Get("default", "__mv_metric_count_drop"); err == nil {
		t.Fatal("cache table still exists after drop")
	}
	if _, err := db.Catalog().GetJob("__mv_refresh_metric_count_drop_interval"); err == nil {
		t.Fatal("refresh job still exists after drop")
	}
}

func TestAlterViewMaterializeAndBackToView(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()

	mvExecSQL(t, ctx, db, "CREATE TABLE orders (id INT, amount INT, status TEXT)")
	mvExecSQL(t, ctx, db, "INSERT INTO orders VALUES (1, 10, 'paid'), (2, 5, 'open')")
	mvExecSQL(t, ctx, db, `
		CREATE VIEW paid_total_convert AS
		WITH paid AS (
			SELECT amount FROM orders WHERE status = 'paid'
		)
		SELECT SUM(amount) AS total FROM paid
	`)

	mvExecSQL(t, ctx, db, "ALTER VIEW paid_total_convert MATERIALIZE REFRESH EVERY 10 MINUTES WITH DATA")
	if _, ok := db.Catalog().GetView("main", "paid_total_convert"); ok {
		t.Fatal("view catalog entry still exists after materialize")
	}
	if _, ok := db.Catalog().GetMaterializedView("main", "paid_total_convert"); !ok {
		t.Fatal("materialized view catalog entry missing after materialize")
	}
	if _, err := db.Catalog().GetJob("__mv_refresh_paid_total_convert_interval"); err != nil {
		t.Fatalf("refresh job missing after materialize: %v", err)
	}

	rs := mvQuerySQL(t, ctx, db, "SELECT total FROM paid_total_convert")
	if len(rs.Rows) != 1 || rs.Rows[0]["total"] != float64(10) {
		t.Fatalf("unexpected materialized conversion rows: %#v", rs.Rows)
	}

	mvExecSQL(t, ctx, db, "ALTER MATERIALIZED VIEW paid_total_convert TO VIEW")
	if _, ok := db.Catalog().GetMaterializedView("main", "paid_total_convert"); ok {
		t.Fatal("materialized view catalog entry still exists after TO VIEW")
	}
	if _, ok := db.Catalog().GetView("main", "paid_total_convert"); !ok {
		t.Fatal("view catalog entry missing after TO VIEW")
	}
	if _, err := db.Get("default", "__mv_paid_total_convert"); err == nil {
		t.Fatal("cache table still exists after TO VIEW")
	}
	if _, err := db.Catalog().GetJob("__mv_refresh_paid_total_convert_interval"); err == nil {
		t.Fatal("refresh job still exists after TO VIEW")
	}

	mvExecSQL(t, ctx, db, "INSERT INTO orders VALUES (3, 7, 'paid')")
	rs = mvQuerySQL(t, ctx, db, "SELECT total FROM paid_total_convert")
	if len(rs.Rows) != 1 || rs.Rows[0]["total"] != float64(17) {
		t.Fatalf("converted view should re-run query: %#v", rs.Rows)
	}
}

func TestObjectStatusViews(t *testing.T) {
	db := storage.NewDB()
	ctx := context.Background()

	mvExecSQL(t, ctx, db, "CREATE TABLE status_base (id INT)")
	mvExecSQL(t, ctx, db, "INSERT INTO status_base VALUES (1)")
	mvExecSQL(t, ctx, db, "CREATE VIEW status_view AS SELECT id FROM status_base")
	mvExecSQL(t, ctx, db, "CREATE MATERIALIZED VIEW status_mv AS SELECT COUNT(*) AS cnt FROM status_base REFRESH ON STALE AFTER 1 HOURS WITH DATA")
	mvExecSQL(t, ctx, db, "CREATE JOB status_job SCHEDULE INTERVAL 1000 AS SELECT COUNT(*) FROM status_base")

	rs := mvQuerySQL(t, ctx, db, "SELECT name, object_type, status FROM sys.objects WHERE name IN ('status_base', 'status_view', 'status_mv', 'status_job')")
	seen := map[string]string{}
	for _, r := range rs.Rows {
		seen[r["name"].(string)] = r["object_type"].(string)
		if r["status"] == nil || r["status"] == "" {
			t.Fatalf("missing status in sys.objects row: %#v", r)
		}
	}
	for name, typ := range map[string]string{
		"status_base": "TABLE",
		"status_view": "VIEW",
		"status_mv":   "MATERIALIZED_VIEW",
		"status_job":  "JOB",
	} {
		if seen[name] != typ {
			t.Fatalf("sys.objects[%s] = %q, want %q (rows=%#v)", name, seen[name], typ, rs.Rows)
		}
	}

	rs = mvQuerySQL(t, ctx, db, "SELECT name, object_type FROM catalog.objects WHERE name = 'status_mv'")
	if len(rs.Rows) != 1 || rs.Rows[0]["object_type"] != "MATERIALIZED_VIEW" {
		t.Fatalf("catalog.objects materialized view row = %#v", rs.Rows)
	}
}

func BenchmarkMaterializedViewCachedRead(b *testing.B) {
	db := storage.NewDB()
	ctx := context.Background()
	setupMaterializedViewBenchmark(b, ctx, db)
	stmt := mustParse("SELECT total FROM bench_sales_mv")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := Execute(ctx, db, "default", stmt); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMaterializedViewBaseQuery(b *testing.B) {
	db := storage.NewDB()
	ctx := context.Background()
	setupMaterializedViewBenchmark(b, ctx, db)
	stmt := mustParse("SELECT SUM(amount) AS total FROM bench_sales WHERE kind = 'sale'")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := Execute(ctx, db, "default", stmt); err != nil {
			b.Fatal(err)
		}
	}
}

func setupMaterializedViewBenchmark(b *testing.B, ctx context.Context, db *storage.DB) {
	b.Helper()
	if _, err := Execute(ctx, db, "default", mustParse("CREATE TABLE bench_sales (kind TEXT, amount INT)")); err != nil {
		b.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		if _, err := Execute(ctx, db, "default", mustParse("INSERT INTO bench_sales VALUES ('sale', 10), ('ignore', 1)")); err != nil {
			b.Fatal(err)
		}
	}
	if _, err := Execute(ctx, db, "default", mustParse(`
		CREATE MATERIALIZED VIEW bench_sales_mv AS
		SELECT SUM(amount) AS total FROM bench_sales WHERE kind = 'sale'
		WITH DATA
	`)); err != nil {
		b.Fatal(err)
	}
}

func mvExecSQL(t *testing.T, ctx context.Context, db *storage.DB, sql string) {
	t.Helper()
	if _, err := Execute(ctx, db, "default", mustParse(sql)); err != nil {
		t.Fatalf("%s failed: %v", sql, err)
	}
}

func mvQuerySQL(t *testing.T, ctx context.Context, db *storage.DB, sql string) *ResultSet {
	t.Helper()
	rs, err := Execute(ctx, db, "default", mustParse(sql))
	if err != nil {
		t.Fatalf("%s failed: %v", sql, err)
	}
	return rs
}
