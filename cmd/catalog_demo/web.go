package main

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	tinysql "github.com/SimonWaldherr/tinySQL"
)

//go:embed templates/*.html static/*
var catalogAssets embed.FS

type catalogWebApp struct {
	db      *tinysql.DB
	catalog *tinysql.CatalogManager
	tenant  string
	tpl     *template.Template
	mu      sync.Mutex
	runs    []jobRun
}

type catalogJobView struct {
	Name      string `json:"name"`
	Schedule  string `json:"schedule"`
	Enabled   bool   `json:"enabled"`
	SQL       string `json:"sql"`
	LastRunAt string `json:"last_run_at"`
	NextRunAt string `json:"next_run_at"`
}

type jobRun struct {
	Job       string `json:"job"`
	StartedAt int64  `json:"started_at"`
	Duration  int64  `json:"duration_ms"`
	Rows      int    `json:"rows"`
	Error     string `json:"error,omitempty"`
	Preview   string `json:"preview,omitempty"`
}

type webSchedulerExecutor struct{ app *catalogWebApp }

func (e *webSchedulerExecutor) ExecuteSQL(ctx context.Context, sql string) (interface{}, error) {
	return e.app.executeSQL(ctx, "scheduled", sql)
}

func serveCatalogDashboard(addr string) error {
	a, err := newCatalogWebApp(context.Background())
	if err != nil {
		return err
	}
	defer a.db.StopJobScheduler()
	mux := http.NewServeMux()
	a.routes(mux)
	mux.Handle("GET /static/", http.FileServer(http.FS(catalogAssets)))
	return http.ListenAndServe(addr, catalogSecurityHeaders(mux))
}

func newCatalogWebApp(ctx context.Context) (*catalogWebApp, error) {
	db := tinysql.NewDB()
	a := &catalogWebApp{db: db, catalog: db.Catalog(), tenant: "default"}
	tpl, err := template.ParseFS(catalogAssets, "templates/*.html")
	if err != nil {
		return nil, err
	}
	a.tpl = tpl
	if err := a.execSeed(ctx, `CREATE TABLE events (id INT PRIMARY KEY, kind TEXT, ts INT, payload TEXT)`); err != nil {
		return nil, err
	}
	if err := a.execSeed(ctx, `CREATE TABLE event_stats (kind TEXT, total INT, last_updated INT)`); err != nil {
		return nil, err
	}
	kinds := []string{"click", "click", "view"}
	base := time.Now().UTC().Unix()
	for i := 1; i <= 20; i++ {
		if err := a.execSeed(ctx, fmt.Sprintf("INSERT INTO events VALUES (%d, '%s', %d, 'payload-%d')", i, kinds[i%len(kinds)], base-int64(i*60), i)); err != nil {
			return nil, err
		}
	}
	a.catalog.RegisterTable("main", "events", []tinysql.Column{{Name: "id", Type: tinysql.IntType}, {Name: "kind", Type: tinysql.StringType}, {Name: "ts", Type: tinysql.IntType}, {Name: "payload", Type: tinysql.StringType}})
	a.catalog.RegisterTable("main", "event_stats", []tinysql.Column{{Name: "kind", Type: tinysql.StringType}, {Name: "total", Type: tinysql.IntType}, {Name: "last_updated", Type: tinysql.IntType}})
	a.catalog.RegisterView("main", "recent_events", "SELECT * FROM events ORDER BY ts DESC LIMIT 10")
	a.catalog.RegisterFunction(&tinysql.CatalogFunction{Schema: "main", Name: "json_get", FunctionType: "SCALAR", ArgTypes: []string{"STRING", "STRING"}, ReturnType: "STRING", Language: "BUILTIN", IsDeterministic: true, Description: "Extract a field from JSON"})
	if err := a.catalog.RegisterJob(&tinysql.CatalogJob{Name: "refresh_event_stats", SQLText: `SELECT kind, COUNT(*) AS total FROM events GROUP BY kind ORDER BY kind`, ScheduleType: "INTERVAL", IntervalMs: 15_000, Enabled: true, CatchUp: false, MaxRuntimeMs: 5_000}); err != nil {
		return nil, err
	}
	runAt := time.Now().UTC().Add(time.Minute)
	if err := a.catalog.RegisterJob(&tinysql.CatalogJob{Name: "integrity_check", SQLText: `SELECT COUNT(*) AS total_events FROM events`, ScheduleType: "ONCE", RunAt: &runAt, Enabled: true, MaxRuntimeMs: 5_000}); err != nil {
		return nil, err
	}
	if err := db.StartJobScheduler(&webSchedulerExecutor{app: a}); err != nil {
		return nil, err
	}
	return a, nil
}

func (a *catalogWebApp) execSeed(ctx context.Context, sql string) error {
	stmt, err := tinysql.ParseSQL(sql)
	if err != nil {
		return err
	}
	_, err = tinysql.Execute(ctx, a.db, a.tenant, stmt)
	return err
}
func (a *catalogWebApp) routes(mux *http.ServeMux) {
	mux.HandleFunc("GET /", a.index)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { writeCatalogJSON(w, 200, map[string]bool{"ok": true}) })
	mux.HandleFunc("GET /api/state", a.state)
	mux.HandleFunc("POST /api/jobs/{name}/run", a.runJob)
}
func (a *catalogWebApp) index(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := a.tpl.ExecuteTemplate(w, "index.html", nil); err != nil {
		http.Error(w, "template error", 500)
	}
}
func (a *catalogWebApp) state(w http.ResponseWriter, r *http.Request) {
	tables := a.catalog.GetTables()
	names := make([]string, 0, len(tables))
	for _, t := range tables {
		names = append(names, t.Schema+"."+t.Name)
	}
	sort.Strings(names)
	jobs := a.jobViews()
	a.mu.Lock()
	runs := make([]jobRun, len(a.runs))
	copy(runs, a.runs)
	a.mu.Unlock()
	writeCatalogJSON(w, 200, map[string]any{"tables": names, "jobs": jobs, "runs": runs})
}
func (a *catalogWebApp) jobViews() []catalogJobView {
	jobs := a.catalog.ListJobs()
	views := make([]catalogJobView, 0, len(jobs))
	for _, job := range jobs {
		v := catalogJobView{Name: job.Name, Schedule: job.ScheduleType, Enabled: job.Enabled, SQL: job.SQLText}
		if job.LastRunAt != nil {
			v.LastRunAt = job.LastRunAt.UTC().Format(time.RFC3339)
		}
		if job.NextRunAt != nil {
			v.NextRunAt = job.NextRunAt.UTC().Format(time.RFC3339)
		}
		views = append(views, v)
	}
	sort.Slice(views, func(i, j int) bool { return views[i].Name < views[j].Name })
	return views
}
func (a *catalogWebApp) runJob(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimSpace(r.PathValue("name"))
	job, err := a.catalog.GetJob(name)
	if err != nil {
		catalogError(w, 404, "job not found")
		return
	}
	value, err := a.executeSQL(r.Context(), job.Name, job.SQLText)
	if err != nil {
		catalogError(w, 500, "job execution failed")
		return
	}
	writeCatalogJSON(w, 200, value)
}
func (a *catalogWebApp) executeSQL(ctx context.Context, name, sqlText string) (interface{}, error) {
	started := time.Now()
	run := jobRun{Job: name, StartedAt: started.UTC().Unix()}
	stmt, err := tinysql.ParseSQL(sqlText)
	if err == nil {
		var rs *tinysql.ResultSet
		rs, err = tinysql.Execute(ctx, a.db, a.tenant, stmt)
		if rs != nil {
			run.Rows = len(rs.Rows)
			run.Preview = formatFirstRow(rs)
		}
	}
	run.Duration = time.Since(started).Milliseconds()
	if err != nil {
		run.Error = err.Error()
	}
	a.mu.Lock()
	a.runs = append([]jobRun{run}, a.runs...)
	if len(a.runs) > 30 {
		a.runs = a.runs[:30]
	}
	a.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return run, nil
}
func writeCatalogJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func catalogError(w http.ResponseWriter, status int, msg string) {
	writeCatalogJSON(w, status, map[string]string{"error": msg})
}
func catalogSecurityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Security-Policy", "default-src 'self'; style-src 'self'; script-src 'self'; connect-src 'self'; base-uri 'none'; frame-ancestors 'none'")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		next.ServeHTTP(w, r)
	})
}
