package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	tinysql "github.com/SimonWaldherr/tinySQL"
)

type daemonConfig struct {
	DefaultTenant  string
	AuthToken      string
	RequestTimeout time.Duration
	MaxBodyBytes   int64
	MaxSQLBytes    int
}

type daemon struct {
	inst           *tinysql.Instance
	defaultTenant  string
	authToken      string
	requestTimeout time.Duration
	maxBodyBytes   int64
	maxSQLBytes    int
	startedAt      time.Time
	ready          atomic.Bool
}

type sqlRequest struct {
	Tenant    string `json:"tenant,omitempty"`
	SQL       string `json:"sql"`
	TimeoutMS int64  `json:"timeout_ms,omitempty"`
}

type runJobRequest struct {
	Tenant    string `json:"tenant,omitempty"`
	Name      string `json:"name"`
	TimeoutMS int64  `json:"timeout_ms,omitempty"`
}

type sqlResponse struct {
	Success bool              `json:"success"`
	Columns []string          `json:"columns,omitempty"`
	Rows    []tinysql.Row     `json:"rows,omitempty"`
	Error   string            `json:"error,omitempty"`
	Meta    map[string]string `json:"meta,omitempty"`
}

func newDaemon(inst *tinysql.Instance, cfg daemonConfig) *daemon {
	defaultTenant := strings.TrimSpace(cfg.DefaultTenant)
	if defaultTenant == "" {
		defaultTenant = "default"
	}
	maxBodyBytes := cfg.MaxBodyBytes
	if maxBodyBytes <= 0 {
		maxBodyBytes = 1 << 20
	}
	maxSQLBytes := cfg.MaxSQLBytes
	if maxSQLBytes <= 0 {
		maxSQLBytes = 1 << 20
	}
	d := &daemon{
		inst:           inst,
		defaultTenant:  defaultTenant,
		authToken:      strings.TrimSpace(cfg.AuthToken),
		requestTimeout: cfg.RequestTimeout,
		maxBodyBytes:   maxBodyBytes,
		maxSQLBytes:    maxSQLBytes,
		startedAt:      time.Now(),
	}
	d.ready.Store(true)
	return d
}

func (d *daemon) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", d.handleHealth)
	mux.HandleFunc("/readyz", d.handleReady)
	mux.HandleFunc("/api/status", d.requireAuth(d.handleStatus))
	mux.HandleFunc("/api/exec", d.requireAuth(d.handleExec))
	mux.HandleFunc("/api/query", d.requireAuth(d.handleQuery))
	mux.HandleFunc("/api/catalog/tables", d.requireAuth(d.handleCatalogTables))
	mux.HandleFunc("/api/catalog/columns", d.requireAuth(d.handleCatalogColumns))
	mux.HandleFunc("/api/jobs", d.requireAuth(d.handleJobs))
	mux.HandleFunc("/api/job-history", d.requireAuth(d.handleJobHistory))
	mux.HandleFunc("/api/jobs/run", d.requireAuth(d.handleRunJob))
	return mux
}

func (d *daemon) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeErrorJSON(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (d *daemon) handleReady(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeErrorJSON(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if !d.ready.Load() {
		writeErrorJSON(w, http.StatusServiceUnavailable, "server not ready")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ready": true})
}

func (d *daemon) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeErrorJSON(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	stats := d.inst.DB.BackendStats()
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":            true,
		"ready":         d.ready.Load(),
		"time":          time.Now().Format(time.RFC3339),
		"uptime":        time.Since(d.startedAt).String(),
		"mode":          d.inst.Mode.String(),
		"tenant":        d.defaultTenant,
		"storage_mode":  d.inst.DB.StorageMode().String(),
		"tables":        len(d.inst.DB.ListTables(d.defaultTenant)),
		"job_scheduler": d.inst.DB.JobScheduler() != nil,
		"backend_stats": map[string]any{
			"tables_in_memory":   stats.TablesInMemory,
			"tables_on_disk":     stats.TablesOnDisk,
			"memory_used_bytes":  stats.MemoryUsedBytes,
			"memory_limit_bytes": stats.MemoryLimitBytes,
			"disk_used_bytes":    stats.DiskUsedBytes,
			"cache_hit_rate":     stats.CacheHitRate,
			"sync_count":         stats.SyncCount,
			"load_count":         stats.LoadCount,
			"eviction_count":     stats.EvictionCount,
		},
	})
}

func (d *daemon) handleExec(w http.ResponseWriter, r *http.Request) {
	d.handleSQL(w, r)
}

func (d *daemon) handleQuery(w http.ResponseWriter, r *http.Request) {
	d.handleSQL(w, r)
}

func (d *daemon) handleCatalogTables(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeErrorJSON(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	tenant := d.tenantOrDefault(r.URL.Query().Get("tenant"))
	d.writeCatalogQuery(w, r, tenant, "SELECT schema, name, type, rows FROM catalog.tables")
}

func (d *daemon) handleCatalogColumns(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeErrorJSON(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	tenant := d.tenantOrDefault(r.URL.Query().Get("tenant"))
	d.writeCatalogQuery(w, r, tenant, "SELECT tenant, table_name, name, position, data_type, constraint, is_nullable, fk_table, fk_column FROM sys.columns")
}

func (d *daemon) handleJobs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeErrorJSON(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	tenant := d.tenantOrDefault(r.URL.Query().Get("tenant"))
	d.writeCatalogQuery(w, r, tenant, "SELECT name, enabled, schedule_type, cron_expr, interval_ms, run_at, timezone, catch_up, no_overlap, max_runtime_ms, last_run_at, next_run_at, created_at, updated_at FROM catalog.jobs")
}

func (d *daemon) handleJobHistory(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeErrorJSON(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	tenant := d.tenantOrDefault(r.URL.Query().Get("tenant"))
	d.writeCatalogQuery(w, r, tenant, "SELECT run_id, job_name, started_at, finished_at, duration_ms, status, error_message FROM catalog.job_history")
}

func (d *daemon) writeCatalogQuery(w http.ResponseWriter, r *http.Request, tenant, sqlText string) {
	ctx, cancel, err := d.requestContext(r.Context(), 0)
	if err != nil {
		writeErrorJSON(w, http.StatusBadRequest, err.Error())
		return
	}
	defer cancel()
	rs, err := d.executeSQL(ctx, tenant, sqlText)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, sqlResponse{Success: false, Error: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, sqlResponse{
		Success: true,
		Columns: rs.Cols,
		Rows:    rs.Rows,
		Meta:    map[string]string{"tenant": tenant},
	})
}

func (d *daemon) handleRunJob(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErrorJSON(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req runJobRequest
	if err := decodeJSONBody(w, r, d.maxBodyBytes, &req); err != nil {
		writeErrorJSON(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	name := strings.TrimSpace(req.Name)
	if name == "" {
		writeErrorJSON(w, http.StatusBadRequest, "job name must not be empty")
		return
	}
	job, err := d.inst.DB.Catalog().GetJob(name)
	if err != nil {
		writeErrorJSON(w, http.StatusNotFound, err.Error())
		return
	}
	ctx, cancel, err := d.requestContext(r.Context(), req.TimeoutMS)
	if err != nil {
		writeErrorJSON(w, http.StatusBadRequest, err.Error())
		return
	}
	defer cancel()

	tenant := d.tenantOrDefault(req.Tenant)
	started := time.Now()
	status := "SUCCEEDED"
	errMsg := ""
	executor := tinysql.NewSQLJobExecutor(d.inst.DB, tenant)
	if _, err := executor.ExecuteSQL(ctx, job.SQLText); err != nil {
		status = "FAILED"
		errMsg = err.Error()
	}
	finished := time.Now()
	if ctx.Err() != nil && status == "SUCCEEDED" {
		status = "CANCELED"
		errMsg = ctx.Err().Error()
	}
	if err := d.inst.DB.Catalog().UpdateJobRuntimePtr(job.Name, started, job.NextRunAt); err != nil {
		writeErrorJSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	if err := d.inst.DB.Catalog().AddJobHistory(&tinysql.CatalogJobHistory{
		JobName:      job.Name,
		StartedAt:    started,
		FinishedAt:   finished,
		DurationMs:   finished.Sub(started).Milliseconds(),
		Status:       status,
		ErrorMessage: errMsg,
	}); err != nil {
		writeErrorJSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	if err := d.inst.DB.Sync(); err != nil {
		writeErrorJSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	resp := map[string]any{
		"success":     status == "SUCCEEDED",
		"job_name":    job.Name,
		"tenant":      tenant,
		"status":      status,
		"duration_ms": finished.Sub(started).Milliseconds(),
	}
	if errMsg != "" {
		resp["error"] = errMsg
	}
	if status == "SUCCEEDED" {
		writeJSON(w, http.StatusOK, resp)
		return
	}
	writeJSON(w, http.StatusBadRequest, resp)
}

func (d *daemon) handleSQL(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErrorJSON(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req sqlRequest
	if err := decodeJSONBody(w, r, d.maxBodyBytes, &req); err != nil {
		writeErrorJSON(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	sqlText, err := d.normalizeSQL(req.SQL)
	if err != nil {
		writeErrorJSON(w, http.StatusBadRequest, err.Error())
		return
	}
	ctx, cancel, err := d.requestContext(r.Context(), req.TimeoutMS)
	if err != nil {
		writeErrorJSON(w, http.StatusBadRequest, err.Error())
		return
	}
	defer cancel()

	tenant := d.tenantOrDefault(req.Tenant)
	rs, err := d.executeSQL(ctx, tenant, sqlText)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, sqlResponse{Success: false, Error: err.Error()})
		return
	}
	resp := sqlResponse{
		Success: true,
		Meta: map[string]string{
			"tenant": tenant,
		},
	}
	if rs != nil {
		resp.Columns = rs.Cols
		resp.Rows = rs.Rows
	}
	writeJSON(w, http.StatusOK, resp)
}

func (d *daemon) executeSQL(ctx context.Context, tenant, sqlText string) (*tinysql.ResultSet, error) {
	stmt, err := tinysql.ParseSQL(sqlText)
	if err != nil {
		return nil, err
	}
	return tinysql.Execute(ctx, d.inst.DB, tenant, stmt)
}

func (d *daemon) tenantOrDefault(tenant string) string {
	tenant = strings.TrimSpace(tenant)
	if tenant == "" {
		return d.defaultTenant
	}
	return tenant
}

func (d *daemon) normalizeSQL(sqlText string) (string, error) {
	sqlText = strings.TrimSpace(sqlText)
	if sqlText == "" {
		return "", fmt.Errorf("sql must not be empty")
	}
	if d.maxSQLBytes > 0 && len(sqlText) > d.maxSQLBytes {
		return "", fmt.Errorf("sql exceeds max length (%d bytes)", d.maxSQLBytes)
	}
	return sqlText, nil
}

func (d *daemon) requestContext(parent context.Context, timeoutMS int64) (context.Context, context.CancelFunc, error) {
	if timeoutMS < 0 {
		return nil, nil, fmt.Errorf("timeout_ms must be >= 0")
	}
	if timeoutMS > 0 {
		timeout := time.Duration(timeoutMS) * time.Millisecond
		if timeout <= 0 {
			return nil, nil, fmt.Errorf("timeout_ms is out of range")
		}
		if d.requestTimeout > 0 && timeout > d.requestTimeout {
			timeout = d.requestTimeout
		}
		ctx, cancel := context.WithTimeout(parent, timeout)
		return ctx, cancel, nil
	}
	if d.requestTimeout > 0 {
		ctx, cancel := context.WithTimeout(parent, d.requestTimeout)
		return ctx, cancel, nil
	}
	return parent, func() {}, nil
}

func (d *daemon) requireAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if d.authToken == "" {
			next(w, r)
			return
		}
		if bearerToken(r.Header.Get("Authorization")) == d.authToken || r.Header.Get("X-TinySQL-Auth") == d.authToken {
			next(w, r)
			return
		}
		writeErrorJSON(w, http.StatusUnauthorized, "unauthorized")
	}
}

func bearerToken(header string) string {
	header = strings.TrimSpace(header)
	if len(header) < 7 || !strings.EqualFold(header[:7], "Bearer ") {
		return ""
	}
	return strings.TrimSpace(header[7:])
}

func decodeJSONBody(w http.ResponseWriter, r *http.Request, limit int64, dst any) error {
	r.Body = http.MaxBytesReader(w, r.Body, limit)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return err
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return fmt.Errorf("request body must contain exactly one JSON object")
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErrorJSON(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]any{
		"success": false,
		"error":   msg,
	})
}
