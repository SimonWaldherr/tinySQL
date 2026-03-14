package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"html/template"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	tinysql "github.com/SimonWaldherr/tinySQL"
)

// webState holds the shared state for the web server.
type webState struct {
	db     *tinysql.DB
	ctx    context.Context
	tenant string
}

// apiResponse is the JSON structure returned by API endpoints.
type apiResponse struct {
	Success  bool              `json:"success"`
	Columns  []string          `json:"columns,omitempty"`
	Rows     []map[string]any  `json:"rows,omitempty"`
	RowCount int               `json:"rowCount"`
	Duration string            `json:"duration,omitempty"`
	Message  string            `json:"message,omitempty"`
	Error    string            `json:"error,omitempty"`
	Tables   []tableInfo       `json:"tables,omitempty"`
	Conns    []connectionInfo  `json:"connections,omitempty"`
}

type tableInfo struct {
	Name    string `json:"name"`
	Rows    int    `json:"rows"`
	Columns int    `json:"columns"`
}

type connectionInfo struct {
	Name   string `json:"name"`
	Driver string `json:"driver"`
	DSN    string `json:"dsn"`
}

func runWeb(args []string) error {
	fs := flag.NewFlagSet("web", flag.ExitOnError)
	addr := fs.String("addr", ":8080", "Listen address (host:port)")
	files := fs.String("files", "", "Comma-separated files to pre-load")
	verbose := fs.Bool("verbose", false, "Verbose logging")

	if err := fs.Parse(args); err != nil {
		return err
	}

	db := tinysql.NewDB()
	ctx := context.Background()
	tenant := "default"

	// Pre-load files
	if *files != "" {
		for _, f := range strings.Split(*files, ",") {
			f = strings.TrimSpace(f)
			if f == "" {
				continue
			}
			tableName := tableNameFromFile(f)
			if err := importFileToTinySQL(db, ctx, tenant, f, tableName, true, *verbose); err != nil {
				log.Printf("Warning: failed to load %s: %v", f, err)
			} else {
				log.Printf("Loaded %s as table '%s'", f, tableName)
			}
		}
	}

	state := &webState{db: db, ctx: ctx, tenant: tenant}

	mux := http.NewServeMux()
	mux.HandleFunc("/", state.handleIndex)
	mux.HandleFunc("/api/query", state.handleQuery)
	mux.HandleFunc("/api/tables", state.handleTables)
	mux.HandleFunc("/api/connections", state.handleConnections)
	mux.HandleFunc("/api/connect", state.handleAPIConnect)
	mux.HandleFunc("/api/disconnect", state.handleAPIDisconnect)
	mux.HandleFunc("/api/import-file", state.handleImportFile)
	mux.HandleFunc("/api/import-db", state.handleImportDB)
	mux.HandleFunc("/api/export", state.handleAPIExport)

	log.Printf("tinySQL Migrate web interface listening on http://localhost%s", *addr)
	return http.ListenAndServe(*addr, mux)
}

func (s *webState) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	tmpl, err := template.New("index").Parse(indexHTML)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	tmpl.Execute(w, nil)
}

func (s *webState) handleQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, apiResponse{Error: "POST required"})
		return
	}

	var req struct {
		SQL string `json:"sql"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, apiResponse{Error: "invalid JSON body"})
		return
	}

	sql := strings.TrimSpace(req.SQL)
	if sql == "" {
		writeJSON(w, http.StatusBadRequest, apiResponse{Error: "empty SQL"})
		return
	}

	// Handle COPY ... INTO syntax
	upper := strings.ToUpper(sql)
	if strings.HasPrefix(upper, "COPY ") && strings.Contains(upper, " INTO ") {
		s.handleCopyQuery(w, sql)
		return
	}

	stmt, err := tinysql.ParseSQL(sql)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, apiResponse{Error: fmt.Sprintf("parse error: %v", err)})
		return
	}

	start := time.Now()
	result, err := tinysql.Execute(s.ctx, s.db, s.tenant, stmt)
	duration := time.Since(start)

	if err != nil {
		writeJSON(w, http.StatusInternalServerError, apiResponse{Error: fmt.Sprintf("execute error: %v", err)})
		return
	}

	resp := apiResponse{
		Success:  true,
		Duration: duration.String(),
	}

	if result != nil {
		resp.Columns = result.Cols
		resp.RowCount = len(result.Rows)
		rows := make([]map[string]any, 0, len(result.Rows))
		for _, row := range result.Rows {
			cleanRow := make(map[string]any)
			for _, col := range result.Cols {
				cleanRow[col] = row[strings.ToLower(col)]
			}
			rows = append(rows, cleanRow)
		}
		resp.Rows = rows
	} else {
		resp.Message = "OK"
	}

	writeJSON(w, http.StatusOK, resp)
}

func (s *webState) handleCopyQuery(w http.ResponseWriter, sql string) {
	upper := strings.ToUpper(sql)
	intoIdx := strings.LastIndex(upper, " INTO ")
	if intoIdx == -1 {
		writeJSON(w, http.StatusBadRequest, apiResponse{Error: "COPY requires INTO clause"})
		return
	}

	query := strings.TrimSpace(sql[5:intoIdx])
	target := strings.TrimSpace(sql[intoIdx+6:])

	dotIdx := strings.Index(target, ".")
	if dotIdx == -1 {
		writeJSON(w, http.StatusBadRequest, apiResponse{Error: "target must be <connection>.<table>"})
		return
	}

	connName := target[:dotIdx]
	targetTable := target[dotIdx+1:]

	extDB, driver, err := getConnection(connName)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, apiResponse{Error: err.Error()})
		return
	}

	stmt, err := tinysql.ParseSQL(query)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, apiResponse{Error: fmt.Sprintf("parse error: %v", err)})
		return
	}

	start := time.Now()
	result, err := tinysql.Execute(s.ctx, s.db, s.tenant, stmt)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, apiResponse{Error: fmt.Sprintf("execute error: %v", err)})
		return
	}

	if result == nil || len(result.Rows) == 0 {
		writeJSON(w, http.StatusOK, apiResponse{Success: true, Message: "No data to copy", RowCount: 0})
		return
	}

	count, err := exportToExternal(extDB, driver, result, targetTable, true)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, apiResponse{Error: fmt.Sprintf("export error: %v", err)})
		return
	}

	writeJSON(w, http.StatusOK, apiResponse{
		Success:  true,
		RowCount: count,
		Duration: time.Since(start).String(),
		Message:  fmt.Sprintf("Copied %d rows to %s.%s", count, connName, targetTable),
	})
}

func (s *webState) handleTables(w http.ResponseWriter, r *http.Request) {
	tables := s.db.ListTables(s.tenant)
	infos := make([]tableInfo, 0, len(tables))
	for _, t := range tables {
		infos = append(infos, tableInfo{
			Name:    t.Name,
			Rows:    len(t.Rows),
			Columns: len(t.Cols),
		})
	}
	writeJSON(w, http.StatusOK, apiResponse{Success: true, Tables: infos})
}

func (s *webState) handleConnections(w http.ResponseWriter, r *http.Request) {
	connRegistryMux.RLock()
	defer connRegistryMux.RUnlock()

	conns := make([]connectionInfo, 0, len(connRegistry))
	for name, dsn := range connDSN {
		conns = append(conns, connectionInfo{
			Name:   name,
			Driver: connDrivers[name],
			DSN:    maskDSN(dsn),
		})
	}
	writeJSON(w, http.StatusOK, apiResponse{Success: true, Conns: conns})
}

func (s *webState) handleAPIConnect(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, apiResponse{Error: "POST required"})
		return
	}

	var req struct {
		Name string `json:"name"`
		DSN  string `json:"dsn"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, apiResponse{Error: "invalid JSON"})
		return
	}

	if req.Name == "" || req.DSN == "" {
		writeJSON(w, http.StatusBadRequest, apiResponse{Error: "name and dsn are required"})
		return
	}

	driver, connStr := parseDSN(req.DSN)
	extDB, err := openExternalDB(driver, connStr)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, apiResponse{Error: err.Error()})
		return
	}

	connRegistryMux.Lock()
	if old, exists := connRegistry[req.Name]; exists {
		old.Close()
	}
	connRegistry[req.Name] = extDB
	connDSN[req.Name] = req.DSN
	connDrivers[req.Name] = driver
	connRegistryMux.Unlock()

	writeJSON(w, http.StatusOK, apiResponse{
		Success: true,
		Message: fmt.Sprintf("Connected '%s' (%s)", req.Name, driver),
	})
}

func (s *webState) handleAPIDisconnect(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, apiResponse{Error: "POST required"})
		return
	}

	var req struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, apiResponse{Error: "invalid JSON"})
		return
	}

	connRegistryMux.Lock()
	defer connRegistryMux.Unlock()

	if db, exists := connRegistry[req.Name]; exists {
		db.Close()
		delete(connRegistry, req.Name)
		delete(connDSN, req.Name)
		delete(connDrivers, req.Name)
		writeJSON(w, http.StatusOK, apiResponse{Success: true, Message: fmt.Sprintf("Disconnected '%s'", req.Name)})
	} else {
		writeJSON(w, http.StatusNotFound, apiResponse{Error: fmt.Sprintf("connection '%s' not found", req.Name)})
	}
}

func (s *webState) handleImportFile(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, apiResponse{Error: "POST required"})
		return
	}

	if err := r.ParseMultipartForm(32 << 20); err != nil {
		writeJSON(w, http.StatusBadRequest, apiResponse{Error: "invalid multipart form"})
		return
	}

	file, header, err := r.FormFile("file")
	if err != nil {
		writeJSON(w, http.StatusBadRequest, apiResponse{Error: "file field required"})
		return
	}
	defer file.Close()

	tableName := r.FormValue("table")
	if tableName == "" {
		tableName = tableNameFromFile(header.Filename)
	}

	// Write uploaded file to temp location
	tmpDir := os.TempDir()
	tmpFile := filepath.Join(tmpDir, "migrate_upload_"+header.Filename)
	out, err := os.Create(tmpFile)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, apiResponse{Error: "failed to create temp file"})
		return
	}
	if _, err := io.Copy(out, file); err != nil {
		out.Close()
		os.Remove(tmpFile)
		writeJSON(w, http.StatusInternalServerError, apiResponse{Error: "failed to write temp file"})
		return
	}
	out.Close()
	defer os.Remove(tmpFile)

	start := time.Now()
	if err := importFileToTinySQL(s.db, s.ctx, s.tenant, tmpFile, tableName, true, false); err != nil {
		writeJSON(w, http.StatusInternalServerError, apiResponse{Error: fmt.Sprintf("import failed: %v", err)})
		return
	}

	writeJSON(w, http.StatusOK, apiResponse{
		Success:  true,
		Duration: time.Since(start).String(),
		Message:  fmt.Sprintf("Imported '%s' as table '%s'", header.Filename, tableName),
	})
}

func (s *webState) handleImportDB(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, apiResponse{Error: "POST required"})
		return
	}

	var req struct {
		Connection string `json:"connection"`
		Query      string `json:"query"`
		Table      string `json:"table"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, apiResponse{Error: "invalid JSON"})
		return
	}

	if req.Connection == "" || (req.Query == "" && req.Table == "") {
		writeJSON(w, http.StatusBadRequest, apiResponse{Error: "connection and query/table required"})
		return
	}

	extDB, _, err := getConnection(req.Connection)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, apiResponse{Error: err.Error()})
		return
	}

	query := req.Query
	tableName := req.Table
	if query == "" {
		query = fmt.Sprintf("SELECT * FROM %s", tableName)
	}
	if tableName == "" {
		tableName = "imported"
	}

	start := time.Now()
	count, err := importFromExternal(s.db, s.ctx, s.tenant, extDB, query, tableName, false)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, apiResponse{Error: fmt.Sprintf("import failed: %v", err)})
		return
	}

	writeJSON(w, http.StatusOK, apiResponse{
		Success:  true,
		RowCount: count,
		Duration: time.Since(start).String(),
		Message:  fmt.Sprintf("Imported %d rows into '%s'", count, tableName),
	})
}

func (s *webState) handleAPIExport(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, apiResponse{Error: "POST required"})
		return
	}

	var req struct {
		SQL    string `json:"sql"`
		Format string `json:"format"` // "json" or "csv"
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, apiResponse{Error: "invalid JSON"})
		return
	}

	if req.SQL == "" {
		writeJSON(w, http.StatusBadRequest, apiResponse{Error: "sql is required"})
		return
	}
	if req.Format == "" {
		req.Format = "json"
	}

	stmt, err := tinysql.ParseSQL(req.SQL)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, apiResponse{Error: fmt.Sprintf("parse error: %v", err)})
		return
	}

	result, err := tinysql.Execute(s.ctx, s.db, s.tenant, stmt)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, apiResponse{Error: fmt.Sprintf("execute error: %v", err)})
		return
	}

	if result == nil {
		writeJSON(w, http.StatusOK, apiResponse{Success: true, Message: "No results"})
		return
	}

	switch req.Format {
	case "csv":
		w.Header().Set("Content-Type", "text/csv")
		w.Header().Set("Content-Disposition", "attachment; filename=export.csv")
		outputCSV(w, result)
	default:
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Disposition", "attachment; filename=export.json")
		outputJSON(w, result)
	}
}

// openExternalDB opens and pings an external database connection.
func openExternalDB(driver, connStr string) (*sql.DB, error) {
	db, err := sql.Open(driver, connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to open %s: %v", driver, err)
	}
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to connect to %s: %v", driver, err)
	}
	return db, nil
}

func writeJSON(w http.ResponseWriter, status int, data apiResponse) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

// ============================================================================
// Embedded HTML/CSS/JS — single binary deployment
// ============================================================================

const indexHTML = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>tinySQL Migrate</title>
<style>
:root {
  --bg: #0f172a;
  --surface: #1e293b;
  --surface2: #334155;
  --border: #475569;
  --text: #e2e8f0;
  --text-dim: #94a3b8;
  --accent: #38bdf8;
  --accent2: #818cf8;
  --success: #4ade80;
  --error: #f87171;
  --warning: #fbbf24;
  --font: "Inter", system-ui, -apple-system, sans-serif;
  --mono: "JetBrains Mono", "Fira Code", "Cascadia Code", monospace;
}
* { margin: 0; padding: 0; box-sizing: border-box; }
body {
  font-family: var(--font);
  background: var(--bg);
  color: var(--text);
  min-height: 100vh;
  display: flex;
  flex-direction: column;
}
header {
  background: var(--surface);
  border-bottom: 1px solid var(--border);
  padding: 0.75rem 1.5rem;
  display: flex;
  align-items: center;
  gap: 1rem;
}
header .logo {
  font-size: 1.1rem;
  font-weight: 700;
  color: var(--accent);
  letter-spacing: -0.02em;
}
header .logo span { color: var(--text-dim); font-weight: 400; }
header nav { display: flex; gap: 0.5rem; margin-left: auto; }
header nav button {
  background: var(--surface2);
  border: 1px solid var(--border);
  color: var(--text);
  padding: 0.4rem 0.8rem;
  border-radius: 6px;
  cursor: pointer;
  font-size: 0.8rem;
  transition: background 0.15s;
}
header nav button:hover { background: var(--border); }
header nav button.active { background: var(--accent); color: var(--bg); border-color: var(--accent); }

.container {
  display: flex;
  flex: 1;
  overflow: hidden;
}

.sidebar {
  width: 260px;
  background: var(--surface);
  border-right: 1px solid var(--border);
  display: flex;
  flex-direction: column;
  overflow-y: auto;
}
.sidebar h3 {
  font-size: 0.7rem;
  text-transform: uppercase;
  letter-spacing: 0.05em;
  color: var(--text-dim);
  padding: 1rem 1rem 0.5rem;
}
.sidebar .table-item {
  padding: 0.5rem 1rem;
  cursor: pointer;
  display: flex;
  justify-content: space-between;
  font-size: 0.85rem;
  transition: background 0.1s;
}
.sidebar .table-item:hover { background: var(--surface2); }
.sidebar .table-item .meta { color: var(--text-dim); font-size: 0.75rem; }
.sidebar .conn-item {
  padding: 0.5rem 1rem;
  font-size: 0.85rem;
  display: flex;
  justify-content: space-between;
  align-items: center;
}
.sidebar .conn-item .driver { color: var(--accent2); font-size: 0.75rem; }
.sidebar .conn-item button {
  background: none;
  border: none;
  color: var(--error);
  cursor: pointer;
  font-size: 0.75rem;
  padding: 2px 6px;
  border-radius: 3px;
}
.sidebar .conn-item button:hover { background: rgba(248,113,113,0.15); }
.sidebar .empty { color: var(--text-dim); font-size: 0.8rem; padding: 0.5rem 1rem; font-style: italic; }

.main-panel {
  flex: 1;
  display: flex;
  flex-direction: column;
  overflow: hidden;
}

.editor-area {
  padding: 1rem;
  border-bottom: 1px solid var(--border);
  display: flex;
  flex-direction: column;
  gap: 0.5rem;
}
.editor-area textarea {
  font-family: var(--mono);
  font-size: 0.9rem;
  background: var(--bg);
  color: var(--text);
  border: 1px solid var(--border);
  border-radius: 8px;
  padding: 0.75rem;
  resize: vertical;
  min-height: 100px;
  outline: none;
  line-height: 1.5;
}
.editor-area textarea:focus { border-color: var(--accent); }
.editor-bar {
  display: flex;
  gap: 0.5rem;
  align-items: center;
}
.editor-bar button {
  padding: 0.5rem 1.2rem;
  border-radius: 6px;
  border: none;
  font-size: 0.85rem;
  cursor: pointer;
  font-weight: 600;
  transition: background 0.15s, transform 0.1s;
}
.editor-bar button:active { transform: scale(0.97); }
.btn-run { background: var(--accent); color: var(--bg); }
.btn-run:hover { background: #7dd3fc; }
.btn-secondary { background: var(--surface2); color: var(--text); border: 1px solid var(--border) !important; }
.btn-secondary:hover { background: var(--border); }
.editor-bar .status {
  font-size: 0.8rem;
  color: var(--text-dim);
  margin-left: auto;
}
.editor-bar .status.ok { color: var(--success); }
.editor-bar .status.err { color: var(--error); }

.results-area {
  flex: 1;
  overflow: auto;
  padding: 0 1rem 1rem;
}
.results-area table {
  width: 100%;
  border-collapse: collapse;
  font-size: 0.85rem;
}
.results-area th {
  position: sticky;
  top: 0;
  background: var(--surface);
  text-align: left;
  padding: 0.5rem 0.75rem;
  border-bottom: 2px solid var(--border);
  font-weight: 600;
  font-size: 0.8rem;
  color: var(--accent);
  white-space: nowrap;
}
.results-area td {
  padding: 0.4rem 0.75rem;
  border-bottom: 1px solid var(--surface2);
  white-space: nowrap;
  max-width: 300px;
  overflow: hidden;
  text-overflow: ellipsis;
}
.results-area tr:hover td { background: rgba(56,189,248,0.05); }
.results-area .message {
  color: var(--text-dim);
  padding: 2rem;
  text-align: center;
  font-size: 0.9rem;
}
.results-area .error-msg {
  color: var(--error);
  background: rgba(248,113,113,0.1);
  padding: 1rem;
  border-radius: 8px;
  margin: 1rem 0;
  font-family: var(--mono);
  font-size: 0.85rem;
}

/* Dialogs / Modals */
.modal-overlay {
  display: none;
  position: fixed;
  inset: 0;
  background: rgba(0,0,0,0.6);
  z-index: 100;
  justify-content: center;
  align-items: center;
}
.modal-overlay.active { display: flex; }
.modal {
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: 12px;
  padding: 1.5rem;
  min-width: 400px;
  max-width: 90vw;
}
.modal h2 {
  font-size: 1rem;
  margin-bottom: 1rem;
  color: var(--accent);
}
.modal label {
  display: block;
  font-size: 0.8rem;
  color: var(--text-dim);
  margin-bottom: 0.3rem;
  margin-top: 0.75rem;
}
.modal input, .modal select {
  width: 100%;
  padding: 0.5rem;
  background: var(--bg);
  border: 1px solid var(--border);
  border-radius: 6px;
  color: var(--text);
  font-size: 0.85rem;
  outline: none;
}
.modal input:focus, .modal select:focus { border-color: var(--accent); }
.modal .actions {
  display: flex;
  gap: 0.5rem;
  margin-top: 1rem;
  justify-content: flex-end;
}
.modal .actions button {
  padding: 0.5rem 1rem;
  border-radius: 6px;
  border: 1px solid var(--border);
  font-size: 0.85rem;
  cursor: pointer;
}
.modal .actions .btn-primary { background: var(--accent); color: var(--bg); border-color: var(--accent); font-weight: 600; }
.modal .actions .btn-cancel { background: var(--surface2); color: var(--text); }

/* File upload */
.upload-zone {
  border: 2px dashed var(--border);
  border-radius: 8px;
  padding: 2rem;
  text-align: center;
  color: var(--text-dim);
  cursor: pointer;
  transition: border-color 0.2s;
  margin-top: 0.75rem;
}
.upload-zone:hover, .upload-zone.dragover { border-color: var(--accent); color: var(--accent); }
.upload-zone input { display: none; }

@media (max-width: 768px) {
  .sidebar { display: none; }
  .modal { min-width: auto; margin: 1rem; }
}
</style>
</head>
<body>

<header>
  <div class="logo">tinySQL <span>Migrate</span></div>
  <nav>
    <button onclick="showModal('importFileModal')">📂 Import File</button>
    <button onclick="showModal('connectModal')">🔌 Connect DB</button>
    <button onclick="showModal('importDBModal')">⬇ Import DB</button>
    <button onclick="exportResults('json')">📤 Export JSON</button>
    <button onclick="exportResults('csv')">📤 Export CSV</button>
  </nav>
</header>

<div class="container">
  <div class="sidebar" id="sidebar">
    <h3>Tables</h3>
    <div id="tableList"><div class="empty">No tables loaded</div></div>
    <h3>Connections</h3>
    <div id="connList"><div class="empty">No connections</div></div>
  </div>

  <div class="main-panel">
    <div class="editor-area">
      <textarea id="sqlEditor" placeholder="Enter SQL query... (Ctrl+Enter to run)"
        spellcheck="false">SELECT 'Hello from tinySQL Migrate!' AS message</textarea>
      <div class="editor-bar">
        <button class="btn-run" onclick="runQuery()">▶ Run Query</button>
        <button class="btn-secondary" onclick="clearResults()">Clear</button>
        <span class="status" id="status"></span>
      </div>
    </div>
    <div class="results-area" id="results">
      <div class="message">Run a query to see results here</div>
    </div>
  </div>
</div>

<!-- Import File Modal -->
<div class="modal-overlay" id="importFileModal">
  <div class="modal">
    <h2>📂 Import File</h2>
    <label for="importTable">Table Name (optional)</label>
    <input type="text" id="importTable" placeholder="Auto-detected from filename">
    <div class="upload-zone" id="uploadZone" onclick="document.getElementById('fileInput').click()">
      Drop CSV or JSON file here, or click to browse
      <input type="file" id="fileInput" accept=".csv,.json,.jsonl,.tsv,.txt,.ndjson">
    </div>
    <div class="actions">
      <button class="btn-cancel" onclick="hideModal('importFileModal')">Cancel</button>
      <button class="btn-primary" onclick="uploadFile()">Import</button>
    </div>
  </div>
</div>

<!-- Connect DB Modal -->
<div class="modal-overlay" id="connectModal">
  <div class="modal">
    <h2>🔌 Connect to Database</h2>
    <label for="connName">Connection Name</label>
    <input type="text" id="connName" placeholder="e.g., mydb">
    <label for="connDSN">DSN (Connection String)</label>
    <input type="text" id="connDSN" placeholder="e.g., postgres://user:pass@localhost/db?sslmode=disable">
    <div class="actions">
      <button class="btn-cancel" onclick="hideModal('connectModal')">Cancel</button>
      <button class="btn-primary" onclick="connectDB()">Connect</button>
    </div>
  </div>
</div>

<!-- Import from DB Modal -->
<div class="modal-overlay" id="importDBModal">
  <div class="modal">
    <h2>⬇ Import from Database</h2>
    <label for="impConn">Connection</label>
    <select id="impConn"><option value="">Select connection...</option></select>
    <label for="impQuery">SQL Query or Table Name</label>
    <input type="text" id="impQuery" placeholder="SELECT * FROM users">
    <label for="impTable">Target Table Name in tinySQL</label>
    <input type="text" id="impTable" placeholder="users">
    <div class="actions">
      <button class="btn-cancel" onclick="hideModal('importDBModal')">Cancel</button>
      <button class="btn-primary" onclick="importFromDB()">Import</button>
    </div>
  </div>
</div>

<script>
const editor = document.getElementById('sqlEditor');
const statusEl = document.getElementById('status');
const resultsEl = document.getElementById('results');
let lastSQL = '';

// Keyboard shortcuts
editor.addEventListener('keydown', e => {
  if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
    e.preventDefault();
    runQuery();
  }
  if (e.key === 'Tab') {
    e.preventDefault();
    const s = editor.selectionStart;
    editor.value = editor.value.substring(0, s) + '  ' + editor.value.substring(editor.selectionEnd);
    editor.selectionStart = editor.selectionEnd = s + 2;
  }
});

// Drag and drop
const uploadZone = document.getElementById('uploadZone');
uploadZone.addEventListener('dragover', e => { e.preventDefault(); uploadZone.classList.add('dragover'); });
uploadZone.addEventListener('dragleave', () => uploadZone.classList.remove('dragover'));
uploadZone.addEventListener('drop', e => {
  e.preventDefault();
  uploadZone.classList.remove('dragover');
  if (e.dataTransfer.files.length) {
    document.getElementById('fileInput').files = e.dataTransfer.files;
    uploadZone.textContent = e.dataTransfer.files[0].name;
  }
});
document.getElementById('fileInput').addEventListener('change', function() {
  if (this.files.length) uploadZone.textContent = this.files[0].name;
});

async function runQuery() {
  const sql = editor.value.trim();
  if (!sql) return;
  lastSQL = sql;

  statusEl.textContent = 'Running...';
  statusEl.className = 'status';

  try {
    const res = await fetch('/api/query', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({sql})
    });
    const data = await res.json();

    if (data.error) {
      statusEl.textContent = 'Error';
      statusEl.className = 'status err';
      resultsEl.innerHTML = '<div class="error-msg">' + escapeHtml(data.error) + '</div>';
      return;
    }

    statusEl.textContent = data.rowCount + ' rows · ' + data.duration;
    statusEl.className = 'status ok';

    if (data.rows && data.rows.length > 0) {
      let html = '<table><thead><tr>';
      data.columns.forEach(c => html += '<th>' + escapeHtml(c) + '</th>');
      html += '</tr></thead><tbody>';
      data.rows.forEach(row => {
        html += '<tr>';
        data.columns.forEach(c => {
          const v = row[c];
          html += '<td>' + (v === null || v === undefined ? '<span style="color:var(--text-dim)">NULL</span>' : escapeHtml(String(v))) + '</td>';
        });
        html += '</tr>';
      });
      html += '</tbody></table>';
      resultsEl.innerHTML = html;
    } else if (data.message) {
      resultsEl.innerHTML = '<div class="message">✓ ' + escapeHtml(data.message) + (data.duration ? ' (' + data.duration + ')' : '') + '</div>';
    } else {
      resultsEl.innerHTML = '<div class="message">Query returned no results</div>';
    }

    refreshSidebar();
  } catch (err) {
    statusEl.textContent = 'Network error';
    statusEl.className = 'status err';
    resultsEl.innerHTML = '<div class="error-msg">Network error: ' + escapeHtml(err.message) + '</div>';
  }
}

function clearResults() {
  resultsEl.innerHTML = '<div class="message">Run a query to see results here</div>';
  statusEl.textContent = '';
  statusEl.className = 'status';
}

async function refreshSidebar() {
  // Tables
  try {
    const res = await fetch('/api/tables');
    const data = await res.json();
    const list = document.getElementById('tableList');
    if (data.tables && data.tables.length > 0) {
      list.innerHTML = data.tables.map(t =>
        '<div class="table-item" onclick="selectTable(\'' + escapeHtml(t.name) + '\')">' +
        '<span>' + escapeHtml(t.name) + '</span>' +
        '<span class="meta">' + t.rows + ' rows · ' + t.columns + ' cols</span>' +
        '</div>'
      ).join('');
    } else {
      list.innerHTML = '<div class="empty">No tables loaded</div>';
    }
  } catch(e) {}

  // Connections
  try {
    const res = await fetch('/api/connections');
    const data = await res.json();
    const list = document.getElementById('connList');
    if (data.connections && data.connections.length > 0) {
      list.innerHTML = data.connections.map(c =>
        '<div class="conn-item">' +
        '<div><span>' + escapeHtml(c.name) + '</span> <span class="driver">' + escapeHtml(c.driver) + '</span></div>' +
        '<button onclick="disconnectDB(\'' + escapeHtml(c.name) + '\')">✕</button>' +
        '</div>'
      ).join('');
    } else {
      list.innerHTML = '<div class="empty">No connections</div>';
    }

    // Update import DB modal connection dropdown
    const sel = document.getElementById('impConn');
    const current = sel.value;
    sel.innerHTML = '<option value="">Select connection...</option>';
    if (data.connections) {
      data.connections.forEach(c => {
        const opt = document.createElement('option');
        opt.value = c.name;
        opt.textContent = c.name + ' (' + c.driver + ')';
        sel.appendChild(opt);
      });
    }
    sel.value = current;
  } catch(e) {}
}

function selectTable(name) {
  editor.value = 'SELECT * FROM ' + name + ' LIMIT 100';
  runQuery();
}

async function uploadFile() {
  const fileInput = document.getElementById('fileInput');
  if (!fileInput.files.length) {
    alert('Please select a file');
    return;
  }

  const form = new FormData();
  form.append('file', fileInput.files[0]);
  const table = document.getElementById('importTable').value;
  if (table) form.append('table', table);

  try {
    const res = await fetch('/api/import-file', { method: 'POST', body: form });
    const data = await res.json();
    if (data.error) {
      alert('Import error: ' + data.error);
    } else {
      hideModal('importFileModal');
      refreshSidebar();
      statusEl.textContent = data.message;
      statusEl.className = 'status ok';
      // Reset
      fileInput.value = '';
      uploadZone.textContent = 'Drop CSV or JSON file here, or click to browse';
      document.getElementById('importTable').value = '';
    }
  } catch (err) {
    alert('Network error: ' + err.message);
  }
}

async function connectDB() {
  const name = document.getElementById('connName').value.trim();
  const dsn = document.getElementById('connDSN').value.trim();
  if (!name || !dsn) { alert('Name and DSN are required'); return; }

  try {
    const res = await fetch('/api/connect', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({name, dsn})
    });
    const data = await res.json();
    if (data.error) {
      alert('Connection error: ' + data.error);
    } else {
      hideModal('connectModal');
      refreshSidebar();
      statusEl.textContent = data.message;
      statusEl.className = 'status ok';
      document.getElementById('connName').value = '';
      document.getElementById('connDSN').value = '';
    }
  } catch (err) {
    alert('Network error: ' + err.message);
  }
}

async function disconnectDB(name) {
  try {
    await fetch('/api/disconnect', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({name})
    });
    refreshSidebar();
  } catch(e) {}
}

async function importFromDB() {
  const connection = document.getElementById('impConn').value;
  const query = document.getElementById('impQuery').value.trim();
  const table = document.getElementById('impTable').value.trim();
  if (!connection) { alert('Select a connection'); return; }
  if (!query && !table) { alert('Enter a query or table name'); return; }

  try {
    const res = await fetch('/api/import-db', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({connection, query, table})
    });
    const data = await res.json();
    if (data.error) {
      alert('Import error: ' + data.error);
    } else {
      hideModal('importDBModal');
      refreshSidebar();
      statusEl.textContent = data.message;
      statusEl.className = 'status ok';
    }
  } catch (err) {
    alert('Network error: ' + err.message);
  }
}

async function exportResults(format) {
  const sql = lastSQL || editor.value.trim();
  if (!sql) { alert('No query to export'); return; }

  try {
    const res = await fetch('/api/export', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({sql, format})
    });

    if (!res.ok) {
      const data = await res.json();
      alert('Export error: ' + (data.error || 'Unknown'));
      return;
    }

    const blob = await res.blob();
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'export.' + format;
    a.click();
    URL.revokeObjectURL(url);
  } catch (err) {
    alert('Export error: ' + err.message);
  }
}

function showModal(id) {
  document.getElementById(id).classList.add('active');
  if (id === 'importDBModal') refreshSidebar();
}
function hideModal(id) {
  document.getElementById(id).classList.remove('active');
}

// Close modal on overlay click
document.querySelectorAll('.modal-overlay').forEach(el => {
  el.addEventListener('click', e => {
    if (e.target === el) el.classList.remove('active');
  });
});

function escapeHtml(s) {
  const d = document.createElement('div');
  d.textContent = s;
  return d.innerHTML;
}

// Initial load
refreshSidebar();
</script>

</body>
</html>`
