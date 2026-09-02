// Command import-validate is a local CSV intake tool. It validates uploads
// against explicit, reviewable rules before appending them to a TinySQL table;
// rejected imports and per-cell errors remain available in the browser.
package main

import (
	"context"
	"database/sql"
	"embed"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"html/template"
	"io"
	"log"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/SimonWaldherr/tinySQL/cmd/internal/webapp"
)

//go:embed templates/*.html static/*
var assets embed.FS

const (
	maxUploadBytes = 5 << 20
	maxRows        = 10000
)

var identifier = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

type app struct {
	db  *sql.DB
	tpl *template.Template
}

type validationRule struct {
	Column   string   `json:"column"`
	Target   string   `json:"target,omitempty"`
	Required bool     `json:"required,omitempty"`
	Type     string   `json:"type,omitempty"`
	Unique   bool     `json:"unique,omitempty"`
	Min      *float64 `json:"min,omitempty"`
	Max      *float64 `json:"max,omitempty"`
	Pattern  string   `json:"pattern,omitempty"`
}

type importRun struct {
	ID        int64    `json:"id"`
	Filename  string   `json:"filename"`
	Target    string   `json:"target"`
	CreatedAt int64    `json:"created_at"`
	Rows      int      `json:"rows"`
	ValidRows int      `json:"valid_rows"`
	Errors    int      `json:"errors"`
	Status    string   `json:"status"`
	Headers   []string `json:"headers,omitempty"`
}

type importError struct {
	Row     int    `json:"row"`
	Column  string `json:"column"`
	Message string `json:"message"`
	Value   string `json:"value"`
}
type parsedRow struct {
	number int
	values map[string]string
}

func main() {
	addr := flag.String("addr", "127.0.0.1:8093", "HTTP listen address; use :8093 to accept connections from other machines")
	dsn := flag.String("dsn", "file:import-validate.db?autosave=1", "TinySQL DSN")
	flag.Parse()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	db, err := webapp.Open(ctx, *dsn)
	if err != nil {
		log.Fatalf("open database: %v", err)
	}
	defer db.Close()
	tpl, err := template.ParseFS(assets, "templates/*.html")
	if err != nil {
		log.Fatalf("parse templates: %v", err)
	}
	a := &app{db: db, tpl: tpl}
	if err := a.bootstrap(ctx); err != nil {
		log.Fatalf("prepare database: %v", err)
	}
	log.Printf("import-validate listening on %s", *addr)
	log.Fatal(http.ListenAndServe(*addr, a.handler()))
}

func (a *app) bootstrap(ctx context.Context) error {
	return webapp.Apply(ctx, a.db,
		`CREATE TABLE IF NOT EXISTS import_runs (id INT PRIMARY KEY, filename TEXT NOT NULL, target_name TEXT NOT NULL, created_at INT NOT NULL, rows_count INT NOT NULL, valid_rows INT NOT NULL, error_count INT NOT NULL, status TEXT NOT NULL, headers_json TEXT NOT NULL, rules_json TEXT NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS import_rows (id INT PRIMARY KEY, import_id INT NOT NULL, row_number INT NOT NULL, data_json TEXT NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS import_errors (id INT PRIMARY KEY, import_id INT NOT NULL, row_number INT NOT NULL, column_name TEXT NOT NULL, message TEXT NOT NULL, value TEXT)`)
}

func (a *app) routes(mux *http.ServeMux) {
	mux.HandleFunc("GET /", a.index)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		webapp.JSON(w, http.StatusOK, map[string]bool{"ok": true})
	})
	mux.HandleFunc("POST /api/validate", a.validate)
	mux.HandleFunc("GET /api/imports", a.listRuns)
	mux.HandleFunc("GET /api/imports/{id}", a.getRun)
	mux.HandleFunc("POST /api/imports/{id}/commit", a.commitRun)
}

func (a *app) index(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := a.tpl.ExecuteTemplate(w, "index.html", nil); err != nil {
		http.Error(w, "template error", 500)
	}
}

func (a *app) validate(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxUploadBytes)
	if err := r.ParseMultipartForm(maxUploadBytes); err != nil {
		webapp.Error(w, r, http.StatusBadRequest, "upload must be a CSV smaller than 5 MiB")
		return
	}
	target := strings.TrimSpace(r.FormValue("target"))
	rulesRaw := strings.TrimSpace(r.FormValue("rules"))
	file, hdr, err := r.FormFile("file")
	if err != nil {
		webapp.Error(w, r, http.StatusBadRequest, "CSV file is required")
		return
	}
	defer file.Close()
	run, errs, err := a.validateCSV(r.Context(), hdr.Filename, target, rulesRaw, file)
	if err != nil {
		webapp.Error(w, r, http.StatusBadRequest, err.Error())
		return
	}
	webapp.JSON(w, http.StatusCreated, map[string]any{"run": run, "errors": errs})
}

func (a *app) listRuns(w http.ResponseWriter, r *http.Request) {
	runs, err := a.runs(r.Context())
	if err != nil {
		webapp.Error(w, r, 500, "could not load imports")
		return
	}
	webapp.JSON(w, 200, runs)
}
func (a *app) getRun(w http.ResponseWriter, r *http.Request) {
	run, errs, err := a.runDetail(r.Context(), r.PathValue("id"))
	if errors.Is(err, sql.ErrNoRows) {
		webapp.Error(w, r, 404, "import not found")
		return
	}
	if err != nil {
		webapp.Error(w, r, 500, "could not load import")
		return
	}
	webapp.JSON(w, 200, map[string]any{"run": run, "errors": errs})
}
func (a *app) commitRun(w http.ResponseWriter, r *http.Request) {
	run, err := a.commit(r.Context(), r.PathValue("id"))
	if errors.Is(err, errHasErrors) {
		webapp.Error(w, r, 409, err.Error())
		return
	}
	if errors.Is(err, sql.ErrNoRows) {
		webapp.Error(w, r, 404, "import not found")
		return
	}
	if err != nil {
		webapp.Error(w, r, 400, err.Error())
		return
	}
	webapp.JSON(w, 200, run)
}

func parseRules(raw string) ([]validationRule, error) {
	if raw == "" {
		return nil, nil
	}
	var rules []validationRule
	if err := json.Unmarshal([]byte(raw), &rules); err != nil {
		return nil, errors.New("rules must be a JSON array")
	}
	seen := map[string]bool{}
	for i := range rules {
		r := &rules[i]
		r.Column = strings.TrimSpace(r.Column)
		r.Target = strings.TrimSpace(r.Target)
		r.Type = strings.ToLower(strings.TrimSpace(r.Type))
		if r.Column == "" {
			return nil, fmt.Errorf("rule %d needs a column", i+1)
		}
		if seen[r.Column] {
			return nil, fmt.Errorf("duplicate rule for %q", r.Column)
		}
		seen[r.Column] = true
		if r.Target != "" && !identifier.MatchString(r.Target) {
			return nil, fmt.Errorf("invalid target name %q", r.Target)
		}
		switch r.Type {
		case "", "text", "email", "integer", "number", "date":
		default:
			return nil, fmt.Errorf("unsupported type %q", r.Type)
		}
		if r.Pattern != "" {
			if _, err := regexp.Compile(r.Pattern); err != nil {
				return nil, fmt.Errorf("invalid pattern for %s", r.Column)
			}
		}
		if r.Min != nil && r.Max != nil && *r.Min > *r.Max {
			return nil, fmt.Errorf("min exceeds max for %s", r.Column)
		}
	}
	return rules, nil
}

func (a *app) validateCSV(ctx context.Context, filename, target, rulesRaw string, input io.Reader) (importRun, []importError, error) {
	target = strings.TrimSpace(target)
	if !identifier.MatchString(target) {
		return importRun{}, nil, errors.New("target table must contain only letters, digits and underscores")
	}
	rules, err := parseRules(rulesRaw)
	if err != nil {
		return importRun{}, nil, err
	}
	reader := csv.NewReader(input)
	reader.FieldsPerRecord = -1
	header, err := reader.Read()
	if err != nil {
		return importRun{}, nil, errors.New("CSV needs a header row")
	}
	if len(header) == 0 {
		return importRun{}, nil, errors.New("CSV needs at least one column")
	}
	headers := make([]string, len(header))
	headerSet := map[string]bool{}
	for i, h := range header {
		h = strings.TrimSpace(h)
		if !identifier.MatchString(h) || reservedColumn(h) {
			return importRun{}, nil, fmt.Errorf("invalid or reserved CSV column %q", h)
		}
		if headerSet[h] {
			return importRun{}, nil, fmt.Errorf("duplicate CSV column %q", h)
		}
		headers[i] = h
		headerSet[h] = true
	}
	for _, rule := range rules {
		if !headerSet[rule.Column] {
			return importRun{}, nil, fmt.Errorf("rule column %q is missing", rule.Column)
		}
	}
	targetHeaders, err := mappedHeaders(headers, rules)
	if err != nil {
		return importRun{}, nil, err
	}
	rows := make([]parsedRow, 0)
	errs := make([]importError, 0)
	unique := map[string]map[string]int{}
	for n := 2; ; n++ {
		record, readErr := reader.Read()
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return importRun{}, nil, fmt.Errorf("CSV row %d: %w", n, readErr)
		}
		if len(rows) >= maxRows {
			return importRun{}, nil, fmt.Errorf("CSV exceeds %d data rows", maxRows)
		}
		if len(record) != len(headers) {
			return importRun{}, nil, fmt.Errorf("CSV row %d has %d fields, expected %d", n, len(record), len(headers))
		}
		values := map[string]string{}
		for i, v := range record {
			values[headers[i]] = strings.TrimSpace(v)
		}
		rowErrs := validateRow(n, values, rules, unique)
		errs = append(errs, rowErrs...)
		mapped := map[string]string{}
		for i, h := range headers {
			mapped[targetHeaders[i]] = values[h]
		}
		rows = append(rows, parsedRow{number: n, values: mapped})
	}
	if len(rows) == 0 {
		return importRun{}, nil, errors.New("CSV has no data rows")
	}
	headersJSON, _ := json.Marshal(targetHeaders)
	rulesJSON, _ := json.Marshal(rules)
	run := importRun{Filename: filename, Target: target, CreatedAt: time.Now().UTC().Unix(), Rows: len(rows), ValidRows: len(rows), Errors: len(errs), Status: "validated", Headers: targetHeaders}
	if len(errs) > 0 {
		run.ValidRows = 0
		run.Status = "rejected"
	}
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return importRun{}, nil, err
	}
	defer tx.Rollback()
	run.ID, err = webapp.NextID(ctx, tx, "import_runs")
	if err != nil {
		return importRun{}, nil, err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO import_runs (id,filename,target_name,created_at,rows_count,valid_rows,error_count,status,headers_json,rules_json) VALUES (?,?,?,?,?,?,?,?,?,?)`, run.ID, run.Filename, run.Target, run.CreatedAt, run.Rows, run.ValidRows, run.Errors, run.Status, string(headersJSON), string(rulesJSON)); err != nil {
		return importRun{}, nil, err
	}
	rowID, err := webapp.NextID(ctx, tx, "import_rows")
	if err != nil {
		return importRun{}, nil, err
	}
	for _, row := range rows {
		data, _ := json.Marshal(row.values)
		if _, err = tx.ExecContext(ctx, "INSERT INTO import_rows (id,import_id,row_number,data_json) VALUES (?,?,?,?)", rowID, run.ID, row.number, string(data)); err != nil {
			return importRun{}, nil, err
		}
		rowID++
	}
	errID, err := webapp.NextID(ctx, tx, "import_errors")
	if err != nil {
		return importRun{}, nil, err
	}
	for _, issue := range errs {
		if _, err = tx.ExecContext(ctx, "INSERT INTO import_errors (id,import_id,row_number,column_name,message,value) VALUES (?,?,?,?,?,?)", errID, run.ID, issue.Row, issue.Column, issue.Message, issue.Value); err != nil {
			return importRun{}, nil, err
		}
		errID++
	}
	if err = tx.Commit(); err != nil {
		return importRun{}, nil, err
	}
	return run, errs, nil
}

func validateRow(number int, values map[string]string, rules []validationRule, unique map[string]map[string]int) []importError {
	issues := []importError{}
	for _, r := range rules {
		value := values[r.Column]
		if r.Required && value == "" {
			issues = append(issues, importError{number, r.Column, "required value is missing", value})
			continue
		}
		if value == "" {
			continue
		}
		if r.Unique {
			if unique[r.Column] == nil {
				unique[r.Column] = map[string]int{}
			}
			if first, ok := unique[r.Column][strings.ToLower(value)]; ok {
				issues = append(issues, importError{number, r.Column, fmt.Sprintf("duplicate of row %d", first), value})
			} else {
				unique[r.Column][strings.ToLower(value)] = number
			}
		}
		if msg := checkValue(value, r); msg != "" {
			issues = append(issues, importError{number, r.Column, msg, value})
		}
	}
	return issues
}
func checkValue(value string, r validationRule) string {
	switch r.Type {
	case "email":
		if !strings.Contains(value, "@") || strings.HasPrefix(value, "@") || strings.HasSuffix(value, "@") {
			return "must be an email address"
		}
	case "integer":
		n, e := strconv.ParseInt(value, 10, 64)
		if e != nil {
			return "must be an integer"
		}
		if r.Min != nil && float64(n) < *r.Min {
			return "is below minimum"
		}
		if r.Max != nil && float64(n) > *r.Max {
			return "is above maximum"
		}
	case "number":
		n, e := strconv.ParseFloat(value, 64)
		if e != nil {
			return "must be a number"
		}
		if r.Min != nil && n < *r.Min {
			return "is below minimum"
		}
		if r.Max != nil && n > *r.Max {
			return "is above maximum"
		}
	case "date":
		if _, e := time.Parse("2006-01-02", value); e != nil {
			if _, e = time.Parse(time.RFC3339, value); e != nil {
				return "must be an ISO date"
			}
		}
	}
	if r.Pattern != "" {
		re := regexp.MustCompile(r.Pattern)
		if !re.MatchString(value) {
			return "does not match pattern"
		}
	}
	return ""
}
func mappedHeaders(headers []string, rules []validationRule) ([]string, error) {
	mapping := map[string]string{}
	for _, r := range rules {
		if r.Target != "" {
			mapping[r.Column] = r.Target
		}
	}
	seen := map[string]bool{}
	out := make([]string, len(headers))
	for i, h := range headers {
		name := h
		if mapping[h] != "" {
			name = mapping[h]
		}
		if seen[name] {
			return nil, fmt.Errorf("mapped target column %q occurs twice", name)
		}
		seen[name] = true
		out[i] = name
	}
	return out, nil
}
func reservedColumn(name string) bool {
	switch strings.ToLower(name) {
	case "id", "source_import_id", "source_row":
		return true
	}
	return false
}

func (a *app) runs(ctx context.Context) ([]importRun, error) {
	rows, err := a.db.QueryContext(ctx, "SELECT id,filename,target_name,created_at,rows_count,valid_rows,error_count,status FROM import_runs ORDER BY created_at DESC LIMIT 50")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	runs := []importRun{}
	for rows.Next() {
		var x importRun
		if err := rows.Scan(&x.ID, &x.Filename, &x.Target, &x.CreatedAt, &x.Rows, &x.ValidRows, &x.Errors, &x.Status); err != nil {
			return nil, err
		}
		runs = append(runs, x)
	}
	return runs, rows.Err()
}
func (a *app) runDetail(ctx context.Context, id string) (importRun, []importError, error) {
	var x importRun
	var headers string
	parsedID, err := strconv.ParseInt(strings.TrimSpace(id), 10, 64)
	if err != nil || parsedID < 1 {
		return x, nil, errors.New("invalid import id")
	}
	err = a.db.QueryRowContext(ctx, "SELECT id,filename,target_name,created_at,rows_count,valid_rows,error_count,status,headers_json FROM import_runs WHERE id=?", parsedID).Scan(&x.ID, &x.Filename, &x.Target, &x.CreatedAt, &x.Rows, &x.ValidRows, &x.Errors, &x.Status, &headers)
	if err != nil {
		return x, nil, err
	}
	_ = json.Unmarshal([]byte(headers), &x.Headers)
	rows, err := a.db.QueryContext(ctx, "SELECT row_number,column_name,message,value FROM import_errors WHERE import_id=? ORDER BY row_number,column_name LIMIT 200", x.ID)
	if err != nil {
		return x, nil, err
	}
	defer rows.Close()
	issues := []importError{}
	for rows.Next() {
		var e importError
		if err := rows.Scan(&e.Row, &e.Column, &e.Message, &e.Value); err != nil {
			return x, nil, err
		}
		issues = append(issues, e)
	}
	return x, issues, rows.Err()
}

var errHasErrors = errors.New("only error-free imports can be committed")

func (a *app) commit(ctx context.Context, id string) (importRun, error) {
	run, issues, err := a.runDetail(ctx, id)
	if err != nil {
		return run, err
	}
	if len(issues) > 0 || run.Status == "rejected" {
		return run, errHasErrors
	}
	if run.Status == "committed" {
		return run, nil
	}
	if !identifier.MatchString(run.Target) {
		return run, errors.New("invalid stored target table")
	}
	if len(run.Headers) == 0 {
		return run, errors.New("import has no columns")
	}
	parts := []string{"id INT PRIMARY KEY", "source_import_id INT NOT NULL", "source_row INT NOT NULL"}
	for _, h := range run.Headers {
		if !identifier.MatchString(h) || reservedColumn(h) {
			return run, errors.New("invalid stored column")
		}
		parts = append(parts, h+" TEXT")
	}
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return run, err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS "+run.Target+" ("+strings.Join(parts, ",")+")"); err != nil {
		return run, err
	}
	next, err := webapp.NextID(ctx, tx, run.Target)
	if err != nil {
		return run, err
	}
	rows, err := tx.QueryContext(ctx, "SELECT row_number,data_json FROM import_rows WHERE import_id=? ORDER BY row_number", run.ID)
	if err != nil {
		return run, err
	}
	defer rows.Close()
	fields := append([]string{"id", "source_import_id", "source_row"}, run.Headers...)
	marks := make([]string, len(fields))
	for i := range marks {
		marks[i] = "?"
	}
	for rows.Next() {
		var n int
		var raw string
		if err := rows.Scan(&n, &raw); err != nil {
			return run, err
		}
		values := map[string]string{}
		if err := json.Unmarshal([]byte(raw), &values); err != nil {
			return run, err
		}
		args := []any{next, run.ID, n}
		for _, h := range run.Headers {
			args = append(args, values[h])
		}
		if _, err := tx.ExecContext(ctx, "INSERT INTO "+run.Target+" ("+strings.Join(fields, ",")+") VALUES ("+strings.Join(marks, ",")+")", args...); err != nil {
			return run, err
		}
		next++
	}
	if err := rows.Err(); err != nil {
		return run, err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE import_runs SET status='committed' WHERE id=?", run.ID); err != nil {
		return run, err
	}
	if err = tx.Commit(); err != nil {
		return run, err
	}
	run.Status = "committed"
	return run, nil
}

// stableIssues makes unit tests and API output stable if rules evolve.
func stableIssues(issues []importError) []importError {
	sort.Slice(issues, func(i, j int) bool {
		if issues[i].Row == issues[j].Row {
			return issues[i].Column < issues[j].Column
		}
		return issues[i].Row < issues[j].Row
	})
	return issues
}

// handler assembles the complete HTTP stack: the application routes, the
// stylesheet shared with the other cmd/ applications, this application's own
// static files and the security headers. main and the tests both go through
// it, so a test always exercises exactly what the binary serves.
func (a *app) handler() http.Handler {
	mux := http.NewServeMux()
	a.routes(mux)
	webapp.MountShared(mux)
	mux.Handle("GET /static/", http.FileServer(http.FS(assets)))
	return webapp.SecurityHeaders(mux)
}
