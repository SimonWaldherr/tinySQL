// Command worklog is a compact, persistent work-time tracker. It follows an
// event-based model: Work, Break and Clocked out are immutable entries. A
// timeout worker creates a critical system checkout after a configurable time
// without any status change, which works across midnight and night shifts.
package main

import (
	"context"
	"database/sql"
	"embed"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"html/template"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/SimonWaldherr/tinySQL/cmd/internal/webapp"
)

//go:embed templates/*.html static/*
var assets embed.FS

const (
	workTypeName  = "Work"
	breakTypeName = "Break"
	offTypeName   = "Clocked out"
)

type app struct {
	db       *sql.DB
	tpl      *template.Template
	timeout  time.Duration
	location *time.Location
}

type department struct {
	ID   int64  `json:"id"`
	Name string `json:"name"`
}
type user struct {
	ID           int64  `json:"id"`
	StampKey     string `json:"stampkey"`
	Name         string `json:"name"`
	Email        string `json:"email"`
	Role         string `json:"role"`
	Position     string `json:"position"`
	DepartmentID int64  `json:"department_id"`
	Department   string `json:"department"`
}
type activityType struct {
	ID              int64  `json:"id"`
	Name            string `json:"name"`
	CountsWork      bool   `json:"counts_work"`
	KeepsAttendance bool   `json:"keeps_attendance"`
	Comment         string `json:"comment"`
}
type entry struct {
	ID               int64  `json:"id"`
	UserID           int64  `json:"user_id"`
	UserName         string `json:"user_name"`
	OccurredAt       int64  `json:"occurred_at"`
	TypeID           int64  `json:"type_id"`
	TypeName         string `json:"type_name"`
	CountsWork       bool   `json:"counts_work"`
	KeepsAttendance  bool   `json:"keeps_attendance"`
	Comment          string `json:"comment"`
	Source           string `json:"source"`
	Critical         bool   `json:"critical"`
	CriticalReason   string `json:"critical_reason"`
	CriticalResolved bool   `json:"critical_resolved"`
	ReviewNote       string `json:"review_note"`
}
type status struct {
	UserID          int64  `json:"user_id"`
	UserName        string `json:"user_name"`
	Department      string `json:"department"`
	TypeID          int64  `json:"type_id"`
	TypeName        string `json:"type_name"`
	KeepsAttendance bool   `json:"keeps_attendance"`
	OccurredAt      int64  `json:"occurred_at"`
	Critical        bool   `json:"critical"`
}
type dayTotal struct {
	Date  string  `json:"date"`
	Hours float64 `json:"hours"`
}

type stampInput struct {
	UserID  int64  `json:"user_id"`
	TypeID  int64  `json:"type_id"`
	Comment string `json:"comment"`
}
type userInput struct {
	Name         string `json:"name"`
	Email        string `json:"email"`
	StampKey     string `json:"stampkey"`
	Role         string `json:"role"`
	Position     string `json:"position"`
	DepartmentID int64  `json:"department_id"`
}
type departmentInput struct {
	Name string `json:"name"`
}
type resolveInput struct {
	Note string `json:"note"`
}

func main() {
	addr := flag.String("addr", ":8094", "HTTP listen address")
	dsn := flag.String("dsn", "file:worklog.db?autosave=1", "TinySQL DSN")
	timeout := flag.Duration("timeout", 10*time.Hour, "Automatic checkout after this period without a status change")
	sweep := flag.Duration("sweep", 5*time.Minute, "How often to check for overdue active status")
	tz := flag.String("timezone", "Europe/Berlin", "IANA timezone for daily reports")
	flag.Parse()
	if *timeout <= 0 || *sweep <= 0 {
		log.Fatal("-timeout and -sweep must be positive")
	}
	location, err := time.LoadLocation(*tz)
	if err != nil {
		log.Fatalf("load timezone: %v", err)
	}
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
	a := &app{db: db, tpl: tpl, timeout: *timeout, location: location}
	if err := a.bootstrap(ctx); err != nil {
		log.Fatalf("prepare database: %v", err)
	}
	if _, err := a.autoCheckout(ctx, time.Now().UTC()); err != nil {
		log.Fatalf("initial timeout check: %v", err)
	}
	go a.timeoutLoop(context.Background(), *sweep)
	mux := http.NewServeMux()
	a.routes(mux)
	mux.Handle("GET /static/", http.FileServer(http.FS(assets)))
	log.Printf("worklog listening on %s (timeout: %s, timezone: %s)", *addr, *timeout, *tz)
	log.Fatal(http.ListenAndServe(*addr, webapp.SecurityHeaders(mux)))
}

func (a *app) bootstrap(ctx context.Context) error {
	if err := webapp.Apply(ctx, a.db,
		`CREATE TABLE IF NOT EXISTS departments (id INT PRIMARY KEY, name TEXT NOT NULL UNIQUE)`,
		`CREATE TABLE IF NOT EXISTS users (id INT PRIMARY KEY, stampkey TEXT NOT NULL UNIQUE, name TEXT NOT NULL, email TEXT NOT NULL UNIQUE, role TEXT NOT NULL, position TEXT, department_id INT)`,
		`CREATE TABLE IF NOT EXISTS activity_types (id INT PRIMARY KEY, name TEXT NOT NULL UNIQUE, counts_work BOOL NOT NULL, keeps_attendance BOOL NOT NULL, comment TEXT)`,
		`CREATE TABLE IF NOT EXISTS entries (id INT PRIMARY KEY, occurred_at INT NOT NULL, type_id INT NOT NULL, user_id INT NOT NULL, comment TEXT, source TEXT NOT NULL, critical BOOL NOT NULL, critical_reason TEXT, critical_resolved BOOL NOT NULL, review_note TEXT)`); err != nil {
		return err
	}
	var n int
	if err := a.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM activity_types").Scan(&n); err != nil {
		return err
	}
	if n == 0 {
		if _, err := a.db.ExecContext(ctx, `INSERT INTO activity_types (id,name,counts_work,keeps_attendance,comment) VALUES (1,'Work',TRUE,TRUE,'Working time'),(2,'Break',FALSE,TRUE,'Pause / break'),(3,'Clocked out',FALSE,FALSE,'End of attendance')`); err != nil {
			return err
		}
	}
	if err := a.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM departments").Scan(&n); err != nil {
		return err
	}
	if n == 0 {
		if _, err := a.db.ExecContext(ctx, `INSERT INTO departments (id,name) VALUES (1,'Operations'),(2,'Engineering')`); err != nil {
			return err
		}
	}
	if err := a.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM users").Scan(&n); err != nil {
		return err
	}
	if n == 0 {
		_, err := a.db.ExecContext(ctx, `INSERT INTO users (id,stampkey,name,email,role,position,department_id) VALUES (1,'anna','Anna Beispiel','anna@example.test','admin','Operations lead',1),(2,'ben','Ben Beispiel','ben@example.test','user','Developer',2)`)
		return err
	}
	return nil
}

func (a *app) routes(mux *http.ServeMux) {
	mux.HandleFunc("GET /", a.index)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { webapp.JSON(w, 200, map[string]bool{"ok": true}) })
	mux.HandleFunc("GET /api/dashboard", a.dashboard)
	mux.HandleFunc("POST /api/stamp", a.stamp)
	mux.HandleFunc("POST /api/departments", a.createDepartment)
	mux.HandleFunc("POST /api/users", a.createUser)
	mux.HandleFunc("POST /api/entries/{id}/resolve", a.resolveCritical)
	mux.HandleFunc("GET /api/export", a.exportEntries)
}
func (a *app) index(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := a.tpl.ExecuteTemplate(w, "index.html", map[string]string{"Timeout": a.timeout.String()}); err != nil {
		http.Error(w, "template error", 500)
	}
}

func (a *app) dashboard(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := webapp.ContextWithTimeout(r.Context())
	defer cancel()
	if _, err := a.autoCheckout(ctx, time.Now().UTC()); err != nil {
		webapp.Error(w, r, 500, "timeout check failed")
		return
	}
	users, err := a.users(ctx)
	if err != nil {
		webapp.Error(w, r, 500, "could not load users")
		return
	}
	types, err := a.activityTypes(ctx)
	if err != nil {
		webapp.Error(w, r, 500, "could not load activities")
		return
	}
	departments, err := a.departments(ctx)
	if err != nil {
		webapp.Error(w, r, 500, "could not load departments")
		return
	}
	selected := parsePositive(r.URL.Query().Get("user_id"))
	if selected == 0 && len(users) > 0 {
		selected = users[0].ID
	}
	team, err := a.teamStatus(ctx)
	if err != nil {
		webapp.Error(w, r, 500, "could not load status")
		return
	}
	history, err := a.entriesForUser(ctx, selected, 100)
	if err != nil {
		webapp.Error(w, r, 500, "could not load entries")
		return
	}
	reports, err := a.report(ctx, selected, time.Now())
	if err != nil {
		webapp.Error(w, r, 500, "could not calculate hours")
		return
	}
	critical, err := a.criticalEntries(ctx)
	if err != nil {
		webapp.Error(w, r, 500, "could not load critical entries")
		return
	}
	webapp.JSON(w, 200, map[string]any{"users": users, "types": types, "departments": departments, "selected_user_id": selected, "team": team, "history": history, "report": reports, "critical": critical, "timeout_seconds": int64(a.timeout.Seconds())})
}

func (a *app) stamp(w http.ResponseWriter, r *http.Request) {
	var in stampInput
	if err := decodeJSON(w, r, &in); err != nil {
		webapp.Error(w, r, 400, err.Error())
		return
	}
	e, err := a.createStamp(r.Context(), in, time.Now().UTC())
	if errors.Is(err, errInvalidTransition) {
		webapp.Error(w, r, 409, err.Error())
		return
	}
	if errors.Is(err, sql.ErrNoRows) {
		webapp.Error(w, r, 404, "user or activity not found")
		return
	}
	if err != nil {
		webapp.Error(w, r, 400, err.Error())
		return
	}
	webapp.JSON(w, 201, e)
}
func (a *app) createDepartment(w http.ResponseWriter, r *http.Request) {
	var in departmentInput
	if err := decodeJSON(w, r, &in); err != nil {
		webapp.Error(w, r, 400, err.Error())
		return
	}
	in.Name = strings.TrimSpace(in.Name)
	if in.Name == "" {
		webapp.Error(w, r, 400, "name is required")
		return
	}
	id, err := webapp.NextID(r.Context(), a.db, "departments")
	if err == nil {
		_, err = a.db.ExecContext(r.Context(), "INSERT INTO departments (id,name) VALUES (?,?)", id, in.Name)
	}
	if err != nil {
		webapp.Error(w, r, 409, "department could not be created")
		return
	}
	webapp.JSON(w, 201, department{ID: id, Name: in.Name})
}
func (a *app) createUser(w http.ResponseWriter, r *http.Request) {
	var in userInput
	if err := decodeJSON(w, r, &in); err != nil {
		webapp.Error(w, r, 400, err.Error())
		return
	}
	in.Name = strings.TrimSpace(in.Name)
	in.Email = strings.TrimSpace(in.Email)
	in.StampKey = strings.TrimSpace(in.StampKey)
	in.Role = strings.TrimSpace(in.Role)
	if in.Name == "" || in.Email == "" || in.StampKey == "" {
		webapp.Error(w, r, 400, "name, email and stamp key are required")
		return
	}
	if in.Role == "" {
		in.Role = "user"
	}
	id, err := webapp.NextID(r.Context(), a.db, "users")
	if err == nil {
		_, err = a.db.ExecContext(r.Context(), `INSERT INTO users (id,stampkey,name,email,role,position,department_id) VALUES (?,?,?,?,?,?,?)`, id, in.StampKey, in.Name, in.Email, in.Role, strings.TrimSpace(in.Position), in.DepartmentID)
	}
	if err != nil {
		webapp.Error(w, r, 409, "user could not be created")
		return
	}
	webapp.JSON(w, 201, map[string]any{"id": id})
}
func (a *app) resolveCritical(w http.ResponseWriter, r *http.Request) {
	var in resolveInput
	if err := decodeJSON(w, r, &in); err != nil {
		webapp.Error(w, r, 400, err.Error())
		return
	}
	id := parsePositive(r.PathValue("id"))
	if id == 0 {
		webapp.Error(w, r, 400, "invalid entry id")
		return
	}
	result, err := a.db.ExecContext(r.Context(), "UPDATE entries SET critical_resolved=TRUE, review_note=? WHERE id=? AND critical=TRUE", strings.TrimSpace(in.Note), id)
	if err != nil {
		webapp.Error(w, r, 500, "entry could not be resolved")
		return
	}
	changed, _ := result.RowsAffected()
	if changed == 0 {
		webapp.Error(w, r, 404, "critical entry not found")
		return
	}
	webapp.JSON(w, 200, map[string]bool{"ok": true})
}

func (a *app) exportEntries(w http.ResponseWriter, r *http.Request) {
	userID := parsePositive(r.URL.Query().Get("user_id"))
	if userID == 0 {
		webapp.Error(w, r, 400, "user_id is required")
		return
	}
	items, err := a.entriesForUser(r.Context(), userID, 10000)
	if err != nil {
		webapp.Error(w, r, 500, "export failed")
		return
	}
	w.Header().Set("Content-Type", "text/csv; charset=utf-8")
	w.Header().Set("Content-Disposition", `attachment; filename="worklog-entries.csv"`)
	out := csv.NewWriter(w)
	_ = out.Write([]string{"occurred_at", "type", "comment", "source", "critical", "critical_reason"})
	for _, e := range items {
		_ = out.Write([]string{time.Unix(e.OccurredAt, 0).In(a.location).Format(time.RFC3339), e.TypeName, e.Comment, e.Source, strconv.FormatBool(e.Critical), e.CriticalReason})
	}
	out.Flush()
}

var errInvalidTransition = errors.New("this status change is not valid for the current status")

func (a *app) createStamp(ctx context.Context, in stampInput, when time.Time) (entry, error) {
	if in.UserID < 1 || in.TypeID < 1 {
		return entry{}, errors.New("user_id and type_id are required")
	}
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return entry{}, err
	}
	defer tx.Rollback()
	var userExists int
	if err := tx.QueryRowContext(ctx, "SELECT id FROM users WHERE id=?", in.UserID).Scan(&userExists); err != nil {
		return entry{}, err
	}
	typ, err := activityTypeByID(ctx, tx, in.TypeID)
	if err != nil {
		return entry{}, err
	}
	previous, err := currentStatus(ctx, tx, in.UserID)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return entry{}, err
	}
	if errors.Is(err, sql.ErrNoRows) {
		if typ.Name == breakTypeName || typ.Name == offTypeName {
			return entry{}, errInvalidTransition
		}
	} else {
		if previous.TypeID == typ.ID || (!previous.KeepsAttendance && typ.Name == offTypeName) {
			return entry{}, errInvalidTransition
		}
	}
	id, err := webapp.NextID(ctx, tx, "entries")
	if err != nil {
		return entry{}, err
	}
	comment := strings.TrimSpace(in.Comment)
	if _, err = tx.ExecContext(ctx, `INSERT INTO entries (id,occurred_at,type_id,user_id,comment,source,critical,critical_reason,critical_resolved,review_note) VALUES (?,?,?,?,?,'manual',FALSE,'',FALSE,'')`, id, when.UTC().Unix(), typ.ID, in.UserID, comment); err != nil {
		return entry{}, err
	}
	if err = tx.Commit(); err != nil {
		return entry{}, err
	}
	return entry{ID: id, UserID: in.UserID, OccurredAt: when.UTC().Unix(), TypeID: typ.ID, TypeName: typ.Name, CountsWork: typ.CountsWork, KeepsAttendance: typ.KeepsAttendance, Comment: comment, Source: "manual"}, nil
}

func (a *app) timeoutLoop(ctx context.Context, every time.Duration) {
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			if count, err := a.autoCheckout(ctx, now.UTC()); err != nil {
				log.Printf("worklog timeout check: %v", err)
			} else if count > 0 {
				log.Printf("worklog: automatically checked out %d overdue status(es)", count)
			}
		}
	}
}
func (a *app) autoCheckout(ctx context.Context, now time.Time) (int, error) {
	team, err := a.teamStatus(ctx)
	if err != nil {
		return 0, err
	}
	off, err := a.activityTypeByName(ctx, a.db, offTypeName)
	if err != nil {
		return 0, err
	}
	count := 0
	for _, s := range team {
		if !s.KeepsAttendance || now.Unix()-s.OccurredAt < int64(a.timeout.Seconds()) {
			continue
		}
		tx, err := a.db.BeginTx(ctx, nil)
		if err != nil {
			return count, err
		}
		current, err := currentStatus(ctx, tx, s.UserID)
		if err != nil {
			tx.Rollback()
			return count, err
		}
		if !current.KeepsAttendance || now.Unix()-current.OccurredAt < int64(a.timeout.Seconds()) {
			tx.Rollback()
			continue
		}
		id, err := webapp.NextID(ctx, tx, "entries")
		if err == nil {
			deadline := current.OccurredAt + int64(a.timeout.Seconds())
			_, err = tx.ExecContext(ctx, `INSERT INTO entries (id,occurred_at,type_id,user_id,comment,source,critical,critical_reason,critical_resolved,review_note) VALUES (?,?,?,?,?,'system',TRUE,'timeout_after_inactivity',FALSE,'')`, id, deadline, off.ID, s.UserID, "Automatic checkout after "+a.timeout.String())
		}
		if err != nil {
			tx.Rollback()
			return count, err
		}
		if err = tx.Commit(); err != nil {
			return count, err
		}
		count++
	}
	return count, nil
}

type queryOne interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func activityTypeByID(ctx context.Context, q queryOne, id int64) (activityType, error) {
	var t activityType
	err := q.QueryRowContext(ctx, "SELECT id,name,counts_work,keeps_attendance,comment FROM activity_types WHERE id=?", id).Scan(&t.ID, &t.Name, &t.CountsWork, &t.KeepsAttendance, &t.Comment)
	return t, err
}
func (a *app) activityTypeByName(ctx context.Context, q queryOne, name string) (activityType, error) {
	var t activityType
	err := q.QueryRowContext(ctx, "SELECT id,name,counts_work,keeps_attendance,comment FROM activity_types WHERE name=?", name).Scan(&t.ID, &t.Name, &t.CountsWork, &t.KeepsAttendance, &t.Comment)
	return t, err
}
func currentStatus(ctx context.Context, q queryOne, userID int64) (status, error) {
	var s status
	err := q.QueryRowContext(ctx, `SELECT e.user_id,u.name,COALESCE(d.name,''),e.type_id,t.name,t.keeps_attendance,e.occurred_at,e.critical FROM entries e JOIN users u ON u.id=e.user_id LEFT JOIN departments d ON d.id=u.department_id JOIN activity_types t ON t.id=e.type_id WHERE e.user_id=? ORDER BY e.occurred_at DESC,e.id DESC LIMIT 1`, userID).Scan(&s.UserID, &s.UserName, &s.Department, &s.TypeID, &s.TypeName, &s.KeepsAttendance, &s.OccurredAt, &s.Critical)
	return s, err
}

func (a *app) users(ctx context.Context) ([]user, error) {
	rows, err := a.db.QueryContext(ctx, `SELECT u.id,u.stampkey,u.name,u.email,u.role,COALESCE(u.position,''),u.department_id,COALESCE(d.name,'') FROM users u LEFT JOIN departments d ON d.id=u.department_id ORDER BY u.name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []user{}
	for rows.Next() {
		var u user
		if err := rows.Scan(&u.ID, &u.StampKey, &u.Name, &u.Email, &u.Role, &u.Position, &u.DepartmentID, &u.Department); err != nil {
			return nil, err
		}
		items = append(items, u)
	}
	return items, rows.Err()
}
func (a *app) departments(ctx context.Context) ([]department, error) {
	rows, err := a.db.QueryContext(ctx, "SELECT id,name FROM departments ORDER BY name")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []department{}
	for rows.Next() {
		var d department
		if err := rows.Scan(&d.ID, &d.Name); err != nil {
			return nil, err
		}
		items = append(items, d)
	}
	return items, rows.Err()
}
func (a *app) activityTypes(ctx context.Context) ([]activityType, error) {
	rows, err := a.db.QueryContext(ctx, "SELECT id,name,counts_work,keeps_attendance,comment FROM activity_types ORDER BY id")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []activityType{}
	for rows.Next() {
		var t activityType
		if err := rows.Scan(&t.ID, &t.Name, &t.CountsWork, &t.KeepsAttendance, &t.Comment); err != nil {
			return nil, err
		}
		items = append(items, t)
	}
	return items, rows.Err()
}
func (a *app) teamStatus(ctx context.Context) ([]status, error) {
	users, err := a.users(ctx)
	if err != nil {
		return nil, err
	}
	out := []status{}
	for _, u := range users {
		s, err := currentStatus(ctx, a.db, u.ID)
		if errors.Is(err, sql.ErrNoRows) {
			out = append(out, status{UserID: u.ID, UserName: u.Name, Department: u.Department, TypeName: "Not clocked in"})
			continue
		}
		if err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, nil
}
func (a *app) entriesForUser(ctx context.Context, userID int64, limit int) ([]entry, error) {
	rows, err := a.db.QueryContext(ctx, `SELECT e.id,e.user_id,u.name,e.occurred_at,e.type_id,t.name,t.counts_work,t.keeps_attendance,COALESCE(e.comment,''),e.source,e.critical,COALESCE(e.critical_reason,''),e.critical_resolved,COALESCE(e.review_note,'') FROM entries e JOIN users u ON u.id=e.user_id JOIN activity_types t ON t.id=e.type_id WHERE e.user_id=? ORDER BY e.occurred_at DESC,e.id DESC LIMIT ?`, userID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []entry{}
	for rows.Next() {
		var e entry
		if err := rows.Scan(&e.ID, &e.UserID, &e.UserName, &e.OccurredAt, &e.TypeID, &e.TypeName, &e.CountsWork, &e.KeepsAttendance, &e.Comment, &e.Source, &e.Critical, &e.CriticalReason, &e.CriticalResolved, &e.ReviewNote); err != nil {
			return nil, err
		}
		items = append(items, e)
	}
	return items, rows.Err()
}
func (a *app) criticalEntries(ctx context.Context) ([]entry, error) {
	rows, err := a.db.QueryContext(ctx, `SELECT e.id,e.user_id,u.name,e.occurred_at,e.type_id,t.name,t.counts_work,t.keeps_attendance,COALESCE(e.comment,''),e.source,e.critical,COALESCE(e.critical_reason,''),e.critical_resolved,COALESCE(e.review_note,'') FROM entries e JOIN users u ON u.id=e.user_id JOIN activity_types t ON t.id=e.type_id WHERE e.critical=TRUE AND e.critical_resolved=FALSE ORDER BY e.occurred_at DESC LIMIT 100`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []entry{}
	for rows.Next() {
		var e entry
		if err := rows.Scan(&e.ID, &e.UserID, &e.UserName, &e.OccurredAt, &e.TypeID, &e.TypeName, &e.CountsWork, &e.KeepsAttendance, &e.Comment, &e.Source, &e.Critical, &e.CriticalReason, &e.CriticalResolved, &e.ReviewNote); err != nil {
			return nil, err
		}
		items = append(items, e)
	}
	return items, rows.Err()
}

func (a *app) report(ctx context.Context, userID int64, now time.Time) ([]dayTotal, error) {
	items, err := a.entriesForUser(ctx, userID, 10000)
	if err != nil {
		return nil, err
	}
	// Entries arrive newest-first for the UI; time accounting needs oldest-first.
	for i, j := 0, len(items)-1; i < j; i, j = i+1, j-1 {
		items[i], items[j] = items[j], items[i]
	}
	totals := map[string]float64{}
	for i, item := range items {
		if !item.CountsWork {
			continue
		}
		end := now.UTC()
		if i+1 < len(items) {
			end = time.Unix(items[i+1].OccurredAt, 0).UTC()
		}
		start := time.Unix(item.OccurredAt, 0).UTC()
		if end.After(start) {
			a.splitAcrossDays(totals, start, end)
		}
	}
	result := make([]dayTotal, 0, len(totals))
	for day, hours := range totals {
		result = append(result, dayTotal{Date: day, Hours: float64(int(hours*100+0.5)) / 100})
	}
	for i := 0; i < len(result); i++ {
		for j := i + 1; j < len(result); j++ {
			if result[j].Date > result[i].Date {
				result[i], result[j] = result[j], result[i]
			}
		}
	}
	return result, nil
}

func (a *app) splitAcrossDays(totals map[string]float64, start, end time.Time) {
	for start.Before(end) {
		local := start.In(a.location)
		nextLocal := time.Date(local.Year(), local.Month(), local.Day()+1, 0, 0, 0, 0, a.location)
		next := nextLocal.UTC()
		if next.After(end) {
			next = end
		}
		totals[local.Format("2006-01-02")] += next.Sub(start).Hours()
		start = next
	}
}

func parsePositive(value string) int64 {
	n, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
	if err != nil || n < 1 {
		return 0
	}
	return n
}
func decodeJSON(w http.ResponseWriter, r *http.Request, dst any) error {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	defer r.Body.Close()
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return errors.New("invalid JSON")
	}
	return nil
}
