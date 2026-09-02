package main

import (
	"context"
	"database/sql"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"net/http"
	"strings"
	"time"

	"github.com/SimonWaldherr/tinySQL/cmd/internal/webapp"
)

//go:embed templates/*.html static/*
var supportAssets embed.FS

type supportDeskApp struct {
	db  *sql.DB
	tpl *template.Template
}

type supportArticle struct {
	ID       int64  `json:"id"`
	Title    string `json:"title"`
	Body     string `json:"body"`
	Category string `json:"category"`
	Score    any    `json:"score,omitempty"`
}

type supportTicket struct {
	ID        int64  `json:"id"`
	ArticleID int64  `json:"article_id"`
	Subject   string `json:"subject"`
	Status    string `json:"status"`
	Article   string `json:"article"`
}

type ticketInput struct {
	ArticleID int64  `json:"article_id"`
	Subject   string `json:"subject"`
}

func serveSupportDesk(ctx context.Context, addr, dsn string) error {
	db, err := webapp.Open(ctx, dsn)
	if err != nil {
		return err
	}
	tpl, err := template.ParseFS(supportAssets, "templates/*.html")
	if err != nil {
		_ = db.Close()
		return err
	}
	a := &supportDeskApp{db: db, tpl: tpl}
	if err := a.bootstrap(ctx); err != nil {
		_ = db.Close()
		return err
	}
	return http.ListenAndServe(addr, a.handler())
}

func (a *supportDeskApp) bootstrap(ctx context.Context) error {
	if err := webapp.Apply(ctx, a.db,
		`CREATE TABLE IF NOT EXISTS articles (id INT PRIMARY KEY, title TEXT NOT NULL, body TEXT NOT NULL, category TEXT NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS tickets (id INT PRIMARY KEY, article_id INT NOT NULL, subject TEXT NOT NULL, status TEXT NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS ticket_events (id INT PRIMARY KEY, ticket_id INT NOT NULL, event TEXT NOT NULL, occurred_at INT NOT NULL)`,
	); err != nil {
		return err
	}
	var count int
	if err := a.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM articles").Scan(&count); err != nil {
		return err
	}
	if count > 0 {
		return nil
	}
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, item := range seedArticles {
		if _, err := tx.ExecContext(ctx, "INSERT INTO articles (id,title,body,category) VALUES (?,?,?,?)", item.id, item.title, item.body, item.category); err != nil {
			return err
		}
	}
	now := time.Now().UTC().Unix()
	for _, item := range seedTickets {
		if _, err := tx.ExecContext(ctx, "INSERT INTO tickets (id,article_id,subject,status) VALUES (?,?,?,?)", item.id, item.articleID, item.subject, item.status); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, "INSERT INTO ticket_events (id,ticket_id,event,occurred_at) VALUES (?,?,?,?)", item.id, item.id, "created", now); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (a *supportDeskApp) routes(mux *http.ServeMux) {
	mux.HandleFunc("GET /", a.index)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		webapp.JSON(w, http.StatusOK, map[string]bool{"ok": true})
	})
	mux.HandleFunc("GET /api/status", a.status)
	mux.HandleFunc("GET /api/articles", a.articles)
	mux.HandleFunc("GET /api/search", a.search)
	mux.HandleFunc("GET /api/tickets", a.tickets)
	mux.HandleFunc("POST /api/tickets", a.createTicket)
}

func (a *supportDeskApp) index(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := a.tpl.ExecuteTemplate(w, "index.html", nil); err != nil {
		http.Error(w, "template error", http.StatusInternalServerError)
	}
}

func (a *supportDeskApp) status(w http.ResponseWriter, r *http.Request) {
	var articles, open int
	if err := a.db.QueryRowContext(r.Context(), "SELECT COUNT(*) FROM articles").Scan(&articles); err != nil {
		webapp.Error(w, r, 500, "could not load status")
		return
	}
	if err := a.db.QueryRowContext(r.Context(), "SELECT COUNT(*) FROM tickets WHERE status='open'").Scan(&open); err != nil {
		webapp.Error(w, r, 500, "could not load status")
		return
	}
	webapp.JSON(w, http.StatusOK, map[string]int{"articles": articles, "open_tickets": open})
}

func (a *supportDeskApp) articles(w http.ResponseWriter, r *http.Request) {
	items, err := a.listArticles(r.Context())
	if err != nil {
		webapp.Error(w, r, 500, "could not load articles")
		return
	}
	webapp.JSON(w, http.StatusOK, items)
}

func (a *supportDeskApp) search(w http.ResponseWriter, r *http.Request) {
	q := strings.TrimSpace(r.URL.Query().Get("q"))
	if q == "" {
		a.articles(w, r)
		return
	}
	rows, err := a.db.QueryContext(r.Context(), `SELECT id,title,body,category,_fts_score FROM FTS_SEARCH('articles', ?, 20, 'title', 'body') ORDER BY _fts_rank`, q)
	if err != nil {
		webapp.Error(w, r, 400, "invalid search query")
		return
	}
	defer rows.Close()
	items := []supportArticle{}
	for rows.Next() {
		var item supportArticle
		if err := rows.Scan(&item.ID, &item.Title, &item.Body, &item.Category, &item.Score); err != nil {
			webapp.Error(w, r, 500, "could not read search result")
			return
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		webapp.Error(w, r, 500, "search failed")
		return
	}
	webapp.JSON(w, http.StatusOK, items)
}

func (a *supportDeskApp) tickets(w http.ResponseWriter, r *http.Request) {
	items, err := a.listTickets(r.Context())
	if err != nil {
		webapp.Error(w, r, 500, "could not load tickets")
		return
	}
	webapp.JSON(w, http.StatusOK, items)
}

func (a *supportDeskApp) createTicket(w http.ResponseWriter, r *http.Request) {
	var in ticketInput
	if err := supportDecodeJSON(w, r, &in); err != nil {
		webapp.Error(w, r, 400, err.Error())
		return
	}
	in.Subject = strings.TrimSpace(in.Subject)
	if in.ArticleID < 1 || in.Subject == "" {
		webapp.Error(w, r, 400, "article_id and subject are required")
		return
	}
	tx, err := a.db.BeginTx(r.Context(), nil)
	if err != nil {
		webapp.Error(w, r, 500, "could not create ticket")
		return
	}
	defer tx.Rollback()
	var articleTitle string
	if err := tx.QueryRowContext(r.Context(), "SELECT title FROM articles WHERE id=?", in.ArticleID).Scan(&articleTitle); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			webapp.Error(w, r, 404, "article not found")
		} else {
			webapp.Error(w, r, 500, "could not create ticket")
		}
		return
	}
	id, err := webapp.NextID(r.Context(), tx, "tickets")
	if err == nil {
		_, err = tx.ExecContext(r.Context(), "INSERT INTO tickets (id,article_id,subject,status) VALUES (?,?,?,'open')", id, in.ArticleID, in.Subject)
	}
	if err == nil {
		eventID, eventErr := webapp.NextID(r.Context(), tx, "ticket_events")
		if eventErr != nil {
			err = eventErr
		} else {
			_, err = tx.ExecContext(r.Context(), "INSERT INTO ticket_events (id,ticket_id,event,occurred_at) VALUES (?,?,'created',?)", eventID, id, time.Now().UTC().Unix())
		}
	}
	if err != nil {
		webapp.Error(w, r, 500, "could not create ticket")
		return
	}
	if err = tx.Commit(); err != nil {
		webapp.Error(w, r, 500, "could not save ticket")
		return
	}
	webapp.JSON(w, http.StatusCreated, supportTicket{ID: id, ArticleID: in.ArticleID, Subject: in.Subject, Status: "open", Article: articleTitle})
}

func (a *supportDeskApp) listArticles(ctx context.Context) ([]supportArticle, error) {
	rows, err := a.db.QueryContext(ctx, "SELECT id,title,body,category FROM articles ORDER BY category,title")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []supportArticle{}
	for rows.Next() {
		var item supportArticle
		if err := rows.Scan(&item.ID, &item.Title, &item.Body, &item.Category); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}
func (a *supportDeskApp) listTickets(ctx context.Context) ([]supportTicket, error) {
	rows, err := a.db.QueryContext(ctx, `SELECT t.id,t.article_id,t.subject,t.status,a.title FROM tickets t JOIN articles a ON a.id=t.article_id ORDER BY t.id DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []supportTicket{}
	for rows.Next() {
		var item supportTicket
		if err := rows.Scan(&item.ID, &item.ArticleID, &item.Subject, &item.Status, &item.Article); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func supportDecodeJSON(w http.ResponseWriter, r *http.Request, dst any) error {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	defer r.Body.Close()
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return fmt.Errorf("invalid JSON")
	}
	return nil
}

// handler assembles the complete HTTP stack: the application routes, the
// stylesheet shared with the other cmd/ applications, this application's own
// static files and the security headers. main and the tests both go through
// it, so a test always exercises exactly what the binary serves.
func (a *supportDeskApp) handler() http.Handler {
	mux := http.NewServeMux()
	a.routes(mux)
	webapp.MountShared(mux)
	mux.Handle("GET /static/", http.FileServer(http.FS(supportAssets)))
	return webapp.SecurityHeaders(mux)
}
