// Command docsearch provides a local, persistent document search with a small
// browser UI. It indexes a chosen directory into TinySQL and uses FTS_SEARCH
// where available, with a conservative substring-search fallback.
package main

import (
	"context"
	"database/sql"
	"embed"
	"flag"
	"fmt"
	"html/template"
	"io/fs"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/SimonWaldherr/tinySQL/cmd/internal/webapp"
)

//go:embed templates/*.html static/*
var assets embed.FS

const maxDocumentBytes = 1 << 20

type app struct {
	db   *sql.DB
	root string
	tpl  *template.Template
}

type document struct {
	ID       int64  `json:"id"`
	Path     string `json:"path"`
	Title    string `json:"title"`
	Content  string `json:"content,omitempty"`
	Modified int64  `json:"modified_at"`
	Score    any    `json:"score,omitempty"`
	Rank     any    `json:"rank,omitempty"`
	Snippet  string `json:"snippet,omitempty"`
}

func main() {
	addr := flag.String("addr", "127.0.0.1:8092", "HTTP listen address; use :8092 to accept connections from other machines")
	dsn := flag.String("dsn", "file:docsearch.db?autosave=1", "TinySQL DSN")
	root := flag.String("docs", ".", "Directory to index (read-only)")
	flag.Parse()

	absRoot, err := filepath.Abs(*root)
	if err != nil {
		log.Fatalf("resolve -docs: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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
	a := &app{db: db, root: absRoot, tpl: tpl}
	if err := a.bootstrap(ctx); err != nil {
		log.Fatalf("prepare index: %v", err)
	}

	log.Printf("docsearch listening on %s (documents: %s)", *addr, absRoot)
	log.Fatal(http.ListenAndServe(*addr, a.handler()))
}

func (a *app) bootstrap(ctx context.Context) error {
	if err := webapp.Apply(ctx, a.db, `CREATE TABLE IF NOT EXISTS documents (
		id INT PRIMARY KEY, path TEXT NOT NULL UNIQUE, title TEXT NOT NULL,
		content TEXT NOT NULL, modified_at INT NOT NULL
	)`); err != nil {
		return err
	}
	var count int
	if err := a.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM documents").Scan(&count); err != nil {
		return err
	}
	if count == 0 {
		_, err := a.index(ctx)
		return err
	}
	return nil
}

func (a *app) routes(mux *http.ServeMux) {
	mux.HandleFunc("GET /", a.indexPage)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		webapp.JSON(w, http.StatusOK, map[string]bool{"ok": true})
	})
	mux.HandleFunc("GET /api/status", a.status)
	mux.HandleFunc("GET /api/search", a.search)
	mux.HandleFunc("GET /api/documents/{id}", a.getDocument)
	mux.HandleFunc("POST /api/reindex", a.reindex)
}

func (a *app) indexPage(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := a.tpl.ExecuteTemplate(w, "index.html", map[string]string{"Root": a.root}); err != nil {
		http.Error(w, "template error", http.StatusInternalServerError)
	}
}

func (a *app) status(w http.ResponseWriter, r *http.Request) {
	var count int
	var newest sql.NullInt64
	if err := a.db.QueryRowContext(r.Context(), "SELECT COUNT(*), MAX(modified_at) FROM documents").Scan(&count, &newest); err != nil {
		webapp.Error(w, r, http.StatusInternalServerError, "could not read index status")
		return
	}
	webapp.JSON(w, http.StatusOK, map[string]any{"root": a.root, "documents": count, "newest_modified_at": newest.Int64})
}

func (a *app) reindex(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Minute)
	defer cancel()
	count, err := a.index(ctx)
	if err != nil {
		webapp.Error(w, r, http.StatusInternalServerError, "indexing failed")
		return
	}
	webapp.JSON(w, http.StatusOK, map[string]any{"documents": count})
}

func (a *app) search(w http.ResponseWriter, r *http.Request) {
	query := strings.TrimSpace(r.URL.Query().Get("q"))
	items, err := a.searchDocuments(r.Context(), query)
	if err != nil {
		webapp.Error(w, r, http.StatusInternalServerError, "search failed")
		return
	}
	webapp.JSON(w, http.StatusOK, map[string]any{"query": query, "results": items})
}

func (a *app) getDocument(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(strings.TrimSpace(r.PathValue("id")), 10, 64)
	if err != nil || id < 1 {
		webapp.Error(w, r, http.StatusNotFound, "document not found")
		return
	}
	var d document
	err = a.db.QueryRowContext(r.Context(), "SELECT id, path, title, content, modified_at FROM documents WHERE id = ?", id).Scan(&d.ID, &d.Path, &d.Title, &d.Content, &d.Modified)
	if err == sql.ErrNoRows {
		webapp.Error(w, r, http.StatusNotFound, "document not found")
		return
	}
	if err != nil {
		webapp.Error(w, r, http.StatusInternalServerError, "document could not be read")
		return
	}
	webapp.JSON(w, http.StatusOK, d)
}

func (a *app) index(ctx context.Context) (int, error) {
	files, err := collectDocuments(a.root)
	if err != nil {
		return 0, err
	}
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, "DELETE FROM documents"); err != nil {
		return 0, err
	}
	for i, file := range files {
		id := int64(i + 1)
		if _, err := tx.ExecContext(ctx, `INSERT INTO documents (id, path, title, content, modified_at) VALUES (?, ?, ?, ?, ?)`, id, file.path, file.title, file.content, file.modified); err != nil {
			return 0, fmt.Errorf("index %s: %w", file.path, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return len(files), nil
}

type indexFile struct {
	path, title, content string
	modified             int64
}

func collectDocuments(root string) ([]indexFile, error) {
	info, err := os.Stat(root)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("document root is not a directory")
	}
	files := make([]indexFile, 0)
	err = filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			if path != root && ignoredDirectory(entry.Name()) {
				return filepath.SkipDir
			}
			return nil
		}
		if !searchableExtension(filepath.Ext(entry.Name())) {
			return nil
		}
		info, err := entry.Info()
		if err != nil || info.Size() > maxDocumentBytes {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil || !utf8.Valid(data) {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return nil
		}
		content := strings.ReplaceAll(string(data), "\x00", "")
		files = append(files, indexFile{path: filepath.ToSlash(rel), title: documentTitle(rel, content), content: content, modified: info.ModTime().UTC().Unix()})
		return nil
	})
	sort.Slice(files, func(i, j int) bool { return files[i].path < files[j].path })
	return files, err
}

func ignoredDirectory(name string) bool {
	return name == ".git" || name == "node_modules" || name == "vendor" || strings.HasPrefix(name, ".")
}
func searchableExtension(ext string) bool {
	switch strings.ToLower(ext) {
	case ".md", ".txt", ".rst", ".html", ".htm", ".go", ".sql", ".csv":
		return true
	}
	return false
}
func documentTitle(path, content string) string {
	for _, line := range strings.Split(content, "\n") {
		line = strings.TrimSpace(strings.TrimLeft(line, "#"))
		if line != "" {
			if len(line) > 120 {
				return line[:120]
			}
			return line
		}
	}
	return filepath.Base(path)
}

func (a *app) searchDocuments(ctx context.Context, query string) ([]document, error) {
	if query == "" {
		return a.recentDocuments(ctx)
	}
	rows, err := a.db.QueryContext(ctx, `SELECT id, path, title, content, modified_at, _fts_score, _fts_rank
		FROM FTS_SEARCH('documents', ?, 50, 'title', 'content') ORDER BY _fts_rank`, query)
	if err == nil {
		defer rows.Close()
		items, err := scanSearch(rows, query)
		if err == nil {
			return items, nil
		}
	}
	// Fallback keeps the application usable if an advanced FTS expression is
	// malformed or a future storage mode intentionally lacks an FTS index.
	needle := "%" + strings.ToLower(query) + "%"
	rows, err = a.db.QueryContext(ctx, `SELECT id, path, title, content, modified_at FROM documents
		WHERE LOWER(title) LIKE ? OR LOWER(content) LIKE ? ORDER BY modified_at DESC LIMIT 50`, needle, needle)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]document, 0)
	for rows.Next() {
		var d document
		if err := rows.Scan(&d.ID, &d.Path, &d.Title, &d.Content, &d.Modified); err != nil {
			return nil, err
		}
		d.Snippet = snippet(d.Content, query)
		d.Content = ""
		items = append(items, d)
	}
	return items, rows.Err()
}

func scanSearch(rows *sql.Rows, query string) ([]document, error) {
	items := make([]document, 0)
	for rows.Next() {
		var d document
		if err := rows.Scan(&d.ID, &d.Path, &d.Title, &d.Content, &d.Modified, &d.Score, &d.Rank); err != nil {
			return nil, err
		}
		d.Snippet = snippet(d.Content, query)
		d.Content = ""
		items = append(items, d)
	}
	return items, rows.Err()
}

func (a *app) recentDocuments(ctx context.Context) ([]document, error) {
	rows, err := a.db.QueryContext(ctx, "SELECT id, path, title, content, modified_at FROM documents ORDER BY modified_at DESC LIMIT 50")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]document, 0)
	for rows.Next() {
		var d document
		if err := rows.Scan(&d.ID, &d.Path, &d.Title, &d.Content, &d.Modified); err != nil {
			return nil, err
		}
		d.Snippet = snippet(d.Content, "")
		d.Content = ""
		items = append(items, d)
	}
	return items, rows.Err()
}

func snippet(content, query string) string {
	content = strings.TrimSpace(strings.Join(strings.Fields(content), " "))
	if content == "" {
		return ""
	}
	needle := strings.ToLower(query)
	at := strings.Index(strings.ToLower(content), needle)
	if at < 0 {
		at = 0
	}
	start := at - 100
	if start < 0 {
		start = 0
	}
	end := start + 280
	if end > len(content) {
		end = len(content)
	}
	result := content[start:end]
	if start > 0 {
		result = "…" + result
	}
	if end < len(content) {
		result += "…"
	}
	return result
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
