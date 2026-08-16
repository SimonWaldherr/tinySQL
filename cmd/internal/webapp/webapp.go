// Package webapp contains deliberately small building blocks shared by the
// runnable web applications in cmd/. It keeps the examples dependency-free
// while making their database and HTTP behaviour consistent.
package webapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	_ "github.com/SimonWaldherr/tinySQL/driver"
)

// Open opens a TinySQL database and verifies it before returning. Callers can
// pass mem:// for ephemeral data or file:path.db?autosave=1 for persistence.
func Open(ctx context.Context, dsn string) (*sql.DB, error) {
	if strings.TrimSpace(dsn) == "" {
		dsn = "mem://?tenant=default"
	}
	db, err := sql.Open("tinysql", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(4)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

// Apply executes schema statements one by one so error messages name the
// failing statement. The schemas in these examples are intentionally simple
// and use only TinySQL's public SQL surface.
func Apply(ctx context.Context, db *sql.DB, statements ...string) error {
	for _, statement := range statements {
		statement = strings.TrimSpace(statement)
		if statement == "" {
			continue
		}
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("apply schema %q: %w", firstLine(statement), err)
		}
	}
	return nil
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		s = s[:i]
	}
	return strings.TrimSpace(s)
}

// NextID returns the next positive integral key for a fixed, trusted table
// name. Table names are supplied by application code, never HTTP input.
func NextID(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, table string) (int64, error) {
	var max sql.NullInt64
	if err := q.QueryRowContext(ctx, "SELECT MAX(id) FROM "+table).Scan(&max); err != nil {
		return 0, err
	}
	return max.Int64 + 1, nil
}

// JSON writes a JSON response. It is suitable for the compact browser APIs in
// the examples and intentionally does not expose internal errors itself.
func JSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

// Error writes a JSON error when a request targets /api/, otherwise a plain
// text HTTP error. It avoids leaking implementation details from handlers.
func Error(w http.ResponseWriter, r *http.Request, status int, message string) {
	if strings.HasPrefix(r.URL.Path, "/api/") {
		JSON(w, status, map[string]string{"error": message})
		return
	}
	http.Error(w, message, status)
}

// SecurityHeaders adds a compact baseline appropriate for local web tools.
// No third-party scripts, frames or plugin content are required by these apps.
func SecurityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Security-Policy", "default-src 'self'; style-src 'self'; script-src 'self'; img-src 'self' data:; connect-src 'self'; form-action 'self'; base-uri 'none'; frame-ancestors 'none'")
		w.Header().Set("Referrer-Policy", "same-origin")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		next.ServeHTTP(w, r)
	})
}

// ContextWithTimeout returns a modest request timeout used by command apps.
func ContextWithTimeout(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, 15*time.Second)
}
