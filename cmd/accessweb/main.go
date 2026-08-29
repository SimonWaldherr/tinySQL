package main

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"flag"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	tinysql "github.com/SimonWaldherr/tinySQL"
	tsqldriver "github.com/SimonWaldherr/tinySQL/driver"
)

//go:embed templates static
var webFS embed.FS

// defaultTenant is the tenant the driver's embedding path uses. See the check
// in main for why it cannot be varied.
const defaultTenant = "default"

func main() {
	addr := flag.String("addr", ":8080", "HTTP listen address")
	dbFile := flag.String("db", "accessweb.db", "Database file path (empty or :memory: for in-memory)")
	tenant := flag.String("tenant", "default", "Tenant / schema name")
	flag.Parse()

	// Open or create the tinySQL database.
	nativeDB, err := openNativeDB(*dbFile)
	if err != nil {
		log.Fatalf("open database: %v", err)
	}

	// The driver shares an existing database only through the empty DSN, which
	// pins the tenant to "default". A named DSN such as mem://?tenant=x builds
	// its own storage, so the handlers wrote to one database while the file was
	// loaded from and saved to another.
	if *tenant != defaultTenant {
		log.Fatalf("-tenant must be %q: the embedded driver connection shares the saved database only on the default tenant", defaultTenant)
	}

	sqlDB, err := tsqldriver.OpenWithDB(nativeDB)
	if err != nil {
		log.Fatalf("open sql handle: %v", err)
	}
	sqlDB.SetMaxOpenConns(8)
	sqlDB.SetMaxIdleConns(4)

	if err := sqlDB.PingContext(context.Background()); err != nil {
		log.Fatalf("ping: %v", err)
	}

	tpl, err := parseTemplates()
	if err != nil {
		log.Fatalf("templates: %v", err)
	}

	app := newApp(nativeDB, sqlDB, *tenant, tpl)

	mux := http.NewServeMux()
	app.registerRoutes(mux)
	mux.Handle("GET /static/", http.FileServer(http.FS(webFS)))

	srv := &http.Server{
		Addr:              *addr,
		Handler:           securityHeaders(mux),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Printf("AccessWeb listening on %s  (db: %s, tenant: %s)", *addr, dbLabel(*dbFile), *tenant)
	if err := serveUntilSignal(ctx, srv, sqlDB, nativeDB, *dbFile); err != nil {
		log.Fatalf("serve: %v", err)
	}
}

// serveUntilSignal runs srv until ctx is cancelled, then drains connections and
// writes the database back to dbFile.
//
// The save has to happen here rather than in a defer in main: the database is
// held in memory for the lifetime of the process, and the previous
// log.Fatal(ListenAndServe(...)) exited through os.Exit, which runs no defers.
// Every edit made through the web UI was discarded on shutdown.
func serveUntilSignal(ctx context.Context, srv *http.Server, sqlDB *sql.DB, nativeDB *tinysql.DB, dbFile string) error {
	errCh := make(chan error, 1)
	go func() {
		serveErr := srv.ListenAndServe()
		if errors.Is(serveErr, http.ErrServerClosed) {
			serveErr = nil
		}
		errCh <- serveErr
	}()

	var serveErr error
	select {
	case serveErr = <-errCh:
	case <-ctx.Done():
		log.Println("shutting down")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			log.Printf("shutdown: %v", err)
		}
		serveErr = <-errCh
	}

	if err := sqlDB.Close(); err != nil {
		log.Printf("close pool: %v", err)
	}
	if dbFile != "" && dbFile != ":memory:" {
		if saveErr := tinysql.SaveToFile(nativeDB, dbFile); saveErr != nil {
			log.Printf("autosave: %v", saveErr)
		} else {
			log.Printf("saved database to %s", dbFile)
		}
	}
	// Close after the save: it stops the WAL checkpoint worker and releases the
	// descriptors that back the database.
	if err := nativeDB.Close(); err != nil {
		log.Printf("close database: %v", err)
	}
	return serveErr
}

// openNativeDB loads a file-backed DB or creates a new in-memory one.
func openNativeDB(filePath string) (*tinysql.DB, error) {
	if filePath == "" || filePath == ":memory:" {
		return tinysql.NewDB(), nil
	}
	db, err := tinysql.LoadFromFile(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return tinysql.NewDB(), nil
		}
		return nil, err
	}
	return db, nil
}

// dbLabel returns a human-readable label for the database location.
func dbLabel(filePath string) string {
	if filePath == "" || filePath == ":memory:" {
		return "in-memory"
	}
	return filePath
}

// parseTemplates parses all embedded HTML templates.
func parseTemplates() (*template.Template, error) {
	return template.New("").Funcs(template.FuncMap{
		"add": func(a, b int) int { return a + b },
		"sub": func(a, b int) int { return a - b },
		"seq": func(n int) []int {
			s := make([]int, n)
			for i := range s {
				s[i] = i + 1
			}
			return s
		},
		// dict builds a map[string]interface{} for use inside template calls.
		"dict": func(pairs ...interface{}) (map[string]interface{}, error) {
			if len(pairs)%2 != 0 {
				return nil, fmt.Errorf("dict: odd number of arguments")
			}
			m := make(map[string]interface{}, len(pairs)/2)
			for i := 0; i < len(pairs); i += 2 {
				key, ok := pairs[i].(string)
				if !ok {
					return nil, fmt.Errorf("dict: key must be string, got %T", pairs[i])
				}
				m[key] = pairs[i+1]
			}
			return m, nil
		},
		"not": func(b bool) bool { return !b },
	}).ParseFS(webFS, "templates/*.html")
}

// securityHeaders adds baseline browser security headers.
func securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Security-Policy",
			"default-src 'self'; style-src 'self' https://cdn.jsdelivr.net; "+
				"script-src 'self' https://cdn.jsdelivr.net; "+
				"font-src 'self' https://cdn.jsdelivr.net; "+
				"img-src 'self' data:; form-action 'self'; frame-ancestors 'none'; base-uri 'self'")
		w.Header().Set("Referrer-Policy", "strict-origin-when-cross-origin")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		next.ServeHTTP(w, r)
	})
}
