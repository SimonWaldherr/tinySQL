package main

import (
	"context"
	"database/sql"
	"embed"
	"flag"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"os"

	tinysql "github.com/SimonWaldherr/tinySQL"
	idrv "github.com/SimonWaldherr/tinySQL/internal/driver"

	_ "github.com/SimonWaldherr/tinySQL/driver"
)

//go:embed templates static
var webFS embed.FS

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

	// Autosave on clean shutdown when using a file.
	if *dbFile != "" && *dbFile != ":memory:" {
		defer func() {
			if saveErr := tinysql.SaveToFile(nativeDB, *dbFile); saveErr != nil {
				log.Printf("autosave: %v", saveErr)
			}
		}()
	}

	// Register the native DB instance with the database/sql driver so that
	// sql.Open("tinysql", ...) shares the same underlying storage.
	idrv.SetDefaultDB(nativeDB)

	sqlDB, err := sql.Open("tinysql", "mem://?tenant="+*tenant)
	if err != nil {
		log.Fatalf("sql.Open: %v", err)
	}
	defer sqlDB.Close()
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

	handler := securityHeaders(mux)
	log.Printf("AccessWeb listening on %s  (db: %s, tenant: %s)", *addr, dbLabel(*dbFile), *tenant)
	log.Fatal(http.ListenAndServe(*addr, handler))
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
