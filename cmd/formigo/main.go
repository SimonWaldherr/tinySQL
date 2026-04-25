package main

import (
	"context"
	"embed"
	"flag"
	"html/template"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
)

//go:embed templates/*.html static/*
var webFS embed.FS

// main starts the Formigo HTTP server.
func main() {
	addr := flag.String("addr", ":8080", "HTTP listen address")
	dsn := flag.String("dsn", "file:formigo.db?autosave=1", "tinySQL DSN (file:path.db or mem://) or sqlserver:// DSN")
	adminUser := flag.String("admin-user", envDefault("FORMIGO_ADMIN_USER", "admin"), "initial admin username")
	adminPassword := flag.String("admin-password", envDefault("FORMIGO_ADMIN_PASSWORD", "admin123"), "initial admin password")
	cookieSecure := flag.Bool("secure-cookie", envDefault("FORMIGO_SECURE_COOKIE", "false") == "true", "mark session cookies as Secure")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db, d, err := openDB(ctx, *dsn)
	if err != nil {
		log.Fatalf("open database: %v", err)
	}
	defer db.Close()

	if err := migrate(ctx, db, d); err != nil {
		log.Fatalf("migrate database: %v", err)
	}

	store := NewStore(db, d)
	if err := seedInitialAdmin(ctx, store, *adminUser, *adminPassword); err != nil {
		log.Fatalf("seed admin: %v", err)
	}

	tpl, err := parseTemplates()
	if err != nil {
		log.Fatalf("parse templates: %v", err)
	}

	auth := NewAuthService(store, *cookieSecure)
	app := NewApp(store, auth, tpl, *adminUser, *adminPassword)

	// Check if the default admin password is still active.
	if u, err := store.FindUserByUsername(ctx, *adminUser); err == nil {
		if CheckPassword(u.PasswordHash, *adminPassword) {
			app.adminHintActive.Store(true)
		}
	}

	mux := http.NewServeMux()
	app.RegisterRoutes(mux)
	mux.Handle("GET /static/", http.FileServer(http.FS(webFS)))

	handler := securityHeaders(auth.Middleware(mux))
	log.Printf("Formigo listening on %s", *addr)
	log.Fatal(http.ListenAndServe(*addr, handler))
}

// parseTemplates parses embedded HTML templates and template functions.
func parseTemplates() (*template.Template, error) {
	return template.New("").Funcs(template.FuncMap{
		"isInputField": IsInputField,
		"hasRole": func(user *User, roles ...Role) bool {
			if user == nil {
				return false
			}
			return roleAllowed(user.Role, roles...)
		},
		"roleAdmin":  func() Role { return RoleAdmin },
		"roleEditor": func() Role { return RoleEditor },
		"roleViewer": func() Role { return RoleViewer },
		"roleUser":   func() Role { return RoleUser },
		"initial": func(s string) string {
			for _, r := range s {
				return string(r)
			}
			return "?"
		},
	}).ParseFS(webFS, "templates/*.html")
}

// seedInitialAdmin creates the first administrator if no users exist.
func seedInitialAdmin(ctx context.Context, store *Store, username, password string) error {
	count, err := store.CountUsers(ctx)
	if err != nil {
		return err
	}
	if count > 0 {
		return nil
	}
	hash, err := HashPassword(password)
	if err != nil {
		return err
	}
	_, err = store.CreateUser(ctx, username, "Administrator", hash, RoleAdmin, true)
	if err == nil {
		log.Printf("created initial admin user %q with password %q", username, password)
		log.Printf("change the initial password after first login")
	}
	return err
}

// securityHeaders adds baseline browser security headers.
func securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Security-Policy", "default-src 'self'; style-src 'self' https://cdn.jsdelivr.net; script-src 'self' https://cdn.jsdelivr.net; font-src 'self' https://cdn.jsdelivr.net; img-src 'self' data:; form-action 'self'; frame-ancestors 'none'; base-uri 'self'")
		w.Header().Set("Referrer-Policy", "strict-origin-when-cross-origin")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		next.ServeHTTP(w, r)
	})
}

// envDefault returns an environment variable or fallback when unset.
func envDefault(key, fallback string) string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	return v
}
