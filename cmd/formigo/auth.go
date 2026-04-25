package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"net/http"
	"strings"
	"time"

	"golang.org/x/crypto/bcrypt"
)

const sessionCookieName = "formigo_session"

type contextKey string

const requestContextKey contextKey = "formigo_request_context"

// RequestContext contains authenticated request state for handlers and templates.
type RequestContext struct {
	User      *User
	CSRFToken string
}

// AuthService manages login sessions and CSRF tokens.
type AuthService struct {
	store        *Store
	cookieSecure bool
	sessionTTL   time.Duration
}

// NewAuthService creates an authentication service.
func NewAuthService(store *Store, cookieSecure bool) *AuthService {
	return &AuthService{store: store, cookieSecure: cookieSecure, sessionTTL: 12 * time.Hour}
}

// HashPassword hashes a password for storage.
func HashPassword(password string) (string, error) {
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	return string(hash), err
}

// CheckPassword verifies a plaintext password against a bcrypt hash.
func CheckPassword(hash, password string) bool {
	return bcrypt.CompareHashAndPassword([]byte(hash), []byte(password)) == nil
}

// Login validates credentials and creates a session cookie.
func (a *AuthService) Login(w http.ResponseWriter, r *http.Request, username, password string) error {
	user, err := a.store.FindUserByUsername(r.Context(), strings.TrimSpace(username))
	if err != nil || !user.Active || !CheckPassword(user.PasswordHash, password) {
		return errors.New("invalid credentials")
	}
	token, err := randomToken(32)
	if err != nil {
		return err
	}
	csrf, err := randomToken(32)
	if err != nil {
		return err
	}
	expires := time.Now().UTC().Add(a.sessionTTL)
	if err := a.store.CreateSession(r.Context(), tokenHash(token), csrf, user.ID, expires.Format(timeLayout)); err != nil {
		return err
	}
	a.setCookie(w, token, expires)
	return nil
}

// Logout deletes the current session and clears the cookie.
func (a *AuthService) Logout(w http.ResponseWriter, r *http.Request) {
	if c, err := r.Cookie(sessionCookieName); err == nil {
		_ = a.store.DeleteSession(r.Context(), tokenHash(c.Value))
	}
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookieName,
		Value:    "",
		Path:     "/",
		MaxAge:   -1,
		HttpOnly: true,
		Secure:   a.cookieSecure,
		SameSite: http.SameSiteLaxMode,
	})
}

// Middleware loads the current session into the request context when present.
func (a *AuthService) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rc := &RequestContext{}
		if c, err := r.Cookie(sessionCookieName); err == nil && c.Value != "" {
			user, csrf, err := a.store.SessionUser(r.Context(), tokenHash(c.Value), time.Now().UTC().Format(timeLayout))
			if err == nil {
				rc.User = &user
				rc.CSRFToken = csrf
			}
		}
		ctx := context.WithValue(r.Context(), requestContextKey, rc)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// RequireRole authorizes a request for at least one allowed role.
func (a *AuthService) RequireRole(roles ...Role) func(http.HandlerFunc) http.HandlerFunc {
	return func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			rc := CurrentRequestContext(r)
			if rc.User == nil {
				http.Redirect(w, r, "/login", http.StatusSeeOther)
				return
			}
			if !roleAllowed(rc.User.Role, roles...) {
				http.Error(w, "forbidden", http.StatusForbidden)
				return
			}
			if isStateChanging(r.Method) && !validCSRF(r, rc.CSRFToken) {
				http.Error(w, "invalid csrf token", http.StatusForbidden)
				return
			}
			next(w, r)
		}
	}
}

// CurrentRequestContext returns auth state from a request.
func CurrentRequestContext(r *http.Request) *RequestContext {
	if rc, ok := r.Context().Value(requestContextKey).(*RequestContext); ok && rc != nil {
		return rc
	}
	return &RequestContext{}
}

// CurrentUser returns the authenticated user or nil.
func CurrentUser(r *http.Request) *User {
	return CurrentRequestContext(r).User
}

// CSRFToken returns the session CSRF token or an empty string.
func CSRFToken(r *http.Request) string {
	return CurrentRequestContext(r).CSRFToken
}

// setCookie writes the session cookie.
func (a *AuthService) setCookie(w http.ResponseWriter, token string, expires time.Time) {
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookieName,
		Value:    token,
		Path:     "/",
		Expires:  expires,
		HttpOnly: true,
		Secure:   a.cookieSecure,
		SameSite: http.SameSiteLaxMode,
	})
}

// validCSRF validates a CSRF token from form or header.
func validCSRF(r *http.Request, expected string) bool {
	if expected == "" {
		return false
	}
	token := r.Header.Get("X-CSRF-Token")
	if token == "" {
		token = r.FormValue("csrf_token")
	}
	if token == "" {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(token), []byte(expected)) == 1
}

// roleAllowed reports whether role is one of allowed.
func roleAllowed(role Role, allowed ...Role) bool {
	for _, r := range allowed {
		if role == r {
			return true
		}
	}
	return false
}

// isStateChanging reports whether a method can modify server state.
func isStateChanging(method string) bool {
	switch method {
	case http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
		return true
	default:
		return false
	}
}

// randomToken returns a URL-safe random token.
func randomToken(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

// tokenHash returns the SHA-256 hex digest used for server-side session lookup.
func tokenHash(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}
