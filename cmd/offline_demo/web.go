package main

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	tinysql "github.com/SimonWaldherr/tinySQL"
)

func (a *offlineApp) index(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := a.tpl.ExecuteTemplate(w, "index.html", nil); err != nil {
		http.Error(w, "template error", http.StatusInternalServerError)
	}
}

func (a *offlineApp) status(w http.ResponseWriter, r *http.Request) {
	items, err := listPOIs(r.Context(), a.db)
	if err != nil {
		offlineError(w, http.StatusInternalServerError, "could not read snapshot")
		return
	}
	writeOfflineJSON(w, http.StatusOK, map[string]any{
		"source": a.source, "read_only": a.readOnly, "pois": len(items),
	})
}

func (a *offlineApp) search(w http.ResponseWriter, r *http.Request) {
	values := r.URL.Query()
	query := strings.TrimSpace(values.Get("q"))
	var near *proximity
	if values.Has("lon") || values.Has("lat") || values.Has("radius_m") {
		var err error
		near, err = parseProximity(values.Get("lon")+","+values.Get("lat"), values.Get("radius_m"))
		if err != nil {
			offlineError(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	items, err := queryPOIs(r.Context(), a.db, query, near)
	if err != nil {
		offlineError(w, http.StatusInternalServerError, "search failed")
		return
	}
	writeOfflineJSON(w, http.StatusOK, map[string]any{"query": query, "results": items})
}

func listPOIs(ctx context.Context, db *tinysql.DB) ([]poi, error) {
	return queryPOIs(ctx, db, "", nil)
}

func writeOfflineJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func offlineError(w http.ResponseWriter, status int, message string) {
	writeOfflineJSON(w, status, map[string]string{"error": message})
}

func offlineSecurityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Security-Policy", "default-src 'self'; style-src 'self'; script-src 'self'; img-src 'self'; connect-src 'self'; base-uri 'none'; frame-ancestors 'none'")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		next.ServeHTTP(w, r)
	})
}
