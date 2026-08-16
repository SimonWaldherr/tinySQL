package main

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"

	tinysql "github.com/SimonWaldherr/tinySQL"
	"github.com/SimonWaldherr/tinySQL/tinyorm"
)

//go:embed templates/*.html static/*
var placeAssets embed.FS

type placeApp struct {
	raw      *tinysql.DB
	orm      *tinyorm.DB
	snapshot string
	tpl      *template.Template
	mu       sync.Mutex
}

type placeInput struct {
	Name    string  `json:"name"`
	Country string  `json:"country"`
	Lat     float64 `json:"lat"`
	Lon     float64 `json:"lon"`
	Active  bool    `json:"active"`
}

func servePlaces(ctx context.Context, addr, snapshot string) error {
	a, err := newPlaceApp(ctx, snapshot)
	if err != nil {
		return err
	}
	mux := http.NewServeMux()
	a.routes(mux)
	mux.Handle("GET /static/", http.FileServer(http.FS(placeAssets)))
	return http.ListenAndServe(addr, placeSecurityHeaders(mux))
}

func newPlaceApp(ctx context.Context, snapshot string) (*placeApp, error) {
	raw := tinysql.NewDB()
	if snapshot != "" {
		loaded, err := tinysql.LoadFromFile(snapshot)
		if err == nil {
			raw = loaded
		} else if !os.IsNotExist(err) {
			return nil, fmt.Errorf("load snapshot: %w", err)
		}
	}
	orm := tinyorm.New(raw, "default")
	if err := orm.AutoMigrate(ctx, Place{}); err != nil {
		return nil, err
	}
	a := &placeApp{raw: raw, orm: orm, snapshot: snapshot}
	tpl, err := template.ParseFS(placeAssets, "templates/*.html")
	if err != nil {
		return nil, err
	}
	a.tpl = tpl
	places, err := a.list(ctx, "", "all")
	if err != nil {
		return nil, err
	}
	if len(places) == 0 {
		for _, p := range []Place{
			{ID: 1, Name: "Berlin", Country: "DE", Lat: 52.52, Lon: 13.405, Active: true},
			{ID: 2, Name: "Munich", Country: "DE", Lat: 48.1372, Lon: 11.5755, Active: true},
			{ID: 3, Name: "Cologne", Country: "DE", Lat: 50.9375, Lon: 6.9603, Active: false},
			{ID: 4, Name: "Zurich", Country: "CH", Lat: 47.3769, Lon: 8.5417, Active: false},
		} {
			if err := orm.Insert(ctx, p); err != nil {
				return nil, err
			}
		}
		if err := a.save(); err != nil {
			return nil, err
		}
	}
	return a, nil
}

func (a *placeApp) routes(mux *http.ServeMux) {
	mux.HandleFunc("GET /", a.index)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writePlaceJSON(w, http.StatusOK, map[string]bool{"ok": true})
	})
	mux.HandleFunc("GET /api/places", a.places)
	mux.HandleFunc("POST /api/places", a.createPlace)
	mux.HandleFunc("DELETE /api/places/{id}", a.deletePlace)
}

func (a *placeApp) index(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := a.tpl.ExecuteTemplate(w, "index.html", nil); err != nil {
		http.Error(w, "template error", 500)
	}
}

func (a *placeApp) places(w http.ResponseWriter, r *http.Request) {
	items, err := a.list(r.Context(), strings.TrimSpace(r.URL.Query().Get("country")), strings.TrimSpace(r.URL.Query().Get("active")))
	if err != nil {
		placeError(w, 500, "could not load places")
		return
	}
	writePlaceJSON(w, 200, items)
}

func (a *placeApp) createPlace(w http.ResponseWriter, r *http.Request) {
	var in placeInput
	if err := placeDecodeJSON(w, r, &in); err != nil {
		placeError(w, 400, err.Error())
		return
	}
	in.Name = strings.TrimSpace(in.Name)
	in.Country = strings.ToUpper(strings.TrimSpace(in.Country))
	if in.Name == "" || len(in.Country) != 2 || in.Lat < -90 || in.Lat > 90 || in.Lon < -180 || in.Lon > 180 {
		placeError(w, 400, "name, two-letter country and valid coordinates are required")
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	items, err := a.list(r.Context(), "", "all")
	if err != nil {
		placeError(w, 500, "could not create place")
		return
	}
	next := 1
	for _, p := range items {
		if p.ID >= next {
			next = p.ID + 1
		}
	}
	out := Place{ID: next, Name: in.Name, Country: in.Country, Lat: in.Lat, Lon: in.Lon, Active: in.Active}
	if err := a.orm.Insert(r.Context(), out); err != nil {
		placeError(w, 409, "place could not be created")
		return
	}
	if err := a.save(); err != nil {
		placeError(w, 500, "place was saved but snapshot could not be updated")
		return
	}
	writePlaceJSON(w, 201, out)
}

func (a *placeApp) deletePlace(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.Atoi(r.PathValue("id"))
	if err != nil || id < 1 {
		placeError(w, 400, "invalid place id")
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	var existing Place
	if err := a.orm.FindByPK(r.Context(), &existing, id); err != nil {
		if errors.Is(err, tinyorm.ErrNotFound) {
			placeError(w, 404, "place not found")
		} else {
			placeError(w, 500, "could not delete place")
		}
		return
	}
	if err := a.orm.DeleteByPK(r.Context(), Place{}, id); err != nil {
		placeError(w, 500, "could not delete place")
		return
	}
	if err := a.save(); err != nil {
		placeError(w, 500, "place deleted but snapshot could not be updated")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a *placeApp) list(ctx context.Context, country, active string) ([]Place, error) {
	var items []Place
	where := ""
	params := map[string]any{}
	if country != "" {
		where = "country = :country"
		params["country"] = strings.ToUpper(country)
	}
	if err := a.orm.Select(ctx, &items, where, params); err != nil {
		return nil, err
	}
	out := items[:0]
	for _, item := range items {
		if active == "active" && !item.Active {
			continue
		}
		if active == "inactive" && item.Active {
			continue
		}
		out = append(out, item)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}
func (a *placeApp) save() error {
	if a.snapshot == "" {
		return nil
	}
	return tinysql.SaveToFile(a.raw, a.snapshot)
}
func writePlaceJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func placeError(w http.ResponseWriter, status int, msg string) {
	writePlaceJSON(w, status, map[string]string{"error": msg})
}
func placeDecodeJSON(w http.ResponseWriter, r *http.Request, dst any) error {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	defer r.Body.Close()
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return errors.New("invalid JSON")
	}
	return nil
}
func placeSecurityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Security-Policy", "default-src 'self'; style-src 'self'; script-src 'self'; connect-src 'self'; base-uri 'none'; frame-ancestors 'none'")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		next.ServeHTTP(w, r)
	})
}
