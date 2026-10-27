// Command offline_demo is a small local-first tinySQL example.
//
// It creates a compact POI database, optionally persists it as a snapshot,
// reopens that snapshot on later runs, and serves a read-only text search. No
// network service, external database, or application-specific runtime is
// required.
package main

import (
	"context"
	"embed"
	"encoding/json"
	"flag"
	"fmt"
	"html/template"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	tinysql "github.com/SimonWaldherr/tinySQL"
	"github.com/SimonWaldherr/tinySQL/cmd/internal/webapp"
)

type config struct {
	snapshot string
	query    string
	near     string
	radius   string
	json     bool
	rebuild  bool
	readOnly bool
	web      bool
	addr     string
}

type poi struct {
	ID       int      `json:"id"`
	Name     string   `json:"name"`
	Category string   `json:"category"`
	City     string   `json:"city"`
	Lat      float64  `json:"lat"`
	Lon      float64  `json:"lon"`
	Distance *float64 `json:"distance_m,omitempty"`
}

type proximity struct {
	lon, lat, radius float64
}

func main() {
	var cfg config
	flag.StringVar(&cfg.snapshot, "snapshot", "", "Optional snapshot path; created on first run and reused afterwards")
	flag.StringVar(&cfg.query, "query", "München", "Search name, city, or category")
	flag.StringVar(&cfg.near, "near", "", "Search near longitude,latitude (requires -radius-m)")
	flag.StringVar(&cfg.radius, "radius-m", "", "Search radius in metres (requires -near)")
	flag.BoolVar(&cfg.json, "json", false, "Write stable JSON instead of a text table")
	flag.BoolVar(&cfg.rebuild, "rebuild", false, "Ignore an existing snapshot and rebuild the sample dataset")
	flag.BoolVar(&cfg.readOnly, "read-only", true, "Reject writes after loading or creating the dataset")
	flag.BoolVar(&cfg.web, "web", false, "Serve the local POI explorer in a browser")
	flag.StringVar(&cfg.addr, "addr", "127.0.0.1:8086", "HTTP listen address when -web is set; use :8086 to accept connections from other machines")
	flag.Parse()
	// A nearby search without an explicit text query covers all POIs.
	queryExplicit := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == "query" {
			queryExplicit = true
		}
	})
	if cfg.near != "" && !queryExplicit {
		cfg.query = ""
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if cfg.web {
		if err := serveWeb(ctx, cfg); err != nil {
			fmt.Fprintln(os.Stderr, "offline_demo:", err)
			os.Exit(1)
		}
		return
	}
	if err := run(ctx, cfg, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "offline_demo:", err)
		os.Exit(1)
	}
}

//go:embed templates/*.html static/*
var webAssets embed.FS

type offlineApp struct {
	db       *tinysql.DB
	source   string
	readOnly bool
	tpl      *template.Template
}

func serveWeb(ctx context.Context, cfg config) error {
	if cfg.near != "" || cfg.radius != "" {
		return fmt.Errorf("-near and -radius-m apply to CLI searches; use the browser form in web mode")
	}
	db, source, err := openPOIDatabase(ctx, cfg)
	if err != nil {
		return err
	}
	if cfg.readOnly {
		db.SetReadOnly(true)
	}
	tpl, err := template.ParseFS(webAssets, "templates/*.html")
	if err != nil {
		return err
	}
	a := &offlineApp{db: db, source: source, readOnly: db.IsReadOnly(), tpl: tpl}
	log.Printf("offline POI explorer listening on %s (%s, read-only=%t)", cfg.addr, source, db.IsReadOnly())
	return http.ListenAndServe(cfg.addr, a.handler())
}

// handler assembles the complete HTTP stack: the application routes, the
// stylesheet shared with the other cmd/ applications, this application's own
// static files and the security headers. serveWeb and the tests both go
// through it, so a test always exercises exactly what the binary serves.
func (a *offlineApp) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /", a.index)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeOfflineJSON(w, http.StatusOK, map[string]bool{"ok": true})
	})
	mux.HandleFunc("GET /api/status", a.status)
	mux.HandleFunc("GET /api/search", a.search)
	webapp.MountShared(mux)
	mux.Handle("GET /static/", http.FileServer(http.FS(webAssets)))
	return offlineSecurityHeaders(mux)
}

func run(ctx context.Context, cfg config, out io.Writer) error {
	near, err := parseProximity(cfg.near, cfg.radius)
	if err != nil {
		return err
	}
	if near == nil && strings.TrimSpace(cfg.query) == "" {
		return fmt.Errorf("query must not be empty")
	}
	db, source, err := openPOIDatabase(ctx, cfg)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	if cfg.readOnly {
		db.SetReadOnly(true)
	}
	results, err := queryPOIs(ctx, db, cfg.query, near)
	if err != nil {
		return err
	}

	if cfg.json {
		return json.NewEncoder(out).Encode(struct {
			Source   string `json:"source"`
			ReadOnly bool   `json:"read_only"`
			Query    string `json:"query"`
			Results  []poi  `json:"results"`
		}{source, db.IsReadOnly(), cfg.query, results})
	}

	_, _ = fmt.Fprintf(out, "Offline POI demo (%s, read-only=%t)\n", source, db.IsReadOnly())
	if near != nil {
		_, _ = fmt.Fprintf(out, "Search: %q near %.6f,%.6f within %g m — %d result(s)\n\n", cfg.query, near.lon, near.lat, near.radius, len(results))
	} else {
		_, _ = fmt.Fprintf(out, "Search: %q — %d result(s)\n\n", cfg.query, len(results))
	}
	if len(results) == 0 {
		return nil
	}
	if near != nil {
		_, _ = fmt.Fprintln(out, "ID  NAME                 CATEGORY       CITY        DISTANCE (m)")
		_, _ = fmt.Fprintln(out, "--  -------------------  -------------  ----------  ------------")
	} else {
		_, _ = fmt.Fprintln(out, "ID  NAME                 CATEGORY       CITY")
		_, _ = fmt.Fprintln(out, "--  -------------------  -------------  ----------")
	}
	for _, p := range results {
		if p.Distance != nil {
			_, _ = fmt.Fprintf(out, "%-2d  %-19s  %-13s  %-10s  %.0f\n", p.ID, p.Name, p.Category, p.City, *p.Distance)
		} else {
			_, _ = fmt.Fprintf(out, "%-2d  %-19s  %-13s  %s\n", p.ID, p.Name, p.Category, p.City)
		}
	}
	return nil
}

func openPOIDatabase(ctx context.Context, cfg config) (*tinysql.DB, string, error) {
	if cfg.snapshot != "" && !cfg.rebuild {
		if _, err := os.Stat(cfg.snapshot); err == nil {
			db, err := tinysql.LoadFromFile(cfg.snapshot)
			if err != nil {
				return nil, "", fmt.Errorf("load snapshot: %w", err)
			}
			return db, "snapshot", nil
		} else if !os.IsNotExist(err) {
			return nil, "", fmt.Errorf("stat snapshot: %w", err)
		}
	}

	db := tinysql.NewDB()
	if err := seedPOIs(ctx, db); err != nil {
		return nil, "", err
	}
	if cfg.snapshot != "" {
		if err := tinysql.SaveToFile(db, cfg.snapshot); err != nil {
			return nil, "", fmt.Errorf("save snapshot: %w", err)
		}
		return db, "new snapshot", nil
	}
	return db, "in-memory dataset", nil
}

func seedPOIs(ctx context.Context, db *tinysql.DB) error {
	for _, sql := range []string{
		`CREATE TABLE poi (id INTEGER PRIMARY KEY, name TEXT, category TEXT, city TEXT, lat REAL, lon REAL)`,
		`INSERT INTO poi VALUES (1, 'Marienplatz', 'sight', 'München', 48.1372, 11.5756)`,
		`INSERT INTO poi VALUES (2, 'Deutsches Museum', 'museum', 'München', 48.1303, 11.5838)`,
		`INSERT INTO poi VALUES (3, 'Englischer Garten', 'park', 'München', 48.1642, 11.6050)`,
		`INSERT INTO poi VALUES (4, 'Brandenburger Tor', 'sight', 'Berlin', 52.5163, 13.3777)`,
		`INSERT INTO poi VALUES (5, 'Alte Pinakothek', 'museum', 'München', 48.1480, 11.5707)`,
	} {
		if _, err := tinysql.ExecSQL(ctx, db, "default", sql); err != nil {
			return fmt.Errorf("seed POIs: %w", err)
		}
	}
	return nil
}

func searchPOIs(ctx context.Context, db *tinysql.DB, query string) ([]poi, error) {
	if strings.TrimSpace(query) == "" {
		return nil, fmt.Errorf("query must not be empty")
	}
	return queryPOIs(ctx, db, query, nil)
}

// parseProximity accepts the same longitude,latitude ordering as GeoJSON and
// the routing demos. The empty pair selects ordinary text-only search.
func parseProximity(coords, radius string) (*proximity, error) {
	coords, radius = strings.TrimSpace(coords), strings.TrimSpace(radius)
	if coords == "" && radius == "" {
		return nil, nil
	}
	parts := strings.Split(coords, ",")
	if len(parts) != 2 || radius == "" {
		return nil, fmt.Errorf("nearby search requires longitude,latitude and a radius in metres")
	}
	parse := func(s string) (float64, error) {
		v, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
		if err != nil || math.IsNaN(v) || math.IsInf(v, 0) {
			return 0, fmt.Errorf("invalid nearby search number %q", s)
		}
		return v, nil
	}
	lon, err := parse(parts[0])
	if err != nil {
		return nil, err
	}
	lat, err := parse(parts[1])
	if err != nil {
		return nil, err
	}
	meters, err := parse(radius)
	if err != nil {
		return nil, err
	}
	if lon < -180 || lon > 180 || lat < -90 || lat > 90 || meters <= 0 {
		return nil, fmt.Errorf("longitude must be between -180 and 180, latitude between -90 and 90, and radius positive")
	}
	return &proximity{lon: lon, lat: lat, radius: meters}, nil
}

func queryPOIs(ctx context.Context, db *tinysql.DB, query string, near *proximity) ([]poi, error) {
	needle := strings.TrimSpace(query)
	// The demo builds a literal from a local CLI argument; production callers
	// should use the database/sql driver's bound parameters instead.
	needle = strings.ReplaceAll(needle, "'", "''")
	var conditions []string
	if needle != "" {
		pattern := "'%" + needle + "%'"
		conditions = append(conditions, fmt.Sprintf("(name LIKE %s OR city LIKE %s OR category LIKE %s)", pattern, pattern, pattern))
	}
	columns := "id, name, category, city, lat, lon"
	order := "name"
	if near != nil {
		number := func(v float64) string { return strconv.FormatFloat(v, 'f', -1, 64) }
		distance := "GEO_DISTANCE(" + number(near.lat) + ", " + number(near.lon) + ", lat, lon)"
		columns += ", " + distance + " AS distance_m"
		conditions = append(conditions, distance+" <= "+number(near.radius))
		order = "distance_m, name, id"
	}
	sql := "SELECT " + columns + " FROM poi"
	if len(conditions) > 0 {
		sql += " WHERE " + strings.Join(conditions, " AND ")
	}
	sql += " ORDER BY " + order
	rs, err := tinysql.ExecSQL(ctx, db, "default", sql)
	if err != nil {
		return nil, err
	}
	result := make([]poi, 0, len(rs.Rows))
	for _, row := range rs.Rows {
		p := poi{
			ID:       intValue(row, "id"),
			Name:     stringValue(row, "name"),
			Category: stringValue(row, "category"),
			City:     stringValue(row, "city"),
			Lat:      floatValue(row, "lat"),
			Lon:      floatValue(row, "lon"),
		}
		if near != nil {
			distance := floatValue(row, "distance_m")
			p.Distance = &distance
		}
		result = append(result, p)
	}
	return result, nil
}

func stringValue(row tinysql.Row, column string) string {
	v, _ := tinysql.GetVal(row, column)
	return fmt.Sprint(v)
}

func intValue(row tinysql.Row, column string) int {
	v, _ := tinysql.GetVal(row, column)
	switch n := v.(type) {
	case int:
		return n
	case int64:
		return int(n)
	case float64:
		return int(n)
	default:
		return 0
	}
}

func floatValue(row tinysql.Row, column string) float64 {
	v, _ := tinysql.GetVal(row, column)
	switch n := v.(type) {
	case float64:
		return n
	case float32:
		return float64(n)
	case int:
		return float64(n)
	case int64:
		return float64(n)
	default:
		return 0
	}
}
