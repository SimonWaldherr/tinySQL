// Command geofence-service is a small, persistent GPS geofencing service with
// a browser UI. It is intentionally useful without any cloud dependency: a
// device can POST positions, operators manage GeoJSON zones, and entry/exit
// events remain queryable in the local TinySQL database.
package main

import (
	"context"
	"database/sql"
	"embed"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"html/template"
	"io"
	"log"
	"math"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/SimonWaldherr/tinySQL/cmd/internal/webapp"
)

//go:embed templates/*.html static/*
var assets embed.FS

type app struct {
	db  *sql.DB
	tpl *template.Template
}

type vehicle struct {
	ID         int64   `json:"id"`
	Name       string  `json:"name"`
	LastLon    float64 `json:"last_lon,omitempty"`
	LastLat    float64 `json:"last_lat,omitempty"`
	RecordedAt int64   `json:"recorded_at,omitempty"`
}

type zone struct {
	ID       int64  `json:"id"`
	Name     string `json:"name"`
	Geometry string `json:"geometry"`
}

type event struct {
	ID         int64   `json:"id"`
	VehicleID  int64   `json:"vehicle_id"`
	Vehicle    string  `json:"vehicle"`
	ZoneID     int64   `json:"zone_id"`
	Zone       string  `json:"zone"`
	Kind       string  `json:"kind"`
	Lon        float64 `json:"lon"`
	Lat        float64 `json:"lat"`
	OccurredAt int64   `json:"occurred_at"`
}

type positionInput struct {
	VehicleID  int64   `json:"vehicle_id"`
	Lon        float64 `json:"lon"`
	Lat        float64 `json:"lat"`
	RecordedAt int64   `json:"recorded_at"`
}

type zoneInput struct {
	Name     string          `json:"name"`
	Geometry json.RawMessage `json:"geometry"`
}

type vehicleInput struct {
	Name string `json:"name"`
}

func main() {
	addr := flag.String("addr", ":8091", "HTTP listen address")
	dsn := flag.String("dsn", "file:geofence.db?autosave=1", "TinySQL DSN")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
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
	a := &app{db: db, tpl: tpl}
	if err := a.bootstrap(ctx); err != nil {
		log.Fatalf("prepare database: %v", err)
	}

	mux := http.NewServeMux()
	a.routes(mux)
	mux.Handle("GET /static/", http.FileServer(http.FS(assets)))
	log.Printf("geofence-service listening on %s", *addr)
	log.Fatal(http.ListenAndServe(*addr, webapp.SecurityHeaders(mux)))
}

func (a *app) bootstrap(ctx context.Context) error {
	if err := webapp.Apply(ctx, a.db,
		`CREATE TABLE IF NOT EXISTS vehicles (
			id INT PRIMARY KEY, name TEXT NOT NULL UNIQUE
		)`,
		`CREATE TABLE IF NOT EXISTS zones (
			id INT PRIMARY KEY, name TEXT NOT NULL UNIQUE, geometry TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS positions (
			id INT PRIMARY KEY, vehicle_id INT NOT NULL, lon FLOAT NOT NULL, lat FLOAT NOT NULL,
			recorded_at INT NOT NULL, geometry TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS geofence_state (
			vehicle_id INT NOT NULL, zone_id INT NOT NULL, inside BOOL NOT NULL, updated_at INT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS geofence_events (
			id INT PRIMARY KEY, vehicle_id INT NOT NULL, zone_id INT NOT NULL, kind TEXT NOT NULL,
			lon FLOAT NOT NULL, lat FLOAT NOT NULL, occurred_at INT NOT NULL
		)`,
	); err != nil {
		return err
	}
	var n int
	if err := a.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM vehicles").Scan(&n); err != nil {
		return err
	}
	if n != 0 {
		return nil
	}
	// A small real-world-shaped seed makes the UI immediately inspectable. It
	// can be deleted or replaced through the API without changing the schema.
	if _, err := a.db.ExecContext(ctx, `INSERT INTO vehicles (id, name) VALUES
		(1, 'Lieferwagen Nord'), (2, 'Service-Team')`); err != nil {
		return err
	}
	_, err := a.db.ExecContext(ctx, `INSERT INTO zones (id, name, geometry) VALUES
		(1, 'München Zentrum', '{"type":"Polygon","coordinates":[[[11.55,48.12],[11.61,48.12],[11.61,48.16],[11.55,48.16],[11.55,48.12]]]}'),
		(2, 'Depot', '{"type":"Polygon","coordinates":[[[11.50,48.10],[11.53,48.10],[11.53,48.12],[11.50,48.12],[11.50,48.10]]]}')`)
	return err
}

func (a *app) routes(mux *http.ServeMux) {
	mux.HandleFunc("GET /", a.index)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		webapp.JSON(w, http.StatusOK, map[string]bool{"ok": true})
	})
	mux.HandleFunc("GET /api/state", a.state)
	mux.HandleFunc("GET /api/events", a.events)
	mux.HandleFunc("POST /api/positions", a.postPosition)
	mux.HandleFunc("POST /api/zones", a.postZone)
	mux.HandleFunc("POST /api/vehicles", a.postVehicle)
}

func (a *app) index(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := a.tpl.ExecuteTemplate(w, "index.html", nil); err != nil {
		http.Error(w, "template error", http.StatusInternalServerError)
	}
}

func (a *app) state(w http.ResponseWriter, r *http.Request) {
	vehicles, err := a.vehicles(r.Context())
	if err != nil {
		webapp.Error(w, r, http.StatusInternalServerError, "could not load vehicles")
		return
	}
	zones, err := a.zones(r.Context())
	if err != nil {
		webapp.Error(w, r, http.StatusInternalServerError, "could not load zones")
		return
	}
	events, err := a.listEvents(r.Context(), 30)
	if err != nil {
		webapp.Error(w, r, http.StatusInternalServerError, "could not load events")
		return
	}
	webapp.JSON(w, http.StatusOK, map[string]any{"vehicles": vehicles, "zones": zones, "events": events})
}

func (a *app) events(w http.ResponseWriter, r *http.Request) {
	items, err := a.listEvents(r.Context(), 200)
	if err != nil {
		webapp.Error(w, r, http.StatusInternalServerError, "could not load events")
		return
	}
	webapp.JSON(w, http.StatusOK, items)
}

func (a *app) postVehicle(w http.ResponseWriter, r *http.Request) {
	var in vehicleInput
	if err := decodeJSON(w, r, &in); err != nil {
		webapp.Error(w, r, http.StatusBadRequest, err.Error())
		return
	}
	in.Name = strings.TrimSpace(in.Name)
	if in.Name == "" {
		webapp.Error(w, r, http.StatusBadRequest, "name is required")
		return
	}
	ctx, cancel := webapp.ContextWithTimeout(r.Context())
	defer cancel()
	id, err := webapp.NextID(ctx, a.db, "vehicles")
	if err == nil {
		_, err = a.db.ExecContext(ctx, "INSERT INTO vehicles (id, name) VALUES (?, ?)", id, in.Name)
	}
	if err != nil {
		webapp.Error(w, r, http.StatusConflict, "vehicle could not be created")
		return
	}
	webapp.JSON(w, http.StatusCreated, vehicle{ID: id, Name: in.Name})
}

func (a *app) postZone(w http.ResponseWriter, r *http.Request) {
	var in zoneInput
	if err := decodeJSON(w, r, &in); err != nil {
		webapp.Error(w, r, http.StatusBadRequest, err.Error())
		return
	}
	in.Name = strings.TrimSpace(in.Name)
	if in.Name == "" || !json.Valid(in.Geometry) {
		webapp.Error(w, r, http.StatusBadRequest, "name and valid GeoJSON geometry are required")
		return
	}
	geometry := string(in.Geometry)
	var valid bool
	if err := a.db.QueryRowContext(r.Context(), "SELECT GEO_IS_VALID(?)", geometry).Scan(&valid); err != nil || !valid {
		webapp.Error(w, r, http.StatusBadRequest, "geometry must be a valid GeoJSON polygon")
		return
	}
	ctx, cancel := webapp.ContextWithTimeout(r.Context())
	defer cancel()
	id, err := webapp.NextID(ctx, a.db, "zones")
	if err == nil {
		_, err = a.db.ExecContext(ctx, "INSERT INTO zones (id, name, geometry) VALUES (?, ?, ?)", id, in.Name, geometry)
	}
	if err != nil {
		webapp.Error(w, r, http.StatusConflict, "zone could not be created")
		return
	}
	webapp.JSON(w, http.StatusCreated, zone{ID: id, Name: in.Name, Geometry: geometry})
}

func (a *app) postPosition(w http.ResponseWriter, r *http.Request) {
	var in positionInput
	if err := decodeJSON(w, r, &in); err != nil {
		webapp.Error(w, r, http.StatusBadRequest, err.Error())
		return
	}
	events, err := a.recordPosition(r.Context(), in)
	if err != nil {
		if errors.Is(err, errInvalidPosition) {
			webapp.Error(w, r, http.StatusBadRequest, err.Error())
		} else if errors.Is(err, sql.ErrNoRows) {
			webapp.Error(w, r, http.StatusNotFound, "vehicle not found")
		} else {
			webapp.Error(w, r, http.StatusInternalServerError, "position could not be saved")
		}
		return
	}
	webapp.JSON(w, http.StatusCreated, map[string]any{"events": events})
}

var errInvalidPosition = errors.New("vehicle_id, longitude and latitude are required and must be in range")

// recordPosition persists a position and atomically derives zone transitions.
// The first position only establishes state; it does not claim an artificial
// entry event for a vehicle whose prior position is unknown.
func (a *app) recordPosition(ctx context.Context, in positionInput) ([]event, error) {
	if in.VehicleID < 1 || math.IsNaN(in.Lon) || math.IsNaN(in.Lat) || in.Lon < -180 || in.Lon > 180 || in.Lat < -90 || in.Lat > 90 {
		return nil, errInvalidPosition
	}
	if in.RecordedAt == 0 {
		in.RecordedAt = time.Now().UTC().Unix()
	}
	geometry := fmt.Sprintf(`{"type":"Point","coordinates":[%.7f,%.7f]}`, in.Lon, in.Lat)
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var exists int
	if err := tx.QueryRowContext(ctx, "SELECT id FROM vehicles WHERE id = ?", in.VehicleID).Scan(&exists); err != nil {
		return nil, err
	}
	positionID, err := webapp.NextID(ctx, tx, "positions")
	if err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO positions
		(id, vehicle_id, lon, lat, recorded_at, geometry) VALUES (?, ?, ?, ?, ?, ?)`,
		positionID, in.VehicleID, in.Lon, in.Lat, in.RecordedAt, geometry); err != nil {
		return nil, err
	}
	zones, err := queryZones(ctx, tx)
	if err != nil {
		return nil, err
	}
	created := make([]event, 0)
	for _, z := range zones {
		var inside bool
		if err := tx.QueryRowContext(ctx, "SELECT GEO_WITHIN_POLYGON(?, ?)", geometry, z.Geometry).Scan(&inside); err != nil {
			return nil, fmt.Errorf("evaluate zone %q: %w", z.Name, err)
		}
		var previous bool
		err := tx.QueryRowContext(ctx, "SELECT inside FROM geofence_state WHERE vehicle_id = ? AND zone_id = ?", in.VehicleID, z.ID).Scan(&previous)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return nil, err
		}
		if errors.Is(err, sql.ErrNoRows) {
			if _, err := tx.ExecContext(ctx, "INSERT INTO geofence_state (vehicle_id, zone_id, inside, updated_at) VALUES (?, ?, ?, ?)", in.VehicleID, z.ID, inside, in.RecordedAt); err != nil {
				return nil, err
			}
			continue
		}
		if _, err := tx.ExecContext(ctx, "UPDATE geofence_state SET inside = ?, updated_at = ? WHERE vehicle_id = ? AND zone_id = ?", inside, in.RecordedAt, in.VehicleID, z.ID); err != nil {
			return nil, err
		}
		if previous == inside {
			continue
		}
		id, err := webapp.NextID(ctx, tx, "geofence_events")
		if err != nil {
			return nil, err
		}
		kind := "exited"
		if inside {
			kind = "entered"
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO geofence_events
			(id, vehicle_id, zone_id, kind, lon, lat, occurred_at) VALUES (?, ?, ?, ?, ?, ?, ?)`,
			id, in.VehicleID, z.ID, kind, in.Lon, in.Lat, in.RecordedAt); err != nil {
			return nil, err
		}
		created = append(created, event{ID: id, VehicleID: in.VehicleID, ZoneID: z.ID, Zone: z.Name, Kind: kind, Lon: in.Lon, Lat: in.Lat, OccurredAt: in.RecordedAt})
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return created, nil
}

func (a *app) vehicles(ctx context.Context) ([]vehicle, error) {
	rows, err := a.db.QueryContext(ctx, "SELECT id, name FROM vehicles ORDER BY name")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []vehicle
	for rows.Next() {
		var v vehicle
		if err := rows.Scan(&v.ID, &v.Name); err != nil {
			return nil, err
		}
		_ = a.db.QueryRowContext(ctx, `SELECT lon, lat, recorded_at FROM positions WHERE vehicle_id = ? ORDER BY recorded_at DESC LIMIT 1`, v.ID).Scan(&v.LastLon, &v.LastLat, &v.RecordedAt)
		result = append(result, v)
	}
	return result, rows.Err()
}

func (a *app) zones(ctx context.Context) ([]zone, error) { return queryZones(ctx, a.db) }

type queryRower interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func queryZones(ctx context.Context, db queryRower) ([]zone, error) {
	rows, err := db.QueryContext(ctx, "SELECT id, name, geometry FROM zones ORDER BY name")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []zone
	for rows.Next() {
		var z zone
		if err := rows.Scan(&z.ID, &z.Name, &z.Geometry); err != nil {
			return nil, err
		}
		result = append(result, z)
	}
	return result, rows.Err()
}

func (a *app) listEvents(ctx context.Context, limit int) ([]event, error) {
	rows, err := a.db.QueryContext(ctx, `SELECT e.id, e.vehicle_id, v.name, e.zone_id, z.name, e.kind, e.lon, e.lat, e.occurred_at
		FROM geofence_events e JOIN vehicles v ON v.id = e.vehicle_id JOIN zones z ON z.id = e.zone_id
		ORDER BY e.occurred_at DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]event, 0)
	for rows.Next() {
		var e event
		if err := rows.Scan(&e.ID, &e.VehicleID, &e.Vehicle, &e.ZoneID, &e.Zone, &e.Kind, &e.Lon, &e.Lat, &e.OccurredAt); err != nil {
			return nil, err
		}
		result = append(result, e)
	}
	return result, rows.Err()
}

func decodeJSON(w http.ResponseWriter, r *http.Request, dst any) error {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	defer r.Body.Close()
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return fmt.Errorf("invalid JSON")
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		return fmt.Errorf("request must contain one JSON value")
	}
	return nil
}

// sortedEvents is only used by tests and keeps expected output deterministic.
func sortedEvents(in []event) []event {
	sort.Slice(in, func(i, j int) bool { return in[i].ID < in[j].ID })
	return in
}
