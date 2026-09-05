package routing

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Handler exposes GET/POST /route, GET /nearest, /profiles and /healthz.
// Its profile map is copied; replace the handler to publish a new snapshot.
func Handler(routers map[Profile]*Router) http.Handler {
	profiles := make(map[Profile]*Router, len(routers))
	for p, r := range routers {
		if r != nil {
			profiles[p] = r
		}
	}
	point := func(raw string) (Point, error) {
		parts := strings.Split(raw, ",")
		if len(parts) != 2 {
			return Point{}, fmt.Errorf("expected longitude,latitude")
		}
		lon, e := strconv.ParseFloat(parts[0], 64)
		if e != nil {
			return Point{}, e
		}
		lat, e := strconv.ParseFloat(parts[1], 64)
		p := Point{lon, lat}
		if e != nil {
			return Point{}, e
		}
		return p, p.validate()
	}
	write := func(w http.ResponseWriter, status int, value any) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(value)
	}
	failure := func(w http.ResponseWriter, err error) {
		status := http.StatusBadRequest
		switch {
		case errors.Is(err, ErrNoSnap), errors.Is(err, ErrNoRoute):
			status = http.StatusNotFound
		case errors.Is(err, ErrSearchLimit):
			status = http.StatusServiceUnavailable
		case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
			status = http.StatusRequestTimeout
		}
		write(w, status, map[string]string{"error": err.Error()})
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) })
	mux.HandleFunc("GET /profiles", func(w http.ResponseWriter, r *http.Request) {
		out := make([]Stats, 0, len(profiles))
		for _, p := range []Profile{Car, Bicycle, Foot} {
			if router := profiles[p]; router != nil {
				out = append(out, router.Stats())
			}
		}
		write(w, 200, out)
	})
	route := func(w http.ResponseWriter, req *http.Request) {
		var input struct {
			Request
			Profile Profile `json:"profile"`
		}
		if req.Method == http.MethodPost {
			decoder := json.NewDecoder(http.MaxBytesReader(w, req.Body, 8192))
			decoder.DisallowUnknownFields()
			var body struct {
				From          *Point  `json:"from"`
				To            *Point  `json:"to"`
				Profile       Profile `json:"profile"`
				MaxSnapMeters float64 `json:"max_snap_meters"`
			}
			if err := decoder.Decode(&body); err != nil {
				failure(w, err)
				return
			}
			if body.From == nil || body.To == nil {
				failure(w, fmt.Errorf("from and to are required"))
				return
			}
			input.Request = Request{From: *body.From, To: *body.To, MaxSnapMeters: body.MaxSnapMeters}
			input.Profile = body.Profile
			var extra any
			if err := decoder.Decode(&extra); err != io.EOF {
				failure(w, fmt.Errorf("expected one JSON request"))
				return
			}
		} else {
			q := req.URL.Query()
			input.Profile = Profile(q.Get("profile"))
			var err error
			input.From, err = point(q.Get("from"))
			if err != nil {
				failure(w, err)
				return
			}
			input.To, err = point(q.Get("to"))
			if err != nil {
				failure(w, err)
				return
			}
			if raw := q.Get("max_snap_meters"); raw != "" {
				input.MaxSnapMeters, err = strconv.ParseFloat(raw, 64)
				if err != nil {
					failure(w, err)
					return
				}
			}
		}
		if input.Profile == "" {
			input.Profile = Car
		}
		router := profiles[input.Profile]
		if router == nil {
			failure(w, fmt.Errorf("unknown profile %q", input.Profile))
			return
		}
		ctx, cancel := context.WithTimeout(req.Context(), 10*time.Second)
		defer cancel()
		result, err := router.Route(ctx, input.Request)
		if err != nil {
			failure(w, err)
			return
		}
		write(w, 200, result)
	}
	mux.HandleFunc("GET /route", route)
	mux.HandleFunc("POST /route", route)
	mux.HandleFunc("GET /nearest", func(w http.ResponseWriter, req *http.Request) {
		q := req.URL.Query()
		p := Profile(q.Get("profile"))
		if p == "" {
			p = Car
		}
		router := profiles[p]
		if router == nil {
			failure(w, fmt.Errorf("unknown profile %q", p))
			return
		}
		pos, err := point(q.Get("point"))
		if err != nil {
			failure(w, err)
			return
		}
		radius := 100.0
		if raw := q.Get("max_snap_meters"); raw != "" {
			radius, err = strconv.ParseFloat(raw, 64)
			if err != nil {
				failure(w, err)
				return
			}
		}
		snap, err := router.Nearest(req.Context(), pos, radius)
		if err != nil {
			failure(w, err)
			return
		}
		write(w, 200, snap)
	})
	return mux
}
