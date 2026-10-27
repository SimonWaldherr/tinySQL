package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/SimonWaldherr/tinySQL/cmd/internal/webapp"
)

func TestRecordPositionCreatesOnlyTransitions(t *testing.T) {
	ctx := context.Background()
	db, err := webapp.Open(ctx, "mem://?tenant=geofence-test")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	a := &app{db: db}
	if err := a.bootstrap(ctx); err != nil {
		t.Fatal(err)
	}
	// First position establishes an outside state, second enters the seeded
	// centre polygon, third leaves it again.
	for i, p := range []positionInput{
		{VehicleID: 1, Lon: 11.50, Lat: 48.13},
		{VehicleID: 1, Lon: 11.58, Lat: 48.13},
		{VehicleID: 1, Lon: 11.50, Lat: 48.13},
	} {
		p.RecordedAt = int64(i + 1)
		events, err := a.recordPosition(ctx, p)
		if err != nil {
			t.Fatalf("position %d: %v", i, err)
		}
		if i == 0 && len(events) != 0 {
			t.Fatalf("first position generated %d events", len(events))
		}
	}
	events, err := a.listEvents(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 || events[0].Kind != "exited" || events[1].Kind != "entered" {
		t.Fatalf("events = %#v, want entered then exited", events)
	}
}

func TestEventFilters(t *testing.T) {
	ctx := context.Background()
	db, err := webapp.Open(ctx, "mem://?tenant=geofence-filters")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	a := &app{db: db}
	if err := a.bootstrap(ctx); err != nil {
		t.Fatal(err)
	}
	for i, p := range []positionInput{
		{VehicleID: 1, Lon: 11.50, Lat: 48.13},
		{VehicleID: 1, Lon: 11.58, Lat: 48.13},
		{VehicleID: 1, Lon: 11.50, Lat: 48.13},
		{VehicleID: 2, Lon: 11.50, Lat: 48.13},
		{VehicleID: 2, Lon: 11.58, Lat: 48.13},
	} {
		p.RecordedAt = int64(i + 1)
		if _, err := a.recordPosition(ctx, p); err != nil {
			t.Fatal(err)
		}
	}
	for _, tc := range []struct {
		query  string
		ids    []int64
		status int
	}{
		{"?vehicle_id=1&zone_id=1&kind=entered", []int64{1}, http.StatusOK},
		{"?vehicle_id=1&limit=1", []int64{2}, http.StatusOK},
		{"?vehicle_id=2", []int64{3}, http.StatusOK},
		{"?kind=invalid", nil, http.StatusBadRequest},
		{"?limit=201", nil, http.StatusBadRequest},
		{"?zone_id=0", nil, http.StatusBadRequest},
	} {
		t.Run(tc.query, func(t *testing.T) {
			res := httptest.NewRecorder()
			a.events(res, httptest.NewRequest(http.MethodGet, "/api/events"+tc.query, nil))
			if res.Code != tc.status {
				t.Fatalf("status %d: %s", res.Code, res.Body.String())
			}
			if tc.status != http.StatusOK {
				return
			}
			var got []event
			if err := json.Unmarshal(res.Body.Bytes(), &got); err != nil {
				t.Fatal(err)
			}
			if len(got) != len(tc.ids) {
				t.Fatalf("got %#v", got)
			}
			for i := range got {
				if got[i].ID != tc.ids[i] {
					t.Fatalf("got %#v", got)
				}
			}
		})
	}
}
