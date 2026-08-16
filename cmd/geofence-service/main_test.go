package main

import (
	"context"
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
