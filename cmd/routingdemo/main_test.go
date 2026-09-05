package main

import (
	"github.com/SimonWaldherr/tinySQL/routing"
	"testing"
)

func TestDemoShowsRestrictedDetour(t *testing.T) {
	results, err := runDemo(t.Context(), 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 3 {
		t.Fatal("missing profiles")
	}
	if results[0].Profile != routing.Car || results[0].Route.DistanceMeters < results[1].Route.DistanceMeters+200 {
		t.Fatal("car did not detour")
	}
	for _, r := range results {
		if r.Route.Geometry.Type != "LineString" || len(r.Route.Geometry.Coordinates) < 2 {
			t.Fatal("missing geometry")
		}
		if r.Route.From.DistanceMeters < 0.5 {
			t.Fatal("demo did not snap off-road endpoint")
		}
	}
	if _, err := runDemo(t.Context(), 0); err == nil {
		t.Fatal("accepted zero iterations")
	}
}
