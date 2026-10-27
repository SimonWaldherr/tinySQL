package main

import (
	"encoding/json"
	"errors"
	"math"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/SimonWaldherr/tinySQL/routing"
)

func TestDemoShowsRestrictedDetour(t *testing.T) {
	from, err := parsePoint(defaultFrom)
	if err != nil {
		t.Fatal(err)
	}
	to, err := parsePoint(defaultTo)
	if err != nil {
		t.Fatal(err)
	}
	profiles, err := parseProfiles("all")
	if err != nil {
		t.Fatal(err)
	}
	request := routing.Request{From: from, To: to, MaxSnapMeters: 10}
	results, err := runDemo(t.Context(), 2, request, profiles)
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
	if _, err := runDemo(t.Context(), 0, request, profiles); err == nil {
		t.Fatal("accepted zero iterations")
	}
}

func TestDemoCustomRouteAndValidation(t *testing.T) {
	point, err := parsePoint(" 11, 48.001 ")
	if err != nil || point != (routing.Point{Lon: 11, Lat: 48.001}) {
		t.Fatalf("point=%v err=%v", point, err)
	}
	for _, raw := range []string{"", "11", "11,48,0", "NaN,48", "11,Inf", "181,48", "11,91", "west,48"} {
		if _, err := parsePoint(raw); err == nil {
			t.Errorf("accepted invalid point %q", raw)
		}
	}
	profiles, err := parseProfiles(" Bicycle ")
	if err != nil || len(profiles) != 1 || profiles[0] != routing.Bicycle {
		t.Fatalf("profiles=%v err=%v", profiles, err)
	}
	if _, err := parseProfiles("bus"); err == nil {
		t.Fatal("accepted invalid profile")
	}
	request := routing.Request{
		From: routing.Point{Lon: 10.9995, Lat: 48.00001},
		To:   point, MaxSnapMeters: 10,
	}
	results, err := runDemo(t.Context(), 1, request, profiles)
	if err != nil || len(results) != 1 || results[0].Profile != routing.Bicycle || results[0].Route.DistanceMeters <= 0 {
		t.Fatalf("custom route=%v err=%v", results, err)
	}
	request.From = routing.Point{Lon: 13.4, Lat: 52.5}
	if _, err := runDemo(t.Context(), 1, request, profiles); !errors.Is(err, routing.ErrNoSnap) {
		t.Fatalf("distant point: got %v, want ErrNoSnap", err)
	}
	for _, radius := range []float64{0, -1, 5001, math.NaN(), math.Inf(1)} {
		request.MaxSnapMeters = radius
		if _, err := runDemo(t.Context(), 1, request, profiles); err == nil {
			t.Errorf("accepted invalid snap radius %v", radius)
		}
	}
}

func TestCommandCustomProfileJSONAndErrors(t *testing.T) {
	bin := filepath.Join(t.TempDir(), "routingdemo")
	if runtime.GOOS == "windows" {
		bin += ".exe"
	}
	if output, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, output)
	}
	output, err := exec.Command(bin, "-profile", "bicycle", "-from", "10.9995,48", "-to", "11.0005,48", "-json").CombinedOutput()
	if err != nil {
		t.Fatalf("custom route: %v\n%s", err, output)
	}
	var reports []report
	if err := json.Unmarshal(output, &reports); err != nil || len(reports) != 1 || reports[0].Profile != routing.Bicycle || reports[0].Route.Geometry.Type != "LineString" {
		t.Fatalf("unexpected JSON %s: %v", output, err)
	}
	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"-from", "NaN,48"}, "invalid -from"},
		{[]string{"-profile", "bus"}, "unknown profile"},
		{[]string{"-max-snap-meters", "0"}, "max-snap-meters"},
	} {
		output, err := exec.Command(bin, tc.args...).CombinedOutput()
		if err == nil || !strings.Contains(string(output), tc.want) {
			t.Errorf("args %v: output=%s err=%v", tc.args, output, err)
		}
	}
}
