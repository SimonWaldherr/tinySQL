// routingdemo demonstrates fresh OSM routes without downloads or a running server.
package main

import (
	"context"
	_ "embed"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"time"

	"github.com/SimonWaldherr/tinySQL/routing"
)

//go:embed network.osm
var network string

const (
	defaultFrom = "10.9995,48.00001"
	defaultTo   = "11.0005,48.00001"
)

type report struct {
	Profile     routing.Profile `json:"profile"`
	BuildMicros int64           `json:"build_us"`
	QueryMicros float64         `json:"mean_query_us"`
	Iterations  int             `json:"iterations"`
	Route       routing.Result  `json:"route"`
}

func parsePoint(raw string) (routing.Point, error) {
	parts := strings.Split(raw, ",")
	if len(parts) != 2 {
		return routing.Point{}, fmt.Errorf("expected longitude,latitude")
	}
	lon, err := strconv.ParseFloat(strings.TrimSpace(parts[0]), 64)
	if err != nil {
		return routing.Point{}, fmt.Errorf("invalid longitude: %w", err)
	}
	lat, err := strconv.ParseFloat(strings.TrimSpace(parts[1]), 64)
	if err != nil {
		return routing.Point{}, fmt.Errorf("invalid latitude: %w", err)
	}
	if math.IsNaN(lon) || math.IsInf(lon, 0) || lon < -180 || lon > 180 ||
		math.IsNaN(lat) || math.IsInf(lat, 0) || lat < -90 || lat > 90 {
		return routing.Point{}, fmt.Errorf("coordinates must be finite WGS84 longitude,latitude")
	}
	return routing.Point{Lon: lon, Lat: lat}, nil
}

func parseProfiles(raw string) ([]routing.Profile, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "all":
		return []routing.Profile{routing.Car, routing.Bicycle, routing.Foot}, nil
	case "car":
		return []routing.Profile{routing.Car}, nil
	case "bicycle":
		return []routing.Profile{routing.Bicycle}, nil
	case "foot":
		return []routing.Profile{routing.Foot}, nil
	default:
		return nil, fmt.Errorf("unknown profile %q (choose all, car, bicycle, or foot)", raw)
	}
}

func runDemo(ctx context.Context, iterations int, request routing.Request, profiles []routing.Profile) ([]report, error) {
	if iterations < 1 || iterations > 10000 {
		return nil, fmt.Errorf("iterations must be between 1 and 10000")
	}
	if math.IsNaN(request.MaxSnapMeters) || math.IsInf(request.MaxSnapMeters, 0) || request.MaxSnapMeters <= 0 || request.MaxSnapMeters > 5000 {
		return nil, fmt.Errorf("max-snap-meters must be in (0,5000]")
	}
	if len(profiles) == 0 {
		return nil, fmt.Errorf("at least one profile is required")
	}
	data, err := routing.ReadOSM(ctx, strings.NewReader(network))
	if err != nil {
		return nil, err
	}
	reports := make([]report, 0, len(profiles))
	for _, profile := range profiles {
		started := time.Now()
		router, err := routing.Build(ctx, data, profile, routing.Options{})
		if err != nil {
			return nil, fmt.Errorf("profile %s: %w", profile, err)
		}
		build := time.Since(started).Microseconds()
		started = time.Now()
		var result routing.Result
		for i := 0; i < iterations; i++ {
			result, err = router.Route(ctx, request)
			if err != nil {
				return nil, fmt.Errorf("profile %s: %w", profile, err)
			}
		}
		reports = append(reports, report{profile, build, float64(time.Since(started).Nanoseconds()) / 1000 / float64(iterations), iterations, result})
	}
	return reports, nil
}

func main() {
	iterations := flag.Int("iterations", 1, "fresh route calculations per profile (1..10000)")
	asJSON := flag.Bool("json", false, "print results including GeoJSON")
	from := flag.String("from", defaultFrom, "start point as longitude,latitude")
	to := flag.String("to", defaultTo, "destination as longitude,latitude")
	profile := flag.String("profile", "all", "routing profile: all, car, bicycle, or foot")
	maxSnap := flag.Float64("max-snap-meters", 10, "maximum distance from a point to a road in metres (0,5000]")
	flag.Parse()
	start, err := parsePoint(*from)
	if err != nil {
		fmt.Fprintln(os.Stderr, "routingdemo: invalid -from:", err)
		os.Exit(1)
	}
	end, err := parsePoint(*to)
	if err != nil {
		fmt.Fprintln(os.Stderr, "routingdemo: invalid -to:", err)
		os.Exit(1)
	}
	profiles, err := parseProfiles(*profile)
	if err != nil {
		fmt.Fprintln(os.Stderr, "routingdemo:", err)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	request := routing.Request{From: start, To: end, MaxSnapMeters: *maxSnap}
	results, err := runDemo(ctx, *iterations, request, profiles)
	if err != nil {
		fmt.Fprintln(os.Stderr, "routingdemo:", err)
		os.Exit(1)
	}
	if *asJSON {
		if err := json.NewEncoder(os.Stdout).Encode(results); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	fmt.Println("Synthetic OSM network: a car turn restriction exempts bicycles and pedestrians.")
	fmt.Printf("From %.6f,%.6f to %.6f,%.6f; maximum snap %.1f m.\n", start.Lon, start.Lat, end.Lon, end.Lat, *maxSnap)
	fmt.Println("Every route is computed again; no answer cache.")
	fmt.Printf("%-9s %10s %10s %10s %14s\n", "Profile", "Metres", "Seconds", "Build µs", "Mean query µs")
	for _, r := range results {
		fmt.Printf("%-9s %10.1f %10.1f %10d %14.1f\n", r.Profile, r.Route.DistanceMeters, r.Route.DurationSeconds, r.BuildMicros, r.QueryMicros)
	}
	fmt.Printf("%d query/queries per selected profile; tiny synthetic example, not a production benchmark. Use -json for route geometry.\n", *iterations)
}
