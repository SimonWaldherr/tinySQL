// routingdemo demonstrates fresh OSM routes without downloads or a running server.
package main

import (
	"context"
	_ "embed"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/SimonWaldherr/tinySQL/routing"
)

//go:embed network.osm
var network string

type report struct {
	Profile     routing.Profile `json:"profile"`
	BuildMicros int64           `json:"build_us"`
	QueryMicros float64         `json:"mean_query_us"`
	Iterations  int             `json:"iterations"`
	Route       routing.Result  `json:"route"`
}

func runDemo(ctx context.Context, iterations int) ([]report, error) {
	if iterations < 1 || iterations > 10000 {
		return nil, fmt.Errorf("iterations must be between 1 and 10000")
	}
	data, err := routing.ReadOSM(ctx, strings.NewReader(network))
	if err != nil {
		return nil, err
	}
	reports := make([]report, 0, 3)
	// Endpoints sit off the road and halfway along the first/last segments.
	request := routing.Request{From: routing.Point{Lon: 10.9995, Lat: 48.00001}, To: routing.Point{Lon: 11.0005, Lat: 48.00001}, MaxSnapMeters: 10}
	for _, profile := range []routing.Profile{routing.Car, routing.Bicycle, routing.Foot} {
		started := time.Now()
		router, err := routing.Build(ctx, data, profile, routing.Options{})
		if err != nil {
			return nil, err
		}
		build := time.Since(started).Microseconds()
		started = time.Now()
		var result routing.Result
		for i := 0; i < iterations; i++ {
			result, err = router.Route(ctx, request)
			if err != nil {
				return nil, err
			}
		}
		reports = append(reports, report{profile, build, float64(time.Since(started).Nanoseconds()) / 1000 / float64(iterations), iterations, result})
	}
	return reports, nil
}

func main() {
	iterations := flag.Int("iterations", 1, "fresh route calculations per profile (1..10000)")
	asJSON := flag.Bool("json", false, "print results including GeoJSON")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	results, err := runDemo(ctx, *iterations)
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
	fmt.Println("Synthetic OSM network: car must detour; bicycle is exempt; foot ignores the vehicle turn restriction.")
	fmt.Println("Endpoints are snapped onto partial road segments. Every route is computed again; no answer cache.")
	fmt.Printf("%-9s %10s %10s %10s %14s\n", "Profile", "Metres", "Seconds", "Build µs", "Mean query µs")
	for _, r := range results {
		fmt.Printf("%-9s %10.1f %10.1f %10d %14.1f\n", r.Profile, r.Route.DistanceMeters, r.Route.DurationSeconds, r.BuildMicros, r.QueryMicros)
	}
	fmt.Printf("%d query/queries per profile; tiny synthetic example, not a production benchmark. Use -json for route geometry.\n", *iterations)
}
