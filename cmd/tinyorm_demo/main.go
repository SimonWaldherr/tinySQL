package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"

	tinysql "github.com/SimonWaldherr/tinySQL"
	"github.com/SimonWaldherr/tinySQL/tinyorm"
)

type Place struct {
	ID      int     `db:"id,pk"`
	Name    string  `db:"name"`
	Country string  `db:"country"`
	Lat     float64 `db:"lat"`
	Lon     float64 `db:"lon"`
	Active  bool    `db:"active"`
}

func (Place) TableName() string { return "places" }

func main() {
	format := flag.String("format", "text", "Output format: text or json")
	includeInactive := flag.Bool("include-inactive", false, "Include inactive places in the country result")
	flag.Parse()
	if err := run(context.Background(), os.Stdout, *format, *includeInactive); err != nil {
		log.Fatal(err)
	}
}

// run demonstrates AutoMigrate, Insert, Select with named parameters, and
// FindByPK. Keeping it separate from main makes the example testable and lets
// scripts choose stable JSON output.
func run(ctx context.Context, out io.Writer, format string, includeInactive bool) error {
	db := tinyorm.New(tinysql.NewDB(), "default")

	if err := db.AutoMigrate(ctx, Place{}); err != nil {
		return err
	}
	for _, place := range []Place{
		{ID: 1, Name: "Berlin", Country: "DE", Lat: 52.52, Lon: 13.405, Active: true},
		{ID: 2, Name: "Munich", Country: "DE", Lat: 48.1372, Lon: 11.5755, Active: true},
		{ID: 3, Name: "Cologne", Country: "DE", Lat: 50.9375, Lon: 6.9603, Active: false},
		{ID: 4, Name: "Zurich", Country: "CH", Lat: 47.3769, Lon: 8.5417, Active: false},
	} {
		if err := db.Insert(ctx, place); err != nil {
			return err
		}
	}

	var germanPlaces []Place
	where := "country = :country AND active = :active"
	params := map[string]any{"country": "DE", "active": true}
	if includeInactive {
		where = "country = :country"
		params = map[string]any{"country": "DE"}
	}
	if err := db.Select(ctx, &germanPlaces, where, params); err != nil {
		return err
	}

	var berlin Place
	if err := db.FindByPK(ctx, &berlin, 1); err != nil {
		return err
	}

	if format == "json" {
		return json.NewEncoder(out).Encode(struct {
			LoadedByPK Place   `json:"loaded_by_pk"`
			Places     []Place `json:"places"`
		}{berlin, germanPlaces})
	}
	if format != "text" {
		return fmt.Errorf("unsupported format %q (use text or json)", format)
	}
	_, _ = fmt.Fprintf(out, "loaded by pk: %s %.4f %.4f\n", berlin.Name, berlin.Lat, berlin.Lon)
	_, _ = fmt.Fprintf(out, "DE places: %d\n", len(germanPlaces))
	for _, place := range germanPlaces {
		_, _ = fmt.Fprintf(out, "- %s (%s, active=%t)\n", place.Name, place.Country, place.Active)
	}
	return nil
}
