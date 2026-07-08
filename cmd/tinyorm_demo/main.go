package main

import (
	"context"
	"fmt"
	"log"

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
	ctx := context.Background()
	db := tinyorm.New(tinysql.NewDB(), "default")

	if err := db.AutoMigrate(ctx, Place{}); err != nil {
		log.Fatal(err)
	}
	for _, place := range []Place{
		{ID: 1, Name: "Berlin", Country: "DE", Lat: 52.52, Lon: 13.405, Active: true},
		{ID: 2, Name: "Munich", Country: "DE", Lat: 48.1372, Lon: 11.5755, Active: true},
		{ID: 3, Name: "Zurich", Country: "CH", Lat: 47.3769, Lon: 8.5417, Active: false},
	} {
		if err := db.Insert(ctx, place); err != nil {
			log.Fatal(err)
		}
	}

	var germanPlaces []Place
	if err := db.Select(ctx, &germanPlaces, "country = :country AND active = :active", map[string]any{
		"country": "DE",
		"active":  true,
	}); err != nil {
		log.Fatal(err)
	}

	var berlin Place
	if err := db.FindByPK(ctx, &berlin, 1); err != nil {
		log.Fatal(err)
	}

	fmt.Printf("loaded by pk: %s %.4f %.4f\n", berlin.Name, berlin.Lat, berlin.Lon)
	fmt.Printf("active DE places: %d\n", len(germanPlaces))
	for _, place := range germanPlaces {
		fmt.Printf("- %s (%s)\n", place.Name, place.Country)
	}
}
