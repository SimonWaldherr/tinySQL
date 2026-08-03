//go:build sqliteimport && !js && !wasm && !baremetal

package main

// Deterministic, procedurally-named points of interest scattered across the
// generative world (see noise.go), used to demonstrate tinySQL's GEO_*
// functions (GEO_DISTANCE, GEO_BEARING, GEO_MIDPOINT — see
// internal/engine/geo_functions.go) in the browser demo the same way the
// tiles table demonstrates TILE_*.
//
// Settlements are placed only on "habitable" elevation bands (grassland
// through forest — never ocean, coast, or high mountain/snow), so each one
// reads as plausible on the terrain it sits on even though neither the world
// nor the names are real. Names, positions and attributes are all derived
// from the same hash() used to build the terrain (noise.go), so re-running
// the generator reproduces the identical settlement set.

import (
	"encoding/json"
	"fmt"
	"math"
)

// settlement is one point of interest: a GeoJSON Point plus flavor fields
// (biome, population) derived from the same elevation/moisture fields that
// painted the tile under it.
type settlement struct {
	Name       string
	Lon, Lat   float64
	Biome      string
	Population int
}

// namePrefixes/nameSuffixes combine into short, plausible-sounding place
// names (Ashford, Ravenmere, ...) without claiming to be real toponyms —
// consistent with the rest of this generator, which is explicit that
// nothing here is real geography.
var namePrefixes = []string{
	"Ash", "Elm", "Oak", "Thorn", "Green", "Stone", "River", "Wolf",
	"Raven", "Fern", "Bramble", "Hollow", "Moor", "Bright", "Storm",
	"North", "South", "Amber", "Silver", "Iron", "Fox", "Hawk",
	"Wren", "Cedar", "Birch",
}

var nameSuffixes = []string{
	"ford", "haven", "wick", "burg", "stead", "hollow", "dale", "port",
	"ridge", "mere", "field", "gate", "cross", "reach", "vale", "moor",
	"bridge", "hurst",
}

// settlementGridCells is how many candidate cells the placement scan checks
// per axis; settlementDensity is the fraction of habitable cells that end up
// with a settlement. Both are tuned so the map reads as a scattering of
// towns rather than either an empty world or a solid grid of markers.
const (
	settlementGridCells = 24
	settlementDensity   = 0.16
)

// buildSettlements scans a coarse grid over the whole world, keeping cells
// whose elevation lands in a habitable band and whose placement hash clears
// the density threshold, then names and jitters each into a point within its
// cell so settlements don't all sit on a visible lattice.
func buildSettlements() []settlement {
	var out []settlement
	used := make(map[string]bool)
	for gy := 0; gy < settlementGridCells; gy++ {
		for gx := 0; gx < settlementGridCells; gx++ {
			cu := (float64(gx) + 0.5) / settlementGridCells
			cv := (float64(gy) + 0.5) / settlementGridCells
			elev := fbm(cu, cv, 1, 4, 3)
			if elev < 0.50 || elev > 0.80 {
				continue // ocean, coast, highland or snow: left uninhabited
			}
			if hash(int32(gx), int32(gy), 7001) > settlementDensity {
				continue
			}

			ju := hash(int32(gx), int32(gy), 9001)
			jv := hash(int32(gx), int32(gy), 9002)
			u := (float64(gx) + ju) / settlementGridCells
			v := (float64(gy) + jv) / settlementGridCells

			moist := fbm(u, v, 4201, 3, 2)
			popRoll := hash(int32(gx), int32(gy), 9003)
			out = append(out, settlement{
				Name:       settlementName(gx, gy, used),
				Lon:        lonForU(u),
				Lat:        latForV(v),
				Biome:      biomeFor(elev, moist),
				Population: 200 + int(popRoll*49_800),
			})
		}
	}
	return out
}

// lonForU/latForV convert the same continuous [0,1) world-space coordinates
// noise.go samples into real longitude/latitude, using the identical inverse
// Web Mercator formula tinySQL's own TILE_LAT applies to a tile row fraction
// (see tileNorthLat in internal/engine/tile_functions.go) — so a settlement's
// declared position matches the terrain pixel actually rendered under it,
// not just an approximation of it.
func lonForU(u float64) float64 { return u*360 - 180 }

func latForV(v float64) float64 {
	n := math.Pi * (1 - 2*v)
	return 180 / math.Pi * math.Atan(math.Sinh(n))
}

// settlementName derives a hash-based prefix+suffix combination and walks
// forward through suffixes (then prefixes) on a collision, so every name is
// still fully determined by (gx, gy) and generation order -- re-running the
// generator reproduces the identical, unique name set -- rather than
// silently letting two settlements share a name, which would violate the
// settlements table's unique name index in addSettlementsTable (main.go).
func settlementName(gx, gy int, used map[string]bool) string {
	pi := nameIndex(hash(int32(gx), int32(gy), 5001), len(namePrefixes))
	si := nameIndex(hash(int32(gx), int32(gy), 5002), len(nameSuffixes))
	for attempt := 0; attempt < len(namePrefixes)*len(nameSuffixes); attempt++ {
		name := namePrefixes[pi] + nameSuffixes[si]
		if !used[name] {
			used[name] = true
			return name
		}
		si = (si + 1) % len(nameSuffixes)
		if si == 0 {
			pi = (pi + 1) % len(namePrefixes)
		}
	}
	// Every prefix+suffix combination is taken -- unreachable at this
	// generator's settlement count (a small fraction of
	// len(namePrefixes)*len(nameSuffixes) combinations), but a guaranteed-
	// unique fallback beats silently violating the unique index.
	name := fmt.Sprintf("%s%s-%d", namePrefixes[pi], nameSuffixes[si], gx*settlementGridCells+gy)
	used[name] = true
	return name
}

func nameIndex(roll float64, n int) int {
	i := int(roll * float64(n))
	if i >= n {
		i = n - 1
	}
	return i
}

// biomeFor labels a settlement using the same elevation/moisture bands
// tileimage.go paints, so the label matches what the map actually shows.
func biomeFor(elev, moist float64) string {
	switch {
	case elev < 0.66:
		if moist > 0.5 {
			return "Grassland"
		}
		return "Plains"
	case elev < 0.76:
		return "Forest"
	default:
		return "Highland"
	}
}

// geoPointJSON encodes (lon, lat) as the GeoJSON Point string tinySQL's
// GEO_* functions and GEO_POINT() itself both produce, so a settlement's
// geometry column round-trips through GEO_DISTANCE/GEO_BEARING/GEO_MIDPOINT
// without any conversion in SQL.
func geoPointJSON(lon, lat float64) (string, error) {
	body, err := json.Marshal(map[string]any{"type": "Point", "coordinates": []float64{lon, lat}})
	if err != nil {
		return "", err
	}
	return string(body), nil
}
