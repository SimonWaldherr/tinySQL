// Package routing compiles tinySQL OSM tables into immutable, profile-specific
// road graphs. Queries use edge snapping and restriction-aware A* without an
// answer cache. Routers are safe for concurrent use.
package routing

import (
	"context"
	"errors"
	"fmt"
	"math"
)

type Profile string

const (
	Car     Profile = "car"
	Bicycle Profile = "bicycle"
	Foot    Profile = "foot"
)

var ErrNoRoute = errors.New("routing: no legal route")
var ErrNoSnap = errors.New("routing: no eligible road within snap radius")
var ErrSearchLimit = errors.New("routing: search state limit exceeded")

type Point struct {
	Lon float64 `json:"lon"`
	Lat float64 `json:"lat"`
}

func (p Point) validate() error {
	if math.IsNaN(p.Lon) || math.IsNaN(p.Lat) || math.IsInf(p.Lon, 0) || math.IsInf(p.Lat, 0) || p.Lon < -180 || p.Lon > 180 || p.Lat < -90 || p.Lat > 90 {
		return fmt.Errorf("routing: invalid WGS84 coordinate")
	}
	return nil
}

type Node struct {
	ID    int64
	Point Point
	Tags  map[string]string
}
type Way struct {
	ID    int64
	Nodes []int64
	Tags  map[string]string
}
type Member struct {
	Type string `json:"type"`
	Ref  int64  `json:"ref"`
	Role string `json:"role"`
}
type Relation struct {
	ID      int64
	Members []Member
	Tags    map[string]string
}
type Data struct {
	Nodes     []Node
	Ways      []Way
	Relations []Relation
}
type Options struct {
	MaxStates              int
	MaxRestrictionPrefixes int
}
type Request struct {
	From          Point   `json:"from"`
	To            Point   `json:"to"`
	MaxSnapMeters float64 `json:"max_snap_meters,omitempty"`
}
type Snap struct {
	Point          Point   `json:"point"`
	DistanceMeters float64 `json:"distance_meters"`
	WayID          int64   `json:"way_id"`
	Fraction       float64 `json:"fraction"`
	segment        int
}
type Geometry struct {
	Type        string       `json:"type"`
	Coordinates [][2]float64 `json:"coordinates"`
}
type Result struct {
	Profile         Profile  `json:"profile"`
	DistanceMeters  float64  `json:"distance_meters"`
	DurationSeconds float64  `json:"duration_seconds"`
	From            Snap     `json:"from"`
	To              Snap     `json:"to"`
	Geometry        Geometry `json:"geometry"`
	VisitedStates   int      `json:"visited_states"`
}
type node struct {
	id    int64
	point Point
	xyz   vec3
}
type edge struct {
	from, to, segment int
	seconds, meters   float64
}
type segment struct {
	a, b              int
	way               int64
	forward, backward int
	meters            float64
	box               box3
}
type Router struct {
	profile      Profile
	nodes        []node
	edges        []edge
	segments     []segment
	offsets, adj []int
	spatial      []bvhNode
	spatialOrder []int
	rules        automaton
	maxSpeed     float64
	maxStates    int
}
type Stats struct {
	Profile             Profile `json:"profile"`
	Nodes               int     `json:"nodes"`
	DirectedEdges       int     `json:"directed_edges"`
	Segments            int     `json:"segments"`
	RestrictionPrefixes int     `json:"restriction_prefixes"`
}

func (r *Router) Stats() Stats {
	return Stats{r.profile, len(r.nodes), len(r.edges), len(r.segments), len(r.rules.nodes) - 1}
}
func check(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	return ctx.Err()
}
