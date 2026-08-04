// GEO_DISSOLVE's polygon-merging core: a shared-edge-cancellation dissolve.
//
// Two adjacent, topologically-clean polygons that share a boundary segment
// traverse that segment in *opposite* directions -- each ring's own winding
// keeps its own polygon's interior on its own side of the edge. Cancelling
// exact-reverse directed-edge pairs therefore removes every interior
// boundary between adjacent input polygons and leaves only the true outer
// (and inner, i.e. hole) boundary of their union.
//
// This is correct for topologically-clean, vertex-aligned adjacent polygons
// -- real GIS boundary data, or this project's own dissolve output fed back
// in -- and deliberately NOT a general polygon-boolean-union: overlapping
// but not edge-aligned polygons would need a true Weiler-Atherton/
// Greiner-Hormann boolean union, which is out of scope here. That is a
// documented v1 limitation, the same kind of honest scope cut GEO_IS_VALID
// (no self-intersection check) and plan_range_index.go (no R-tree) already
// make elsewhere in this codebase.
package engine

import (
	"fmt"
	"math"
	"sort"
)

// edgeLess orders two directed edges lexicographically by (a.Lon, a.Lat,
// b.Lon, b.Lat), giving reassembleDissolvedRings a deterministic tie-break
// wherever more than one edge is a valid next step.
func edgeLess(a, b geoDissolveEdge) bool {
	if a[0].Lon != b[0].Lon {
		return a[0].Lon < b[0].Lon
	}
	if a[0].Lat != b[0].Lat {
		return a[0].Lat < b[0].Lat
	}
	if a[1].Lon != b[1].Lon {
		return a[1].Lon < b[1].Lon
	}
	return a[1].Lat < b[1].Lat
}

// geoDissolveDefaultSnapDegrees is used when the caller does not supply an
// explicit snap grid: ~1e-7 degrees is on the order of a centimeter at the
// equator, enough to make near-matching shared edges (the usual result of
// floating-point round-tripping through different tools) compare exactly
// equal without perceptibly moving any vertex.
const geoDissolveDefaultSnapDegrees = 1e-7

type geoDissolveEdge [2]geoPoint

// dissolvePolygons merges every polygon/multipolygon part in parts into the
// minimal set of rings via shared-edge cancellation, snapping coordinates
// to snapDegrees first (geoDissolveDefaultSnapDegrees if snapDegrees <= 0)
// so nearly-identical shared boundaries compare exactly equal -- the same
// snap-to-grid idea GEO_SNAP already implements, applied here as a
// preprocessing step rather than a separate returned geometry.
func dissolvePolygons(parts []geoMultiPolygon, snapDegrees float64) (map[string]any, error) {
	if snapDegrees <= 0 {
		snapDegrees = geoDissolveDefaultSnapDegrees
	}
	snap := func(p geoPoint) geoPoint {
		return geoPoint{
			Lon: math.Round(p.Lon/snapDegrees) * snapDegrees,
			Lat: math.Round(p.Lat/snapDegrees) * snapDegrees,
		}
	}

	net := make(map[geoDissolveEdge]int)
	for _, mp := range parts {
		for _, poly := range mp.Polygons {
			for _, ring := range poly.Rings {
				if len(ring) < 4 {
					return nil, fmt.Errorf("ring has fewer than 4 positions")
				}
				first, last := snap(ring[0]), snap(ring[len(ring)-1])
				if first.Lon != last.Lon || first.Lat != last.Lat {
					return nil, fmt.Errorf("ring is not closed")
				}
				for i := 0; i+1 < len(ring); i++ {
					a, b := snap(ring[i]), snap(ring[i+1])
					if a.Lon == b.Lon && a.Lat == b.Lat {
						continue // zero-length edge after snapping
					}
					reverse := geoDissolveEdge{b, a}
					if net[reverse] > 0 {
						net[reverse]--
					} else {
						net[geoDissolveEdge{a, b}]++
					}
				}
			}
		}
	}

	rings, err := reassembleDissolvedRings(net)
	if err != nil {
		return nil, err
	}
	if len(rings) == 0 {
		return map[string]any{"type": "MultiPolygon", "coordinates": []any{}}, nil
	}
	polygons, err := classifyDissolvedRings(rings)
	if err != nil {
		return nil, err
	}
	if len(polygons) == 1 {
		return ringsToPolygonGeoJSON(polygons[0]), nil
	}
	return multiPolygonGeoJSON(polygons), nil
}

// reassembleDissolvedRings chains every surviving (net count > 0) directed
// edge tail-to-head into closed rings. A vertex with no outgoing edge left
// to continue a walk means the input's boundaries did not fully cancel
// into closed loops -- reported as an error rather than guessed at, since
// silently fabricating a closure risks a wrong boundary in a BI/GIS
// correctness context.
func reassembleDissolvedRings(net map[geoDissolveEdge]int) ([][]geoPoint, error) {
	remaining := make(map[geoDissolveEdge]int, len(net))
	edgesFrom := make(map[geoPoint][]geoDissolveEdge)
	total := 0
	for edge, count := range net {
		if count <= 0 {
			continue
		}
		remaining[edge] = count
		edgesFrom[edge[0]] = append(edgesFrom[edge[0]], edge)
		total += count
	}
	// Every candidate list is sorted once up front so that which edge a
	// degree->1 vertex's walk picks (and which edge starts each new ring,
	// below) never depends on Go's randomized map iteration order --
	// otherwise the same input, dissolved twice, could come back as
	// geometrically identical but differently-rotated/reversed rings.
	for v := range edgesFrom {
		sort.Slice(edgesFrom[v], func(i, j int) bool { return edgeLess(edgesFrom[v][i], edgesFrom[v][j]) })
	}

	var rings [][]geoPoint
	for total > 0 {
		start, found := geoDissolveEdge{}, false
		for edge, count := range remaining {
			if count > 0 && (!found || edgeLess(edge, start)) {
				start, found = edge, true
			}
		}
		ring := []geoPoint{start[0]}
		current := start[0]
		next := start[1]
		remaining[start]--
		total--
		for {
			ring = append(ring, next)
			if next.Lon == start[0].Lon && next.Lat == start[0].Lat {
				break
			}
			foundNext := false
			for _, edge := range edgesFrom[next] {
				if remaining[edge] > 0 {
					remaining[edge]--
					total--
					current = next
					next = edge[1]
					foundNext = true
					break
				}
			}
			if !foundNext {
				return nil, fmt.Errorf("boundary does not close; input polygons may not share exact edges (see GEO_SNAP precision)")
			}
			_ = current
		}
		rings = append(rings, ring)
	}
	return rings, nil
}

// signedRingArea returns the planar shoelace area of a closed ring
// (ring[0] == ring[len-1]), signed: positive for a counterclockwise ring,
// negative for clockwise. Used only to classify
// dissolve output rings as exterior vs. hole candidates, not as a
// general-purpose area function (GEO_POLYGON_AREA already covers that,
// on the sphere).
func signedRingArea(ring []geoPoint) float64 {
	area := 0.0
	for i := 0; i+1 < len(ring); i++ {
		a, b := ring[i], ring[i+1]
		area += a.Lon*b.Lat - b.Lon*a.Lat
	}
	return area / 2
}

// classifyDissolvedRings groups reassembled rings into polygons: rings with
// non-negative signed area are exterior-boundary candidates, negative-area
// rings are holes assigned to whichever candidate exterior's point-in-ring
// test contains the hole's first vertex. If every ring came out
// negative-signed (e.g. every input's winding was reversed from the usual
// convention), every ring is instead treated as its own exterior rather
// than silently dropping data.
func classifyDissolvedRings(rings [][]geoPoint) ([][][]geoPoint, error) {
	areas := make([]float64, len(rings))
	var exteriors, holes []int
	for i, ring := range rings {
		areas[i] = signedRingArea(ring)
		if areas[i] >= 0 {
			exteriors = append(exteriors, i)
		} else {
			holes = append(holes, i)
		}
	}
	if len(exteriors) == 0 {
		exteriors = make([]int, len(rings))
		for i := range rings {
			exteriors[i] = i
		}
		holes = nil
	}

	polygons := make([][][]geoPoint, len(exteriors))
	exteriorPos := make(map[int]int, len(exteriors))
	for pos, ei := range exteriors {
		polygons[pos] = [][]geoPoint{rings[ei]}
		exteriorPos[ei] = pos
	}
	for _, hi := range holes {
		holeRing := rings[hi]
		if len(holeRing) == 0 {
			continue
		}
		assigned := false
		for pos, ei := range exteriors {
			if pointInRing(holeRing[0], geoRing(rings[ei])) {
				polygons[pos] = append(polygons[pos], holeRing)
				assigned = true
				break
			}
		}
		if !assigned {
			polygons = append(polygons, [][]geoPoint{holeRing})
		}
	}
	return polygons, nil
}

func ringToCoordinates(ring []geoPoint) []any {
	out := make([]any, len(ring))
	for i, p := range ring {
		out[i] = []float64{p.Lon, p.Lat}
	}
	return out
}

func ringsToPolygonGeoJSON(rings [][]geoPoint) map[string]any {
	coords := make([]any, len(rings))
	for i, ring := range rings {
		coords[i] = ringToCoordinates(ring)
	}
	return map[string]any{"type": "Polygon", "coordinates": coords}
}

func multiPolygonGeoJSON(polygons [][][]geoPoint) map[string]any {
	coords := make([]any, len(polygons))
	for i, rings := range polygons {
		ringsCoords := make([]any, len(rings))
		for j, ring := range rings {
			ringsCoords[j] = ringToCoordinates(ring)
		}
		coords[i] = ringsCoords
	}
	return map[string]any{"type": "MultiPolygon", "coordinates": coords}
}
