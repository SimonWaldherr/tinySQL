// ST_TOUCHES, ST_COVERS/ST_COVEREDBY and ST_PERIMETER round out geo_relate.go's
// ST_INTERSECTS/ST_DISJOINT/ST_EQUALS trio with the other DE-9IM predicates
// GIS users reach for most. Same engineering stance as that file: exact,
// non-epsilon comparisons, and a documented sampling-based approximation
// (vertices plus edge midpoints, rather than a full DE-9IM matrix) wherever
// exactness would require a general polygon-arrangement algorithm this
// codebase does not otherwise need. See the package doc comment on
// getGeoRelateFunctions for the same judgment call already made about
// self-intersecting input.
package engine

import (
	"fmt"
)

func getGeoExtraRelateFunctions() map[string]funcHandler {
	return map[string]funcHandler{
		"GEO_TOUCHES":   evalGeoTouches,
		"ST_TOUCHES":    evalGeoTouches,
		"GEO_COVERS":    evalGeoCovers,
		"ST_COVERS":     evalGeoCovers,
		"GEO_COVEREDBY": evalGeoCoveredBy,
		"ST_COVEREDBY":  evalGeoCoveredBy,
		"GEO_PERIMETER": evalGeoPerimeter,
		"ST_PERIMETER":  evalGeoPerimeter,
	}
}

// segmentsProperlyCross reports whether closed segments p1-p2 and p3-p4
// cross transversally -- each segment's endpoints strictly straddle the
// other segment's line. This is segmentsIntersect's first branch pulled out
// on its own: unlike segmentsIntersect, an endpoint touch or a collinear
// overlap does NOT count, because those describe boundary-only contact
// (what ST_TOUCHES allows) rather than the interiors actually cutting
// through one another (what ST_TOUCHES must rule out).
func segmentsProperlyCross(p1, p2, p3, p4 geoPoint) bool {
	d1 := orientation(p3, p4, p1)
	d2 := orientation(p3, p4, p2)
	d3 := orientation(p1, p2, p3)
	d4 := orientation(p1, p2, p4)
	return ((d1 > 0 && d2 < 0) || (d1 < 0 && d2 > 0)) &&
		((d3 > 0 && d4 < 0) || (d3 < 0 && d4 > 0))
}

// segmentsOverlapCollinearly reports whether p1-p2 and p3-p4 are collinear
// and share more than a single point -- i.e. an actual overlapping
// sub-segment, not just a shared endpoint. This is the one case
// segmentsProperlyCross cannot see (every orientation term is zero for
// collinear points) but that still represents interior-interior contact for
// two *lines* specifically: unlike a polygon's boundary, a line's interior
// is almost all of it (everything but its two endpoints), so an overlapping
// stretch of two collinear lines is genuine interior overlap, not a touch.
func segmentsOverlapCollinearly(p1, p2, p3, p4 geoPoint) bool {
	if orientation(p1, p2, p3) != 0 || orientation(p1, p2, p4) != 0 {
		return false
	}
	// Project onto whichever axis the segment actually varies along (a
	// perfectly vertical segment has zero longitude range, so latitude is
	// used instead) and compare the two closed intervals.
	axisSpan := func(a, b geoPoint) (lo, hi float64) {
		if a.Lon != b.Lon {
			if a.Lon < b.Lon {
				return a.Lon, b.Lon
			}
			return b.Lon, a.Lon
		}
		if a.Lat < b.Lat {
			return a.Lat, b.Lat
		}
		return b.Lat, a.Lat
	}
	lo1, hi1 := axisSpan(p1, p2)
	lo2, hi2 := axisSpan(p3, p4)
	overlapLo := lo1
	if lo2 > overlapLo {
		overlapLo = lo2
	}
	overlapHi := hi1
	if hi2 < overlapHi {
		overlapHi = hi2
	}
	return overlapHi > overlapLo
}

// geoLineParts normalizes a MultiLineString/LineString value to its
// constituent LineStrings, the shape geoLinesFromObject (geo_relate.go)
// already produces -- reused directly here.
func geoLineParts(obj map[string]any) ([]geoLineString, error) {
	return geoLinesFromObject(obj)
}

// ── ST_TOUCHES ───────────────────────────────────────────────────────────

func evalGeoTouches(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if err := requireArgs(ex.Name, ex, 2, 2); err != nil {
		return nil, err
	}
	aObj, err := evalGeoObjectArg(env, ex, row, 0)
	if err != nil {
		return nil, err
	}
	bObj, err := evalGeoObjectArg(env, ex, row, 1)
	if err != nil {
		return nil, err
	}
	aKind, err := classifyGeoRelateKind(aObj)
	if err != nil {
		return nil, fmt.Errorf("%s arg1: %w", ex.Name, err)
	}
	bKind, err := classifyGeoRelateKind(bObj)
	if err != nil {
		return nil, fmt.Errorf("%s arg2: %w", ex.Name, err)
	}
	if aKind == geoRelatePoints && bKind == geoRelatePoints {
		return false, nil // a point has no boundary; touches is never true
	}
	// Symmetric predicate: always evaluate with the polygon/line argument
	// first so only one direction of each pairing needs an implementation.
	if bKind > aKind {
		aObj, bObj = bObj, aObj
		aKind, bKind = bKind, aKind
	}
	switch {
	case aKind == geoRelatePolygons && bKind == geoRelatePoints:
		return geoTouchesPointsPolygons(bObj, aObj)
	case aKind == geoRelatePolygons && bKind == geoRelateLines:
		return geoTouchesLinesPolygons(bObj, aObj)
	case aKind == geoRelatePolygons && bKind == geoRelatePolygons:
		return geoTouchesPolygonsPolygons(aObj, bObj)
	case aKind == geoRelateLines && bKind == geoRelatePoints:
		return geoTouchesPointsLines(bObj, aObj)
	case aKind == geoRelateLines && bKind == geoRelateLines:
		return geoTouchesLinesLines(aObj, bObj)
	default:
		return nil, fmt.Errorf("%s: unsupported geometry combination", ex.Name)
	}
}

func geoTouchesPointsPolygons(ptsObj, polyObj map[string]any) (any, error) {
	pts, err := geoPointsFromObject(ptsObj)
	if err != nil {
		return nil, err
	}
	mp, err := geoMultiPolygonFromValue(polyObj)
	if err != nil {
		return nil, err
	}
	touched := false
	for _, p := range pts {
		if pointStrictlyInsideMultiPolygon(p, mp) {
			return false, nil // interior contact: not a touch
		}
		if pointOnMultiPolygonBoundary(p, mp) {
			touched = true
		}
	}
	return touched, nil
}

func geoTouchesPointsLines(ptsObj, lineObj map[string]any) (any, error) {
	pts, err := geoPointsFromObject(ptsObj)
	if err != nil {
		return nil, err
	}
	lines, err := geoLineParts(lineObj)
	if err != nil {
		return nil, err
	}
	touched := false
	for _, p := range pts {
		onInterior, onEndpoint := false, false
		for _, ls := range lines {
			if len(ls) == 0 {
				continue
			}
			if geoPointsEqualExact(p, ls[0]) || geoPointsEqualExact(p, ls[len(ls)-1]) {
				onEndpoint = true
				continue
			}
			for i := 0; i+1 < len(ls); i++ {
				if pointOnSegment(p, ls[i], ls[i+1]) {
					onInterior = true
				}
			}
		}
		if onInterior {
			return false, nil
		}
		if onEndpoint {
			touched = true
		}
	}
	return touched, nil
}

func geoPointsEqualExact(a, b geoPoint) bool {
	return a.Lon == b.Lon && a.Lat == b.Lat
}

// pointStrictlyInsideMultiPolygon reports whether p is in mp's interior,
// excluding its boundary. pointInMultiPolygon's ray-casting is exact
// everywhere except for a point that lies precisely on an edge or vertex,
// where the even-odd rule can go either way (see pointOnRingBoundary's own
// doc comment in geo_relate.go for the same ambiguity ST_INTERSECTS already
// works around). ST_TOUCHES's whole job is telling boundary contact apart
// from interior overlap, so it cannot tolerate that ambiguity: the exact
// boundary test is checked first, and only a point that is not on the
// boundary is allowed to fall through to the (otherwise ambiguous)
// ray-casting interior test.
func pointStrictlyInsideMultiPolygon(p geoPoint, mp geoMultiPolygon) bool {
	if pointOnMultiPolygonBoundary(p, mp) {
		return false
	}
	return pointInMultiPolygon(p, mp)
}

func geoTouchesLinesLines(aObj, bObj map[string]any) (any, error) {
	aLines, err := geoLineParts(aObj)
	if err != nil {
		return nil, err
	}
	bLines, err := geoLineParts(bObj)
	if err != nil {
		return nil, err
	}
	shares := false
	for _, la := range aLines {
		for _, lb := range bLines {
			for i := 0; i+1 < len(la); i++ {
				for j := 0; j+1 < len(lb); j++ {
					a1, a2, b1, b2 := la[i], la[i+1], lb[j], lb[j+1]
					if segmentsProperlyCross(a1, a2, b1, b2) || segmentsOverlapCollinearly(a1, a2, b1, b2) {
						return false, nil
					}
					if segmentsIntersect(a1, a2, b1, b2) {
						shares = true
					}
				}
			}
		}
	}
	return shares, nil
}

func geoTouchesLinesPolygons(lineObj, polyObj map[string]any) (any, error) {
	lines, err := geoLineParts(lineObj)
	if err != nil {
		return nil, err
	}
	mp, err := geoMultiPolygonFromValue(polyObj)
	if err != nil {
		return nil, err
	}
	shares := false
	for _, ls := range lines {
		for _, p := range ls {
			if pointStrictlyInsideMultiPolygon(p, mp) {
				return false, nil
			}
			if pointOnMultiPolygonBoundary(p, mp) {
				shares = true
			}
		}
		for i := 0; i+1 < len(ls); i++ {
			mid := geoPoint{Lon: (ls[i].Lon + ls[i+1].Lon) / 2, Lat: (ls[i].Lat + ls[i+1].Lat) / 2}
			if pointStrictlyInsideMultiPolygon(mid, mp) {
				return false, nil
			}
			for _, poly := range mp.Polygons {
				for _, ring := range poly.Rings {
					for k := 0; k+1 < len(ring); k++ {
						if segmentsProperlyCross(ls[i], ls[i+1], ring[k], ring[k+1]) {
							return false, nil
						}
						if segmentsIntersect(ls[i], ls[i+1], ring[k], ring[k+1]) {
							shares = true
						}
					}
				}
			}
		}
	}
	return shares, nil
}

func geoTouchesPolygonsPolygons(aObj, bObj map[string]any) (any, error) {
	aMP, err := geoMultiPolygonFromValue(aObj)
	if err != nil {
		return nil, err
	}
	bMP, err := geoMultiPolygonFromValue(bObj)
	if err != nil {
		return nil, err
	}
	shares := false
	for _, pa := range aMP.Polygons {
		for _, ra := range pa.Rings {
			for i := 0; i+1 < len(ra); i++ {
				if pointStrictlyInsideMultiPolygon(ra[i], bMP) {
					return false, nil
				}
				mid := geoPoint{Lon: (ra[i].Lon + ra[i+1].Lon) / 2, Lat: (ra[i].Lat + ra[i+1].Lat) / 2}
				if pointStrictlyInsideMultiPolygon(mid, bMP) {
					return false, nil
				}
				for _, pb := range bMP.Polygons {
					for _, rb := range pb.Rings {
						for j := 0; j+1 < len(rb); j++ {
							if segmentsProperlyCross(ra[i], ra[i+1], rb[j], rb[j+1]) {
								return false, nil
							}
							if segmentsIntersect(ra[i], ra[i+1], rb[j], rb[j+1]) {
								shares = true
							}
						}
					}
				}
			}
		}
	}
	for _, pb := range bMP.Polygons {
		for _, rb := range pb.Rings {
			for j := 0; j+1 < len(rb); j++ {
				if pointStrictlyInsideMultiPolygon(rb[j], aMP) {
					return false, nil
				}
				mid := geoPoint{Lon: (rb[j].Lon + rb[j+1].Lon) / 2, Lat: (rb[j].Lat + rb[j+1].Lat) / 2}
				if pointStrictlyInsideMultiPolygon(mid, aMP) {
					return false, nil
				}
			}
		}
	}
	return shares, nil
}

// ── ST_COVERS / ST_COVEREDBY ─────────────────────────────────────────────

func evalGeoCovers(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if err := requireArgs(ex.Name, ex, 2, 2); err != nil {
		return nil, err
	}
	aObj, err := evalGeoObjectArg(env, ex, row, 0)
	if err != nil {
		return nil, err
	}
	bObj, err := evalGeoObjectArg(env, ex, row, 1)
	if err != nil {
		return nil, err
	}
	return geoCovers(ex.Name, aObj, bObj)
}

func evalGeoCoveredBy(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if err := requireArgs(ex.Name, ex, 2, 2); err != nil {
		return nil, err
	}
	aObj, err := evalGeoObjectArg(env, ex, row, 0)
	if err != nil {
		return nil, err
	}
	bObj, err := evalGeoObjectArg(env, ex, row, 1)
	if err != nil {
		return nil, err
	}
	return geoCovers(ex.Name, bObj, aObj)
}

// geoCovers reports whether every point of b lies within a or on a's
// boundary. Point-in-B checks are exact; for a line or polygon b whose
// vertices all satisfy that but whose *edges* could still leave a between
// two covered vertices, an additional "no edge of b properly crosses a's
// boundary" check catches the common case (see the package doc comment for
// the one shape of case this still cannot rule out: a b-edge that leaves
// and returns to a between two of a's own vertices without crossing any
// edge, which needs a instead of b to be non-convex in exactly the wrong
// way -- rare enough in real data to accept as a documented limitation).
func geoCovers(name string, aObj, bObj map[string]any) (any, error) {
	aKind, err := classifyGeoRelateKind(aObj)
	if err != nil {
		return nil, fmt.Errorf("%s arg1: %w", name, err)
	}
	bKind, err := classifyGeoRelateKind(bObj)
	if err != nil {
		return nil, fmt.Errorf("%s arg2: %w", name, err)
	}

	pointInA, err := geoContainmentTest(aKind, aObj)
	if err != nil {
		return nil, err
	}

	switch bKind {
	case geoRelatePoints:
		pts, err := geoPointsFromObject(bObj)
		if err != nil {
			return nil, err
		}
		for _, p := range pts {
			if !pointInA(p) {
				return false, nil
			}
		}
		return true, nil
	case geoRelateLines:
		lines, err := geoLineParts(bObj)
		if err != nil {
			return nil, err
		}
		return geoCoversLines(aKind, aObj, pointInA, lines)
	case geoRelatePolygons:
		bMP, err := geoMultiPolygonFromValue(bObj)
		if err != nil {
			return nil, err
		}
		if aKind != geoRelatePolygons {
			return false, nil // an area can never be covered by a line or point set
		}
		return geoCoversPolygonByPolygon(aObj, bMP, pointInA)
	default:
		return nil, fmt.Errorf("%s: unsupported geometry combination", name)
	}
}

// geoContainmentTest returns a "point p is in-or-on aObj" predicate for
// whichever kind of geometry aObj turns out to be.
func geoContainmentTest(aKind geoRelateKind, aObj map[string]any) (func(geoPoint) bool, error) {
	switch aKind {
	case geoRelatePoints:
		pts, err := geoPointsFromObject(aObj)
		if err != nil {
			return nil, err
		}
		return func(p geoPoint) bool {
			for _, q := range pts {
				if geoPointsEqualExact(p, q) {
					return true
				}
			}
			return false
		}, nil
	case geoRelateLines:
		lines, err := geoLineParts(aObj)
		if err != nil {
			return nil, err
		}
		return func(p geoPoint) bool {
			for _, ls := range lines {
				for i := 0; i+1 < len(ls); i++ {
					if pointOnSegment(p, ls[i], ls[i+1]) {
						return true
					}
				}
			}
			return false
		}, nil
	case geoRelatePolygons:
		mp, err := geoMultiPolygonFromValue(aObj)
		if err != nil {
			return nil, err
		}
		return func(p geoPoint) bool {
			return pointInMultiPolygon(p, mp) || pointOnMultiPolygonBoundary(p, mp)
		}, nil
	default:
		return nil, fmt.Errorf("unsupported or missing geometry type")
	}
}

func geoCoversLines(aKind geoRelateKind, aObj map[string]any, pointInA func(geoPoint) bool, bLines []geoLineString) (any, error) {
	for _, ls := range bLines {
		for _, p := range ls {
			if !pointInA(p) {
				return false, nil
			}
		}
	}
	if aKind != geoRelatePolygons {
		return true, nil
	}
	mp, err := geoMultiPolygonFromValue(aObj)
	if err != nil {
		return nil, err
	}
	for _, ls := range bLines {
		for i := 0; i+1 < len(ls); i++ {
			for _, poly := range mp.Polygons {
				for _, ring := range poly.Rings {
					for k := 0; k+1 < len(ring); k++ {
						if segmentsProperlyCross(ls[i], ls[i+1], ring[k], ring[k+1]) {
							return false, nil
						}
					}
				}
			}
		}
	}
	return true, nil
}

func geoCoversPolygonByPolygon(aObj map[string]any, bMP geoMultiPolygon, pointInA func(geoPoint) bool) (any, error) {
	for _, poly := range bMP.Polygons {
		for _, ring := range poly.Rings {
			for _, p := range ring {
				if !pointInA(p) {
					return false, nil
				}
			}
		}
	}
	aMP, err := geoMultiPolygonFromValue(aObj)
	if err != nil {
		return nil, err
	}
	for _, pa := range aMP.Polygons {
		for _, ra := range pa.Rings {
			for i := 0; i+1 < len(ra); i++ {
				for _, pb := range bMP.Polygons {
					for _, rb := range pb.Rings {
						for j := 0; j+1 < len(rb); j++ {
							if segmentsProperlyCross(ra[i], ra[i+1], rb[j], rb[j+1]) {
								return false, nil
							}
						}
					}
				}
			}
		}
	}
	return true, nil
}

// ── ST_PERIMETER ─────────────────────────────────────────────────────────

// evalGeoPerimeter sums the great-circle length of every ring (exterior and
// holes alike -- a hole's boundary is still boundary length) across every
// part of a Polygon/MultiPolygon, the natural counterpart to GEO_LENGTH
// (open lines) and GEO_POLYGON_AREA (enclosed area).
func evalGeoPerimeter(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if err := requireArgs(ex.Name, ex, 1, 1); err != nil {
		return nil, err
	}
	mp, err := evalGeoPolygonArg(env, ex, row, 0)
	if err != nil {
		return nil, err
	}
	total := 0.0
	for _, poly := range mp.Polygons {
		for _, ring := range poly.Rings {
			for i := 1; i < len(ring); i++ {
				total += haversineMeters(ring[i-1].Lat, ring[i-1].Lon, ring[i].Lat, ring[i].Lon)
			}
		}
	}
	return total, nil
}
