// GEO_CLIP/ST_CLIP(geometry, boundary_polygon [, allow_nonconvex]):
// Sutherland-Hodgman polygon clipping.
//
// Sutherland-Hodgman is only guaranteed correct when the *clip* boundary is
// convex -- clipping against a non-convex boundary with the plain algorithm
// can produce a wrong shape. This is validated by default (protect first,
// explicit override for advanced use, matching GEO_SMOOTH's iteration cap
// and GEO_AFFINE's anchor requirement elsewhere in this file family) and
// left as an opt-in best-effort via a third `allow_nonconvex` argument
// rather than refused outright, since clipping to a bbox or a convex-hull
// region -- both very common BI operations ("clip this layer to my
// dashboard's current map viewport") -- is exactly what it's for.
package engine

import (
	"fmt"
)

func getGeoClipFunctions() map[string]funcHandler {
	return map[string]funcHandler{
		"GEO_CLIP": evalGeoClip,
		"ST_CLIP":  evalGeoClip,
	}
}

func evalGeoClip(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if err := requireArgs(ex.Name, ex, 2, 3); err != nil {
		return nil, err
	}
	subjectVal, err := evalExpr(env, ex.Args[0], row)
	if err != nil {
		return nil, err
	}
	if subjectVal == nil {
		return nil, nil
	}
	boundaryVal, err := evalExpr(env, ex.Args[1], row)
	if err != nil {
		return nil, err
	}
	if boundaryVal == nil {
		return nil, nil
	}

	allowNonconvex := false
	if len(ex.Args) == 3 {
		v, err := evalExpr(env, ex.Args[2], row)
		if err != nil {
			return nil, err
		}
		bv, err := coerceToBool(v)
		if err != nil {
			return nil, fmt.Errorf("%s allow_nonconvex: %w", ex.Name, err)
		}
		allowNonconvex = bv.(bool)
	}

	boundaryMP, err := geoMultiPolygonFromValue(boundaryVal)
	if err != nil {
		return nil, fmt.Errorf("%s boundary: %w", ex.Name, err)
	}
	if len(boundaryMP.Polygons) != 1 || len(boundaryMP.Polygons[0].Rings) != 1 {
		return nil, fmt.Errorf("%s: boundary must be a single Polygon with no holes", ex.Name)
	}
	boundaryRing := boundaryMP.Polygons[0].Rings[0]
	if !allowNonconvex && !ringIsConvex(boundaryRing) {
		return nil, fmt.Errorf("%s: boundary polygon is not convex; use GEO_BBOX/GEO_ENVELOPE or GEO_CONVEX_HULL to build a convex boundary, or pass allow_nonconvex=true for a best-effort, not-guaranteed-correct result", ex.Name)
	}

	subjectObj, err := geoObjectFromValue(subjectVal)
	if err != nil {
		return nil, fmt.Errorf("%s subject: %w", ex.Name, err)
	}
	kind, err := classifyGeoRelateKind(subjectObj)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", ex.Name, err)
	}

	switch kind {
	case geoRelatePoints:
		return clipPoints(subjectObj, boundaryRing)
	case geoRelatePolygons:
		subjectMP, err := geoMultiPolygonFromValue(subjectObj)
		if err != nil {
			return nil, fmt.Errorf("%s subject: %w", ex.Name, err)
		}
		return clipMultiPolygon(subjectMP, boundaryRing)
	default:
		return nil, fmt.Errorf("%s: LineString/MultiLineString subjects are not supported (Sutherland-Hodgman clips closed shapes; open-path clipping needs a different algorithm); pass a Point/MultiPoint or Polygon/MultiPolygon", ex.Name)
	}
}

// ringIsConvex reports whether ring's vertices turn the same direction at
// every corner -- convexity is orientation-independent (true for both a CW
// and a CCW convex ring), so this needs no assumption about which winding
// direction is "correct." Reuses orientation() (geo_relate.go), the same
// primitive convexHull's own cross() computes.
func ringIsConvex(ring geoRing) bool {
	pts := dropRingClosure(ring)
	n := len(pts)
	if n < 3 {
		return false
	}
	sign := 0.0
	for i := 0; i < n; i++ {
		a := pts[i]
		b := pts[(i+1)%n]
		c := pts[(i+2)%n]
		cross := orientation(a, b, c)
		if cross == 0 {
			continue
		}
		if sign == 0 {
			sign = cross
		} else if (cross > 0) != (sign > 0) {
			return false
		}
	}
	return true
}

// segmentPlaneIntersection returns the point on segment p1-p2 where
// orientation(a, b, ·) == 0 -- i.e. where p1-p2 crosses the infinite line
// through a-b. Solving orientation's own formula for t directly (rather
// than a separate line-intersection formula) keeps this consistent with
// exactly the same side test clipRingAgainstEdge uses to decide a point is
// inside or outside.
func segmentPlaneIntersection(p1, p2, a, b geoPoint) geoPoint {
	dx := b.Lon - a.Lon
	dy := b.Lat - a.Lat
	denom := dx*(p2.Lat-p1.Lat) - dy*(p2.Lon-p1.Lon)
	if denom == 0 {
		return p1 // parallel/degenerate: cannot happen when p1/p2 are on opposite sides, kept as a defensive fallback
	}
	t := (dy*(p1.Lon-a.Lon) - dx*(p1.Lat-a.Lat)) / denom
	return geoPoint{Lon: p1.Lon + t*(p2.Lon-p1.Lon), Lat: p1.Lat + t*(p2.Lat-p1.Lat)}
}

// clipRingAgainstEdge is one Sutherland-Hodgman pass: subject clipped to the
// single half-plane inside reports true for.
func clipRingAgainstEdge(subject []geoPoint, a, b geoPoint, inside func(geoPoint) bool) []geoPoint {
	if len(subject) == 0 {
		return nil
	}
	var output []geoPoint
	prev := subject[len(subject)-1]
	prevInside := inside(prev)
	for _, curr := range subject {
		currInside := inside(curr)
		switch {
		case currInside && prevInside:
			output = append(output, curr)
		case currInside && !prevInside:
			output = append(output, segmentPlaneIntersection(prev, curr, a, b), curr)
		case !currInside && prevInside:
			output = append(output, segmentPlaneIntersection(prev, curr, a, b))
		}
		prev, prevInside = curr, currInside
	}
	return output
}

// clipRingAgainstBoundary runs one Sutherland-Hodgman pass per boundary
// edge, chaining each pass's output into the next. The boundary's own
// winding direction (CW or CCW -- nothing in this codebase enforces a
// convention, e.g. GEO_BUFFER's generated ring is clockwise) determines
// which side of each edge counts as "inside": signedRingArea's sign picks
// the matching orientation() comparison, so this is correct either way.
func clipRingAgainstBoundary(subject []geoPoint, boundary geoRing) []geoPoint {
	boundaryPts := dropRingClosure(boundary)
	n := len(boundaryPts)
	if n < 3 {
		return nil
	}
	ccw := signedRingArea(boundary) >= 0
	output := subject
	for i := 0; i < n && len(output) > 0; i++ {
		a := boundaryPts[i]
		b := boundaryPts[(i+1)%n]
		inside := func(p geoPoint) bool {
			o := orientation(a, b, p)
			if ccw {
				return o >= 0
			}
			return o <= 0
		}
		output = clipRingAgainstEdge(output, a, b, inside)
	}
	return output
}

func closeRing(pts []geoPoint) []geoPoint {
	if len(pts) == 0 {
		return pts
	}
	first, last := pts[0], pts[len(pts)-1]
	if first.Lon == last.Lon && first.Lat == last.Lat {
		return pts
	}
	out := make([]geoPoint, len(pts)+1)
	copy(out, pts)
	out[len(pts)] = pts[0]
	return out
}

func clipMultiPolygon(subject geoMultiPolygon, boundary geoRing) (any, error) {
	var survivingParts [][][]geoPoint
	for _, poly := range subject.Polygons {
		if len(poly.Rings) == 0 {
			continue
		}
		exterior := clipRingAgainstBoundary(dropRingClosure(poly.Rings[0]), boundary)
		if len(exterior) < 3 {
			continue
		}
		rings := [][]geoPoint{closeRing(exterior)}
		for _, hole := range poly.Rings[1:] {
			clippedHole := clipRingAgainstBoundary(dropRingClosure(hole), boundary)
			if len(clippedHole) >= 3 {
				rings = append(rings, closeRing(clippedHole))
			}
		}
		survivingParts = append(survivingParts, rings)
	}
	if len(survivingParts) == 0 {
		return nil, nil
	}
	if len(survivingParts) == 1 {
		return marshalGeoAggregateResult("GEO_CLIP", ringsToPolygonGeoJSON(survivingParts[0]))
	}
	return marshalGeoAggregateResult("GEO_CLIP", multiPolygonGeoJSON(survivingParts))
}

func clipPoints(subjectObj map[string]any, boundary geoRing) (any, error) {
	pts, err := geoPointsFromObject(subjectObj)
	if err != nil {
		return nil, err
	}
	boundaryMP := geoMultiPolygon{Polygons: []geoPolygon{{Rings: []geoRing{boundary}}}}
	var kept []geoPoint
	for _, p := range pts {
		if pointInMultiPolygon(p, boundaryMP) {
			kept = append(kept, p)
		}
	}
	if len(kept) == 0 {
		return nil, nil
	}
	if len(kept) == 1 {
		return geoPointJSON(kept[0].Lon, kept[0].Lat, nil)
	}
	return marshalGeoAggregateResult("GEO_CLIP", map[string]any{
		"type":        "MultiPoint",
		"coordinates": ringToCoordinates(kept),
	})
}
