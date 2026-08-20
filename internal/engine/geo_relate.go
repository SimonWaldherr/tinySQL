package engine

import (
	"fmt"
	"math"
	"sort"
	"strings"
)

// getGeoRelateFunctions returns the OGC-style spatial relationship
// predicates that go beyond point-in-polygon: ST_INTERSECTS/ST_DISJOINT
// (any shared point, or its exact negation) and ST_EQUALS (same point-set,
// scoped down to a tractable coordinate/shape equality -- see evalGeoEquals).
//
// ST_TOUCHES, ST_CROSSES and ST_OVERLAPS are deliberately not implemented:
// rigorously distinguishing boundary-only contact from interior overlap
// needs a full DE-9IM computation, and a naive attempt would be silently
// wrong on exactly the inputs (shared edges between adjacent polygons,
// polygons with holes) that real GIS data hits constantly. That failure
// mode -- a wrong boolean with no error -- is worse than not offering the
// function at all, so this is a documented scope limit (see README.md),
// the same judgment call GEO_IS_VALID already makes about self-intersection.
func getGeoRelateFunctions() map[string]funcHandler {
	return map[string]funcHandler{
		"GEO_INTERSECTS": evalGeoIntersects,
		"ST_INTERSECTS":  evalGeoIntersects,
		"GEO_DISJOINT":   evalGeoDisjoint,
		"ST_DISJOINT":    evalGeoDisjoint,
		"GEO_EQUALS":     evalGeoEquals,
		"ST_EQUALS":      evalGeoEquals,
	}
}

// geoRelateKind classifies a geometry into one of the three broad shapes
// these predicates dispatch on, collapsing the Point/MultiPoint,
// LineString/MultiLineString and Polygon/MultiPolygon distinctions (the
// pairwise algorithms below already treat a single geometry as a
// one-part Multi* of itself).
type geoRelateKind int

const (
	geoRelateUnknown geoRelateKind = iota
	geoRelatePoints
	geoRelateLines
	geoRelatePolygons
)

func classifyGeoRelateKind(object map[string]any) (geoRelateKind, error) {
	typ, _ := object["type"].(string)
	switch strings.ToLower(typ) {
	case "point", "multipoint":
		return geoRelatePoints, nil
	case "linestring", "multilinestring":
		return geoRelateLines, nil
	case "polygon", "multipolygon":
		return geoRelatePolygons, nil
	case "geometrycollection":
		return geoRelateUnknown, fmt.Errorf("GeometryCollection is not supported")
	default:
		return geoRelateUnknown, fmt.Errorf("unsupported or missing GeoJSON geometry type %q", typ)
	}
}

func geoPointsFromObject(object map[string]any) ([]geoPoint, error) {
	typ, _ := object["type"].(string)
	if strings.EqualFold(typ, "Point") {
		p, err := geoPositionFromValue(object["coordinates"])
		if err != nil {
			return nil, err
		}
		return []geoPoint{p}, nil
	}
	return positionsFromArray(object["coordinates"])
}

// geoLinesFromObject returns every component line of a LineString (one
// line) or MultiLineString (its parts) as a uniform []geoLineString,
// mirroring how geoMultiPolygonFromValue treats a bare Polygon as a
// one-part MultiPolygon.
func geoLinesFromObject(object map[string]any) ([]geoLineString, error) {
	typ, _ := object["type"].(string)
	if strings.EqualFold(typ, "LineString") {
		ls, err := geoLineStringFromValue(object)
		if err != nil {
			return nil, err
		}
		return []geoLineString{ls}, nil
	}
	rawLines, ok := object["coordinates"].([]any)
	if !ok || len(rawLines) == 0 {
		return nil, fmt.Errorf("multilinestring coordinates must be a non-empty array of lines")
	}
	out := make([]geoLineString, 0, len(rawLines))
	for i, rawLine := range rawLines {
		positions, ok := rawLine.([]any)
		if !ok || len(positions) < 2 {
			return nil, fmt.Errorf("line %d needs at least 2 positions", i)
		}
		ls := make(geoLineString, 0, len(positions))
		for j, rawPos := range positions {
			p, err := geoPositionFromValue(rawPos)
			if err != nil {
				return nil, fmt.Errorf("line %d position %d: %w", i, j, err)
			}
			ls = append(ls, p)
		}
		out = append(out, ls)
	}
	return out, nil
}

// ── Geometric primitives ─────────────────────────────────────────────────

// orientation returns the sign of the cross product (b-a) x (c-a): positive
// for a counterclockwise turn a->b->c, negative for clockwise, zero if a, b
// and c are collinear. The same formula convexHull's cross() already uses.
func orientation(a, b, c geoPoint) float64 {
	return (b.Lon-a.Lon)*(c.Lat-a.Lat) - (b.Lat-a.Lat)*(c.Lon-a.Lon)
}

// onSegment reports whether p, already known to be collinear with a and b,
// lies within segment a-b's bounding box (i.e. actually between the two
// endpoints, not on the line's extension beyond either one).
func onSegment(p, a, b geoPoint) bool {
	return math.Min(a.Lon, b.Lon) <= p.Lon && p.Lon <= math.Max(a.Lon, b.Lon) &&
		math.Min(a.Lat, b.Lat) <= p.Lat && p.Lat <= math.Max(a.Lat, b.Lat)
}

// pointOnSegment reports whether p lies exactly on the closed segment a-b.
func pointOnSegment(p, a, b geoPoint) bool {
	return orientation(a, b, p) == 0 && onSegment(p, a, b)
}

// segmentsIntersect reports whether closed segments p1-p2 and p3-p4 share
// at least one point: a proper crossing (both interiors cross) or an
// improper one (an endpoint touches the other segment, or the two are
// collinear and overlap). Standard orientation-based test; equality is
// exact (no epsilon), matching this file's existing exact-float-equality
// convention (see geo_simplify.go's sameSimplifyCoordinate).
func segmentsIntersect(p1, p2, p3, p4 geoPoint) bool {
	d1 := orientation(p3, p4, p1)
	d2 := orientation(p3, p4, p2)
	d3 := orientation(p1, p2, p3)
	d4 := orientation(p1, p2, p4)

	if ((d1 > 0 && d2 < 0) || (d1 < 0 && d2 > 0)) &&
		((d3 > 0 && d4 < 0) || (d3 < 0 && d4 > 0)) {
		return true
	}
	if d1 == 0 && onSegment(p1, p3, p4) {
		return true
	}
	if d2 == 0 && onSegment(p2, p3, p4) {
		return true
	}
	if d3 == 0 && onSegment(p3, p1, p2) {
		return true
	}
	if d4 == 0 && onSegment(p4, p1, p2) {
		return true
	}
	return false
}

// pointOnRingBoundary reports whether p lies exactly on any edge of ring.
// Rings in this file are always stored explicitly closed (ring[0] equals
// ring[len-1], per polygonFromRingsValue), so edges are just consecutive
// pairs -- no wraparound edge needs to be added separately.
//
// This exists because pointInRing's ray-casting rule can be
// parity-inconsistent for a point exactly on an edge, and ST_INTERSECTS
// needs an exact answer for that case: two adjacent polygons sharing a
// boundary is a common configuration in real GIS data, not a rare edge case.
func pointOnRingBoundary(p geoPoint, ring geoRing) bool {
	for i := 0; i+1 < len(ring); i++ {
		if pointOnSegment(p, ring[i], ring[i+1]) {
			return true
		}
	}
	return false
}

func pointOnMultiPolygonBoundary(p geoPoint, mp geoMultiPolygon) bool {
	for _, poly := range mp.Polygons {
		for _, ring := range poly.Rings {
			if pointOnRingBoundary(p, ring) {
				return true
			}
		}
	}
	return false
}

// ── Pairwise intersection tests ──────────────────────────────────────────
//
// Each pair is implemented once; geoIntersectsDispatch normalizes argument
// order so only the six unordered kind-pairs need a body.

func geoIntersectsDispatch(aObj map[string]any, aKind geoRelateKind, bObj map[string]any, bKind geoRelateKind) (bool, error) {
	if aKind > bKind {
		aObj, bObj = bObj, aObj
		aKind, bKind = bKind, aKind
	}
	switch {
	case aKind == geoRelatePoints && bKind == geoRelatePoints:
		return geoPointsIntersectPoints(aObj, bObj)
	case aKind == geoRelatePoints && bKind == geoRelateLines:
		return geoPointsIntersectLines(aObj, bObj)
	case aKind == geoRelatePoints && bKind == geoRelatePolygons:
		return geoPointsIntersectPolygons(aObj, bObj)
	case aKind == geoRelateLines && bKind == geoRelateLines:
		return geoLinesIntersectLines(aObj, bObj)
	case aKind == geoRelateLines && bKind == geoRelatePolygons:
		return geoLinesIntersectPolygons(aObj, bObj)
	case aKind == geoRelatePolygons && bKind == geoRelatePolygons:
		return geoPolygonsIntersectPolygons(aObj, bObj)
	default:
		return false, fmt.Errorf("unsupported geometry combination")
	}
}

func geoPointsIntersectPoints(aObj, bObj map[string]any) (bool, error) {
	aPts, err := geoPointsFromObject(aObj)
	if err != nil {
		return false, err
	}
	bPts, err := geoPointsFromObject(bObj)
	if err != nil {
		return false, err
	}
	for _, a := range aPts {
		for _, b := range bPts {
			if a.Lon == b.Lon && a.Lat == b.Lat {
				return true, nil
			}
		}
	}
	return false, nil
}

func geoPointsIntersectLines(aObj, bObj map[string]any) (bool, error) {
	pts, err := geoPointsFromObject(aObj)
	if err != nil {
		return false, err
	}
	lines, err := geoLinesFromObject(bObj)
	if err != nil {
		return false, err
	}
	for _, p := range pts {
		for _, ls := range lines {
			for i := 0; i+1 < len(ls); i++ {
				if pointOnSegment(p, ls[i], ls[i+1]) {
					return true, nil
				}
			}
		}
	}
	return false, nil
}

func geoPointsIntersectPolygons(aObj, bObj map[string]any) (bool, error) {
	pts, err := geoPointsFromObject(aObj)
	if err != nil {
		return false, err
	}
	mp, err := geoMultiPolygonFromValue(bObj)
	if err != nil {
		return false, err
	}
	for _, p := range pts {
		if pointInMultiPolygon(p, mp) || pointOnMultiPolygonBoundary(p, mp) {
			return true, nil
		}
	}
	return false, nil
}

// geoLinesIntersectLines is the one fully rigorous pair here: two
// 1-dimensional curves have no "interior containment without touching"
// case the way areas do, so exhaustive segment-pair testing is complete,
// not an approximation.
func geoLinesIntersectLines(aObj, bObj map[string]any) (bool, error) {
	aLines, err := geoLinesFromObject(aObj)
	if err != nil {
		return false, err
	}
	bLines, err := geoLinesFromObject(bObj)
	if err != nil {
		return false, err
	}
	for _, la := range aLines {
		for _, lb := range bLines {
			for i := 0; i+1 < len(la); i++ {
				for j := 0; j+1 < len(lb); j++ {
					if segmentsIntersect(la[i], la[i+1], lb[j], lb[j+1]) {
						return true, nil
					}
				}
			}
		}
	}
	return false, nil
}

// geoLinesIntersectPolygons tests every line segment against every ring
// (exterior and holes) of every polygon part first. If nothing crosses any
// boundary anywhere, a line cannot have moved from inside to outside
// without crossing a boundary, so testing just its first vertex for
// containment correctly classifies the whole line -- not a shortcut that
// risks a wrong answer, just the cheaper of two equivalent checks once the
// boundary loop has already come up empty.
func geoLinesIntersectPolygons(aObj, bObj map[string]any) (bool, error) {
	lines, err := geoLinesFromObject(aObj)
	if err != nil {
		return false, err
	}
	mp, err := geoMultiPolygonFromValue(bObj)
	if err != nil {
		return false, err
	}
	for _, ls := range lines {
		crossed := false
		for i := 0; i+1 < len(ls) && !crossed; i++ {
			for _, poly := range mp.Polygons {
				for _, ring := range poly.Rings {
					for k := 0; k+1 < len(ring); k++ {
						if segmentsIntersect(ls[i], ls[i+1], ring[k], ring[k+1]) {
							crossed = true
							break
						}
					}
					if crossed {
						break
					}
				}
				if crossed {
					break
				}
			}
		}
		if crossed {
			return true, nil
		}
		if pointInMultiPolygon(ls[0], mp) {
			return true, nil
		}
	}
	return false, nil
}

// geoPolygonsIntersectPolygons tests every ring (including holes) of every
// part of A against every ring of every part of B first -- iterating holes,
// not just exterior rings, is what makes the hole-nesting case correct: a
// polygon whose boundary coincides with a hole's boundary is caught here
// directly. If no boundary anywhere crosses, each part's containment status
// (fully inside vs. fully outside the other geometry, including "inside a
// hole" which pointInMultiPolygon already treats as outside) is determined
// by a single representative vertex, by the same reasoning as
// geoLinesIntersectPolygons.
func geoPolygonsIntersectPolygons(aObj, bObj map[string]any) (bool, error) {
	aMP, err := geoMultiPolygonFromValue(aObj)
	if err != nil {
		return false, err
	}
	bMP, err := geoMultiPolygonFromValue(bObj)
	if err != nil {
		return false, err
	}
	for _, pa := range aMP.Polygons {
		for _, pb := range bMP.Polygons {
			if polygonsShareBoundary(pa, pb) {
				return true, nil
			}
		}
	}
	for _, pa := range aMP.Polygons {
		if len(pa.Rings) == 0 || len(pa.Rings[0]) == 0 {
			continue
		}
		if pointInMultiPolygon(pa.Rings[0][0], bMP) {
			return true, nil
		}
	}
	for _, pb := range bMP.Polygons {
		if len(pb.Rings) == 0 || len(pb.Rings[0]) == 0 {
			continue
		}
		if pointInMultiPolygon(pb.Rings[0][0], aMP) {
			return true, nil
		}
	}
	return false, nil
}

func polygonsShareBoundary(pa, pb geoPolygon) bool {
	for _, ra := range pa.Rings {
		for _, rb := range pb.Rings {
			if ringsShareBoundary(ra, rb) {
				return true
			}
		}
	}
	return false
}

func ringsShareBoundary(ra, rb geoRing) bool {
	for i := 0; i+1 < len(ra); i++ {
		for j := 0; j+1 < len(rb); j++ {
			if segmentsIntersect(ra[i], ra[i+1], rb[j], rb[j+1]) {
				return true
			}
		}
	}
	return false
}

// ── ST_INTERSECTS / ST_DISJOINT ──────────────────────────────────────────

func evalGeoIntersects(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if err := requireArgs(ex.Name, ex, 2, 2); err != nil {
		return nil, err
	}
	av, err := evalExpr(env, ex.Args[0], row)
	if err != nil {
		return nil, err
	}
	bv, err := evalExpr(env, ex.Args[1], row)
	if err != nil {
		return nil, err
	}
	if av == nil || bv == nil {
		return nil, nil
	}
	aObj, err := geoObjectFromValue(av)
	if err != nil {
		return nil, fmt.Errorf("%s arg1: %w", ex.Name, err)
	}
	bObj, err := geoObjectFromValue(bv)
	if err != nil {
		return nil, fmt.Errorf("%s arg2: %w", ex.Name, err)
	}
	aKind, err := classifyGeoRelateKind(aObj)
	if err != nil {
		return nil, fmt.Errorf("%s arg1: %w", ex.Name, err)
	}
	bKind, err := classifyGeoRelateKind(bObj)
	if err != nil {
		return nil, fmt.Errorf("%s arg2: %w", ex.Name, err)
	}
	result, err := geoIntersectsDispatch(aObj, aKind, bObj, bKind)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", ex.Name, err)
	}
	return result, nil
}

// evalGeoDisjoint is the exact logical negation of ST_INTERSECTS, reusing
// all of its validated logic (and error propagation) rather than
// re-deriving "no shared point" independently.
func evalGeoDisjoint(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	v, err := evalGeoIntersects(env, ex, row)
	if err != nil {
		return nil, err
	}
	if v == nil {
		return nil, nil
	}
	return !v.(bool), nil
}

// ── ST_EQUALS ─────────────────────────────────────────────────────────────
//
// True OGC ST_Equals is point-set equality: two differently-vertexed
// polygons covering the identical area are equal. Computing that rigorously
// needs a full boolean overlay, out of reach for hand-rolled code here. This
// is deliberately scoped down to coordinate/shape equality instead:
//   - Points/MultiPoint: order-independent (multiset) equality -- fully
//     rigorous, no caveat needed.
//   - Lines/MultiLineString, Polygon/MultiPolygon: positional part-by-part
//     comparison (part i of A vs part i of B, not multiset-matched across
//     parts) -- a documented simplification. Within a polygon, holes are
//     likewise compared positionally, not multiset-matched. Each ring pair
//     is compared allowing for a different start vertex and winding
//     direction, which fixes the single most common "same geometry,
//     different encoding" mismatch.
//
// Different geometry *kinds* are always unequal (false, not an error).
func evalGeoEquals(env ExecEnv, ex *FuncCall, row Row) (any, error) {
	if err := requireArgs(ex.Name, ex, 2, 2); err != nil {
		return nil, err
	}
	av, err := evalExpr(env, ex.Args[0], row)
	if err != nil {
		return nil, err
	}
	bv, err := evalExpr(env, ex.Args[1], row)
	if err != nil {
		return nil, err
	}
	if av == nil || bv == nil {
		return nil, nil
	}
	aObj, err := geoObjectFromValue(av)
	if err != nil {
		return nil, fmt.Errorf("%s arg1: %w", ex.Name, err)
	}
	bObj, err := geoObjectFromValue(bv)
	if err != nil {
		return nil, fmt.Errorf("%s arg2: %w", ex.Name, err)
	}
	aKind, err := classifyGeoRelateKind(aObj)
	if err != nil {
		return nil, fmt.Errorf("%s arg1: %w", ex.Name, err)
	}
	bKind, err := classifyGeoRelateKind(bObj)
	if err != nil {
		return nil, fmt.Errorf("%s arg2: %w", ex.Name, err)
	}
	if aKind != bKind {
		return false, nil
	}
	switch aKind {
	case geoRelatePoints:
		return geoPointSetsEqual(aObj, bObj)
	case geoRelateLines:
		return geoLineSetsEqual(aObj, bObj)
	case geoRelatePolygons:
		return geoPolygonSetsEqual(aObj, bObj)
	default:
		return false, fmt.Errorf("%s: unsupported geometry type", ex.Name)
	}
}

func geoPointSetsEqual(aObj, bObj map[string]any) (any, error) {
	aPts, err := geoPointsFromObject(aObj)
	if err != nil {
		return nil, err
	}
	bPts, err := geoPointsFromObject(bObj)
	if err != nil {
		return nil, err
	}
	if len(aPts) != len(bPts) {
		return false, nil
	}
	sortGeoPoints(aPts)
	sortGeoPoints(bPts)
	for i := range aPts {
		if aPts[i].Lon != bPts[i].Lon || aPts[i].Lat != bPts[i].Lat {
			return false, nil
		}
	}
	return true, nil
}

func sortGeoPoints(pts []geoPoint) {
	sort.Sort(geoPointsByLonLat(pts))
}

func geoLineSetsEqual(aObj, bObj map[string]any) (any, error) {
	aLines, err := geoLinesFromObject(aObj)
	if err != nil {
		return nil, err
	}
	bLines, err := geoLinesFromObject(bObj)
	if err != nil {
		return nil, err
	}
	if len(aLines) != len(bLines) {
		return false, nil
	}
	for i := range aLines {
		if !lineStringsEqual(aLines[i], bLines[i]) {
			return false, nil
		}
	}
	return true, nil
}

// lineStringsEqual reports whether a and b are the same sequence of
// vertices, forward or exactly reversed (OGC treats a line and its reverse
// as the same point-set).
func lineStringsEqual(a, b geoLineString) bool {
	if len(a) != len(b) {
		return false
	}
	forward := true
	for i := range a {
		if a[i].Lon != b[i].Lon || a[i].Lat != b[i].Lat {
			forward = false
			break
		}
	}
	if forward {
		return true
	}
	n := len(a)
	for i := range a {
		bp := b[n-1-i]
		if a[i].Lon != bp.Lon || a[i].Lat != bp.Lat {
			return false
		}
	}
	return true
}

func geoPolygonSetsEqual(aObj, bObj map[string]any) (any, error) {
	aMP, err := geoMultiPolygonFromValue(aObj)
	if err != nil {
		return nil, err
	}
	bMP, err := geoMultiPolygonFromValue(bObj)
	if err != nil {
		return nil, err
	}
	if len(aMP.Polygons) != len(bMP.Polygons) {
		return false, nil
	}
	for i := range aMP.Polygons {
		pa, pb := aMP.Polygons[i], bMP.Polygons[i]
		if len(pa.Rings) != len(pb.Rings) {
			return false, nil
		}
		for j := range pa.Rings {
			if !ringsEqual(pa.Rings[j], pb.Rings[j]) {
				return false, nil
			}
		}
	}
	return true, nil
}

// ringsEqual reports whether closed rings a and b trace the same closed
// point-set, allowing for a different start vertex and/or winding
// direction -- the two most common "same boundary, different encoding"
// mismatches. This does NOT detect a ring with an extra collinear vertex
// splitting an edge as equal to one without it -- that is a documented v1
// limitation, not full OGC point-set equality (see the package doc comment
// on evalGeoEquals).
func ringsEqual(a, b geoRing) bool {
	ao := dropRingClosure(a)
	bo := dropRingClosure(b)
	if len(ao) != len(bo) {
		return false
	}
	n := len(ao)
	if n == 0 {
		return true
	}
	candidates := [][]geoPoint{bo, reverseGeoPoints(bo)}
	for _, candidate := range candidates {
		for offset := 0; offset < n; offset++ {
			match := true
			for i := 0; i < n; i++ {
				p := ao[i]
				q := candidate[(i+offset)%n]
				if p.Lon != q.Lon || p.Lat != q.Lat {
					match = false
					break
				}
			}
			if match {
				return true
			}
		}
	}
	return false
}

func dropRingClosure(ring geoRing) []geoPoint {
	if len(ring) > 1 && ring[0].Lon == ring[len(ring)-1].Lon && ring[0].Lat == ring[len(ring)-1].Lat {
		return ring[:len(ring)-1]
	}
	return ring
}

func reverseGeoPoints(pts []geoPoint) []geoPoint {
	out := make([]geoPoint, len(pts))
	for i, p := range pts {
		out[len(pts)-1-i] = p
	}
	return out
}
