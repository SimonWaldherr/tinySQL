// Geometry-collecting aggregate functions: GEO_DISSOLVE/GEO_UNION_AGG/
// ST_UNION (dispatch by geometry type; polygons go through geo_dissolve.go's
// shared-edge-cancellation dissolve, points/lines are trivially
// concatenated into a Multi* geometry), GEO_BBOX_AGG (fold every row's own
// bounding box), and GEO_CENTROID_AGG (optionally weighted centroid,
// folding the existing per-row GEO_CENTROID rather than re-deriving its
// area/length-weighting rules).
//
// These are registered as aggregates the same way every other one is in
// this codebase: added to isAggregate()'s name switch (so they trigger
// aggregation even without an unrelated GROUP BY forcing it) and to
// evalAggregateFuncCall()'s dispatch switch (both in eval_aggregate.go).
package engine

import (
	"encoding/json"
	"fmt"
	"math"
)

// geoDissolveMaxResultBytes defensively bounds a dissolve/union-agg result.
// Nothing upstream caps an aggregate's returned value size (unlike
// MaxBlobBytes, which only applies to BLOB-typed column coercion), so a
// pathologically large merged geometry would otherwise just grow the
// process's heap unbounded.
const geoDissolveMaxResultBytes = 16 << 20

func marshalGeoAggregateResult(name string, object map[string]any) (any, error) {
	body, err := json.Marshal(object)
	if err != nil {
		return nil, fmt.Errorf("%s: encode result: %w", name, err)
	}
	if len(body) > geoDissolveMaxResultBytes {
		return nil, fmt.Errorf("%s: result is %d bytes, exceeding the %d byte limit", name, len(body), geoDissolveMaxResultBytes)
	}
	return string(body), nil
}

// evalAggregateGeoDissolve backs GEO_DISSOLVE, GEO_UNION_AGG and ST_UNION --
// one shared handler, since for polygons "dissolve a group" and "union a
// group" are the same edge-cancellation operation (mirrors how GEO_BBOX/
// ST_BBOX already share one implementation, and MIN_BY/ARG_MIN share
// another).
func evalAggregateGeoDissolve(env ExecEnv, ex *FuncCall, rows []Row) (any, error) {
	if len(ex.Args) < 1 || len(ex.Args) > 2 {
		return nil, fmt.Errorf("%s expects (geometry [, snap_grid_degrees])", ex.Name)
	}
	snapDegrees := 0.0
	if len(ex.Args) == 2 && len(rows) > 0 {
		v, err := evalExpr(env, ex.Args[1], rows[0])
		if err != nil {
			return nil, err
		}
		f, err := geoFloat(v)
		if err != nil {
			return nil, fmt.Errorf("%s snap grid: %w", ex.Name, err)
		}
		if f <= 0 || math.IsNaN(f) || math.IsInf(f, 0) {
			return nil, fmt.Errorf("%s snap grid must be a positive finite number", ex.Name)
		}
		snapDegrees = f
	}

	kind := geoRelateUnknown
	var objects []map[string]any
	for _, r := range rows {
		if err := checkCtx(env.ctx); err != nil {
			return nil, err
		}
		v, err := evalExpr(env, ex.Args[0], r)
		if err != nil {
			return nil, err
		}
		if v == nil {
			continue
		}
		obj, err := geoObjectFromValue(v)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", ex.Name, err)
		}
		rowKind, err := classifyGeoRelateKind(obj)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", ex.Name, err)
		}
		if kind == geoRelateUnknown {
			kind = rowKind
		} else if kind != rowKind {
			return nil, fmt.Errorf("%s requires every geometry in the group to be the same broad type (points, lines, or polygons)", ex.Name)
		}
		objects = append(objects, obj)
	}
	if len(objects) == 0 {
		return nil, nil
	}

	switch kind {
	case geoRelatePoints:
		return dissolvePointGroup(ex.Name, objects)
	case geoRelateLines:
		return dissolveLineGroup(ex.Name, objects)
	case geoRelatePolygons:
		parts := make([]geoMultiPolygon, 0, len(objects))
		for _, obj := range objects {
			mp, err := geoMultiPolygonFromValue(obj)
			if err != nil {
				return nil, fmt.Errorf("%s: %w", ex.Name, err)
			}
			parts = append(parts, mp)
		}
		result, err := dissolvePolygons(parts, snapDegrees)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", ex.Name, err)
		}
		return marshalGeoAggregateResult(ex.Name, result)
	default:
		return nil, fmt.Errorf("%s: unsupported geometry type", ex.Name)
	}
}

func dissolvePointGroup(name string, objects []map[string]any) (any, error) {
	var points []geoPoint
	for _, obj := range objects {
		pts, err := geoPointsFromObject(obj)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		points = append(points, pts...)
	}
	return marshalGeoAggregateResult(name, map[string]any{
		"type":        "MultiPoint",
		"coordinates": ringToCoordinates(points),
	})
}

func dissolveLineGroup(name string, objects []map[string]any) (any, error) {
	var lines []geoLineString
	for _, obj := range objects {
		ls, err := geoLinesFromObject(obj)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		lines = append(lines, ls...)
	}
	coords := make([]any, len(lines))
	for i, ls := range lines {
		coords[i] = ringToCoordinates(ls)
	}
	return marshalGeoAggregateResult(name, map[string]any{
		"type":        "MultiLineString",
		"coordinates": coords,
	})
}

// evalAggregateGeoBBoxAgg folds every row's own bounding box (via the same
// collectGeoBBox accumulator GEO_BBOX/ST_BBOX use for one geometry) into a
// single [minLon, minLat, maxLon, maxLat] result.
func evalAggregateGeoBBoxAgg(env ExecEnv, ex *FuncCall, rows []Row) (any, error) {
	if len(ex.Args) != 1 {
		return nil, fmt.Errorf("%s expects 1 argument", ex.Name)
	}
	acc := geoEditBBox{}
	for _, r := range rows {
		if err := checkCtx(env.ctx); err != nil {
			return nil, err
		}
		v, err := evalExpr(env, ex.Args[0], r)
		if err != nil {
			return nil, err
		}
		if v == nil {
			continue
		}
		object, err := geoSimplifyObject(v)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", ex.Name, err)
		}
		if err := collectGeoBBox(object, &acc); err != nil {
			return nil, fmt.Errorf("%s: %w", ex.Name, err)
		}
	}
	if !acc.Set {
		return nil, nil
	}
	body, err := json.Marshal([]float64{acc.MinX, acc.MinY, acc.MaxX, acc.MaxY})
	if err != nil {
		return nil, fmt.Errorf("%s: encode result: %w", ex.Name, err)
	}
	return string(body), nil
}

// evalAggregateGeoCentroidAgg computes a (optionally weighted) centroid
// across a group of geometries by folding the existing, unmodified
// GEO_CENTROID result per row -- rather than re-deriving its area/length
// weighting rules -- through an optional second expression's business
// weight (population, revenue, ...; default 1). This combines geometric
// weighting (already inside GEO_CENTROID: area for polygons, length for
// lines) with business weighting without either layer needing to know
// about the other, following the MIN_BY/MAX_BY "two expressions evaluated
// together per row" pattern (eval_aggregate.go).
func evalAggregateGeoCentroidAgg(env ExecEnv, ex *FuncCall, rows []Row) (any, error) {
	if len(ex.Args) < 1 || len(ex.Args) > 2 {
		return nil, fmt.Errorf("%s expects (geometry [, weight])", ex.Name)
	}
	var sumX, sumY, sumW float64
	for _, r := range rows {
		if err := checkCtx(env.ctx); err != nil {
			return nil, err
		}
		v, err := evalExpr(env, ex.Args[0], r)
		if err != nil {
			return nil, err
		}
		if v == nil {
			continue
		}
		centroidVal, err := geoCentroidFromValue("GEO_CENTROID", v)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", ex.Name, err)
		}
		if centroidVal == nil {
			continue
		}
		p, err := geoPointFromValue(centroidVal)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", ex.Name, err)
		}
		weight := 1.0
		if len(ex.Args) == 2 {
			wv, err := evalExpr(env, ex.Args[1], r)
			if err != nil {
				return nil, err
			}
			if wv == nil {
				continue
			}
			wf, ok := numeric(wv)
			if !ok || wf <= 0 || math.IsNaN(wf) || math.IsInf(wf, 0) {
				return nil, fmt.Errorf("%s: weight must be a positive finite number, got %v", ex.Name, wv)
			}
			weight = wf
		}
		sumX += p.Lon * weight
		sumY += p.Lat * weight
		sumW += weight
	}
	if sumW == 0 {
		return nil, nil
	}
	return geoPointJSON(sumX/sumW, sumY/sumW, nil)
}
