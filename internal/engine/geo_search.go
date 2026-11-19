package engine

import (
	"context"
	"fmt"
	"math"
	"slices"
	"strings"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

// GeoSearchTableFunc implements GEO_SEARCH(table, geom_col, mode, ...), a
// table-valued function following VEC_SEARCH's exact calling convention
// (one function name, a mode string argument selects the internal
// structure) rather than one function per mode -- see vector_search.go's
// VecSearchTableFunc for the precedent this mirrors.
//
// GEO_SEARCH indexes each row by its geometry's centroid/bbox
// (spatial_index.go), so it is exact for Point columns -- the dominant BI
// case (store locations, sensor readouts, event feeds) -- but for
// polygon/line columns it answers "is this shape's bounding area within the
// query window." Use bbox_intersects for GIS layers whose polygon/line extent
// must overlap the viewport; bbox retains the legacy centroid-within behavior.
// The nearest mode orders centroids by distance within a radius and returns
// at most k rows.
// True shape-overlap search belongs to ST_INTERSECTS (geo_relate.go) run as an
// ordinary per-row filter after bbox_intersects has narrowed the candidates.
type GeoSearchTableFunc struct{}

func (f *GeoSearchTableFunc) Name() string { return "GEO_SEARCH" }

func (f *GeoSearchTableFunc) ValidateArgs(args []Expr) error {
	if len(args) < 6 {
		return fmt.Errorf("GEO_SEARCH requires at least (table, geom_col, mode, ...), got %d arguments", len(args))
	}
	return nil
}

func evalGeoSearchFloatArg(env ExecEnv, args []Expr, row Row, idx int) (float64, error) {
	v, err := evalExpr(env, args[idx], row)
	if err != nil {
		return 0, err
	}
	f, err := geoFloat(v)
	if err != nil {
		return 0, fmt.Errorf("GEO_SEARCH arg%d: %w", idx+1, err)
	}
	return f, nil
}

func (f *GeoSearchTableFunc) Execute(ctx context.Context, args []Expr, env ExecEnv, row Row) (*ResultSet, error) {
	if err := f.ValidateArgs(args); err != nil {
		return nil, err
	}

	tableVal, err := evalExpr(env, args[0], row)
	if err != nil {
		return nil, fmt.Errorf("GEO_SEARCH table: %w", err)
	}
	tableName, ok := tableVal.(string)
	if !ok {
		return nil, fmt.Errorf("GEO_SEARCH: table name must be a string, got %T", tableVal)
	}

	colVal, err := evalExpr(env, args[1], row)
	if err != nil {
		return nil, fmt.Errorf("GEO_SEARCH geom_col: %w", err)
	}
	colName, ok := colVal.(string)
	if !ok {
		return nil, fmt.Errorf("GEO_SEARCH: geom_col must be a string, got %T", colVal)
	}

	modeVal, err := evalExpr(env, args[2], row)
	if err != nil {
		return nil, fmt.Errorf("GEO_SEARCH mode: %w", err)
	}
	modeStr, ok := modeVal.(string)
	if !ok {
		return nil, fmt.Errorf("GEO_SEARCH: mode must be a string, got %T", modeVal)
	}
	mode := strings.ToLower(strings.TrimSpace(modeStr))

	tenant := env.tenant
	if tenant == "" {
		tenant = "default"
	}
	table, err := env.db.Get(tenant, tableName)
	if err != nil {
		return nil, fmt.Errorf("GEO_SEARCH: table %q not found: %w", tableName, err)
	}
	colIdx, err := table.ColIndex(colName)
	if err != nil {
		return nil, fmt.Errorf("GEO_SEARCH: %w", err)
	}

	var selectCandidates func(*geoGridIndex) []int32
	var residual func(idx *geoGridIndex, rowIdx int32) bool
	var nearestLimit int
	var radiusMeters float64
	var radiusDistance func(geoGridCentroid) float64
	switch mode {
	case "bbox", "bbox_intersects":
		if len(args) != 7 {
			return nil, fmt.Errorf("GEO_SEARCH bbox mode requires (table, geom_col, 'bbox', minLon, minLat, maxLon, maxLat)")
		}
		minLon, err := evalGeoSearchFloatArg(env, args, row, 3)
		if err != nil {
			return nil, err
		}
		minLat, err := evalGeoSearchFloatArg(env, args, row, 4)
		if err != nil {
			return nil, err
		}
		maxLon, err := evalGeoSearchFloatArg(env, args, row, 5)
		if err != nil {
			return nil, err
		}
		maxLat, err := evalGeoSearchFloatArg(env, args, row, 6)
		if err != nil {
			return nil, err
		}
		if !geoSearchFinite(minLon, minLat, maxLon, maxLat) {
			return nil, fmt.Errorf("GEO_SEARCH: bbox coordinates must be finite numbers")
		}
		if mode == "bbox_intersects" && (minLon < -180 || minLon > 180 || maxLon < -180 || maxLon > 180 || minLat < -90 || minLat > 90 || maxLat < -90 || maxLat > 90) {
			return nil, fmt.Errorf("GEO_SEARCH: bbox_intersects requires WGS84 longitude/latitude bounds")
		}
		if mode == "bbox" {
			selectCandidates = func(idx *geoGridIndex) []int32 {
				return idx.candidatesCentroidBBox(minLon, minLat, maxLon, maxLat)
			}
			residual = func(idx *geoGridIndex, rowIdx int32) bool {
				return geoPointWithinBBox(idx.centroids[rowIdx].point(), minLon, minLat, maxLon, maxLat)
			}
		} else {
			if minLat > maxLat {
				minLat, maxLat = maxLat, minLat
			}
			crossesAntimeridian := minLon > maxLon
			selectCandidates = func(idx *geoGridIndex) []int32 {
				if crossesAntimeridian {
					return geoMergeCandidateRows(
						idx.candidatesBBox(minLon, minLat, 180, maxLat),
						idx.candidatesBBox(-180, minLat, maxLon, maxLat),
					)
				}
				return idx.candidatesBBox(minLon, minLat, maxLon, maxLat)
			}
			residual = func(idx *geoGridIndex, rowIdx int32) bool {
				box := idx.bboxes[rowIdx]
				if !box.Set || box.MaxY < minLat || box.MinY > maxLat {
					return false
				}
				if crossesAntimeridian {
					return box.MaxX >= minLon || box.MinX <= maxLon
				}
				return box.MaxX >= minLon && box.MinX <= maxLon
			}
		}
	case "radius", "nearest":
		if mode == "nearest" {
			if len(args) != 7 {
				return nil, fmt.Errorf("GEO_SEARCH nearest mode requires (table, geom_col, 'nearest', centerLon, centerLat, radiusMeters, k)")
			}
			k, err := evalGeoSearchFloatArg(env, args, row, 6)
			if err != nil {
				return nil, err
			}
			if !(k > 0 && k <= math.MaxInt32 && k == math.Trunc(k)) {
				return nil, fmt.Errorf("GEO_SEARCH: k must be a positive integer no greater than %d", math.MaxInt32)
			}
			nearestLimit = int(k)
		} else if len(args) != 6 {
			return nil, fmt.Errorf("GEO_SEARCH radius mode requires (table, geom_col, 'radius', centerLon, centerLat, radiusMeters)")
		}
		centerLon, err := evalGeoSearchFloatArg(env, args, row, 3)
		if err != nil {
			return nil, err
		}
		centerLat, err := evalGeoSearchFloatArg(env, args, row, 4)
		if err != nil {
			return nil, err
		}
		radiusMeters, err = evalGeoSearchFloatArg(env, args, row, 5)
		if err != nil {
			return nil, err
		}
		if radiusMeters <= 0 || math.IsNaN(radiusMeters) || math.IsInf(radiusMeters, 0) {
			return nil, fmt.Errorf("GEO_SEARCH: radius must be a finite positive number of meters")
		}
		if !geoSearchFinite(centerLon, centerLat) {
			return nil, fmt.Errorf("GEO_SEARCH: radius center must contain finite coordinates")
		}
		if mode == "nearest" && (math.Abs(centerLon) > 180 || math.Abs(centerLat) > 90) {
			return nil, fmt.Errorf("GEO_SEARCH: nearest requires a WGS84 center")
		}
		selectCandidates = func(idx *geoGridIndex) []int32 {
			return idx.candidatesRadius(centerLon, centerLat, radiusMeters)
		}
		// cos(centerLat) is loop-invariant across every candidate this
		// residual check re-verifies, so it is computed once here instead
		// of once per candidate inside haversineMeters — see
		// haversineMetersFromOrigin.
		cosCenterLat := math.Cos(centerLat * math.Pi / 180)
		radiusDistance = func(p geoGridCentroid) float64 {
			return p.distanceFrom(centerLat, cosCenterLat, centerLon)
		}
		residual = func(idx *geoGridIndex, rowIdx int32) bool {
			return radiusDistance(idx.centroids[rowIdx]) <= radiusMeters
		}
	default:
		return nil, fmt.Errorf("GEO_SEARCH: unknown mode %q (supported: bbox, bbox_intersects, radius, nearest)", modeStr)
	}

	// Parse and validate the complete query before paying a possible cold grid
	// build. Invalid user input should be cheap even for a large geometry table.
	searchCtx := ctx
	if searchCtx == nil {
		searchCtx = env.ctx
	}
	if err := checkCtx(searchCtx); err != nil {
		return nil, err
	}
	idx, err := getGeoGridIndex(searchCtx, tenant, table, colIdx)
	if err != nil {
		return nil, fmt.Errorf("GEO_SEARCH: %w", err)
	}
	candidates := selectCandidates(idx)
	if nearestLimit > 0 {
		nearest, err := geoSearchTopK(searchCtx, idx, candidates, nearestLimit, radiusMeters, radiusDistance)
		if err != nil {
			return nil, err
		}
		cols := geoSearchColumns(table, 2)
		cols = append(cols, "_geo_distance_m")
		result := &ResultSet{Cols: cols, Rows: make([]Row, len(nearest))}
		for i, hit := range nearest {
			r := geoSearchResultRow(table, hit.rowIdx, len(cols), i+1)
			r["_geo_distance_m"] = hit.distance
			result.Rows[i] = r
		}
		return result, nil
	}

	// Candidates are query-local; compact matches into the same allocation.
	matched := candidates[:0]
	for i, rowIdx := range candidates {
		if i&255 == 0 {
			if err := checkCtx(searchCtx); err != nil {
				return nil, err
			}
		}
		if rowIdx < 0 || int(rowIdx) >= len(idx.valid) || !idx.valid[rowIdx] {
			continue
		}
		if residual(idx, rowIdx) {
			matched = append(matched, rowIdx)
		}
	}
	slices.Sort(matched)

	resultCols := geoSearchColumns(table, 1)
	resultRows := make([]Row, 0, len(matched))
	for rank, rowIdx := range matched {
		if int(rowIdx) >= len(table.Rows) {
			continue
		}
		resultRows = append(resultRows, geoSearchResultRow(table, int(rowIdx), len(resultCols), rank+1))
	}
	return &ResultSet{Cols: resultCols, Rows: resultRows}, nil
}

func geoSearchTopK(ctx context.Context, idx *geoGridIndex, candidates []int32, k int, radiusMeters float64, distance func(geoGridCentroid) float64) ([]vecScoredRow, error) {
	heap := newScoredHeap(k, len(candidates))
	for i, rowID := range candidates {
		if i&255 == 0 {
			if err := checkCtx(ctx); err != nil {
				return nil, err
			}
		}
		p := idx.centroids[rowID]
		if !(math.Abs(p.Lon) <= 180 && math.Abs(p.Lat) <= 90) {
			continue
		}
		if meters := distance(p); meters <= radiusMeters {
			pushTopK(heap, int(rowID), meters, k)
		}
	}
	return topKFromHeap(heap, k), nil
}

func geoSearchColumns(table *storage.Table, extra int) []string {
	cols := make([]string, 0, len(table.Cols)+extra)
	for _, c := range table.Cols {
		cols = append(cols, c.Name)
	}
	return append(cols, "_geo_rank")
}

func geoSearchResultRow(table *storage.Table, rowID, numCols, rank int) Row {
	values := table.Rows[rowID]
	r := make(Row, numCols)
	for ci, c := range table.Cols[:min(len(table.Cols), len(values))] {
		r[c.Name] = values[ci]
	}
	r["_geo_rank"] = rank
	return r
}

func geoSearchFinite(values ...float64) bool {
	for _, value := range values {
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return false
		}
	}
	return true
}

func init() {
	RegisterTableFunc(&GeoSearchTableFunc{})
}
