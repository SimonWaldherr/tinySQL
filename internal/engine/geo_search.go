package engine

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
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
				return idx.candidatesBBox(minLon, minLat, maxLon, maxLat)
			}
			residual = func(idx *geoGridIndex, rowIdx int32) bool {
				return geoPointWithinBBox(idx.centroids[rowIdx], minLon, minLat, maxLon, maxLat)
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
	case "radius":
		if len(args) != 6 {
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
		radiusMeters, err := evalGeoSearchFloatArg(env, args, row, 5)
		if err != nil {
			return nil, err
		}
		if radiusMeters <= 0 || math.IsNaN(radiusMeters) || math.IsInf(radiusMeters, 0) {
			return nil, fmt.Errorf("GEO_SEARCH: radius must be a finite positive number of meters")
		}
		if !geoSearchFinite(centerLon, centerLat) {
			return nil, fmt.Errorf("GEO_SEARCH: radius center must contain finite coordinates")
		}
		selectCandidates = func(idx *geoGridIndex) []int32 {
			return idx.candidatesRadius(centerLon, centerLat, radiusMeters)
		}
		residual = func(idx *geoGridIndex, rowIdx int32) bool {
			p := idx.centroids[rowIdx]
			return haversineMeters(centerLat, centerLon, p.Lat, p.Lon) <= radiusMeters
		}
	default:
		return nil, fmt.Errorf("GEO_SEARCH: unknown mode %q (supported: bbox, bbox_intersects, radius)", modeStr)
	}

	// Parse and validate the complete query before paying a possible cold grid
	// build. Invalid user input should be cheap even for a large geometry table.
	searchCtx := ctx
	if searchCtx == nil {
		searchCtx = env.ctx
	}
	idx, err := getGeoGridIndex(searchCtx, tenant, table, colIdx)
	if err != nil {
		return nil, fmt.Errorf("GEO_SEARCH: %w", err)
	}
	candidates := selectCandidates(idx)

	var matched []int32
	for _, rowIdx := range candidates {
		if rowIdx < 0 || int(rowIdx) >= len(idx.valid) || !idx.valid[rowIdx] {
			continue
		}
		if residual(idx, rowIdx) {
			matched = append(matched, rowIdx)
		}
	}
	sort.Slice(matched, func(i, j int) bool { return matched[i] < matched[j] })

	resultCols := make([]string, 0, len(table.Cols)+1)
	for _, c := range table.Cols {
		resultCols = append(resultCols, c.Name)
	}
	resultCols = append(resultCols, "_geo_rank")

	resultRows := make([]Row, 0, len(matched))
	for rank, rowIdx := range matched {
		if int(rowIdx) >= len(table.Rows) {
			continue
		}
		r := make(Row)
		for ci, c := range table.Cols {
			if ci < len(table.Rows[rowIdx]) {
				r[c.Name] = table.Rows[rowIdx][ci]
			}
		}
		r["_geo_rank"] = rank + 1
		resultRows = append(resultRows, r)
	}
	return &ResultSet{Cols: resultCols, Rows: resultRows}, nil
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
