// A lazy, version-invalidated spatial index for GEO_SEARCH, following the
// exact same shape as the Vector index (vector_index.go) and FTS's doc
// cache (fts.go): tinySQL has no R-tree and no CREATE INDEX ... USING
// clause, so a fundamentally different index structure is bolted on the
// side as a table-valued-function-scoped cache rather than wired into the
// query planner (which today only ever recognizes *Binary comparisons in a
// WHERE clause, never a *FuncCall predicate like GEO_DWITHIN).
//
// The structure itself is a fixed-size equirectangular lon/lat grid: each
// row's geometry is bucketed into every cell its bounding box touches, by
// its own bounding box (not just its centroid), so a geometry spanning
// several cells is a candidate from all of them. Candidate selection is
// deliberately a superset filter -- GEO_SEARCH always re-verifies every
// candidate against the exact, unmodified GEO_WITHIN_BBOX/GEO_DWITHIN math
// (geo_search.go), the same "index narrows, per-row function call verifies"
// division of labor plan_range_index.go already documents for its own
// 1-D range seeks.
package engine

import (
	"context"
	"math"
	"math/bits"
	"sync"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

const (
	// geoGridTargetRowsPerCell aims for roughly this many candidate rows per
	// cell at query time -- small enough that the residual exact-test stays
	// cheap, large enough that cell bookkeeping overhead does not dominate.
	geoGridTargetRowsPerCell = 8
	// geoGridMinCellSizeDegrees floors cell size so an all-identical-point
	// (or otherwise zero-extent) table doesn't divide by zero when computing
	// cell size from its bounding box.
	geoGridMinCellSizeDegrees = 1e-6
	// geoGridMaxCellsPerGeometry bounds how many cells one geometry's bbox
	// may be fanned out into. A single geometry spanning most of the grid
	// (e.g. one huge country polygon sharing a table with many small points)
	// would otherwise touch nearly every cell, defeating the index; such
	// geometries are instead added to a small overflow list every query
	// always checks, same as a table with no useful index at all would.
	geoGridMaxCellsPerGeometry = 4096
	// geoGridCacheMaxEntries bounds the index cache, mirroring
	// vecIndexCacheMaxEntries (vector_index.go).
	geoGridCacheMaxEntries = 64
)

type geoCellID struct {
	X int32
	Y int32
}

type geoIndexCacheKey struct {
	tenant string
	table  string
	colIdx int
}

type geoGridIndex struct {
	table       *storage.Table
	version     int
	cellSizeLon float64
	cellSizeLat float64
	// bounds is the union geometry extent used to build the grid. Intersecting
	// a query with it before converting to cell coordinates prevents a broad
	// query from walking millions of cells that the sparse grid cannot contain
	// (especially for a zero-extent point corpus at the minimum cell size).
	bounds    geoEditBBox
	cells     map[geoCellID][]int32
	overflow  []int32
	centroids []geoPoint
	// bboxes retains the already-computed per-row extent. GIS/RAG viewport
	// filters can therefore perform an exact bbox residual check without
	// reparsing GeoJSON after the grid has narrowed the candidate set.
	bboxes      []geoEditBBox
	valid       []bool
	uniqueCells bool // no row occurs in multiple cells; overflow rows are disjoint
}

var (
	geoGridCacheMu sync.RWMutex
	geoGridCache   = make(map[geoIndexCacheKey]*geoGridIndex)
	// geoGridBuilds coalesces concurrent cold index builds for the same
	// (tenant, table, colIdx) key, mirroring vecIVFBuilds/vecHNSWBuilds
	// (vector_index.go) -- reusing the exact same vecIndexBuildCall type,
	// since the coalescing pattern (a done channel guarding one in-flight
	// build) has nothing vector-specific about it.
	geoGridBuilds = make(map[geoIndexCacheKey]*vecIndexBuildCall)
)

// purgeGeoGridCachesFor drops every cached grid index for one table, the
// GEO_SEARCH counterpart of purgeVectorCachesFor (vector_search.go) --
// called from the same DROP TABLE / rollback call sites so a dropped and
// recreated table's stale row indices can never leak through.
func purgeGeoGridCachesFor(tenant, table string) {
	geoGridCacheMu.Lock()
	for k := range geoGridCache {
		if k.tenant == tenant && k.table == table {
			delete(geoGridCache, k)
		}
	}
	geoGridCacheMu.Unlock()
}

func getGeoGridIndex(ctx context.Context, tenant string, table *storage.Table, colIdx int) (*geoGridIndex, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	key := geoIndexCacheKey{tenant: tenant, table: table.Name, colIdx: colIdx}

	for {
		geoGridCacheMu.RLock()
		if idx := geoGridCache[key]; idx != nil && idx.table == table && idx.version == table.Version {
			geoGridCacheMu.RUnlock()
			return idx, nil
		}
		geoGridCacheMu.RUnlock()

		geoGridCacheMu.Lock()
		if idx := geoGridCache[key]; idx != nil && idx.table == table && idx.version == table.Version {
			geoGridCacheMu.Unlock()
			return idx, nil
		}
		if call := geoGridBuilds[key]; call != nil {
			geoGridCacheMu.Unlock()
			select {
			case <-call.done:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			continue
		}
		call := &vecIndexBuildCall{done: make(chan struct{})}
		geoGridBuilds[key] = call
		geoGridCacheMu.Unlock()

		idx, err := buildGeoGridIndex(ctx, table, colIdx)

		geoGridCacheMu.Lock()
		delete(geoGridBuilds, key)
		if err == nil {
			if _, exists := geoGridCache[key]; !exists {
				evictOverCap(geoGridCache, geoGridCacheMaxEntries)
			}
			geoGridCache[key] = idx
		}
		close(call.done)
		geoGridCacheMu.Unlock()
		return idx, err
	}
}

// buildGeoGridIndex makes one pass to find every row's bbox/centroid (via
// the existing collectGeoBBox/collectGeoCentroid accumulators -- the same
// ones GEO_BBOX/GEO_CENTROID already use per-row), picks a cell size from
// the resulting row count and union bbox, then a second pass buckets each
// row into every cell its own bbox touches.
func buildGeoGridIndex(ctx context.Context, table *storage.Table, colIdx int) (*geoGridIndex, error) {
	idx := &geoGridIndex{table: table, version: table.Version, uniqueCells: true}
	n := len(table.Rows)
	idx.valid = make([]bool, n)
	idx.centroids = make([]geoPoint, n)
	idx.bboxes = make([]geoEditBBox, n)
	union := geoEditBBox{}
	validCount := 0

	for i, row := range table.Rows {
		if i&1023 == 0 {
			if err := checkCtx(ctx); err != nil {
				return nil, err
			}
		}
		if colIdx >= len(row) || row[colIdx] == nil {
			continue
		}
		object, err := geoSimplifyObject(row[colIdx])
		if err != nil {
			continue // not parseable geometry: excluded from the index, not a build failure
		}
		bbox := geoEditBBox{}
		if err := collectGeoBBox(object, &bbox); err != nil || !bbox.Set {
			continue
		}
		acc := geoCentroidAccumulator{}
		var cx, cy float64
		if err := collectGeoCentroid(object, &acc); err == nil && acc.Weight != 0 {
			cx, cy = acc.X/acc.Weight, acc.Y/acc.Weight
		} else if len(acc.Fallback) > 0 {
			for _, p := range acc.Fallback {
				cx += p.X
				cy += p.Y
			}
			cx /= float64(len(acc.Fallback))
			cy /= float64(len(acc.Fallback))
		} else {
			continue
		}

		idx.valid[i] = true
		idx.centroids[i] = geoPoint{Lon: cx, Lat: cy}
		idx.bboxes[i] = bbox
		union.add(geoEditPoint{X: bbox.MinX, Y: bbox.MinY})
		union.add(geoEditPoint{X: bbox.MaxX, Y: bbox.MaxY})
		validCount++
	}

	idx.cells = make(map[geoCellID][]int32)
	idx.bounds = union
	idx.cellSizeLon, idx.cellSizeLat = geoGridMinCellSizeDegrees, geoGridMinCellSizeDegrees
	if validCount == 0 {
		return idx, nil
	}

	targetCells := validCount / geoGridTargetRowsPerCell
	if targetCells < 1 {
		targetCells = 1
	}
	cellsPerAxis := math.Ceil(math.Sqrt(float64(targetCells)))
	cellSizeLon := (union.MaxX - union.MinX) / cellsPerAxis
	cellSizeLat := (union.MaxY - union.MinY) / cellsPerAxis
	if !(cellSizeLon > geoGridMinCellSizeDegrees) {
		cellSizeLon = geoGridMinCellSizeDegrees
	}
	if !(cellSizeLat > geoGridMinCellSizeDegrees) {
		cellSizeLat = geoGridMinCellSizeDegrees
	}
	idx.cellSizeLon, idx.cellSizeLat = cellSizeLon, cellSizeLat

	for i := 0; i < n; i++ {
		if i&1023 == 0 {
			if err := checkCtx(ctx); err != nil {
				return nil, err
			}
		}
		if !idx.valid[i] {
			continue
		}
		bbox := idx.bboxes[i]
		minCX := int32(math.Floor(bbox.MinX / cellSizeLon))
		maxCX := int32(math.Floor(bbox.MaxX / cellSizeLon))
		minCY := int32(math.Floor(bbox.MinY / cellSizeLat))
		maxCY := int32(math.Floor(bbox.MaxY / cellSizeLat))
		cellCount := int64(maxCX-minCX+1) * int64(maxCY-minCY+1)
		if cellCount <= 0 || cellCount > geoGridMaxCellsPerGeometry {
			idx.overflow = append(idx.overflow, int32(i))
			continue
		}
		if cellCount > 1 {
			idx.uniqueCells = false
		}
		for cx := minCX; cx <= maxCX; cx++ {
			for cy := minCY; cy <= maxCY; cy++ {
				cell := geoCellID{X: cx, Y: cy}
				idx.cells[cell] = append(idx.cells[cell], int32(i))
			}
		}
	}
	return idx, nil
}

// candidatesBBox returns every row (deduplicated) whose cell range overlaps
// the query bbox, plus every overflow row -- a superset of rows that could
// possibly satisfy GEO_WITHIN_BBOX; the caller re-verifies exactly.
func (idx *geoGridIndex) candidatesBBox(minLon, minLat, maxLon, maxLat float64) []int32 {
	if minLon > maxLon {
		minLon, maxLon = maxLon, minLon
	}
	if minLat > maxLat {
		minLat, maxLat = maxLat, minLat
	}
	if !idx.bounds.Set || maxLon < idx.bounds.MinX || minLon > idx.bounds.MaxX ||
		maxLat < idx.bounds.MinY || minLat > idx.bounds.MaxY {
		return nil
	}
	// A query covering the complete extent needs no cell traversal or
	// deduplication. Row order is already deterministic in this path.
	if minLon <= idx.bounds.MinX && maxLon >= idx.bounds.MaxX && minLat <= idx.bounds.MinY && maxLat >= idx.bounds.MaxY {
		out := make([]int32, 0, len(idx.valid))
		for row, valid := range idx.valid {
			if valid {
				out = append(out, int32(row))
			}
		}
		return out
	}
	// The result cannot change outside the indexed data extent. Clamping here
	// also keeps extreme-but-finite inputs away from int32 conversion overflow.
	minLon = math.Max(minLon, idx.bounds.MinX)
	maxLon = math.Min(maxLon, idx.bounds.MaxX)
	minLat = math.Max(minLat, idx.bounds.MinY)
	maxLat = math.Min(maxLat, idx.bounds.MaxY)
	minCX := int32(math.Floor(minLon / idx.cellSizeLon))
	maxCX := int32(math.Floor(maxLon / idx.cellSizeLon))
	minCY := int32(math.Floor(minLat / idx.cellSizeLat))
	maxCY := int32(math.Floor(maxLat / idx.cellSizeLat))

	// Point layers have no duplicates. Dense polygon queries use a compact
	// bitmap; selective queries keep a sparse set so a tiny viewport does not
	// allocate scratch memory proportional to a very large source table.
	cellCount := (int64(maxCX) - int64(minCX) + 1) * (int64(maxCY) - int64(minCY) + 1)
	useBitmap := !idx.uniqueCells && cellCount >= int64(len(idx.cells))/64
	var bitmap []uint64
	var seen map[int32]bool
	if useBitmap {
		bitmap = make([]uint64, (len(idx.valid)+63)/64)
	} else if !idx.uniqueCells {
		seen = make(map[int32]bool)
	}
	var out []int32
	addRows := func(rows []int32) {
		if idx.uniqueCells {
			out = append(out, rows...)
		} else if useBitmap {
			for _, row := range rows {
				bitmap[row/64] |= uint64(1) << (uint(row) % 64)
			}
		} else {
			for _, row := range rows {
				if !seen[row] {
					seen[row] = true
					out = append(out, row)
				}
			}
		}
	}
	addRows(idx.overflow)
	for cx := minCX; cx <= maxCX; cx++ {
		for cy := minCY; cy <= maxCY; cy++ {
			addRows(idx.cells[geoCellID{X: cx, Y: cy}])
		}
	}
	if useBitmap {
		count := 0
		for _, word := range bitmap {
			count += bits.OnesCount64(word)
		}
		out = make([]int32, 0, count)
		for i, word := range bitmap {
			for word != 0 {
				out = append(out, int32(i*64+bits.TrailingZeros64(word)))
				word &= word - 1
			}
		}
	}
	return out
}

// candidatesRadius bounds a spherical cap using the same Earth radius as
// haversineMeters. A linear meters-per-degree approximation can under-include
// boundary points, and any cap touching a pole must cover all longitudes.
func (idx *geoGridIndex) candidatesRadius(centerLon, centerLat, radiusMeters float64) []int32 {
	// GEO_SEARCH historically accepts any finite center. Outside WGS84, keep
	// the candidate pass conservative and let the existing distance test decide.
	if centerLon < -180 || centerLon > 180 || centerLat < -90 || centerLat > 90 {
		return idx.candidatesBBox(-math.MaxFloat64, -math.MaxFloat64, math.MaxFloat64, math.MaxFloat64)
	}
	// Round the bounds outward so floating-point error cannot exclude a point
	// accepted by the residual haversine comparison at the circle boundary.
	angle := radiusMeters/geoEarthRadiusMeters + 1e-12
	if angle >= math.Pi {
		return idx.candidatesBBox(-180, -90, 180, 90)
	}
	padLat := angle * 180 / math.Pi
	minLat := math.Max(-90, centerLat-padLat)
	maxLat := math.Min(90, centerLat+padLat)
	if minLat <= -90 || maxLat >= 90 {
		return idx.candidatesBBox(-180, minLat, 180, maxLat)
	}
	padLon := math.Asin(math.Min(1, math.Sin(angle)/math.Cos(centerLat*math.Pi/180))) * 180 / math.Pi
	minLon, maxLon := centerLon-padLon, centerLon+padLon

	if minLon < -180 {
		return geoMergeCandidateRows(
			idx.candidatesBBox(-180, minLat, maxLon, maxLat),
			idx.candidatesBBox(minLon+360, minLat, 180, maxLat),
		)
	}
	if maxLon > 180 {
		return geoMergeCandidateRows(
			idx.candidatesBBox(minLon, minLat, 180, maxLat),
			idx.candidatesBBox(-180, minLat, maxLon-360, maxLat),
		)
	}
	return idx.candidatesBBox(minLon, minLat, maxLon, maxLat)
}

func geoMergeCandidateRows(groups ...[]int32) []int32 {
	total := 0
	for _, group := range groups {
		total += len(group)
	}
	out := make([]int32, 0, total)
	seen := make(map[int32]struct{}, total)
	for _, group := range groups {
		for _, rowIdx := range group {
			if _, exists := seen[rowIdx]; exists {
				continue
			}
			seen[rowIdx] = struct{}{}
			out = append(out, rowIdx)
		}
	}
	return out
}
