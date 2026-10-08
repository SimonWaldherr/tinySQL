// A small cache of parsed polygons for constant arguments.
//
// ST_WITHIN(pt, <region>) and ST_CONTAINS(<region>, pt) are almost always
// evaluated with one constant region over many rows. The region is an argument
// like any other, so it was decoded from GeoJSON for every row: for a
// 5,000-vertex border that is hundreds of microseconds and a megabyte of
// allocation per row, far more than the point-in-polygon test itself.
//
// The cache holds the last few large polygon texts together with their parsed
// form and bounding box. A lookup is a handful of string comparisons -- Go
// compares equal strings with a pointer check first, so the same constant
// string is an O(1) hit -- and the read path takes no lock. Parsed polygons are
// shared between callers and must be treated as read-only.
package engine

import (
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
)

const (
	// geoPolyCacheSlots is how many distinct polygon texts are remembered.
	geoPolyCacheSlots = 8
	// geoPolyCacheMinText is the shortest text worth caching: below it, decoding
	// costs about as much as looking the text up.
	geoPolyCacheMinText = 160
	// geoPolyCacheMaxText bounds the memory one cached polygon can pin.
	geoPolyCacheMaxText = 1 << 20
)

// geoPolygonEntry is a parsed polygon plus the bounding box of all its rings.
type geoPolygonEntry struct {
	text      string
	mp        geoMultiPolygon
	hasBounds bool
	minX      float64
	minY      float64
	maxX      float64
	maxY      float64
}

var (
	geoPolyCache     [geoPolyCacheSlots]atomic.Pointer[geoPolygonEntry]
	geoPolyCacheNext atomic.Uint32
)

// newGeoPolygonEntry computes the bounding box over every ring. A coordinate that
// is NaN disables the bounding-box shortcut instead of corrupting it.
func newGeoPolygonEntry(text string, mp geoMultiPolygon) *geoPolygonEntry {
	e := &geoPolygonEntry{text: text, mp: mp, hasBounds: true}
	first := true
	for _, poly := range mp.Polygons {
		for _, ring := range poly.Rings {
			for _, p := range ring {
				if p.Lon != p.Lon || p.Lat != p.Lat {
					e.hasBounds = false
					return e
				}
				if first {
					e.minX, e.maxX, e.minY, e.maxY = p.Lon, p.Lon, p.Lat, p.Lat
					first = false
					continue
				}
				e.minX, e.maxX = min(e.minX, p.Lon), max(e.maxX, p.Lon)
				e.minY, e.maxY = min(e.minY, p.Lat), max(e.maxY, p.Lat)
			}
		}
	}
	e.hasBounds = !first
	return e
}

// contains is pointInMultiPolygon with a bounding-box rejection first. A point
// strictly outside the box of every ring is outside every polygon: the ray
// casting test crosses an even number of edges of an (implicitly closed) ring
// from there. NaN coordinates skip the shortcut.
func (e *geoPolygonEntry) contains(p geoPoint) bool {
	if e.hasBounds && p.Lon == p.Lon && p.Lat == p.Lat &&
		(p.Lon < e.minX || p.Lon > e.maxX || p.Lat < e.minY || p.Lat > e.maxY) {
		return false
	}
	return pointInMultiPolygon(p, e.mp)
}

// pointsIntersect is pointsIntersectPolygons with the bounding-box rejection: a
// point strictly outside every ring's box is neither inside nor on a boundary.
func (e *geoPolygonEntry) pointsIntersect(pts []geoPoint) bool {
	for _, p := range pts {
		if e.hasBounds && p.Lon == p.Lon && p.Lat == p.Lat &&
			(p.Lon < e.minX || p.Lon > e.maxX || p.Lat < e.minY || p.Lat > e.maxY) {
			continue
		}
		if pointInMultiPolygon(p, e.mp) || pointOnMultiPolygonBoundary(p, e.mp) {
			return true
		}
	}
	return false
}

// geoPolyCacheLookup returns the cached entry for text, or nil. It does no
// parsing and never scans text beyond the string comparison.
func geoPolyCacheLookup(text string) *geoPolygonEntry {
	if len(text) < geoPolyCacheMinText {
		return nil
	}
	for i := range geoPolyCache {
		if e := geoPolyCache[i].Load(); e != nil && e.text == text {
			return e
		}
	}
	return nil
}

// geoPolygonFromValueCached is geoMultiPolygonFromValue for a value that may be
// a repeated constant. The returned entry is shared and read-only.
func geoPolygonFromValueCached(v any) (*geoPolygonEntry, error) {
	text, isString := v.(string)
	if !isString || len(text) < geoPolyCacheMinText || len(text) > geoPolyCacheMaxText {
		mp, err := geoMultiPolygonFromValue(v)
		if err != nil {
			return nil, err
		}
		return newGeoPolygonEntry("", mp), nil
	}
	if e := geoPolyCacheLookup(text); e != nil {
		return e, nil
	}
	mp, err := geoMultiPolygonFromValue(v)
	if err != nil {
		return nil, err
	}
	e := newGeoPolygonEntry(text, mp)
	slot := geoPolyCacheNext.Add(1) % geoPolyCacheSlots
	geoPolyCache[slot].Store(e)
	return e, nil
}

func evalGeoPolygonEntryArg(env ExecEnv, ex *FuncCall, row Row, idx int) (*geoPolygonEntry, error) {
	v, err := evalExpr(env, ex.Args[idx], row)
	if err != nil {
		return nil, err
	}
	e, err := geoPolygonFromValueCached(v)
	if err != nil {
		return nil, fmt.Errorf("%s arg%d: %w", ex.Name, idx+1, err)
	}
	return e, nil
}

// ── decoded-object cache ───────────────────────────────────────────────────
//
// The map-based GIS functions (relations, clipping, WKT, ...) decode their
// geometry arguments with geoObjectFromValue. For a large constant argument that
// is the same decode on every row, so the last few large texts are kept with
// their decoded maps. geoObjectFromValue has always handed callers the caller's
// own map when given one, so its callers already treat the result as read-only;
// a function that wants to modify a geometry goes through geoSimplifyObject,
// which clones. TestGeoObjectCacheIsNeverMutated enforces that for every
// function.
const (
	geoObjCacheSlots   = 8
	geoObjCacheMinText = 256
	geoObjCacheMaxText = 1 << 20
)

type geoObjectEntry struct {
	text string
	obj  map[string]any
	// ptr identifies obj: geoCachedObjectPolygon finds the entry of a map by its
	// address, so only a map that really is the cached one gets the typed form.
	ptr uintptr
	// The typed polygon form of obj, converted at most once and shared
	// read-only. Its error, if any, is what converting obj would report.
	once sync.Once
	mp   geoMultiPolygon
	err  error
}

// geoCachedObjectPolygon returns the typed polygon form of obj if obj is one of
// the cached decoded objects.
func geoCachedObjectPolygon(obj map[string]any) (*geoObjectEntry, bool) {
	if obj == nil {
		return nil, false
	}
	ptr := reflect.ValueOf(obj).Pointer()
	for i := range geoObjCache {
		e := geoObjCache[i].Load()
		if e == nil || e.ptr != ptr {
			continue
		}
		e.once.Do(func() { e.mp, e.err = geoMultiPolygonFromObject(e.obj) })
		return e, true
	}
	return nil, false
}

var (
	geoObjCache     [geoObjCacheSlots]atomic.Pointer[geoObjectEntry]
	geoObjCacheNext atomic.Uint32
)

func geoObjCacheLookup(text string) map[string]any {
	if len(text) < geoObjCacheMinText {
		return nil
	}
	for i := range geoObjCache {
		if e := geoObjCache[i].Load(); e != nil && e.text == text {
			return e.obj
		}
	}
	return nil
}

func geoObjCacheStore(text string, obj map[string]any) {
	if len(text) < geoObjCacheMinText || len(text) > geoObjCacheMaxText || obj == nil {
		return
	}
	slot := geoObjCacheNext.Add(1) % geoObjCacheSlots
	geoObjCache[slot].Store(&geoObjectEntry{text: text, obj: obj, ptr: reflect.ValueOf(obj).Pointer()})
}
