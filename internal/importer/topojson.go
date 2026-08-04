// TopoJSON v3 import: dequantize/un-delta-encode every arc once, resolve
// each geometry's arc-index references (including mapshaper- and
// topojson-server-style multi-arc-per-ring stitching, and the spec's
// ~i-means-reversed convention) back into plain GeoJSON coordinate arrays,
// then hand off to the SAME buildGeoJSONColumns/buildGeoJSONRow (geojson.go)
// ImportGeoJSON already uses -- both are already generic over any
// GeoJSON-Feature-shaped []map[string]any, so nothing there needs to change
// to accept a TopoJSON-derived source instead of a real GeoJSON document.
package importer

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

type topoJSONTransform struct {
	Scale     [2]float64 `json:"scale"`
	Translate [2]float64 `json:"translate"`
}

type topoJSONDocument struct {
	Type      string                     `json:"type"`
	Transform *topoJSONTransform         `json:"transform"`
	Arcs      [][][]float64              `json:"arcs"`
	Objects   map[string]json.RawMessage `json:"objects"`
}

// topoRawGeometry is the shape of one entry inside "objects" (or one child
// of a GeometryCollection): arcs/coordinates are left as json.RawMessage
// since their nesting depth depends on the geometry's own "type".
type topoRawGeometry struct {
	Type        string            `json:"type"`
	Arcs        json.RawMessage   `json:"arcs"`
	Coordinates json.RawMessage   `json:"coordinates"`
	Geometries  []topoRawGeometry `json:"geometries"`
	Properties  map[string]any    `json:"properties"`
}

// dequantizeArcs converts every arc into absolute float coordinates, once,
// up front. Per the TopoJSON spec, delta-encoding is only used together
// with a "transform" -- an arc in a transform-less Topology already holds
// absolute coordinates, so accumulation is skipped in that case rather than
// double-decoding coordinates that were never delta-encoded to begin with.
func dequantizeArcs(doc *topoJSONDocument) [][][2]float64 {
	out := make([][][2]float64, len(doc.Arcs))
	for i, arc := range doc.Arcs {
		pts := make([][2]float64, len(arc))
		if doc.Transform == nil {
			for j, p := range arc {
				if len(p) < 2 {
					continue
				}
				pts[j] = [2]float64{p[0], p[1]}
			}
		} else {
			var x, y float64
			for j, p := range arc {
				if len(p) < 2 {
					continue
				}
				if j == 0 {
					x, y = p[0], p[1]
				} else {
					x += p[0]
					y += p[1]
				}
				pts[j] = [2]float64{
					x*doc.Transform.Scale[0] + doc.Transform.Translate[0],
					y*doc.Transform.Scale[1] + doc.Transform.Translate[1],
				}
			}
		}
		out[i] = pts
	}
	return out
}

func detransformPoint(p [2]float64, transform *topoJSONTransform) [2]float64 {
	if transform == nil {
		return p
	}
	return [2]float64{
		p[0]*transform.Scale[0] + transform.Translate[0],
		p[1]*transform.Scale[1] + transform.Translate[1],
	}
}

// resolveArcRefs stitches one or more arc references (a ring's "arcs"
// entry) into a single absolute-coordinate position sequence, handling the
// spec's negative-index-means-reversed convention (~i, i.e. -i-1) and
// dropping the duplicated shared vertex between consecutive stitched arcs.
// This project's own exporter only ever emits a single-arc ring (no
// partial-arc splitting), but a general reader -- needed to import files
// built by other tools, e.g. mapshaper itself -- must handle a ring
// referencing multiple concatenated arcs.
func resolveArcRefs(refs []int, arcs [][][2]float64) ([][2]float64, error) {
	var out [][2]float64
	for i, ref := range refs {
		idx := ref
		reversed := false
		if ref < 0 {
			idx = -ref - 1
			reversed = true
		}
		if idx < 0 || idx >= len(arcs) {
			return nil, fmt.Errorf("arc index %d out of range (have %d arcs)", ref, len(arcs))
		}
		pts := arcs[idx]
		if reversed {
			rev := make([][2]float64, len(pts))
			for j, p := range pts {
				rev[len(pts)-1-j] = p
			}
			pts = rev
		}
		if i > 0 && len(out) > 0 && len(pts) > 0 {
			pts = pts[1:]
		}
		out = append(out, pts...)
	}
	return out, nil
}

func positionsJSON(pts [][2]float64) []any {
	out := make([]any, len(pts))
	for i, p := range pts {
		out[i] = []float64{p[0], p[1]}
	}
	return out
}

// resolveTopoGeometry converts one decoded topoRawGeometry (Point/
// MultiPoint hold quantized "coordinates" directly; every other type holds
// arc-index references) into a plain GeoJSON-shaped geometry map.
func resolveTopoGeometry(raw topoRawGeometry, arcs [][][2]float64, transform *topoJSONTransform) (map[string]any, error) {
	switch strings.ToLower(raw.Type) {
	case "point":
		var p [2]float64
		if len(raw.Coordinates) > 0 {
			if err := json.Unmarshal(raw.Coordinates, &p); err != nil {
				return nil, fmt.Errorf("point coordinates: %w", err)
			}
		}
		p = detransformPoint(p, transform)
		return map[string]any{"type": "Point", "coordinates": []float64{p[0], p[1]}}, nil
	case "multipoint":
		var pts [][2]float64
		if len(raw.Coordinates) > 0 {
			if err := json.Unmarshal(raw.Coordinates, &pts); err != nil {
				return nil, fmt.Errorf("multipoint coordinates: %w", err)
			}
		}
		coords := make([][]float64, len(pts))
		for i, p := range pts {
			q := detransformPoint(p, transform)
			coords[i] = []float64{q[0], q[1]}
		}
		return map[string]any{"type": "MultiPoint", "coordinates": coords}, nil
	case "linestring":
		var refs []int
		if len(raw.Arcs) > 0 {
			if err := json.Unmarshal(raw.Arcs, &refs); err != nil {
				return nil, fmt.Errorf("linestring arcs: %w", err)
			}
		}
		pts, err := resolveArcRefs(refs, arcs)
		if err != nil {
			return nil, err
		}
		return map[string]any{"type": "LineString", "coordinates": positionsJSON(pts)}, nil
	case "multilinestring":
		var lineRefs [][]int
		if len(raw.Arcs) > 0 {
			if err := json.Unmarshal(raw.Arcs, &lineRefs); err != nil {
				return nil, fmt.Errorf("multilinestring arcs: %w", err)
			}
		}
		lines := make([]any, len(lineRefs))
		for i, refs := range lineRefs {
			pts, err := resolveArcRefs(refs, arcs)
			if err != nil {
				return nil, fmt.Errorf("line %d: %w", i, err)
			}
			lines[i] = positionsJSON(pts)
		}
		return map[string]any{"type": "MultiLineString", "coordinates": lines}, nil
	case "polygon":
		var ringRefs [][]int
		if len(raw.Arcs) > 0 {
			if err := json.Unmarshal(raw.Arcs, &ringRefs); err != nil {
				return nil, fmt.Errorf("polygon arcs: %w", err)
			}
		}
		rings := make([]any, len(ringRefs))
		for i, refs := range ringRefs {
			pts, err := resolveArcRefs(refs, arcs)
			if err != nil {
				return nil, fmt.Errorf("ring %d: %w", i, err)
			}
			rings[i] = positionsJSON(pts)
		}
		return map[string]any{"type": "Polygon", "coordinates": rings}, nil
	case "multipolygon":
		var polyRefs [][][]int
		if len(raw.Arcs) > 0 {
			if err := json.Unmarshal(raw.Arcs, &polyRefs); err != nil {
				return nil, fmt.Errorf("multipolygon arcs: %w", err)
			}
		}
		polys := make([]any, len(polyRefs))
		for i, ringRefs := range polyRefs {
			rings := make([]any, len(ringRefs))
			for j, refs := range ringRefs {
				pts, err := resolveArcRefs(refs, arcs)
				if err != nil {
					return nil, fmt.Errorf("polygon %d ring %d: %w", i, j, err)
				}
				rings[j] = positionsJSON(pts)
			}
			polys[i] = rings
		}
		return map[string]any{"type": "MultiPolygon", "coordinates": polys}, nil
	case "geometrycollection":
		geoms := make([]any, 0, len(raw.Geometries))
		for i, g := range raw.Geometries {
			resolved, err := resolveTopoGeometry(g, arcs, transform)
			if err != nil {
				return nil, fmt.Errorf("geometry %d: %w", i, err)
			}
			if resolved != nil {
				geoms = append(geoms, resolved)
			}
		}
		return map[string]any{"type": "GeometryCollection", "geometries": geoms}, nil
	case "":
		return nil, nil
	default:
		return nil, fmt.Errorf("unsupported TopoJSON geometry type %q", raw.Type)
	}
}

func topoFeatureMap(geom map[string]any, properties map[string]any, objectKey string, tag bool) map[string]any {
	props := make(map[string]any, len(properties)+1)
	for k, v := range properties {
		props[k] = v
	}
	if tag {
		props["topo_object"] = objectKey
	}
	return map[string]any{"type": "Feature", "properties": props, "geometry": geom}
}

// topoObjectFeatures turns one "objects.<key>" entry into one or more
// Feature-shaped maps. The standard, Power-BI/D3-compatible convention this
// project's own exporter follows is objects.<key> being a GeometryCollection
// whose children each carry their own "properties" -- handled here as one
// Feature per child. A bare (non-collection) geometry value, also spec-legal,
// becomes a single Feature with whatever "properties" it carries itself.
func topoObjectFeatures(raw topoRawGeometry, arcs [][][2]float64, transform *topoJSONTransform, objectKey string, tag bool) ([]map[string]any, error) {
	if strings.EqualFold(raw.Type, "GeometryCollection") {
		features := make([]map[string]any, 0, len(raw.Geometries))
		for i, child := range raw.Geometries {
			geom, err := resolveTopoGeometry(child, arcs, transform)
			if err != nil {
				return nil, fmt.Errorf("geometry %d: %w", i, err)
			}
			features = append(features, topoFeatureMap(geom, child.Properties, objectKey, tag))
		}
		return features, nil
	}
	geom, err := resolveTopoGeometry(raw, arcs, transform)
	if err != nil {
		return nil, err
	}
	return []map[string]any{topoFeatureMap(geom, raw.Properties, objectKey, tag)}, nil
}

// extractTopoJSONFeatures decodes a TopoJSON Topology and returns
// GeoJSON-Feature-shaped maps -- the same shape extractGeoJSONFeatures
// (geojson.go) produces from a real GeoJSON document -- so
// buildGeoJSONColumns/buildGeoJSONRow need no changes to consume either
// source. objectFilter selects a single "objects" key; empty imports every
// object, tagging each resulting feature's properties with "topo_object"
// when (and only when) more than one object key actually exists, so a
// single-object import's column set stays identical to a GeoJSON import's.
func extractTopoJSONFeatures(src io.Reader, objectFilter string) ([]map[string]any, error) {
	br := bufio.NewReader(src)
	var doc topoJSONDocument
	if err := json.NewDecoder(br).Decode(&doc); err != nil {
		return nil, fmt.Errorf("decode topojson: %w", err)
	}
	if !strings.EqualFold(doc.Type, "Topology") {
		return nil, fmt.Errorf("not a Topology document (type=%q)", doc.Type)
	}
	arcs := dequantizeArcs(&doc)

	var keys []string
	if objectFilter != "" {
		if _, ok := doc.Objects[objectFilter]; !ok {
			return nil, fmt.Errorf("object %q not found in topology", objectFilter)
		}
		keys = []string{objectFilter}
	} else {
		for k := range doc.Objects {
			keys = append(keys, k)
		}
		sort.Strings(keys)
	}
	tag := objectFilter == "" && len(keys) > 1

	var features []map[string]any
	for _, key := range keys {
		var raw topoRawGeometry
		if err := json.Unmarshal(doc.Objects[key], &raw); err != nil {
			return nil, fmt.Errorf("object %q: %w", key, err)
		}
		objFeatures, err := topoObjectFeatures(raw, arcs, doc.Transform, key, tag)
		if err != nil {
			return nil, fmt.Errorf("object %q: %w", key, err)
		}
		features = append(features, objFeatures...)
	}
	if len(features) == 0 {
		return nil, fmt.Errorf("no features found in TopoJSON")
	}
	return features, nil
}

// ImportTopoJSON imports a TopoJSON Topology file. Arc references are
// resolved to absolute coordinates before rows are built; see
// extractTopoJSONFeatures. Properties become table columns; geometry is
// stored in a `geometry` column with type GeometryType, identically to
// ImportGeoJSON -- whose buildGeoJSONColumns/buildGeoJSONRow this reuses
// unmodified.
func ImportTopoJSON(
	ctx context.Context,
	db *storage.DB,
	tenant string,
	tableName string,
	src io.Reader,
	opts *ImportOptions,
) (*ImportResult, error) {
	if opts == nil {
		opts = &ImportOptions{}
	}
	applyDefaults(opts)

	features, err := extractTopoJSONFeatures(src, opts.TopoJSONObject)
	if err != nil {
		return nil, err
	}

	propKeys := buildGeoJSONColumns(features)
	colNames := sanitizeColumnNames(append([]string{}, propKeys...))
	colNames = append(colNames, "geometry_type", "geometry")

	sampleData := buildGeoJSONSampleData(features, propKeys)

	var colTypes []storage.ColType
	if opts.TypeInference {
		colTypes = inferColumnTypes(sampleData, len(propKeys), opts)
	} else {
		colTypes = make([]storage.ColType, len(propKeys))
		for i := range colTypes {
			colTypes[i] = storage.TextType
		}
	}
	colTypes = append(colTypes, storage.TextType, storage.GeometryType)

	result := &ImportResult{Encoding: "utf-8", Errors: make([]string, 0), ColumnNames: colNames, ColumnTypes: colTypes}

	if opts.CreateTable {
		if err := createTable(ctx, db, tenant, tableName, colNames, colTypes); err != nil {
			return nil, err
		}
	}
	if opts.Truncate {
		if err := truncateTable(ctx, db, tenant, tableName); err != nil {
			return nil, err
		}
	}

	tbl, err := db.Get(tenant, tableName)
	if err != nil {
		return nil, fmt.Errorf("get table: %w", err)
	}

	for i, f := range features {
		row := buildGeoJSONRow(f, propKeys, colTypes, opts, i, result)
		tbl.Rows = append(tbl.Rows, row)
		result.RowsInserted++
	}

	return result, nil
}
