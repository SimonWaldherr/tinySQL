// ExportTopoJSON: TopoJSON v3 export.
//
// Scope decision (v1): whole-ring shared-arc dedup, WITHOUT partial-arc
// splitting. Two rings (Polygon/MultiPolygon rings, or LineString/
// MultiLineString lines) that trace the identical closed boundary --
// allowing for a different start vertex and/or winding direction -- become
// one shared arc, referenced forward by one and reversed (~i) by the other.
// This is cheap once every ring is being hashed anyway (which building
// "arcs" requires regardless), and it is exactly the case this project's
// own GEO_DISSOLVE will produce (two adjacent regions sharing a full
// boundary). Detecting a PARTIAL match -- two boundaries that coincide for
// only part of their length and diverge elsewhere, which would require
// splitting an arc at the junction point -- is the genuinely hard part of
// real topology-building tools like mapshaper, and is out of scope here;
// every ring that isn't a whole-ring match gets its own arc. The result is
// still 100% spec-valid TopoJSON either way, just less maximally compact
// than a full topology-builder's output would be.
//
// Unlike every other exporter in this package, this cannot stream row by
// row: arc assignment and quantization both need a global pass over every
// coordinate before any arc can be finalized (a ring's arc may turn out to
// be shared with a row not yet seen, and the quantization scale/translate
// need the global bounding box). Memory use is O(total coordinate count),
// not O(one row) -- an unavoidable consequence of the format, not a
// shortcut.
package exporter

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"strings"

	"github.com/SimonWaldherr/tinySQL/internal/engine"
)

// topoQuantizeBits sets the integer quantization resolution applied to
// every coordinate (arcs and Point/MultiPoint alike) via the Topology's
// "transform". Skipping quantization would make TopoJSON output *larger*
// than the equivalent GeoJSON (extra arcs/objects indirection with no
// compensating size win) -- defeating the format's entire point.
const topoQuantizeBits = 16

// topoSnapPrecision snaps coordinates before computing a ring's dedup key,
// so two boundaries that are the same edge but differ by floating-point
// noise (the usual result of round-tripping through different tools) still
// compare exactly equal. Matches the magnitude GEO_DISSOLVE's own default
// snap grid uses (~1cm at the equator) -- the same idea, applied here as a
// comparison tool rather than a returned, mutated geometry.
const topoSnapPrecision = 1e-7

// rawRing is one extracted ring or line, in original (un-snapped,
// un-quantized) coordinates, exactly as the source geometry gave it.
type rawRing struct {
	coords [][2]float64
	closed bool
}

// topoNode is the intermediate, arc-index-agnostic form of one row's
// geometry: ring/line/polygon references are indices into a shared
// []rawRing slice, resolved to actual (possibly negative/reversed) arc
// indices only after every row has been walked and arcs have been
// deduplicated.
type topoNode struct {
	typ       string // GeoJSON type name, or "" for a null geometry
	point     [2]float64
	points    [][2]float64
	lineRings []int   // LineString: len 1; MultiLineString: one per line
	polyRings [][]int // Polygon: len 1 (that polygon's rings); MultiPolygon: one per part
	children  []*topoNode
}

func toFloat(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case json.Number:
		f, err := x.Float64()
		return f, err == nil
	case int:
		return float64(x), true
	default:
		return 0, false
	}
}

func coordPair(v any) ([2]float64, bool) {
	arr, ok := v.([]any)
	if !ok || len(arr) < 2 {
		return [2]float64{}, false
	}
	x, ok1 := toFloat(arr[0])
	y, ok2 := toFloat(arr[1])
	if !ok1 || !ok2 {
		return [2]float64{}, false
	}
	return [2]float64{x, y}, true
}

func coordPairs(v any) ([][2]float64, bool) {
	arr, ok := v.([]any)
	if !ok {
		return nil, false
	}
	out := make([][2]float64, 0, len(arr))
	for _, item := range arr {
		p, ok := coordPair(item)
		if !ok {
			return nil, false
		}
		out = append(out, p)
	}
	return out, true
}

func addRing(rings *[]rawRing, pts [][2]float64, closed bool) int {
	*rings = append(*rings, rawRing{coords: pts, closed: closed})
	return len(*rings) - 1
}

// extractTopoNode walks a decoded GeoJSON geometry object into a topoNode,
// registering every ring/line it finds into rings (shared across the whole
// export, via the pointer). A nil or malformed obj becomes a null-geometry
// node rather than an error -- one bad row's geometry should not abort the
// whole export.
func extractTopoNode(obj map[string]any, rings *[]rawRing) *topoNode {
	if obj == nil {
		return &topoNode{typ: ""}
	}
	typ, _ := obj["type"].(string)
	switch strings.ToLower(typ) {
	case "point":
		p, ok := coordPair(obj["coordinates"])
		if !ok {
			return &topoNode{typ: ""}
		}
		return &topoNode{typ: "Point", point: p}
	case "multipoint":
		pts, ok := coordPairs(obj["coordinates"])
		if !ok {
			return &topoNode{typ: ""}
		}
		return &topoNode{typ: "MultiPoint", points: pts}
	case "linestring":
		pts, ok := coordPairs(obj["coordinates"])
		if !ok || len(pts) < 2 {
			return &topoNode{typ: ""}
		}
		return &topoNode{typ: "LineString", lineRings: []int{addRing(rings, pts, false)}}
	case "multilinestring":
		rawLines, ok := obj["coordinates"].([]any)
		if !ok {
			return &topoNode{typ: ""}
		}
		var refs []int
		for _, rl := range rawLines {
			pts, ok := coordPairs(rl)
			if !ok || len(pts) < 2 {
				continue
			}
			refs = append(refs, addRing(rings, pts, false))
		}
		if len(refs) == 0 {
			return &topoNode{typ: ""}
		}
		return &topoNode{typ: "MultiLineString", lineRings: refs}
	case "polygon":
		refs := polygonRingRefs(obj["coordinates"], rings)
		if len(refs) == 0 {
			return &topoNode{typ: ""}
		}
		return &topoNode{typ: "Polygon", polyRings: [][]int{refs}}
	case "multipolygon":
		rawPolys, ok := obj["coordinates"].([]any)
		if !ok {
			return &topoNode{typ: ""}
		}
		var polys [][]int
		for _, rp := range rawPolys {
			refs := polygonRingRefs(rp, rings)
			if len(refs) > 0 {
				polys = append(polys, refs)
			}
		}
		if len(polys) == 0 {
			return &topoNode{typ: ""}
		}
		return &topoNode{typ: "MultiPolygon", polyRings: polys}
	case "geometrycollection":
		rawGeoms, ok := obj["geometries"].([]any)
		if !ok {
			return &topoNode{typ: ""}
		}
		children := make([]*topoNode, 0, len(rawGeoms))
		for _, rg := range rawGeoms {
			childObj, _ := rg.(map[string]any)
			children = append(children, extractTopoNode(childObj, rings))
		}
		return &topoNode{typ: "GeometryCollection", children: children}
	default:
		return &topoNode{typ: ""}
	}
}

func polygonRingRefs(coordinates any, rings *[]rawRing) []int {
	rawRingsAny, ok := coordinates.([]any)
	if !ok {
		return nil
	}
	var refs []int
	for _, rr := range rawRingsAny {
		pts, ok := coordPairs(rr)
		if !ok || len(pts) < 4 {
			continue
		}
		refs = append(refs, addRing(rings, pts, true))
	}
	return refs
}

func growNodePoints(node *topoNode, grow func([2]float64)) {
	if node == nil {
		return
	}
	switch node.typ {
	case "Point":
		grow(node.point)
	case "MultiPoint":
		for _, p := range node.points {
			grow(p)
		}
	case "GeometryCollection":
		for _, c := range node.children {
			growNodePoints(c, grow)
		}
	}
}

// ── Whole-ring shared-arc dedup ──────────────────────────────────────────

func less2(a, b [2]float64) bool {
	if a[0] != b[0] {
		return a[0] < b[0]
	}
	return a[1] < b[1]
}

func seqLess(a, b [][2]float64) bool {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		if less2(a[i], b[i]) {
			return true
		}
		if less2(b[i], a[i]) {
			return false
		}
	}
	return len(a) < len(b)
}

func seqEqual(a, b [][2]float64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func reverseSeq(pts [][2]float64) [][2]float64 {
	out := make([][2]float64, len(pts))
	for i, p := range pts {
		out[len(pts)-1-i] = p
	}
	return out
}

func snapPoint(p [2]float64) [2]float64 {
	return [2]float64{
		math.Round(p[0]/topoSnapPrecision) * topoSnapPrecision,
		math.Round(p[1]/topoSnapPrecision) * topoSnapPrecision,
	}
}

// normalizedForward snaps pts, then, for a closed ring, drops the closing
// duplicate and rotates to start at the lexicographically smallest vertex
// -- resolving "same ring, different starting vertex" before comparison.
// Open lines (closed=false) have no rotation freedom (their two endpoints
// are fixed), so they are returned snapped but otherwise unchanged.
func normalizedForward(pts [][2]float64, closed bool) [][2]float64 {
	snapped := make([][2]float64, len(pts))
	for i, p := range pts {
		snapped[i] = snapPoint(p)
	}
	if !closed {
		return snapped
	}
	open := snapped
	if len(open) > 1 && open[0] == open[len(open)-1] {
		open = open[:len(open)-1]
	}
	n := len(open)
	if n == 0 {
		return open
	}
	startIdx := 0
	for i := 1; i < n; i++ {
		if less2(open[i], open[startIdx]) {
			startIdx = i
		}
	}
	out := make([][2]float64, n)
	for i := 0; i < n; i++ {
		out[i] = open[(startIdx+i)%n]
	}
	return out
}

func sequenceKey(pts [][2]float64) string {
	var b strings.Builder
	for _, p := range pts {
		fmt.Fprintf(&b, "%.9g,%.9g;", p[0], p[1])
	}
	return b.String()
}

type arcAssignment struct {
	arcIndex int
	reversed bool
}

func arcRefValue(a arcAssignment) int {
	if a.reversed {
		return -a.arcIndex - 1
	}
	return a.arcIndex
}

// topoArcBuilder deduplicates whole rings/lines into a minimal arcs list.
// Rings are assigned in a fixed order (row order, then nesting order within
// each row -- see extractTopoNode) rather than via any map iteration, so
// which ring becomes "the" stored arc for a shared boundary is
// deterministic given the same input, not an accident of Go's randomized
// map ordering.
type topoArcBuilder struct {
	arcs   [][][2]float64
	dirSeq [][][2]float64
	keyIdx map[string]int
}

func newTopoArcBuilder() *topoArcBuilder {
	return &topoArcBuilder{keyIdx: make(map[string]int)}
}

func (b *topoArcBuilder) assign(ring rawRing) arcAssignment {
	fwd := normalizedForward(ring.coords, ring.closed)
	rev := normalizedForward(reverseSeq(ring.coords), ring.closed)
	canonical := fwd
	if seqLess(rev, fwd) {
		canonical = rev
	}
	kind := byte('L')
	if ring.closed {
		kind = 'R'
	}
	key := string(kind) + ":" + sequenceKey(canonical)

	if idx, ok := b.keyIdx[key]; ok {
		if seqEqual(fwd, b.dirSeq[idx]) {
			return arcAssignment{arcIndex: idx, reversed: false}
		}
		return arcAssignment{arcIndex: idx, reversed: true}
	}
	idx := len(b.arcs)
	b.arcs = append(b.arcs, ring.coords)
	b.dirSeq = append(b.dirSeq, fwd)
	b.keyIdx[key] = idx
	return arcAssignment{arcIndex: idx, reversed: false}
}

func assignAllRings(rings []rawRing, builder *topoArcBuilder) []arcAssignment {
	out := make([]arcAssignment, len(rings))
	for i, r := range rings {
		out[i] = builder.assign(r)
	}
	return out
}

// ── Geometry JSON assembly ────────────────────────────────────────────────

func lineArcsJSON(ringIdxs []int, assignments []arcAssignment) [][]int {
	out := make([][]int, len(ringIdxs))
	for i, ri := range ringIdxs {
		out[i] = []int{arcRefValue(assignments[ri])}
	}
	return out
}

func polyArcsJSON(polyRingIdxs [][]int, assignments []arcAssignment) [][][]int {
	out := make([][][]int, len(polyRingIdxs))
	for p, ringIdxs := range polyRingIdxs {
		rings := make([][]int, len(ringIdxs))
		for r, ri := range ringIdxs {
			rings[r] = []int{arcRefValue(assignments[ri])}
		}
		out[p] = rings
	}
	return out
}

func topoNodeToJSON(node *topoNode, assignments []arcAssignment, quantize func([2]float64) [2]int) map[string]any {
	if node == nil || node.typ == "" {
		return map[string]any{"type": nil}
	}
	switch node.typ {
	case "Point":
		q := quantize(node.point)
		return map[string]any{"type": "Point", "coordinates": []int{q[0], q[1]}}
	case "MultiPoint":
		coords := make([][]int, len(node.points))
		for i, p := range node.points {
			q := quantize(p)
			coords[i] = []int{q[0], q[1]}
		}
		return map[string]any{"type": "MultiPoint", "coordinates": coords}
	case "LineString":
		return map[string]any{"type": "LineString", "arcs": []int{arcRefValue(assignments[node.lineRings[0]])}}
	case "MultiLineString":
		return map[string]any{"type": "MultiLineString", "arcs": lineArcsJSON(node.lineRings, assignments)}
	case "Polygon":
		return map[string]any{"type": "Polygon", "arcs": polyArcsJSON(node.polyRings, assignments)[0]}
	case "MultiPolygon":
		return map[string]any{"type": "MultiPolygon", "arcs": polyArcsJSON(node.polyRings, assignments)}
	case "GeometryCollection":
		children := make([]map[string]any, len(node.children))
		for i, c := range node.children {
			children[i] = topoNodeToJSON(c, assignments, quantize)
		}
		return map[string]any{"type": "GeometryCollection", "geometries": children}
	default:
		return map[string]any{"type": nil}
	}
}

// ExportTopoJSON writes ResultSet rows as a TopoJSON v3 Topology. geomCol
// and auto-detect behave identically to ExportGeoJSON. objectName becomes
// the single key of the "objects" map (default "collection" if empty, or
// if a caller-supplied name is empty). Every row becomes one child geometry
// inside that object's GeometryCollection, carrying a "properties" member
// -- the standard, Power-BI/D3-compatible convention for attaching
// per-feature attributes in TopoJSON (there is no first-class per-feature
// "properties" the way GeoJSON has; a GeometryCollection whose children
// each carry one is how every real TopoJSON consumer expects it).
func ExportTopoJSON(w io.Writer, rs *engine.ResultSet, geomCol, objectName string, opts Options) error {
	geomCol, err := resolveGeometryColumn(rs, geomCol)
	if err != nil {
		return fmt.Errorf("ExportTopoJSON: %w", err)
	}
	if objectName == "" {
		objectName = "collection"
	}
	lowerGeomCol := strings.ToLower(geomCol)

	var rings []rawRing
	nodes := make([]*topoNode, len(rs.Rows))
	for i, r := range rs.Rows {
		raw := geometryCellBytes(r[lowerGeomCol])
		var obj map[string]any
		if raw != nil {
			_ = json.Unmarshal(raw, &obj) // obj stays nil on failure -> null geometry, non-aborting
		}
		nodes[i] = extractTopoNode(obj, &rings)
	}

	builder := newTopoArcBuilder()
	assignments := assignAllRings(rings, builder)

	minX, minY := math.Inf(1), math.Inf(1)
	maxX, maxY := math.Inf(-1), math.Inf(-1)
	grow := func(p [2]float64) {
		if p[0] < minX {
			minX = p[0]
		}
		if p[0] > maxX {
			maxX = p[0]
		}
		if p[1] < minY {
			minY = p[1]
		}
		if p[1] > maxY {
			maxY = p[1]
		}
	}
	for _, arc := range builder.arcs {
		for _, p := range arc {
			grow(p)
		}
	}
	for _, node := range nodes {
		growNodePoints(node, grow)
	}
	if math.IsInf(minX, 1) {
		minX, minY, maxX, maxY = 0, 0, 0, 0
	}
	scaleX, scaleY := 1.0, 1.0
	if maxX > minX {
		scaleX = (maxX - minX) / float64((1<<topoQuantizeBits)-1)
	}
	if maxY > minY {
		scaleY = (maxY - minY) / float64((1<<topoQuantizeBits)-1)
	}
	quantize := func(p [2]float64) [2]int {
		return [2]int{
			int(math.Round((p[0] - minX) / scaleX)),
			int(math.Round((p[1] - minY) / scaleY)),
		}
	}

	arcsJSON := make([][][2]int, len(builder.arcs))
	for i, arc := range builder.arcs {
		encoded := make([][2]int, len(arc))
		prevX, prevY := 0, 0
		for j, p := range arc {
			q := quantize(p)
			if j == 0 {
				encoded[j] = q
			} else {
				encoded[j] = [2]int{q[0] - prevX, q[1] - prevY}
			}
			prevX, prevY = q[0], q[1]
		}
		arcsJSON[i] = encoded
	}

	geometries := make([]map[string]any, len(nodes))
	for i, node := range nodes {
		g := topoNodeToJSON(node, assignments, quantize)
		props := make(map[string]any, len(rs.Cols))
		for _, c := range rs.Cols {
			lc := strings.ToLower(c)
			if lc == lowerGeomCol || lc == "geometry_type" {
				continue
			}
			props[c] = jsonValue(rs.Rows[i][lc], opts)
		}
		g["properties"] = props
		geometries[i] = g
	}

	topology := map[string]any{
		"type": "Topology",
		"objects": map[string]any{
			objectName: map[string]any{
				"type":       "GeometryCollection",
				"geometries": geometries,
			},
		},
		"arcs": arcsJSON,
		"transform": map[string]any{
			"scale":     []float64{scaleX, scaleY},
			"translate": []float64{minX, minY},
		},
	}

	var b []byte
	if opts.PrettyJSON {
		b, err = json.MarshalIndent(topology, "", "  ")
	} else {
		b, err = json.Marshal(topology)
	}
	if err != nil {
		return err
	}
	b = append(b, '\n')
	_, err = w.Write(b)
	return err
}
