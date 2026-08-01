package main

// Tile serving: an XYZ endpoint over a tileset held in tinySQL.
//
//	GET /tiles/{tileset}/{z}/{x}/{y}.{ext}   one tile
//	GET /tiles/{tileset}.json                TileJSON 3.0.0
//	GET /tiles/{tileset}/metadata            the raw MBTiles metadata rows
//
// The URL uses XYZ, because that is what every map client speaks, while the
// MBTiles specification stores TMS rows. The flip happens here, once, in
// tileRowForRequest — serving a tileset without it produces a map that is
// vertically mirrored and looks almost right, which is the single most common
// MBTiles mistake.
//
// Tiles are public by design: a tile endpoint is consumed by a browser that
// cannot send an Authorization header on image requests. It is therefore
// registered outside requireAuth, and gated by an explicit -tiles flag so no
// deployment starts serving data unasked.

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	tinysql "github.com/SimonWaldherr/tinySQL"
)

// tileFormat describes how one tile payload should be sent.
type tileFormat struct {
	contentType     string
	contentEncoding string // non-empty when the stored bytes are compressed
}

// tileFormats maps an MBTiles `format` metadata value to response headers.
//
// pbf is the case that bites: Mapbox Vector Tiles are stored gzip-compressed
// inside MBTiles, so the response has to declare Content-Encoding: gzip or the
// client receives bytes it cannot parse.
var tileFormats = map[string]tileFormat{
	"png":  {contentType: "image/png"},
	"jpg":  {contentType: "image/jpeg"},
	"jpeg": {contentType: "image/jpeg"},
	"webp": {contentType: "image/webp"},
	"avif": {contentType: "image/avif"},
	"pbf":  {contentType: "application/x-protobuf", contentEncoding: "gzip"},
	"mvt":  {contentType: "application/x-protobuf", contentEncoding: "gzip"},
	"json": {contentType: "application/json"},
}

func tileFormatFor(format string) tileFormat {
	if f, ok := tileFormats[strings.ToLower(strings.TrimSpace(format))]; ok {
		return f
	}
	// Unknown formats are sent as opaque bytes rather than guessed at.
	return tileFormat{contentType: "application/octet-stream"}
}

// tilesetName validates the tileset path segment before it reaches SQL.
//
// The name becomes a table identifier, so it is restricted to characters that
// cannot change the meaning of the statement it is interpolated into. Tile
// coordinates are bound as parameters, but a table name cannot be.
func tilesetName(s string) (string, error) {
	if s == "" {
		return "", fmt.Errorf("empty tileset name")
	}
	if len(s) > 64 {
		return "", fmt.Errorf("tileset name too long")
	}
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '_':
		default:
			return "", fmt.Errorf("tileset name may contain only letters, digits and underscore")
		}
	}
	return s, nil
}

// tileRowForRequest converts the XYZ row in a tile URL to the TMS row stored in
// an MBTiles tiles table.
func tileRowForRequest(z, y int) int { return (1 << uint(z)) - 1 - y }

// parseTilePath splits "{tileset}/{z}/{x}/{y}.{ext}" into its parts.
func parseTilePath(rest string) (name string, z, x, y int, ext string, err error) {
	parts := strings.Split(strings.Trim(rest, "/"), "/")
	if len(parts) != 4 {
		return "", 0, 0, 0, "", fmt.Errorf("expected /tiles/{tileset}/{z}/{x}/{y}.{ext}")
	}
	if name, err = tilesetName(parts[0]); err != nil {
		return "", 0, 0, 0, "", err
	}
	if z, err = strconv.Atoi(parts[1]); err != nil {
		return "", 0, 0, 0, "", fmt.Errorf("zoom %q is not a number", parts[1])
	}
	if x, err = strconv.Atoi(parts[2]); err != nil {
		return "", 0, 0, 0, "", fmt.Errorf("x %q is not a number", parts[2])
	}
	yPart := parts[3]
	if dot := strings.LastIndexByte(yPart, '.'); dot >= 0 {
		ext = yPart[dot+1:]
		yPart = yPart[:dot]
	}
	if y, err = strconv.Atoi(yPart); err != nil {
		return "", 0, 0, 0, "", fmt.Errorf("y %q is not a number", parts[3])
	}
	if z < 0 || z > 30 {
		return "", 0, 0, 0, "", fmt.Errorf("zoom %d out of range 0..30", z)
	}
	limit := 1 << uint(z)
	if x < 0 || x >= limit || y < 0 || y >= limit {
		return "", 0, 0, 0, "", fmt.Errorf("tile %d/%d/%d is outside the %d-tile grid at that zoom", z, x, y, limit)
	}
	return name, z, x, y, ext, nil
}

// handleTiles dispatches the tile routes.
func (d *daemon) handleTiles(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		writeErrorJSON(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	rest := strings.TrimPrefix(r.URL.Path, "/tiles/")

	switch {
	case strings.HasSuffix(rest, ".json") && !strings.Contains(strings.Trim(rest, "/"), "/"):
		d.handleTileJSON(w, r, strings.TrimSuffix(rest, ".json"))
	case strings.HasSuffix(rest, "/metadata"):
		d.handleTileMetadata(w, r, strings.TrimSuffix(rest, "/metadata"))
	default:
		d.handleTile(w, r)
	}
}

// tileQueryContext applies the daemon's request timeout to a tile lookup.
func (d *daemon) tileQueryContext(r *http.Request) (context.Context, context.CancelFunc) {
	ctx, cancel, err := d.requestContext(r.Context(), 0)
	if err != nil {
		return r.Context(), func() {}
	}
	return ctx, cancel
}

// tileScalar renders an already-validated integer for interpolation into SQL.
//
// tinySQL's public Execute takes no bind parameters, so tile coordinates are
// formatted into the statement. That is safe here only because every value has
// been parsed by strconv.Atoi and range-checked against the zoom's grid before
// reaching this point, and because the tileset name went through tilesetName —
// neither can carry SQL syntax.
func tileScalar(n int) string { return strconv.Itoa(n) }

// tilesetMetadata reads a tileset's metadata rows into a map. A missing metadata
// table is not an error: a tiles table alone is still servable, just without a
// declared format.
func (d *daemon) tilesetMetadata(ctx context.Context, tenant, name string) map[string]string {
	rs, err := d.executeSQL(ctx, tenant, fmt.Sprintf("SELECT name, value FROM %s_metadata", name))
	if err != nil || rs == nil {
		return nil
	}
	out := make(map[string]string, len(rs.Rows))
	for _, row := range rs.Rows {
		rawKey, ok := tinysql.GetVal(row, "name")
		if !ok || rawKey == nil {
			continue
		}
		k := fmt.Sprintf("%v", rawKey)
		v, ok := tinysql.GetVal(row, "value")
		if !ok || v == nil {
			out[k] = ""
			continue
		}
		out[k] = fmt.Sprintf("%v", v)
	}
	return out
}

func (d *daemon) handleTile(w http.ResponseWriter, r *http.Request) {
	name, z, x, y, _, err := parseTilePath(strings.TrimPrefix(r.URL.Path, "/tiles/"))
	if err != nil {
		writeErrorJSON(w, http.StatusBadRequest, err.Error())
		return
	}
	tenant := d.tenantOrDefault(r.URL.Query().Get("tenant"))
	ctx, cancel := d.tileQueryContext(r)
	defer cancel()

	rs, err := d.executeSQL(ctx, tenant, fmt.Sprintf(
		"SELECT tile_data FROM %s WHERE zoom_level = %s AND tile_column = %s AND tile_row = %s LIMIT 1",
		name, tileScalar(z), tileScalar(x), tileScalar(tileRowForRequest(z, y))))
	if err != nil {
		// A missing tileset is a client error (wrong URL), not a server fault.
		writeErrorJSON(w, http.StatusNotFound, err.Error())
		return
	}
	if rs == nil || len(rs.Rows) == 0 {
		// An absent tile is normal for a sparse tileset: the client asked for an
		// area the tileset does not cover.
		http.NotFound(w, r)
		return
	}

	raw, _ := tinysql.GetVal(rs.Rows[0], "tile_data")
	data, ok := tileBytes(raw)
	if !ok {
		writeErrorJSON(w, http.StatusInternalServerError, "tile_data is not a blob")
		return
	}

	format := tileFormatFor(d.tilesetMetadata(ctx, tenant, name)["format"])
	w.Header().Set("Content-Type", format.contentType)
	if format.contentEncoding != "" {
		w.Header().Set("Content-Encoding", format.contentEncoding)
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	// Tilesets are immutable in practice; a rebuild publishes a new tileset.
	w.Header().Set("Cache-Control", "public, max-age=3600")
	if r.Method == http.MethodHead {
		w.WriteHeader(http.StatusOK)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

// tileBytes accepts the representations a tile payload can arrive in.
func tileBytes(v any) ([]byte, bool) {
	switch b := v.(type) {
	case []byte:
		return b, true
	case string:
		return []byte(b), true
	case nil:
		return nil, false
	default:
		return nil, false
	}
}

// handleTileJSON serves TileJSON 3.0.0, which is what a map client fetches to
// discover a tileset's URL template, zoom range and bounds.
func (d *daemon) handleTileJSON(w http.ResponseWriter, r *http.Request, rawName string) {
	name, err := tilesetName(strings.Trim(rawName, "/"))
	if err != nil {
		writeErrorJSON(w, http.StatusBadRequest, err.Error())
		return
	}
	tenant := d.tenantOrDefault(r.URL.Query().Get("tenant"))
	ctx, cancel := d.tileQueryContext(r)
	defer cancel()

	meta := d.tilesetMetadata(ctx, tenant, name)
	format := strings.ToLower(strings.TrimSpace(meta["format"]))
	if format == "" {
		format = "png"
	}

	scheme := "http"
	if r.TLS != nil || strings.EqualFold(r.Header.Get("X-Forwarded-Proto"), "https") {
		scheme = "https"
	}
	tileURL := fmt.Sprintf("%s://%s/tiles/%s/{z}/{x}/{y}.%s", scheme, r.Host, name, format)

	out := map[string]any{
		"tilejson": "3.0.0",
		"name":     firstNonEmpty(meta["name"], name),
		"scheme":   "xyz",
		"tiles":    []string{tileURL},
		"format":   format,
	}
	for _, key := range []string{"description", "attribution", "version", "type"} {
		if v := meta[key]; v != "" {
			out[key] = v
		}
	}
	// minzoom/maxzoom come from metadata when present, otherwise from the tiles
	// themselves so a client still gets a usable range.
	minZoom, maxZoom, haveRange := tileZoomRange(meta)
	if !haveRange {
		minZoom, maxZoom, haveRange = d.observedZoomRange(ctx, tenant, name)
	}
	if haveRange {
		out["minzoom"] = minZoom
		out["maxzoom"] = maxZoom
	}
	if bounds, ok := parseFloatList(meta["bounds"], 4); ok {
		out["bounds"] = bounds
	}
	if center, ok := parseFloatList(meta["center"], 3); ok {
		out["center"] = center
	}
	writeJSON(w, http.StatusOK, out)
}

func (d *daemon) handleTileMetadata(w http.ResponseWriter, r *http.Request, rawName string) {
	name, err := tilesetName(strings.Trim(rawName, "/"))
	if err != nil {
		writeErrorJSON(w, http.StatusBadRequest, err.Error())
		return
	}
	tenant := d.tenantOrDefault(r.URL.Query().Get("tenant"))
	ctx, cancel := d.tileQueryContext(r)
	defer cancel()
	meta := d.tilesetMetadata(ctx, tenant, name)
	if meta == nil {
		writeErrorJSON(w, http.StatusNotFound, "no metadata table for tileset "+name)
		return
	}
	writeJSON(w, http.StatusOK, meta)
}

// observedZoomRange derives a zoom range from the tiles table when metadata does
// not declare one.
func (d *daemon) observedZoomRange(ctx context.Context, tenant, name string) (int, int, bool) {
	rs, err := d.executeSQL(ctx, tenant, fmt.Sprintf(
		"SELECT MIN(zoom_level) AS lo, MAX(zoom_level) AS hi FROM %s", name))
	if err != nil || rs == nil || len(rs.Rows) == 0 {
		return 0, 0, false
	}
	rawLo, _ := tinysql.GetVal(rs.Rows[0], "lo")
	rawHi, _ := tinysql.GetVal(rs.Rows[0], "hi")
	lo, okLo := tileInt(rawLo)
	hi, okHi := tileInt(rawHi)
	if !okLo || !okHi {
		return 0, 0, false
	}
	return lo, hi, true
}

func tileZoomRange(meta map[string]string) (int, int, bool) {
	lo, errLo := strconv.Atoi(strings.TrimSpace(meta["minzoom"]))
	hi, errHi := strconv.Atoi(strings.TrimSpace(meta["maxzoom"]))
	if errLo != nil || errHi != nil {
		return 0, 0, false
	}
	return lo, hi, true
}

func tileInt(v any) (int, bool) {
	switch n := v.(type) {
	case int:
		return n, true
	case int32:
		return int(n), true
	case int64:
		return int(n), true
	case float64:
		return int(n), true
	default:
		return 0, false
	}
}

// parseFloatList parses an MBTiles comma-separated numeric metadata value, such
// as bounds ("west,south,east,north") or center ("lon,lat,zoom").
func parseFloatList(s string, want int) ([]float64, bool) {
	if strings.TrimSpace(s) == "" {
		return nil, false
	}
	parts := strings.Split(s, ",")
	if len(parts) != want {
		return nil, false
	}
	out := make([]float64, 0, want)
	for _, p := range parts {
		f, err := strconv.ParseFloat(strings.TrimSpace(p), 64)
		if err != nil {
			return nil, false
		}
		out = append(out, f)
	}
	return out, true
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}
