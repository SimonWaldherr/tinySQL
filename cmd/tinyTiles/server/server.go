// Package server exposes a mountable HTTP surface for a tinyTiles Dataset.
// It deliberately owns no listener, TLS configuration, authentication, or
// deployment policy. Use Handler with an existing application mux, or use the
// cmd/tinytiles-server binary for the small standalone application.
package server

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	tinytiles "github.com/Karte-Bayern/tinyTiles"
	"github.com/Karte-Bayern/tinyTiles/offline"
	tiles "github.com/SimonWaldherr/tinySQL/tiles"
)

// Config controls the HTTP representation of a Dataset. Dataset remains owned
// by the caller and must be closed after its HTTP server has shut down.
type Config struct {
	// TileCacheBytes bounds cached tile payloads and checksums. Zero disables
	// this shared HTTP cache; reader page-cache budgets are separate.
	TileCacheBytes int64
	Dataset        *tinytiles.Dataset

	// DatasetID is the stable name used by the offline synchronization
	// manifest. It is not derived from a filesystem path.
	DatasetID string
	// PublicBase is an optional canonical http(s) origin or base path used in
	// TileJSON and sync manifests. Without it, the server derives an origin
	// from the incoming request; deployments behind TLS proxies should set it.
	PublicBase string
	// CORSOrigin is empty for no CORS header, * for public access, or one
	// http(s) origin.
	CORSOrigin string
	// ContentType and ContentEncoding override MBTiles metadata. Empty values
	// infer pbf/gzip from format and kb:content_encoding respectively.
	ContentType     string
	ContentEncoding string
}

// Server is a handler factory for one immutable dataset revision. It is safe
// for concurrent requests as long as its Dataset remains open.
type Server struct {
	tileCache       *tileCache
	dataset         *tinytiles.Dataset
	datasetID       string
	revision        string
	createdAt       time.Time
	metadata        map[string]string
	contentType     string
	contentEncoding string
	publicBase      string
	corsOrigin      string
}

// New validates the static server configuration. It does not open or close
// files: Dataset.Open has already fail-closed validated the artifact.
func New(config Config) (*Server, error) {
	if config.TileCacheBytes < 0 {
		return nil, errors.New("tinytiles server: tile cache budget must be non-negative")
	}
	if config.Dataset == nil {
		return nil, errors.New("tinytiles server: dataset is required")
	}
	if err := validateDatasetID(config.DatasetID); err != nil {
		return nil, err
	}
	publicBase, err := normalizePublicBase(config.PublicBase)
	if err != nil {
		return nil, err
	}
	corsOrigin, err := normalizeCORSOrigin(config.CORSOrigin)
	if err != nil {
		return nil, err
	}
	info := config.Dataset.Info()
	if len(info.TileDigestSHA256) != sha256.Size*2 {
		return nil, errors.New("tinytiles server: artifact has no usable tile digest revision")
	}
	if _, err := hex.DecodeString(info.TileDigestSHA256); err != nil {
		return nil, fmt.Errorf("tinytiles server: invalid tile digest revision: %w", err)
	}
	metadata, err := config.Dataset.Metadata()
	if err != nil {
		return nil, fmt.Errorf("tinytiles server: read metadata: %w", err)
	}
	contentType := strings.TrimSpace(config.ContentType)
	if contentType == "" {
		contentType = "application/octet-stream"
		if strings.EqualFold(metadata["format"], "pbf") {
			contentType = "application/vnd.mapbox-vector-tile"
		}
	}
	contentEncoding := strings.TrimSpace(config.ContentEncoding)
	if contentEncoding == "" && strings.EqualFold(metadata["kb:content_encoding"], "gzip") {
		contentEncoding = "gzip"
	}
	return &Server{
		tileCache:       newTileCache(config.TileCacheBytes),
		dataset:         config.Dataset,
		datasetID:       config.DatasetID,
		revision:        info.TileDigestSHA256,
		createdAt:       info.CreatedAt,
		metadata:        metadata,
		contentType:     contentType,
		contentEncoding: contentEncoding,
		publicBase:      publicBase,
		corsOrigin:      corsOrigin,
	}, nil
}

// Handler exposes the complete standalone route set:
//
//   - /tiles/{z}/{x}/{y}[.mvt] is an XYZ slippy-map endpoint;
//   - /tilejson.json and /metadata expose standard consumer metadata;
//   - /sync/manifest.json and /sync/tiles/{revision}/{z}/{x}/{y} provide the
//     revisioned TMS cache protocol for native and WASM clients;
//   - /healthz returns 204 when this Server is configured.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/tiles/", http.StripPrefix("/tiles/", s.XYZHandler()))
	mux.Handle("/sync/", http.StripPrefix("/sync", s.SyncHandler()))
	mux.HandleFunc("/tilejson.json", s.serveTileJSON)
	mux.HandleFunc("/metadata", s.serveMetadata)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	return s.withCORS(mux)
}

// XYZHandler serves bare z/x/y[.mvt] paths. This makes it safe to mount in an
// existing service with http.StripPrefix("/tiles/", handler), which is the
// integration shape used by Karte.Bayern-style application servers.
func (s *Server) XYZHandler() http.Handler {
	return http.HandlerFunc(s.serveXYZ)
}

// SyncHandler serves /manifest.json and /tiles/{revision}/{z}/{x}/{y}. It is
// mountable below a caller-selected path with http.StripPrefix.
func (s *Server) SyncHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/manifest.json", s.serveManifest)
	mux.HandleFunc("/tiles/", s.serveTMSSync)
	return s.withCORS(mux)
}

func (s *Server) serveXYZ(w http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet && request.Method != http.MethodHead {
		methodNotAllowed(w, http.MethodGet, http.MethodHead)
		return
	}
	z, x, y, err := parseXYZPath(request.URL.Path)
	if err != nil {
		http.Error(w, "invalid XYZ tile coordinate", http.StatusBadRequest)
		return
	}
	// Only revisioned URLs can be cached immutably across deployments.
	// Bare XYZ URLs must revalidate when a new artifact replaces the server.
	cacheControl := "public, max-age=0, must-revalidate"
	if revision := request.URL.Query().Get("tinytiles_rev"); revision != "" {
		if revision != s.revision {
			http.NotFound(w, request)
			return
		}
		cacheControl = "public, max-age=31536000, immutable"
	}
	etag := quoteETag(s.revision + ":xyz:" + strconv.Itoa(z) + "/" + strconv.Itoa(x) + "/" + strconv.Itoa(y))
	if writeConditionalHeaders(w, request, etag, cacheControl) {
		return
	}
	yTMS, _ := tinytiles.XYZToTMSY(z, y) // parseXYZPath already validated coordinates
	tile, found, err := s.lookupTile(request.Context(), tiles.Key{Z: z, X: x, Y: yTMS})
	if err != nil {
		http.Error(w, "tile lookup failed", http.StatusInternalServerError)
		return
	}
	if !found {
		w.Header().Set("Cache-Control", "public, max-age=60")
		w.WriteHeader(http.StatusNoContent)
		return
	}
	s.writeTileHeaders(w, tile, true)
	if request.Method == http.MethodHead {
		return
	}
	_, _ = w.Write(tile.data)
}

func (s *Server) serveManifest(w http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet && request.Method != http.MethodHead {
		methodNotAllowed(w, http.MethodGet, http.MethodHead)
		return
	}
	baseURL, err := s.baseURL(request)
	if err != nil {
		http.Error(w, "invalid public base URL", http.StatusBadRequest)
		return
	}
	manifest := offline.Manifest{
		FormatVersion:    offline.ProtocolVersion,
		Dataset:          s.datasetID,
		Revision:         s.revision,
		CoordinateSystem: "TMS",
		TileURLTemplate:  baseURL + "/sync/tiles/{revision}/{z}/{x}/{y}",
		ContentType:      s.contentType,
		ContentEncoding:  s.contentEncoding,
		CreatedAt:        s.createdAt,
	}
	payload, err := json.Marshal(manifest)
	if err != nil {
		http.Error(w, "encode sync manifest", http.StatusInternalServerError)
		return
	}
	writeJSON(w, request, payload, quoteETag(s.revision), "no-cache")
}

func (s *Server) serveTMSSync(w http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet && request.Method != http.MethodHead {
		methodNotAllowed(w, http.MethodGet, http.MethodHead)
		return
	}
	parts := strings.Split(strings.TrimPrefix(path.Clean(request.URL.Path), "/tiles/"), "/")
	if len(parts) != 4 || parts[0] != s.revision {
		http.NotFound(w, request)
		return
	}
	key, err := parseTMSParts(parts[1:])
	if err != nil {
		http.Error(w, "invalid TMS tile coordinate", http.StatusBadRequest)
		return
	}
	etag := quoteETag(s.revision + ":tms:" + key.String())
	if writeConditionalHeaders(w, request, etag, "public, max-age=31536000, immutable") {
		return
	}
	tile, found, err := s.lookupTile(request.Context(), key)
	if err != nil {
		http.Error(w, "tile lookup failed", http.StatusInternalServerError)
		return
	}
	if !found {
		http.NotFound(w, request)
		return
	}
	// Do not set HTTP Content-Encoding here. Browser Fetch may transparently
	// decode it, whereas the offline protocol stores and checksums exact raw
	// bytes. The protocol-specific header retains the representation instead.
	s.writeTileHeaders(w, tile, false)
	if request.Method == http.MethodHead {
		return
	}
	_, _ = w.Write(tile.data)
}

func (s *Server) serveTileJSON(w http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet && request.Method != http.MethodHead {
		methodNotAllowed(w, http.MethodGet, http.MethodHead)
		return
	}
	baseURL, err := s.baseURL(request)
	if err != nil {
		http.Error(w, "invalid public base URL", http.StatusBadRequest)
		return
	}
	metadata := cloneMetadata(s.metadata)
	tileURL := baseURL + "/tiles/{z}/{x}/{y}.mvt?tinytiles_rev=" + url.QueryEscape(s.revision)
	payload, err := json.Marshal(tileJSON(metadata, tileURL, s.revision))
	if err != nil {
		http.Error(w, "encode TileJSON", http.StatusInternalServerError)
		return
	}
	writeJSON(w, request, payload, quoteETag(digest(payload)), "public, max-age=300, stale-while-revalidate=60")
}

func (s *Server) serveMetadata(w http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet && request.Method != http.MethodHead {
		methodNotAllowed(w, http.MethodGet, http.MethodHead)
		return
	}
	payload, err := json.Marshal(cloneMetadata(s.metadata))
	if err != nil {
		http.Error(w, "encode metadata", http.StatusInternalServerError)
		return
	}
	writeJSON(w, request, payload, quoteETag(digest(payload)), "public, max-age=300, stale-while-revalidate=60")
}

func (s *Server) writeTileHeaders(w http.ResponseWriter, tile tilePayload, browserTile bool) {
	w.Header().Set("Content-Type", s.contentType)
	if s.contentEncoding != "" {
		w.Header().Set(offline.HeaderTileContentEncoding, s.contentEncoding)
		if browserTile {
			w.Header().Set("Content-Encoding", s.contentEncoding)
		}
	}
	w.Header().Set(offline.HeaderTileChecksum, tile.checksum)
	w.Header().Set("Content-Length", strconv.Itoa(len(tile.data)))
}

func (s *Server) withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if s.corsOrigin != "" {
			w.Header().Set("Access-Control-Allow-Origin", s.corsOrigin)
			w.Header().Set("Access-Control-Allow-Methods", "GET, HEAD, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, If-None-Match")
			w.Header().Set("Access-Control-Expose-Headers", offline.HeaderTileChecksum+", "+offline.HeaderTileContentEncoding+", ETag")
		}
		if request.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, request)
	})
}

func (s *Server) baseURL(request *http.Request) (string, error) {
	if s.publicBase != "" {
		return s.publicBase, nil
	}
	scheme := "http"
	if request != nil && request.TLS != nil {
		scheme = "https"
	}
	host := ""
	if request != nil {
		host = request.Host
	}
	candidate := scheme + "://" + host
	parsed, err := url.Parse(candidate)
	if err != nil || parsed.Host == "" || parsed.Host != host || parsed.Path != "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", errors.New("request host is not a valid origin")
	}
	return candidate, nil
}

func validateDatasetID(dataset string) error {
	manifest := offline.Manifest{FormatVersion: offline.ProtocolVersion, Dataset: dataset, Revision: strings.Repeat("0", sha256.Size*2), CoordinateSystem: "TMS", TileURLTemplate: "https://example.invalid/{z}/{x}/{y}"}
	return manifest.Validate()
}

func normalizePublicBase(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", nil
	}
	parsed, err := url.Parse(value)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", errors.New("tinytiles server: public base must be an absolute http(s) URL without credentials, query or fragment")
	}
	return strings.TrimRight(parsed.String(), "/"), nil
}

func normalizeCORSOrigin(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" || value == "*" {
		return value, nil
	}
	parsed, err := url.Parse(value)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || (parsed.Path != "" && parsed.Path != "/") {
		return "", errors.New("tinytiles server: cors origin must be * or an http(s) origin without path, credentials, query or fragment")
	}
	return strings.TrimRight(parsed.String(), "/"), nil
}

func parseXYZPath(raw string) (int, int, int, error) {
	parts := strings.Split(strings.TrimPrefix(path.Clean("/"+raw), "/"), "/")
	if len(parts) != 3 {
		return 0, 0, 0, errors.New("need z/x/y")
	}
	if strings.HasSuffix(parts[2], ".mvt") {
		parts[2] = strings.TrimSuffix(parts[2], ".mvt")
	}
	return parseCoordinates(parts)
}

func parseTMSParts(parts []string) (tiles.Key, error) {
	z, x, y, err := parseCoordinates(parts)
	if err != nil {
		return tiles.Key{}, err
	}
	return tiles.Key{Z: z, X: x, Y: y}, nil
}

func parseCoordinates(parts []string) (int, int, int, error) {
	if len(parts) != 3 {
		return 0, 0, 0, errors.New("need z/x/y")
	}
	values := [3]int{}
	for index, part := range parts {
		value, err := strconv.Atoi(part)
		if err != nil {
			return 0, 0, 0, err
		}
		values[index] = value
	}
	if err := (tiles.Key{Z: values[0], X: values[1], Y: values[2]}).Validate(); err != nil {
		return 0, 0, 0, err
	}
	return values[0], values[1], values[2], nil
}

func writeConditionalHeaders(w http.ResponseWriter, request *http.Request, etag, cacheControl string) bool {
	w.Header().Set("ETag", etag)
	w.Header().Set("Cache-Control", cacheControl)
	if request.Header.Get("If-None-Match") == etag {
		w.WriteHeader(http.StatusNotModified)
		return true
	}
	return false
}

func writeJSON(w http.ResponseWriter, request *http.Request, payload []byte, etag, cacheControl string) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Content-Length", strconv.Itoa(len(payload)))
	if writeConditionalHeaders(w, request, etag, cacheControl) || request.Method == http.MethodHead {
		return
	}
	_, _ = w.Write(payload)
}

func methodNotAllowed(w http.ResponseWriter, methods ...string) {
	w.Header().Set("Allow", strings.Join(methods, ", "))
	http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
}

func quoteETag(value string) string { return `"` + value + `"` }

func digest(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func cloneMetadata(source map[string]string) map[string]string {
	copy := make(map[string]string, len(source))
	for name, value := range source {
		copy[name] = value
	}
	return copy
}

func tileJSON(metadata map[string]string, tileURL, revision string) map[string]any {
	minZoom := integerMetadata(metadata, "minzoom", 0)
	maxZoom := integerMetadata(metadata, "maxzoom", 22)
	attribution := strings.TrimSpace(metadata["attribution"])
	if attribution == "" {
		attribution = "© OpenStreetMap contributors"
	}
	return map[string]any{
		"tilejson":           "3.0.0",
		"name":               metadata["name"],
		"description":        metadata["description"],
		"version":            metadata["version"],
		"attribution":        attribution,
		"scheme":             "xyz",
		"tiles":              []string{tileURL},
		"minzoom":            minZoom,
		"maxzoom":            maxZoom,
		"bounds":             floatListMetadata(metadata["bounds"], 4),
		"center":             floatListMetadata(metadata["center"], 3),
		"format":             metadata["format"],
		"tinytiles:revision": revision,
	}
}

func integerMetadata(metadata map[string]string, name string, fallback int) int {
	value, err := strconv.Atoi(strings.TrimSpace(metadata[name]))
	if err != nil {
		return fallback
	}
	return value
}

func floatListMetadata(value string, count int) []float64 {
	parts := strings.Split(value, ",")
	if len(parts) != count {
		return nil
	}
	result := make([]float64, 0, count)
	for _, part := range parts {
		parsed, err := strconv.ParseFloat(strings.TrimSpace(part), 64)
		if err != nil {
			return nil
		}
		result = append(result, parsed)
	}
	return result
}
