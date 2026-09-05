//go:build !js && !wasm && !baremetal

// tinytiles-demo-server serves a published .ttiles directory through the
// revisioned HTTP sync protocol used by the WASM demo. It is intentionally a
// small reference server, not an authentication or deployment product.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/Karte-Bayern/tinyTiles/offline"
	tiles "github.com/SimonWaldherr/tinySQL/tiles"
)

type tileServer struct {
	readers         chan tiles.Reader
	revision        string
	dataset         string
	contentType     string
	contentEncoding string
	publicBase      string
	corsOrigin      string
	createdAt       time.Time
}

func main() {
	artifact := flag.String("artifact", "", "published dataset.ttiles directory")
	addr := flag.String("addr", ":8080", "listen address")
	dataset := flag.String("dataset", "demo", "stable dataset identifier")
	publicBase := flag.String("public-base", "", "public base URL; default derives from the request")
	corsOrigin := flag.String("cors", "", "optional CORS allow-origin, e.g. http://localhost:8081 or *")
	readers := flag.Int("readers", min(runtime.GOMAXPROCS(0), 8), "independent artifact readers")
	memory := flag.Int64("max-memory", 16<<20, "per-reader tinySQL cache budget in bytes")
	flag.Parse()
	if *artifact == "" || *readers < 1 || *memory <= 0 {
		fmt.Fprintln(os.Stderr, "usage: tinytiles-demo-server -artifact dataset.ttiles/ [-addr :8080]")
		os.Exit(2)
	}
	server, err := newTileServer(context.Background(), *artifact, *dataset, *publicBase, *corsOrigin, *readers, *memory)
	if err != nil {
		log.Fatal(err)
	}
	defer server.Close()
	listener := &http.Server{
		Addr:              *addr,
		Handler:           server.routes(),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = listener.Shutdown(shutdown)
	}()
	log.Printf("tinyTiles demo server listening on %s; sync manifest at /sync/manifest.json", *addr)
	if err := listener.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}

func newTileServer(ctx context.Context, artifact, dataset, publicBase, corsOrigin string, count int, memory int64) (*tileServer, error) {
	if count < 1 {
		return nil, errors.New("reader count must be positive")
	}
	if memory <= 0 {
		return nil, errors.New("reader memory budget must be positive")
	}
	if err := validateDataset(dataset); err != nil {
		return nil, err
	}
	var err error
	publicBase, err = normalizePublicBase(publicBase)
	if err != nil {
		return nil, err
	}
	corsOrigin, err = normalizeCORSOrigin(corsOrigin)
	if err != nil {
		return nil, err
	}
	first, err := tiles.OpenArtifact(ctx, artifact, tiles.OpenOptions{MaxMemoryBytes: memory})
	if err != nil {
		return nil, fmt.Errorf("open artifact: %w", err)
	}
	manifest := first.Info()
	revision := manifest.TileDigestSHA256
	if len(revision) != 64 {
		_ = first.Close()
		return nil, errors.New("artifact manifest has no usable tile digest revision")
	}
	contentType := "application/octet-stream"
	if format, found, err := first.Metadata(ctx, "format"); err == nil && found && strings.EqualFold(format, "pbf") {
		contentType = "application/vnd.mapbox-vector-tile"
	}
	contentEncoding := ""
	if encoding, found, err := first.Metadata(ctx, "kb:content_encoding"); err == nil && found && strings.EqualFold(encoding, "gzip") {
		contentEncoding = "gzip"
	}
	server := &tileServer{readers: make(chan tiles.Reader, count), revision: revision, dataset: dataset, contentType: contentType, contentEncoding: contentEncoding, publicBase: publicBase, corsOrigin: corsOrigin, createdAt: manifest.CreatedAt}
	server.readers <- first
	for i := 1; i < count; i++ {
		reader, err := tiles.OpenArtifact(ctx, artifact, tiles.OpenOptions{MaxMemoryBytes: memory})
		if err != nil {
			server.Close()
			return nil, fmt.Errorf("open reader %d: %w", i, err)
		}
		server.readers <- reader
	}
	return server, nil
}

func (s *tileServer) Close() {
	if s == nil || s.readers == nil {
		return
	}
	for {
		select {
		case reader := <-s.readers:
			_ = reader.Close()
		default:
			return
		}
	}
}

func (s *tileServer) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/sync/manifest.json", s.serveManifest)
	mux.HandleFunc("/tiles/", s.serveTile)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	return http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if s.corsOrigin != "" {
			w.Header().Set("Access-Control-Allow-Origin", s.corsOrigin)
			w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, If-None-Match")
			w.Header().Set("Access-Control-Expose-Headers", offline.HeaderTileChecksum+", "+offline.HeaderTileContentEncoding+", ETag")
		}
		if request.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		mux.ServeHTTP(w, request)
	})
}

func (s *tileServer) serveManifest(w http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	baseURL, err := s.baseURL(request)
	if err != nil {
		http.Error(w, "invalid public base URL", http.StatusBadRequest)
		return
	}
	manifest := offline.Manifest{
		FormatVersion:    offline.ProtocolVersion,
		Dataset:          s.dataset,
		Revision:         s.revision,
		CoordinateSystem: "TMS",
		TileURLTemplate:  baseURL + "/tiles/{revision}/{z}/{x}/{y}",
		ContentType:      s.contentType,
		ContentEncoding:  s.contentEncoding,
		CreatedAt:        s.createdAt,
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("ETag", quoteETag(s.revision))
	if request.Header.Get("If-None-Match") == quoteETag(s.revision) {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	if err := json.NewEncoder(w).Encode(manifest); err != nil {
		log.Printf("encode sync manifest: %v", err)
	}
}

func (s *tileServer) serveTile(w http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	parts := strings.Split(strings.TrimPrefix(path.Clean(request.URL.Path), "/tiles/"), "/")
	if len(parts) != 4 || parts[0] != s.revision {
		http.NotFound(w, request)
		return
	}
	key, err := parseKey(parts[1:])
	if err != nil {
		http.Error(w, "invalid TMS tile coordinate", http.StatusBadRequest)
		return
	}
	etag := quoteETag(s.revision + ":" + key.String())
	if request.Header.Get("If-None-Match") == etag {
		w.Header().Set("ETag", etag)
		w.WriteHeader(http.StatusNotModified)
		return
	}
	reader, err := s.acquire(request.Context())
	if err != nil {
		http.Error(w, "reader unavailable", http.StatusServiceUnavailable)
		return
	}
	defer s.release(reader)
	tile, found, err := reader.Lookup(request.Context(), tiles.Key{Z: key.Z, X: key.X, Y: key.Y})
	if err != nil {
		log.Printf("read tile %s: %v", key, err)
		http.Error(w, "tile lookup failed", http.StatusInternalServerError)
		return
	}
	if !found {
		http.NotFound(w, request)
		return
	}
	w.Header().Set("Content-Type", s.contentType)
	if s.contentEncoding != "" {
		w.Header().Set(offline.HeaderTileContentEncoding, s.contentEncoding)
	}
	w.Header().Set(offline.HeaderTileChecksum, offline.Checksum(tile.Data))
	w.Header().Set("ETag", etag)
	w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
	w.Header().Set("Content-Length", strconv.Itoa(len(tile.Data)))
	_, _ = w.Write(tile.Data)
}

func (s *tileServer) acquire(ctx context.Context) (tiles.Reader, error) {
	if s == nil || s.readers == nil {
		return nil, errors.New("reader pool is unavailable")
	}
	select {
	case reader := <-s.readers:
		return reader, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *tileServer) release(reader tiles.Reader) {
	if reader != nil {
		s.readers <- reader
	}
}

func (s *tileServer) baseURL(request *http.Request) (string, error) {
	if s.publicBase != "" {
		return s.publicBase, nil
	}
	scheme := "http"
	if request.TLS != nil {
		scheme = "https"
	}
	candidate := scheme + "://" + request.Host
	parsed, err := url.Parse(candidate)
	if err != nil || parsed.Host != request.Host || parsed.Path != "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", errors.New("request host is not a valid origin")
	}
	return candidate, nil
}

func parseKey(parts []string) (offline.TileKey, error) {
	if len(parts) != 3 {
		return offline.TileKey{}, errors.New("need z/x/y")
	}
	values := [3]int{}
	for index, value := range parts {
		parsed, err := strconv.Atoi(value)
		if err != nil {
			return offline.TileKey{}, err
		}
		values[index] = parsed
	}
	key := offline.TileKey{Z: values[0], X: values[1], Y: values[2]}
	return key, key.Validate()
}

func validateDataset(dataset string) error {
	manifest := offline.Manifest{FormatVersion: offline.ProtocolVersion, Dataset: dataset, Revision: strings.Repeat("0", 64), CoordinateSystem: "TMS", TileURLTemplate: "https://example.invalid/{z}/{x}/{y}"}
	return manifest.Validate()
}

func normalizePublicBase(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", nil
	}
	parsed, err := url.Parse(value)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", fmt.Errorf("public base must be an absolute http(s) URL without credentials, query or fragment")
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
		return "", fmt.Errorf("cors origin must be * or an http(s) origin without path, credentials, query or fragment")
	}
	return strings.TrimRight(parsed.String(), "/"), nil
}

func quoteETag(value string) string { return `"` + value + `"` }

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
