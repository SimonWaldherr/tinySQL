// Package offline provides a small, revisioned tile cache protocol for native
// applications and WebAssembly clients. It deliberately caches individual
// tile responses instead of trying to open a server-side .ttiles pager inside
// a browser.
package offline

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// ProtocolVersion is the compatibility version for sync manifests and cache
// records. A revision names an immutable set of tiles for one dataset.
const ProtocolVersion = 1

const maxTileZoom = 30

// TileKey uses the MBTiles/TMS coordinate convention. The key does not infer
// Web Mercator or XYZ row flipping: callers must pass the coordinate system
// advertised by the server manifest.
type TileKey struct {
	Z int `json:"z"`
	X int `json:"x"`
	Y int `json:"y"`
}

func (k TileKey) Validate() error {
	if k.Z < 0 || k.Z > maxTileZoom {
		return fmt.Errorf("invalid tile zoom %d", k.Z)
	}
	limit := 1 << uint(k.Z)
	if k.X < 0 || k.X >= limit || k.Y < 0 || k.Y >= limit {
		return fmt.Errorf("invalid TMS tile coordinate %d/%d/%d", k.Z, k.X, k.Y)
	}
	return nil
}

func (k TileKey) String() string {
	return strconv.Itoa(k.Z) + "/" + strconv.Itoa(k.X) + "/" + strconv.Itoa(k.Y)
}

// TileRange streams a spatially contiguous inclusive TMS rectangle. Visit
// never materializes every key, so a large region remains bounded by the sync
// worker pool rather than its tile count.
type TileRange struct {
	Z    int `json:"z"`
	XMin int `json:"x_min"`
	XMax int `json:"x_max"`
	YMin int `json:"y_min"`
	YMax int `json:"y_max"`
}

func (r TileRange) Validate() error {
	if r.XMin > r.XMax || r.YMin > r.YMax {
		return fmt.Errorf("invalid tile range %d: x/y minimum exceeds maximum", r.Z)
	}
	if err := (TileKey{Z: r.Z, X: r.XMin, Y: r.YMin}).Validate(); err != nil {
		return err
	}
	return (TileKey{Z: r.Z, X: r.XMax, Y: r.YMax}).Validate()
}

func (r TileRange) Count() (uint64, error) {
	if err := r.Validate(); err != nil {
		return 0, err
	}
	width := uint64(r.XMax-r.XMin) + 1
	height := uint64(r.YMax-r.YMin) + 1
	if height != 0 && width > ^uint64(0)/height {
		return 0, errors.New("tile range count overflows uint64")
	}
	return width * height, nil
}

func (r TileRange) Visit(ctx context.Context, fn func(TileKey) error) error {
	if err := r.Validate(); err != nil {
		return err
	}
	if fn == nil {
		return errors.New("tile range visitor is nil")
	}
	for x := r.XMin; x <= r.XMax; x++ {
		for y := r.YMin; y <= r.YMax; y++ {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := fn(TileKey{Z: r.Z, X: x, Y: y}); err != nil {
				return err
			}
		}
	}
	return nil
}

// Manifest is fetched from a server before a synchronization pass. Revision
// must change whenever any tile served by TileURLTemplate changes; that makes
// a client-side cache safe to publish only after the whole requested set is
// stored under its new namespace.
type Manifest struct {
	FormatVersion    int       `json:"format_version"`
	Dataset          string    `json:"dataset"`
	Revision         string    `json:"revision"`
	CoordinateSystem string    `json:"coordinate_system"`
	TileURLTemplate  string    `json:"tile_url_template"`
	ContentType      string    `json:"content_type,omitempty"`
	ContentEncoding  string    `json:"content_encoding,omitempty"`
	CreatedAt        time.Time `json:"created_at,omitempty"`
}

func (m Manifest) Validate() error {
	if m.FormatVersion != ProtocolVersion {
		return fmt.Errorf("unsupported sync manifest version %d", m.FormatVersion)
	}
	if err := validateIdentifier("dataset", m.Dataset, 256); err != nil {
		return err
	}
	if err := validateIdentifier("revision", m.Revision, 512); err != nil {
		return err
	}
	if !strings.EqualFold(strings.TrimSpace(m.CoordinateSystem), "TMS") {
		return fmt.Errorf("unsupported coordinate system %q (want TMS)", m.CoordinateSystem)
	}
	template := strings.TrimSpace(m.TileURLTemplate)
	if template == "" {
		return errors.New("tile_url_template is empty")
	}
	for _, placeholder := range []string{"{z}", "{x}", "{y}"} {
		if !strings.Contains(template, placeholder) {
			return fmt.Errorf("tile_url_template is missing %s", placeholder)
		}
	}
	if _, err := url.Parse(template); err != nil {
		return fmt.Errorf("parse tile_url_template: %w", err)
	}
	return nil
}

// TileURL resolves a manifest template without concatenating untrusted paths.
// {revision} is optional for backwards-compatible servers, but a revision in
// the path is strongly recommended because it enables immutable HTTP caching.
func (m Manifest) TileURL(key TileKey) (string, error) {
	if err := m.Validate(); err != nil {
		return "", err
	}
	if err := key.Validate(); err != nil {
		return "", err
	}
	resolved := m.TileURLTemplate
	replacements := map[string]string{
		"{revision}": url.PathEscape(m.Revision),
		"{z}":        strconv.Itoa(key.Z),
		"{x}":        strconv.Itoa(key.X),
		"{y}":        strconv.Itoa(key.Y),
	}
	for placeholder, value := range replacements {
		resolved = strings.ReplaceAll(resolved, placeholder, value)
	}
	return resolved, nil
}

// Tile contains one immutable tile response. Checksum, when provided, is a
// case-insensitive SHA-256 hex digest of Data and is verified before storage.
type Tile struct {
	Data            []byte `json:"-"`
	ContentType     string `json:"content_type,omitempty"`
	ContentEncoding string `json:"content_encoding,omitempty"`
	ETag            string `json:"etag,omitempty"`
	Checksum        string `json:"checksum_sha256,omitempty"`
}

func (t Tile) Validate() error {
	if t.Checksum == "" {
		return nil
	}
	if len(t.Checksum) != 64 {
		return fmt.Errorf("invalid tile SHA-256 length %d", len(t.Checksum))
	}
	if _, err := hex.DecodeString(t.Checksum); err != nil {
		return fmt.Errorf("invalid tile SHA-256: %w", err)
	}
	return nil
}

func (t Tile) Clone() Tile {
	t.Data = append([]byte(nil), t.Data...)
	return t
}

// Store is implemented by an in-memory store for tests, a native FileStore,
// and the browser IndexedDBStore. Tiles are namespaced by immutable revision;
// PutManifest is the single operation that switches the active revision.
type Store interface {
	GetManifest(ctx context.Context, dataset string) (Manifest, bool, error)
	PutManifest(ctx context.Context, manifest Manifest) error
	GetTile(ctx context.Context, dataset, revision string, key TileKey) (Tile, bool, error)
	PutTile(ctx context.Context, dataset, revision string, key TileKey, tile Tile) error
	DeleteRevision(ctx context.Context, dataset, revision string) error
}

// Fetcher supplies an immutable remote manifest and individual tiles. HTTPFetcher
// is the standard implementation; tests and embedded applications can provide
// their own source without an HTTP dependency.
type Fetcher interface {
	FetchManifest(ctx context.Context) (Manifest, error)
	FetchTile(ctx context.Context, manifest Manifest, key TileKey) (Tile, error)
}

func validateIdentifier(kind, value string, maxBytes int) error {
	value = strings.TrimSpace(value)
	if value == "" {
		return fmt.Errorf("%s is empty", kind)
	}
	if len(value) > maxBytes {
		return fmt.Errorf("%s exceeds %d bytes", kind, maxBytes)
	}
	for _, r := range value {
		if r < 0x20 || r == 0x7f {
			return fmt.Errorf("%s contains a control character", kind)
		}
	}
	return nil
}
