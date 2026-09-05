// Package tinytiles is the importable, read-only runtime for published
// .ttiles artifacts. It is deliberately independent of HTTP: applications
// can embed Dataset directly, mount the server subpackage, or run one of the
// command binaries.
package tinytiles

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"runtime"
	"sync"

	tiles "github.com/SimonWaldherr/tinySQL/tiles"
)

var (
	// ErrClosed is returned after Dataset.Close has begun. A dataset is
	// immutable, so callers open a new instance to switch revisions.
	ErrClosed = errors.New("tinytiles: dataset is closed")
	// ErrTileNotFound is compatible with existing database/sql-backed tile
	// readers. It lets applications migrate their GetTileXYZ implementation
	// without changing their ordinary missing-tile handling.
	ErrTileNotFound = sql.ErrNoRows
)

const (
	// DefaultReaderMemoryBytes bounds each reader's page cache when callers do
	// not specify a budget. With the default pool this caps aggregate page
	// caches at 128 MiB instead of inheriting tinySQL's 64 MiB single-reader
	// default eight times.
	DefaultReaderMemoryBytes int64 = 16 << 20
	// MaxDefaultReaders prevents the CPU-derived default from creating an
	// unexpectedly large set of independent page caches on bigger machines.
	MaxDefaultReaders = 8
)

// OpenOptions controls a Dataset reader pool. Each reader has its own tinySQL
// page cache; MaxMemoryBytes is therefore a per-reader, not a process-wide,
// budget.
type OpenOptions struct {
	// Readers is the maximum number of concurrent lookups. A value of zero
	// selects a bounded CPU-based default; negative values are rejected.
	Readers int
	// MaxMemoryBytes is passed to each tinySQL artifact reader. Zero selects
	// DefaultReaderMemoryBytes; negative values are rejected.
	MaxMemoryBytes int64
}

// Dataset is a validated, immutable artifact plus a pool of independent
// readers. It is safe for concurrent use. A tile request borrows exactly one
// reader, so no caller shares a pager handle with another request.
type Dataset struct {
	artifactPath string
	info         tiles.ArtifactInfo
	metadata     map[string]string
	readers      chan tiles.Reader
	done         chan struct{}

	mu       sync.RWMutex
	closed   bool
	inFlight sync.WaitGroup
}

// Keep the narrow application-reader contract compile-checked. It is the
// shape used by Karte.Bayern and other servers that intentionally own their
// HTTP cache and presentation policy outside tinyTiles.
var _ interface {
	GetTileXYZ(z, x, yXYZ int) ([]byte, error)
	Metadata() (map[string]string, error)
	Close() error
} = (*Dataset)(nil)

// Open validates artifactPath before making it available. It opens every
// reader eagerly: a partially unreadable deployment fails at startup instead
// of under production traffic.
func Open(ctx context.Context, artifactPath string, options OpenOptions) (*Dataset, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if options.Readers < 0 {
		return nil, fmt.Errorf("tinytiles: readers must not be negative")
	}
	if options.MaxMemoryBytes < 0 {
		return nil, fmt.Errorf("tinytiles: max memory must not be negative")
	}
	if options.MaxMemoryBytes == 0 {
		options.MaxMemoryBytes = DefaultReaderMemoryBytes
	}
	readerCount := options.Readers
	if readerCount == 0 {
		readerCount = defaultReaderCount()
	}
	first, err := tiles.OpenArtifact(ctx, artifactPath, tiles.OpenOptions{MaxMemoryBytes: options.MaxMemoryBytes})
	if err != nil {
		return nil, fmt.Errorf("tinytiles: open artifact: %w", err)
	}
	dataset := &Dataset{
		artifactPath: artifactPath,
		info:         first.Info(),
		metadata:     make(map[string]string),
		readers:      make(chan tiles.Reader, readerCount),
		done:         make(chan struct{}),
	}
	if err := first.ScanMetadata(ctx, func(name, value string) error {
		if _, exists := dataset.metadata[name]; exists {
			return fmt.Errorf("tinytiles: duplicate metadata name %q", name)
		}
		dataset.metadata[name] = value
		return nil
	}); err != nil {
		_ = first.Close()
		return nil, fmt.Errorf("tinytiles: read artifact metadata: %w", err)
	}
	dataset.readers <- first
	for index := 1; index < readerCount; index++ {
		reader, err := tiles.OpenArtifact(ctx, artifactPath, tiles.OpenOptions{MaxMemoryBytes: options.MaxMemoryBytes})
		if err != nil {
			_ = dataset.Close()
			return nil, fmt.Errorf("tinytiles: open reader %d: %w", index+1, err)
		}
		dataset.readers <- reader
	}
	return dataset, nil
}

func defaultReaderCount() int {
	if count := runtime.GOMAXPROCS(0); count < MaxDefaultReaders {
		return max(count, 1)
	}
	return MaxDefaultReaders
}

// ArtifactPath returns the opened, validated artifact directory exactly as it
// was passed to Open. It is useful for diagnostics, not for reopening files.
func (d *Dataset) ArtifactPath() string {
	if d == nil {
		return ""
	}
	return d.artifactPath
}

// Info returns an independent description of the immutable artifact.
func (d *Dataset) Info() tiles.ArtifactInfo {
	if d == nil {
		return tiles.ArtifactInfo{}
	}
	return d.info.Clone()
}

// Metadata returns a defensive copy of all MBTiles metadata read once at open
// time. It has no tile-table scan or tinySQL page access on the request path.
func (d *Dataset) Metadata() (map[string]string, error) {
	if d == nil {
		return nil, ErrClosed
	}
	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.closed {
		return nil, ErrClosed
	}
	return cloneMetadata(d.metadata), nil
}

// MetadataValue returns one metadata value from the immutable open-time map.
func (d *Dataset) MetadataValue(ctx context.Context, name string) (string, bool, error) {
	if err := ctx.Err(); err != nil {
		return "", false, err
	}
	if d == nil {
		return "", false, ErrClosed
	}
	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.closed {
		return "", false, ErrClosed
	}
	value, found := d.metadata[name]
	return value, found, nil
}

// LookupTMS performs one exact MBTiles/TMS tile lookup.
func (d *Dataset) LookupTMS(ctx context.Context, key tiles.Key) (tiles.Tile, bool, error) {
	if err := key.Validate(); err != nil {
		return tiles.Tile{}, false, err
	}
	reader, err := d.acquire(ctx)
	if err != nil {
		return tiles.Tile{}, false, err
	}
	defer d.release(reader)
	return reader.Lookup(ctx, key)
}

// LookupXYZ performs one slippy-map XYZ tile lookup. The stored artifact
// always uses TMS; only this explicit boundary flips the row.
func (d *Dataset) LookupXYZ(ctx context.Context, z, x, yXYZ int) (tiles.Tile, bool, error) {
	yTMS, err := XYZToTMSY(z, yXYZ)
	if err != nil {
		return tiles.Tile{}, false, err
	}
	return d.LookupTMS(ctx, tiles.Key{Z: z, X: x, Y: yTMS})
}

// GetTileXYZ is a migration-friendly adapter for servers with the established
// GetTileXYZ(z, x, y) ([]byte, error) contract. A missing tile returns
// ErrTileNotFound, which is database/sql's sql.ErrNoRows sentinel.
func (d *Dataset) GetTileXYZ(z, x, yXYZ int) ([]byte, error) {
	tile, found, err := d.LookupXYZ(context.Background(), z, x, yXYZ)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, ErrTileNotFound
	}
	return tile.Data, nil
}

// ScanTMS visits a contiguous TMS rectangle through one borrowed reader.
// The callback is invoked in the stable artifact order documented by
// tinySQL/tiles.
func (d *Dataset) ScanTMS(ctx context.Context, tileRange tiles.Range, fn func(tiles.Tile) error) error {
	if err := tileRange.Validate(); err != nil {
		return err
	}
	reader, err := d.acquire(ctx)
	if err != nil {
		return err
	}
	defer d.release(reader)
	return reader.Scan(ctx, tileRange, fn)
}

// Close waits for active lookups, then closes every reader. It is idempotent;
// requests that have not borrowed a reader fail with ErrClosed.
func (d *Dataset) Close() error {
	if d == nil {
		return nil
	}
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return nil
	}
	d.closed = true
	close(d.done)
	d.mu.Unlock()

	d.inFlight.Wait()
	var closeErr error
	for {
		select {
		case reader := <-d.readers:
			if err := reader.Close(); err != nil {
				closeErr = errors.Join(closeErr, err)
			}
		default:
			return closeErr
		}
	}
}

func (d *Dataset) acquire(ctx context.Context) (tiles.Reader, error) {
	if d == nil {
		return nil, ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	select {
	case reader := <-d.readers:
		d.mu.RLock()
		if d.closed {
			d.mu.RUnlock()
			_ = reader.Close()
			return nil, ErrClosed
		}
		d.inFlight.Add(1)
		d.mu.RUnlock()
		return reader, nil
	case <-d.done:
		return nil, ErrClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (d *Dataset) release(reader tiles.Reader) {
	if d == nil || reader == nil {
		return
	}
	d.mu.RLock()
	closed := d.closed
	if !closed {
		d.readers <- reader
	}
	d.mu.RUnlock()
	if closed {
		_ = reader.Close()
	}
	d.inFlight.Done()
}

// XYZToTMSY converts a valid XYZ row to its MBTiles/TMS row at z.
func XYZToTMSY(z, yXYZ int) (int, error) {
	if err := (tiles.Key{Z: z, X: 0, Y: yXYZ}).Validate(); err != nil {
		return 0, fmt.Errorf("tinytiles: invalid XYZ row: %w", err)
	}
	return (1 << z) - 1 - yXYZ, nil
}

// TMSToXYZY converts a valid MBTiles/TMS row to its slippy-map XYZ row at z.
func TMSToXYZY(z, yTMS int) (int, error) {
	if err := (tiles.Key{Z: z, X: 0, Y: yTMS}).Validate(); err != nil {
		return 0, fmt.Errorf("tinytiles: invalid TMS row: %w", err)
	}
	return (1 << z) - 1 - yTMS, nil
}

func cloneMetadata(source map[string]string) map[string]string {
	copy := make(map[string]string, len(source))
	for name, value := range source {
		copy[name] = value
	}
	return copy
}
