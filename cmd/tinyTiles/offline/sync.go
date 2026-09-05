package offline

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
)

const (
	defaultSyncConcurrency = 4
	maxSyncConcurrency     = 32
)

// SyncRequest selects a bounded set of tiles to retain offline. Ranges are
// generated directly into workers; the implementation never expands an entire
// region into a tile slice. Ranges and explicit keys must not overlap.
type SyncRequest struct {
	Dataset       string             `json:"dataset,omitempty"`
	Ranges        []TileRange        `json:"ranges,omitempty"`
	Keys          []TileKey          `json:"keys,omitempty"`
	Concurrency   int                `json:"concurrency,omitempty"`
	PrunePrevious bool               `json:"prune_previous,omitempty"`
	Progress      func(SyncProgress) `json:"-"`
}

// SyncProgress is emitted after a manifest was fetched and after each tile is
// retained or reused. Callbacks are invoked from workers and must be safe for
// concurrent use.
type SyncProgress struct {
	Phase      string  `json:"phase"`
	Total      uint64  `json:"total"`
	Completed  uint64  `json:"completed"`
	Downloaded uint64  `json:"downloaded"`
	Reused     uint64  `json:"reused"`
	Key        TileKey `json:"key,omitempty"`
}

// SyncResult describes a completed cache switch. A prune error is reported but
// does not roll back the new manifest: retaining an old immutable namespace is
// safe, whereas rolling back a fully synchronized new revision is not useful.
type SyncResult struct {
	Dataset        string `json:"dataset"`
	Revision       string `json:"revision"`
	Total          uint64 `json:"total"`
	Downloaded     uint64 `json:"downloaded"`
	Reused         uint64 `json:"reused"`
	ManifestWasNew bool   `json:"manifest_was_new"`
	PruneError     string `json:"prune_error,omitempty"`
}

// Synchronizer commits a new active manifest only after every requested tile
// for that revision is present. If a fetch fails, a previous manifest remains
// usable and the next run can safely resume from retained revisioned tiles.
type Synchronizer struct {
	Store   Store
	Fetcher Fetcher
	mu      sync.Mutex
}

func (s *Synchronizer) Sync(ctx context.Context, request SyncRequest) (SyncResult, error) {
	if s == nil {
		return SyncResult{}, errors.New("offline synchronizer is nil")
	}
	// One synchronizer serializes manifest publication. A caller that shares a
	// Store across components should likewise share this synchronizer instance.
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Store == nil {
		return SyncResult{}, errors.New("offline sync store is nil")
	}
	if s.Fetcher == nil {
		return SyncResult{}, errors.New("offline sync fetcher is nil")
	}
	if err := request.validate(); err != nil {
		return SyncResult{}, err
	}
	manifest, err := s.Fetcher.FetchManifest(ctx)
	if err != nil {
		return SyncResult{}, fmt.Errorf("fetch sync manifest: %w", err)
	}
	if err := manifest.Validate(); err != nil {
		return SyncResult{}, fmt.Errorf("validate sync manifest: %w", err)
	}
	if request.Dataset != "" && request.Dataset != manifest.Dataset {
		return SyncResult{}, fmt.Errorf("requested dataset %q does not match manifest dataset %q", request.Dataset, manifest.Dataset)
	}
	total, err := request.total()
	if err != nil {
		return SyncResult{}, err
	}
	emitProgress(request.Progress, SyncProgress{Phase: "manifest", Total: total})
	previous, hadPrevious, err := s.Store.GetManifest(ctx, manifest.Dataset)
	if err != nil {
		return SyncResult{}, fmt.Errorf("read cached manifest: %w", err)
	}

	workers := request.workerCount()
	jobs := make(chan TileKey, workers*2)
	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var state syncState
	state.total = total
	var workersWG sync.WaitGroup
	var firstErr error
	var errMu sync.Mutex
	fail := func(err error) {
		if err == nil {
			return
		}
		errMu.Lock()
		if firstErr == nil {
			firstErr = err
			cancel()
		}
		errMu.Unlock()
	}
	for i := 0; i < workers; i++ {
		workersWG.Add(1)
		go func() {
			defer workersWG.Done()
			for key := range jobs {
				if err := workCtx.Err(); err != nil {
					return
				}
				cached, found, err := s.Store.GetTile(workCtx, manifest.Dataset, manifest.Revision, key)
				if err != nil {
					fail(fmt.Errorf("read cached tile %s: %w", key, err))
					return
				}
				if found {
					if err := verifyTile(cached); err != nil {
						fail(fmt.Errorf("validate cached tile %s: %w", key, err))
						return
					}
					state.advance(false, key, request.Progress)
					continue
				}
				tile, err := s.Fetcher.FetchTile(workCtx, manifest, key)
				if err != nil {
					fail(fmt.Errorf("fetch tile %s: %w", key, err))
					return
				}
				if err := verifyTile(tile); err != nil {
					fail(fmt.Errorf("verify tile %s: %w", key, err))
					return
				}
				if err := s.Store.PutTile(workCtx, manifest.Dataset, manifest.Revision, key, tile); err != nil {
					fail(fmt.Errorf("store tile %s: %w", key, err))
					return
				}
				state.advance(true, key, request.Progress)
			}
		}()
	}
	produceErr := request.visit(workCtx, func(key TileKey) error {
		select {
		case jobs <- key:
			return nil
		case <-workCtx.Done():
			return workCtx.Err()
		}
	})
	close(jobs)
	workersWG.Wait()
	errMu.Lock()
	workerErr := firstErr
	errMu.Unlock()
	if workerErr != nil {
		return SyncResult{}, workerErr
	}
	if produceErr != nil {
		return SyncResult{}, produceErr
	}
	if err := ctx.Err(); err != nil {
		return SyncResult{}, err
	}
	if err := s.Store.PutManifest(ctx, manifest); err != nil {
		return SyncResult{}, fmt.Errorf("publish cached manifest: %w", err)
	}
	result := SyncResult{
		Dataset:        manifest.Dataset,
		Revision:       manifest.Revision,
		Total:          total,
		Downloaded:     state.downloadedCount(),
		Reused:         state.reusedCount(),
		ManifestWasNew: !hadPrevious || previous.Revision != manifest.Revision,
	}
	if request.PrunePrevious && hadPrevious && previous.Revision != manifest.Revision {
		if err := s.Store.DeleteRevision(ctx, previous.Dataset, previous.Revision); err != nil {
			result.PruneError = err.Error()
		}
	}
	emitProgress(request.Progress, SyncProgress{Phase: "published", Total: total, Completed: total, Downloaded: result.Downloaded, Reused: result.Reused})
	return result, nil
}

func (r SyncRequest) validate() error {
	if r.Dataset != "" {
		if err := validateIdentifier("dataset", r.Dataset, 256); err != nil {
			return err
		}
	}
	if r.Concurrency < 0 || r.Concurrency > maxSyncConcurrency {
		return fmt.Errorf("sync concurrency must be between 0 and %d", maxSyncConcurrency)
	}
	for i, tileRange := range r.Ranges {
		if err := tileRange.Validate(); err != nil {
			return fmt.Errorf("range %d: %w", i, err)
		}
		for previous := 0; previous < i; previous++ {
			if rangesOverlap(tileRange, r.Ranges[previous]) {
				return fmt.Errorf("ranges %d and %d overlap", previous, i)
			}
		}
	}
	seenKeys := make(map[string]struct{}, len(r.Keys))
	for _, key := range r.Keys {
		if err := key.Validate(); err != nil {
			return err
		}
		if _, found := seenKeys[key.String()]; found {
			return fmt.Errorf("duplicate explicit tile key %s", key)
		}
		seenKeys[key.String()] = struct{}{}
		for _, tileRange := range r.Ranges {
			if key.Z == tileRange.Z && key.X >= tileRange.XMin && key.X <= tileRange.XMax && key.Y >= tileRange.YMin && key.Y <= tileRange.YMax {
				return fmt.Errorf("explicit tile key %s overlaps a requested range", key)
			}
		}
	}
	return nil
}

func (r SyncRequest) total() (uint64, error) {
	total := uint64(len(r.Keys))
	for _, tileRange := range r.Ranges {
		count, err := tileRange.Count()
		if err != nil {
			return 0, err
		}
		if total > ^uint64(0)-count {
			return 0, errors.New("sync tile count overflows uint64")
		}
		total += count
	}
	return total, nil
}

func (r SyncRequest) workerCount() int {
	if r.Concurrency > 0 {
		return r.Concurrency
	}
	return defaultSyncConcurrency
}

func (r SyncRequest) visit(ctx context.Context, fn func(TileKey) error) error {
	for _, key := range r.Keys {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := fn(key); err != nil {
			return err
		}
	}
	for _, tileRange := range r.Ranges {
		if err := tileRange.Visit(ctx, fn); err != nil {
			return err
		}
	}
	return nil
}

func rangesOverlap(a, b TileRange) bool {
	return a.Z == b.Z && a.XMin <= b.XMax && b.XMin <= a.XMax && a.YMin <= b.YMax && b.YMin <= a.YMax
}

func verifyTile(tile Tile) error {
	if err := tile.Validate(); err != nil {
		return err
	}
	if tile.Checksum == "" {
		return nil
	}
	digest := sha256.Sum256(tile.Data)
	if !strings.EqualFold(hex.EncodeToString(digest[:]), tile.Checksum) {
		return errors.New("SHA-256 checksum mismatch")
	}
	return nil
}

type syncState struct {
	mu         sync.Mutex
	total      uint64
	completed  uint64
	downloaded uint64
	reused     uint64
}

func (s *syncState) advance(downloaded bool, key TileKey, progress func(SyncProgress)) {
	s.mu.Lock()
	s.completed++
	if downloaded {
		s.downloaded++
	} else {
		s.reused++
	}
	next := SyncProgress{Phase: "tile", Total: s.total, Completed: s.completed, Downloaded: s.downloaded, Reused: s.reused, Key: key}
	s.mu.Unlock()
	emitProgress(progress, next)
}

func (s *syncState) downloadedCount() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.downloaded
}

func (s *syncState) reusedCount() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reused
}

func emitProgress(fn func(SyncProgress), progress SyncProgress) {
	if fn != nil {
		fn(progress)
	}
}
