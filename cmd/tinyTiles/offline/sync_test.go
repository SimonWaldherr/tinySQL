package offline

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestSynchronizerPublishesNewRevisionOnlyAfterSuccess(t *testing.T) {
	store := NewMemoryStore()
	old := testManifest("old-revision")
	if err := store.PutManifest(context.Background(), old); err != nil {
		t.Fatal(err)
	}
	oldKey := TileKey{Z: 2, X: 0, Y: 0}
	if err := store.PutTile(context.Background(), old.Dataset, old.Revision, oldKey, checkedTile([]byte("old"))); err != nil {
		t.Fatal(err)
	}
	newManifest := testManifest("new-revision")
	fetcher := &fakeFetcher{manifest: newManifest, fail: map[string]error{"2/1/0": errors.New("network interruption")}}
	synchronizer := &Synchronizer{Store: store, Fetcher: fetcher}
	request := SyncRequest{Keys: []TileKey{{Z: 2, X: 0, Y: 0}, {Z: 2, X: 1, Y: 0}}, Concurrency: 2}
	if _, err := synchronizer.Sync(context.Background(), request); err == nil {
		t.Fatal("partial sync unexpectedly succeeded")
	}
	active, found, err := store.GetManifest(context.Background(), old.Dataset)
	if err != nil || !found || active.Revision != old.Revision {
		t.Fatalf("active manifest after failed sync=%#v found=%t err=%v", active, found, err)
	}
	if _, found, err := store.GetTile(context.Background(), old.Dataset, old.Revision, oldKey); err != nil || !found {
		t.Fatalf("old revision disappeared after failed sync: found=%t err=%v", found, err)
	}

	delete(fetcher.fail, "2/1/0")
	result, err := synchronizer.Sync(context.Background(), request)
	if err != nil {
		t.Fatalf("resumed sync: %v", err)
	}
	if result.Revision != newManifest.Revision || result.Total != 2 || result.Downloaded+result.Reused != 2 {
		t.Fatalf("unexpected sync result: %#v", result)
	}
	active, found, err = store.GetManifest(context.Background(), old.Dataset)
	if err != nil || !found || active.Revision != newManifest.Revision {
		t.Fatalf("new manifest was not published: %#v found=%t err=%v", active, found, err)
	}
}

func TestSynchronizerStreamsRangesAndReusesCachedRevision(t *testing.T) {
	store := NewMemoryStore()
	fetcher := &fakeFetcher{manifest: testManifest("range-revision"), delay: time.Millisecond}
	synchronizer := &Synchronizer{Store: store, Fetcher: fetcher}
	request := SyncRequest{Ranges: []TileRange{{Z: 3, XMin: 0, XMax: 3, YMin: 0, YMax: 3}}, Concurrency: 3}
	result, err := synchronizer.Sync(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if result.Total != 16 || result.Downloaded != 16 || result.Reused != 0 {
		t.Fatalf("first sync=%#v", result)
	}
	if got := fetcher.maxConcurrent(); got < 2 || got > 3 {
		t.Fatalf("worker concurrency=%d, want between 2 and 3", got)
	}
	calls := fetcher.callCount()
	result, err = synchronizer.Sync(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if result.Downloaded != 0 || result.Reused != 16 || fetcher.callCount() != calls {
		t.Fatalf("cached sync=%#v calls=%d want=%d", result, fetcher.callCount(), calls)
	}
}

func TestSynchronizerPrunesOldRevisionAfterPublish(t *testing.T) {
	store := NewMemoryStore()
	old := testManifest("old")
	if err := store.PutManifest(context.Background(), old); err != nil {
		t.Fatal(err)
	}
	key := TileKey{Z: 1, X: 0, Y: 0}
	if err := store.PutTile(context.Background(), old.Dataset, old.Revision, key, checkedTile([]byte("old"))); err != nil {
		t.Fatal(err)
	}
	newManifest := testManifest("new")
	synchronizer := &Synchronizer{Store: store, Fetcher: &fakeFetcher{manifest: newManifest}}
	if _, err := synchronizer.Sync(context.Background(), SyncRequest{Keys: []TileKey{key}, PrunePrevious: true}); err != nil {
		t.Fatal(err)
	}
	if _, found, err := store.GetTile(context.Background(), old.Dataset, old.Revision, key); err != nil || found {
		t.Fatalf("old revision still available: found=%t err=%v", found, err)
	}
}

func TestSyncRequestRejectsOverlapsAndInvalidConcurrency(t *testing.T) {
	cases := []SyncRequest{
		{Ranges: []TileRange{{Z: 2, XMin: 0, XMax: 1, YMin: 0, YMax: 1}, {Z: 2, XMin: 1, XMax: 2, YMin: 0, YMax: 1}}},
		{Ranges: []TileRange{{Z: 2, XMin: 0, XMax: 1, YMin: 0, YMax: 1}}, Keys: []TileKey{{Z: 2, X: 1, Y: 1}}},
		{Keys: []TileKey{{Z: 1, X: 0, Y: 0}, {Z: 1, X: 0, Y: 0}}},
		{Concurrency: maxSyncConcurrency + 1},
	}
	for _, request := range cases {
		if err := request.validate(); err == nil {
			t.Fatalf("invalid request accepted: %#v", request)
		}
	}
}

func TestSynchronizerRejectsChecksumMismatch(t *testing.T) {
	store := NewMemoryStore()
	fetcher := &fakeFetcher{manifest: testManifest("bad-checksum"), override: map[string]Tile{"1/0/0": {Data: []byte("bad"), Checksum: Checksum([]byte("different"))}}}
	synchronizer := &Synchronizer{Store: store, Fetcher: fetcher}
	if _, err := synchronizer.Sync(context.Background(), SyncRequest{Keys: []TileKey{{Z: 1, X: 0, Y: 0}}}); err == nil {
		t.Fatal("checksum mismatch accepted")
	}
	if _, found, err := store.GetManifest(context.Background(), fetcher.manifest.Dataset); err != nil || found {
		t.Fatalf("corrupt tile published a manifest: found=%t err=%v", found, err)
	}
}

func TestTileRangeVisitHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	visited := 0
	err := (TileRange{Z: 4, XMin: 0, XMax: 3, YMin: 0, YMax: 3}).Visit(ctx, func(TileKey) error {
		visited++
		if visited == 2 {
			cancel()
		}
		return nil
	})
	if !errors.Is(err, context.Canceled) || visited != 2 {
		t.Fatalf("visit err=%v visited=%d", err, visited)
	}
}

func testManifest(revision string) Manifest {
	return Manifest{FormatVersion: ProtocolVersion, Dataset: "demo", Revision: revision, CoordinateSystem: "TMS", TileURLTemplate: "https://tiles.example.invalid/tiles/{revision}/{z}/{x}/{y}", ContentType: "application/vnd.mapbox-vector-tile"}
}

func checkedTile(data []byte) Tile {
	return Tile{Data: append([]byte(nil), data...), ContentType: "application/octet-stream", Checksum: Checksum(data)}
}

type fakeFetcher struct {
	mu       sync.Mutex
	manifest Manifest
	fail     map[string]error
	override map[string]Tile
	delay    time.Duration
	active   int
	max      int
	calls    int
}

func (f *fakeFetcher) FetchManifest(context.Context) (Manifest, error) { return f.manifest, nil }

func (f *fakeFetcher) FetchTile(ctx context.Context, _ Manifest, key TileKey) (Tile, error) {
	f.mu.Lock()
	f.calls++
	f.active++
	if f.active > f.max {
		f.max = f.active
	}
	err := f.fail[key.String()]
	override, overridden := f.override[key.String()]
	delay := f.delay
	f.mu.Unlock()
	defer func() {
		f.mu.Lock()
		f.active--
		f.mu.Unlock()
	}()
	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return Tile{}, ctx.Err()
		}
	}
	if err != nil {
		return Tile{}, err
	}
	if overridden {
		return override, nil
	}
	return checkedTile([]byte(fmt.Sprintf("tile:%s", key))), nil
}

func (f *fakeFetcher) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func (f *fakeFetcher) maxConcurrent() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.max
}
