// tinytiles-native-client synchronizes one bounded TMS rectangle into a
// durable FileStore. It is useful both as a command-line demo and as a small
// reference for native mobile/desktop integrations.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Karte-Bayern/tinyTiles/offline"
)

func main() {
	manifestURL := flag.String("manifest", "", "absolute URL of /sync/manifest.json")
	cacheDir := flag.String("cache", "./tinytiles-cache", "durable offline cache directory")
	dataset := flag.String("dataset", "", "optional expected dataset identifier")
	z := flag.Int("z", 0, "TMS zoom")
	xMin := flag.Int("xmin", 0, "minimum TMS x")
	xMax := flag.Int("xmax", 0, "maximum TMS x")
	yMin := flag.Int("ymin", 0, "minimum TMS y")
	yMax := flag.Int("ymax", 0, "maximum TMS y")
	concurrency := flag.Int("concurrency", 4, "bounded parallel tile fetches")
	prune := flag.Bool("prune", false, "remove the previous immutable revision after successful sync")
	read := flag.String("read", "", "read a cached TMS tile as z/x/y without contacting the server")
	output := flag.String("output", "", "write the cached tile's raw bytes to this file (requires -read)")
	flag.Parse()
	if *read != "" {
		if *dataset == "" || *manifestURL != "" {
			fmt.Fprintln(os.Stderr, "-read requires -dataset and cannot be combined with -manifest")
			os.Exit(2)
		}
		if err := readCachedTile(context.Background(), *cacheDir, *dataset, *read, *output); err != nil {
			fmt.Fprintln(os.Stderr, "read cache:", err)
			os.Exit(1)
		}
		return
	}
	if *output != "" {
		fmt.Fprintln(os.Stderr, "-output requires -read")
		os.Exit(2)
	}
	if *manifestURL == "" {
		fmt.Fprintln(os.Stderr, "usage: tinytiles-native-client -manifest http://host/sync/manifest.json -z 8 -xmin 137 -xmax 138 -ymin 167 -ymax 168")
		os.Exit(2)
	}
	store, err := offline.NewFileStore(*cacheDir)
	if err != nil {
		fmt.Fprintln(os.Stderr, "open cache:", err)
		os.Exit(1)
	}
	synchronizer := &offline.Synchronizer{Store: store, Fetcher: &offline.HTTPFetcher{ManifestURL: *manifestURL}}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	result, err := synchronizer.Sync(ctx, offline.SyncRequest{
		Dataset:       *dataset,
		Ranges:        []offline.TileRange{{Z: *z, XMin: *xMin, XMax: *xMax, YMin: *yMin, YMax: *yMax}},
		Concurrency:   *concurrency,
		PrunePrevious: *prune,
		Progress: func(progress offline.SyncProgress) {
			if progress.Phase == "tile" && progress.Completed%100 == 0 {
				fmt.Printf("synced=%d/%d downloaded=%d reused=%d\n", progress.Completed, progress.Total, progress.Downloaded, progress.Reused)
			}
		},
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "sync:", err)
		os.Exit(1)
	}
	fmt.Printf("dataset=%s revision=%s tiles=%d downloaded=%d reused=%d\n", result.Dataset, result.Revision, result.Total, result.Downloaded, result.Reused)
	if result.PruneError != "" {
		fmt.Fprintln(os.Stderr, "prune warning:", result.PruneError)
	}
}

// readCachedTile resolves the active revision from the local manifest and does
// not use the network. Stored bytes retain the tile's original encoding.
func readCachedTile(ctx context.Context, cacheDir, dataset, coordinate, output string) error {
	parts := strings.Split(coordinate, "/")
	if len(parts) != 3 {
		return fmt.Errorf("tile must be z/x/y in TMS coordinates")
	}
	var values [3]int
	for i, part := range parts {
		n, err := strconv.Atoi(part)
		if err != nil {
			return fmt.Errorf("invalid tile coordinate %q: %w", coordinate, err)
		}
		values[i] = n
	}
	key := offline.TileKey{Z: values[0], X: values[1], Y: values[2]}
	if err := key.Validate(); err != nil {
		return err
	}
	store, err := offline.NewFileStore(cacheDir)
	if err != nil {
		return err
	}
	manifest, found, err := store.GetManifest(ctx, dataset)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("no active manifest for dataset %q", dataset)
	}
	tile, found, err := store.GetTile(ctx, dataset, manifest.Revision, key)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("tile %s is not cached for dataset %q revision %q", key, dataset, manifest.Revision)
	}
	if output != "" {
		if err := os.WriteFile(output, tile.Data, 0o600); err != nil {
			return fmt.Errorf("write tile: %w", err)
		}
	}
	fmt.Printf("dataset=%s revision=%s tile=%s bytes=%d content_type=%q content_encoding=%q sha256=%s\n", dataset, manifest.Revision, key, len(tile.Data), tile.ContentType, tile.ContentEncoding, offline.Checksum(tile.Data))
	return nil
}
