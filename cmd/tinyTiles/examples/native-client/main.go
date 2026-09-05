// tinytiles-native-client synchronizes one bounded TMS rectangle into a
// durable FileStore. It is useful both as a command-line demo and as a small
// reference for native mobile/desktop integrations.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
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
	flag.Parse()
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
