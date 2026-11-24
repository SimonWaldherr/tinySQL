package main

import (
	"flag"

	tinysql "github.com/SimonWaldherr/tinySQL"
)

var (
	skipImages = flag.Bool("skipimages", false, "StorageConfig.SkipSearchIndexPersistence (storage=disk|hybrid)")
	rowBlocks  = flag.Bool("rowblocks", false, "StorageConfig.RowBlockFiles (storage=disk|hybrid)")
)

// applyStorageOpts sets the persistence options under test on the storage
// config used by the persist and convoysync modes.
func applyStorageOpts(cfg *tinysql.StorageConfig) {
	cfg.SkipSearchIndexPersistence = *skipImages
	cfg.RowBlockFiles = *rowBlocks
}
