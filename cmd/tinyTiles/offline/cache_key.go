package offline

import (
	"crypto/sha256"
	"encoding/hex"
)

func cacheComponent(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}

func persistentTileKey(dataset, revision string, key TileKey) string {
	return "tile:" + cacheComponent(dataset) + ":" + cacheComponent(revision) + ":" + key.String()
}

func persistentManifestKey(dataset string) string {
	return "manifest:" + cacheComponent(dataset)
}
