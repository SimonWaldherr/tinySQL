package importer

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
)

const mbtilesArtifactFormatVersion = 1

// ErrMBTilesArtifactSQLiteUnavailable reports that importing an MBTiles SQLite
// source needs the optional SQLite importer build tag. Opening an already
// published tinySQL artifact does not need SQLite on supported native targets.
var ErrMBTilesArtifactSQLiteUnavailable = errors.New("MBTiles import requires a build with -tags=sqliteimport")

var ErrTileArtifactImportUnavailable = errors.New("tile artifact import requires a native builder")

func hashMetadata(h io.Writer, name, value string) {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], uint32(len(name)))
	_, _ = h.Write(b[:])
	_, _ = io.WriteString(h, name)
	binary.BigEndian.PutUint32(b[:], uint32(len(value)))
	_, _ = h.Write(b[:])
	_, _ = io.WriteString(h, value)
}

func hashTile(h io.Writer, z, x, y int, data []byte) {
	var b [8]byte
	for _, v := range []int{z, x, y} {
		binary.BigEndian.PutUint64(b[:], uint64(int64(v)))
		_, _ = h.Write(b[:])
	}
	binary.BigEndian.PutUint64(b[:], uint64(len(data)))
	_, _ = h.Write(b[:])
	_, _ = h.Write(data)
}

func validateTileCoordinate(z, x, y int) error {
	if z < 0 || z > 30 {
		return fmt.Errorf("invalid MBTiles zoom %d", z)
	}
	limit := 1 << z
	if x < 0 || y < 0 || x >= limit || y >= limit {
		return fmt.Errorf("invalid TMS tile coordinate z=%d x=%d y=%d", z, x, y)
	}
	return nil
}

func artifactDataFileNames(root string) []string {
	var names []string
	for _, dir := range []string{"database", "indexes"} {
		_ = filepath.Walk(filepath.Join(root, dir), func(path string, info os.FileInfo, err error) error {
			if err == nil && info.Mode().IsRegular() {
				rel, relErr := filepath.Rel(root, path)
				if relErr == nil {
					names = append(names, filepath.ToSlash(rel))
				}
			}
			return nil
		})
	}
	sort.Strings(names)
	return names
}

func checksumFile(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
