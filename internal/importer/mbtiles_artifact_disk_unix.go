//go:build (linux || darwin || freebsd || openbsd || netbsd) && sqliteimport && !js && !wasm && !baremetal

package importer

import (
	"golang.org/x/sys/unix"
)

func availableDiskBytes(path string) int64 {
	var stat unix.Statfs_t
	if err := unix.Statfs(path, &stat); err != nil {
		return -1
	}
	return int64(stat.Bavail) * int64(stat.Bsize)
}
