//go:build darwin

package storage

import (
	"os"

	"golang.org/x/sys/unix"
)

// syncWALFile keeps ModeWAL's original strongest flush as the default, while
// allowing callers to explicitly choose SQLite-compatible regular fsync. Go's
// os.File.Sync maps to F_FULLFSYNC on Darwin; calling unix.Fsync through
// SyscallConn deliberately selects ordinary fsync without changing the file's
// blocking mode via File.Fd().
func syncWALFile(file *os.File, mode WALSyncMode) error {
	if file == nil {
		return nil
	}
	if mode != WALSyncNormal {
		return file.Sync()
	}
	raw, err := file.SyscallConn()
	if err != nil {
		return err
	}
	var syncErr error
	if err := raw.Control(func(fd uintptr) {
		syncErr = unix.Fsync(int(fd))
	}); err != nil {
		return err
	}
	return syncErr
}
