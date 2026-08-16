//go:build !darwin

package storage

import "os"

// syncWALFile uses the portable file-sync implementation outside Darwin.
// There, Go does not provide a stronger File.Sync variant that differs from
// regular fsync, so both policies intentionally have identical behavior.
func syncWALFile(file *os.File, _ WALSyncMode) error {
	if file == nil {
		return nil
	}
	return file.Sync()
}
