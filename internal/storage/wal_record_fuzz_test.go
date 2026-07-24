// WAL record decode fuzzing — optional Stage 0 addition (see
// wal_fixture_test.go's package doc comment for the rest of the
// characterization harness). Mirrors the convention already established by
// internal/storage/pager/row_codec_fuzz_test.go: decode arbitrary bytes and
// assert only that decoding never panics. Decode errors are expected and
// fine; a panic would mean a corrupted or truncated WAL file could crash
// the process during recovery, which is a far worse failure mode than the
// misapplication/detection gaps characterized elsewhere in this package.
package storage

import (
	"bytes"
	"encoding/gob"
	"os"
	"path/filepath"
	"testing"
)

// FuzzWALManagerRecordDecode decodes arbitrary bytes as a stream of
// WALManager's on-disk walRecord values (see db.go), exactly as replayWAL
// does, and asserts it never panics.
func FuzzWALManagerRecordDecode(f *testing.F) {
	if data, err := readFixtureBytesForFuzz("walmanager_legacy.wal"); err == nil {
		f.Add(data)
	}
	f.Add([]byte{})
	f.Add([]byte{0, 0})
	f.Add([]byte{0x7f, 0xff, 0xff, 0xff})
	f.Fuzz(func(t *testing.T, data []byte) {
		dec := gob.NewDecoder(bytes.NewReader(data))
		for i := 0; i < 64; i++ { // bound iterations: a crafted stream of tiny valid records could otherwise loop for a very long time
			var rec walRecord
			if err := dec.Decode(&rec); err != nil {
				return
			}
		}
	})
}

// FuzzAdvancedWALRecordDecode is FuzzWALManagerRecordDecode's counterpart
// for AdvancedWAL's on-disk WALRecord (see wal_advanced.go), exactly as
// Recover does.
func FuzzAdvancedWALRecordDecode(f *testing.F) {
	if data, err := readFixtureBytesForFuzz("advancedwal_legacy.wal"); err == nil {
		f.Add(data)
	}
	f.Add([]byte{})
	f.Add([]byte{0, 0})
	f.Add([]byte{0x7f, 0xff, 0xff, 0xff})
	f.Fuzz(func(t *testing.T, data []byte) {
		dec := gob.NewDecoder(bytes.NewReader(data))
		for i := 0; i < 64; i++ {
			var rec WALRecord
			if err := dec.Decode(&rec); err != nil {
				return
			}
		}
	})
}

// readFixtureBytesForFuzz seeds the fuzz corpus with a real golden fixture
// when available, without failing the seed-add step if it isn't (e.g. run
// from a context where the working directory differs).
func readFixtureBytesForFuzz(name string) ([]byte, error) {
	return os.ReadFile(filepath.Join(walFixturesDir, name))
}
