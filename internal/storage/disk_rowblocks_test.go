package storage

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func setRowBlockTuning(t testing.TB, target, workers int) {
	t.Helper()
	oldTarget, oldWorkers := rowBlockTargetBytes, rowBlockWorkers
	rowBlockTargetBytes = target
	rowBlockWorkers = func() int { return workers }
	t.Cleanup(func() { rowBlockTargetBytes, rowBlockWorkers = oldTarget, oldWorkers })
}

// allTypesTable has one column of every storable kind the file format has to
// carry, with NULLs sprinkled in.
func allTypesTable(name string, n int) *Table {
	table := NewTable(name, []Column{
		{Name: "id", Type: IntType},
		{Name: "score", Type: FloatType},
		{Name: "body", Type: TextType},
		{Name: "flag", Type: BoolType},
		{Name: "doc", Type: JsonType},
		{Name: "blob", Type: BlobType},
		{Name: "emb", Type: VectorType},
	}, false)
	for i := 0; i < n; i++ {
		row := []any{
			i,
			float64(i) / 7,
			fmt.Sprintf("text %d", i),
			i%2 == 0,
			map[string]any{"n": float64(i), "s": "x"},
			[]byte{byte(i), byte(i >> 8), 0xff},
			[]float64{float64(i), 0.5, -1},
		}
		if i%5 == 0 {
			row[2] = nil
		}
		if i%7 == 0 {
			row[4] = nil
			row[5] = nil
		}
		if i%11 == 0 {
			row[6] = nil
			row[1] = nil
		}
		table.Rows = append(table.Rows, row)
	}
	table.Version = 3
	return table
}

func newBlockBackend(t *testing.T, dir string, gzip, blocks bool) *DiskBackend {
	t.Helper()
	b, err := NewDiskBackend(dir, gzip)
	if err != nil {
		t.Fatal(err)
	}
	b.SetRowBlockFiles(blocks)
	return b
}

func tablePath(dir string, gzip bool) string {
	if gzip {
		return filepath.Join(dir, "default", "t.tbl.gz")
	}
	return filepath.Join(dir, "default", "t.tbl")
}

func readHeaderOf(t *testing.T, path string) diskRowBlockHeader {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !hasRowBlockMagic(raw) {
		t.Fatal("file does not start with the row-block magic")
	}
	r := bytes.NewReader(raw[9:])
	payload, _, err := readFrame(r, raw[:9])
	if err != nil {
		t.Fatal(err)
	}
	var hdr diskRowBlockHeader
	if err := decodeFramePayload(payload, raw[8]&rowBlockFlagGzip != 0, &hdr); err != nil {
		t.Fatal(err)
	}
	return hdr
}

func BenchmarkRowBlockReadFrame(b *testing.B) {
	for _, size := range []int{1 << 20, 32 << 20} {
		b.Run(fmt.Sprintf("MiB=%d", size>>20), func(b *testing.B) {
			binding := blockBinding(123, 0)
			var frame bytes.Buffer
			frame.Grow(size + 12)
			crc, err := writeFrame(&frame, binding, make([]byte, size))
			if err != nil {
				b.Fatal(err)
			}
			var reader bytes.Reader
			b.SetBytes(int64(size))
			b.ReportAllocs()
			for b.Loop() {
				reader.Reset(frame.Bytes())
				payload, gotCRC, err := readFrame(&reader, binding)
				if err != nil || len(payload) != size || gotCRC != crc {
					b.Fatalf("read frame: len=%d crc=%08x err=%v", len(payload), gotCRC, err)
				}
			}
		})
	}
}

func BenchmarkRowBlockSaveJSONMetadata(b *testing.B) {
	setRowBlockTuning(b, 2<<20, 3)
	backend, err := NewDiskBackend(b.TempDir(), false)
	if err != nil {
		b.Fatal(err)
	}
	defer backend.Close()
	backend.SetRowBlockFiles(true)
	table := NewTable("vectors", []Column{
		{Name: "id", Type: IntType},
		{Name: "embedding", Type: VectorType},
		{Name: "metadata", Type: JsonType, StrictJSON: true},
	}, false)
	const rowCount, dimensions, metadataBytes = 32_768, 256, 1024
	metadata := map[string]any{"text": strings.Repeat("x", metadataBytes)}
	table.Rows = make([][]any, rowCount)
	for i := range table.Rows {
		vector := make([]float64, dimensions)
		for d := range vector {
			vector[d] = float64(i+d) / 3
		}
		table.Rows[i] = []any{i, vector, metadata}
	}
	b.SetBytes(rowCount * (dimensions*8 + metadataBytes))
	b.ReportAllocs()
	for b.Loop() {
		if err := backend.SaveTable("default", table); err != nil {
			b.Fatal(err)
		}
	}
}

func TestRowBlockFilesRoundTripAllColumnTypes(t *testing.T) {
	setRowBlockTuning(t, 400, 3)
	for _, gzip := range []bool{false, true} {
		t.Run(fmt.Sprintf("gzip=%v", gzip), func(t *testing.T) {
			live := allTypesTable("t", 300)
			if err := live.CreateSecondaryIndex("idx_id", []string{"id"}, true); err != nil {
				t.Fatal(err)
			}
			classicDir, blockDir := t.TempDir(), t.TempDir()
			classic := newBlockBackend(t, classicDir, gzip, false)
			blocks := newBlockBackend(t, blockDir, gzip, true)
			if err := classic.SaveTable("default", live); err != nil {
				t.Fatal(err)
			}
			if err := blocks.SaveTable("default", live); err != nil {
				t.Fatal(err)
			}
			hdr := readHeaderOf(t, tablePath(blockDir, gzip))
			if len(hdr.BlockRows) < 10 {
				t.Fatalf("only %d blocks, the tiny block size was not applied", len(hdr.BlockRows))
			}
			total := 0
			for _, n := range hdr.BlockRows {
				total += n
			}
			if total != 300 || hdr.Table.Rows != nil {
				t.Fatalf("header rows: total=%d tableRows=%v", total, hdr.Table.Rows != nil)
			}

			want, err := classic.LoadTable("default", "t")
			if err != nil || want == nil {
				t.Fatalf("load classic: %v", err)
			}
			got, err := blocks.LoadTable("default", "t")
			if err != nil || got == nil {
				t.Fatalf("load blocks: %v", err)
			}
			if !reflect.DeepEqual(got.Rows, want.Rows) || !reflect.DeepEqual(got.Cols, want.Cols) || got.Version != want.Version {
				t.Fatal("block file loads differently from the classic file")
			}
			if len(got.Indexes["idx_id"].Entries) != 300 {
				t.Fatalf("secondary index lost: %d entries", len(got.Indexes["idx_id"].Entries))
			}
			// Either backend reads either layout, whatever its own setting.
			cross := newBlockBackend(t, blockDir, gzip, false)
			if again, err := cross.LoadTable("default", "t"); err != nil || !reflect.DeepEqual(again.Rows, want.Rows) {
				t.Fatalf("flag-off backend cannot read the block file: %v", err)
			}
			crossClassic := newBlockBackend(t, classicDir, gzip, true)
			if again, err := crossClassic.LoadTable("default", "t"); err != nil || !reflect.DeepEqual(again.Rows, want.Rows) {
				t.Fatalf("flag-on backend cannot read the classic file: %v", err)
			}
		})
	}
}

type rowBlockJSONSequence struct{ next *atomic.Int32 }

func (v rowBlockJSONSequence) MarshalJSON() ([]byte, error) {
	return strconv.AppendInt(nil, int64(v.next.Add(1)), 10), nil
}

type rowBlockJSONPanic struct{ err error }

func (v rowBlockJSONPanic) MarshalJSON() ([]byte, error) { panic(v.err) }

func TestRowBlockFilesConvertJSONWhenWriting(t *testing.T) {
	setRowBlockTuning(t, 70, 3)
	dir := t.TempDir()
	b := newBlockBackend(t, dir, false, true)
	table := NewTable("t", []Column{
		{Name: "embedding", Type: VectorType},
		{Name: "strict_metadata", Type: JsonType, StrictJSON: true},
		{Name: "metadata", Type: JsonType},
	}, false)
	if err := b.SaveTable("default", table); err != nil {
		t.Fatal(err)
	}
	var converted atomic.Int32
	for i := 0; i < 50; i++ {
		table.Rows = append(table.Rows, []any{[]float64{float64(i), 0.5}, rowBlockJSONSequence{&converted}, `{"sample":true}`})
	}
	openErr := errors.New("before writing row blocks")
	setTableFileWriteHook(t, &tableFileWriteTestHook{Before: func(string) error { return openErr }})
	if err := b.SaveTable("default", table); !errors.Is(err, openErr) {
		t.Fatalf("save with write failure: %v", err)
	}
	if n := converted.Load(); n != 0 {
		t.Fatalf("converted %d rows before writing began", n)
	}
	if old, err := b.LoadTable("default", "t"); err != nil || old == nil || len(old.Rows) != 0 {
		t.Fatalf("previous file after failed save: %v, err=%v", old, err)
	}
	if _, err := os.Stat(tablePath(dir, false) + ".tmp"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("temporary file after failed save: %v", err)
	}
	tableFileWriteHook.Store(nil)
	if err := b.SaveTable("default", table); err != nil {
		t.Fatal(err)
	}
	got, err := b.LoadTable("default", "t")
	if err != nil || got == nil {
		t.Fatalf("load converted rows: %v", err)
	}
	if len(got.Rows) != len(table.Rows) || converted.Load() != int32(len(table.Rows)) {
		t.Fatalf("persisted %d rows after %d conversions, want %d", len(got.Rows), converted.Load(), len(table.Rows))
	}
	for i, row := range got.Rows {
		want := []any{table.Rows[i][0], float64(i + 1), map[string]any{"sample": true}}
		if !reflect.DeepEqual(row, want) {
			t.Fatalf("row %d: got %v, want %v", i, row, want)
		}
	}

	panicErr := errors.New("JSON marshaler panic")
	table.Rows[20][1] = rowBlockJSONPanic{panicErr}
	func() {
		defer func() {
			if p := recover(); p != panicErr {
				t.Fatalf("save recovered %v, want %v", p, panicErr)
			}
		}()
		_ = b.SaveTable("default", table)
	}()
	if _, err := os.Stat(tablePath(dir, false) + ".tmp"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("temporary file after marshaler panic: %v", err)
	}
	if old, err := b.LoadTable("default", "t"); err != nil || old == nil || !reflect.DeepEqual(old.Rows, got.Rows) {
		t.Fatalf("previous file changed after marshaler panic, err=%v", err)
	}
}

func TestRowBlockFilesKeepSearchImagesAndEmptyTables(t *testing.T) {
	setRowBlockTuning(t, 64, 2)
	dir := t.TempDir()
	b := newBlockBackend(t, dir, false, true)
	live := searchImageTable(t, "t")
	if err := b.SaveTable("default", live); err != nil {
		t.Fatal(err)
	}
	got, err := b.LoadTable("default", "t")
	if err != nil || got == nil {
		t.Fatal(err)
	}
	if len(got.FTSIndexes) != 1 || len(got.VectorIndexes) != 1 || len(got.Indexes) != 1 || len(got.Rows) != 2 {
		t.Fatalf("images lost: fts=%d ann=%d idx=%d rows=%d", len(got.FTSIndexes), len(got.VectorIndexes), len(got.Indexes), len(got.Rows))
	}
	// A zero-row table has a header and no blocks.
	empty := NewTable("e", []Column{{Name: "id", Type: IntType}}, false)
	if err := b.SaveTable("default", empty); err != nil {
		t.Fatal(err)
	}
	if e, err := b.LoadTable("default", "e"); err != nil || e == nil || len(e.Rows) != 0 {
		t.Fatalf("empty table: %v %v", e, err)
	}
	// With the skip flag the images stay out of block files as well.
	b.SetSkipSearchIndexPersistence(true)
	if err := b.SaveTable("default", live); err != nil {
		t.Fatal(err)
	}
	if h := readHeaderOf(t, filepath.Join(dir, "default", "t.tbl")); len(h.Table.FTSIndexes) != 0 || len(h.Table.VectorIndexes) != 0 {
		t.Fatal("skip flag ignored by the row-block layout")
	}
}

func TestRowBlockFilesDetectTruncationAndCorruption(t *testing.T) {
	setRowBlockTuning(t, 300, 2)
	for _, gzip := range []bool{false, true} {
		dir := t.TempDir()
		b := newBlockBackend(t, dir, gzip, true)
		if err := b.SaveTable("default", allTypesTable("t", 120)); err != nil {
			t.Fatal(err)
		}
		path := tablePath(dir, gzip)
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := b.LoadTable("default", "t"); err != nil {
			t.Fatalf("intact file: %v", err)
		}
		hdr := readHeaderOf(t, path)

		// Every prefix of the file (stepping to keep the test short) fails to
		// load, including the ones that end exactly on a block boundary.
		boundaries := map[int]bool{}
		{
			off := 9
			r := bytes.NewReader(raw[off:])
			var headerCRC uint32
			for i := 0; i <= len(hdr.BlockRows); i++ {
				binding := raw[:9]
				if i > 0 {
					binding = blockBinding(headerCRC, i-1)
				}
				p, crc, err := readFrame(r, binding)
				if err != nil {
					t.Fatal(err)
				}
				if i == 0 {
					headerCRC = crc
				}
				off += 12 + len(p)
				boundaries[off] = true
			}
		}
		for cut := 0; cut < len(raw); cut += 1 + len(raw)/257 {
			if err := os.WriteFile(path, raw[:cut], 0o644); err != nil {
				t.Fatal(err)
			}
			if got, err := b.LoadTable("default", "t"); err == nil {
				t.Fatalf("gzip=%v: file cut at %d of %d bytes loaded %d rows without error", gzip, cut, len(raw), len(got.Rows))
			}
		}
		for off := range boundaries {
			if off >= len(raw) {
				continue
			}
			if err := os.WriteFile(path, raw[:off], 0o644); err != nil {
				t.Fatal(err)
			}
			if _, err := b.LoadTable("default", "t"); err == nil {
				t.Fatalf("gzip=%v: file cut at block boundary %d loaded without error", gzip, off)
			}
		}

		// One flipped bit anywhere past the magic is caught by a checksum.
		for pos := 9; pos < len(raw); pos += 1 + len(raw)/131 {
			bad := append([]byte(nil), raw...)
			bad[pos] ^= 0x10
			if err := os.WriteFile(path, bad, 0o644); err != nil {
				t.Fatal(err)
			}
			if _, err := b.LoadTable("default", "t"); err == nil {
				t.Fatalf("gzip=%v: flipped bit at byte %d went unnoticed", gzip, pos)
			}
		}

		// Trailing bytes are not ignored either.
		if err := os.WriteFile(path, append(append([]byte(nil), raw...), 0), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := b.LoadTable("default", "t"); err == nil {
			t.Fatal("trailing data went unnoticed")
		}
		if err := os.WriteFile(path, raw, 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := b.LoadTable("default", "t"); err != nil {
			t.Fatalf("restored file: %v", err)
		}
	}
}

// The bytes of a block file do not depend on how many workers encoded it, and
// are the same on every run.
func TestRowBlockFilesAreIdenticalForEveryWorkerCount(t *testing.T) {
	for _, gzip := range []bool{false, true} {
		var first []byte
		for _, workers := range []int{1, 2, 3, 8} {
			setRowBlockTuning(t, 500, workers)
			dir := t.TempDir()
			b := newBlockBackend(t, dir, gzip, true)
			// No secondary index or search image: the header's Go maps would
			// make the bytes depend on map iteration order, as in the classic
			// layout. The row blocks themselves are what is under test.
			if err := b.SaveTable("default", allTypesTable("t", 400)); err != nil {
				t.Fatal(err)
			}
			raw, err := os.ReadFile(tablePath(dir, gzip))
			if err != nil {
				t.Fatal(err)
			}
			if first == nil {
				first = raw
			} else if !bytes.Equal(first, raw) {
				t.Fatalf("gzip=%v: output with %d workers differs from the 1-worker output", gzip, workers)
			}
			got, err := b.LoadTable("default", "t")
			if err != nil || len(got.Rows) != 400 {
				t.Fatalf("workers=%d: load: %v", workers, err)
			}
		}
	}
}

// Encoding a table in blocks needs memory for a few blocks, not for the whole
// encoded message.
func TestRowBlockFilesBoundPeakAllocation(t *testing.T) {
	if testing.Short() {
		t.Skip("allocates a few hundred MB")
	}
	const rows, dims = 3000, 8000 // about 192 MB of vectors
	table := NewTable("t", []Column{{Name: "id", Type: IntType}, {Name: "emb", Type: VectorType}}, false)
	for i := 0; i < rows; i++ {
		v := make([]float64, dims)
		for d := range v {
			v[d] = float64(i*d%97) + 0.25
		}
		table.Rows = append(table.Rows, []any{i, v})
	}
	setRowBlockTuning(t, 8<<20, 2)
	// Garbage that has not been collected yet counts as heap: collect eagerly so
	// the peak reflects what the save really holds.
	defer debug.SetGCPercent(debug.SetGCPercent(10))

	peakOf := func(save func() error) uint64 {
		runtime.GC()
		var base runtime.MemStats
		runtime.ReadMemStats(&base)
		var peak atomic.Uint64
		stop, done := make(chan struct{}), make(chan struct{})
		go func() {
			defer close(done)
			for {
				var m runtime.MemStats
				runtime.ReadMemStats(&m)
				if m.HeapAlloc > peak.Load() {
					peak.Store(m.HeapAlloc)
				}
				select {
				case <-stop:
					return
				case <-time.After(5 * time.Millisecond):
				}
			}
		}()
		if err := save(); err != nil {
			t.Error(err)
		}
		close(stop)
		<-done
		if peak.Load() < base.HeapAlloc {
			return 0
		}
		return peak.Load() - base.HeapAlloc
	}
	classic := newBlockBackend(t, t.TempDir(), false, false)
	blocks := newBlockBackend(t, t.TempDir(), false, true)
	classicPeak := peakOf(func() error { return classic.SaveTable("default", table) })
	blockPeak := peakOf(func() error { return blocks.SaveTable("default", table) })
	t.Logf("peak heap over the live table: classic %d MB, row blocks %d MB", classicPeak>>20, blockPeak>>20)
	// 2 workers, a window of 4 blocks of about 8 MB of rows (each block encodes
	// to roughly the same size), plus the writer's own buffers.
	if limit := uint64(140 << 20); blockPeak > limit {
		t.Fatalf("row-block save peaked at %d MB, want below %d MB", blockPeak>>20, limit>>20)
	}
	if blockPeak*2 > classicPeak {
		t.Fatalf("row blocks (%d MB) did not need clearly less than the single message (%d MB)", blockPeak>>20, classicPeak>>20)
	}
}

// The whole Sync path works with the row-block layout, including a Sync that
// runs while the table changes.
func TestSyncWithRowBlockFiles(t *testing.T) {
	setRowBlockTuning(t, 600, 3)
	for _, mode := range []StorageMode{ModeDisk, ModeHybrid} {
		t.Run(mode.String(), func(t *testing.T) {
			dir := t.TempDir()
			cfg := StorageConfig{Mode: mode, Path: dir, MaxMemoryBytes: 64 << 20, RowBlockFiles: true}
			db, err := OpenDB(cfg)
			if err != nil {
				t.Fatal(err)
			}
			live := snapshotTestTable(t, "docs", 400)
			if err := db.Put("default", live); err != nil {
				t.Fatal(err)
			}
			setSyncHook(t, func() {
				db.LockContentForWrite()
				appendRow(t, live, 9000)
				db.UnlockContentForWrite()
			})
			db.LockContentForWrite()
			appendRow(t, live, 400)
			db.UnlockContentForWrite()
			if err := db.Sync(); err != nil {
				t.Fatal(err)
			}
			syncSnapshotHook.Store(nil)
			if err := db.Sync(); err != nil {
				t.Fatal(err)
			}
			if h := readHeaderOf(t, filepath.Join(dir, "default", "docs.tbl")); len(h.BlockRows) < 5 {
				t.Fatalf("only %d blocks", len(h.BlockRows))
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := OpenDB(cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			got, err := reopened.Get("default", "docs")
			if err != nil {
				t.Fatal(err)
			}
			if len(got.Rows) != 402 || len(got.Indexes["idx_id"].Entries) != 402 {
				t.Fatalf("reopened: %d rows, %d index entries, want 402", len(got.Rows), len(got.Indexes["idx_id"].Entries))
			}
		})
	}
}

// Encryption keeps the classic whole-file layout (one AEAD blob).
func TestRowBlockFilesFallBackToClassicLayoutWhenEncrypted(t *testing.T) {
	dir := t.TempDir()
	b := newBlockBackend(t, dir, false, true)
	enc, err := NewEncryptor(bytes.Repeat([]byte{7}, EncryptionKeySize))
	if err != nil {
		t.Fatal(err)
	}
	b.SetEncryptor(enc)
	if err := b.SaveTable("default", allTypesTable("t", 20)); err != nil {
		t.Fatal(err)
	}
	got, err := b.LoadTable("default", "t")
	if err != nil || len(got.Rows) != 20 {
		t.Fatalf("encrypted round trip: %v", err)
	}
	raw, _ := os.ReadFile(filepath.Join(dir, "default", "t.tbl"))
	if hasRowBlockMagic(raw) {
		t.Fatal("encrypted file is not ciphertext")
	}
}

// frameOffsets returns the byte offsets at which the header frame and every
// block frame of a row-block file start and end.
func frameOffsets(t *testing.T, raw []byte) (starts, ends []int) {
	t.Helper()
	off := 9
	r := bytes.NewReader(raw[off:])
	var headerCRC uint32
	for i := 0; ; i++ {
		binding := raw[:9]
		if i > 0 {
			binding = blockBinding(headerCRC, i-1)
		}
		p, crc, err := readFrame(r, binding)
		if err != nil {
			if i == 0 {
				t.Fatal(err)
			}
			return starts, ends
		}
		if i == 0 {
			headerCRC = crc
		}
		starts = append(starts, off)
		off += 12 + len(p)
		ends = append(ends, off)
		if off >= len(raw) {
			return starts, ends
		}
	}
}

// A block that is intact on its own but sits in the wrong place, or comes from
// another version of the file, is rejected: the checksum covers its position and
// the header.
func TestRowBlockFilesRejectReorderedAndForeignBlocks(t *testing.T) {
	// Fixed-size rows and a block size of a few rows: all full blocks have the
	// same length, so swapping two keeps every length and row count plausible.
	setRowBlockTuning(t, 4*(8+9+20), 1)
	dir := t.TempDir()
	b := newBlockBackend(t, dir, false, true)
	mk := func(version int) *Table {
		table := NewTable("t", []Column{{Name: "id", Type: IntType}, {Name: "s", Type: TextType}}, false)
		for i := 0; i < 64; i++ {
			table.Rows = append(table.Rows, []any{1000 + i, fmt.Sprintf("row%05d", i)})
		}
		table.Version = version
		return table
	}
	if err := b.SaveTable("default", mk(1)); err != nil {
		t.Fatal(err)
	}
	path := tablePath(dir, false)
	v1, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.SaveTable("default", mk(2)); err != nil {
		t.Fatal(err)
	}
	v2, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	starts, ends := frameOffsets(t, v2)
	if len(starts) < 5 {
		t.Fatalf("only %d frames, want a header and several blocks", len(starts))
	}
	size := func(i int) int { return ends[i] - starts[i] }
	if size(1) != size(2) {
		t.Fatalf("blocks 0 and 1 differ in size (%d, %d); the test needs equal blocks", size(1), size(2))
	}

	// Blocks 0 and 1 swapped.
	swapped := append([]byte(nil), v2...)
	copy(swapped[starts[1]:ends[1]], v2[starts[2]:ends[2]])
	copy(swapped[starts[2]:ends[2]], v2[starts[1]:ends[1]])
	if err := os.WriteFile(path, swapped, 0o644); err != nil {
		t.Fatal(err)
	}
	if got, err := b.LoadTable("default", "t"); err == nil {
		t.Fatalf("reordered blocks loaded %d rows without error", len(got.Rows))
	}

	// A block of the previous version of the file (same shape, other header).
	s1, e1 := frameOffsets(t, v1)
	if e1[1]-s1[1] != size(1) {
		t.Fatal("the two versions' blocks differ in size")
	}
	foreign := append([]byte(nil), v2...)
	copy(foreign[starts[1]:ends[1]], v1[s1[1]:e1[1]])
	if err := os.WriteFile(path, foreign, 0o644); err != nil {
		t.Fatal(err)
	}
	if got, err := b.LoadTable("default", "t"); err == nil {
		t.Fatalf("a block of another file version loaded (%d rows) without error", len(got.Rows))
	}

	// Intact again.
	if err := os.WriteFile(path, v2, 0o644); err != nil {
		t.Fatal(err)
	}
	if got, err := b.LoadTable("default", "t"); err != nil || len(got.Rows) != 64 {
		t.Fatalf("intact file: %v", err)
	}

	// Unknown flag bits: a file from a newer writer is refused, not guessed at.
	flagged := append([]byte(nil), v2...)
	flagged[8] |= 0x02
	if err := os.WriteFile(path, flagged, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := b.LoadTable("default", "t"); err == nil {
		t.Fatal("unknown flag bits went unnoticed")
	}
}

// The block boundaries follow the real size of nested cells and never hold more
// rows than the cap.
func TestRowBlockSplitHonoursNestedCellsAndRowCap(t *testing.T) {
	big := make([]any, 500)
	for i := range big {
		big[i] = "0123456789"
	}
	rows := make([][]any, 40)
	for i := range rows {
		rows[i] = []any{i, big, map[string]any{"k": big}}
	}
	if got := estimateRowBytes(rows[0]); got < 500*10 {
		t.Fatalf("estimate %d for a row holding 5000+ bytes of nested strings", got)
	}
	// 40 rows of about 12 KB at a 100 KB target: several blocks, not one.
	if spans := splitRowBlocks(rows, 100_000); len(spans) < 3 {
		t.Fatalf("%d blocks for 480 KB of nested cells at a 100 KB target", len(spans))
	}

	old := rowBlockMaxRows
	rowBlockMaxRows = 7
	defer func() { rowBlockMaxRows = old }()
	tiny := make([][]any, 50)
	for i := range tiny {
		tiny[i] = []any{i}
	}
	spans := splitRowBlocks(tiny, 1<<30)
	covered := 0
	for _, s := range spans {
		if s.end-s.start > 7 || s.end <= s.start {
			t.Fatalf("block %v exceeds the row cap", s)
		}
		covered += s.end - s.start
	}
	if covered != 50 || spans[0].start != 0 || spans[len(spans)-1].end != 50 {
		t.Fatalf("blocks %v do not cover the rows", spans)
	}
}

// goroutinesSettle waits for the goroutine count to fall back to want.
func goroutinesSettle(t *testing.T, want int, what string) {
	t.Helper()
	for i := 0; i < 100; i++ {
		if runtime.NumGoroutine() <= want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	buf := make([]byte, 1<<16)
	t.Fatalf("%s left goroutines behind (%d, want %d):\n%s", what, runtime.NumGoroutine(), want, buf[:runtime.Stack(buf, true)])
}

// Errors on the parallel encode and decode paths end every goroutine they
// started.
func TestRowBlockParallelErrorPathsDoNotLeakGoroutines(t *testing.T) {
	setRowBlockTuning(t, 300, 4)
	base := runtime.NumGoroutine()

	// An encode that fails in the middle.
	var sink bytes.Buffer
	prepare := func(int) [][]any { return nil }
	err := writeBlocksParallel(&sink, 0, 40, 4, prepare, func(i int, _ [][]any) ([]byte, error) {
		if i == 13 {
			return nil, fmt.Errorf("encode boom")
		}
		return []byte{byte(i)}, nil
	})
	if err == nil {
		t.Fatal("an encode error was swallowed")
	}
	goroutinesSettle(t, base, "a failed parallel encode")

	// A write that fails in the middle.
	fw := &failingWriter{w: &sink, left: 100}
	err = writeBlocksParallel(fw, 0, 60, 4, prepare, func(int, [][]any) ([]byte, error) { return bytes.Repeat([]byte{1}, 30), nil })
	if err == nil {
		t.Fatal("a write error was swallowed")
	}
	goroutinesSettle(t, base, "a failed parallel write")

	// A corrupt block in the middle of a file read by several workers.
	dir := t.TempDir()
	b := newBlockBackend(t, dir, false, true)
	if err := b.SaveTable("default", allTypesTable("t", 200)); err != nil {
		t.Fatal(err)
	}
	path := tablePath(dir, false)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	starts, ends := frameOffsets(t, raw)
	mid := len(starts) / 2
	raw[(starts[mid]+ends[mid])/2] ^= 0x40
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := b.LoadTable("default", "t"); err == nil {
		t.Fatal("a corrupt block went unnoticed")
	}
	goroutinesSettle(t, base, "a failed parallel decode")
}

// Options that cannot be honoured are refused when the database is opened
// instead of being ignored silently.
func TestOpenDBRefusesRowBlockFilesWhereTheyCannotApply(t *testing.T) {
	key := bytes.Repeat([]byte{3}, EncryptionKeySize)
	for _, tc := range []struct {
		name string
		cfg  StorageConfig
	}{
		{"disk+encryption", StorageConfig{Mode: ModeDisk, RowBlockFiles: true, EncryptionKey: key}},
		{"hybrid+encryption", StorageConfig{Mode: ModeHybrid, RowBlockFiles: true, EncryptionKey: key}},
		{"index+encryption", StorageConfig{Mode: ModeIndex, RowBlockFiles: true, EncryptionKey: key}},
		{"json", StorageConfig{Mode: ModeJSON, RowBlockFiles: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.cfg.Path = t.TempDir()
			db, err := OpenDB(tc.cfg)
			if err == nil {
				_ = db.Close()
				t.Fatal("OpenDB accepted RowBlockFiles where it has no effect")
			}
			if !strings.Contains(err.Error(), "RowBlockFiles") {
				t.Fatalf("error does not name the option: %v", err)
			}
		})
	}
	// The supported combinations still open.
	for _, cfg := range []StorageConfig{
		{Mode: ModeDisk, RowBlockFiles: true},
		{Mode: ModeHybrid, RowBlockFiles: true},
		{Mode: ModeDisk, EncryptionKey: key},
	} {
		cfg.Path = t.TempDir()
		db, err := OpenDB(cfg)
		if err != nil {
			t.Fatalf("%v: %v", cfg.Mode, err)
		}
		_ = db.Close()
	}
}
